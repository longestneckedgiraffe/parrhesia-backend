#!/usr/bin/env bash

set -euo pipefail

REPO_DIR="${PARRHESIA_REPO_DIR:-/home/parrhesia/parrhesia}"
BRANCH="${PARRHESIA_DEPLOY_BRANCH:-main}"
SERVICE="${PARRHESIA_SERVICE:-parrhesia}"
GITHUB_REPO="${PARRHESIA_GITHUB_REPO:-longestneckedgiraffe/parrhesia-backend}"
FETCH_URL="${PARRHESIA_FETCH_URL:-https://github.com/${GITHUB_REPO}.git}"
ALLOWED_SIGNERS="${PARRHESIA_ALLOWED_SIGNERS:-/home/parrhesia/.config/parrhesia-deploy/allowed_signers}"
HEALTH_URL="${PARRHESIA_HEALTH_URL:-http://127.0.0.1:3000/health}"
HEALTH_RETRIES="${PARRHESIA_HEALTH_RETRIES:-15}"
HEALTH_INTERVAL="${PARRHESIA_HEALTH_INTERVAL:-2}"
ENABLE_CI_GATE="${PARRHESIA_CI_GATE:-true}"
STATE_DIR="${PARRHESIA_STATE_DIR:-/home/parrhesia/.local/state/parrhesia-deploy}"
DRY_RUN="${PARRHESIA_DRY_RUN:-false}"

BINARY_REL="target/release/${SERVICE}"
LOCK_FILE="${STATE_DIR}/deploy.lock"

export PATH="${HOME}/.cargo/bin:/usr/local/bin:/usr/bin:/bin"
if [ -f "${HOME}/.cargo/env" ]; then
    source "${HOME}/.cargo/env"
fi

log() { printf '%s deploy: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
die() { log "ERROR: $*"; exit 1; }

mkdir -p "$STATE_DIR"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    log "another deploy run is in progress; skipping"
    exit 0
fi

cd "$REPO_DIR" || die "repo dir not found: $REPO_DIR"

if [ "$DRY_RUN" != "true" ]; then
    CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
    if [ "$CURRENT_BRANCH" != "$BRANCH" ]; then
        log "checkout is on '${CURRENT_BRANCH}', not '${BRANCH}'; skipping"
        exit 0
    fi
    if ! git diff --quiet || ! git diff --cached --quiet; then
        die "working tree is dirty; refusing to deploy"
    fi
fi

git fetch --quiet "$FETCH_URL" "$BRANCH" || die "git fetch failed"
REMOTE_SHA="$(git rev-parse FETCH_HEAD)"
DEPLOYED_SHA="$(cat "${STATE_DIR}/deployed.sha" 2>/dev/null || echo none)"
FAILED_SHA="$(cat "${STATE_DIR}/failed.sha" 2>/dev/null || echo none)"

if [ "$REMOTE_SHA" = "$DEPLOYED_SHA" ]; then
    exit 0
fi
if [ "$REMOTE_SHA" = "$FAILED_SHA" ]; then
    log "commit ${REMOTE_SHA:0:12} previously failed; waiting for a new commit"
    exit 0
fi
log "new ${BRANCH} tip ${REMOTE_SHA:0:12} (deployed: ${DEPLOYED_SHA:0:12})"

[ -f "$ALLOWED_SIGNERS" ] || die "allowed_signers file missing: $ALLOWED_SIGNERS"
if ! git -c gpg.ssh.allowedSignersFile="$ALLOWED_SIGNERS" verify-commit FETCH_HEAD 2>/dev/null; then
    die "commit ${REMOTE_SHA:0:12} is not signed by a trusted key; refusing to deploy"
fi
log "signature OK for ${REMOTE_SHA:0:12}"

if [ "$ENABLE_CI_GATE" = "true" ]; then
    CONCLUSION="$(curl -fsS --max-time 15 \
        "https://api.github.com/repos/${GITHUB_REPO}/commits/${REMOTE_SHA}/check-runs" \
        | grep -o '"conclusion":[ ]*"[^"]*"' | head -1 \
        | sed 's/.*"\([^"]*\)"$/\1/' || echo "")"
    if [ "$CONCLUSION" != "success" ]; then
        log "CI not green for ${REMOTE_SHA:0:12} (conclusion='${CONCLUSION:-pending}'); will retry"
        exit 0
    fi
    log "CI green for ${REMOTE_SHA:0:12}"
fi

if [ "$DRY_RUN" = "true" ]; then
    log "DRY RUN: would deploy ${REMOTE_SHA:0:12}; stopping before build/restart"
    exit 0
fi

log "fast-forwarding to ${REMOTE_SHA:0:12} and building"
git checkout --quiet "$BRANCH"
git merge --ff-only --quiet FETCH_HEAD || die "local ${BRANCH} diverged; refusing non-fast-forward"

if [ -f "$BINARY_REL" ]; then
    cp -p "$BINARY_REL" "${STATE_DIR}/parrhesia.prev"
fi

if ! cargo build --release --locked; then
    die "build failed; previous binary still running"
fi

log "restarting ${SERVICE}"
sudo -n systemctl restart "$SERVICE" || die "systemctl restart failed"

healthy=false
for _ in $(seq 1 "$HEALTH_RETRIES"); do
    if curl -fsS --max-time 3 "$HEALTH_URL" >/dev/null 2>&1; then
        healthy=true
        break
    fi
    sleep "$HEALTH_INTERVAL"
done

if [ "$healthy" != "true" ]; then
    log "health check failed; rolling back"
    if [ -f "${STATE_DIR}/parrhesia.prev" ]; then
        cp -p "${STATE_DIR}/parrhesia.prev" "$BINARY_REL"
        sudo -n systemctl restart "$SERVICE" || log "WARNING: rollback restart failed"
    fi
    if [ "$DEPLOYED_SHA" != "none" ]; then
        git reset --hard --quiet "$DEPLOYED_SHA" || log "WARNING: tree reset failed"
    fi
    echo "$REMOTE_SHA" > "${STATE_DIR}/failed.sha"
    die "deploy of ${REMOTE_SHA:0:12} failed health check; rolled back"
fi

echo "$REMOTE_SHA" > "${STATE_DIR}/deployed.sha"
rm -f "${STATE_DIR}/failed.sha"
log "deployed ${REMOTE_SHA:0:12} successfully"
