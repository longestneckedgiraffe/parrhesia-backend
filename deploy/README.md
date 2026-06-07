# deploy

Pull-based and signature verified auto deployment for Parrhesia.

## Setup

```bash
mkdir -p ~/.config/parrhesia-deploy

printf '%s %s\n' 'contact@ridhwanzaman.me' "$(cat ~/.ssh/signing.pub)" > ~/.config/parrhesia-deploy/allowed_signers

chmod +x deploy/deploy.sh
PARRHESIA_DRY_RUN=true deploy/deploy.sh

sudo install -m 0440 -o root -g root deploy/sudoers.d-parrhesia-deploy /etc/sudoers.d/parrhesia-deploy

sudo cp deploy/parrhesia-deploy.service deploy/parrhesia-deploy.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now parrhesia-deploy.timer
```

## Notes

- This runs in the dev checkout by default and pauses while you're on a branch or if the tree is dirty. For a dedicated clone, set `PARRHESIA_REPO_DIR`.
- Deployments restart the service and may drop connections. 
- It's heavily recommended to require signed commits on `main`.
