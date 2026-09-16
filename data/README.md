# Common-password blocklist

`common-passwords.txt` is the NFC-normalized, sorted, deduplicated subset of
SecLists' `Passwords/Common-Credentials/100k-most-used-passwords-NCSC.txt` that
satisfies this application's length limits (15–256 Unicode code points and no
more than 1,024 UTF-8 bytes). Shorter and longer entries are already rejected by
the password policy. This keeps the vendored data reviewable without dropping
any otherwise-accepted password from the source list. Comparison uses the whole,
case-sensitive normalized password, not substrings.

- Source: https://github.com/danielmiessler/SecLists/blob/1a7bb9127eca9e6ff2fc0301c597fe6e16a0cb56/Passwords/Common-Credentials/100k-most-used-passwords-NCSC.txt
- Upstream commit: `1a7bb9127eca9e6ff2fc0301c597fe6e16a0cb56`
- Original SHA-256: `c2e5696882c603b76bb67a47ee970897e5a76fc4c3f5547abe3d0ca340c576e0`
- Bundled SHA-256: `279bf70358d4c849051ffa87523eed2ca4f941488c6e6dfee0b83cee3e962955`
- Bundled entries: 331
- License: MIT; the upstream notice is preserved in `SecLists-LICENSE`.

Reproduce the transformation from the pinned, checksum-verified source using
Python's `unicodedata.normalize('NFC', line)`, filter by the limits above, sort
the unique strings, and write UTF-8 with an LF after each entry. Updates must
review the source, license, checksums, and password-policy coverage together.
The list is embedded in the binary; passwords never leave the service for a
blocklist lookup. It is a finite common-password list, not an exhaustive breach
corpus or a guarantee that a chosen password is strong.
