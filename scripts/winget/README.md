# Winget manifest templates

Templates and a generator for [Windows Package Manager (winget)](https://learn.microsoft.com/en-us/windows/package-manager/) manifests. Used for the one-time submission and as reference for automation.

## Files

| File | Purpose |
|------|---------|
| `derekwisong.datui.yaml.template` | Version manifest (PackageIdentifier, PackageVersion, DefaultLocale). |
| `derekwisong.datui.locale.en-US.yaml.template` | Default locale (Publisher, PackageName, description, license, URLs). |
| `derekwisong.datui.installer.yaml.template` | Installer (zip URL, SHA256, scope, install modes). |
| `generate_manifests.sh` | Substitutes `VERSION`, `TAG`, and `SHA256` and writes to `out/`. |

## Generate manifests for a release

> Use the appropriate version number

1. Download the Windows zip from the [release](https://github.com/derekwisong/datui/releases) and compute its SHA256:
   - Linux/macOS: `sha256sum datui-v0.2.34-windows-x86_64.zip`
   - Windows: `Get-FileHash -Algorithm SHA256 .\datui-v0.2.34-windows-x86_64.zip | Select-Object -ExpandProperty Hash`
2. From the repo root:
   ```bash
   export VERSION=0.2.34 TAG=v0.2.34 SHA256=<paste_hash_here>
   scripts/winget/generate_manifests.sh
   ```
3. Copy the files from `scripts/winget/out/` into your winget-pkgs clone at:
   `manifests/d/derekwisong/datui/0.2.34/`
