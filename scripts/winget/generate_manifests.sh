#!/usr/bin/env bash
# Generate winget manifest files from templates.
# Usage:
#   export VERSION=0.2.34 TAG=v0.2.34 SHA256=<hash>
#   scripts/winget/generate_manifests.sh
# Output is written to scripts/winget/out/ (filenames without .template).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${SCRIPT_DIR}/out"

if [[ -z "${VERSION:-}" ]] || [[ -z "${TAG:-}" ]] || [[ -z "${SHA256:-}" ]]; then
  echo "Usage: VERSION=0.2.34 TAG=v0.2.34 SHA256=<sha256> $0" >&2
  echo "  VERSION: version without 'v' (e.g. 0.2.34)" >&2
  echo "  TAG: tag with 'v' (e.g. v0.2.34)" >&2
  echo "  SHA256: SHA256 hash of datui-<tag>-windows-x86_64.zip" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

for t in "$SCRIPT_DIR"/*.template; do
  base=$(basename "$t")
  out_name="${base%.template}"
  sed -e "s/VERSION/$VERSION/g" -e "s/TAG/$TAG/g" -e "s/SHA256/$SHA256/g" "$t" > "$OUT_DIR/$out_name"
  echo "Wrote $OUT_DIR/$out_name"
done

echo "Done. Copy files from $OUT_DIR to winget-pkgs: manifests/d/derekwisong/datui/$VERSION/"
