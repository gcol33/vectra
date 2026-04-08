#!/usr/bin/env bash
# vendor_tdc.sh — sync tdc into vectra/src/tdc/
#
# tdc is the source of truth in its own repo. Vectra vendors a snapshot
# of tdc/include/ and tdc/src/ into vectra/src/tdc/ so that CRAN tarballs
# stay self-contained. Run this manually before release commits — NOT at
# install time.
#
# Usage:
#   tools/vendor_tdc.sh                # default tdc path
#   TDC=/path/to/tdc tools/vendor_tdc.sh
#
# This script lives under tools/ which is .Rbuildignore'd, so it never
# ships in the source tarball.

set -euo pipefail

VECTRA_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TDC="${TDC:-$VECTRA_ROOT/../tdc}"

if [[ ! -d "$TDC/include/tdc" || ! -d "$TDC/src" ]]; then
  echo "vendor_tdc.sh: tdc tree not found at $TDC" >&2
  echo "  set TDC=/path/to/tdc to override" >&2
  exit 1
fi

DEST="$VECTRA_ROOT/src/tdc"

echo "vendor_tdc.sh: vendoring $TDC -> $DEST"

rm -rf "$DEST"
mkdir -p "$DEST/include" "$DEST/src"

# Public headers
cp -R "$TDC/include/tdc"  "$DEST/include/tdc"
cp    "$TDC/include/tdc.h" "$DEST/include/tdc.h"

# Source tree (api/, core/, entropy/, format/, layout/, model/, symbols/,
# transform/) — copy whole subdirs so internal headers come along.
for sub in api core entropy format layout model symbols transform; do
  if [[ -d "$TDC/src/$sub" ]]; then
    cp -R "$TDC/src/$sub" "$DEST/src/$sub"
  fi
done

# Stamp the snapshot with the source commit so we know what we vendored.
if command -v git >/dev/null 2>&1 && [[ -d "$TDC/.git" ]]; then
  ( cd "$TDC" && git rev-parse HEAD ) > "$DEST/VENDORED_FROM" 2>/dev/null || true
fi

# Count vendored files
n_h=$(find "$DEST" -name '*.h' | wc -l)
n_c=$(find "$DEST" -name '*.c' | wc -l)
echo "vendor_tdc.sh: vendored $n_h headers + $n_c .c files"
echo "vendor_tdc.sh: done. Remember to rebuild vectra (devtools::clean_dll())."
