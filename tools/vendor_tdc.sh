#!/bin/sh
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
# ships in the source tarball. POSIX sh only.

set -eu

VECTRA_ROOT=`cd "\`dirname "$0"\`/.." && pwd`
TDC=${TDC-"$VECTRA_ROOT/../tdc"}

if [ ! -d "$TDC/include/tdc" ] || [ ! -d "$TDC/src" ]; then
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
  if [ -d "$TDC/src/$sub" ]; then
    cp -R "$TDC/src/$sub" "$DEST/src/$sub"
  fi
done

# Stamp the snapshot with the source commit so we know what we vendored.
#
# A dirty source tree is stamped "<hash>-dirty" and warned about, loudly.
# The CI workflows check tdc out at its pushed HEAD and re-vendor from
# there, so anything vendored from uncommitted work exists only on this
# machine: the build goes green locally and then fails to link on CI. A
# bare hash here would be claiming a provenance the bytes do not have.
if command -v git >/dev/null 2>&1 && [ -d "$TDC/.git" ]; then
  head=`cd "$TDC" && git rev-parse HEAD 2>/dev/null` || head=""
  if [ -n "$head" ]; then
    if [ -n "`cd "$TDC" && git status --porcelain 2>/dev/null`" ]; then
      echo "$head-dirty" > "$DEST/VENDORED_FROM"
      echo "vendor_tdc.sh: WARNING - $TDC has uncommitted changes." >&2
      echo "  The vendored snapshot does NOT match any tdc commit. CI" >&2
      echo "  re-vendors from tdc's pushed HEAD, so it will build" >&2
      echo "  something different from what you just built locally." >&2
      echo "  Commit and push tdc, then re-run this script." >&2
    else
      echo "$head" > "$DEST/VENDORED_FROM"
    fi
  fi
fi

# Count vendored files
n_h=`find "$DEST" -name '*.h' | wc -l`
n_c=`find "$DEST" -name '*.c' | wc -l`
echo "vendor_tdc.sh: vendored $n_h headers + $n_c .c files"
echo "vendor_tdc.sh: done. Remember to rebuild vectra (devtools::clean_dll())."
