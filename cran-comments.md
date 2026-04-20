## Resubmission

This is a resubmission of vectra after the 0.5.0 incoming pretest flagged
three items (2026-04-18 Debian flavor, r-devel-linux-x86_64-debian-gcc).
All three have been addressed:

* `src/window.c`: removed the `-Wunneeded-internal-declaration` warning by
  guarding the OpenMP task-parallel merge-sort helper (`win_merge_sort_par`)
  with `#ifdef _OPENMP`, matching its call sites. Local clang build with
  `-Wunneeded-internal-declaration -Werror` and no OpenMP is now clean.

* `configure` / `configure.win`: rewritten as POSIX `/bin/sh` (previously
  `#!/usr/bin/env bash` with `set -o pipefail` and `[[ ... ]]`). No bash-isms
  remain, so the "`env bash` is not portable" NOTE no longer applies.

* Compiled-code `stderr` reference: the vendored tdc codec had debug/timing
  `fprintf(stderr, ...)` calls in six translation units (all guarded by
  runtime flags, never reached in normal use). These are now routed through
  a `TDC_LOG(...)` macro that is a no-op unless `TDC_ENABLE_STDERR_LOG` is
  defined at build time. `#include <stdio.h>` was removed from files that no
  longer needed it. `nm` on the built `.so` confirms no `fprintf` / `stderr`
  symbols in `tdc/src/api/decode_impl.o`, `tdc/src/api/encode.o`,
  `tdc/src/core/decode_profile.o`, `tdc/src/entropy/lz_opt.o`,
  `tdc/src/entropy/lz_streams.o`, or `tdc/src/model/plane2d.o`.

No other user-visible changes since 0.5.0 apart from the `collect()`
use-after-free fix described in NEWS.md.

## Test environments

* local Windows 11, R 4.5.2 (GCC 14.3.0 via Rtools 45)

## R CMD check results

0 errors, 0 warnings, 0 NOTEs related to the package.
(One local-environment NOTE "unable to verify current time" appears on this
Windows machine; it does not occur on CRAN's builders.)
