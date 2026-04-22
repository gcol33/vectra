## Resubmission

This is a resubmission of vectra in response to the CRAN team email dated
2026-04-22, which flagged an installation ERROR on
`r-devel-linux-x86_64-fedora-clang` and a gcc-ASAN heap-buffer-overflow under
"Additional issues" (deadline 2026-05-06). Both have been addressed.

* **Fedora clang-22 install ERROR.** R's `Rinternals.h` defines `match` as
  `Rf_match` via the default remapping macros. clang-22's `<omp.h>` uses
  `match` as a pragma-clause token inside
  `#pragma omp declare variant(...) match(...)`. When `<omp.h>` was included
  after an R header, the macro rewrote the pragma and compilation failed in
  `block.c` and `collect.c`. All OpenMP inclusion now goes through
  `src/vec_omp.h`, which `#undef`s `match` across `#include <omp.h>` and
  restores it afterwards. No raw `#include <omp.h>` remains in the package.
  The four affected files (`block.c`, `collect.c`, `expr_string.c`,
  `fuzzy_join.c`) now include `"vec_omp.h"`.

* **gcc-ASAN heap-buffer-overflow.** The LZ fast-path decoder
  (`src/tdc/src/entropy/lz.c`) called `tdc_copy16` (16-byte SIMD load) for
  literal runs of 1–16 bytes without reserving 16 readable bytes on the
  literals source. The last literal run of a compressed block could overread
  `lit_data` by up to 15 bytes into adjacent heap memory. The fast-path bail
  check now requires 16 readable tail bytes on the literals side whenever
  `lit_len` is in the SIMD fast-copy range; shorter tails fall through to the
  memcpy-based safe path. Valgrind did not catch this because the overread
  landed in a still-mapped page; ASAN red-zones caught it cleanly. Fix is
  applied to the vendored `src/tdc/` tree (and upstream).

## Test environments

* local Windows 11, R 4.5.2 (GCC 14.3.0 via Rtools 45)
* win-builder: R-devel, R-release
* mac-builder: R-release
* R-hub: clang-ASAN (the configuration that originally triggered the
  heap-buffer-overflow)

## R CMD check results

0 errors, 0 warnings, 0 NOTEs related to the package.
(One local-environment NOTE "unable to verify current time" appears on this
Windows machine; it does not occur on CRAN's builders. A second local NOTE
"GNU make is a SystemRequirements" reflects the declaration in DESCRIPTION.)
