## Submission

This release fixes the UndefinedBehaviorSanitizer report on the M1 sanitizer
check (M1-SAN), flagged on the CRAN check page with a request to correct before
2026-07-14.

Cause: the bulk-copy fast path in `df_to_batch()` (src/r_bridge_core.c) called
`memcpy()` over a double column unconditionally. When the written data frame
has zero rows, `REAL()` hands back a degenerate (zero-length) pointer; passing
it to `memcpy()` had clang's alignment sanitizer report a misaligned load of a
`double *`. The copy is now skipped when the column has no rows, so the
degenerate pointer is never accessed. This was a regression introduced when the
per-element copy loop was replaced by a single `memcpy()`; the old loop simply
did not execute for an empty column.

This version also adds new features (embedding columns and distance functions,
time-series resampling, time-based rolling aggregates, and interval joins); see
NEWS.md.

## Test environments

* Local: Windows 11, R 4.6.0 -- 0 errors | 0 warnings | 0 notes
* win-builder: R-devel (ucrt)
* GitHub Actions: ubuntu-latest (R-devel, R-release, R-oldrel-1),
  macOS-release, windows-release

## R CMD check results

0 errors | 0 warnings | 0 notes expected. win-builder may report:

    Days since last update: N

if the previous version was published recently; this resubmission corrects the
M1-SAN sanitizer report.

## Reverse dependencies

vectra has no reverse dependencies on CRAN.
