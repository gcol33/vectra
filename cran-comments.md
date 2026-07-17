## Submission

This is a bug-fix hotfix for the version currently on CRAN (0.11.1).

It corrects five defects found in an audit of the package's bounded-memory and
consume-once invariants:

* `collect_chunked()` / `chunk_feeder()` now consume their input node like the
  other terminals, so re-collecting a drained streaming cursor raises the
  documented "already consumed" error instead of re-driving an exhausted plan
  and returning wrong data.
* the external record merge behind `median()`, `n_distinct()` and `kmer()` now
  reduces its spilled runs to a bounded fan-in before the final merge (as the
  row sort already did), so a larger-than-RAM aggregate no longer opens every
  run at once and cannot exhaust the file-handle table.
* `propagate()` runs to convergence instead of a fixed 20 iterations.
* `resolve()` / `propagate()` coerce their key columns to a common type before
  matching, so mismatched numeric key types no longer silently miss.
* `lookup(.report = TRUE)` streams its diagnostic counts instead of collecting
  the whole fact table.

It also folds in the streaming gzip reader from the unreleased 0.11.2 (a `.gz`
larger than RAM, and past 2 GB compressed, now reads).

The incoming-feasibility NOTE flags the maintainer update count. These are
memory-safety and silent-wrong-answer fixes to a larger-than-RAM engine, hence
the quick turnaround after 0.11.1; there are no user-facing API changes.

## Test environments

* Local: Windows 11, R 4.6.0 -- 0 errors | 0 warnings | 0 notes
* win-builder: R-devel (ucrt) -- 0 errors | 0 warnings | 1 NOTE (incoming
  feasibility: maintainer update count)
* GitHub Actions: ubuntu-latest (R-devel, R-release, R-oldrel-1),
  macOS-release, windows-release -- all OK; ASAN/UBSAN clean

The OpenMP team size is capped at two cores when `_R_CHECK_LIMIT_CORES_` is set
(R_init_vectra), so the parallel string, fuzzy-join, and spatial kernels stay
within the check farm's two-core limit.

## R CMD check results

0 errors | 0 warnings | 1 NOTE (incoming feasibility only).

## Reverse dependencies

vectra has no reverse dependencies on CRAN.
