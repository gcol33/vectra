## Submission

This is an update to the version currently on CRAN (0.9.8).

The headline fix (issue #5): a lazy query is now consumed by exactly one
terminal operation. `collect()` and `append_vtr()` join the `write_*()` verbs
in invalidating a node once its pull cursor is drained, so a second terminal op
on the same node object raises a clear "already consumed" error instead of
re-driving an exhausted plan. Previously a `collect()` followed by a
`write_vtr()` on the same node returned empty output or, on a multi-row-group
plan, silently reinterpreted a string column's bytes as doubles. `vec_builder_*`
also now errors on a type-mismatched array rather than reinterpreting raw bytes.

This release additionally folds in the feature work from the 0.10.x and 0.11.0
development line (bounded-memory joins, sort, grouped and holistic aggregates,
top-N, fuzzy and interval joins; a k-mer spectrum node; FASTA/BED scan
backends; feature-space kNN and the MOP transferability surface; and a `delim`
argument to `tbl_csv()`); see NEWS.md.

## Test environments

* Local: Windows 11, R 4.6.0 -- 0 errors | 0 warnings | 0 notes
* win-builder: R-devel (ucrt)
* GitHub Actions: ubuntu-latest (R-devel, R-release, R-oldrel-1),
  macOS-release, windows-release

The OpenMP team size is capped at two cores when `_R_CHECK_LIMIT_CORES_` is set
(R_init_vectra), so the parallel string, fuzzy-join, and spatial kernels stay
within the check farm's two-core limit.

## R CMD check results

0 errors | 0 warnings | 0 notes expected.

## Reverse dependencies

vectra has no reverse dependencies on CRAN.
