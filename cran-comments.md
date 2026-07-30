## Submission

This is a bug-fix update to 0.11.3, the version currently on CRAN. Versions
0.11.4 through 0.11.7 were developed but never submitted, so this release carries
their fixes as well.

The published version has the following defects on ordinary input, all corrected
here:

* `roll_min()` / `roll_max()` corrupt the heap on a long partition with a short
  time window: the monotonic-deque index could run past its buffer.
* `mutate()` with a unary math function (`sqrt()`, `abs()`, `log()`, ...) on a
  `double` column leaks memory on every batch, which can exhaust memory during a
  large streamed `collect()`.
* Row-group pruning drops rows that match the filter. A fractional threshold on a
  sorted integer column (`filter(x < 2.9)`), an interior all-`NaN` row group, and
  a quantized column each return a subset with no error or warning.
* Reading a `BLOB` column or a `TEXT` value larger than 64 KB from a SQLite
  database reads past the reader's buffer; writing a row larger than a page
  overflows the page buffer.
* A hash join can duplicate rows or loop indefinitely on a many-to-many key whose
  build-chain length coincides with the internal emit cap.
* A `.vtri` index left behind by `append_vtr()` prunes row groups that hold
  matching rows, so a filter on the indexed column silently returns a subset.

The release also brings several results into line with base R and dplyr
(`grepl()` / `gsub()` / `sub()` treat the pattern as a regular expression by
default, `round()` rounds halves to even, `arrange(desc(x))` places `NA` last),
and adds `dim()` / `nrow()` / `ncol()` for a lazy query and a column-wise
`append_vtr(along = "cols")`. `NEWS.md` lists the full set.

On update frequency: the policy suggests no more than every 1-2 months for an
established package, and 0.11.3 was published on 2026-07-17. I judged that
holding memory-safety and silent-wrong-answer defects in a larger-than-RAM query
engine for a month was worse than submitting inside that window. There are no
breaking API changes. The one breaking change is that `.vtri` index sidecars
written by earlier versions now read as absent, so queries and `has_index()`
behave as though the store has no index until `create_index()` is called again;
the `.vtr` data format itself is unchanged.

## Test environments

* Local: Windows 11, R 4.6.0, `R CMD check --as-cran` -- 0 errors | 0 warnings |
  1 NOTE (incoming feasibility: number of updates in past 6 months)
* win-builder: R-devel (2026-07-29 r90317 ucrt) -- 0 errors | 0 warnings |
  1 NOTE (the same incoming-feasibility NOTE)
* GitHub Actions: ubuntu-latest (R-devel, R-release, R-oldrel-1),
  macOS-release, windows-release -- all OK; ASAN/UBSAN clean

The OpenMP team size is capped at two cores when `_R_CHECK_LIMIT_CORES_` is set
(`R_init_vectra`), so the parallel string, fuzzy-join, and spatial kernels stay
within the check farm's two-core limit.

## R CMD check results

0 errors | 0 warnings | 1 NOTE.

The NOTE is the incoming-feasibility one reporting the number of updates in the
past six months; it is addressed above.

## Reverse dependencies

vectra has no reverse dependencies on CRAN.
