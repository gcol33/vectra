## Submission

This update corrects two bugs reported after 0.12.3 was published on
2026-09-14.

* `filter()` on a store with a '.vtri' index could return too few rows. An
  index checked only the row count and row-group count of the store it was
  built on, so after `write_vtr()` replaced the store with another of the same
  shape, a filter pruned row groups by the old store's keys and silently
  dropped matching rows. An index now also records a fingerprint of the store
  and is ignored when it no longer matches, and `write_vtr()` removes the
  indexes of the store it replaces.

* A source install failed under a non-UTF-8 locale. `R/verbs.R` started with a
  UTF-8 byte-order mark, which the parser reads as a token under
  `LC_CTYPE=C`, so `R CMD INSTALL` stopped with "unable to collate and parse R
  files". The mark is removed, and CI now parses every R file in a C locale.

## Test environments

* Local: Windows 11, R 4.6.1, `R CMD check --as-cran` on the built tarball --
  0 errors | 0 warnings | 1 note (3778 test expectations)
* Local: Windows 11, R 4.6.1, `R CMD INSTALL` from source with `LC_ALL=C`
* GitHub Actions: R-CMD-check (macOS, Windows, Ubuntu release/devel/oldrel-1),
  gcc ASAN/UBSAN, clang `-fsanitize=undefined,function`

## R CMD check results

0 errors | 0 warnings | 1 note

The note is from the incoming feasibility check (days since the last update).
This update fixes a filter that returns wrong results without an error, and an
install failure that also affects the reverse dependency 'taxify'.

## Reverse dependencies

taxify imports vectra. `R CMD check` of taxify 0.5.0 (the CRAN version)
against this release: Status OK, its test suite passes in full (7547
expectations, 0 failures).
