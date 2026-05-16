## Minor fix release

0.6.3 is a small follow-up to 0.6.2 (accepted on CRAN 2026-05-08). It
fixes a single user-visible bug in `summarise()`: namespace-qualified
aggregation calls of the form `vectra::n()` / `vectra::sum(x)` /
`vectra:::mean(x)` produced "the condition has length > 1" instead of
dispatching to the corresponding aggregation. The parser now strips
the `::` / `:::` qualifier before resolving the function name. Bare
calls (`n()`, `sum(x)`) are unaffected.

This came up in a downstream package that explicitly qualifies vectra
verbs to avoid namespace clashes; the fix lets that pattern work as
users would expect.

A regression test for namespace-qualified calls (ungrouped, grouped,
and the clean error path for an unknown `pkg::nope()`) is in
`tests/testthat/test-groupby.R`.

No other code, documentation, or DESCRIPTION changes since 0.6.2.

## Test environments

* local Windows 11, R 4.6.0 (GCC 14.3.0 via Rtools 46) -- 0/0/0
* GitHub Actions: ubuntu-latest, macos-latest, windows-latest,
  R-devel + R-release
* GitHub Actions: ASAN/UBSAN job on Linux (gcc -fsanitize=address,undefined)

## R CMD check results

0 errors, 0 warnings, 0 NOTEs related to the package.

## Reverse dependencies

vectra has no reverse dependencies on CRAN.
