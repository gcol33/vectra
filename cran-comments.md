## Submission

This release corrects the clang-UBSAN issue shown for vectra 0.12.0 under
'Additional issues':

    scan.c:280:20: runtime error: 1e+300 is outside the range of representable
    values of type 'long'

When a filter compares an integer column with a double literal, the scan
converts the literal to a 64-bit integer to search sorted row groups, and did
so also when the value lay outside the int64 range or was NaN. Such a literal
now leaves the search off and the filter decides every row. The
composite-index probe goes through the same range check the single-column probe
already used. On x86-64 the conversion also gave wrong results:
`filter(k < 1e300)` on a sorted integer column returned no rows. New tests
check these comparisons against base R subsetting.

The ERRORs shown for r-patched-linux-x86_64 are from 0.11.8 and were corrected
in 0.12.0.

## Test environments

* Local: Windows 11, R 4.6.0, `R CMD check --as-cran` -- 0 errors | 0 warnings |
  1 note
* Local: Windows 11, R 4.6.0, built with gcc
  `-fsanitize=float-cast-overflow -fsanitize-undefined-trap-on-error`: the full
  test suite passes (3716 expectations); the same build of 0.12.0 traps in
  `tests/testthat/test-index.R`.

## R CMD check results

0 errors | 0 warnings | 1 note

The note is from the incoming feasibility check and reports two days since the
last update and ten updates in the past six months. This update is the
correction of the issue notified on 2026-09-11, which asked for a fix before
2026-10-02.

## Reverse dependencies

taxify imports vectra. Its test suite passes in full against this fix (taxify
0.5.2: 7577 tests, 0 failures).
