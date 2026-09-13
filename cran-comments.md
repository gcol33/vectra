## Submission

This release corrects the clang-UBSAN issue shown for vectra 0.12.0 under
'Additional issues', and the clang-san recheck that stopped the 0.12.1
submission on 2026-09-11.

The 0.12.0 issue,

    scan.c:280:20: runtime error: 1e+300 is outside the range of representable
    values of type 'long'

was already fixed in 0.12.1, and the recheck log confirms it no longer occurs.
A double literal outside the int64 range, or NaN, now leaves the sorted-column
search off and the filter decides every row.

The recheck instead reported clang's function sanitizer at every call into the
GEOS C API, for example

    expr_geom.c:85:22: runtime error: call to function GEOSArea_r through
    pointer to incorrect function type 'int (*)(struct GEOSContextHandle_HS *,
    const struct GEOSGeom_t *, double *)'

vectra reaches GEOS through the function pointers the 'libgeos' package
registers with R_RegisterCCallable. They are declared in C, where the opaque
handles are pointers to incomplete structs, while GEOS defines the same
functions in C++, where the handles are pointers to C++ classes. The types are
ABI-identical but carry different names, which is what the check compares. No
C declaration can name the C++ types, so the three files calling GEOS turn off
that one check (`no_sanitize("function")`, clang only); every other sanitizer
check still applies to them. Calls made inside OpenMP parallel regions now go
through named worker functions, because the region bodies clang outlines do not
inherit the attribute.

The same log also lists `NCList.c:1038:2` from IRanges, a suggested package
used by the tests; it is not vectra code.

A new CI job builds 'libgeos' and vectra from source with clang
`-fsanitize=undefined,function`. On 0.12.1 it reproduced the reports above.
It also found three "applying zero offset to null pointer" reports on
all-empty string columns, which are fixed here as well.

The ERROR shown for r-patched-linux-x86_64 is from 0.11.8 and was corrected in
0.12.0.

## Test environments

* Local: Windows 11, R 4.6.1, `R CMD check --as-cran` -- 0 errors | 0 warnings |
  0 notes (3714 test expectations)
* GitHub Actions, Ubuntu 24.04, clang 18 `-fsanitize=undefined,function` with
  'libgeos' built from source under the same flags: full test suite and
  examples, no runtime error reports
* GitHub Actions: R-CMD-check, gcc ASAN/UBSAN -- all OK
* win-builder: R-devel (2026-09-12 r90533 ucrt) -- 0 errors | 0 warnings |
  1 note

## R CMD check results

0 errors | 0 warnings | 1 note

The note is from the incoming feasibility check (days since the last update,
number of updates in the past six months). This update corrects the issue
notified on 2026-09-11, which asked for a fix before 2026-10-02.

## Reverse dependencies

taxify imports vectra. `R CMD check` of taxify 0.5.0 (the CRAN version)
against this release: Status OK, its test suite passes in full (7547
expectations, 0 failures).
