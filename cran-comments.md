## Submission

This release corrects the clang-UBSAN issue shown for vectra 0.12.0 under
'Additional issues', notified on 2026-09-11. It replaces 0.12.2, which the
incoming clang-san check stopped on 2026-09-13.

The 0.12.0 issue,

    scan.c:280:20: runtime error: 1e+300 is outside the range of representable
    values of type 'long'

was fixed in 0.12.1: a double literal outside the int64 range, or NaN, now
leaves the sorted-column search off and the filter decides every row.

The 0.12.1 recheck then reported clang's function sanitizer at every call into
the GEOS C API, for example

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
check still applies to them. Calls made inside OpenMP parallel regions go
through named worker functions, because the region bodies clang outlines do not
inherit the attribute. The 0.12.2 clang-san check confirmed this: no report
from vectra code remained.

The one report left on 0.12.2 was

    NCList.c:1038:2: runtime error: call to function NCList_get_y_overlaps
    through pointer to incorrect function type

which is in 'IRanges', reached from a single test that compared
`interval_join()` with `GenomicRanges::findOverlaps()`. That test now compares
against an all-pairs overlap computed in base R, and 'GenomicRanges',
'IRanges' and 'S4Vectors' are no longer in Suggests. Package code is unchanged
from 0.12.2.

A CI job builds 'libgeos' and vectra from source with clang
`-fsanitize=undefined,function`. It reproduced the 0.12.1 reports, and also
found three "applying zero offset to null pointer" reports on all-empty string
columns, which are fixed as well.

The ERROR shown for r-patched-linux-x86_64 is from 0.11.8 and was corrected in
0.12.0.

## Test environments

* Local: Windows 11, R 4.6.1, `R CMD check --as-cran` -- 0 errors | 0 warnings |
  0 notes (3717 test expectations)
* GitHub Actions, Ubuntu 24.04, clang 18 `-fsanitize=undefined,function` with
  'libgeos' built from source under the same flags: full test suite and
  examples, no runtime error reports
* GitHub Actions: R-CMD-check, gcc ASAN/UBSAN -- all OK
* win-builder: R-devel

## R CMD check results

0 errors | 0 warnings | 1 note

The note is from the incoming feasibility check (days since the last update,
number of updates in the past six months). This update corrects the issue
notified on 2026-09-11, which asked for a fix before 2026-10-02.

## Reverse dependencies

taxify imports vectra. `R CMD check` of taxify 0.5.0 (the CRAN version)
against 0.12.2, whose package code this release shares: Status OK, its test
suite passes in full (7547 expectations, 0 failures).
