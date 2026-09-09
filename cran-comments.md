## Submission

This release corrects the ERRORs currently shown for vectra on the Debian
flavors, in examples, tests and vignette rebuild:

    Error in st_line_merge.sfc(sf::st_union(geo[ix])) :
      inherits(x, "sfc_MULTILINESTRING") is not TRUE

`sf::st_line_merge()` accepts only a MULTILINESTRING, but the union of a set of
lines carries that type only while it has more than one part. A group of one
segment, or of segments whose union already collapses into a single chain,
yields a LINESTRING and trips the assertion. The three functions that merge
linework (`contours(merge = TRUE)`, `spatial_line_merge()`,
`spatial_centerline()`) now dispatch on the type the union actually returned: a
LINESTRING is already maximal and passes through, a GEOMETRYCOLLECTION has its
linear parts extracted and recombined, and a MULTILINESTRING merges as before,
so results are unchanged wherever the previous path worked.

The failure is not specific to one GEOS build. It is reachable on GEOS 3.14.1
with a single segment in a group, and through s2 on geographic coordinates; the
system upgrade on the check machines appears to have made the collapsing case
common enough to reach the examples and tests.

Version 0.11.9 was prepared but never submitted, so this release carries its
fixes as well. One of them affects results on ordinary input:

* `filter()` drops rows when a `%in%` predicate runs against an indexed column.
  The scan probed the `.vtri` sidecar with whichever representation the
  predicate happened to carry and contributed nothing when none matched the
  column's own type, so the row-group bitmap came back empty and every row group
  was pruned. `filter(k %in% c(5, 9))` on an integer column returns zero rows
  where the same data without an index returns all of them, with no error or
  warning. Since R writes a bare numeric literal as a double whatever the column
  holds, that is the ordinary way of writing the predicate rather than a corner
  case; the same went for a logical column and for an `NA` in the set. Every set
  element is now probed by the column's type, and a key that cannot be probed
  leaves the scan unpruned rather than being passed over. The test file answers
  21 predicate shapes against both an indexed store and a plain copy of the same
  data and requires the two to agree.

The remaining 0.11.9 fixes are to memory and cost:

* Reading a `.vtri` index larger than 2 GB raised `corrupt .vtri: entry/slot
  counts exceed file size` from `tbl()` on an intact index, leaving no way to
  open the store at all, not even by falling back to a scan. `ftell()` into a
  `long` is 32 bits on Windows and gave a meaningless size; offsets now go
  through the 64-bit calls used elsewhere in the package. Independently, every
  way of failing to read a sidecar now reports no index rather than raising. An
  index only ever saves a scan work, so an unusable one costs speed and never
  rows.

* `append_vtr(along = "rows")` restreamed every existing row group through a
  fresh writer on each call, so building a store by repeated appends was
  quadratic in the number of calls and degraded invisibly as the store grew. New
  rows are now written past the container trailer with the header patched last,
  which also leaves an interrupted append readable exactly as it was.

* Building a `.vtri` held the whole index in memory, so an index too large for
  memory could be read but not made. Entries are sorted rather than chained now
  and written in a single forward pass against the same streaming budget the
  rest of the package spills against. Opening a large sidecar no longer copies
  it: past 4 MB it is mapped read-only and probed in place.

`NEWS.md` lists the full set.

Two format notes. `.vtri` sidecars written by earlier versions read as absent,
so queries and `has_index()` behave as though the store has no index until
`create_index()` is called again. A store grown in place by `append_vtr()`
carries a stamp that readers predating this format refuse rather than misread.
The `.vtr` data format is otherwise unchanged, and there are no breaking API
changes.

## Test environments

* Local: Windows 11, R 4.6.0, `R CMD check --as-cran` -- 0 errors | 0 warnings |
  0 notes
* win-builder: R-devel (2026-09-08 r90509 ucrt) -- 0 errors | 0 warnings |
  1 note
* GitHub Actions: ubuntu-latest (R-devel, R-release, R-oldrel-1),
  macOS-release, windows-release -- all OK; ASAN/UBSAN clean

The OpenMP team size is capped at two cores when `_R_CHECK_LIMIT_CORES_` is set
(`R_init_vectra`), so the parallel string, fuzzy-join, and spatial kernels stay
within the check farm's two-core limit.

## R CMD check results

0 errors | 0 warnings | 1 note

The note is from the incoming feasibility check and reports nine updates in
the past six months. Most of those are the memory-boundedness work the
package has been through since spring; this one is a correction of the check
ERRORs notified on 2026-09-08, which asked for a fix before 2026-09-29.

## Reverse dependencies

vectra has no reverse dependencies on CRAN.
