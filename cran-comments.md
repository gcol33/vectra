## Feature release

0.8.2 is a feature update to the version currently published on CRAN
(0.7.1). It adds streamed spatial operations and group-aware row slicing
on top of the existing columnar engine, with no breaking changes to the
public verbs or the `.vtr` / `.vec` on-disk formats. It also fixes an
`ifelse()` result-type bug for branches of differing type.

New user-visible functions:

* `spatial_map(x, fn)` streams a lazy query through an `sf` transform
  one batch at a time, so a per-feature geometry operation runs on a
  table larger than RAM at one-batch peak memory.
* `spatial_join(x, y, join)` joins a streamed left side against a small
  resident `sf` object with an `sf` binary predicate (the spatial
  analogue of a hash join with the small side resident).
* `spatial_overlay(x)` splits a polygon layer along its own overlaps into
  disjoint pieces (single-layer union overlay), streamed to a `.vtr`.
* `collect_sf(x)` materializes a spatial query as an `sf` object.

Geometry rides through the engine as hex-encoded WKB in an ordinary
string column (no new column type); topology stays with `sf`/GEOS. `sf`
is added to Suggests and is used only in examples and tests, all guarded
by `requireNamespace()`.

This release also makes `slice_min()` and `slice_max()` respect
`group_by()` (top-n within each group, keeping the whole winning row)
and gives `row_number()` an optional ordering column. No new dependency.

## Test environments

* local Windows 11, R 4.6.0 (GCC 14.3.0 via Rtools 46) -- 0/0/0
* win-builder, R-devel and R-release (x86_64) -- Status: OK
* GitHub Actions: macOS, Windows, ubuntu-latest (R-devel, R-release,
  R-oldrel-1)
* GitHub Actions: ASAN/UBSAN job on Linux (gcc -fsanitize=address,undefined)

## R CMD check results

0 errors, 0 warnings, 0 notes.

## Reverse dependencies

vectra has no reverse dependencies on CRAN.
