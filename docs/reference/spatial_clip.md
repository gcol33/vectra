# Clip or erase a streamed layer against a resident mask

Streams a large layer `x` through the engine and cuts each batch's
geometry against a small resident `mask` (a study boundary, a buffer, a
set of patches). By default this clips – the intersection with the mask,
the GIS "Clip" tool – keeping only the parts of `x` that fall inside
`mask`. With `erase = TRUE` it instead erases – the difference, the
"Erase"/"Difference" tool – keeping the parts of `x` outside `mask`. The
mask is dissolved to a single geometry once and held resident while the
billion-row left stream flows past one batch at a time.

## Usage

``` r
spatial_clip(
  x,
  mask,
  erase = FALSE,
  geom = "geometry",
  coords = NULL,
  crs = NA,
  out_geom = NULL,
  flush_rows = NULL
)
```

## Arguments

- x:

  A `vectra_node` (from
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
  [`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md),
  any verb chain, ...). It is consumed by the stream.

- mask:

  An `sf` or `sfc` object whose dissolved geometry clips (or, with
  `erase = TRUE`, erases) the stream.

- erase:

  If `TRUE`, keep the parts of `x` *outside* `mask` (difference) rather
  than inside (intersection). Default `FALSE`.

- geom:

  Name of the input geometry column holding hex-WKB or WKT strings.
  Default `"geometry"`. Ignored when `coords` is given.

- coords:

  Optional length-2 character vector naming the x and y coordinate
  columns to assemble point geometry from (e.g. `c("x", "y")`), for
  inputs such as
  [`tiff_extract_points()`](https://gillescolling.com/vectra/reference/tiff_extract_points.md)
  output. The coordinate columns are retained.

- crs:

  Coordinate reference system of the input geometry, in any form
  [`sf::st_crs()`](https://r-spatial.github.io/sf/reference/st_crs.html)
  accepts (EPSG integer, WKT, proj string). Defaults to the CRS the
  upstream node carries, or unknown.

- out_geom:

  Name of the output geometry column. Defaults to `geom` (or
  `"geometry"` when `coords` is used).

- flush_rows:

  Transformed rows buffered before a spill flush. Larger values mean
  fewer, bigger temporary files. Defaults to
  `getOption("vectra.spatial_flush", 5e5)`.

## Value

A `vectra_node` of the cut geometry with `x`'s attributes, backed by
temporary `.vtr` spills and carrying the input CRS.

## Details

Geometry travels through the engine as hex-encoded WKB in a string
column and the CRS is carried on the returned node; use
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize. On projected or unprojected planar data the cut runs
natively on the GEOS C API straight off the hex-WKB column (the mask
parsed once); geographic coordinates with spherical geometry on
([`sf::sf_use_s2()`](https://r-spatial.github.io/sf/reference/s2.html))
and coordinate-assembled (`coords`) input cut through sf instead. When
`mask` carries no CRS it inherits the stream's.

## See also

[`spatial_filter()`](https://gillescolling.com/vectra/reference/spatial_filter.md)
to keep whole features by location without cutting them,
[`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md)
for per-feature transforms,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md).

## Examples

``` r
nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
mask <- sf::st_union(nc[nc$NAME %in% c("Ashe", "Alleghany"), ])

f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  NAME = nc$NAME,
  geometry = sf::st_as_binary(sf::st_geometry(nc), hex = TRUE)
), f)

# Clip every county polygon to the two-county mask, streaming.
clipped <- tbl(f) |> spatial_clip(mask, crs = sf::st_crs(nc))
collect_sf(clipped)
unlink(f)
```
