# Stream a query through an sf transform

Applies a per-feature sf operation (buffer, centroid, area, CRS
transform, simplify, ...) to a lazy vectra query one batch at a time and
returns a new lazy node. The engine pulls one batch, hands it to `fn` as
an `sf` object, encodes the result back into the stream, and spills to
disk, so peak memory is one batch regardless of result size. This is the
streaming, larger-than-RAM counterpart to running the same `sf` call on
a whole in-memory table.

## Usage

``` r
spatial_map(
  x,
  fn,
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

- fn:

  A function (or purrr-style formula such as
  `~ sf::st_buffer(.x, 1000)`) taking one `sf` batch and returning an
  `sf` object, `sfc`, or plain data.frame. The active geometry of the
  return becomes the output geometry.

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
  fewer, bigger temporary files. `NULL` (the default) instead flushes
  once a spill buffer's size crosses the streaming memory budget (a
  fraction of
  [`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md),
  set with `options(vectra.memory = )`); an explicit value caps each
  buffer at that many rows.

## Value

A `vectra_node` backed by temporary `.vtr` spills (removed when the node
is garbage-collected), carrying the output CRS for
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md).

## Details

Geometry travels through the engine as hex-encoded WKB in an ordinary
string column (vectra has no native geometry type), and the coordinate
reference system is carried on the returned node rather than in the
`.vtr` file. Use
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize the result as an `sf` object, or
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) to
get the underlying data.frame with the WKB string column.

Topology is delegated entirely to sf/GEOS; vectra only supplies the
streaming. The `sf` package is an optional dependency (Suggests).

## See also

[`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md)
to join a streamed side against a resident `sf` object,
[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize as `sf`.

## Examples

``` r
nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  NAME = nc$NAME,
  geometry = sf::st_as_binary(sf::st_centroid(sf::st_geometry(nc)),
                              hex = TRUE)
), f)

# Buffer every county centroid by 0.1 degree, streaming.
buffered <- tbl(f) |>
  spatial_map(~ sf::st_buffer(.x, 0.1), crs = sf::st_crs(nc))
collect_sf(buffered)
unlink(f)
```
