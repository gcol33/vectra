# Stream a vectra node's geometry to a vector file

An
[`sf::st_write()`](https://r-spatial.github.io/sf/reference/st_write.html)
method (also reached through
[`sf::write_sf()`](https://r-spatial.github.io/sf/reference/st_write.html))
for a `vectra_node`: writes the result a batch at a time, appending
each, so the whole layer is never held in memory. This is the streaming
counterpart to `collect_sf(x) |> sf::st_write(...)` – that route
materializes every feature as an `sf` object first, which for a
multi-million-feature result dominates memory; this route's peak is one
batch.

## Usage

``` r
# S3 method for class 'vectra_node'
st_write(
  obj,
  dsn,
  layer = NULL,
  ...,
  geom = "geometry",
  crs = NULL,
  delete_dsn = FALSE,
  quiet = TRUE
)
```

## Arguments

- obj:

  A `vectra_node` whose rows carry a hex-WKB geometry column (from
  [`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md),
  a grouped
  [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)
  /
  [`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
  resolution, a `.vtr` scan, ...). It is consumed by the stream.

- dsn:

  Destination data source name (file path).

- layer:

  Layer name. `NULL` lets sf derive it from `dsn`.

- ...:

  Unused; for S3 generic compatibility.

- geom:

  Name of the hex-WKB geometry column. Default `"geometry"`.

- crs:

  CRS to tag the output with. `NULL` takes the CRS carried on the node.

- delete_dsn:

  If `TRUE`, remove an existing `dsn` before writing.

- quiet:

  Passed to
  [`sf::st_write()`](https://r-spatial.github.io/sf/reference/st_write.html).

## Value

The `dsn`, invisibly.

## See also

[`collect_sf()`](https://gillescolling.com/vectra/reference/collect_sf.md)
to materialize the whole result as one `sf` object.
