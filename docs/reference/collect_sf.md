# Materialize a spatial query as an sf object

Collects a `vectra_node` (typically the result of
[`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md)
or
[`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md))
into memory and rebuilds an `sf` object from its hex-WKB geometry
column. The CRS defaults to the one carried on the node.

## Usage

``` r
collect_sf(x, geom = "geometry", crs = NULL)
```

## Arguments

- x:

  A `vectra_node` with a hex-WKB / WKT geometry column, or a data.frame
  already collected from one.

- geom:

  Name of the geometry column. Default `"geometry"`.

- crs:

  Override the coordinate reference system. Defaults to the CRS the node
  carries, or unknown.

## Value

An `sf` object.

## Details

This is the spatial counterpart to
[`collect()`](https://gillescolling.com/vectra/reference/collect.md):
use it when the final result fits in memory as `sf`. For a result still
larger than RAM, keep it as a node and write it out with
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
(the geometry stays as a WKB string column) or reduce it with
[`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md).

## See also

[`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md),
[`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md),
[`collect()`](https://gillescolling.com/vectra/reference/collect.md).

## Examples

``` r
nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  NAME = nc$NAME,
  geometry = sf::st_as_binary(sf::st_geometry(nc), hex = TRUE)
), f)
result <- tbl(f) |> spatial_map(~ sf::st_centroid(.x), crs = sf::st_crs(nc))
collect_sf(result)
unlink(f)
```
