# Execute a lazy query and return the result

Pulls all batches from the execution plan and materializes the result in
memory. A plain query returns a data.frame. A query whose output still
carries a geometry column, which the spatial verbs
([`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md),
[`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md),
[`spatial_clip()`](https://gillescolling.com/vectra/reference/spatial_clip.md),
[`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md)
and the rest) mark on the node, returns an `sf` object, with the hex-WKB
column decoded and the node's coordinate reference system set. A string
column whose values are hex-encoded WKB is recognised from its content,
so a stored geometry column comes back as `sf` without any spatial verb.
The geometry marker follows the column through
[`filter()`](https://gillescolling.com/vectra/reference/filter.md),
[`select()`](https://gillescolling.com/vectra/reference/select.md),
[`rename()`](https://gillescolling.com/vectra/reference/rename.md),
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) and
joins, and is dropped once the column is.

## Usage

``` r
collect(x, ...)

# S3 method for class 'vectra_node'
collect(x, sf = TRUE, geom = NULL, crs = NULL, ...)
```

## Arguments

- x:

  A `vectra_node` object.

- ...:

  Ignored.

- sf:

  If `TRUE` (default), a spatial result is returned as an `sf` object.
  `FALSE` returns the data.frame with the geometry as a hex-WKB string
  column. Ignored for a non-spatial query.

- geom:

  Name of a hex-WKB geometry column to decode when the node does not
  already carry one, for instance a column read straight from a `.vtr`
  file. `NULL` (default) uses the geometry column the node carries.

- crs:

  Coordinate reference system for the `sf` result. `NULL` (default) uses
  the one the node carries, or leaves it unknown.

## Value

A data.frame, or an `sf` object for a spatial query.

## Details

For a result still larger than RAM, keep it as a node and write it out
with
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md),
stream it to a vector file with
[`sf::st_write()`](https://r-spatial.github.io/sf/reference/st_write.html),
or reduce it with
[`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md).

## See also

[`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md),
[`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md)

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
result <- tbl(f) |> collect()
head(result)
unlink(f)
```
