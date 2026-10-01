# Materialize a spatial query as an sf object (deprecated)

[`collect()`](https://gillescolling.com/vectra/reference/collect.md) is
deprecated:
[`collect()`](https://gillescolling.com/vectra/reference/collect.md)
returns an `sf` object for any query that carries a geometry column.
This wrapper remains for existing code and, unlike
[`collect()`](https://gillescolling.com/vectra/reference/collect.md),
also accepts a data.frame that was already collected.

## Usage

``` r
collect_sf(x, geom = "geometry", crs = NULL)
```

## Arguments

- x:

  A `vectra_node` with a hex-WKB geometry column, or a data.frame
  already collected from one.

- geom:

  Name of the geometry column. Default `"geometry"`.

- crs:

  Override the coordinate reference system. Defaults to the CRS the node
  carries, or unknown.

## Value

An `sf` object.

## See also

[`collect()`](https://gillescolling.com/vectra/reference/collect.md).
