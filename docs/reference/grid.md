# Define a uniform grid for a partitioned spatial join

Describes the regular grid that
[`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md)
uses to partition two streamed layers for the both-sides-larger-than-RAM
case. Cell `(cx, cy)` covers
`[origin_x + cx * cellsize_x, origin_x + (cx + 1) * cellsize_x)` and
likewise in y, so a coordinate maps to the cell
`floor((coord - origin) / cellsize)`. Pick a `cellsize` comparable to
the scale of the join (large enough that most cells hold a workable
shard, small enough that one cell's features fit in memory); for an
extended-on-extended join choose it larger than the left features.

## Usage

``` r
grid(cellsize, origin = c(0, 0))
```

## Arguments

- cellsize:

  Cell size: a single number for square cells, or
  `c(cellsize_x, cellsize_y)`.

- origin:

  Grid origin `c(x0, y0)` (a cell corner). Default `c(0, 0)`.

## Value

A `vectra_grid` specification to pass as `spatial_join(partition =)`.

## See also

[`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md)
for the join it partitions.

## Examples

``` r
grid(1000)
grid(c(0.5, 0.25), origin = c(-180, -90))
```
