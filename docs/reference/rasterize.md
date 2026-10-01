# Rasterize a streamed point layer onto a fixed grid

Folds a larger-than-RAM stream of points into a fixed raster grid one
batch at a time. The grid (`template`) is held resident in memory while
the points flow past the engine, so peak memory is the grid plus one
batch regardless of how many points there are – the streaming
counterpart to running
[`terra::rasterize()`](https://rspatial.github.io/terra/reference/rasterize.html)
on a point set that has to fit in RAM. Each point's coordinate is mapped
to its grid cell through the raster geotransform and the per-cell value
is accumulated in C.

## Usage

``` r
rasterize(
  x,
  template = NULL,
  field = NULL,
  fun = c("count", "sum", "mean", "min", "max"),
  extent = NULL,
  res = NULL,
  dims = NULL,
  coords = c("x", "y"),
  geom = NULL,
  crs = NA,
  background = NA_real_,
  path = NULL,
  dtype = "f32"
)
```

## Arguments

- x:

  A `vectra_node` streaming the points (from
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
  [`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md),
  any verb chain). It is consumed by the stream.

- template:

  Optional grid to borrow geometry and CRS from: a `vectra_raster` (from
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md)),
  or a numeric `c(xmin, ymin, xmax, ymax)` extent. When omitted, supply
  `extent` with `res` or `dims`.

- field:

  Name of a numeric column to aggregate. Required for every `fun` except
  `"count"` (which ignores it).

- fun:

  Reduction over the points in each cell: one of `"count"`, `"sum"`,
  `"mean"`, `"min"`, `"max"`. `NA` values in `field` are skipped.

- extent:

  Numeric `c(xmin, ymin, xmax, ymax)` defining the grid extent when no
  `template` is given.

- res:

  Cell size: a single number for square cells, or `c(xres, yres)`. The
  cell counts are rounded to fit `extent` exactly. Supply `res` or
  `dims`.

- dims:

  Grid shape `c(nrow, ncol)`, an alternative to `res`.

- coords:

  Length-2 character vector naming the x and y coordinate columns.
  Default `c("x", "y")`. Ignored when `geom` is supplied.

- geom:

  Name of a hex-WKB point-geometry column to rasterize instead of
  coordinate columns. Requires sf.

- crs:

  Coordinate reference system recorded on the output, in any form
  [`sf::st_crs()`](https://r-spatial.github.io/sf/reference/st_crs.html)
  accepts or a bare EPSG integer. Defaults to the template's, then the
  node's, else unknown.

- background:

  Value for cells that receive no point. Default `NA_real_`.

- path:

  Optional output path. When given, the grid is written to a `.vec`
  raster via
  [`vec_write_raster()`](https://gillescolling.com/vectra/reference/vec_write_raster.md)
  and the opened
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md)
  handle is returned invisibly. When `NULL`, the grid is returned in
  memory.

- dtype:

  Storage dtype for the `.vec` output (see
  [`vec_write_raster()`](https://gillescolling.com/vectra/reference/vec_write_raster.md)).
  Default `"f32"`.

## Value

When `path` is `NULL`, a numeric matrix with `nrow` grid rows (row 1
northmost) and `ncol` grid columns, carrying `gt`, `extent`, `res`,
`crs`, and `fun` attributes. When `path` is given, the written
`vectra_raster` handle (invisibly).

## Details

The reduction `fun` is a monoid over the points falling in each cell:
`"count"` tallies points (no `field` needed); `"sum"`, `"mean"`,
`"min"`, `"max"` aggregate a numeric `field`. Cells that receive no
point take the `background` value (`NA` by default). This is the *monoid
fold* tier of the spatial toolbox: bounded memory, a single streaming
pass, no spill.

Points arrive either as two numeric coordinate columns (`coords`, the
default and fully sf-free path – the headline larger-than-RAM case) or
decoded from a hex-WKB point-geometry column (`geom`, which needs sf).
Geometry input is expected to be points (one coordinate per row); line
and polygon coverage rasterization is out of scope here.

## See also

[`vec_write_raster()`](https://gillescolling.com/vectra/reference/vec_write_raster.md)
and
[`vec_to_tiff()`](https://gillescolling.com/vectra/reference/vec_to_tiff.md)
for raster output,
[`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md)
to instead tag points with polygon attributes.

## Examples

``` r
set.seed(1)
n <- 1e4
pts <- data.frame(x = runif(n, 0, 10), y = runif(n, 0, 10), z = rnorm(n))
f <- tempfile(fileext = ".vtr")
write_vtr(pts, f)

# Point density on a 10x10 grid, streamed: the grid is resident, the
# points are not.
counts <- tbl(f) |> rasterize(extent = c(0, 0, 10, 10), dims = c(10, 10))
counts
#>       [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10]
#>  [1,]  118  102   98  116  102   90  102   95  113    96
#>  [2,]   99   78   95  102   90  104   89   91  114   108
#>  [3,]   97  109  114   99   94   95   88   94  100    93
#>  [4,]   92  102   95  107  101  108   97  102  119   108
#>  [5,]  115   90  103   97  105   93   90   92   97   108
#>  [6,]   91   82  105  115  105  101   93  101   90   124
#>  [7,]   95   97  102   80  115  101   95   84   97   104
#>  [8,]  118   94   95   96  118   97   92  105  114   100
#>  [9,]  103  111   92   93  104   88   93   99   93   106
#> [10,]  107  112  109  106   82  110   91  105   87    97
#> attr(,"gt")
#> [1]  0  1  0 10  0 -1
#> attr(,"extent")
#> [1]  0  0 10 10
#> attr(,"res")
#> [1] 1 1
#> attr(,"crs")
#> [1] NA
#> attr(,"fun")
#> [1] "count"

# Mean of z per cell.
zmean <- tbl(f) |>
  rasterize(extent = c(0, 0, 10, 10), dims = c(10, 10),
            field = "z", fun = "mean")
unlink(f)
```
