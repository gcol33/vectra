# Terrain derivatives from a streamed elevation raster

Computes DEM derivatives from a `.vec` elevation raster with Horn's 3x3
method, on the same haloed tile-row strip pass as
[`focal()`](https://gillescolling.com/vectra/reference/focal.md) – the
input is read one strip at a time and, when `path` is given, the outputs
are streamed straight back to a multi-band `.vec`. Matches terra's
`terrain()` / `shade()` conventions.

## Usage

``` r
terrain(
  x,
  v = c("slope", "aspect", "hillshade", "TPI", "roughness", "TRI"),
  unit = c("degrees", "radians"),
  azimuth = 315,
  altitude = 45,
  band = 1L,
  path = NULL,
  dtype = "f32",
  compression = c("fast", "balanced", "max")
)
```

## Arguments

- x:

  A `vectra_raster` (from
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md))
  or a path to a `.vec` elevation raster.

- v:

  Derivatives to compute, any of `"slope"`, `"aspect"`, `"hillshade"`,
  `"TPI"` (topographic position index), `"roughness"`, `"TRI"` (terrain
  ruggedness index). The return follows the input: one matrix for a
  single `v`, a named list for several.

- unit:

  Angular unit for `slope` and `aspect`: `"degrees"` (default) or
  `"radians"`.

- azimuth, altitude:

  Sun position for `"hillshade"`, in degrees. Defaults 315 (NW) and 45.

- band:

  Band to read (1-based). Default 1.

- path:

  Optional output `.vec` path (one band per `v`, named after `v`). When
  given the result is streamed to disk and the opened
  [`vec_open_raster()`](https://gillescolling.com/vectra/reference/vec_open_raster.md)
  handle is returned invisibly; when `NULL` the result is returned in
  memory.

- dtype:

  Storage dtype for `.vec` output (see
  [`vec_write_raster()`](https://gillescolling.com/vectra/reference/vec_write_raster.md)).
  Default `"f32"`.

- compression:

  Compression effort for `.vec` output. Default `"fast"`.

## Value

When `path` is `NULL`: a numeric matrix for a single `v`, or a named
list of matrices for several, each carrying `gt`, `extent`, and `crs`
attributes (row 1 northmost). When `path` is given, the written
multi-band `vectra_raster` handle (invisibly).

## Details

Slope and aspect use the Horn (1981) finite-difference gradient over the
3x3 neighbourhood; `aspect` is degrees clockwise from north (flat cells
return 90). `hillshade` is the cosine of the incidence angle for the
given sun position, clamped at 0. `TPI` is the cell minus the mean of
its eight neighbours; `roughness` is the range over the 3x3; `TRI` is
the mean absolute difference to the eight neighbours. Cells whose 3x3
neighbourhood touches a nodata value or the raster edge return `NA`.

## See also

[`focal()`](https://gillescolling.com/vectra/reference/focal.md) for
arbitrary moving windows.

## Examples

``` r
# A tilted surface so slope and aspect are well defined.
z <- outer(1:8, 1:8, function(r, c) 10 + 2 * c + r)
f <- tempfile(fileext = ".vec")
vec_write_raster(z, f, dtype = "f64", extent = c(0, 0, 8, 8))

slp <- terrain(f, v = "slope")
deriv <- terrain(f, v = c("slope", "aspect", "hillshade"))
names(deriv)
unlink(f)
```
