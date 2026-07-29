# Build overview pyramids for a .vec raster

Appends `n_levels - 1` reduced-resolution copies of the raster to the
file. Each level is computed by 2x downsampling the previous level with
the chosen kernel. Reading via `vec_read_window(level = L)` picks tiles
at level L; the file's `n_levels` is updated in place.

## Usage

``` r
vec_build_overviews(
  path,
  levels,
  resampling = c("average", "nearest", "bilinear", "mode", "gauss"),
  compression = c("fast", "balanced", "max")
)
```

## Arguments

- path:

  Path to a `.vec` raster file. The file is modified in place.

- levels:

  Total levels including level 0 (so `levels = 5` adds four overviews:
  levels 1..4). Must be in `[2, 16]`.

- resampling:

  One of `"nearest"`, `"average"`, `"bilinear"`, `"mode"`, `"gauss"`.
  `"average"` is the right choice for continuous rasters; `"mode"` for
  categorical/land-cover.

- compression:

  Compression effort for the new tiles. Defaults to `"fast"` because
  overview tiles are usually one-shot writes.

## Value

Invisible `NULL`.

## Details

Unlike the streamed raster verbs, this decodes every band of the base
raster into memory at once to build the pyramid, so peak memory is on
the order of the full base raster (all bands). Build overviews before a
raster grows past what fits in RAM, or on a per-band basis for very
large stacks.
