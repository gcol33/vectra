# Interval (range overlap) join of two vectra tables

Joins each row of `x` to every row of `y` whose `[start, end]` interval
overlaps it – the one-dimensional analogue of a spatial bounding-box
join
([`data.table::foverlaps`](https://rdrr.io/pkg/data.table/man/foverlaps.html),
[`GenomicRanges::findOverlaps`](https://rdrr.io/pkg/IRanges/man/findOverlaps-methods.html)).
An optional equality key in `by` restricts overlap testing to rows that
share that key (a chromosome, a sensor id), the same role a regular
join's `by` plays.

## Usage

``` r
interval_join(
  x,
  y,
  start,
  end,
  by = NULL,
  type = c("inner", "left"),
  closed = TRUE,
  n_threads = 4L,
  suffix = ".y"
)
```

## Arguments

- x:

  A `vectra_node` (the streamed / probe side).

- y:

  A `vectra_node` (the resident / build side).

- start:

  The interval start columns, as `c("x_col" = "y_col")` or a single name
  shared by both sides.

- end:

  The interval end columns, in the same form as `start`.

- by:

  Optional equality key restricting overlap to rows that match on it, as
  `c("x_col" = "y_col")` or a single shared name. `NULL` (default) tests
  every pair.

- type:

  `"inner"` (default) keeps only overlapping pairs; `"left"` keeps every
  `x` row, filling `y` columns with `NA` where nothing overlaps.

- closed:

  Logical. `TRUE` (default) counts intervals that touch at an endpoint
  as overlapping; `FALSE` requires a strictly positive overlap.

- n_threads:

  Integer. OpenMP threads for the per-group sweep. Default `4L`.

- suffix:

  Character. Suffix appended to `y` column names that collide with `x`
  names. Default `".y"`.

## Value

A `vectra_node` with all `x` columns followed by all `y` columns
(suffixed on collision).

## Details

Both sides are read into memory, then within each `by` group a sweep
over the interval endpoints emits each overlapping pair once, scaling
with the number of overlaps rather than the product of the row counts.
