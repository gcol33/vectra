# Fuzzy join two vectra tables by string distance

Joins two tables using approximate string matching on key columns.
Optionally blocks by a second column (e.g., genus) for performance —
only rows sharing the same blocking key are compared.

## Usage

``` r
fuzzy_join(
  x,
  y,
  by,
  method = "dl",
  max_dist = 0.2,
  block_by = NULL,
  n_threads = 4L,
  suffix = ".y"
)
```

## Arguments

- x:

  A `vectra_node` object (probe / query side).

- y:

  A `vectra_node` object (build / reference side).

- by:

  A named character vector of length 1: `c("probe_col" = "build_col")`.
  The columns to compute string distance on.

- method:

  Character. Distance algorithm: `"dl"` (Damerau-Levenshtein, default),
  `"levenshtein"`, or `"jw"` (Jaro-Winkler).

- max_dist:

  Numeric. Maximum normalized distance (0-1) to keep a match. Default
  `0.2`.

- block_by:

  Optional named character vector of length 1:
  `c("probe_col" = "build_col")`. Rows must match exactly on these
  columns before distance is computed. Dramatically reduces comparisons.

- n_threads:

  Integer. Number of OpenMP threads for parallel distance computation
  over partitions. Default `4L`.

- suffix:

  Character. Suffix appended to build-side column names that collide
  with probe-side names. Default `".y"`.

## Value

A `vectra_node` with all probe columns, all build columns (suffixed on
collision), and a `fuzzy_dist` column (double).
