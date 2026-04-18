# Define a link between a fact table and a dimension table

Creates a link descriptor that specifies how to join a dimension table
to a fact table via one or more key columns.

## Usage

``` r
link(key, node)
```

## Arguments

- key:

  A character vector or named character vector specifying join keys.
  Unnamed: same column name in both tables. Named:
  `c("fact_col" = "dim_col")`.

- node:

  A `vectra_node` object (the dimension table). Must be file-backed
  (created via
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
  [`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md),
  or
  [`tbl_sqlite()`](https://gillescolling.com/vectra/reference/tbl_sqlite.md)).

## Value

A `vectra_link` object.

## Examples

``` r
# \donttest{
f_obs <- tempfile(fileext = ".vtr")
f_sp  <- tempfile(fileext = ".vtr")
write_vtr(data.frame(sp_id = 1:3, value = c(10, 20, 30)), f_obs)
write_vtr(data.frame(sp_id = 1:3, name = c("A", "B", "C")), f_sp)
lnk <- link("sp_id", tbl(f_sp))
unlink(c(f_obs, f_sp))
# }
```
