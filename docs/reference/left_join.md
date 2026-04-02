# Join two vectra tables

Join two vectra tables

## Usage

``` r
left_join(x, y, by = NULL, suffix = c(".x", ".y"), ...)

inner_join(x, y, by = NULL, suffix = c(".x", ".y"), ...)

right_join(x, y, by = NULL, suffix = c(".x", ".y"), ...)

full_join(x, y, by = NULL, suffix = c(".x", ".y"), ...)

semi_join(x, y, by = NULL, ...)

anti_join(x, y, by = NULL, ...)
```

## Arguments

- x:

  A `vectra_node` object (left table).

- y:

  A `vectra_node` object (right table).

- by:

  A character vector of column names to join by, or a named vector like
  `c("a" = "b")`. `NULL` for natural join (common columns).

- suffix:

  A character vector of length 2 for disambiguating non-key columns with
  the same name (default `c(".x", ".y")`).

- ...:

  Ignored.

## Value

A `vectra_node` with the joined result.

## Details

All joins use a build-right, probe-left hash join. The entire right-side
table is materialized into a hash table; left-side batches stream
through. Memory cost is proportional to the right-side table size.

NA keys never match (SQL NULL semantics). Key types are auto-coerced
following the `bool < int64 < double` hierarchy. Joining string against
numeric keys is an error.

## Examples

``` r
f1 <- tempfile(fileext = ".vtr")
f2 <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = c(1, 2, 3), x = c(10, 20, 30)), f1)
write_vtr(data.frame(id = c(1, 2, 4), y = c(100, 200, 400)), f2)
left_join(tbl(f1), tbl(f2), by = "id") |> collect()
unlink(c(f1, f2))
```
