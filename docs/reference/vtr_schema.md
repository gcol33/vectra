# Create a star schema over linked vectra tables

Registers a fact table with named dimension links. The schema enables
[`lookup()`](https://gillescolling.com/vectra/reference/lookup.md) to
resolve columns from dimension tables without writing explicit joins.

## Usage

``` r
vtr_schema(fact, ...)
```

## Arguments

- fact:

  A `vectra_node` object (the central fact table). Must be file-backed
  (created via
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md),
  [`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md),
  or
  [`tbl_sqlite()`](https://gillescolling.com/vectra/reference/tbl_sqlite.md)).

- ...:

  Named `vectra_link` objects created by
  [`link()`](https://gillescolling.com/vectra/reference/link.md). Names
  become the dimension aliases used in
  [`lookup()`](https://gillescolling.com/vectra/reference/lookup.md)
  (e.g., `species$name`).

## Value

A `vectra_schema` object.

## Examples

``` r
# \donttest{
f_obs <- tempfile(fileext = ".vtr")
f_sp  <- tempfile(fileext = ".vtr")
f_ct  <- tempfile(fileext = ".vtr")
write_vtr(data.frame(sp_id = 1:3, ct_code = c("AT", "DE", "FR"),
                      value = 10:12), f_obs)
write_vtr(data.frame(sp_id = 1:3,
                      name = c("Oak", "Beech", "Pine")), f_sp)
write_vtr(data.frame(ct_code = c("AT", "DE", "FR"),
                      gdp = c(400, 3800, 2700)), f_ct)

s <- vtr_schema(
  fact    = tbl(f_obs),
  species = link("sp_id", tbl(f_sp)),
  country = link("ct_code", tbl(f_ct))
)
print(s)
unlink(c(f_obs, f_sp, f_ct))
# }
```
