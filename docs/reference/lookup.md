# Look up columns from linked dimension tables

Resolves columns from dimension tables registered in a
[`vtr_schema()`](https://gillescolling.com/vectra/reference/vtr_schema.md),
automatically building the necessary join tree. Reports unmatched keys
as a diagnostic message.

## Usage

``` r
lookup(.schema, ..., .join = "left", .report = TRUE)
```

## Arguments

- .schema:

  A `vectra_schema` object.

- ...:

  Column references: bare names for fact columns, or `dimension$column`
  for dimension columns.

- .join:

  Join type: `"left"` (default, keeps all fact rows) or `"inner"` (drops
  unmatched fact rows).

- .report:

  Logical. If `TRUE` (default), print a message with the number of
  unmatched keys per dimension.

## Value

A `vectra_node` with the selected columns.

## Details

Column references use `dimension$column` syntax (e.g., `species$name`).
Columns from the fact table can be referenced by name directly.

When `.report = TRUE`, each needed dimension is checked for unmatched
keys by opening fresh scans of the fact and dimension tables. This adds
one extra read pass per dimension but does not affect the lazy result
node.

Only dimensions referenced in `...` are joined. Unreferenced dimensions
are never scanned.

## Examples

``` r
# \donttest{
f_obs <- tempfile(fileext = ".vtr")
f_sp  <- tempfile(fileext = ".vtr")
f_ct  <- tempfile(fileext = ".vtr")
write_vtr(data.frame(sp_id = 1:4, ct_code = c("AT", "DE", "FR", "XX"),
                      value = 10:13), f_obs)
write_vtr(data.frame(sp_id = 1:3,
                      name = c("Oak", "Beech", "Pine")), f_sp)
write_vtr(data.frame(ct_code = c("AT", "DE", "FR"),
                      gdp = c(400, 3800, 2700)), f_ct)

s <- vtr_schema(
  fact    = tbl(f_obs),
  species = link("sp_id", tbl(f_sp)),
  country = link("ct_code", tbl(f_ct))
)

# Pull columns from any linked dimension
result <- lookup(s, value, species$name, country$gdp)
collect(result)

unlink(c(f_obs, f_sp, f_ct))
# }
```
