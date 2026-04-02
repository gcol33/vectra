# Mark a column for descending sort order

Used inside
[`arrange()`](https://gillescolling.com/vectra/reference/arrange.md) to
sort a column in descending order.

## Usage

``` r
desc(x)
```

## Arguments

- x:

  A column name.

## Value

A marker used by
[`arrange()`](https://gillescolling.com/vectra/reference/arrange.md).

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)
tbl(f) |> arrange(desc(mpg)) |> collect() |> head()
unlink(f)
```
