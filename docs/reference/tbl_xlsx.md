# Create a lazy table reference from an Excel (.xlsx) file

Reads a sheet from an Excel workbook into a vectra node for lazy query
execution. The sheet is read into memory via
[`openxlsx2::read_xlsx()`](https://janmarvin.github.io/openxlsx2/reference/wb_to_df.html)
and then converted to vectra's internal format. Requires the openxlsx2
package.

## Usage

``` r
tbl_xlsx(path, sheet = 1L, batch_size = .DEFAULT_BATCH_SIZE)
```

## Arguments

- path:

  Path to an `.xlsx` file.

- sheet:

  Sheet to read: either a name (character) or 1-based index (integer).
  Default `1L` (first sheet).

- batch_size:

  Number of rows per batch (default 65536).

## Value

A `vectra_node` object representing a lazy scan of the sheet.

## Examples

``` r
if (FALSE) { # \dontrun{
node <- tbl_xlsx("data.xlsx")
node |> filter(score > 80) |> collect()

# Read a specific sheet by name
node <- tbl_xlsx("data.xlsx", sheet = "Sheet2")
} # }
```
