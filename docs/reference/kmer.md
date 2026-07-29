# k-mer spectrum of a sequence column

Counts every fixed-length subsequence (k-mer) of a nucleotide string
column, grouped by zero or more key columns. The result is one row per
distinct (group, k-mer): the grouping columns, a `kmer` column, and an
integer `count`. This is the set-wise counterpart to the per-row
[seq_expressions](https://gillescolling.com/vectra/reference/seq_expressions.md)
family — it materializes the whole spectrum, so it is a blocking step
like
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md),
but the k-mer table is the only state held, not the input.

## Usage

``` r
kmer(x, seq, k = 4, by = NULL, canonical = FALSE)
```

## Arguments

- x:

  A `vectra_node` object.

- seq:

  The sequence column (unquoted). Defaults to `seq`, the column
  [`tbl_fasta()`](https://gillescolling.com/vectra/reference/tbl_fasta.md)
  and
  [`tbl_fastq()`](https://gillescolling.com/vectra/reference/tbl_fastq.md)
  produce.

- k:

  k-mer length, an integer in `1:32` (default 4).

- by:

  Grouping column(s): an unquoted name, `c(col1, col2)`, or a character
  vector. `NULL` (default) counts one spectrum over the whole input.

- canonical:

  If `TRUE`, collapse each k-mer with its reverse complement (default
  `FALSE`).

## Value

A `vectra_node` with the grouping columns, a `kmer` column, and a
`count` column.

## Details

Each k-mer is packed into 2 bits per base (`A`/`C`/`G`/`T`) and counted
in a native hash table, so `k` is limited to `1:32`. A window containing
any non-`ACGT` base (`N`, an IUPAC ambiguity code, or a gap) is skipped,
matching the convention of dedicated k-mer counters. With
`canonical = TRUE`, a k-mer and its reverse complement are counted
together under the lexicographically smaller of the two — the usual
choice for strand-agnostic data.

Row order in the result is unspecified; pipe into
[`arrange()`](https://gillescolling.com/vectra/reference/arrange.md) for
a stable order.

## See also

[seq_expressions](https://gillescolling.com/vectra/reference/seq_expressions.md),
[`tbl_fasta()`](https://gillescolling.com/vectra/reference/tbl_fasta.md)

## Examples

``` r
f <- tempfile(fileext = ".vtr")
write_vtr(data.frame(id = c("a", "b"),
                     seq = c("ACGTACGT", "AAAAT"),
                     stringsAsFactors = FALSE), f)
tbl(f) |> kmer(seq, k = 3, by = id) |> arrange(id, kmer) |> collect()
unlink(f)
```
