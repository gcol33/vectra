# Create a lazy table reference from a FASTA file

Streams a FASTA file of biological sequences as rows: one row per record
with columns `id`, `desc`, and `seq`. `id` is the first
whitespace-delimited token of the header line, `desc` is the remainder
(an empty string when the header carries no description), and `seq` is
the sequence with line breaks removed. Records stream one batch at a
time, so a read set larger than RAM never fully materializes.
Gzip-compressed files (`.fasta.gz`, `.fa.gz`) are read transparently. No
data is read until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) is
called.

## Usage

``` r
tbl_fasta(path, batch_size = .DEFAULT_BATCH_SIZE, quiet = FALSE)
```

## Arguments

- path:

  Path to a `.fasta`/`.fa` file, optionally gzip-compressed.

- batch_size:

  Number of records per batch (default 65536).

- quiet:

  If `FALSE` (default), report the record count when the scan completes.

## Value

A `vectra_node` object representing a lazy scan of the FASTA file.

## Details

The `seq_*` expression family (`seq_revcomp()`, `seq_gc()`,
`seq_translate()`, `seq_dist()`, ...) operates on the `seq` column
directly inside
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)/[`filter()`](https://gillescolling.com/vectra/reference/filter.md).
See
[seq_expressions](https://gillescolling.com/vectra/reference/seq_expressions.md).

A record cut short is a loud error, not a silent drop: a header that is
not the first non-blank token, or a byte where a `>` is expected, stops
the scan. When the scan reaches the end of the file it reports the
number of records read (suppress with `quiet = TRUE`).

## See also

[`tbl_fastq()`](https://gillescolling.com/vectra/reference/tbl_fastq.md),
[seq_expressions](https://gillescolling.com/vectra/reference/seq_expressions.md)

## Examples

``` r
f <- tempfile(fileext = ".fasta")
writeLines(c(">seq1 first", "ACGTACGT", ">seq2 second", "GGGGCCCC"), f)
node <- tbl_fasta(f, quiet = TRUE)
node |> mutate(gc = seq_gc(seq)) |> collect()
unlink(f)
```
