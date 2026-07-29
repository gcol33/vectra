# Create a lazy table reference from a FASTQ file

Streams a FASTQ file as rows: one row per record with columns `id`,
`desc`, `seq`, and `qual` (the raw quality string, same length as
`seq`). `id` and `desc` are split from the header as in
[`tbl_fasta()`](https://gillescolling.com/vectra/reference/tbl_fasta.md).
Records stream one batch at a time, so a read set larger than RAM never
fully materializes. Gzip-compressed files (`.fastq.gz`, `.fq.gz`) are
read transparently. No data is read until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) is
called.

## Usage

``` r
tbl_fastq(path, batch_size = .DEFAULT_BATCH_SIZE, quiet = FALSE)
```

## Arguments

- path:

  Path to a `.fastq`/`.fq` file, optionally gzip-compressed.

- batch_size:

  Number of records per batch (default 65536).

- quiet:

  If `FALSE` (default), report the record count when the scan completes.

## Value

A `vectra_node` object representing a lazy scan of the FASTQ file.

## Details

Records are parsed in the standard four-line form (header, sequence, `+`
separator, quality). A record cut short — a missing sequence, separator,
or quality line, or a quality string whose length does not match the
sequence — is a loud error, not a silent drop. When the scan reaches the
end of the file it reports the number of records read (suppress with
`quiet = TRUE`).

## See also

[`tbl_fasta()`](https://gillescolling.com/vectra/reference/tbl_fasta.md),
[seq_expressions](https://gillescolling.com/vectra/reference/seq_expressions.md)

## Examples

``` r
f <- tempfile(fileext = ".fastq")
writeLines(c("@r1 read one", "ACGT", "+", "IIII",
             "@r2 read two", "GGCC", "+", "!!!!"), f)
node <- tbl_fastq(f, quiet = TRUE)
node |> mutate(len = seq_length(seq)) |> collect()
unlink(f)
```
