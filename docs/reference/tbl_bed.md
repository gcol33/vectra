# Create a lazy table reference from a BED file

Streams a BED (Browser Extensible Data) file of genomic features as
rows, one row per feature. Columns follow the BED specification in
order: `chrom`, `start`, `end`, `name`, `score`, `strand`, `thickStart`,
`thickEnd`, `itemRgb`, `blockCount`, `blockSizes`, `blockStarts`; any
fields beyond the twelfth (a "BED N+" file) are read as strings named
`V13`, `V14`, ... The column count is fixed by the first feature line
and every later line must match it. Features stream one batch at a time,
so a genome-scale interval set never fully materializes. Gzip-compressed
files (`.bed.gz`) are read transparently. No data is read until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) is
called.

## Usage

``` r
tbl_bed(path, batch_size = .DEFAULT_BATCH_SIZE, quiet = FALSE)
```

## Arguments

- path:

  Path to a `.bed` file, optionally gzip-compressed.

- batch_size:

  Number of features per batch (default 65536).

- quiet:

  If `FALSE` (default), report the feature count when the scan
  completes.

## Value

A `vectra_node` object representing a lazy scan of the BED file.

## Details

Fields are whitespace-delimited (tab or space), and blank lines, `#`
comments, and UCSC `track`/`browser` directive lines are skipped.
`chrom`, `start`, and `end` are required; `start` and `end` are integers
(a non-integer there stops the scan). Optional integer fields (`score`,
`thickStart`, `thickEnd`, `blockCount`) accept `.` or `NA` as missing.
When the scan reaches the end of the file it reports the number of
features read (suppress with `quiet = TRUE`).

**Coordinates are read faithfully.** BED `start` is 0-based and `end` is
half-open (the first position past the feature), and both are returned
exactly as stored — no coordinate is rewritten. Two features that abut
(`end` of one equal to `start` of the next) share no base. To
overlap-join BED tables with those half-open semantics — matching
bedtools and
[`GenomicRanges::findOverlaps()`](https://rdrr.io/pkg/IRanges/man/findOverlaps-methods.html)
— pass `closed = FALSE` to
[`interval_join()`](https://gillescolling.com/vectra/reference/interval_join.md),
which requires a strictly positive overlap and so does not pair abutting
features:

    peaks <- tbl_bed("peaks.bed")
    genes <- tbl_bed("genes.bed")
    interval_join(peaks, genes, start = "start", end = "end",
                  by = "chrom", closed = FALSE)

## See also

[`interval_join()`](https://gillescolling.com/vectra/reference/interval_join.md),
[`tbl_fasta()`](https://gillescolling.com/vectra/reference/tbl_fasta.md)

## Examples

``` r
f <- tempfile(fileext = ".bed")
writeLines(c("chr1\t100\t200\tfeatA\t0\t+",
             "chr1\t150\t400\tfeatB\t0\t-",
             "chr2\t500\t900\tfeatC\t0\t+"), f)
node <- tbl_bed(f, quiet = TRUE)
node |> filter(chrom == "chr1") |> collect()
unlink(f)
```
