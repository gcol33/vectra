# Biological-sequence functions inside mutate(), filter(), and summarise()

vectra recognizes a family of `seq_*` functions directly inside
expression verbs. A DNA, RNA, or protein sequence rides through the
engine as an ordinary ASCII string column; these functions decode it in
C one row at a time and compute a measure, a transformed sequence, or an
edit distance straight off it, streaming at engine speed with no
round-trip through Biostrings.
`tbl_fasta("reads.fa") |> mutate(gc = seq_gc(seq))` adds a GC-content
column; `filter(seq_dist(seq, ref) <= 3)` prunes the stream in C.

## Details

These names are interpreted by the expression engine; they are not
exported R functions and are not called as such. They are available only
inside
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
[`transmute()`](https://gillescolling.com/vectra/reference/transmute.md),
[`filter()`](https://gillescolling.com/vectra/reference/filter.md), and
a grouped
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
over a `vectra_node`. The sequence argument is an ordinary string
column.

## Measures

- `seq_length(x)`:

  Number of characters in the sequence (integer).

- `seq_gc(x)`:

  GC fraction: the count of `G` and `C` divided by the sequence length,
  in `[0, 1]` (double). Case-insensitive; only literal `G`/`C` are
  counted, so an ambiguity code such as `S` does not contribute.

## Transforms (return a sequence)

- `seq_revcomp(x)`:

  Reverse complement.

- `seq_complement(x)`:

  Complement, DNA alphabet (`A`\<-\>`T`), with the IUPAC ambiguity codes
  mapped to their complements and case preserved. A `U` is complemented
  to `A`; transcribe first for an RNA-alphabet complement.

- `seq_reverse(x)`:

  Reverse the sequence (no complement).

- `seq_transcribe(x)`:

  Swap `T`\<-\>`U`, turning DNA into RNA and RNA into DNA (case
  preserved).

- `seq_translate(x, table = 1)`:

  Translate in reading frame 1 to a protein sequence using the standard
  genetic code (NCBI `transl_table` 1; `table` currently accepts only
  1). A trailing partial codon is dropped; a codon containing any
  non-`ACGTU` base yields `X`, a stop codon `*`.

- `seq_subseq(x, start, width)`:

  Substring of `width` characters starting at 1-based `start`, clamped
  to the sequence. `start` and `width` may be columns or constants.

## Distance

- `seq_dist(x, ref, method = "levenshtein")`:

  Edit distance between each sequence and a reference (integer). `ref`
  is another sequence column (compared row by row) or a single constant
  string (compared against every row). `method` is `"levenshtein"`
  (default; also `"lv"`, `"edit"`), `"dl"` / `"damerau"` / `"osa"` for
  the optimal-string-alignment Damerau-Levenshtein distance, or
  `"hamming"` (substitutions only; `NA` when the two sequences differ in
  length).

## Missing sequence

A missing (`NA`) cell yields `NA` for that row rather than an error,
matching the `st_*` and embedding-distance contract. Unexpected
characters are processed as-is (passed through by complement, translated
to `X`).

## See also

[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
[`filter()`](https://gillescolling.com/vectra/reference/filter.md);
[geom_expressions](https://gillescolling.com/vectra/reference/geom_expressions.md)
and the embedding distances (`cosine`, `l2`, `dot`) for the other
per-row blob-column families.

## Examples

``` r
reads <- data.frame(
  id  = c("r1", "r2", "r3"),
  seq = c("ATGGCCATTGTA", "GGGCCCTTTAAA", "ATGTAA"),
  stringsAsFactors = FALSE
)
f <- tempfile(fileext = ".vtr")
write_vtr(reads, f)

tbl(f) |>
  mutate(
    len = seq_length(seq),
    gc  = seq_gc(seq),
    rc  = seq_revcomp(seq),
    aa  = seq_translate(seq),
    d   = seq_dist(seq, "ATGGCCATTGTA")
  ) |>
  collect()

unlink(f)
```
