# Sequence set-wise verbs. kmer(): k-mer spectrum node.

#' k-mer spectrum of a sequence column
#'
#' Counts every fixed-length subsequence (k-mer) of a nucleotide string column,
#' grouped by zero or more key columns. The result is one row per distinct
#' (group, k-mer): the grouping columns, a `kmer` column, and an integer
#' `count`. This is the set-wise counterpart to the per-row [seq_expressions]
#' family --- it materializes the whole spectrum, so it is a blocking step like
#' [summarise()], but the k-mer table is the only state held, not the input.
#'
#' Each k-mer is packed into 2 bits per base (`A`/`C`/`G`/`T`) and counted in a
#' native hash table, so `k` is limited to `1:32`. A window containing any
#' non-`ACGT` base (`N`, an IUPAC ambiguity code, or a gap) is skipped, matching
#' the convention of dedicated k-mer counters. With `canonical = TRUE`, a k-mer
#' and its reverse complement are counted together under the lexicographically
#' smaller of the two --- the usual choice for strand-agnostic data.
#'
#' Row order in the result is unspecified; pipe into [arrange()] for a stable
#' order.
#'
#' @param x A `vectra_node` object.
#' @param seq The sequence column (unquoted). Defaults to `seq`, the column
#'   [tbl_fasta()] and [tbl_fastq()] produce.
#' @param k k-mer length, an integer in `1:32` (default 4).
#' @param by Grouping column(s): an unquoted name, `c(col1, col2)`, or a
#'   character vector. `NULL` (default) counts one spectrum over the whole input.
#' @param canonical If `TRUE`, collapse each k-mer with its reverse complement
#'   (default `FALSE`).
#'
#' @return A `vectra_node` with the grouping columns, a `kmer` column, and a
#'   `count` column.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(id = c("a", "b"),
#'                      seq = c("ACGTACGT", "AAAAT"),
#'                      stringsAsFactors = FALSE), f)
#' tbl(f) |> kmer(seq, k = 3, by = id) |> arrange(id, kmer) |> collect()
#' unlink(f)
#'
#' @seealso [seq_expressions], [tbl_fasta()]
#' @export
kmer <- function(x, seq, k = 4, by = NULL, canonical = FALSE) {
  UseMethod("kmer")
}

#' @export
kmer.vectra_node <- function(x, seq, k = 4, by = NULL, canonical = FALSE) {
  seq_name <- if (missing(seq)) "seq"
              else .col_name_from_expr(substitute(seq), "seq")
  by_names <- .by_names_from_expr(substitute(by), parent.frame())

  if (!is.numeric(k) || length(k) != 1 || is.na(k))
    stop(sprintf("k must be a single integer in 1:32, got %s", deparse(k)))
  k <- as.integer(k)
  if (k < 1L || k > 32L)
    stop(sprintf("k must be between 1 and 32, got %d", k))
  if (!is.logical(canonical) || length(canonical) != 1 || is.na(canonical))
    stop("canonical must be TRUE or FALSE")

  new_xptr <- .Call(C_kmer_node, x$.node, seq_name, k, canonical, by_names)
  structure(list(.node = new_xptr, .path = x$.path), class = "vectra_node")
}

# A single column name from a substituted expression (bare name or string).
.col_name_from_expr <- function(expr, arg) {
  if (is.character(expr) && length(expr) == 1) return(expr)
  if (is.name(expr)) return(as.character(expr))
  stop(sprintf("`%s` must be a single column name", arg))
}

# Column names from a substituted `by`: NULL, a bare name, c(a, b), or a
# character vector.
.by_names_from_expr <- function(by_expr, env) {
  if (is.null(by_expr)) return(character(0))
  if (is.name(by_expr)) return(as.character(by_expr))
  if (is.character(by_expr)) return(by_expr)
  if (is.call(by_expr) && identical(by_expr[[1]], quote(c)))
    return(vapply(as.list(by_expr)[-1], function(e)
      if (is.character(e)) e else as.character(e), character(1)))
  val <- eval(by_expr, env)
  if (is.null(val)) character(0) else as.character(val)
}
