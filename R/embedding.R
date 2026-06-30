# Embedding columns: fixed-width float vectors stored as a hex-encoded
# little-endian float32 blob in an ordinary string column, the same "opaque
# blob per cell" representation geometry uses for hex-WKB. Distance ops
# (cosine, l2, dot) decode the blob inside the C engine (see expr_vec.c).

# Encode one numeric vector as a hex float32 blob.
.embedding_hex <- function(v) {
  raw_bytes <- writeBin(as.double(v), raw(), size = 4L, endian = "little")
  paste0(sprintf("%02x", as.integer(raw_bytes)), collapse = "")
}

#' Encode vectors as an embedding column
#'
#' Packs numeric vectors into the hex float32 blobs vectra stores embedding
#' columns as. Write the result as an ordinary character column; the distance
#' functions [cosine()], [l2()], and [dot()] decode it inside the engine.
#'
#' @param x A numeric matrix (one embedding per row), a list of equal-length
#'   numeric vectors, or a single numeric vector (one embedding).
#'
#' @return A character vector with one hex-encoded blob per embedding.
#'
#' @examples
#' m <- matrix(rnorm(12), nrow = 3)        # 3 embeddings of length 4
#' emb <- as_embedding(m)
#' df <- data.frame(id = 1:3, emb = emb)
#'
#' @seealso [cosine()], [l2()], [dot()]
#' @export
as_embedding <- function(x) {
  if (is.numeric(x) && is.null(dim(x)))
    x <- matrix(as.double(x), nrow = 1)
  if (is.list(x) && !is.data.frame(x)) {
    lens <- lengths(x)
    if (length(unique(lens)) > 1L)
      stop("all embedding vectors must have the same length")
    x <- do.call(rbind, lapply(x, as.double))
  }
  if (!is.matrix(x))
    stop("x must be a numeric matrix, a list of numeric vectors, or a numeric vector")
  storage.mode(x) <- "double"
  vapply(seq_len(nrow(x)), function(i) .embedding_hex(x[i, ]), character(1))
}

#' Embedding distance functions
#'
#' Distance and similarity functions over embedding columns (see
#' [as_embedding()]), usable inside [mutate()] / [filter()] like any other
#' expression. The query side is either another embedding column or a constant
#' numeric vector. Decoding and the arithmetic run in C, one row at a time,
#' parallelized over rows.
#'
#' `cosine()` returns cosine distance (`1 - similarity`) and `l2()` Euclidean
#' distance, so smaller means nearer -- pair them with `slice_min()` for
#' nearest-neighbour search. `dot()` returns the inner product, where larger
#' means nearer (`slice_max()`).
#'
#' @param x An embedding column.
#' @param y An embedding column, or a constant numeric query vector.
#'
#' @return A double column.
#'
#' @examples
#' \dontrun{
#' q <- rnorm(128)
#' tbl("vecs.vtr") |>
#'   mutate(d = cosine(emb, q)) |>
#'   slice_min(d, n = 10) |>
#'   collect()
#' }
#'
#' @name embedding_distance
#' @seealso [as_embedding()]
NULL

#' @rdname embedding_distance
#' @export
cosine <- function(x, y) stop("cosine() is only usable inside a vectra mutate()/filter()")

#' @rdname embedding_distance
#' @export
l2 <- function(x, y) stop("l2() is only usable inside a vectra mutate()/filter()")

#' @rdname embedding_distance
#' @export
dot <- function(x, y) stop("dot() is only usable inside a vectra mutate()/filter()")
