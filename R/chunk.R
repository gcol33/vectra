# chunk.R - streaming consumption of a query one batch at a time.
#
# collect() materializes the whole result into one data.frame. These verbs
# instead hand the result to R in bounded pieces: collect_chunked() folds a
# function over the stream (vectra drives the iteration), and chunk_feeder()
# turns a query into a resettable generator that a pull-based consumer such as
# biglm::bigglm() drives itself. Both sit on the same C cursor (C_node_optimize
# once, then C_node_next_batch per piece), so there is one streaming path.

# Open a streaming cursor over a node. Optimizes the plan once, then returns a
# nullary function that yields the next non-empty chunk as a data.frame, or
# NULL at end of stream. The node is consumed: once drained it cannot be reused
# (build a fresh node to stream again).
.batch_cursor <- function(x) {
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (build one with tbl(), tbl_csv(), ...)")
  node <- x$.node
  .Call(C_node_optimize, node)
  function() .Call(C_node_next_batch, node)
}

#' Fold a function over a query, one batch at a time
#'
#' Streams a lazy query through R in bounded pieces and reduces them with `f`,
#' instead of materializing the whole result the way [collect()] does. The
#' engine pulls one batch (a data.frame of up to a few hundred thousand rows)
#' at a time; `f` is called as `f(acc, chunk)` and its return value becomes the
#' accumulator for the next batch. Peak memory is one batch plus whatever the
#' accumulator holds, so a result far larger than RAM can be reduced to a small
#' summary in a single pass.
#'
#' This is the streaming counterpart to a fold (`Reduce()`): use it when the
#' query returns more rows than fit in memory but the *reduction* is small. A
#' running count, per-group sufficient statistics, the cross-products `X'X` and
#' `X'y` behind a linear fit, an online mean or histogram - all accumulate in
#' bounded space across the stream. When you instead need the model-fitting
#' consumer to drive the iteration (and to re-read the data on each pass, as an
#' iteratively reweighted GLM does), use [chunk_feeder()].
#'
#' @param x A `vectra_node` (from [tbl()], [tbl_csv()], [tbl_tiff()], ... and
#'   any chain of verbs). It is consumed: after `collect_chunked()` returns, the
#'   stream is drained and `x` cannot be collected again.
#' @param f A function of two arguments `function(acc, chunk)` returning the
#'   updated accumulator. `chunk` is a data.frame holding the next batch of
#'   rows.
#' @param .init Initial accumulator value. Passed to `f` with the first batch
#'   and returned unchanged if the query yields no rows. When `combine` is
#'   supplied this is also the monoid identity (the value `combine` leaves
#'   unchanged).
#' @param combine Optional function `function(acc, acc)` that merges two
#'   accumulators. Supplying it declares the reduction a monoid with `.init` as
#'   identity, which is what lets the fold run over the independent shards of a
#'   partition (`offload(x, by = ...)`) and have the partial results merged
#'   correctly. For a plain node the stream is a single sequence, so `combine`
#'   is not needed and is ignored.
#' @param commutative Logical; declare that `combine` does not depend on the
#'   order of its arguments. Lets a partitioned fold merge shards in any order
#'   (no stable sort required). Default `FALSE`.
#'
#' @return The final accumulator. For a node: `f` applied left-to-right across
#'   every batch, seeded with `.init`. For a partition: each shard folded with
#'   `f`/`.init`, then those per-shard accumulators merged with `combine`.
#'
#' @seealso [chunk_feeder()] for pull-based consumers such as `biglm::bigglm()`,
#'   [offload()] for the replay cache and the partitioned monoidal reduce,
#'   [group_map()] and [group_modify()] for per-shard application, and
#'   [collect()] to materialize the full result.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#'
#' # Row count without materializing the result.
#' collect_chunked(tbl(f), function(acc, chunk) acc + nrow(chunk), .init = 0L)
#'
#' # Accumulate the normal-equation pieces X'X and X'y for an exact OLS fit
#' # of mpg ~ wt + hp, in one streaming pass.
#' acc <- collect_chunked(
#'   tbl(f) |> select(mpg, wt, hp),
#'   function(acc, chunk) {
#'     X <- cbind(1, chunk$wt, chunk$hp)
#'     y <- chunk$mpg
#'     list(XtX = acc$XtX + crossprod(X), Xty = acc$Xty + crossprod(X, y))
#'   },
#'   .init = list(XtX = matrix(0, 3, 3), Xty = matrix(0, 3, 1))
#' )
#' solve(acc$XtX, acc$Xty)            # same as coef(lm(mpg ~ wt + hp, mtcars))
#' unlink(f)
#'
#' @export
collect_chunked <- function(x, f, .init = NULL, combine = NULL,
                            commutative = FALSE) {
  UseMethod("collect_chunked")
}

#' @rdname collect_chunked
#' @export
collect_chunked.default <- function(x, f, .init = NULL, combine = NULL,
                                    commutative = FALSE) {
  stop("`x` must be a vectra_node or a vectra_partition (build one with tbl(), ",
       "tbl_csv(), offload(by = ...), ...)")
}

#' @rdname collect_chunked
#' @export
collect_chunked.vectra_node <- function(x, f, .init = NULL, combine = NULL,
                                        commutative = FALSE) {
  if (!is.function(f))
    stop("`f` must be a function of (acc, chunk)")
  nxt <- .batch_cursor(x)
  acc <- .init
  repeat {
    chunk <- nxt()
    if (is.null(chunk)) break
    acc <- f(acc, chunk)
  }
  acc
}

#' Turn a query into a resettable chunk generator
#'
#' Wraps a query so a pull-based consumer can read it one chunk at a time and
#' re-read it from the start as many times as needed. The returned closure
#' follows the `data(reset)` protocol that `biglm::bigglm()` expects: called
#' with `reset = TRUE` it rewinds to the beginning of the data, and called with
#' `reset = FALSE` it returns the next chunk as a data.frame, or `NULL` once the
#' data is exhausted. This lets `bigglm()` fit a generalized linear model on a
#' dataset larger than RAM, streaming each iteratively reweighted pass through
#' the engine without ever holding the full design matrix.
#'
#' Because a vectra node is consumed as it streams, re-reading requires a fresh
#' node on each pass. `chunk_feeder()` accepts either form: a *factory*, a
#' function of no arguments that returns a new node each time it is called; or an
#' offloaded node from [offload()], which is backed by a file and replays from
#' disk directly. On every `reset = TRUE` a fresh stream is started, so the same
#' query is replayed on each pass.
#'
#' Prefer feeding an [offload()] of the prepared query: the pipeline (scan,
#' joins, mutate) runs once into the spill, and every reweighted pass is then a
#' disk scan of the prepared columns rather than a re-run of the pipeline.
#'
#' @param .source Either a function of no arguments returning a fresh
#'   `vectra_node` each time it is called (e.g. `function() tbl_csv("occ.csv") |>
#'   select(presence, bio1, bio12)`), or an offloaded node from [offload()].
#'   Every chunk must contain all variables the consumer's formula references.
#'
#' @return A function `function(reset = FALSE)`. With `reset = TRUE` it rewinds
#'   and returns `invisible(NULL)`; with `reset = FALSE` it returns the next
#'   chunk as a data.frame, or `NULL` at end of stream.
#'
#' @seealso [offload()] for the replay cache, and [collect_chunked()] for
#'   single-pass reductions that vectra drives.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#'
#' feed <- chunk_feeder(function() tbl(f) |> select(mpg, wt, hp))
#' feed(reset = TRUE)       # rewind to the start of the stream
#' first <- feed()          # first chunk as a data.frame
#' head(first)
#'
#' \donttest{
#' # Out-of-core GLM: prepare once with offload(), then bigglm() replays it.
#' if (requireNamespace("biglm", quietly = TRUE)) {
#'   s <- offload(tbl(f) |> select(mpg, wt, hp))
#'   fit <- biglm::bigglm(mpg ~ wt + hp, data = chunk_feeder(s),
#'                        family = gaussian())
#'   coef(fit)
#' }
#' }
#' unlink(f)
#'
#' @export
chunk_feeder <- function(.source) {
  factory <-
    if (is.function(.source)) {
      .source
    } else if (inherits(.source, "vectra_offload")) {
      nd <- .source; force(nd)
      function() tbl(nd$.path)        # replay the spill from disk
    } else {
      stop("`.source` must be a function returning a fresh vectra_node, or an ",
           "offloaded node from offload() (which replays from disk)")
    }
  nxt <- NULL
  function(reset = FALSE) {
    if (reset) {
      nxt <<- .batch_cursor(factory())
      return(invisible(NULL))
    }
    if (is.null(nxt)) nxt <<- .batch_cursor(factory())
    nxt()
  }
}
