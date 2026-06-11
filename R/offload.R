# offload.R - disk-backed streaming: the offload functor.
#
# collect_chunked() and chunk_feeder() consume a query with a bounded in-RAM
# accumulator (one pass, O(1) state). offload() lifts that into the regime where
# the state itself spills to disk. It is an endofunctor on query streams that is
# the identity on values and changes only the memory and I/O profile.
#
# One front door, two return shapes:
#   offload(x)            -> a replay-cache node (same rows as x, now on disk,
#                            replayed by a disk scan instead of a pipeline re-run)
#   offload(x, by = ...)  -> a partition: disjoint per-key shards, so a model
#                            whose rows couple within a group becomes a set of
#                            independent per-shard fits.
# The external-sort instance is the existing arrange() (already disk-backed).

.PARTITION_BUDGET <- 1e6        # rows buffered before a routing flush

# -- cost grade ---------------------------------------------------------------

new_offload_grade <- function(tier, passes, peak, io, note = NULL) {
  structure(list(tier = tier, passes = passes, peak = peak, io = io,
                 note = note), class = "offload_grade")
}

#' @export
print.offload_grade <- function(x, ...) {
  cat(sprintf("<offload grade: %s>\n", x$tier))
  cat(sprintf("  passes over data : %s\n", x$passes))
  cat(sprintf("  peak memory      : %s\n", x$peak))
  cat(sprintf("  I/O cost         : %s\n", x$io))
  if (!is.null(x$note)) cat(sprintf("  note             : %s\n", x$note))
  invisible(x)
}

.scan_grade <- function()
  new_offload_grade("streaming scan", "1 per consumption (lazy)",
                    "O(one batch)", "O(n) per pass",
                    "plain query node; re-reading re-runs the upstream pipeline")

# Internal: the grade of any stream (offloaded or plain). Surfaced through
# print() and explain(), not as a separate exported verb.
grade_of <- function(x) {
  if (inherits(x, "offload_grade")) return(x)
  g <- attr(x, ".grade", exact = TRUE)
  if (is.null(g) && is.list(x)) g <- x$.grade
  if (is.null(g)) .scan_grade() else g
}

# -- offload(): replay cache and partition ------------------------------------

.replay_node <- function(spill, durable) {
  node <- tbl(spill)
  node$.grade <- new_offload_grade(
    tier = "replay cache",
    passes = "1 to spill, then O(1) re-reads",
    peak = "O(one batch)",
    io = "O(n) to write, O(n) per replay",
    note = if (durable) "durable spill"
           else "temp spill, removed when the node is garbage-collected")
  class(node) <- c("vectra_offload", "vectra_node")
  if (!durable) {
    reg <- new.env(parent = emptyenv())
    reg$path <- spill
    reg.finalizer(reg, function(e) try(unlink(e$path), silent = TRUE),
                  onexit = TRUE)
    node$.reg <- reg
  }
  node
}

#' Spill a query to disk and stream it back (the offload functor)
#'
#' Materializes a query once to disk and returns a stream that holds the same
#' rows, so every later pass is a disk scan instead of a re-run of the upstream
#' pipeline. The materialization streams batch by batch, so peak memory stays at
#' one batch regardless of result size. This is the bridge from the bounded
#' single-pass world of [collect_chunked()] to out-of-core fits.
#'
#' With no `by`, `offload()` returns a **replay cache**: a `vectra_node` backed
#' by one `.vtr` file. Feed it to a pull-based consumer such as
#' `biglm::bigglm()` through [chunk_feeder()], which accepts an offloaded node
#' directly, so each iteratively reweighted pass reads the prepared columns from
#' disk rather than rebuilding them. Bake the selects and mutates into the query
#' you offload, and replay does no further work.
#'
#' With `by`, `offload()` returns a **partition**: the rows split into disjoint
#' shards, one per key value (discrete key) or per value range (`method =
#' "range"`, or any numeric key), written in a single streaming pass. A
#' partition prints as a list of shards and behaves like one: `length()`,
#' `names()` (the keys), `p[["key"]]` (a shard node), and `lapply(p, ...)` all
#' work. Fold it with [collect_chunked()] (supplying `combine`). The union of
#' the shards reproduces the input; row totals are checked.
#'
#' @param x A `vectra_node` to materialize.
#' @param by Optional name (string) of a partition key column. When supplied,
#'   the result is a partition rather than a single node.
#' @param n Number of buckets for `method = "range"` or `"hash"`. Ignored for a
#'   one-shard-per-value partition.
#' @param method Partition strategy: `"auto"` (default; one shard per value for
#'   a discrete key, `n` ranges for a numeric key), `"level"` (one shard per
#'   distinct value), `"range"` (`n` equal-width value ranges), or `"hash"`
#'   (`n` buckets by a stable hash of the key, co-locating each key).
#' @param path Optional file path for a durable replay-cache spill (used only
#'   when `by` is `NULL`). When `NULL` a temporary file is used and removed when
#'   the returned node is garbage-collected.
#' @param compress Compression for spill files, passed to [write_vtr()]:
#'   `"fast"` (default), `"small"`, or `"none"`.
#'
#' @return A `vectra_node` (no `by`) or a `vectra_partition` (with `by`), each
#'   carrying a cost grade shown by [print()] and [explain()].
#'
#' @seealso [chunk_feeder()] (accepts an offloaded node), [collect_chunked()]
#'   for the partitioned monoidal reduce, and [arrange()] for the external-sort
#'   instance.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#'
#' # Replay cache: same rows, now on disk.
#' s <- offload(tbl(f) |> filter(cyl > 4) |> select(mpg, wt, hp))
#' nrow(collect(s))
#'
#' # Partition by a key: a list of per-shard nodes.
#' p <- offload(tbl(f), by = "cyl")
#' names(p)
#' length(p)
#' nrow(collect(p[[1]]))
#' unlink(f)
#'
#' @export
offload <- function(x, by = NULL, n = NULL,
                    method = c("auto", "level", "range", "hash"),
                    path = NULL, compress = c("fast", "small", "none")) {
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (build one with tbl(), tbl_csv(), ...)")
  method <- match.arg(method)
  compress <- match.arg(compress)

  if (is.null(by)) {
    durable <- !is.null(path)
    spill <- if (durable) normalizePath(path, mustWork = FALSE)
             else tempfile(fileext = ".vtr")
    write_vtr(x, spill, compress = compress)
    return(.replay_node(spill, durable))
  }

  if (!is.character(by) || length(by) != 1)
    stop("`by` must be a single column name (string)")

  spill <- tempfile(fileext = ".vtr")
  on.exit(unlink(spill), add = TRUE)        # intermediate; shards are run-files
  write_vtr(x, spill, compress = compress)

  schema <- .Call(C_node_schema, tbl(spill)$.node)
  if (!(by %in% schema$name))
    stop(sprintf("column '%s' not found in the query", by))
  is_double <- identical(schema$type[match(by, schema$name)], "double")

  if (method == "auto") method <- if (is_double) "range" else "level"
  if (method %in% c("range", "hash") && is.null(n))
    n <- if (method == "range") 8L else 16L

  budget <- getOption("vectra.partition_budget", .PARTITION_BUDGET)
  spec <- .partition_spec(spill, by, method, n)
  res <- .partition_router(spill, spec$assign, budget)

  labels <- spec$order(names(res$runs))
  shards <- lapply(labels, function(lab) .concat_runs(res$runs[[lab]]))
  names(shards) <- labels
  counts <- vapply(labels, function(lab) res$counts[[lab]], numeric(1))

  total <- res$n
  if (sum(counts) != total)
    stop(sprintf("partition lost rows: %g in shards vs %g in input",
                 sum(counts), total))

  all_runs <- unlist(res$runs, use.names = FALSE)
  reg <- new.env(parent = emptyenv())
  reg$paths <- all_runs
  reg.finalizer(reg, function(e) try(unlink(e$paths), silent = TRUE),
                onexit = TRUE)

  p <- shards
  attr(p, "by") <- by
  attr(p, ".counts") <- stats::setNames(counts, labels)
  attr(p, ".reg") <- reg
  attr(p, ".grade") <- new_offload_grade(
    tier = sprintf("partition by '%s' (%s)", by, method),
    passes = "1 spill + 1 routing pass",
    peak = sprintf("O(routing budget = %g rows), or O(one shard) when collected",
                   budget),
    io = "O(n)",
    note = "localizes coupling so each shard fits in RAM")
  class(p) <- c("vectra_partition", "list")
  p
}

# Build the per-strategy row-to-label assignment and a label ordering.
.partition_spec <- function(spill, by, method, n) {
  if (method == "level") {
    return(list(
      assign = function(chunk) {
        k <- chunk[[by]]
        lab <- as.character(k); lab[is.na(k)] <- "<NA>"; lab
      },
      order = function(labs) {
        non_na <- sort(setdiff(labs, "<NA>"))
        c(non_na, intersect("<NA>", labs))
      }))
  }
  if (method == "hash") {
    return(list(
      assign = function(chunk) {
        k <- chunk[[by]]
        lab <- sprintf("hash %d", as.integer(.hash_chr(as.character(k)) %% n))
        lab[is.na(k)] <- "<NA>"; lab
      },
      order = function(labs) {
        h <- grep("^hash ", labs, value = TRUE)
        h <- h[order(as.integer(sub("^hash ", "", h)))]
        c(h, intersect("<NA>", labs))
      }))
  }
  # range
  mm <- collect_chunked(
    tbl(spill),
    function(a, ch) {
      v <- ch[[by]]; v <- v[!is.na(v)]
      if (!length(v)) return(a)
      c(min(a[1], min(v)), max(a[2], max(v)))
    },
    .init = c(Inf, -Inf))
  lo <- mm[1]; hi <- mm[2]
  if (!is.finite(lo))
    stop(sprintf("cannot range-partition: '%s' has no non-NA values", by))
  breaks <- if (lo == hi) c(lo, hi) else seq(lo, hi, length.out = n + 1L)
  nb <- length(breaks) - 1L
  range_lab <- vapply(seq_len(nb), function(i)
    sprintf("[%g, %g%s", breaks[i], breaks[i + 1L],
            if (i < nb) ")" else "]"), character(1))
  list(
    assign = function(chunk) {
      v <- chunk[[by]]
      i <- findInterval(v, breaks, rightmost.closed = TRUE, all.inside = TRUE)
      lab <- range_lab[i]; lab[is.na(v)] <- "<NA>"; lab
    },
    order = function(labs) c(intersect(range_lab, labs),
                             intersect("<NA>", labs)))
}

# Stable per-value hash of a character vector (value-stable across batches).
.hash_chr <- function(s) {
  u <- unique(s)
  hu <- vapply(u, function(x) {
    if (is.na(x)) return(0)
    cs <- utf8ToInt(x); h <- 0
    for (c in cs) h <- (h * 131 + c) %% 2147483629
    h
  }, numeric(1))
  unname(hu[match(s, u)])
}

# One streaming pass: route each batch's rows to per-label buffers, flush to a
# run-file when the buffered row count crosses the budget. Each label ends as a
# set of run-files (a lazy concat downstream). Bounded by the budget.
.partition_router <- function(spill, assign, budget) {
  st <- new.env(parent = emptyenv())
  st$buffers <- list(); st$runs <- list(); st$counts <- list()
  st$buffered <- 0; st$n <- 0

  flush_one <- function(label) {
    bufs <- st$buffers[[label]]
    if (is.null(bufs) || !length(bufs)) return(invisible())
    df <- if (length(bufs) == 1) bufs[[1]] else do.call(rbind, bufs)
    rf <- tempfile(fileext = ".vtr")
    write_vtr(df, rf)
    st$runs[[label]] <- c(st$runs[[label]], rf)
    st$counts[[label]] <- (st$counts[[label]] %||% 0) + nrow(df)
    st$buffers[[label]] <- NULL
  }
  flush_all <- function() {
    for (lab in names(st$buffers)) flush_one(lab)
    st$buffered <- 0
  }

  nxt <- .batch_cursor(tbl(spill))
  repeat {
    chunk <- nxt(); if (is.null(chunk)) break
    st$n <- st$n + nrow(chunk)
    idx <- split(seq_len(nrow(chunk)), assign(chunk))
    for (lab in names(idx))
      st$buffers[[lab]] <- c(st$buffers[[lab]],
                             list(chunk[idx[[lab]], , drop = FALSE]))
    st$buffered <- st$buffered + nrow(chunk)
    if (st$buffered >= budget) flush_all()
  }
  flush_all()
  list(runs = st$runs, counts = st$counts, n = st$n)
}

`%||%` <- function(a, b) if (is.null(a)) b else a

.concat_runs <- function(paths) {
  nodes <- lapply(paths, tbl)
  if (length(nodes) == 1) nodes[[1]] else do.call(bind_rows, nodes)
}

#' @export
print.vectra_partition <- function(x, ...) {
  by <- attr(x, "by"); counts <- attr(x, ".counts")
  cat(sprintf("<vectra partition: %d shards by '%s'>\n", length(x), by))
  for (lab in names(x))
    cat(sprintf("  %-18s %g rows\n", lab, counts[[lab]]))
  print(grade_of(x))
  invisible(x)
}

# -- monoidal reduce over a partition -----------------------------------------

#' @rdname collect_chunked
#' @export
collect_chunked.vectra_partition <- function(x, f, .init = NULL,
                                             combine = NULL,
                                             commutative = FALSE) {
  if (!is.function(f))
    stop("`f` must be a function of (acc, chunk)")
  if (is.null(combine))
    stop("folding a partition needs `combine` to merge per-shard accumulators; ",
         "pass combine = (a monoid: `.init` is its identity, `combine` its op)")
  if (!is.function(combine))
    stop("`combine` must be a function of (acc, acc)")
  if (length(x) == 0) return(.init)
  accs <- lapply(unclass(x), function(nd)
    collect_chunked(nd, f, .init = .init))
  Reduce(combine, accs)
}

# -- per-shard application ----------------------------------------------------

#' Apply a function to each shard of a partition
#'
#' Run a function once per shard of a partition (`offload(x, by = ...)`) and
#' gather the results. Each shard is read into memory as a data.frame and passed
#' to `.f` together with its key, so a model that couples rows within a group
#' becomes a set of independent per-shard fits. This is the per-group
#' counterpart to [collect_chunked()], which instead merges every shard into a
#' single accumulator.
#'
#' `group_map()` returns a named list, one element per shard keyed by the shard
#' key, and places no constraint on what `.f` returns. Use it for per-group
#' results that do not rebind into a table, such as fitted models.
#'
#' `group_modify()` expects `.f` to return a data.frame for each shard and binds
#' those frames into one. When a shard's result does not already carry the
#' partition key column, the key is added as a leading column (named after the
#' partition's `by`), so every row records the shard it came from. Use it for
#' per-group summaries that recombine into a single table.
#'
#' Each shard is materialized in full before `.f` sees it, so partition the
#' query on a key whose groups fit in memory. For a reduction that stays bounded
#' without ever holding a whole group, fold the partition with
#' [collect_chunked()] instead.
#'
#' @param .data A `vectra_partition` from [offload()] with a `by` key.
#' @param .f A function applied to each shard. It receives the shard as a
#'   data.frame and the shard key (a string) as its first two arguments; any
#'   further arguments in `...` follow. A purrr-style formula such as
#'   `~ lm(y ~ x, .x)` also works, with `.x` the shard data and `.y` the key.
#'   For `group_modify()`, `.f` must return a data.frame.
#' @param ... Additional arguments passed on to `.f`.
#'
#' @return `group_map()` returns a named list with one element per shard.
#'   `group_modify()` returns a single data.frame: the per-shard results
#'   row-bound, with the shard key restored as a column when `.f` dropped it.
#'
#' @seealso [offload()] to build a partition, and [collect_chunked()] for the
#'   partitioned monoidal reduce.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' p <- offload(tbl(f), by = "cyl")
#'
#' # One fit per shard, returned as a named list keyed by cyl.
#' fits <- group_map(p, function(d, cyl) coef(lm(mpg ~ wt, data = d)))
#' fits
#'
#' # Per-shard summaries recombined into one table, key restored as a column.
#' group_modify(p, function(d, cyl)
#'   data.frame(n = nrow(d), mean_mpg = mean(d$mpg)))
#' unlink(f)
#'
#' @export
group_map <- function(.data, .f, ...) {
  UseMethod("group_map")
}

#' @rdname group_map
#' @export
group_map.vectra_partition <- function(.data, .f, ...) {
  .f <- rlang::as_function(.f)
  dots <- list(...)
  shards <- unclass(.data)
  Map(function(nd, key) do.call(.f, c(list(collect(nd), key), dots)),
      shards, names(shards))
}

#' @rdname group_map
#' @export
group_modify <- function(.data, .f, ...) {
  UseMethod("group_modify")
}

#' @rdname group_map
#' @export
group_modify.vectra_partition <- function(.data, .f, ...) {
  .f <- rlang::as_function(.f)
  dots <- list(...)
  by <- attr(.data, "by")
  shards <- unclass(.data)
  parts <- Map(function(nd, key) {
    res <- do.call(.f, c(list(collect(nd), key), dots))
    if (!is.data.frame(res))
      stop("`.f` must return a data.frame for each shard (group_modify)")
    if (!(by %in% names(res))) {
      keycol <- stats::setNames(
        data.frame(rep(key, nrow(res)), stringsAsFactors = FALSE), by)
      res <- cbind(keycol, res)
    }
    res
  }, shards, names(shards))
  out <- do.call(rbind, unname(parts))
  rownames(out) <- NULL
  out
}
