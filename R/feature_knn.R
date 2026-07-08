# feature_knn.R - feature-space (attribute-space) k-nearest-neighbour and the
# MOP (mobility-oriented parity) transferability surface built on top of it.
#
# spatial_knn() finds nearest neighbours in geographic space (GEOS distance over
# coordinates). feature_knn() finds them in *predictor* space: each streamed row
# is a point in n-variable environmental space, and the neighbours are drawn from
# a resident reference cloud in that same space. The query side streams one batch
# at a time (the resident-y pattern shared with the spatial verbs); the reference
# cloud is materialized once, whitened for the chosen metric, and indexed in C.

# -- shared internals ---------------------------------------------------------

# Coerce the reference cloud (data.frame or matrix) to a clean numeric matrix
# over `vars`, dropping rows with any NA. Column order follows `vars`.
.feature_ref_matrix <- function(y, vars) {
  if (is.data.frame(y)) {
    miss <- setdiff(vars, names(y))
    if (length(miss))
      stop(sprintf("reference cloud is missing column(s): %s",
                   paste(miss, collapse = ", ")))
    m <- as.matrix(y[, vars, drop = FALSE])
  } else if (is.matrix(y)) {
    if (!is.null(colnames(y))) {
      miss <- setdiff(vars, colnames(y))
      if (length(miss))
        stop(sprintf("reference cloud is missing column(s): %s",
                     paste(miss, collapse = ", ")))
      m <- y[, vars, drop = FALSE]
    } else {
      if (ncol(y) != length(vars))
        stop("reference cloud matrix has no column names and its column count ",
             "does not match `vars`")
      m <- y
    }
  } else {
    stop("`y` must be a data.frame or numeric matrix (the reference cloud)")
  }
  storage.mode(m) <- "double"
  m <- m[stats::complete.cases(m), , drop = FALSE]
  if (!nrow(m))
    stop("reference cloud has no complete (non-NA) rows")
  m
}

# Whitening transform for the chosen metric. Euclidean needs none (NULL).
# Mahalanobis returns R with R'R = solve(cov(ref)); distance in the R-whitened
# space equals Mahalanobis distance in the original space, so one Euclidean
# kernel serves both. Column-major R is what the C side multiplies each point by.
.feature_transform <- function(refmat, metric) {
  if (metric == "euclidean") return(NULL)
  S <- stats::cov(refmat)
  Si <- tryCatch(solve(S), error = function(e)
    stop("Mahalanobis metric: reference covariance is singular ",
         "(a predictor is constant or collinear over the reference cloud)",
         call. = FALSE))
  tryCatch(chol(Si), error = function(e)
    stop("Mahalanobis metric: inverse covariance is not positive definite",
         call. = FALSE))
}

# Resolve the neighbour count k_eff from exactly one of `k` (absolute) or
# `percentage` (a fraction of the reference cloud), capped to n_ref.
.feature_keff <- function(k, percentage, n_ref) {
  has_k <- !is.null(k)
  has_p <- !is.null(percentage)
  if (has_k == has_p)
    stop("supply exactly one of `k` or `percentage`")
  if (has_k) {
    if (!is.numeric(k) || length(k) != 1L || !is.finite(k) || k < 1)
      stop("`k` must be a single positive integer")
    return(as.integer(min(k, n_ref)))
  }
  if (!is.numeric(percentage) || length(percentage) != 1L ||
      !is.finite(percentage) || percentage <= 0 || percentage > 100)
    stop("`percentage` must be a single number in (0, 100]")
  as.integer(max(1, min(n_ref, ceiling(percentage / 100 * n_ref))))
}

# 0L tells the C side to use omp_get_max_threads().
.feature_nthreads <- function(nthreads) {
  if (is.null(nthreads)) return(0L)
  if (!is.numeric(nthreads) || length(nthreads) != 1L ||
      !is.finite(nthreads) || nthreads < 1)
    stop("`nthreads` must be NULL or a single positive integer")
  as.integer(nthreads)
}

# -- feature_knn front door ---------------------------------------------------

#' Nearest neighbours of a streamed layer in predictor space
#'
#' Streams a query side `x` through the engine and, for each row, returns the
#' mean distance to its nearest neighbours in a resident reference cloud `y` --
#' where distance is measured in *predictor* (attribute) space, not on
#' coordinates. This is the continuous half of the MOP transferability metric
#' (Owens et al. 2013): the mean environmental distance from a projection cell to
#' the nearest part of a calibration cloud. Where [spatial_knn()] measures
#' geographic proximity with GEOS, `feature_knn()` measures environmental
#' novelty with an L2 or Mahalanobis distance in n-variable space.
#'
#' The reference cloud is materialized once, whitened for the chosen metric, and
#' held resident; the query side streams one batch at a time, so the projection
#' side can exceed memory while peak memory stays at one batch plus the resident
#' cloud. The neighbour count is either an absolute `k` or the nearest
#' `percentage`% of the cloud (`ceil(percentage/100 * nrow(y))`), matching MOP's
#' proportion-of-reference formulation.
#'
#' @param x A `vectra_node` (from [tbl()], [tbl_csv()], ...): the streamed query
#'   side. Its predictor columns are read one batch at a time.
#' @param y A data.frame or numeric matrix: the resident reference cloud, one
#'   column per predictor. Rows with any `NA` are dropped.
#' @param vars Character vector of predictor column names present in both `x` and
#'   `y`. Defaults to the column names of `y`.
#' @param k Number of nearest neighbours to average over. Supply exactly one of
#'   `k` or `percentage`.
#' @param percentage Percent of the reference cloud to average over (the nearest
#'   `ceil(percentage/100 * nrow(y))` neighbours). Supply exactly one of `k` or
#'   `percentage`.
#' @param metric `"euclidean"` (default) for straight L2 distance in predictor
#'   units, or `"mahalanobis"` for distance whitened by the reference cloud's
#'   covariance.
#' @param dist_col Name of the appended distance column. Default
#'   `"knn_distance"`.
#' @param nthreads Threads for the per-batch distance scan. `NULL` (default) uses
#'   all available (capped to two under `R CMD check`).
#' @param flush_rows Rows to buffer before spilling a run file. `NULL` (default)
#'   spills by the streaming memory budget instead.
#'
#' @return A `vectra_node` of `x`'s columns plus `dist_col`, backed by temporary
#'   `.vtr` spills removed when the node is garbage-collected.
#'
#' @seealso [rast_feature_distance()] for the same distance computed out-of-core
#'   over a projection raster, [spatial_knn()] for geographic nearest neighbours.
#'
#' @references Owens, H.L. et al. (2013) Constraints on interpretation of
#'   ecological niche models by limited environmental ranges on calibration
#'   areas. \emph{Ecological Modelling} 263:10-18.
#'
#' @examples
#' set.seed(1)
#' ref <- data.frame(bio1 = rnorm(500, 10, 2), bio12 = rnorm(500, 800, 50))
#' qy  <- data.frame(bio1 = c(10, 20), bio12 = c(800, 400))
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(qy, f)
#'
#' # Row 1 sits in the cloud (small distance); row 2 is far outside it.
#' tbl(f) |>
#'   feature_knn(ref, percentage = 5) |>
#'   collect()
#' unlink(f)
#'
#' @export
feature_knn <- function(x, y, vars = NULL, k = NULL, percentage = NULL,
                        metric = c("euclidean", "mahalanobis"),
                        dist_col = "knn_distance", nthreads = NULL,
                        flush_rows = NULL) {
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed query side)")
  metric <- match.arg(metric)
  if (is.null(vars))
    vars <- if (is.data.frame(y)) names(y) else colnames(y)
  if (is.null(vars) || !is.character(vars) || !length(vars))
    stop("`vars` must be a character vector of predictor names (or `y` must ",
         "carry column names)")

  refmat <- .feature_ref_matrix(y, vars)
  keff   <- .feature_keff(k, percentage, nrow(refmat))
  idx    <- .Call(C_feature_knn_build, refmat, .feature_transform(refmat, metric))
  nt     <- .feature_nthreads(nthreads)

  nxt <- .batch_cursor(x)
  acc <- .run_accumulator(flush_rows)
  repeat {
    chunk <- nxt()
    if (is.null(chunk)) break
    miss <- setdiff(vars, names(chunk))
    if (length(miss))
      stop(sprintf("`x` is missing predictor column(s): %s",
                   paste(miss, collapse = ", ")))
    qm <- as.matrix(chunk[, vars, drop = FALSE])
    storage.mode(qm) <- "double"
    chunk[[dist_col]] <- .Call(C_feature_knn_query, idx, qm, keff, nt)
    acc$push(chunk)
  }
  acc$finish(crs = .resolve_crs(x, NA), empty_geom = dist_col)
}

# -- rast_feature_distance() raster wrapper -----------------------------------

# Resolve `vars` (band names, band indices, or NULL for all) to integer band
# indices into a raster.
.feature_bands <- function(vars, r, nb) {
  if (is.null(vars)) return(seq_len(nb))
  if (is.numeric(vars)) {
    b <- as.integer(vars)
    if (any(b < 1L | b > nb))
      stop("`vars` band index out of range")
    return(b)
  }
  if (is.character(vars)) {
    bn <- r$band_names
    if (is.null(bn))
      stop("raster has no band names; pass `vars` as band indices")
    b <- match(vars, bn)
    if (anyNA(b))
      stop(sprintf("band name(s) not found: %s",
                   paste(vars[is.na(b)], collapse = ", ")))
    return(as.integer(b))
  }
  stop("`vars` must be band indices, band names, or NULL (all bands)")
}

# Read a reference raster tile-strip by tile-strip into the complete-case
# reference cloud (one row per non-NA cell, one column per band).
.feature_raster_cloud <- function(r, bands) {
  W <- as.integer(r$width); H <- as.integer(r$height)
  TS <- max(1L, as.integer(r$tile_size))
  nv <- length(bands)
  chunks <- list()
  tiles_y <- (H + TS - 1L) %/% TS
  for (ty in seq_len(tiles_y) - 1L) {
    r0 <- ty * TS + 1L; r1 <- min(r0 + TS - 1L, H); out_h <- r1 - r0 + 1L
    sm <- matrix(NA_real_, out_h * W, nv)
    for (b in seq_len(nv))
      sm[, b] <- as.vector(vec_read_window(r, band = bands[b],
                                           cols = c(1L, W), rows = c(r0, r1)))
    ok <- stats::complete.cases(sm)
    if (any(ok)) chunks[[length(chunks) + 1L]] <- sm[ok, , drop = FALSE]
  }
  if (!length(chunks))
    stop("`reference` raster has no complete cells")
  do.call(rbind, chunks)
}

#' Predictor-space nearest-neighbour distance surface over a raster
#'
#' Computes, aligned to the grid of a projection raster `x`, a surface of the
#' mean distance in predictor space from each cell to the nearest cells of a
#' reference raster `reference`. Both are multi-band environmental rasters with
#' one band per predictor. This is the raster, out-of-core counterpart of
#' [feature_knn()]: the reference raster is read once into memory and indexed,
#' while `x` is walked one tile-row strip at a time and streamed to the sink, so
#' the projection side is larger-than-RAM. `x` and `reference` need not share a
#' grid -- the output follows `x`.
#'
#' The distance is the mean distance to the nearest `k`, or nearest
#' `percentage`% of the reference cells (`ceil(percentage/100 * N)`), under a
#' Euclidean or Mahalanobis metric. Supply exactly one of `k` or `percentage`.
#'
#' This is the streaming distance surface behind an environmental-novelty or
#' transferability diagnostic such as MOP (Owens et al. 2013). The strict
#' non-analogous-conditions layers (per-predictor out-of-range counts) are a
#' separate, already-native computation -- a per-band range reduce plus
#' [rast_calc()] -- and are shown alongside this surface in the
#' `vignette("sdm")`; this function is the continuous-distance piece only.
#'
#' `x` and `reference` must be `.vec` rasters (or paths to them); bring GeoTIFFs
#' onto the `.vec` grid with [warp()] first.
#'
#' @param x,reference `vectra_raster` handles or paths to `.vec` rasters: the
#'   raster to score and the reference raster, with matching bands (predictors)
#'   in the same order.
#' @param vars Predictors to use: band names, band indices, or `NULL` (default,
#'   all bands). Names resolve against `x`'s band names.
#' @param k Number of nearest reference cells to average over. Supply exactly
#'   one of `k` or `percentage`.
#' @param percentage Percent of the reference cells to average over (the nearest
#'   `ceil(percentage/100 * N)`). Supply exactly one of `k` or `percentage`.
#' @param metric `"euclidean"` (default) or `"mahalanobis"` (whitened by the
#'   reference covariance).
#' @param path Optional output `.vec` path. When given the surface is streamed to
#'   disk and the opened [vec_open_raster()] handle is returned invisibly; when
#'   `NULL` the surface is returned as an in-memory matrix.
#' @param dtype Storage dtype for `.vec` output. Default `"f32"`.
#' @param nthreads Threads for the distance scan. `NULL` (default) uses all
#'   available (capped to two under `R CMD check`).
#' @param compression Compression effort for `.vec` output. Default `"fast"`.
#'
#' @return With `path = NULL`, a numeric matrix on `x`'s grid (row 1 northmost)
#'   carrying `gt`, `extent`, and `crs` attributes. With `path` given, the
#'   written single-band `vectra_raster` handle (invisibly). Cells with any `NA`
#'   predictor come back `NA`.
#'
#' @seealso [feature_knn()] for the table-level primitive, [warp()] to bring
#'   rasters onto a shared grid, [rast_calc()] for cellwise raster algebra.
#'
#' @references Owens, H.L. et al. (2013) Constraints on interpretation of
#'   ecological niche models by limited environmental ranges on calibration
#'   areas. \emph{Ecological Modelling} 263:10-18.
#'
#' @examples
#' # Two-band reference and projection rasters.
#' set.seed(1)
#' r1 <- matrix(rnorm(400, 10, 2), 20, 20)
#' r2 <- matrix(rnorm(400, 800, 40), 20, 20)
#' x1 <- r1 + 3               # projection shifted warmer/drier -> more novel
#' x2 <- r2 - 60
#' fr <- tempfile(fileext = ".vec"); fx <- tempfile(fileext = ".vec")
#' vec_write_raster(array(c(r1, r2), c(20, 20, 2)), fr, dtype = "f64",
#'                  extent = c(0, 0, 20, 20), band_names = c("bio1", "bio12"))
#' vec_write_raster(array(c(x1, x2), c(20, 20, 2)), fx, dtype = "f64",
#'                  extent = c(0, 0, 20, 20), band_names = c("bio1", "bio12"))
#'
#' d <- rast_feature_distance(fx, fr, percentage = 10)
#' round(mean(d), 2)
#' unlink(c(fr, fx))
#'
#' @export
rast_feature_distance <- function(x, reference, vars = NULL, k = NULL,
                                  percentage = NULL,
                                  metric = c("euclidean", "mahalanobis"),
                                  path = NULL, dtype = "f32", nthreads = NULL,
                                  compression = c("fast", "balanced", "max")) {
  metric <- match.arg(metric)
  comp_code <- switch(match.arg(compression), fast = 0L, balanced = 1L, max = 2L)

  xh <- .zonal_open(x, "x"); rh <- .zonal_open(reference, "reference")
  on.exit({
    if (xh$close) try(vec_close_raster(xh$r), silent = TRUE)
    if (rh$close) try(vec_close_raster(rh$r), silent = TRUE)
  }, add = TRUE)
  xr <- xh$r; rr <- rh$r

  nb <- as.integer(xr$n_bands)
  if (as.integer(rr$n_bands) != nb)
    stop("`x` and `reference` must have the same number of bands (predictors)")
  bands <- .feature_bands(vars, xr, nb)
  nv <- length(bands)

  refmat <- .feature_raster_cloud(rr, bands)
  keff   <- .feature_keff(k, percentage, nrow(refmat))
  idx    <- .Call(C_feature_knn_build, refmat, .feature_transform(refmat, metric))
  nt     <- .feature_nthreads(nthreads)

  W <- as.integer(xr$width); H <- as.integer(xr$height); gt <- as.numeric(xr$gt)
  TS <- max(1L, as.integer(xr$tile_size))
  epsg <- if (!is.null(xr$epsg)) as.integer(xr$epsg) else 0L
  sink <- .raster_sink(W, H, 1L, gt, epsg, TS, path, dtype, "distance", comp_code)

  tiles_y <- (H + TS - 1L) %/% TS
  for (ty in seq_len(tiles_y) - 1L) {
    r0 <- ty * TS + 1L; r1 <- min(r0 + TS - 1L, H); out_h <- r1 - r0 + 1L
    ncell <- out_h * W
    qm <- matrix(NA_real_, ncell, nv)
    for (b in seq_len(nv))
      qm[, b] <- as.vector(vec_read_window(xr, band = bands[b],
                                           cols = c(1L, W), rows = c(r0, r1)))

    dist <- rep(NA_real_, ncell)
    ok <- stats::complete.cases(qm)
    if (any(ok))
      dist[ok] <- .Call(C_feature_knn_query, idx, qm[ok, , drop = FALSE],
                        keff, nt)

    sink$write(ty, r0, r1, matrix(dist, out_h, W))
  }

  out <- sink$finish()
  if (!is.null(path)) return(invisible(out))
  out[[1L]]
}
