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
#' @seealso [mop()] for the raster transferability surface built on this
#'   primitive, [spatial_knn()] for geographic nearest neighbours.
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

# -- mop() raster wrapper -----------------------------------------------------

# Resolve `vars` (band names, band indices, or NULL for all) to integer band
# indices shared by g and m (m's bands are assumed to align to g's by position).
.mop_bands <- function(vars, gr, nb) {
  if (is.null(vars)) return(seq_len(nb))
  if (is.numeric(vars)) {
    b <- as.integer(vars)
    if (any(b < 1L | b > nb))
      stop("`vars` band index out of range")
    return(b)
  }
  if (is.character(vars)) {
    gn <- gr$band_names
    if (is.null(gn))
      stop("`g` has no band names; pass `vars` as band indices")
    b <- match(vars, gn)
    if (anyNA(b))
      stop(sprintf("band name(s) not found in `g`: %s",
                   paste(vars[is.na(b)], collapse = ", ")))
    return(as.integer(b))
  }
  stop("`vars` must be band indices, band names, or NULL (all bands)")
}

# Stream the calibration raster m tile-strip by tile-strip: accumulate the
# complete-case reference cloud and the per-band calibration range (min/max over
# all non-NA cells) that the NAC layers test against.
.mop_reference <- function(mr, bands) {
  W <- as.integer(mr$width); H <- as.integer(mr$height)
  TS <- max(1L, as.integer(mr$tile_size))
  nv <- length(bands)
  vmin <- rep(Inf, nv); vmax <- rep(-Inf, nv)
  chunks <- list()
  tiles_y <- (H + TS - 1L) %/% TS
  for (ty in seq_len(tiles_y) - 1L) {
    r0 <- ty * TS + 1L; r1 <- min(r0 + TS - 1L, H); out_h <- r1 - r0 + 1L
    sm <- matrix(NA_real_, out_h * W, nv)
    for (b in seq_len(nv))
      sm[, b] <- as.vector(vec_read_window(mr, band = bands[b],
                                           cols = c(1L, W), rows = c(r0, r1)))
    for (b in seq_len(nv)) {
      col <- sm[, b]; col <- col[!is.na(col)]
      if (length(col)) {
        vmin[b] <- min(vmin[b], min(col))
        vmax[b] <- max(vmax[b], max(col))
      }
    }
    ok <- stats::complete.cases(sm)
    if (any(ok)) chunks[[length(chunks) + 1L]] <- sm[ok, , drop = FALSE]
  }
  if (!length(chunks))
    stop("calibration raster `m` has no complete cells")
  list(mat = do.call(rbind, chunks), vmin = vmin, vmax = vmax)
}

#' MOP transferability / novelty surface between two environmental rasters
#'
#' Computes the mobility-oriented parity (MOP) diagnostic of Owens et al. (2013)
#' between a projection raster `g` and a calibration raster `m`, both multi-band
#' environmental layers with one band per predictor. It returns, aligned to the
#' projection grid, both halves of `mop::mop(type = "detailed")`:
#'
#' * `mop_distance` -- the continuous MOP surface: per `g` cell, the mean
#'   distance in predictor space to the nearest `percentage`% (or nearest `k`) of
#'   the `m` cells, via [feature_knn()].
#' * The non-analogous-conditions (NAC) / strict-extrapolation layers, per cell:
#'   `towards_low` (predictors below their calibration minimum), `towards_high`
#'   (above their calibration maximum), `mop_simple` (their sum, the count of
#'   out-of-range predictors), and `mop_basic` (1 where any predictor is out of
#'   range, 0 otherwise).
#'
#' The calibration cloud `m` is read once into memory and indexed; the projection
#' raster `g` is walked one tile-row strip at a time and streamed to the sink, so
#' the projection side is out-of-core. `g` and `m` need not share a grid -- the
#' output follows `g`. When both `k` and `percentage` are given, `k` wins.
#'
#' `g` and `m` must be `.vec` rasters (or paths to them); bring GeoTIFFs onto the
#' `.vec` grid with [warp()] first.
#'
#' @param g,m `vectra_raster` handles or paths to `.vec` rasters: the projection
#'   region and the calibration region, with matching bands (predictors) in the
#'   same order.
#' @param vars Predictors to use: band names, band indices, or `NULL` (default,
#'   all bands). Names resolve against `g`'s band names.
#' @param percentage Percent of the calibration cloud averaged over for the
#'   distance surface. Default `10`.
#' @param k Absolute neighbour count for the distance surface. When given it
#'   overrides `percentage`.
#' @param metric `"euclidean"` (default) or `"mahalanobis"` (whitened by the
#'   calibration covariance).
#' @param path Optional output `.vec` path. When given the five-band result is
#'   streamed to disk and the opened [vec_open_raster()] handle is returned
#'   invisibly; when `NULL` a named list of five in-memory matrices is returned.
#' @param dtype Storage dtype for `.vec` output. Default `"f32"`.
#' @param nthreads Threads for the distance scan. `NULL` (default) uses all
#'   available (capped to two under `R CMD check`).
#' @param compression Compression effort for `.vec` output. Default `"fast"`.
#'
#' @return With `path = NULL`, a named list of five numeric matrices
#'   (`mop_distance`, `mop_basic`, `mop_simple`, `towards_low`, `towards_high`),
#'   each carrying `gt`, `extent`, and `crs` attributes and aligned to `g`. With
#'   `path` given, the written five-band `vectra_raster` handle (invisibly).
#'
#' @seealso [feature_knn()] for the underlying predictor-space kNN, [warp()] to
#'   bring rasters onto a shared grid, [rast_calc()] for cellwise raster algebra.
#'
#' @references Owens, H.L. et al. (2013) Constraints on interpretation of
#'   ecological niche models by limited environmental ranges on calibration
#'   areas. \emph{Ecological Modelling} 263:10-18.
#'
#' @examples
#' # Two-band calibration (m) and projection (g) rasters on a shared grid.
#' set.seed(1)
#' m1 <- matrix(rnorm(400, 10, 2), 20, 20)
#' m2 <- matrix(rnorm(400, 800, 40), 20, 20)
#' g1 <- m1 + 3               # projection shifted warmer/drier -> more novel
#' g2 <- m2 - 60
#' fm <- tempfile(fileext = ".vec"); fg <- tempfile(fileext = ".vec")
#' vec_write_raster(array(c(m1, m2), c(20, 20, 2)), fm, dtype = "f64",
#'                  extent = c(0, 0, 20, 20), band_names = c("bio1", "bio12"))
#' vec_write_raster(array(c(g1, g2), c(20, 20, 2)), fg, dtype = "f64",
#'                  extent = c(0, 0, 20, 20), band_names = c("bio1", "bio12"))
#'
#' out <- mop(fg, fm, percentage = 10)
#' names(out)
#' round(mean(out$mop_distance), 2)
#' unlink(c(fm, fg))
#'
#' @export
mop <- function(g, m, vars = NULL, percentage = 10, k = NULL,
                metric = c("euclidean", "mahalanobis"),
                path = NULL, dtype = "f32", nthreads = NULL,
                compression = c("fast", "balanced", "max")) {
  metric <- match.arg(metric)
  comp_code <- switch(match.arg(compression), fast = 0L, balanced = 1L, max = 2L)

  gh <- .zonal_open(g, "g"); mh <- .zonal_open(m, "m")
  on.exit({
    if (gh$close) try(vec_close_raster(gh$r), silent = TRUE)
    if (mh$close) try(vec_close_raster(mh$r), silent = TRUE)
  }, add = TRUE)
  gr <- gh$r; mr <- mh$r

  nb <- as.integer(gr$n_bands)
  if (as.integer(mr$n_bands) != nb)
    stop("`g` and `m` must have the same number of bands (predictors)")
  bands <- .mop_bands(vars, gr, nb)
  nv <- length(bands)

  ref    <- .mop_reference(mr, bands)
  refmat <- ref$mat
  if (!is.null(k)) percentage <- NULL          # k overrides percentage
  keff   <- .feature_keff(k, percentage, nrow(refmat))
  idx    <- .Call(C_feature_knn_build, refmat, .feature_transform(refmat, metric))
  nt     <- .feature_nthreads(nthreads)

  W <- as.integer(gr$width); H <- as.integer(gr$height); gt <- as.numeric(gr$gt)
  TS <- max(1L, as.integer(gr$tile_size))
  epsg <- if (!is.null(gr$epsg)) as.integer(gr$epsg) else 0L
  bnames <- c("mop_distance", "mop_basic", "mop_simple",
              "towards_low", "towards_high")
  sink <- .raster_sink(W, H, 5L, gt, epsg, TS, path, dtype, bnames, comp_code)

  tiles_y <- (H + TS - 1L) %/% TS
  for (ty in seq_len(tiles_y) - 1L) {
    r0 <- ty * TS + 1L; r1 <- min(r0 + TS - 1L, H); out_h <- r1 - r0 + 1L
    ncell <- out_h * W
    qm <- matrix(NA_real_, ncell, nv)
    for (b in seq_len(nv))
      qm[, b] <- as.vector(vec_read_window(gr, band = bands[b],
                                           cols = c(1L, W), rows = c(r0, r1)))

    dist <- rep(NA_real_, ncell)
    ok <- stats::complete.cases(qm)
    if (any(ok))
      dist[ok] <- .Call(C_feature_knn_query, idx, qm[ok, , drop = FALSE],
                        keff, nt)

    below  <- qm < matrix(ref$vmin, ncell, nv, byrow = TRUE)
    above  <- qm > matrix(ref$vmax, ncell, nv, byrow = TRUE)
    tlow   <- rowSums(below)
    thigh  <- rowSums(above)
    simple <- tlow + thigh
    basic  <- as.numeric(simple > 0)

    os <- cbind(matrix(dist,   out_h, W),
                matrix(basic,  out_h, W),
                matrix(simple, out_h, W),
                matrix(tlow,   out_h, W),
                matrix(thigh,  out_h, W))
    sink$write(ty, r0, r1, os)
  }

  out <- sink$finish()
  if (!is.null(path)) return(invisible(out))
  stats::setNames(out, bnames)
}
