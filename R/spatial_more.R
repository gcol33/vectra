# spatial_more.R - raster <-> vector toolbox ops on the streamed spine.
#
# These complete the vector<->raster round trip on top of the machinery in
# spatial.R: the strip sink (.raster_sink), the pixel-centre helper
# (.zonal_strip_xy), the run-file accumulator (.run_accumulator), and
# spatial_dissolve() for the by-value merge. No new C and no new VecType.
#
#   mask        raster x polygon -> raster (clip pixels to a layer)
#   rast_calc   aligned rasters  -> raster (cellwise map algebra)
#   mosaic      aligned rasters  -> raster (merge onto a common grid)
#   polygonize  raster           -> vector polygons (one per value)
#   contours    raster           -> vector iso-lines

# Build an sfc of axis-aligned rectangles from corner vectors, one polygon per
# entry. Used to turn raster cells / runs into polygon geometry.
.rects_sfc <- function(xmin, xmax, ymin, ymax, crs) {
  polys <- lapply(seq_along(xmin), function(i)
    sf::st_polygon(list(rbind(
      c(xmin[i], ymin[i]), c(xmax[i], ymin[i]), c(xmax[i], ymax[i]),
      c(xmin[i], ymax[i]), c(xmin[i], ymin[i])))))
  sf::st_sfc(polys, crs = crs)
}

# Build an sfc of two-point line segments from endpoint vectors.
.segments_sfc <- function(x1, y1, x2, y2, crs) {
  lines <- lapply(seq_along(x1), function(i)
    sf::st_linestring(rbind(c(x1[i], y1[i]), c(x2[i], y2[i]))))
  sf::st_sfc(lines, crs = crs)
}

# -- mask (clip a raster to a polygon layer) ----------------------------------

#' Mask a streamed raster to a polygon layer
#'
#' Keeps the pixels of a `.vec` raster whose cell centre falls inside a resident
#' polygon layer and sets the rest to `background`, reading the raster one
#' tile-row strip at a time so the whole grid is never resident. It is the raster
#' counterpart of [spatial_clip()]: the streamed side is the (large) raster and
#' the small `mask` layer stays in memory. With `inverse = TRUE` the inside is
#' cleared and the outside kept.
#'
#' This is the *monoid fold* tier of the spatial toolbox: bounded to one strip
#' plus the resident mask, a single streaming pass, no spill. A pixel is tested
#' against the mask only when its centre falls in the mask bounding box, so the
#' point-in-polygon work stays proportional to the overlap rather than the whole
#' grid. Point-in-polygon is delegated to \pkg{sf} (an optional dependency);
#' topology and CRS handling are \pkg{sf}'s.
#'
#' @param x A `vectra_raster` (from [vec_open_raster()]) or a path to a `.vec`
#'   raster.
#' @param mask An `sf` or `sfc` polygon layer to clip against. When it carries no
#'   CRS it inherits the raster's EPSG.
#' @param inverse If `FALSE` (default) keep pixels inside `mask`; if `TRUE` keep
#'   the pixels outside it.
#' @param band Band(s) to mask (1-based). Default `NULL` masks every band.
#' @param background Value written to cleared pixels. Default `NA_real_`.
#' @param path Optional output `.vec` path. When given the result is streamed to
#'   disk and the opened [vec_open_raster()] handle is returned invisibly; when
#'   `NULL` the result is returned in memory (a matrix for one band, a list of
#'   matrices for several).
#' @param dtype Storage dtype for `.vec` output (see [vec_write_raster()]).
#'   Default `"f32"`.
#' @param compression Compression effort for `.vec` output. Default `"fast"`.
#'
#' @return When `path` is `NULL`, a numeric matrix (one band) or a list of
#'   matrices (several), each carrying `gt`, `extent`, and `crs` attributes
#'   (row 1 northmost). When `path` is given, the written `vectra_raster` handle
#'   (invisibly).
#'
#' @seealso [spatial_clip()] for the vector analogue, [zonal()] for per-zone
#'   summaries over the same pixel-in-polygon assignment.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' vals <- matrix(1:100, 10, 10, byrow = TRUE)
#' f <- tempfile(fileext = ".vec")
#' vec_write_raster(vals, f, dtype = "f64", extent = c(0, 0, 10, 10))
#'
#' disc <- sf::st_buffer(sf::st_sfc(sf::st_point(c(5, 5))), 3)
#' inside <- mask(f, disc)
#' sum(!is.na(inside))
#' unlink(f)
#'
#' @export
mask <- function(x, mask, inverse = FALSE, band = NULL,
                 background = NA_real_, path = NULL, dtype = "f32",
                 compression = c("fast", "balanced", "max")) {
  .check_sf()
  if (!inherits(mask, "sf") && !inherits(mask, "sfc"))
    stop("`mask` must be an sf or sfc polygon layer")
  comp_code <- switch(match.arg(compression), fast = 0L, balanced = 1L, max = 2L)

  vh <- .zonal_open(x, "x"); r <- vh$r
  if (vh$close) on.exit(vec_close_raster(r), add = TRUE)
  W <- as.integer(r$width); H <- as.integer(r$height); gt <- r$gt
  TS <- max(1L, as.integer(r$tile_size))
  epsg <- if (!is.null(r$epsg)) as.integer(r$epsg) else 0L
  bands <- if (is.null(band)) seq_len(r$n_bands) else as.integer(band)
  nout <- length(bands)

  mcrs <- sf::st_crs(mask)
  if (is.na(mcrs) && epsg > 0L) mcrs <- sf::st_crs(epsg)
  mg <- sf::st_geometry(mask)
  if (is.na(sf::st_crs(mg)) && !is.na(mcrs)) mg <- sf::st_set_crs(mg, mcrs)
  mg <- sf::st_union(mg)
  bb <- sf::st_bbox(mg)

  bn <- if (!is.null(r$band_names)) r$band_names[bands] else NULL
  sink <- .raster_sink(W, H, nout, gt, epsg, TS, path, dtype, bn, comp_code)

  tiles_y <- (H + TS - 1L) %/% TS
  for (ty in seq_len(tiles_y) - 1L) {
    r0 <- ty * TS + 1L; r1 <- min(r0 + TS - 1L, H); out_h <- r1 - r0 + 1L
    xy <- .zonal_strip_xy(gt, r0, r1, W)
    inbb <- xy$x >= bb[["xmin"]] & xy$x <= bb[["xmax"]] &
            xy$y >= bb[["ymin"]] & xy$y <= bb[["ymax"]]
    covered <- logical(length(inbb))
    if (any(inbb)) {
      pts <- sf::st_as_sf(data.frame(x = xy$x[inbb], y = xy$y[inbb]),
                          coords = c("x", "y"), crs = mcrs)
      covered[inbb] <- lengths(sf::st_intersects(pts, mg)) > 0L
    }
    clear <- if (inverse) covered else !covered
    os_list <- vector("list", nout)
    for (k in seq_len(nout)) {
      v <- as.vector(vec_read_window(r, band = bands[k], cols = c(1L, W),
                                     rows = c(r0, r1)))
      v[clear] <- background
      os_list[[k]] <- matrix(v, out_h, W)
    }
    sink$write(ty, r0, r1, do.call(cbind, os_list))
  }
  out <- sink$finish()
  if (!is.null(path)) return(out)
  if (nout == 1L) out[[1L]] else out
}

# -- rast_calc (cellwise map algebra over aligned rasters) --------------------

#' Cellwise calculation over aligned rasters (map algebra)
#'
#' Evaluates an expression cell by cell across one or more `.vec` rasters that
#' share a grid, reading every input one tile-row strip at a time so no whole
#' band is ever resident. Inside `expr` each name in `rasters` refers to that
#' raster's strip as a numeric vector, so ordinary vectorised R expresses the
#' calculation: a band index `(nir - red) / (nir + red)`, a reclassification
#' `cut(dem, breaks)`, a threshold `ifelse(slope > 30, 1L, 0L)`, or arithmetic
#' across layers. The result is written one strip at a time to a single-band
#' output.
#'
#' This is the *monoid fold* tier of the spatial toolbox: bounded to one strip
#' per input, a single streaming pass, no spill. The rasters must share
#' dimensions and geotransform; [warp()] them onto a common grid first if they
#' do not. No \pkg{sf} is needed.
#'
#' @param rasters A named list of `vectra_raster` handles or `.vec` paths sharing
#'   a grid. The names are the variables available inside `expr`.
#' @param expr An expression in those names producing one value per cell (or a
#'   scalar, recycled). Evaluated against each strip with the caller's
#'   environment as the enclosing scope.
#' @param band Band read from every input (1-based). Default 1.
#' @param path Optional output `.vec` path. When given the result is streamed to
#'   disk and the opened [vec_open_raster()] handle is returned invisibly; when
#'   `NULL` the result is returned as an in-memory matrix.
#' @param dtype Storage dtype for `.vec` output (see [vec_write_raster()]).
#'   Default `"f32"`.
#' @param compression Compression effort for `.vec` output. Default `"fast"`.
#'
#' @return When `path` is `NULL`, a numeric matrix (row 1 northmost) carrying
#'   `gt`, `extent`, and `crs` attributes. When `path` is given, the written
#'   `vectra_raster` handle (invisibly).
#'
#' @seealso [warp()] to align rasters onto a shared grid first, [focal()] for
#'   neighbourhood rather than cellwise calculation.
#'
#' @examples
#' nir <- matrix(c(40, 50, 60, 70), 2, 2)
#' red <- matrix(c(10, 20, 30, 40), 2, 2)
#' fn <- tempfile(fileext = ".vec"); fr <- tempfile(fileext = ".vec")
#' vec_write_raster(nir, fn, dtype = "f64", extent = c(0, 0, 2, 2))
#' vec_write_raster(red, fr, dtype = "f64", extent = c(0, 0, 2, 2))
#'
#' ndvi <- rast_calc(list(nir = fn, red = fr), (nir - red) / (nir + red))
#' round(ndvi, 3)
#' unlink(c(fn, fr))
#'
#' @export
rast_calc <- function(rasters, expr, band = 1L, path = NULL, dtype = "f32",
                      compression = c("fast", "balanced", "max")) {
  if (!is.list(rasters) || !length(rasters) ||
      is.null(names(rasters)) || any(names(rasters) == ""))
    stop("`rasters` must be a named list of vectra_raster handles or .vec paths")
  ex <- substitute(expr)
  comp_code <- switch(match.arg(compression), fast = 0L, balanced = 1L, max = 2L)

  handles <- lapply(rasters, function(z) .zonal_open(z, "rasters"))
  on.exit(for (h in handles) if (h$close) try(vec_close_raster(h$r), silent = TRUE),
          add = TRUE)
  rs <- lapply(handles, `[[`, "r")
  r1r <- rs[[1L]]
  W <- as.integer(r1r$width); H <- as.integer(r1r$height); gt <- as.numeric(r1r$gt)
  for (z in rs[-1L])
    if (z$width != W || z$height != H ||
        !isTRUE(all.equal(as.numeric(z$gt), gt)))
      stop("all rasters must share dimensions and geotransform; warp() them first")
  TS <- max(1L, as.integer(r1r$tile_size))
  epsg <- if (!is.null(r1r$epsg)) as.integer(r1r$epsg) else 0L
  band <- as.integer(band)
  pe <- parent.frame()

  sink <- .raster_sink(W, H, 1L, gt, epsg, TS, path, dtype, NULL, comp_code)
  tiles_y <- (H + TS - 1L) %/% TS
  for (ty in seq_len(tiles_y) - 1L) {
    r0 <- ty * TS + 1L; r1 <- min(r0 + TS - 1L, H); out_h <- r1 - r0 + 1L
    env <- new.env(parent = pe)
    for (nm in names(rs))
      assign(nm, as.vector(vec_read_window(rs[[nm]], band = band,
                                           cols = c(1L, W), rows = c(r0, r1))),
             envir = env)
    v <- eval(ex, env)
    if (length(v) == 1L) v <- rep(v, out_h * W)
    if (length(v) != out_h * W)
      stop("`expr` must return one value per cell (or a scalar)")
    sink$write(ty, r0, r1, matrix(as.numeric(v), out_h, W))
  }
  out <- sink$finish()
  if (!is.null(path)) return(out)
  out[[1L]]
}

# -- mosaic (merge aligned rasters onto a common grid) ------------------------

# Resolve the union grid for a set of rasters that must share resolution and sit
# on a common cell grid. Returns the target W/H/gt and each input's integer
# col/row offset into it.
.mosaic_grid <- function(rs) {
  g1 <- as.numeric(rs[[1L]]$gt)
  xres <- g1[2L]; yres <- g1[6L]
  if (g1[3L] != 0 || g1[5L] != 0)
    stop("mosaic needs north-up (unrotated) rasters")
  n <- length(rs)
  xmins <- xmaxs <- ymins <- ymaxs <- numeric(n)
  for (i in seq_len(n)) {
    g <- as.numeric(rs[[i]]$gt); Wi <- rs[[i]]$width; Hi <- rs[[i]]$height
    if (abs(g[2L] - xres) > 1e-9 * abs(xres) ||
        abs(g[6L] - yres) > 1e-9 * abs(yres) || g[3L] != 0 || g[5L] != 0)
      stop("all rasters must share resolution and be north-up; warp() them first")
    xmins[i] <- g[1L]; ymaxs[i] <- g[4L]
    xmaxs[i] <- g[1L] + Wi * g[2L]; ymins[i] <- g[4L] + Hi * g[6L]
  }
  xmin <- min(xmins); xmax <- max(xmaxs); ymin <- min(ymins); ymax <- max(ymaxs)
  W <- as.integer(round((xmax - xmin) / xres))
  H <- as.integer(round((ymax - ymin) / (-yres)))
  col_off <- (xmins - xmin) / xres
  row_off <- (ymax - ymaxs) / (-yres)
  if (any(abs(col_off - round(col_off)) > 1e-6) ||
      any(abs(row_off - round(row_off)) > 1e-6))
    stop("rasters are not on a common cell grid; warp() them to a shared grid first")
  list(W = W, H = H, gt = c(xmin, xres, 0, ymax, 0, yres),
       col_off = as.integer(round(col_off)), row_off = as.integer(round(row_off)))
}

#' Merge aligned rasters onto a common grid
#'
#' Combines several `.vec` rasters that share a resolution and cell grid into one
#' raster spanning their union, resolving overlap with `fun`. The output is
#' walked one tile-row strip at a time and each input contributes only the window
#' overlapping the current strip, so neither the inputs nor the output are held
#' whole in memory.
#'
#' This is the *monoid fold* tier of the spatial toolbox: each output strip folds
#' the overlapping input windows, bounded memory, no spill. The inputs must share
#' resolution and lie on a common cell grid; [warp()] them onto a shared grid
#' first if they do not. No \pkg{sf} is needed.
#'
#' @param rasters A list of `vectra_raster` handles or `.vec` paths sharing
#'   resolution and grid alignment.
#' @param fun Overlap rule where inputs cover the same cell: `"first"` (the
#'   earliest input in `rasters`, default), `"last"`, `"mean"`, `"sum"`, `"min"`,
#'   or `"max"`. Cells covered by no input come back `NA`.
#' @param band Band read from every input (1-based). Default 1.
#' @param path Optional output `.vec` path. When given the result is streamed to
#'   disk and the opened [vec_open_raster()] handle is returned invisibly; when
#'   `NULL` the result is returned as an in-memory matrix.
#' @param dtype Storage dtype for `.vec` output (see [vec_write_raster()]).
#'   Default `"f32"`.
#' @param compression Compression effort for `.vec` output. Default `"fast"`.
#'
#' @return When `path` is `NULL`, a numeric matrix on the union grid (row 1
#'   northmost) carrying `gt`, `extent`, and `crs` attributes. When `path` is
#'   given, the written `vectra_raster` handle (invisibly).
#'
#' @seealso [warp()] to bring rasters onto a shared grid, [rast_calc()] for
#'   cellwise combination of already-aligned rasters.
#'
#' @examples
#' a <- matrix(1, 4, 4); b <- matrix(2, 4, 4)
#' fa <- tempfile(fileext = ".vec"); fb <- tempfile(fileext = ".vec")
#' vec_write_raster(a, fa, dtype = "f64", extent = c(0, 0, 4, 4))
#' vec_write_raster(b, fb, dtype = "f64", extent = c(2, 2, 6, 6))
#'
#' m <- mosaic(list(fa, fb), fun = "mean")
#' dim(m)
#' unlink(c(fa, fb))
#'
#' @export
mosaic <- function(rasters, fun = c("first", "last", "mean", "sum", "min", "max"),
                   band = 1L, path = NULL, dtype = "f32",
                   compression = c("fast", "balanced", "max")) {
  fun <- match.arg(fun)
  comp_code <- switch(match.arg(compression), fast = 0L, balanced = 1L, max = 2L)
  if (!is.list(rasters) || !length(rasters))
    stop("`rasters` must be a list of vectra_raster handles or .vec paths")

  handles <- lapply(rasters, function(z) .zonal_open(z, "rasters"))
  on.exit(for (h in handles) if (h$close) try(vec_close_raster(h$r), silent = TRUE),
          add = TRUE)
  rs <- lapply(handles, `[[`, "r")
  band <- as.integer(band)
  mg <- .mosaic_grid(rs)
  W <- mg$W; H <- mg$H; gt <- mg$gt
  epsg <- if (!is.null(rs[[1L]]$epsg)) as.integer(rs[[1L]]$epsg) else 0L
  TS <- max(1L, as.integer(rs[[1L]]$tile_size))
  sink <- .raster_sink(W, H, 1L, gt, epsg, TS, path, dtype, NULL, comp_code)

  # "first" wants the earliest input to win a shared cell; process inputs in
  # reverse so the earliest is written last under the last-write-wins fill.
  order_in <- if (fun == "first") rev(seq_along(rs)) else seq_along(rs)

  tiles_y <- (H + TS - 1L) %/% TS
  for (ty in seq_len(tiles_y) - 1L) {
    r0 <- ty * TS + 1L; r1 <- min(r0 + TS - 1L, H); out_h <- r1 - r0 + 1L
    sm  <- matrix(0,        out_h, W); cnt <- matrix(0L,        out_h, W)
    mn  <- matrix(Inf,      out_h, W); mx  <- matrix(-Inf,      out_h, W)
    val <- matrix(NA_real_, out_h, W)
    for (i in order_in) {
      ro <- mg$row_off[i]; co <- mg$col_off[i]
      iW <- rs[[i]]$width; iH <- rs[[i]]$height
      sr0 <- max(1L, r0 - ro); sr1 <- min(iH, r1 - ro)
      if (sr1 < sr0) next
      win <- vec_read_window(rs[[i]], band = band, cols = c(1L, iW),
                             rows = c(sr0, sr1))
      lr0 <- (sr0 + ro) - r0 + 1L; lr1 <- (sr1 + ro) - r0 + 1L
      br <- lr0:lr1; bc <- (co + 1L):(co + iW)
      ok <- !is.na(win)
      w0 <- win; w0[!ok] <- 0
      sm[br, bc]  <- sm[br, bc] + w0
      cnt[br, bc] <- cnt[br, bc] + ok
      sub_mn <- mn[br, bc]; sub_mn[ok] <- pmin(sub_mn[ok], win[ok]); mn[br, bc] <- sub_mn
      sub_mx <- mx[br, bc]; sub_mx[ok] <- pmax(sub_mx[ok], win[ok]); mx[br, bc] <- sub_mx
      sub_v  <- val[br, bc]; sub_v[ok]  <- win[ok];                  val[br, bc] <- sub_v
    }
    out <- switch(fun,
      first = val, last = val,
      sum  = ifelse(cnt > 0L, sm, NA_real_),
      mean = ifelse(cnt > 0L, sm / cnt, NA_real_),
      min  = ifelse(is.finite(mn), mn, NA_real_),
      max  = ifelse(is.finite(mx), mx, NA_real_))
    sink$write(ty, r0, r1, out)
  }
  res <- sink$finish()
  if (!is.null(path)) return(res)
  res[[1L]]
}

# -- proximity (Euclidean distance to the nearest feature) --------------------

# Sentinel cost for a cell that holds no feature. The squared distance between
# any two cells of a realistic grid stays far below this, so it acts as
# "infinity" while keeping the parabola intersections in finite arithmetic.
.FH_INF <- 1e20

# One streamed row pass of the distance transform. Reads `rsrc` one tile-row
# strip at a time, optionally maps each strip to feature/sentinel costs through
# `prep`, transforms every row with spacing `scale` in C, and writes to `sink`.
.proximity_row_pass <- function(rsrc, band, W, H, TS, scale, sink, prep) {
  tiles_y <- (H + TS - 1L) %/% TS
  for (ty in seq_len(tiles_y) - 1L) {
    r0 <- ty * TS + 1L; r1 <- min(r0 + TS - 1L, H); out_h <- r1 - r0 + 1L
    vm <- vec_read_window(rsrc, band = band, cols = c(1L, W), rows = c(r0, r1))
    f <- if (is.null(prep)) vm else prep(vm)
    storage.mode(f) <- "double"
    g <- .Call(C_edt_strip, f, c(out_h, W), as.numeric(scale))
    sink$write(ty, r0, r1, g)
  }
}

# Streamed out-of-core transpose. The destination raster has width `srcH` and
# height `srcW`; its tile-row strip [r0, r1] is the source columns [r0, r1] over
# every source row, transposed. `finalize` optionally maps the values before
# they are written (used on the final pass to take the square root and mark
# no-feature cells).
.proximity_transpose <- function(rsrc, srcW, srcH, TS, sink, finalize) {
  tiles_y <- (srcW + TS - 1L) %/% TS
  for (ty in seq_len(tiles_y) - 1L) {
    r0 <- ty * TS + 1L; r1 <- min(r0 + TS - 1L, srcW)
    block <- vec_read_window(rsrc, band = 1L, cols = c(r0, r1), rows = c(1L, srcH))
    tb <- t(block)
    if (!is.null(finalize)) tb <- finalize(tb)
    sink$write(ty, r0, r1, tb)
  }
}

#' Euclidean distance to the nearest feature (proximity)
#'
#' Computes, for every cell of a `.vec` raster, the straight-line Euclidean
#' distance to the nearest feature cell, in CRS units. Feature cells are the
#' non-NA cells by default, or the cells whose value is in `target`. This is the
#' raster proximity / Euclidean-distance staple, the distance companion to
#' [rasterize()].
#'
#' The exact Euclidean distance transform is separable (Felzenszwalb and
#' Huttenlocher 2012): a one-dimensional lower-envelope-of-parabolas transform
#' along the rows, then the same transform along the columns, each linear in the
#' line length and each line independent. vectra runs it as four streamed passes
#' over tile-row strips, with an out-of-core transpose between the row pass and
#' the column pass, so the whole grid is never resident. The row pass scales
#' squared distances by the x resolution and the column pass by the y
#' resolution, so the result is exact on anisotropic (non-square) cells. This
#' places proximity on the sort / partition tier of the spatial toolbox.
#'
#' Distances are straight-line Euclidean in the raster CRS units. Cost-distance,
#' which accumulates a per-cell friction along the path, is a global
#' shortest-path problem and stays resident: [collect()] the raster and run a
#' resident solver for that.
#'
#' @param x A `vectra_raster` (from [vec_open_raster()]) or a path to a `.vec`
#'   raster.
#' @param target Optional numeric vector of feature values. When `NULL`
#'   (default) every non-NA cell is a feature; otherwise a cell is a feature
#'   when its value is in `target`.
#' @param band Band to read (1-based). Default 1.
#' @param path Optional output `.vec` path. When given the result is streamed to
#'   disk and the opened [vec_open_raster()] handle is returned invisibly; when
#'   `NULL` the result is returned as an in-memory matrix.
#' @param dtype Storage dtype for `.vec` output (see [vec_write_raster()]).
#'   Default `"f32"`.
#' @param compression Compression effort for `.vec` output. Default `"fast"`.
#'
#' @return When `path` is `NULL`, a numeric matrix (row 1 northmost) carrying
#'   `gt`, `extent`, and `crs` attributes, with distance in CRS units and `NA`
#'   where the raster holds no feature anywhere. When `path` is given, the
#'   written `vectra_raster` handle (invisibly).
#'
#' @seealso [rasterize()] to build a raster from streamed points, [mask()] to
#'   clip a raster to a polygon layer.
#'
#' @examples
#' m <- matrix(NA_real_, 12, 12)
#' m[3, 4] <- 1; m[9, 10] <- 1
#' f <- tempfile(fileext = ".vec")
#' vec_write_raster(m, f, dtype = "f64", extent = c(0, 0, 12, 12))
#'
#' d <- proximity(f)
#' round(d[1:3, 1:3], 2)
#' unlink(f)
#'
#' @export
proximity <- function(x, target = NULL, band = 1L, path = NULL, dtype = "f32",
                      compression = c("fast", "balanced", "max")) {
  comp_code <- switch(match.arg(compression), fast = 0L, balanced = 1L, max = 2L)
  if (!is.null(target) && !is.numeric(target))
    stop("`target` must be a numeric vector of feature values, or NULL")

  vh <- .zonal_open(x, "x"); r <- vh$r
  if (vh$close) on.exit(vec_close_raster(r), add = TRUE)
  W <- as.integer(r$width); H <- as.integer(r$height); gt <- r$gt
  TS <- max(1L, as.integer(r$tile_size))
  epsg <- if (!is.null(r$epsg)) as.integer(r$epsg) else 0L
  band <- as.integer(band)
  xres <- abs(gt[2L]); yres <- abs(gt[6L])
  dgt <- c(0, 1, 0, 0, 0, -1)   # placeholder grid for the transposed temporaries

  t1  <- tempfile(fileext = ".vec")
  t1t <- tempfile(fileext = ".vec")
  t2t <- tempfile(fileext = ".vec")
  on.exit(unlink(c(t1, t1t, t2t)), add = TRUE)

  # Pass A: row transform of the source (spacing xres), features -> 0 cost and
  # every other cell -> the sentinel. Output t1 holds the partial transform.
  prep <- function(vm) {
    v <- as.numeric(vm)
    feat <- if (is.null(target)) !is.na(v) else !is.na(v) & v %in% target
    f <- rep(.FH_INF, length(v)); f[feat] <- 0
    matrix(f, nrow(vm), ncol(vm))
  }
  s1 <- .raster_sink(W, H, 1L, gt, epsg, TS, t1, "f64", NULL, 0L)
  .proximity_row_pass(r, band, W, H, TS, xres, s1, prep)
  h1 <- s1$finish()

  # Transpose t1 (W x H) -> t1t (H x W): the source columns become rows.
  s1t <- .raster_sink(H, W, 1L, dgt, 0L, TS, t1t, "f64", NULL, 0L)
  .proximity_transpose(h1, W, H, TS, s1t, NULL)
  h1t <- s1t$finish(); vec_close_raster(h1)

  # Pass B: row transform of the transpose (spacing yres) is the column
  # transform of the source, completing the exact squared distance.
  s2 <- .raster_sink(H, W, 1L, dgt, 0L, TS, t2t, "f64", NULL, 0L)
  .proximity_row_pass(h1t, 1L, H, W, TS, yres, s2, NULL)
  h2t <- s2$finish(); vec_close_raster(h1t)

  # Transpose back (H x W -> W x H), take the square root, and map cells with no
  # feature anywhere to NA.
  bn <- if (!is.null(r$band_names)) r$band_names[band] else NULL
  out_sink <- .raster_sink(W, H, 1L, gt, epsg, TS, path, dtype, bn, comp_code)
  finalize <- function(tb) {
    d <- sqrt(tb); d[tb >= 1e19] <- NA_real_; d
  }
  .proximity_transpose(h2t, H, W, TS, out_sink, finalize)
  out <- out_sink$finish(); vec_close_raster(h2t)

  if (!is.null(path)) return(out)
  out[[1L]]
}

# -- polygonize (raster -> vector polygons) -----------------------------------

# Turn one strip of pixels into rectangle corners tagged by value. With
# per_cell = FALSE, equal-valued runs along a row collapse to one rectangle (the
# by-value dissolve then merges across rows and strips); with per_cell = TRUE,
# every cell becomes its own square.
.polygonize_strip <- function(vm, gt, r0, W, na_rm, per_cell) {
  out_h <- nrow(vm)
  xres <- gt[2L]; yres <- gt[6L]; x0 <- gt[1L]; y0 <- gt[4L]
  xmin <- xmax <- ymin <- ymax <- val <- numeric(0)
  for (a in seq_len(out_h)) {
    row <- vm[a, ]
    ytop <- y0 + (r0 - 1L + a - 1L) * yres
    ybot <- y0 + (r0 - 1L + a) * yres
    yt <- max(ytop, ybot); yb <- min(ytop, ybot)
    if (per_cell) {
      cols <- if (na_rm) which(!is.na(row)) else seq_len(W)
      if (!length(cols)) next
      xmin <- c(xmin, x0 + (cols - 1L) * xres); xmax <- c(xmax, x0 + cols * xres)
      ymin <- c(ymin, rep(yb, length(cols)));   ymax <- c(ymax, rep(yt, length(cols)))
      val  <- c(val, row[cols])
    } else {
      rr <- rle(row)
      ends <- cumsum(rr$lengths); starts <- ends - rr$lengths + 1L
      for (k in seq_along(rr$values)) {
        vv <- rr$values[k]
        if (na_rm && is.na(vv)) next
        xmin <- c(xmin, x0 + (starts[k] - 1L) * xres)
        xmax <- c(xmax, x0 + ends[k] * xres)
        ymin <- c(ymin, yb); ymax <- c(ymax, yt); val <- c(val, vv)
      }
    }
  }
  list(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax, val = val)
}

#' Vectorise a raster into polygons
#'
#' Converts a `.vec` raster into polygon features, the inverse of [rasterize()].
#' The raster is read one tile-row strip at a time; within each strip equal-valued
#' cells along a row collapse to a rectangle, and (with `dissolve = TRUE`) the
#' rectangles are then merged by value through [spatial_dissolve()] so each
#' distinct value becomes a single polygon spanning the whole raster. The result
#' is a lazy `vectra_node` carrying a value column and hex-WKB geometry.
#'
#' Extraction is the *monoid fold* tier (one strip at a time); the by-value
#' dissolve rides the *sort / partition* tier of [spatial_dissolve()], localising
#' each value to a shard before the union. Geometry assembly is delegated to
#' \pkg{sf} (an optional dependency).
#'
#' @param x A `vectra_raster` (from [vec_open_raster()]) or a path to a `.vec`
#'   raster.
#' @param band Band to vectorise (1-based). Default 1.
#' @param dissolve If `TRUE` (default) merge cells of equal value into one polygon
#'   per value; if `FALSE` emit one square polygon per cell.
#' @param na_rm Drop nodata cells (`TRUE`, default) or vectorise them as a value.
#' @param values Name of the output value column. Default `"value"`.
#' @param crs Coordinate reference system recorded on the node. Defaults to the
#'   raster's EPSG, else unknown.
#' @param flush_rows Rows buffered before a spill flush. Defaults to
#'   `getOption("vectra.spatial_flush", 5e5)`.
#'
#' @return A `vectra_node` with the value column and a hex-WKB `geometry` column,
#'   materialise it with [collect_sf()].
#'
#' @seealso [rasterize()] for the inverse, [contours()] for iso-lines,
#'   [collect_sf()] to materialise as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' m <- matrix(c(1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3), 4, 4,
#'             byrow = TRUE)
#' f <- tempfile(fileext = ".vec")
#' vec_write_raster(m, f, dtype = "f64", extent = c(0, 0, 4, 4))
#'
#' polys <- polygonize(f)
#' collect_sf(polys)
#' unlink(f)
#'
#' @export
polygonize <- function(x, band = 1L, dissolve = TRUE, na_rm = TRUE,
                       values = "value", crs = NA, flush_rows = NULL) {
  .check_sf()
  vh <- .zonal_open(x, "x"); r <- vh$r
  if (vh$close) on.exit(vec_close_raster(r), add = TRUE)
  W <- as.integer(r$width); H <- as.integer(r$height); gt <- r$gt
  TS <- max(1L, as.integer(r$tile_size))
  if (identical(crs, NA) && !is.null(r$epsg) && r$epsg > 0L) crs <- r$epsg
  rcrs <- .as_crs(crs)
  fr <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  acc <- .run_accumulator(fr)

  tiles_y <- (H + TS - 1L) %/% TS
  for (ty in seq_len(tiles_y) - 1L) {
    r0 <- ty * TS + 1L; r1 <- min(r0 + TS - 1L, H)
    vm <- vec_read_window(r, band = band, cols = c(1L, W), rows = c(r0, r1))
    p <- .polygonize_strip(vm, gt, r0, W, na_rm, per_cell = !dissolve)
    if (!length(p$val)) next
    g <- .rects_sfc(p$xmin, p$xmax, p$ymin, p$ymax, rcrs)
    df <- stats::setNames(data.frame(p$val), values)
    acc$push(.sf_encode_result(sf::st_sf(df, geometry = g), "geometry"))
  }
  node <- acc$finish(crs = rcrs, empty_geom = "geometry")
  if (dissolve) node <- spatial_dissolve(node, by = values, crs = rcrs)
  node
}

# -- contours (raster -> vector iso-lines) ------------------------------------

# Marching-squares iso-line segments for one window of pixel centres. `vm` is an
# in_h x W matrix whose rows span target rows r0 .. r0+in_h-1 (with one bottom
# halo row so every cell-row with its top row in the strip is covered). Returns
# segment endpoint and level vectors for all `levels`.
.contour_segments <- function(vm, gt, r0, W, levels) {
  inh <- nrow(vm)
  if (inh < 2L || W < 2L) return(NULL)
  xres <- gt[2L]; yres <- gt[6L]; x0 <- gt[1L]; y0 <- gt[4L]
  na <- inh - 1L
  tl <- vm[1:na, 1:(W - 1L), drop = FALSE]; tr <- vm[1:na, 2:W, drop = FALSE]
  bl <- vm[2:inh, 1:(W - 1L), drop = FALSE]; br <- vm[2:inh, 2:W, drop = FALSE]
  ctop <- matrix(rep(1:(W - 1L), each = na), na, W - 1L)
  rtop <- matrix(rep(r0:(r0 + na - 1L), times = W - 1L), na, W - 1L)
  xL <- x0 + (ctop - 0.5) * xres; xR <- x0 + (ctop + 0.5) * xres
  yT <- y0 + (rtop - 0.5) * yres; yB <- y0 + (rtop + 0.5) * yres
  ctr <- (tl + tr + bl + br) / 4

  x1 <- y1 <- x2 <- y2 <- lev <- numeric(0)
  # edge map: each case -> pairs of edge codes (T=1, R=2, B=3, L=4).
  segmap <- list(
    `1` = list(c(4, 3)), `2` = list(c(3, 2)), `3` = list(c(4, 2)),
    `4` = list(c(1, 2)), `6` = list(c(1, 3)), `7` = list(c(1, 4)),
    `8` = list(c(1, 4)), `9` = list(c(1, 3)), `11` = list(c(1, 2)),
    `12` = list(c(4, 2)), `13` = list(c(3, 2)), `14` = list(c(4, 3)))

  for (L in levels) {
    ok <- is.finite(tl) & is.finite(tr) & is.finite(bl) & is.finite(br)
    a <- tl > L; b <- tr > L; c <- br > L; d <- bl > L
    code <- 8L * a + 4L * b + 2L * c + 1L * d
    code[!ok] <- 0L
    # edge crossing coordinates (vectors over the cell grid)
    ex <- list(
      Tx = xL + (xR - xL) * (L - tl) / (tr - tl), Ty = yT,
      Rx = xR, Ry = yT + (yB - yT) * (L - tr) / (br - tr),
      Bx = xL + (xR - xL) * (L - bl) / (br - bl), By = yB,
      Lx = xL, Ly = yT + (yB - yT) * (L - tl) / (bl - tl))
    pt <- function(edge, idx) {
      if (edge == 1L) list(x = ex$Tx[idx], y = ex$Ty[idx])
      else if (edge == 2L) list(x = ex$Rx[idx], y = ex$Ry[idx])
      else if (edge == 3L) list(x = ex$Bx[idx], y = ex$By[idx])
      else list(x = ex$Lx[idx], y = ex$Ly[idx])
    }
    emit <- function(idx, e1, e2) {
      if (!length(idx)) return(invisible())
      p1 <- pt(e1, idx); p2 <- pt(e2, idx)
      x1 <<- c(x1, p1$x); y1 <<- c(y1, p1$y)
      x2 <<- c(x2, p2$x); y2 <<- c(y2, p2$y); lev <<- c(lev, rep(L, length(idx)))
    }
    for (cs in names(segmap)) {
      idx <- which(code == as.integer(cs))
      if (length(idx)) for (pair in segmap[[cs]]) emit(idx, pair[1L], pair[2L])
    }
    # saddles: connection depends on the cell-centre value.
    s5 <- which(code == 5L); s10 <- which(code == 10L)
    cf <- ctr > L
    if (length(s5)) {
      hi <- s5[cf[s5]]; lo <- s5[!cf[s5]]
      emit(hi, 1L, 4L); emit(hi, 3L, 2L)   # centre filled: T-L and B-R
      emit(lo, 1L, 2L); emit(lo, 3L, 4L)   # centre empty:  T-R and B-L
    }
    if (length(s10)) {
      hi <- s10[cf[s10]]; lo <- s10[!cf[s10]]
      emit(hi, 1L, 2L); emit(hi, 3L, 4L)   # centre filled: T-R and B-L
      emit(lo, 1L, 4L); emit(lo, 3L, 2L)   # centre empty:  T-L and B-R
    }
  }
  if (!length(lev)) return(NULL)
  list(x1 = x1, y1 = y1, x2 = x2, y2 = y2, level = lev)
}

#' Extract contour iso-lines from a streamed raster
#'
#' Traces contour lines at one or more levels from a `.vec` raster with marching
#' squares, reading the raster one tile-row strip at a time (each strip expanded
#' by one row so a cell straddling the strip boundary is traced once). Each strip
#' contributes line segments, which are accumulated into a lazy `vectra_node`
#' carrying a `level` column and hex-WKB geometry. With `merge = TRUE` the
#' segments of each level are joined into continuous lines.
#'
#' Extraction is the *sort / partition* tier of the spatial toolbox: bounded to
#' one haloed strip at a time. The optional final merge collects the segment set,
#' which is small relative to the raster, and joins it per level; this is the
#' small all-to-all step on the output, not on the grid. Geometry assembly and
#' the merge are delegated to \pkg{sf} (an optional dependency).
#'
#' @param x A `vectra_raster` (from [vec_open_raster()]) or a path to a `.vec`
#'   raster.
#' @param levels Numeric vector of contour levels to trace.
#' @param band Band to contour (1-based). Default 1.
#' @param merge If `TRUE` (default) join each level's segments into continuous
#'   lines with [sf::st_line_merge()]; if `FALSE` return the raw per-cell
#'   segments.
#' @param crs Coordinate reference system recorded on the node. Defaults to the
#'   raster's EPSG, else unknown.
#' @param flush_rows Rows buffered before a spill flush. Defaults to
#'   `getOption("vectra.spatial_flush", 5e5)`.
#'
#' @return A `vectra_node` with a `level` column and a hex-WKB `geometry` column,
#'   materialise it with [collect_sf()].
#'
#' @seealso [polygonize()] for area features, [terrain()] for the DEM
#'   derivatives contours often accompany, [collect_sf()] to materialise as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' z <- outer(1:20, 1:20, function(r, c) r + c)
#' f <- tempfile(fileext = ".vec")
#' vec_write_raster(z, f, dtype = "f64", extent = c(0, 0, 20, 20))
#'
#' iso <- contours(f, levels = c(15, 25, 35))
#' collect_sf(iso)
#' unlink(f)
#'
#' @export
contours <- function(x, levels, band = 1L, merge = TRUE, crs = NA,
                     flush_rows = NULL) {
  .check_sf()
  if (!is.numeric(levels) || !length(levels))
    stop("`levels` must be a numeric vector of contour levels")
  vh <- .zonal_open(x, "x"); r <- vh$r
  if (vh$close) on.exit(vec_close_raster(r), add = TRUE)
  W <- as.integer(r$width); H <- as.integer(r$height); gt <- r$gt
  TS <- max(1L, as.integer(r$tile_size))
  if (identical(crs, NA) && !is.null(r$epsg) && r$epsg > 0L) crs <- r$epsg
  rcrs <- .as_crs(crs)
  fr <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  acc <- .run_accumulator(fr)

  tiles_y <- (H + TS - 1L) %/% TS
  for (ty in seq_len(tiles_y) - 1L) {
    r0 <- ty * TS + 1L; r1 <- min(r0 + TS - 1L, H)
    in_r1 <- min(H, r1 + 1L)             # one bottom halo row for the cell below
    vm <- vec_read_window(r, band = band, cols = c(1L, W), rows = c(r0, in_r1))
    s <- .contour_segments(vm, gt, r0, W, levels)
    if (is.null(s)) next
    g <- .segments_sfc(s$x1, s$y1, s$x2, s$y2, rcrs)
    acc$push(.sf_encode_result(
      sf::st_sf(data.frame(level = s$level), geometry = g), "geometry"))
  }
  node <- acc$finish(crs = rcrs, empty_geom = "geometry")
  if (!merge) return(node)

  df <- collect(node)
  if (!nrow(df)) return(node)
  sb <- .sf_decode_chunk(df, "geometry", NULL, rcrs)
  geo <- sf::st_geometry(sb)
  parts <- split(seq_len(nrow(df)), df$level)
  out_lev <- as.numeric(names(parts))
  merged <- lapply(parts, function(ix)
    sf::st_line_merge(sf::st_union(geo[ix])))
  acc2 <- .run_accumulator(fr)
  for (j in seq_along(merged))
    acc2$push(.sf_encode_result(
      sf::st_sf(data.frame(level = out_lev[j]), geometry = merged[[j]]),
      "geometry"))
  acc2$finish(crs = rcrs, empty_geom = "geometry")
}
