# spatial.R - streamed spatial operations via sf.
#
# vectra has no geometry type and no GEOS: topology stays sf's job. These verbs
# instead stream a larger-than-RAM query through the engine one batch at a time,
# hand each batch to sf for the actual geometry work, and spill the transformed
# batches back to disk as a fresh lazy node. Geometry rides through the engine as
# hex-encoded WKB in an ordinary string column, so no new VecType is needed; the
# CRS is carried on the returned node (the .vtr file stores no CRS).
#
# A shared run-file accumulator (.run_accumulator) plus the .spatial_stream
# engine drive the front doors:
#   spatial_map(x, fn)          per-feature transforms (buffer, transform, ...)
#   spatial_join(x, y, join)    big-x streamed against a resident small-y sf
#   spatial_filter(x, y, pred)  select-by-location: keep big-x rows matching y
#   spatial_clip(x, mask)       clip / erase big-x geometry against a mask
#   spatial_overlay(x)          self-overlay a resident polygon layer into pieces
# This mirrors offload(by = ...): batch cursor -> per-batch work -> run-files ->
# a ConcatNode with a finalizer that clears the temp spills.

.SPATIAL_FLUSH <- 5e5      # transformed rows buffered before a run-file flush

.check_sf <- function() {
  if (!requireNamespace("sf", quietly = TRUE))
    stop("spatial operations require the 'sf' package; install it with ",
         "install.packages(\"sf\")", call. = FALSE)
}

# Default sentinel-aware CRS resolution: an explicit `crs` wins; otherwise
# inherit the CRS the upstream node carries (so spatial_map() |> spatial_join()
# keeps the projection without re-stating it).
.resolve_crs <- function(x, crs) {
  if ((identical(crs, NA) || is.null(crs)) && !is.null(x$.crs)) return(x$.crs)
  crs
}

# -- geometry transport (hex WKB in a string column) --------------------------

# Decode one engine batch (a data.frame) into an sf object. Either a geometry
# column of hex WKB / WKT strings (`geom`), or point coordinates assembled from
# two numeric columns (`coords = c("x", "y")`), in which case the coordinate
# columns are kept so downstream work can still see them.
# Normalize any CRS input (including the logical-NA default) to an sf crs
# object, since sf rejects a bare logical NA where it wants NA_crs_.
.as_crs <- function(crs) {
  if (identical(crs, NA) || is.null(crs)) sf::st_crs(NA) else sf::st_crs(crs)
}

# Give a resident sf/sfc the stream's CRS when it carries none, so a per-batch
# predicate or overlay does not reject on an NA-vs-known CRS mismatch. When both
# sides already carry a CRS they are left untouched and sf enforces the match.
.align_resident_crs <- function(y, crs) {
  cc <- .as_crs(crs)
  if (is.na(sf::st_crs(y)) && !is.na(cc)) y <- sf::st_set_crs(y, cc)
  y
}

# Reduce a CRS (an EPSG integer, an sf crs object, a WKT/proj string) to an
# EPSG code for the raster header, or 0 when none is known. Only touches sf when
# the input is not already a bare integer, so the coordinate-only rasterize path
# stays sf-free.
.crs_to_epsg <- function(crs) {
  if (is.null(crs) || (length(crs) == 1L && is.na(crs))) return(0L)
  if (is.numeric(crs) && length(crs) == 1L) return(as.integer(crs))
  if (requireNamespace("sf", quietly = TRUE)) {
    e <- tryCatch(sf::st_crs(crs)$epsg, error = function(e) NA_integer_)
    if (!is.null(e) && !is.na(e)) return(as.integer(e))
  }
  0L
}

# Resolve the target grid for rasterize() into width/height + a north-up GDAL
# geotransform. A `vectra_raster` template borrows its geometry and CRS; an
# explicit extent is paired with either `dims = c(nrow, ncol)` or `res` (a
# scalar or c(xres, yres)). With `res`, the cell counts are rounded to fit the
# extent exactly, so the extent stays authoritative and cells stay uniform.
.rasterize_grid <- function(template, extent, res, dims, crs) {
  if (inherits(template, "vectra_raster")) {
    gt   <- template$gt
    w    <- as.integer(template$width)
    h    <- as.integer(template$height)
    tcrs <- if (!is.null(template$epsg) && template$epsg > 0L)
      as.integer(template$epsg) else NA
    if (is.null(crs) || (length(crs) == 1L && is.na(crs))) crs <- tcrs
    xres <- gt[2L]; yres <- -gt[6L]
    ext  <- c(gt[1L], gt[4L] - yres * h, gt[1L] + xres * w, gt[4L])
    return(list(width = w, height = h, gt = gt, crs = crs,
                epsg = .crs_to_epsg(crs), extent = ext, res = c(xres, yres)))
  }
  if (is.numeric(template) && length(template) == 4L && is.null(extent))
    extent <- template
  if (is.null(extent) || !is.numeric(extent) || length(extent) != 4L)
    stop("supply `template` (a vectra_raster) or ",
         "`extent = c(xmin, ymin, xmax, ymax)`")
  xmin <- extent[1L]; ymin <- extent[2L]; xmax <- extent[3L]; ymax <- extent[4L]
  if (!(xmax > xmin) || !(ymax > ymin))
    stop("extent must have xmax > xmin and ymax > ymin")
  if (!is.null(dims)) {
    if (length(dims) != 2L) stop("dims must be c(nrow, ncol)")
    h <- as.integer(dims[1L]); w <- as.integer(dims[2L])
  } else if (!is.null(res)) {
    if (length(res) == 1L) res <- c(res, res)
    w <- as.integer(round((xmax - xmin) / res[1L]))
    h <- as.integer(round((ymax - ymin) / res[2L]))
  } else {
    stop("supply either `res` or `dims` together with `extent`")
  }
  if (is.na(w) || is.na(h) || w <= 0L || h <= 0L)
    stop("the derived grid has non-positive dimensions")
  xres <- (xmax - xmin) / w
  yres <- (ymax - ymin) / h
  gt <- c(xmin, xres, 0, ymax, 0, -yres)
  list(width = w, height = h, gt = gt, crs = crs, epsg = .crs_to_epsg(crs),
       extent = c(xmin, ymin, xmax, ymax), res = c(xres, yres))
}

.sf_decode_chunk <- function(chunk, geom, coords, crs) {
  crs <- .as_crs(crs)
  if (!is.null(coords)) {
    miss <- setdiff(coords, names(chunk))
    if (length(miss))
      stop(sprintf("coords column(s) not found: %s",
                   paste(miss, collapse = ", ")))
    return(sf::st_as_sf(chunk, coords = coords, crs = crs, remove = FALSE))
  }
  if (!geom %in% names(chunk))
    stop(sprintf("geometry column '%s' not found; pass geom= or coords=", geom))
  # st_as_sfc.character parses WKT only; hex WKB must be tagged so the WKB
  # reader runs, which round-trips coordinates losslessly (WKT would round to
  # the print precision).
  g <- sf::st_as_sfc(structure(chunk[[geom]], class = "WKB"), EWKB = FALSE)
  g <- sf::st_set_crs(g, crs)
  rest <- chunk[setdiff(names(chunk), geom)]
  sf::st_sf(rest, geometry = g)
}

# Coerce a data.frame's columns to types the .vtr writer accepts (int64 /
# double / bool / string). sf attribute columns can arrive as factors, dates, or
# units; those collapse to character. List columns other than the geometry we
# already encoded are an error.
.coerce_for_vtr <- function(df) {
  for (nm in names(df)) {
    col <- df[[nm]]
    if (is.factor(col) || inherits(col, "Date") || inherits(col, "POSIXt")) {
      df[[nm]] <- as.character(col)
    } else if (is.integer(col) || is.double(col) ||
               is.logical(col) || is.character(col)) {
      # already writable
    } else if (is.list(col)) {
      stop(sprintf("column '%s' is a list column and cannot be written; ",
                   "drop it before the spatial step", nm))
    } else {
      df[[nm]] <- as.character(col)
    }
  }
  df
}

# Encode the per-batch sf result back into a writable data.frame: the active
# geometry becomes hex WKB in `out_geom`, the rest are coerced attribute columns.
# A plain data.frame (geometry deliberately dropped, e.g. a summary) passes
# through unchanged apart from type coercion.
.sf_encode_result <- function(res, out_geom) {
  if (inherits(res, "sfc")) res <- sf::st_sf(geometry = res)
  if (!inherits(res, "sf")) {
    if (is.data.frame(res)) return(.coerce_for_vtr(as.data.frame(res)))
    stop("spatial batch function must return an sf object, sfc, or data.frame")
  }
  g  <- sf::st_geometry(res)
  df <- as.data.frame(sf::st_drop_geometry(res))
  df[[out_geom]] <- sf::st_as_binary(g, hex = TRUE)
  .coerce_for_vtr(df)
}

# -- native libgeos compute paths ---------------------------------------------

# Worker count for the per-batch GEOS loops, mirroring the overlay convention.
.spatial_threads <- function()
  max(as.integer(getOption("vectra.spatial_threads",
                           min(parallel::detectCores(), 8L))), 1L)

# Parse a resident sf/sfc side once into a GEOS locator (an external pointer
# holding the parsed geometries plus an STRtree), reused across every streamed
# batch. Geometry crosses to C as raw WKB; sf is touched only here, on the small
# resident side, not in the per-batch loop.
.geos_locator <- function(y) {
  g <- if (inherits(y, "sf")) sf::st_geometry(y) else y
  .Call(C_geos_locator_build, sf::st_as_binary(g, EWKB = FALSE))
}

# Native GEOS computes in the planar frame. sf computes geographic coordinates
# on the sphere (s2) when `sf_use_s2()` is on, so the native path matches the sf
# path only for projected data, or when s2 is off (both then planar via GEOS),
# or when the CRS is unknown (sf is planar too). Geographic data with s2 on
# keeps the spherical sf path.
.geos_planar_ok <- function(crs) {
  ll <- tryCatch(sf::st_is_longlat(crs), error = function(e) NA)
  if (is.na(ll)) return(TRUE)
  !ll || !sf::sf_use_s2()
}

# Map an sf binary-predicate function to the native predicate code the C filter
# understands, or NA when it is one the native path does not cover (the caller
# then falls back to the sf loop). NULL means the default, st_intersects.
.geos_pred_code <- function(predicate) {
  if (is.null(predicate)) return(0L)
  known <- c(st_intersects = 0L, st_within = 1L, st_contains = 2L,
             st_overlaps = 3L, st_covers = 4L, st_covered_by = 5L,
             st_touches = 6L, st_crosses = 7L, st_equals = 8L,
             st_disjoint = 9L, st_is_within_distance = 10L)
  for (nm in names(known))
    if (identical(predicate, getExportedValue("sf", nm))) return(unname(known[[nm]]))
  NA_integer_
}

# Pull the two coordinate columns from a batch as doubles (the raw-point input to
# the native locate kernel), erroring if either is missing.
.batch_xy <- function(chunk, coords) {
  miss <- setdiff(coords, names(chunk))
  if (length(miss))
    stop(sprintf("coords column(s) not found: %s", paste(miss, collapse = ", ")))
  list(x = as.numeric(chunk[[coords[1L]]]), y = as.numeric(chunk[[coords[2L]]]))
}

# Build one streamed spatial-join batch from the per-row match lists the C join
# returns. `matches` is a list with one integer vector of 1-based resident-row
# indices per left row; each left row is replicated once per match and the
# resident attributes `y_attrs` attached. With `left`, an unmatched left row is
# kept once padded with NA resident columns; otherwise it is dropped. Attribute
# names shared by both sides get `suffix`, mirroring [sf::st_join()]. The left
# geometry column rides through untouched (the left side is never decoded).
.geos_join_assemble <- function(chunk, matches, y_attrs, left, suffix) {
  nmatch <- lengths(matches)
  reps   <- if (left) pmax(nmatch, 1L) else nmatch
  left_idx  <- rep.int(seq_len(nrow(chunk)), reps)
  right_idx <- unlist(lapply(seq_along(matches), function(i) {
    mi <- matches[[i]]
    if (length(mi)) mi else if (left) NA_integer_ else integer(0)
  }), use.names = FALSE)
  if (is.null(right_idx)) right_idx <- integer(0)

  lout <- chunk[left_idx, , drop = FALSE]
  rout <- y_attrs[right_idx, , drop = FALSE]
  shared <- intersect(names(lout), names(rout))
  if (length(shared)) {
    names(lout)[match(shared, names(lout))] <- paste0(shared, suffix[1L])
    names(rout)[match(shared, names(rout))] <- paste0(shared, suffix[2L])
  }
  out <- cbind(lout, rout, stringsAsFactors = FALSE)
  rownames(out) <- NULL
  out
}

# -- self-overlay (QGIS-style Union) ------------------------------------------

# Fixed-precision grid (in CRS units) for the self-overlay. Snapping input
# coordinates to a grid this fine merges the near-duplicate boundaries that
# overlapping polygons share, so the noded arrangement has no sliver faces. The
# grid is derived from coordinate magnitude (not extent), keeping it stable
# against far-flung outliers and invariant to units. Returns 0 when no sensible
# grid exists (degenerate or all-zero coordinates), which disables snapping.
.overlay_grid_mag <- function(mag) {
  if (!is.finite(mag) || mag == 0) return(0)
  mag * 3e-8
}
.overlay_grid <- function(x) .overlay_grid_mag(max(abs(sf::st_bbox(x))))

# Per-input coverage error of an overlay result. For a correct disjoint
# decomposition the piece areas covering each input must sum to that input's
# area; this is the invariant the snapping grid is verified against. Uses the
# piece and input areas the C engine returns, so no geometry is re-measured.
# Returns a data.frame keyed by the input's chunk-local index, so the caller can
# map back to global rows and name the worst offenders, not just the scalar max.
.overlay_coverage_detail <- function(origin, parea, iarea) {
  if (!length(origin))
    return(data.frame(src = integer(0), cov = numeric(0),
                      iarea = numeric(0), err = numeric(0)))
  agg <- rowsum(parea, group = origin)
  cov <- numeric(length(iarea))
  cov[as.integer(rownames(agg))] <- agg[, 1L]
  data.frame(src = seq_along(iarea), cov = cov, iarea = iarea,
             err = abs(cov - iarea) / pmax(iarea, 1))
}

# Memory model. The overlay cost of a tile is driven by the noding of its
# boundary linework, which can balloon far past the input where polygons overlap
# densely. The layer is therefore tiled so no tile is large enough to blow up: a
# tile that lands on a dense blob keeps subdividing. Peak memory is bounded by
# `mem_limit`; lowering it tightens memory at the cost of more tiles.
#
# Tile size is a throughput knob with an interior optimum, not "bigger is faster".
# Smaller tiles replicate a feature spanning several tiles into each (more clip and
# parse work), while larger tiles node more linework per tile, and noding cost is
# superlinear in the linework gathered. Measured on a dense ~470k-feature layer the
# total wall time is flat for per-tile inputs around 1-5 MB and rises on either
# side; a budget far above that (tens of GB of working set) runs slower, not faster.
# The default therefore targets that per-tile size and lets the budget scale with
# the thread count -- more threads run more tiles at once, so the working set grows
# with parallelism -- rather than scaling with installed RAM.
.OVERLAY_TILE_TGT  <- 2.6e6 # target per-tile input bytes (throughput sweet spot)
.OVERLAY_FACTOR    <- 24    # per-tile working set ~ tile input bytes * this
.OVERLAY_FEAT_CAP  <- 3000  # also cap features per tile (node count, not just bytes)
.OVERLAY_MAX_DEPTH <- 16    # quadtree depth cap (coincident features can't split)

# Total physical RAM in bytes, or NA if it cannot be determined. Cached per
# session. Used only to RELAX limits when memory is plentiful and to cap them
# when it is scarce; every caller has a safe fixed fallback for the NA case, so
# nothing about correctness or the peak guarantee depends on detection working.
.sys_ram_cache <- new.env(parent = emptyenv())
.sys_ram_bytes <- function() {
  if (!is.null(.sys_ram_cache$v)) return(.sys_ram_cache$v)
  v <- tryCatch({
    sys <- Sys.info()[["sysname"]]
    if (identical(sys, "Linux")) {
      mt <- grep("^MemTotal:", readLines("/proc/meminfo", n = 64L), value = TRUE)
      if (length(mt)) as.numeric(sub("\\D+(\\d+).*", "\\1", mt[1L])) * 1024 else NA_real_
    } else if (identical(sys, "Darwin")) {
      as.numeric(system2("sysctl", c("-n", "hw.memsize"), stdout = TRUE, stderr = FALSE))
    } else if (identical(sys, "Windows")) {
      pick <- function(out) {
        num <- suppressWarnings(as.numeric(grep("[0-9]", out, value = TRUE)))
        num <- num[is.finite(num) & num > 0]
        if (length(num)) num[1L] else NA_real_
      }
      out <- tryCatch(system2("wmic", c("ComputerSystem", "get", "TotalPhysicalMemory"),
                              stdout = TRUE, stderr = FALSE), error = function(e) character(0))
      r <- pick(out)
      if (is.na(r)) {  # wmic is absent on newer Windows; fall back to CIM
        out <- tryCatch(system2("powershell",
          c("-NoProfile", "-Command",
            "(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory"),
          stdout = TRUE, stderr = FALSE), error = function(e) character(0))
        r <- pick(out)
      }
      r
    } else NA_real_
  }, error = function(e) NA_real_)
  if (length(v) != 1L || !is.finite(v) || v <= 0) v <- NA_real_
  .sys_ram_cache$v <- v
  v
}

# Default overlay working-set budget. tile_bytes = mem / (threads * FACTOR), so to
# hold tile_bytes at the target the budget scales with the thread count. Peak
# working set is then target * FACTOR * threads, modest and proportional to the
# parallelism actually used. When total RAM is known the budget is also capped at
# half of it, so a many-core / low-RAM machine cannot scale the working set past
# what it can hold. An explicit mem_limit always wins.
.overlay_mem_default <- function(nthreads) {
  thread_scaled <- .OVERLAY_TILE_TGT * .OVERLAY_FACTOR * max(as.integer(nthreads), 1L)
  ram <- .sys_ram_bytes()
  if (is.na(ram)) thread_scaled else min(thread_scaled, 0.5 * ram)
}

# Features per parse chunk. The parse materializes the raw WKB for its chunk only,
# so the chunk count bounds the transient input copy that coexists with the
# accumulating cleaned WKB. Scales with total RAM (bigger chunks, fewer round
# trips, on roomy machines) around a 16 GB reference, with a safe fixed fallback.
.OVERLAY_PARSE_CHUNK <- 50000L
# RAM-scaled read/parse batch size. Keeping peak low is the goal, so this only
# scales DOWN on a memory-constrained machine (a bigger batch on a roomy machine
# just saves a few read round trips while raising the peak, which defeats the
# point); the batch never grows past the reference size.
.overlay_chunk_ram <- function() {
  ram <- .sys_ram_bytes()
  if (is.na(ram)) .OVERLAY_PARSE_CHUNK
  else as.integer(.OVERLAY_PARSE_CHUNK * max(0.25, min(1, ram / 16e9)))
}
# Batch size for reads/parse, RAM-scaled, overridable, and (sf path) capped at n.
.overlay_chunk_size <- function()
  as.integer(getOption("vectra.overlay_parse_chunk", .overlay_chunk_ram()))
.overlay_parse_chunk <- function(n)
  max(1L, min(as.integer(n), .overlay_chunk_size()))

# Coordinate magnitude of a GeoPackage layer from its declared extent
# (gpkg_contents), or NA if it cannot be read. Lets the file path derive the
# snapping grid without scanning geometry.
.overlay_source_mag <- function(path, layer) {
  ext <- tryCatch(
    sf::st_read(path, quiet = TRUE,
      query = sprintf(
        "SELECT min_x, min_y, max_x, max_y FROM gpkg_contents WHERE table_name = '%s'",
        layer)),
    error = function(e) NULL)
  if (is.null(ext) || nrow(ext) == 0L) return(NA_real_)
  v <- suppressWarnings(as.numeric(unlist(ext[1L, c("min_x", "min_y", "max_x", "max_y")])))
  v <- v[is.finite(v)]
  if (!length(v)) NA_real_ else max(abs(v))
}

# Common input path for spatial_overlay(): produce cleaned geometry (cwkb),
# bounding boxes, and attributes from either an in-memory sf object or a file
# source read in feature batches. Each batch is parsed once and the raw WKB is
# released before the next, so the transient input copy stays small. The file
# path never holds the whole layer as an sf object, so peak memory tracks the
# cleaned geometry rather than the (much larger) cost of materializing the source.
.overlay_ingest <- function(x, vars, piece, grid, nthreads,
                            layer = NULL, query = NULL, read_chunk = NULL) {
  sel_attrs <- function(df) {
    df <- as.data.frame(df)
    if (!is.null(vars)) {
      miss <- setdiff(vars, names(df))
      if (length(miss))
        stop(sprintf("vars not found in `x`: %s", paste(miss, collapse = ", ")))
      df <- df[, vars, drop = FALSE]
    }
    if (piece %in% names(df))
      stop(sprintf("piece column '%s' already exists in `x`; pass piece=", piece))
    df
  }

  if (inherits(x, "sf")) {
    crs <- sf::st_crs(x)
    n   <- nrow(x)
    if (n == 0L) stop("`x` has no geometries to overlay")
    attrs <- sel_attrs(sf::st_drop_geometry(x))
    if (is.null(grid)) grid <- .overlay_grid_mag(max(abs(sf::st_bbox(x))))
    xgeom <- sf::st_geometry(x)
    bbox  <- matrix(NA_real_, n, 4L)
    cwkb  <- vector("list", n)
    chunk <- read_chunk %||% .overlay_parse_chunk(n)
    for (s in seq.int(1L, n, by = chunk)) {
      e  <- min(s + chunk - 1L, n)
      w  <- sf::st_as_binary(xgeom[s:e], EWKB = FALSE)
      pr <- .Call(C_overlay_parse, w, as.double(grid), as.integer(nthreads))
      bbox[s:e, ] <- pr[[1L]]
      cwkb[s:e]   <- pr[[2L]]
      w <- NULL; pr <- NULL
    }
    return(list(attrs = attrs, crs = crs, n = n,
                cwkb = cwkb, bbox = bbox, grid = as.double(grid)))
  }

  if (!is.character(x) || length(x) != 1L)
    stop("`x` must be an sf object or a single file path")
  if (!file.exists(x)) stop(sprintf("file not found: %s", x))
  if (is.null(layer) && is.null(query))
    stop("reading from a file needs `layer=` (a layer name) or `query=` (SQL)")
  make_q <- function(off) {
    if (is.null(query))
      sprintf('SELECT * FROM "%s" LIMIT %d OFFSET %d', layer, chunk, off)
    else
      sprintf("SELECT * FROM (%s) LIMIT %d OFFSET %d", query, chunk, off)
  }

  if (is.null(grid)) {
    mag <- if (!is.null(layer)) .overlay_source_mag(x, layer) else NA_real_
    if (is.na(mag))
      stop("cannot derive the snapping grid from this source; pass grid= explicitly")
    grid <- .overlay_grid_mag(mag)
  }

  chunk <- read_chunk %||% .overlay_chunk_size()
  cw <- list(); bb <- list(); at <- list(); crs <- NULL; off <- 0L; ci <- 0L
  repeat {
    b  <- sf::st_read(x, query = make_q(off), quiet = TRUE)
    nb <- nrow(b)
    if (nb == 0L) break
    if (is.null(crs)) crs <- sf::st_crs(b)
    ci <- ci + 1L
    at[[ci]] <- sel_attrs(sf::st_drop_geometry(b))
    w  <- sf::st_as_binary(sf::st_geometry(b), EWKB = FALSE)
    pr <- .Call(C_overlay_parse, w, as.double(grid), as.integer(nthreads))
    bb[[ci]] <- pr[[1L]]; cw[[ci]] <- pr[[2L]]
    off <- off + nb
    b <- NULL; w <- NULL; pr <- NULL
    if (nb < chunk) break
  }
  if (ci == 0L) stop("the source returned no features to overlay")
  cwkb <- unlist(cw, recursive = FALSE, use.names = FALSE)
  list(attrs = do.call(rbind, at), crs = crs, n = length(cwkb),
       cwkb = cwkb, bbox = do.call(rbind, bb), grid = as.double(grid))
}

# Adaptive quadtree over feature bounding boxes: subdivide the extent until each
# leaf tile is within the byte and feature budgets, then return the leaves as
# overlay jobs (`idx` = global rows whose bbox meets the tile, `rect` = clip
# bounds). A feature spanning a tile edge is replicated into both tiles and
# clipped there, so pieces are split along the grid but coverage and labels stay
# exact.
.overlay_tiles <- function(bbox, idx, rect, budget_bytes, budget_feat, wbytes, depth) {
  if (length(idx) <= 1L ||
      depth >= .OVERLAY_MAX_DEPTH ||
      (length(idx) <= budget_feat && sum(wbytes[idx]) <= budget_bytes))
    return(list(list(idx = idx, rect = rect)))
  xm <- (rect[1L] + rect[3L]) / 2; ym <- (rect[2L] + rect[4L]) / 2
  quads <- list(c(rect[1L], rect[2L], xm, ym), c(xm, rect[2L], rect[3L], ym),
                c(rect[1L], ym, xm, rect[4L]), c(xm, ym, rect[3L], rect[4L]))
  # Index this cell's features only; pulling whole bbox columns here would copy the
  # entire layer at every quadtree node.
  xmin <- bbox[idx, 1L]; ymin <- bbox[idx, 2L]; xmax <- bbox[idx, 3L]; ymax <- bbox[idx, 4L]
  sels <- lapply(quads, function(q)
    idx[xmin <= q[3L] & xmax >= q[1L] & ymin <= q[4L] & ymax >= q[2L]])
  # Stop if splitting does not separate the features (large mutually-overlapping
  # features whose bounding boxes all span the cell): subdividing would replicate
  # them into every child without shrinking any, exploding the tile count. The
  # cell stays one tile; clipping still shrinks each feature to it.
  if (max(vapply(sels, length, integer(1))) >= length(idx))
    return(list(list(idx = idx, rect = rect)))
  out <- list()
  for (i in seq_along(quads))
    if (length(sels[[i]]))
      out <- c(out, .overlay_tiles(bbox, sels[[i]], quads[[i]],
                                   budget_bytes, budget_feat, wbytes, depth + 1L))
  out
}

# -- run-file accumulator (shared spill machinery) ----------------------------

# Buffer writable data.frames, flush them to `.vtr` spill files once `flush_rows`
# rows are pending, and finalize into a lazy ConcatNode whose temporary spills
# are removed when the node is garbage-collected. This is the single place the
# streamed spatial verbs (map / join / filter / clip / overlay) turn a sequence
# of per-batch data.frames into a lazy node. `push()` records the schema from the
# first frame it sees (even an empty one), so `finish()` can emit a correctly
# typed empty node when nothing matched. Returns a list of `push` and `finish`.
.run_accumulator <- function(flush_rows) {
  st <- new.env(parent = emptyenv())
  st$buf <- list(); st$buffered <- 0L; st$runs <- character(0); st$template <- NULL

  flush <- function() {
    if (!length(st$buf)) return(invisible())
    df <- if (length(st$buf) == 1L) st$buf[[1]] else do.call(rbind, st$buf)
    rf <- tempfile(fileext = ".vtr"); write_vtr(df, rf)
    st$runs <- c(st$runs, rf); st$buf <- list(); st$buffered <- 0L
  }

  push <- function(df) {
    if (is.null(st$template)) st$template <- df[0, , drop = FALSE]
    if (nrow(df)) {
      st$buf <- c(st$buf, list(df))
      st$buffered <- st$buffered + nrow(df)
      if (st$buffered >= flush_rows) flush()
    }
    invisible()
  }

  finish <- function(crs, empty_geom = "geometry") {
    flush()
    if (!length(st$runs)) {
      tmpl <- st$template
      if (is.null(tmpl))
        tmpl <- stats::setNames(
          data.frame(character(0), stringsAsFactors = FALSE), empty_geom)
      rf <- tempfile(fileext = ".vtr"); write_vtr(tmpl, rf); st$runs <- rf
    }
    node <- .concat_runs(st$runs)
    reg <- new.env(parent = emptyenv()); reg$paths <- st$runs
    reg.finalizer(reg, function(e) try(unlink(e$paths), silent = TRUE),
                  onexit = TRUE)
    node$.reg <- reg
    node$.crs <- crs
    node
  }

  list(push = push, finish = finish)
}

# -- the streaming engine -----------------------------------------------------

# Pull `x` one batch at a time, run `batch_fn` (an sf-in / sf-out function) on
# each, encode the result, and accumulate to run-files. Returns a lazy ConcatNode
# carrying the output CRS (the first sf/sfc result's CRS, else the input `crs`).
.spatial_stream <- function(x, batch_fn, geom, coords, crs, out_geom,
                            flush_rows) {
  nxt <- .batch_cursor(x)
  acc <- .run_accumulator(flush_rows)
  out_crs <- NULL
  repeat {
    chunk <- nxt(); if (is.null(chunk)) break
    sb  <- .sf_decode_chunk(chunk, geom, coords, crs)
    res <- batch_fn(sb)
    if (is.null(out_crs) && (inherits(res, "sf") || inherits(res, "sfc")))
      out_crs <- sf::st_crs(res)
    acc$push(.sf_encode_result(res, out_geom))
  }
  acc$finish(crs = if (!is.null(out_crs)) out_crs else crs, empty_geom = out_geom)
}

# Decompose one sfg into its single-part components: a MULTI* into its parts, a
# GEOMETRYCOLLECTION recursively into the parts of its members, and an already
# single-part geometry into itself. An empty multi-geometry has no parts, so it
# passes through as a single row rather than vanishing.
.parts_of <- function(g) {
  out <- switch(class(g)[2L],
    MULTIPOLYGON    = lapply(unclass(g), sf::st_polygon),
    MULTILINESTRING = lapply(unclass(g), sf::st_linestring),
    MULTIPOINT      = {
      m <- unclass(g)
      if (!length(m)) list()
      else lapply(seq_len(nrow(m)), function(i) sf::st_point(m[i, ]))
    },
    GEOMETRYCOLLECTION = unlist(lapply(g, .parts_of), recursive = FALSE),
    list(g))
  if (length(out)) out else list(g)
}

# Explode one decoded sf batch: replace each multipart feature with one row per
# part, replicating the attributes. `part` (when set) numbers the parts within
# each input feature, 1-based.
.explode_batch <- function(sb, part) {
  if (!is.null(part) && part %in% names(sb))
    stop(sprintf("part column '%s' already exists in `x`; pass part=", part))
  if (nrow(sb) == 0L) {
    if (!is.null(part)) sb[[part]] <- integer(0)
    return(sb)
  }
  g     <- sf::st_geometry(sb)
  parts <- lapply(g, .parts_of)
  np    <- lengths(parts)
  out   <- sb[rep.int(seq_len(nrow(sb)), np), , drop = FALSE]
  sf::st_geometry(out) <- sf::st_sfc(unlist(parts, recursive = FALSE),
                                     crs = sf::st_crs(g))
  if (!is.null(part))
    out[[part]] <- unlist(lapply(np, seq_len), use.names = FALSE)
  out
}

# -- front doors --------------------------------------------------------------

#' Stream a query through an sf transform
#'
#' Applies a per-feature \pkg{sf} operation (buffer, centroid, area, CRS
#' transform, simplify, ...) to a lazy vectra query one batch at a time and
#' returns a new lazy node. The engine pulls one batch, hands it to `fn` as an
#' `sf` object, encodes the result back into the stream, and spills to disk, so
#' peak memory is one batch regardless of result size. This is the streaming,
#' larger-than-RAM counterpart to running the same `sf` call on a whole
#' in-memory table.
#'
#' Geometry travels through the engine as hex-encoded WKB in an ordinary string
#' column (vectra has no native geometry type), and the coordinate reference
#' system is carried on the returned node rather than in the `.vtr` file. Use
#' [collect_sf()] to materialize the result as an `sf` object, or [collect()]
#' to get the underlying data.frame with the WKB string column.
#'
#' Topology is delegated entirely to \pkg{sf}/GEOS; vectra only supplies the
#' streaming. The `sf` package is an optional dependency (Suggests).
#'
#' @param x A `vectra_node` (from [tbl()], [tbl_tiff()], any verb chain, ...).
#'   It is consumed by the stream.
#' @param fn A function (or purrr-style formula such as `~ sf::st_buffer(.x,
#'   1000)`) taking one `sf` batch and returning an `sf` object, `sfc`, or plain
#'   data.frame. The active geometry of the return becomes the output geometry.
#' @param geom Name of the input geometry column holding hex-WKB or WKT strings.
#'   Default `"geometry"`. Ignored when `coords` is given.
#' @param coords Optional length-2 character vector naming the x and y
#'   coordinate columns to assemble point geometry from (e.g. `c("x", "y")`),
#'   for inputs such as [tiff_extract_points()] output. The coordinate columns
#'   are retained.
#' @param crs Coordinate reference system of the input geometry, in any form
#'   [sf::st_crs()] accepts (EPSG integer, WKT, proj string). Defaults to the
#'   CRS the upstream node carries, or unknown.
#' @param out_geom Name of the output geometry column. Defaults to `geom`
#'   (or `"geometry"` when `coords` is used).
#' @param flush_rows Transformed rows buffered before a spill flush. Larger
#'   values mean fewer, bigger temporary files. Defaults to
#'   `getOption("vectra.spatial_flush", 5e5)`.
#'
#' @return A `vectra_node` backed by temporary `.vtr` spills (removed when the
#'   node is garbage-collected), carrying the output CRS for [collect_sf()].
#'
#' @seealso [spatial_join()] to join a streamed side against a resident `sf`
#'   object, [collect_sf()] to materialize as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   NAME = nc$NAME,
#'   geometry = sf::st_as_binary(sf::st_centroid(sf::st_geometry(nc)),
#'                               hex = TRUE)
#' ), f)
#'
#' # Buffer every county centroid by 0.1 degree, streaming.
#' buffered <- tbl(f) |>
#'   spatial_map(~ sf::st_buffer(.x, 0.1), crs = sf::st_crs(nc))
#' collect_sf(buffered)
#' unlink(f)
#'
#' @export
spatial_map <- function(x, fn, geom = "geometry", coords = NULL, crs = NA,
                        out_geom = NULL, flush_rows = NULL) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (build one with tbl(), tbl_csv(), ...)")
  fn <- rlang::as_function(fn)
  crs <- .resolve_crs(x, crs)
  if (is.null(out_geom)) out_geom <- if (is.null(coords)) geom else "geometry"
  fr <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  .spatial_stream(x, fn, geom, coords, crs, out_geom, fr)
}

#' Explode multipart geometries into single-part features
#'
#' Streams a lazy vectra query and splits every multipart geometry into its
#' component single-part geometries: a `MULTIPOLYGON` becomes one row per
#' polygon, a `MULTILINESTRING` one row per linestring, a `MULTIPOINT` one row
#' per point, and a `GEOMETRYCOLLECTION` one row per member (recursively). The
#' attributes of the source feature are copied onto each part. Already
#' single-part geometries pass through unchanged, as does an empty geometry
#' (kept as one row). This is the streaming counterpart of the QGIS
#' "multipart to singleparts" tool and of [sf::st_cast()] to a single-part type.
#'
#' One batch is decoded, exploded, and spilled at a time, so peak memory tracks
#' one batch and its parts, not the whole layer.
#'
#' @inheritParams spatial_map
#' @param part Optional name of an integer column numbering the parts within each
#'   source feature, 1-based. Default `NULL` adds no such column.
#'
#' @return A `vectra_node` of single-part features, backed by temporary `.vtr`
#'   spills (removed when the node is garbage-collected) and carrying the input
#'   CRS.
#'
#' @seealso [spatial_map()] for per-feature transforms, [spatial_dissolve()] to
#'   merge features the other way, [collect_sf()] to materialize as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' mp <- sf::st_multipolygon(list(
#'   list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))),
#'   list(rbind(c(2, 2), c(3, 2), c(3, 3), c(2, 3), c(2, 2)))))
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   id = 1L,
#'   geometry = sf::st_as_binary(sf::st_sfc(mp), hex = TRUE)
#' ), f)
#'
#' # One row per polygon, attributes copied, parts numbered.
#' tbl(f) |> spatial_explode(part = "part_id") |> collect_sf()
#' unlink(f)
#'
#' @export
spatial_explode <- function(x, geom = "geometry", crs = NA, out_geom = NULL,
                            part = NULL, flush_rows = NULL) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (build one with tbl(), tbl_csv(), ...)")
  if (!is.null(part) && (!is.character(part) || length(part) != 1L))
    stop("`part` must be a single column name, or NULL")
  crs <- .resolve_crs(x, crs)
  if (is.null(out_geom)) out_geom <- geom
  fr <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  .spatial_stream(x, function(sb) .explode_batch(sb, part),
                  geom, coords = NULL, crs = crs, out_geom = out_geom,
                  flush_rows = fr)
}

#' Spatial join a streamed query against a resident sf object
#'
#' Streams a large left side `x` through the engine and joins each batch against
#' a small right side `y` held resident in memory, using an \pkg{sf} binary
#' predicate (`st_intersects` by default). This is the spatial analogue of a
#' hash join with the small side on the build side: the billion-row left stream
#' never materializes, while `y` (admin polygons, habitat patches, ...) stays in
#' RAM. The dominant real workload it serves is tagging huge point sets with the
#' polygon they fall in.
#'
#' For the recognised predicates -- the topological ones (intersects, within,
#' contains, overlaps, covers, covered by, touches, crosses), equals,
#' within-distance ([sf::st_is_within_distance], radius passed as `dist =`), and
#' nearest feature ([sf::st_nearest_feature]) -- on projected or unprojected
#' planar data, the match runs natively on the GEOS C API straight off the
#' hex-WKB column: `y` is parsed once into a spatial index, each batch's matches
#' come back from C, and `y`'s attributes are attached in R without decoding the
#' left side to \pkg{sf}. Coordinate-assembled (`coords`) point input runs
#' natively too, building each point in C (the emitted point geometry is built in
#' C as well). Geographic coordinates with spherical geometry on
#' (`sf::sf_use_s2()`), a disjoint join (whose matches are the bounding-box
#' complement an index cannot prune), and other extra [sf::st_join()] arguments
#' use \pkg{sf} instead, preserving its semantics.
#'
#' When both sides are larger than RAM, pass `partition = grid(cellsize)` and a
#' streamed `vectra_node` as `y`: both inputs are binned to a uniform spatial
#' grid, then joined one shard at a time. Each left feature is assigned to the
#' single grid cell of its reference point while each right feature is
#' replicated to every cell its bounding box overlaps, so a left row is emitted
#' exactly once and the result equals the resident join. This is exact for point
#' left geometries (the dominant case -- tagging a huge point set with the
#' polygon it falls in) and finds, for an extended left feature, the matches
#' whose right bounding box overlaps the left reference cell; choose a `cellsize`
#' larger than the left features for an extended-on-extended join. The partition
#' path serves topological predicates (intersects, within, contains, overlaps,
#' covers, covered by). It also serves [sf::st_nearest_feature]: because nearest
#' is not local to one cell, each left feature then searches its own cell and the
#' eight around it, so the true nearest is found when it lies within one cell of
#' the left reference cell (pick a `cellsize` at least the largest expected
#' nearest distance). Topology and CRS handling are \pkg{sf}'s; vectra supplies
#' the stream and the grid partition.
#'
#' @inheritParams spatial_map
#' @param y The right side of the join: an `sf` object held resident (the
#'   default), or -- when `partition` is given -- a streamed `vectra_node`.
#' @param join An \pkg{sf} binary predicate function, e.g. [sf::st_intersects]
#'   (default), [sf::st_within], [sf::st_contains], [sf::st_nearest_feature].
#' @param left If `TRUE` (default) keep every left row (left join); if `FALSE`
#'   keep only matches (inner join).
#' @param suffix Length-2 character vector disambiguating columns present on
#'   both sides. Default `c(".x", ".y")`.
#' @param partition Optional [grid()] specification enabling the two-sided
#'   streamed path, in which `y` is itself a `vectra_node`. Default `NULL` keeps
#'   the resident-`y` path.
#' @param y_geom,y_coords Geometry transport for a streamed `y` under
#'   `partition`: the name of `y`'s hex-WKB geometry column (`y_geom`, default
#'   the left `geom`), or a length-2 character vector of `y`'s coordinate columns
#'   (`y_coords`). Ignored without `partition`.
#' @param ... Further arguments passed to [sf::st_join()].
#'
#' @return A `vectra_node` of the joined stream, backed by temporary `.vtr`
#'   spills and carrying the left CRS.
#'
#' @seealso [spatial_map()] for per-feature transforms, [collect_sf()] to
#'   materialize as `sf`, [offload()] to partition both-sides-huge joins.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#'
#' # A stream of points, stored with x/y coordinate columns.
#' set.seed(1)
#' pts <- sf::st_coordinates(sf::st_sample(nc, 200))
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(id = seq_len(nrow(pts)), x = pts[, 1], y = pts[, 2]), f)
#'
#' # Tag each point with the county it falls in, streaming.
#' tagged <- tbl(f) |>
#'   spatial_join(nc["NAME"], join = sf::st_intersects,
#'                coords = c("x", "y"), crs = sf::st_crs(nc))
#' head(collect(tagged))
#'
#' # Both sides streamed: bin to a grid and join per shard. Here y is a
#' # vectra_node rather than a resident sf object.
#' g <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   NAME = nc$NAME,
#'   geometry = sf::st_as_binary(sf::st_geometry(nc), hex = TRUE)
#' ), g)
#' tagged2 <- tbl(f) |>
#'   spatial_join(tbl(g), coords = c("x", "y"), crs = sf::st_crs(nc),
#'                partition = grid(0.5))
#' head(collect(tagged2))
#' unlink(c(f, g))
#'
#' @export
spatial_join <- function(x, y, join = NULL, geom = "geometry", coords = NULL,
                         crs = NA, left = TRUE, suffix = c(".x", ".y"),
                         partition = NULL, y_geom = NULL, y_coords = NULL,
                         out_geom = NULL, flush_rows = NULL, ...) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed left side)")
  if (is.null(join)) join <- sf::st_intersects
  crs <- .resolve_crs(x, crs)
  if (is.null(out_geom)) out_geom <- if (is.null(coords)) geom else "geometry"
  fr <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  dots <- list(...)

  if (!is.null(partition)) {
    if (!inherits(partition, "vectra_grid"))
      stop("`partition` must be a grid() specification, or NULL")
    if (!inherits(y, "vectra_node"))
      stop("with `partition`, `y` must be a vectra_node (the streamed right side)")
    if (is.null(y_geom)) y_geom <- geom
    return(.spatial_join_partition(x, y, join, partition, geom, coords,
                                   y_geom, y_coords, crs, left, suffix,
                                   out_geom, fr, dots))
  }

  if (!inherits(y, "sf"))
    stop("`y` must be an sf object (the resident right side of the join)")

  # Native path: a recognised predicate and planar-equivalent data. `y` is parsed
  # once into a GEOS locator; each batch's per-row matches come back from C and
  # the resident attributes are joined on in R, so the streamed left side is
  # never decoded to sf. `prep_batch(chunk, loc, nt)` returns a list of the
  # (possibly geometry-augmented) left `chunk` and its per-row match lists.
  run_native_join <- function(prep_batch) {
    loc     <- .geos_locator(y)
    y_attrs <- .coerce_for_vtr(as.data.frame(sf::st_drop_geometry(y)))
    nt      <- .spatial_threads()
    acc     <- .run_accumulator(fr)
    nxt     <- .batch_cursor(x)
    repeat {
      chunk <- nxt(); if (is.null(chunk)) break
      pb   <- prep_batch(chunk, loc, nt)
      out  <- .geos_join_assemble(pb$chunk, pb$matches, y_attrs, left, suffix)
      if (!identical(out_geom, geom) && geom %in% names(out))
        names(out)[match(geom, names(out))] <- out_geom
      gcol <- if (out_geom %in% names(out)) out_geom
              else if (geom %in% names(out)) geom else NULL
      if (!is.null(gcol))
        out <- out[c(setdiff(names(out), gcol), gcol)]  # geometry last, as st_join
      acc$push(.coerce_for_vtr(out))
    }
    acc$finish(crs = crs, empty_geom = out_geom)
  }

  # The streamed geometry reaches C as the batch's hex-WKB column (geom=) or as
  # raw point coordinates built into points in C (coords=); both share the match
  # assembly above. within-distance takes its radius through `...` (`dist =`);
  # any other extra st_join argument, or disjoint (whose matches are the bbox
  # complement the index cannot prune), falls back to sf.
  if (.geos_planar_ok(crs)) {
    nearest <- !length(dots) && identical(join, sf::st_nearest_feature)
    code <- if (nearest) NA_integer_ else .geos_pred_code(join)
    dist_val <- 0
    if (!is.na(code) && code == 10L) {
      dist_val <- dots$dist
      if (is.null(dist_val) || length(setdiff(names(dots), "dist")))
        code <- NA_integer_
    } else if (length(dots) || (!is.na(code) && code == 9L)) {
      code <- NA_integer_
    }
    if (nearest || !is.na(code)) {
      d <- as.numeric(dist_val)
      hex_matches <- function(hex, loc, nt)
        if (nearest) {
          near <- .Call(C_geos_nearest, loc, hex, nt)
          lapply(near, function(k) if (is.na(k)) integer(0) else k)
        } else .Call(C_geos_join, loc, hex, code, d, nt)
      xy_matches <- function(xy, loc, nt)
        if (nearest) {
          near <- .Call(C_geos_locate_xy, loc, xy$x, xy$y, 11L, 0, FALSE, nt)
          lapply(near, function(k) if (is.na(k)) integer(0) else k)
        } else .Call(C_geos_locate_xy, loc, xy$x, xy$y, code, d, TRUE, nt)
      if (is.null(coords))
        return(run_native_join(function(chunk, loc, nt) {
          if (!geom %in% names(chunk))
            stop(sprintf("geometry column '%s' not found; pass geom= or coords=", geom))
          list(chunk = chunk,
               matches = hex_matches(as.character(chunk[[geom]]), loc, nt))
        }))
      return(run_native_join(function(chunk, loc, nt) {
        xy <- .batch_xy(chunk, coords)
        chunk[[out_geom]] <- .Call(C_geos_points_to_hex, xy$x, xy$y)
        list(chunk = chunk, matches = xy_matches(xy, loc, nt))
      }))
    }
  }

  batch_fn <- function(sb)
    do.call(sf::st_join,
            c(list(sb, y, join = join, left = left, suffix = suffix), dots))
  .spatial_stream(x, batch_fn, geom, coords, crs, out_geom, fr)
}

# -- two-sided partitioned spatial join (grid partition) ----------------------

#' Define a uniform grid for a partitioned spatial join
#'
#' Describes the regular grid that [spatial_join()] uses to partition two
#' streamed layers for the both-sides-larger-than-RAM case. Cell `(cx, cy)`
#' covers `[origin_x + cx * cellsize_x, origin_x + (cx + 1) * cellsize_x)` and
#' likewise in y, so a coordinate maps to the cell `floor((coord - origin) /
#' cellsize)`. Pick a `cellsize` comparable to the scale of the join (large
#' enough that most cells hold a workable shard, small enough that one cell's
#' features fit in memory); for an extended-on-extended join choose it larger
#' than the left features.
#'
#' @param cellsize Cell size: a single number for square cells, or
#'   `c(cellsize_x, cellsize_y)`.
#' @param origin Grid origin `c(x0, y0)` (a cell corner). Default `c(0, 0)`.
#'
#' @return A `vectra_grid` specification to pass as `spatial_join(partition =)`.
#'
#' @seealso [spatial_join()] for the join it partitions.
#'
#' @examples
#' grid(1000)
#' grid(c(0.5, 0.25), origin = c(-180, -90))
#'
#' @export
grid <- function(cellsize, origin = c(0, 0)) {
  if (!is.numeric(cellsize) || !length(cellsize) %in% 1:2 ||
      any(!is.finite(cellsize)) || any(cellsize <= 0))
    stop("`cellsize` must be one or two positive numbers")
  if (length(cellsize) == 1L) cellsize <- c(cellsize, cellsize)
  if (!is.numeric(origin) || length(origin) != 2L || any(!is.finite(origin)))
    stop("`origin` must be a length-2 numeric c(x0, y0)")
  structure(list(cellsize = as.numeric(cellsize[1:2]),
                 origin = as.numeric(origin)),
            class = "vectra_grid")
}

#' @export
print.vectra_grid <- function(x, ...) {
  cat(sprintf("<vectra grid: cellsize %g x %g, origin (%g, %g)>\n",
              x$cellsize[1L], x$cellsize[2L], x$origin[1L], x$origin[2L]))
  invisible(x)
}

# Integer cell index of a coordinate along one axis.
.grid_ix <- function(v, o, s) as.integer(floor((v - o) / s))

# Cell label "cx:cy" from integer cell indices.
.grid_label <- function(cx, cy) paste(cx, cy, sep = ":")

# Per-feature bounding boxes of an sf/sfc batch as a 4-column matrix, derived
# from the flattened vertices (sf returns the feature index in the last
# st_coordinates column for lines/polygons/multipoints), so the whole batch is
# one vectorized pass. POINT geometry carries no index column (just X, Y), so
# each row is its own degenerate bbox.
.feature_bbox <- function(g) {
  co <- sf::st_coordinates(sf::st_geometry(g))
  if (ncol(co) <= 2L)
    return(cbind(xmin = co[, "X"], xmax = co[, "X"],
                 ymin = co[, "Y"], ymax = co[, "Y"]))
  fid <- co[, ncol(co)]
  cbind(xmin = tapply(co[, "X"], fid, min), xmax = tapply(co[, "X"], fid, max),
        ymin = tapply(co[, "Y"], fid, min), ymax = tapply(co[, "Y"], fid, max))
}

# Reference-cell label (one per row) for the left side: the cell each point or
# bbox centre falls in. Coordinate columns stay sf-free; WKB geometry is decoded.
.left_cell_labels <- function(chunk, geom, coords, crs, g) {
  if (!is.null(coords)) {
    x <- as.numeric(chunk[[coords[1L]]]); y <- as.numeric(chunk[[coords[2L]]])
  } else {
    bb <- .feature_bbox(.sf_decode_chunk(chunk, geom, NULL, crs))
    x <- (bb[, "xmin"] + bb[, "xmax"]) / 2
    y <- (bb[, "ymin"] + bb[, "ymax"]) / 2
  }
  .grid_label(.grid_ix(x, g$origin[1L], g$cellsize[1L]),
              .grid_ix(y, g$origin[2L], g$cellsize[2L]))
}

# Replicate right-side rows to every grid cell their geometry's bounding box
# overlaps, returning the augmented data.frame (original columns plus `.cell`).
# A point geometry hits one cell; a polygon spanning several is repeated once
# per cell so the per-shard join is complete.
.right_replicate <- function(chunk, geom, coords, crs, g) {
  if (!is.null(coords)) {
    x <- as.numeric(chunk[[coords[1L]]]); y <- as.numeric(chunk[[coords[2L]]])
    xmin <- x; xmax <- x; ymin <- y; ymax <- y
  } else {
    bb <- .feature_bbox(.sf_decode_chunk(chunk, geom, NULL, crs))
    xmin <- bb[, "xmin"]; xmax <- bb[, "xmax"]
    ymin <- bb[, "ymin"]; ymax <- bb[, "ymax"]
  }
  cx0 <- .grid_ix(xmin, g$origin[1L], g$cellsize[1L])
  cx1 <- .grid_ix(xmax, g$origin[1L], g$cellsize[1L])
  cy0 <- .grid_ix(ymin, g$origin[2L], g$cellsize[2L])
  cy1 <- .grid_ix(ymax, g$origin[2L], g$cellsize[2L])
  nx  <- cx1 - cx0 + 1L; ny <- cy1 - cy0 + 1L
  reps <- as.numeric(nx) * as.numeric(ny)
  row <- rep.int(seq_len(nrow(chunk)), reps)
  lab <- character(sum(reps))
  at <- 0L
  for (i in seq_len(nrow(chunk))) {
    cxs <- cx0[i]:cx1[i]; cys <- cy0[i]:cy1[i]
    li  <- .grid_label(rep(cxs, times = length(cys)),
                       rep(cys, each = length(cxs)))
    lab[(at + 1L):(at + length(li))] <- li
    at <- at + length(li)
  }
  out <- chunk[row, , drop = FALSE]
  out[[".cell"]] <- lab
  rownames(out) <- NULL
  out
}

# Stream a cursor, augment each batch into rows carrying a `.cell` label, and
# route them to per-cell run-files flushed when the budget is crossed. Returns
# the named list of run-file paths per cell label. Handles the row replication
# the right side needs (augment may return more rows than it received).
.cell_router <- function(cursor, augment, budget) {
  st <- new.env(parent = emptyenv())
  st$buffers <- list(); st$runs <- list(); st$buffered <- 0
  flush_one <- function(lab) {
    bufs <- st$buffers[[lab]]
    if (is.null(bufs) || !length(bufs)) return(invisible())
    df <- if (length(bufs) == 1L) bufs[[1L]] else do.call(rbind, bufs)
    rf <- tempfile(fileext = ".vtr"); write_vtr(df, rf)
    st$runs[[lab]] <- c(st$runs[[lab]], rf); st$buffers[[lab]] <- NULL
  }
  flush_all <- function() {
    for (lab in names(st$buffers)) flush_one(lab); st$buffered <- 0
  }
  repeat {
    chunk <- cursor(); if (is.null(chunk)) break
    aug <- augment(chunk)
    if (!nrow(aug)) next
    idx <- split(seq_len(nrow(aug)), aug[[".cell"]])
    for (lab in names(idx))
      st$buffers[[lab]] <- c(st$buffers[[lab]],
                             list(aug[idx[[lab]], , drop = FALSE]))
    st$buffered <- st$buffered + nrow(aug)
    if (st$buffered >= budget) flush_all()
  }
  flush_all()
  st$runs
}

# Drop the internal `.cell` routing column before a shard is decoded to sf.
.drop_cell <- function(df) df[setdiff(names(df), ".cell")]

# Right-side run files for a left cell, gathered over the 3x3 neighbourhood. The
# nearest-feature join is not local to a single cell, so a left point searches
# its own cell and the eight around it; the true nearest is found when it lies
# within one cell of the left reference cell (pick a cellsize at least the
# largest expected nearest distance).
.halo_runs <- function(rruns, lab) {
  cc <- as.integer(strsplit(lab, ":", fixed = TRUE)[[1L]])
  labs <- character(0)
  for (dx in -1:1) for (dy in -1:1)
    labs <- c(labs, .grid_label(cc[1L] + dx, cc[2L] + dy))
  unlist(rruns[intersect(labs, names(rruns))], use.names = FALSE)
}

.spatial_join_partition <- function(x, y, join, g, geom, coords,
                                    y_geom, y_coords, crs, left, suffix,
                                    out_geom, fr, dots) {
  budget <- getOption("vectra.partition_budget", .PARTITION_BUDGET)
  halo <- identical(join, sf::st_nearest_feature)

  lruns <- .cell_router(
    .batch_cursor(x),
    function(chunk) {
      chunk[[".cell"]] <- .left_cell_labels(chunk, geom, coords, crs, g)
      .coerce_for_vtr(chunk)
    }, budget)
  on.exit(unlink(unlist(lruns, use.names = FALSE)), add = TRUE)

  # An empty right sf with y's attribute schema, for left cells with no right
  # shard (so a left-join still pads them with NA right columns).
  ytmpl <- new.env(parent = emptyenv()); ytmpl$sf <- NULL
  rruns <- .cell_router(
    .batch_cursor(y),
    function(chunk) {
      if (is.null(ytmpl$sf))
        ytmpl$sf <- .sf_decode_chunk(chunk, y_geom, y_coords, crs)[0, ]
      .coerce_for_vtr(.right_replicate(chunk, y_geom, y_coords, crs, g))
    }, budget)
  on.exit(unlink(unlist(rruns, use.names = FALSE)), add = TRUE)

  acc <- .run_accumulator(fr)
  for (lab in names(lruns)) {
    lsf <- .sf_decode_chunk(.drop_cell(collect(.concat_runs(lruns[[lab]]))),
                            geom, coords, crs)
    rpaths <- if (halo) .halo_runs(rruns, lab) else rruns[[lab]]
    rsf <- if (!is.null(rpaths) && length(rpaths))
      .sf_decode_chunk(.drop_cell(collect(.concat_runs(rpaths))),
                       y_geom, y_coords, crs)
    else ytmpl$sf
    if (is.null(rsf)) {
      if (!left) next
      rsf <- sf::st_sf(geometry = sf::st_sfc(crs = .as_crs(crs)))
    }
    if (!left && !nrow(rsf)) next
    res <- do.call(sf::st_join,
                   c(list(lsf, rsf, join = join, left = left, suffix = suffix),
                     dots))
    acc$push(.sf_encode_result(res, out_geom))
  }
  acc$finish(crs = crs, empty_geom = out_geom)
}

#' Keep streamed rows by their spatial relation to a resident layer
#'
#' Streams a large left side `x` through the engine and keeps each row whose
#' geometry satisfies an \pkg{sf} binary predicate against a small resident
#' layer `y` (select by location). This is the spatial counterpart to a
#' [semi_join()]: rows are filtered, never duplicated, and no columns are added,
#' so the output carries `x`'s schema unchanged. With `negate = TRUE` it keeps
#' the rows that do *not* match (select by location, inverted). The billion-row
#' left stream never materializes; `y` (a study region, habitat patches, a
#' coastline buffer, ...) stays resident.
#'
#' For the recognised predicates -- the topological ones (intersects, within,
#' contains, overlaps, covers, covered by, touches, crosses), equals, disjoint,
#' and within-distance ([sf::st_is_within_distance], whose radius is passed as
#' `dist =`) -- on projected or unprojected planar data, the test runs natively
#' on the GEOS C API straight off the hex-WKB column: `y` is parsed once into a
#' spatial index and each batch is tested in C, with no per-batch round-trip
#' through \pkg{sf}. Coordinate-assembled (`coords`) point input runs natively
#' too, building each point in C rather than through \pkg{sf}; disjoint is the
#' one exception there (its matches are the bounding boxes the index prunes
#' away) and keeps the \pkg{sf} loop, as it does for the join. Geographic
#' coordinates with spherical geometry on (`sf::sf_use_s2()`) and any other
#' predicate use \pkg{sf} instead, preserving its semantics. When `y` carries no
#' CRS it inherits the stream's so the predicate does not reject on a mismatch.
#'
#' @inheritParams spatial_map
#' @param y An `sf` or `sfc` object: the resident locator layer to test against.
#' @param predicate An \pkg{sf} binary predicate function, e.g.
#'   [sf::st_intersects] (default), [sf::st_within], [sf::st_covered_by],
#'   [sf::st_is_within_distance]. A left row is kept when the predicate reports
#'   at least one match against `y`.
#' @param negate If `TRUE`, keep the rows with no match instead (the inverted
#'   select-by-location). Default `FALSE`.
#' @param ... Further arguments passed to `predicate`, e.g. `dist =` for
#'   [sf::st_is_within_distance].
#'
#' @return A `vectra_node` of the kept rows with `x`'s schema, backed by
#'   temporary `.vtr` spills and carrying the input CRS.
#'
#' @seealso [spatial_join()] to tag rows with `y`'s attributes, [spatial_clip()]
#'   to cut geometry against a mask, [filter()] for attribute predicates.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#' region <- nc[nc$NAME %in% c("Ashe", "Alleghany", "Surry"), "NAME"]
#'
#' set.seed(1)
#' pts <- sf::st_coordinates(sf::st_sample(nc, 300))
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(id = seq_len(nrow(pts)), x = pts[, 1], y = pts[, 2]), f)
#'
#' # Keep only the points that fall inside the three-county region, streaming.
#' inside <- tbl(f) |>
#'   spatial_filter(region, coords = c("x", "y"), crs = sf::st_crs(nc))
#' nrow(collect(inside))
#' unlink(f)
#'
#' @export
spatial_filter <- function(x, y, predicate = NULL, negate = FALSE,
                           geom = "geometry", coords = NULL, crs = NA,
                           flush_rows = NULL, ...) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed left side)")
  if (!inherits(y, "sf") && !inherits(y, "sfc"))
    stop("`y` must be an sf or sfc object (the resident locator layer)")
  crs <- .resolve_crs(x, crs)
  y   <- .align_resident_crs(y, crs)
  fr  <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  dots <- list(...)
  acc <- .run_accumulator(fr)
  empty_geom <- if (is.null(coords)) geom else "geometry"

  # Native path: a recognised predicate and hex-WKB geometry. The resident side
  # is parsed once into a GEOS locator and each batch is tested in C straight off
  # the column. `st_is_within_distance` takes its radius through `...` (`dist =`);
  # any other extra predicate argument, coordinate-assembled (`coords`) input, or
  # an unrecognised predicate falls back to the per-batch sf loop.
  code <- .geos_pred_code(predicate)
  dist_val <- 0
  if (!is.na(code) && code == 10L) {
    dist_val <- dots$dist
    if (is.null(dist_val) || length(setdiff(names(dots), "dist")))
      code <- NA_integer_
  } else if (length(dots)) {
    code <- NA_integer_
  }
  if (is.null(coords) && !is.na(code) && .geos_planar_ok(crs)) {
    loc <- .geos_locator(y)
    nt  <- .spatial_threads()
    nxt <- .batch_cursor(x)
    repeat {
      chunk <- nxt(); if (is.null(chunk)) break
      if (!geom %in% names(chunk))
        stop(sprintf("geometry column '%s' not found; pass geom= or coords=", geom))
      hit <- .Call(C_geos_filter, loc, as.character(chunk[[geom]]),
                   code, isTRUE(negate), as.numeric(dist_val), nt)
      acc$push(.coerce_for_vtr(chunk[hit, , drop = FALSE]))
    }
    return(acc$finish(crs = crs, empty_geom = empty_geom))
  }

  # Native coords path: raw x/y point columns matched in C with no per-batch sf.
  # Disjoint (its matches are the bbox complement the index cannot prune) keeps
  # the sf loop, as it does for the join.
  if (!is.null(coords) && !is.na(code) && code != 9L && .geos_planar_ok(crs)) {
    loc <- .geos_locator(y)
    nt  <- .spatial_threads()
    nxt <- .batch_cursor(x)
    repeat {
      chunk <- nxt(); if (is.null(chunk)) break
      xy  <- .batch_xy(chunk, coords)
      idx <- .Call(C_geos_locate_xy, loc, xy$x, xy$y, code,
                   as.numeric(dist_val), FALSE, nt)
      hit <- !is.na(idx)
      if (negate) hit <- !hit
      acc$push(.coerce_for_vtr(chunk[hit, , drop = FALSE]))
    }
    return(acc$finish(crs = crs, empty_geom = empty_geom))
  }

  if (is.null(predicate)) predicate <- sf::st_intersects
  nxt <- .batch_cursor(x)
  repeat {
    chunk <- nxt(); if (is.null(chunk)) break
    sb  <- .sf_decode_chunk(chunk, geom, coords, crs)
    hit <- lengths(do.call(predicate, c(list(sb, y), dots))) > 0L
    if (negate) hit <- !hit
    acc$push(.coerce_for_vtr(chunk[hit, , drop = FALSE]))
  }
  acc$finish(crs = crs, empty_geom = empty_geom)
}

#' Clip or erase a streamed layer against a resident mask
#'
#' Streams a large layer `x` through the engine and cuts each batch's geometry
#' against a small resident `mask` (a study boundary, a buffer, a set of
#' patches). By default this clips -- the intersection with the mask, the GIS
#' "Clip" tool -- keeping only the parts of `x` that fall inside `mask`. With
#' `erase = TRUE` it instead erases -- the difference, the "Erase"/"Difference"
#' tool -- keeping the parts of `x` outside `mask`. The mask is dissolved to a
#' single geometry once and held resident while the billion-row left stream
#' flows past one batch at a time.
#'
#' Geometry travels through the engine as hex-encoded WKB in a string column and
#' the CRS is carried on the returned node; use [collect_sf()] to materialize.
#' On projected or unprojected planar data the cut runs natively on the GEOS C
#' API straight off the hex-WKB column (the mask parsed once); geographic
#' coordinates with spherical geometry on (`sf::sf_use_s2()`) and
#' coordinate-assembled (`coords`) input cut through \pkg{sf} instead. When
#' `mask` carries no CRS it inherits the stream's.
#'
#' @inheritParams spatial_map
#' @param mask An `sf` or `sfc` object whose dissolved geometry clips (or, with
#'   `erase = TRUE`, erases) the stream.
#' @param erase If `TRUE`, keep the parts of `x` *outside* `mask` (difference)
#'   rather than inside (intersection). Default `FALSE`.
#'
#' @return A `vectra_node` of the cut geometry with `x`'s attributes, backed by
#'   temporary `.vtr` spills and carrying the input CRS.
#'
#' @seealso [spatial_filter()] to keep whole features by location without
#'   cutting them, [spatial_map()] for per-feature transforms, [collect_sf()].
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#' mask <- sf::st_union(nc[nc$NAME %in% c("Ashe", "Alleghany"), ])
#'
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   NAME = nc$NAME,
#'   geometry = sf::st_as_binary(sf::st_geometry(nc), hex = TRUE)
#' ), f)
#'
#' # Clip every county polygon to the two-county mask, streaming.
#' clipped <- tbl(f) |> spatial_clip(mask, crs = sf::st_crs(nc))
#' collect_sf(clipped)
#' unlink(f)
#'
#' @export
spatial_clip <- function(x, mask, erase = FALSE, geom = "geometry",
                         coords = NULL, crs = NA, out_geom = NULL,
                         flush_rows = NULL) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed layer to clip)")
  if (!inherits(mask, "sf") && !inherits(mask, "sfc"))
    stop("`mask` must be an sf or sfc object (the resident clip/erase mask)")
  crs    <- .resolve_crs(x, crs)
  mask   <- .align_resident_crs(mask, crs)
  mask_u <- sf::st_union(sf::st_geometry(mask))   # one resident mask geometry
  if (is.null(out_geom)) out_geom <- if (is.null(coords)) geom else "geometry"
  fr <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)

  # Native path: cut each batch's hex-WKB geometry against the resident mask in
  # C (intersection to clip, difference to erase), parsing the mask once. The
  # coordinate-assembled (`coords`) and spherical (geographic + s2) cases fall
  # back to the per-batch sf loop.
  if (is.null(coords) && .geos_planar_ok(crs)) {
    loc <- .geos_locator(mask_u)
    nt  <- .spatial_threads()
    acc <- .run_accumulator(fr)
    nxt <- .batch_cursor(x)
    repeat {
      chunk <- nxt(); if (is.null(chunk)) break
      if (!geom %in% names(chunk))
        stop(sprintf("geometry column '%s' not found; pass geom= or coords=", geom))
      cut  <- .Call(C_geos_clip, loc, as.character(chunk[[geom]]),
                    isTRUE(erase), nt)
      keep <- !is.na(cut)
      df   <- chunk[keep, setdiff(names(chunk), geom), drop = FALSE]
      df[[out_geom]] <- cut[keep]
      acc$push(.coerce_for_vtr(df))
    }
    return(acc$finish(crs = crs, empty_geom = out_geom))
  }

  op <- if (erase) sf::st_difference else sf::st_intersection
  # st_intersection/st_difference warn once per batch that attributes are
  # assumed spatially constant; that is the intended behaviour here, so mute
  # just that warning rather than letting it repeat for every streamed batch.
  batch_fn <- function(sb)
    withCallingHandlers(
      op(sb, mask_u),
      warning = function(w) {
        if (grepl("assumed.*spatially constant", conditionMessage(w)))
          invokeRestart("muffleWarning")
      })
  .spatial_stream(x, batch_fn, geom, coords, crs, out_geom, fr)
}

# -- dissolve (aggregate geometries by group) ---------------------------------

# Composite group label for a batch: one string per row joining the `by`
# column values (unit separator, unlikely to collide), or a constant when no
# `by` is given (dissolve the whole layer into one feature).
.dissolve_assign <- function(by) {
  if (is.null(by)) return(function(chunk) rep("all", nrow(chunk)))
  function(chunk) {
    parts <- lapply(by, function(b) as.character(chunk[[b]]))
    do.call(paste, c(parts, sep = ""))
  }
}

#' Dissolve geometries by group
#'
#' Unions the geometries within each `by` group into a single feature (the GIS
#' "Dissolve" tool), optionally summarising attributes. Unlike the streamed
#' per-batch verbs, dissolve needs every geometry of a group together to union
#' them, so it rides the **partition tier**: `x` is spilled once and routed into
#' one disjoint shard per group in a single bounded pass, then each shard is read
#' in and unioned with \pkg{sf}. Peak memory is the routing budget during the
#' pass, then one group's geometries while it is unioned -- partition the input
#' on a key whose groups fit in memory. With no `by`, the whole layer dissolves
#' into one feature.
#'
#' Geometry travels through the engine as hex-encoded WKB in a string column and
#' the CRS is carried on the returned node; use [collect_sf()] to materialize.
#' On projected or unprojected planar data each group is unioned natively on the
#' GEOS C API straight off the hex-WKB column; geographic coordinates with
#' spherical geometry on (`sf::sf_use_s2()`), or any extra [sf::st_union()]
#' arguments (e.g. `is_coverage = TRUE`), union through \pkg{sf} instead. The
#' \pkg{sf} package is an optional dependency (Suggests).
#'
#' @inheritParams spatial_map
#' @param by Character vector of attribute columns to dissolve within: one
#'   output feature per distinct combination of their values. `NULL` (default)
#'   dissolves the entire layer into a single feature.
#' @param ... Further arguments passed to [sf::st_union()] (e.g.
#'   `is_coverage = TRUE`).
#' @param .fun Optional named list of attribute summaries. Each element is a
#'   function taking the group's data.frame and returning a length-1 value; the
#'   list name becomes the output column (e.g.
#'   `.fun = list(total = function(d) sum(d$pop))`). Default `NULL` keeps only
#'   the `by` columns and the dissolved geometry.
#'
#' @return A `vectra_node` of one row per group -- the `by` columns, any `.fun`
#'   summaries, and the dissolved geometry -- backed by temporary `.vtr` spills
#'   removed when the node is garbage-collected, carrying the input CRS for
#'   [collect_sf()].
#'
#' @seealso [spatial_overlay()] to split overlaps apart rather than merge them,
#'   [offload()] for the partition tier this rides on, [collect_sf()].
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#' nc$band <- nc$SID74 > 5            # an attribute to dissolve within
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   band = nc$band, BIR74 = nc$BIR74,
#'   geometry = sf::st_as_binary(sf::st_geometry(nc), hex = TRUE)
#' ), f)
#'
#' # Merge the counties into two features by `band`, summing births.
#' merged <- tbl(f) |>
#'   spatial_dissolve(by = "band", crs = sf::st_crs(nc),
#'                    .fun = list(births = function(d) sum(d$BIR74)))
#' collect_sf(merged)
#' unlink(f)
#'
#' @export
spatial_dissolve <- function(x, by = NULL, ..., geom = "geometry", crs = NA,
                             .fun = NULL, flush_rows = NULL) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed layer to dissolve)")
  if (!is.null(by) && !is.character(by))
    stop("`by` must be a character vector of column names, or NULL")
  if (!is.null(.fun) && (!is.list(.fun) || is.null(names(.fun)) ||
                         any(names(.fun) == "")))
    stop("`.fun` must be a named list of functions, or NULL")
  crs <- .resolve_crs(x, crs)
  dots <- list(...)

  spill <- tempfile(fileext = ".vtr")
  on.exit(unlink(spill), add = TRUE)
  write_vtr(x, spill)

  schema <- .Call(C_node_schema, tbl(spill)$.node)
  miss <- setdiff(c(by, geom), schema$name)
  if (length(miss))
    stop(sprintf("column(s) not found in the stream: %s",
                 paste(miss, collapse = ", ")))

  budget <- getOption("vectra.partition_budget", .PARTITION_BUDGET)
  res <- .partition_router(spill, .dissolve_assign(by), budget)
  on.exit(unlink(unlist(res$runs, use.names = FALSE)), add = TRUE)

  fr  <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  acc <- .run_accumulator(fr)
  for (lab in sort(names(res$runs))) {
    df <- collect(.concat_runs(res$runs[[lab]]))
    # Native union off the hex-WKB column. Extra st_union arguments (e.g.
    # is_coverage = TRUE) are not expressible through GEOSUnaryUnion, and
    # geographic data with s2 on unions on the sphere, so both union through sf
    # instead; the planar projected case unions natively.
    if (length(dots) || !.geos_planar_ok(crs)) {
      sb     <- .sf_decode_chunk(df, geom, NULL, crs)
      u      <- do.call(sf::st_union, c(list(sf::st_geometry(sb)), dots))
      u_hex  <- sf::st_as_binary(u, hex = TRUE)
    } else {
      u_hex  <- .Call(C_geos_union_hex, as.character(df[[geom]]))
    }
    row <- if (is.null(by)) df[1, character(0), drop = FALSE]
           else df[1, by, drop = FALSE]
    if (!is.null(.fun))
      for (nm in names(.fun)) row[[nm]] <- .fun[[nm]](df)
    row[[geom]] <- u_hex
    rownames(row) <- NULL
    acc$push(.coerce_for_vtr(row))
  }
  acc$finish(crs = crs, empty_geom = geom)
}

# -- set-wise geometry constructions ------------------------------------------

# The constructions that need every feature of a group at once. Each maps the
# group's combined geometry `gu` (a length-1 sfc) to the construction: the
# enclosing kinds return one geometry, the tessellation kinds (voronoi,
# delaunay) return one polygon per cell. `pole` is the centre of the maximum
# inscribed circle (the point inside the shape farthest from its edges, the QGIS
# "pole of inaccessibility"). The inscribed-circle and pole kinds need a positive
# tolerance; the caller supplies one derived from the extent when none is given.
.CONSTRUCT_KINDS <- c("convex_hull", "concave_hull", "envelope", "oriented_box",
                      "enclosing_circle", "inscribed_circle", "pole",
                      "voronoi", "delaunay")

# Tolerance for the kinds that require one. inscribed_circle/pole take a small
# fraction of the bounding-box diagonal so the circle is found to a sensible
# precision; the tessellations accept 0 (GEOS picks its own snap).
.construct_tol <- function(gu, kind) {
  if (!kind %in% c("inscribed_circle", "pole")) return(0)
  bb <- sf::st_bbox(gu)
  d  <- sqrt((bb[["xmax"]] - bb[["xmin"]])^2 + (bb[["ymax"]] - bb[["ymin"]])^2)
  if (is.finite(d) && d > 0) d * 1e-3 else 1e-6
}

# Build the construction for one group. `gu` is the group's combined geometry
# (an sfc of length 1); returns an sfc of the result (length 1 for the enclosing
# kinds, one polygon per cell for the tessellations). The inscribed-circle path
# drops the empty companion geometry sf returns alongside the circle.
.construct_group <- function(gu, kind, ratio, allow_holes, tol) {
  switch(kind,
    convex_hull      = sf::st_convex_hull(gu),
    concave_hull     = sf::st_concave_hull(gu, ratio = ratio,
                                           allow_holes = allow_holes),
    envelope         = sf::st_as_sfc(sf::st_bbox(gu)),
    oriented_box     = sf::st_minimum_rotated_rectangle(gu),
    enclosing_circle = sf::st_minimum_bounding_circle(gu),
    inscribed_circle = {
      ic <- sf::st_inscribed_circle(gu, dTolerance = tol)
      ic[!sf::st_is_empty(ic)]
    },
    pole = {
      ic <- sf::st_inscribed_circle(gu, dTolerance = tol)
      ic <- ic[!sf::st_is_empty(ic)]
      suppressWarnings(sf::st_centroid(ic))
    },
    voronoi  = sf::st_collection_extract(
                 sf::st_voronoi(gu, dTolerance = tol), "POLYGON"),
    delaunay = sf::st_collection_extract(
                 sf::st_triangulate(gu, dTolerance = tol), "POLYGON"))
}

#' Build a set-wise geometry construction, optionally per group
#'
#' Constructs one geometry (or a tessellation) from a whole set of features --
#' the constructions a per-feature [spatial_map()] cannot express because they
#' need every feature in scope at once. Like [spatial_dissolve()] it rides the
#' **partition tier**: `x` is spilled once and routed into one disjoint shard per
#' `by` group in a single bounded pass, then each shard's geometry is combined
#' and the construction built with \pkg{sf}. With no `by`, the whole layer yields
#' one construction. Peak memory is the routing budget during the pass, then one
#' group's geometry while it is built -- partition on a key whose groups fit in
#' memory.
#'
#' `kind` selects the construction:
#' \describe{
#'   \item{`"convex_hull"`}{the convex hull of the set.}
#'   \item{`"concave_hull"`}{the concave hull (`ratio`, `allow_holes`).}
#'   \item{`"envelope"`}{the axis-aligned bounding rectangle.}
#'   \item{`"oriented_box"`}{the minimum-area rotated bounding rectangle.}
#'   \item{`"enclosing_circle"`}{the minimum bounding circle.}
#'   \item{`"inscribed_circle"`}{the maximum inscribed circle (largest circle
#'     that fits inside the set's union).}
#'   \item{`"pole"`}{the pole of inaccessibility -- the centre of the maximum
#'     inscribed circle, the point inside the shape farthest from its edges.}
#'   \item{`"voronoi"`}{the Voronoi tessellation, one polygon per cell.}
#'   \item{`"delaunay"`}{the Delaunay triangulation, one polygon per triangle.}
#' }
#' The enclosing kinds and `pole` emit one feature per group; `voronoi` and
#' `delaunay` emit one feature per cell, each carrying the group's `by` values.
#'
#' Geometry travels through the engine as hex-encoded WKB in a string column and
#' the CRS is carried on the returned node; use [collect_sf()] to materialize.
#' Topology is \pkg{sf}/GEOS throughout (an optional dependency, Suggests); some
#' constructions need projected coordinates.
#'
#' @inheritParams spatial_map
#' @param kind The construction to build; one of the values above.
#' @param by Character vector of attribute columns to construct within: one
#'   construction (or tessellation) per distinct combination of their values.
#'   `NULL` (default) builds a single construction from the whole layer.
#' @param ratio For `kind = "concave_hull"`, the concaveness in `[0, 1]` (1 is
#'   the convex hull). Default `0.3`.
#' @param allow_holes For `kind = "concave_hull"`, whether the hull may contain
#'   holes. Default `FALSE`.
#' @param tolerance Distance tolerance for the kinds that take one
#'   (`"inscribed_circle"`, `"pole"`, `"voronoi"`, `"delaunay"`). `0` (default)
#'   lets the inscribed-circle kinds derive a tolerance from the extent and the
#'   tessellations use the GEOS default.
#'
#' @return A `vectra_node` of the construction -- one row per group for the
#'   enclosing kinds, one row per cell for the tessellations -- carrying the
#'   `by` columns and the input CRS, backed by temporary `.vtr` spills removed
#'   when the node is garbage-collected.
#'
#' @seealso [spatial_dissolve()] to merge a group into one feature,
#'   [spatial_map()] for per-feature transforms, [collect_sf()] to materialize.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#' nc$band <- nc$SID74 > 5
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   band = nc$band,
#'   geometry = sf::st_as_binary(sf::st_geometry(nc), hex = TRUE)
#' ), f)
#'
#' # One convex hull per band.
#' tbl(f) |>
#'   spatial_construct("convex_hull", by = "band", crs = sf::st_crs(nc)) |>
#'   collect_sf()
#' unlink(f)
#'
#' @export
spatial_construct <- function(x, kind = .CONSTRUCT_KINDS, by = NULL,
                              geom = "geometry", crs = NA, ratio = 0.3,
                              allow_holes = FALSE, tolerance = 0,
                              flush_rows = NULL) {
  .check_sf()
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (the streamed layer to construct from)")
  kind <- match.arg(kind)
  if (!is.null(by) && !is.character(by))
    stop("`by` must be a character vector of column names, or NULL")
  crs <- .resolve_crs(x, crs)

  spill <- tempfile(fileext = ".vtr")
  on.exit(unlink(spill), add = TRUE)
  write_vtr(x, spill)

  schema <- .Call(C_node_schema, tbl(spill)$.node)
  miss <- setdiff(c(by, geom), schema$name)
  if (length(miss))
    stop(sprintf("column(s) not found in the stream: %s",
                 paste(miss, collapse = ", ")))

  budget <- getOption("vectra.partition_budget", .PARTITION_BUDGET)
  res <- .partition_router(spill, .dissolve_assign(by), budget)
  on.exit(unlink(unlist(res$runs, use.names = FALSE)), add = TRUE)

  fr  <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  acc <- .run_accumulator(fr)
  for (lab in sort(names(res$runs))) {
    df <- collect(.concat_runs(res$runs[[lab]]))
    sb <- .sf_decode_chunk(df, geom, NULL, crs)
    gu <- sf::st_union(sf::st_geometry(sb))
    tol <- if (tolerance > 0) tolerance else .construct_tol(gu, kind)
    out <- .construct_group(gu, kind, ratio, allow_holes, tol)
    out <- out[!sf::st_is_empty(out)]
    if (!length(out)) next
    rowdf <- if (is.null(by)) data.frame(matrix(nrow = length(out), ncol = 0))
             else df[rep(1L, length(out)), by, drop = FALSE]
    rowdf[[geom]] <- sf::st_as_binary(out, hex = TRUE)
    rownames(rowdf) <- NULL
    acc$push(.coerce_for_vtr(rowdf))
  }
  acc$finish(crs = crs, empty_geom = geom)
}

#' Rasterize a streamed point layer onto a fixed grid
#'
#' Folds a larger-than-RAM stream of points into a fixed raster grid one batch
#' at a time. The grid (`template`) is held resident in memory while the points
#' flow past the engine, so peak memory is the grid plus one batch regardless of
#' how many points there are -- the streaming counterpart to running
#' `terra::rasterize()` on a point set that has to fit in RAM. Each point's
#' coordinate is mapped to its grid cell through the raster geotransform and the
#' per-cell value is accumulated in C.
#'
#' The reduction `fun` is a monoid over the points falling in each cell:
#' `"count"` tallies points (no `field` needed); `"sum"`, `"mean"`, `"min"`,
#' `"max"` aggregate a numeric `field`. Cells that receive no point take the
#' `background` value (`NA` by default). This is the *monoid fold* tier of the
#' spatial toolbox: bounded memory, a single streaming pass, no spill.
#'
#' Points arrive either as two numeric coordinate columns (`coords`, the default
#' and fully \pkg{sf}-free path -- the headline larger-than-RAM case) or decoded
#' from a hex-WKB point-geometry column (`geom`, which needs \pkg{sf}). Geometry
#' input is expected to be points (one coordinate per row); line and polygon
#' coverage rasterization is out of scope here.
#'
#' @param x A `vectra_node` streaming the points (from [tbl()], [tbl_csv()], any
#'   verb chain). It is consumed by the stream.
#' @param template Optional grid to borrow geometry and CRS from: a
#'   `vectra_raster` (from [vec_open_raster()]), or a numeric
#'   `c(xmin, ymin, xmax, ymax)` extent. When omitted, supply `extent` with
#'   `res` or `dims`.
#' @param field Name of a numeric column to aggregate. Required for every `fun`
#'   except `"count"` (which ignores it).
#' @param fun Reduction over the points in each cell: one of `"count"`, `"sum"`,
#'   `"mean"`, `"min"`, `"max"`. `NA` values in `field` are skipped.
#' @param extent Numeric `c(xmin, ymin, xmax, ymax)` defining the grid extent
#'   when no `template` is given.
#' @param res Cell size: a single number for square cells, or `c(xres, yres)`.
#'   The cell counts are rounded to fit `extent` exactly. Supply `res` or `dims`.
#' @param dims Grid shape `c(nrow, ncol)`, an alternative to `res`.
#' @param coords Length-2 character vector naming the x and y coordinate
#'   columns. Default `c("x", "y")`. Ignored when `geom` is supplied.
#' @param geom Name of a hex-WKB point-geometry column to rasterize instead of
#'   coordinate columns. Requires \pkg{sf}.
#' @param crs Coordinate reference system recorded on the output, in any form
#'   [sf::st_crs()] accepts or a bare EPSG integer. Defaults to the template's,
#'   then the node's, else unknown.
#' @param background Value for cells that receive no point. Default `NA_real_`.
#' @param path Optional output path. When given, the grid is written to a `.vec`
#'   raster via [vec_write_raster()] and the opened [vec_open_raster()] handle is
#'   returned invisibly. When `NULL`, the grid is returned in memory.
#' @param dtype Storage dtype for the `.vec` output (see [vec_write_raster()]).
#'   Default `"f32"`.
#'
#' @return When `path` is `NULL`, a numeric matrix with `nrow` grid rows
#'   (row 1 northmost) and `ncol` grid columns, carrying `gt`, `extent`, `res`,
#'   `crs`, and `fun` attributes. When `path` is given, the written
#'   `vectra_raster` handle (invisibly).
#'
#' @seealso [vec_write_raster()] and [vec_to_tiff()] for raster output,
#'   [spatial_join()] to instead tag points with polygon attributes.
#'
#' @examples
#' set.seed(1)
#' n <- 1e4
#' pts <- data.frame(x = runif(n, 0, 10), y = runif(n, 0, 10), z = rnorm(n))
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(pts, f)
#'
#' # Point density on a 10x10 grid, streamed: the grid is resident, the
#' # points are not.
#' counts <- tbl(f) |> rasterize(extent = c(0, 0, 10, 10), dims = c(10, 10))
#' counts
#'
#' # Mean of z per cell.
#' zmean <- tbl(f) |>
#'   rasterize(extent = c(0, 0, 10, 10), dims = c(10, 10),
#'             field = "z", fun = "mean")
#' unlink(f)
#'
#' @export
rasterize <- function(x, template = NULL, field = NULL,
                      fun = c("count", "sum", "mean", "min", "max"),
                      extent = NULL, res = NULL, dims = NULL,
                      coords = c("x", "y"), geom = NULL, crs = NA,
                      background = NA_real_, path = NULL, dtype = "f32") {
  if (!inherits(x, "vectra_node"))
    stop("`x` must be a vectra_node (build one with tbl(), tbl_csv(), ...)")
  fun <- match.arg(fun)
  fun_code <- switch(fun, count = 0L, sum = 1L, mean = 2L, min = 3L, max = 4L)
  if (fun != "count" && is.null(field))
    stop(sprintf("fun = \"%s\" needs a `field` column to aggregate", fun))

  crs  <- .resolve_crs(x, crs)
  grid <- .rasterize_grid(template, extent, res, dims, crs)
  crs  <- grid$crs

  use_geom <- !is.null(geom)
  if (use_geom) .check_sf()

  acc <- .Call(C_rasterize_new,
               c(as.integer(grid$width), as.integer(grid$height)),
               as.numeric(grid$gt), fun_code)

  nxt <- .batch_cursor(x)
  repeat {
    chunk <- nxt(); if (is.null(chunk)) break
    if (use_geom) {
      sb <- .sf_decode_chunk(chunk, geom, NULL, crs)
      xy <- sf::st_coordinates(sf::st_geometry(sb))
      if (nrow(xy) != nrow(chunk))
        stop("geom rasterize expects point geometry (one coordinate per row)")
      xs <- as.numeric(xy[, "X"]); ys <- as.numeric(xy[, "Y"])
    } else {
      miss <- setdiff(coords, names(chunk))
      if (length(miss))
        stop(sprintf("coords column(s) not found: %s",
                     paste(miss, collapse = ", ")))
      xs <- as.numeric(chunk[[coords[1L]]])
      ys <- as.numeric(chunk[[coords[2L]]])
    }
    vals <- NULL
    if (!is.null(field)) {
      if (!field %in% names(chunk))
        stop(sprintf("field column '%s' not found", field))
      vals <- as.numeric(chunk[[field]])
    }
    .Call(C_rasterize_push, acc, xs, ys, vals)
  }

  m <- .Call(C_rasterize_finish, acc, as.numeric(background))
  attr(m, "gt")     <- grid$gt
  attr(m, "extent") <- grid$extent
  attr(m, "res")    <- grid$res
  attr(m, "crs")    <- crs
  attr(m, "fun")    <- fun

  if (!is.null(path)) {
    vec_write_raster(m, path, dtype = dtype, gt = grid$gt,
                     epsg = grid$epsg, nodata = as.numeric(background))
    return(invisible(vec_open_raster(path)))
  }
  m
}

# -- zonal statistics (per-zone raster summaries) -----------------------------

# Per-zone running moments, keyed by zone label. n / s (sum) / ss (sum of
# squares) combine additively across strips; mn / mx combine by pmin / pmax;
# `na` records zones tainted by an NA value when na.rm = FALSE. Each is a named
# numeric vector indexed by the zone label, grown by union as new zones appear.
.zonal_acc <- function() {
  e <- new.env(parent = emptyenv())
  e$n <- e$s <- e$ss <- e$mn <- e$mx <- stats::setNames(numeric(0), character(0))
  e$na <- character(0)
  e
}

.zonal_merge_add <- function(a, b) {
  if (!length(a)) return(b)
  nm  <- union(names(a), names(b))
  out <- stats::setNames(numeric(length(nm)), nm)
  out[names(a)] <- out[names(a)] + a
  out[names(b)] <- out[names(b)] + b
  out
}

.zonal_merge_fun <- function(a, b, f) {
  if (!length(a)) return(b)
  nm <- union(names(a), names(b))
  oa <- stats::setNames(rep(NA_real_, length(nm)), nm); oa[names(a)] <- a
  ob <- stats::setNames(rep(NA_real_, length(nm)), nm); ob[names(b)] <- b
  stats::setNames(f(oa, ob, na.rm = TRUE), nm)
}

# Fold one strip's (zone, value) pixels into the accumulator. `z` and `v` are
# aligned vectors; NA handling has already split out tainted zones for the
# na.rm = FALSE path via `tainted`.
.zonal_update <- function(e, z, v, tainted = character(0)) {
  if (length(tainted)) e$na <- union(e$na, tainted)
  keep <- !is.na(z) & !is.na(v)
  z <- z[keep]; v <- v[keep]
  if (!length(z)) return(invisible())
  zc  <- as.character(z)
  agg <- rowsum(cbind(rep.int(1, length(v)), v, v * v), zc)
  labs <- rownames(agg)
  e$n  <- .zonal_merge_add(e$n,  stats::setNames(agg[, 1L], labs))
  e$s  <- .zonal_merge_add(e$s,  stats::setNames(agg[, 2L], labs))
  e$ss <- .zonal_merge_add(e$ss, stats::setNames(agg[, 3L], labs))
  e$mn <- .zonal_merge_fun(e$mn, tapply(v, zc, min), pmin)
  e$mx <- .zonal_merge_fun(e$mx, tapply(v, zc, max), pmax)
  invisible()
}

# Resolve a value/zone raster argument that may be a path or an open handle.
# Returns list(r, close) so the caller closes only handles it opened.
.zonal_open <- function(x, what) {
  if (inherits(x, "vectra_raster")) return(list(r = x, close = FALSE))
  if (is.character(x) && length(x) == 1L)
    return(list(r = vec_open_raster(x), close = TRUE))
  stop(sprintf("`%s` must be a vectra_raster or a path to a .vec raster", what))
}

# Pixel-centre coordinates for a strip of rows r0:r1 over the full width, in the
# column-major order that as.vector() of the read window produces (row within
# strip varies fastest). gt is the value raster's north-up geotransform.
.zonal_strip_xy <- function(gt, r0, r1, width) {
  h <- r1 - r0 + 1L
  rows_idx <- rep.int(r0:r1, width)
  cols_idx <- rep(seq_len(width), each = h)
  list(x = gt[1L] + (cols_idx - 0.5) * gt[2L],
       y = gt[4L] + (rows_idx - 0.5) * gt[6L])
}

#' Summarise raster values within zones
#'
#' Reduces a raster to one summary row per zone, streaming the raster one
#' tile-row strip at a time so the whole grid never has to be resident. Zones
#' come either from a second raster aligned to the value grid (each pixel's zone
#' is that raster's value, the \code{terra::zonal} pattern) or from an \pkg{sf}
#' polygon layer (each pixel is assigned the polygon its centre falls in). The
#' per-zone running moments (count, sum, sum of squares, min, max) are folded in
#' memory as strips arrive, so peak memory is one strip plus the small per-zone
#' table regardless of raster size. This is the *monoid fold* tier of the
#' spatial toolbox: bounded memory, a single streaming pass, no spill.
#'
#' `sd` is derived from the streamed moments
#' (`sqrt((sum2 - sum^2 / n) / (n - 1))`), so it needs no second pass. With
#' `na.rm = TRUE` (the default) nodata pixels are skipped; with `na.rm = FALSE`
#' any nodata pixel in a zone makes that zone's `sum`/`mean`/`min`/`max`/`sd`
#' `NA`, matching the resident behaviour. `count` always reports the number of
#' non-nodata cells in the zone.
#'
#' Polygon zones assign each pixel centre to a polygon natively on the GEOS C
#' API (the polygons parsed once into a spatial index, every strip's centres
#' located in C), so \pkg{sf} is touched only to read the polygons in; geographic
#' polygons with spherical geometry on (`sf::sf_use_s2()`) keep the \pkg{sf}
#' point-in-polygon path. Raster zones are fully \pkg{sf}-free. The zone raster
#' must share the value raster's dimensions and geotransform.
#'
#' @param raster A `vectra_raster` (from [vec_open_raster()]) or a path to a
#'   `.vec` raster holding the values to summarise.
#' @param zones The zones to summarise within: a `vectra_raster` / `.vec` path
#'   aligned to `raster` (zone id per pixel), or an `sf`/`sfc` polygon layer.
#' @param fun One or more of `"mean"`, `"sum"`, `"count"`, `"min"`, `"max"`,
#'   `"sd"`. Each becomes a column in the result. Default `"mean"`.
#' @param band Band of the value `raster` to summarise (1-based). Default 1.
#' @param zone_band Band of a raster `zones` holding the zone ids. Default 1.
#' @param zone_field For an `sf` `zones` layer, the column giving each polygon's
#'   zone id. Default `NULL` uses the polygon row index `1:n`.
#' @param na.rm If `TRUE` (default) skip nodata pixels; if `FALSE` let a nodata
#'   pixel propagate `NA` to its zone's statistics.
#'
#' @return A data.frame with a `zone` column (sorted) followed by one column per
#'   `fun`, one row per zone.
#'
#' @seealso [rasterize()] to build a value raster from streamed points,
#'   [vec_open_raster()] to open the inputs.
#'
#' @examples
#' # A value raster and an aligned 2x2-block zone raster on a 4x4 grid.
#' vals <- matrix(1:16, 4, 4, byrow = TRUE)
#' zone <- matrix(c(1, 1, 2, 2, 1, 1, 2, 2,
#'                  3, 3, 4, 4, 3, 3, 4, 4), 4, 4, byrow = TRUE)
#' fv <- tempfile(fileext = ".vec"); fz <- tempfile(fileext = ".vec")
#' vec_write_raster(vals, fv, dtype = "f64", extent = c(0, 0, 4, 4))
#' vec_write_raster(zone, fz, dtype = "f64", extent = c(0, 0, 4, 4))
#'
#' zonal(fv, fz, fun = c("mean", "sum", "count"))
#' unlink(c(fv, fz))
#'
#' @export
zonal <- function(raster, zones, fun = "mean", band = 1L, zone_band = 1L,
                  zone_field = NULL, na.rm = TRUE) {
  fun <- match.arg(fun, c("mean", "sum", "count", "min", "max", "sd"),
                   several.ok = TRUE)

  vh <- .zonal_open(raster, "raster")
  r  <- vh$r
  if (vh$close) on.exit(vec_close_raster(r), add = TRUE)
  gt <- r$gt; W <- as.integer(r$width); H <- as.integer(r$height)
  ts <- max(1L, as.integer(r$tile_size))

  use_raster_zones <- inherits(zones, "vectra_raster") ||
    (is.character(zones) && length(zones) == 1L)
  if (use_raster_zones) {
    zh <- .zonal_open(zones, "zones")
    zr <- zh$r
    if (zh$close) on.exit(vec_close_raster(zr), add = TRUE)
    if (zr$width != W || zr$height != H)
      stop("zone raster must match the value raster's dimensions")
    if (!isTRUE(all.equal(as.numeric(zr$gt), as.numeric(gt))))
      stop("zone raster geotransform must match the value raster's")
    numeric_zone <- TRUE
  } else {
    if (!inherits(zones, "sf") && !inherits(zones, "sfc"))
      stop("`zones` must be a vectra_raster, a .vec path, or an sf/sfc layer")
    .check_sf()
    zid <- if (inherits(zones, "sfc") || is.null(zone_field)) {
      seq_len(length(sf::st_geometry(zones)))
    } else {
      if (!zone_field %in% names(zones))
        stop(sprintf("zone_field '%s' not found in `zones`", zone_field))
      zones[[zone_field]]
    }
    numeric_zone <- is.numeric(zid)
    zcrs <- sf::st_crs(zones)
    if (is.na(zcrs) && !is.null(r$epsg) && r$epsg > 0L)
      zcrs <- sf::st_crs(r$epsg)
    zones_g <- sf::st_geometry(zones)
    if (is.na(sf::st_crs(zones_g)) && !is.na(zcrs))
      zones_g <- sf::st_set_crs(zones_g, zcrs)
    # Native point-in-polygon for each pixel centre, planar-gated like the other
    # GEOS paths; spherical geographic zones (s2 on) keep the sf intersects loop.
    native_zones <- .geos_planar_ok(zcrs)
    if (native_zones) {
      zloc <- .geos_locator(zones_g)
      znt  <- .spatial_threads()
    }
  }

  acc <- .zonal_acc()
  r0  <- 1L
  while (r0 <= H) {
    r1 <- min(r0 + ts - 1L, H)
    vm <- vec_read_window(r, band = band, cols = c(1L, W), rows = c(r0, r1))
    v  <- as.vector(vm)

    if (use_raster_zones) {
      zm <- vec_read_window(zr, band = zone_band, cols = c(1L, W),
                            rows = c(r0, r1))
      z <- as.vector(zm)
    } else if (native_zones) {
      xy <- .zonal_strip_xy(gt, r0, r1, W)
      first <- .Call(C_geos_locate_xy, zloc, as.numeric(xy$x), as.numeric(xy$y),
                     0L, 0, FALSE, znt)
      z <- zid[first]
    } else {
      xy  <- .zonal_strip_xy(gt, r0, r1, W)
      pts <- sf::st_as_sf(data.frame(x = xy$x, y = xy$y),
                          coords = c("x", "y"), crs = zcrs)
      hit <- sf::st_intersects(pts, zones_g)
      first <- vapply(hit, function(h) if (length(h)) h[1L] else NA_integer_,
                      integer(1L))
      z <- zid[first]
    }

    tainted <- character(0)
    if (!na.rm) {
      bad <- !is.na(z) & is.na(v)
      if (any(bad)) tainted <- unique(as.character(z[bad]))
    }
    .zonal_update(acc, z, v, tainted)
    r0 <- r1 + 1L
  }

  zlab <- names(acc$n)
  if (!length(zlab))
    return(.zonal_frame(character(0), acc, fun, numeric_zone, na.rm))
  ord <- if (numeric_zone) order(as.numeric(zlab)) else order(zlab)
  .zonal_frame(zlab[ord], acc, fun, numeric_zone, na.rm)
}

# Assemble the per-zone result frame from the accumulated moments, deriving each
# requested statistic and blanking na.rm = FALSE tainted zones.
.zonal_frame <- function(zlab, e, fun, numeric_zone, na.rm) {
  n  <- e$n[zlab]; s <- e$s[zlab]; ss <- e$ss[zlab]
  mn <- e$mn[zlab]; mx <- e$mx[zlab]
  zone <- if (numeric_zone) as.numeric(zlab) else zlab
  out <- data.frame(zone = zone, stringsAsFactors = FALSE)
  for (fn in fun) {
    out[[fn]] <- switch(fn,
      count = as.numeric(n),
      sum   = as.numeric(s),
      mean  = s / n,
      min   = as.numeric(mn),
      max   = as.numeric(mx),
      sd    = sqrt(pmax(0, (ss - s * s / n) / (n - 1))))
  }
  if (!na.rm && length(e$na) && nrow(out)) {
    bad <- as.character(out$zone) %in% e$na
    for (fn in setdiff(fun, "count")) out[[fn]][bad] <- NA_real_
  }
  rownames(out) <- NULL
  out
}

# -- focal / terrain (moving-window raster derivatives) -----------------------

# Drive a haloed tile-row strip pass over a VECR raster. For each output
# tile-row the input is read expanded by `radh` rows (the halo), `compute` maps
# the strip to an out_h x (W*nout) result, and each of the nout output bands is
# either streamed to a .vec (never the whole band resident) or assembled into an
# in-memory matrix. Returns the opened handle (path) or a list of nout matrices.
# Output side shared by the streamed raster kernels (focal/terrain/warp). The
# returned object writes one output tile-row at a time -- either streamed to a
# new .vec (path) or folded into in-memory matrices -- so an op never holds the
# whole output band. `write(ty, r0, r1, os)` takes an out_h x (W*nout) strip
# (derivative k in columns [(k-1)*W+1, k*W]); `finish()` returns the opened
# handle (streamed) or the list of attributed matrices (in memory).
.raster_sink <- function(W, H, nout, gt, epsg, TS,
                         path, dtype, band_names, comp_code) {
  writer <- NULL; acc <- NULL
  if (!is.null(path)) {
    path <- normalizePath(path, mustWork = FALSE)
    writer <- .Call(C_vecr_writer_open, path,
                    c(W, H, as.integer(nout)), as.character(dtype),
                    TS, as.numeric(gt), epsg, NA_real_, band_names, comp_code)
  } else {
    acc <- replicate(nout, matrix(NA_real_, H, W), simplify = FALSE)
  }
  list(
    write = function(ty, r0, r1, os) {
      for (k in seq_len(nout)) {
        strip_k <- os[, ((k - 1L) * W + 1L):(k * W), drop = FALSE]
        if (!is.null(writer)) {
          .Call(C_vecr_writer_write_strip, writer, as.integer(k),
                as.integer(ty), strip_k)
        } else {
          acc[[k]][r0:r1, ] <<- strip_k
        }
      }
    },
    finish = function() {
      if (!is.null(writer)) {
        .Call(C_vecr_writer_finish, writer)
        return(invisible(vec_open_raster(path)))
      }
      ext <- c(gt[1L], gt[4L] + H * gt[6L], gt[1L] + W * gt[2L], gt[4L])
      lapply(acc, function(m) {
        attr(m, "gt")     <- gt
        attr(m, "extent") <- ext
        attr(m, "crs")    <- if (epsg > 0L) epsg else NA
        m
      })
    }
  )
}

.raster_focal_run <- function(r, band, radh, nout, compute,
                              path, dtype, band_names, comp_code) {
  gt <- r$gt; W <- as.integer(r$width); H <- as.integer(r$height)
  TS <- max(1L, as.integer(r$tile_size))
  epsg <- if (!is.null(r$epsg)) as.integer(r$epsg) else 0L
  sink <- .raster_sink(W, H, nout, gt, epsg, TS, path, dtype, band_names, comp_code)

  tiles_y <- (H + TS - 1L) %/% TS
  for (ty in seq_len(tiles_y) - 1L) {
    r0 <- ty * TS + 1L
    r1 <- min(r0 + TS - 1L, H)
    in_r0 <- max(1L, r0 - radh)
    in_r1 <- min(H, r1 + radh)
    vm <- vec_read_window(r, band = band, cols = c(1L, W), rows = c(in_r0, in_r1))
    in_h  <- in_r1 - in_r0 + 1L
    top   <- r0 - in_r0
    out_h <- r1 - r0 + 1L
    os <- compute(vm, as.integer(in_h), as.integer(top), as.integer(out_h))
    sink$write(ty, r0, r1, os)
  }
  sink$finish()
}

# Normalize a focal window argument to an odd-by-odd weight matrix.
.focal_window <- function(w) {
  if (is.numeric(w) && is.null(dim(w)) && length(w) == 1L) {
    if (w < 1 || w %% 2 == 0)
      stop("`w` given as a single number must be a positive odd integer")
    w <- matrix(1, w, w)
  }
  if (!is.matrix(w) || !is.numeric(w))
    stop("`w` must be a numeric weight matrix or a single odd integer")
  if (nrow(w) %% 2 == 0 || ncol(w) %% 2 == 0)
    stop("`w` must have an odd number of rows and columns")
  w
}

#' Moving-window (focal) statistics over a streamed raster
#'
#' Applies a moving window to a `.vec` raster, reading the input one tile-row
#' strip at a time -- each strip expanded by the kernel radius (a halo read) so
#' window neighbours are available without ever holding the whole grid resident.
#' The per-window statistic is computed in C. When `path` is given the output is
#' streamed straight back to a new `.vec` one tile-row at a time, so neither the
#' input nor the output band is ever fully in memory; this is the raster op that
#' runs out of core where an in-memory engine needs the whole raster at once.
#'
#' This is the *sort / partition* tier of the spatial toolbox: bounded to one
#' haloed strip at a time, exploiting tile locality.
#'
#' The window `w` is a numeric weight matrix with odd dimensions (or a single
#' odd integer `k`, shorthand for a `k x k` matrix of ones). `NA` weights mark
#' cells outside the window. For `fun = "sum"`/`"mean"` the weights scale the
#' values (sum is `sum(w * x)`, mean is `sum(w * x) / sum(w)`); for the other
#' statistics a finite weight only marks membership. With `na.rm = TRUE` (the
#' default) nodata cells inside the window are skipped; with `na.rm = FALSE` any
#' nodata cell -- including a window that runs off the raster edge -- makes the
#' result `NA`, matching the resident behaviour.
#'
#' @param x A `vectra_raster` (from [vec_open_raster()]) or a path to a `.vec`
#'   raster.
#' @param w A numeric weight matrix with odd dimensions, or a single positive
#'   odd integer `k` for a `k x k` window of ones. Default `matrix(1, 3, 3)`.
#' @param fun Window statistic: one of `"sum"`, `"mean"`, `"min"`, `"max"`,
#'   `"sd"`, `"median"`. Default `"mean"`.
#' @param na.rm Skip nodata cells inside the window (`TRUE`, default) or let
#'   them propagate `NA` (`FALSE`).
#' @param band Band to read (1-based). Default 1.
#' @param path Optional output `.vec` path. When given the result is streamed to
#'   disk and the opened [vec_open_raster()] handle is returned invisibly; when
#'   `NULL` the result is returned as an in-memory matrix.
#' @param dtype Storage dtype for `.vec` output (see [vec_write_raster()]).
#'   Default `"f32"`.
#' @param compression Compression effort for `.vec` output. Default `"fast"`.
#'
#' @return When `path` is `NULL`, a numeric matrix (row 1 northmost) carrying
#'   `gt`, `extent`, `crs`, and `fun` attributes. When `path` is given, the
#'   written `vectra_raster` handle (invisibly).
#'
#' @seealso [terrain()] for DEM derivatives built on the same strip pass,
#'   [zonal()] for per-zone summaries.
#'
#' @examples
#' m <- matrix(1:36, 6, 6, byrow = TRUE)
#' f <- tempfile(fileext = ".vec")
#' vec_write_raster(m, f, dtype = "f64", extent = c(0, 0, 6, 6))
#'
#' # 3x3 mean smoother; edge cells see off-raster neighbours.
#' focal(f, w = matrix(1, 3, 3), fun = "mean")
#' unlink(f)
#'
#' @export
focal <- function(x, w = matrix(1, 3, 3),
                  fun = c("mean", "sum", "min", "max", "sd", "median"),
                  na.rm = TRUE, band = 1L, path = NULL, dtype = "f32",
                  compression = c("fast", "balanced", "max")) {
  fun <- match.arg(fun)
  fun_code <- switch(fun, sum = 0L, mean = 1L, min = 2L, max = 3L,
                     sd = 4L, median = 5L)
  w <- .focal_window(w)
  kh <- nrow(w); kw <- ncol(w)
  weights <- as.numeric(t(w))
  radh <- (kh - 1L) %/% 2L
  comp_code <- switch(match.arg(compression), fast = 0L, balanced = 1L, max = 2L)

  vh <- .zonal_open(x, "x"); r <- vh$r
  if (vh$close) on.exit(vec_close_raster(r), add = TRUE)
  W <- as.integer(r$width)
  kdims <- c(as.integer(kh), as.integer(kw))

  compute <- function(vm, in_h, top, out_h)
    .Call(C_focal_strip, vm, c(in_h, W), weights, kdims,
          fun_code, isTRUE(na.rm), top, out_h)

  res <- .raster_focal_run(r, as.integer(band), radh, 1L, compute,
                           path, dtype, NULL, comp_code)
  if (!is.null(path)) return(res)
  m <- res[[1L]]; attr(m, "fun") <- fun; m
}

#' Terrain derivatives from a streamed elevation raster
#'
#' Computes DEM derivatives from a `.vec` elevation raster with Horn's 3x3
#' method, on the same haloed tile-row strip pass as [focal()] -- the input is
#' read one strip at a time and, when `path` is given, the outputs are streamed
#' straight back to a multi-band `.vec`. Matches \pkg{terra}'s
#' `terrain()` / `shade()` conventions.
#'
#' @param x A `vectra_raster` (from [vec_open_raster()]) or a path to a `.vec`
#'   elevation raster.
#' @param v Derivatives to compute, any of `"slope"`, `"aspect"`,
#'   `"hillshade"`, `"TPI"` (topographic position index), `"roughness"`,
#'   `"TRI"` (terrain ruggedness index). The return follows the input: one
#'   matrix for a single `v`, a named list for several.
#' @param unit Angular unit for `slope` and `aspect`: `"degrees"` (default) or
#'   `"radians"`.
#' @param azimuth,altitude Sun position for `"hillshade"`, in degrees. Defaults
#'   315 (NW) and 45.
#' @param band Band to read (1-based). Default 1.
#' @param path Optional output `.vec` path (one band per `v`, named after `v`).
#'   When given the result is streamed to disk and the opened
#'   [vec_open_raster()] handle is returned invisibly; when `NULL` the result is
#'   returned in memory.
#' @param dtype Storage dtype for `.vec` output (see [vec_write_raster()]).
#'   Default `"f32"`.
#' @param compression Compression effort for `.vec` output. Default `"fast"`.
#'
#' @return When `path` is `NULL`: a numeric matrix for a single `v`, or a named
#'   list of matrices for several, each carrying `gt`, `extent`, and `crs`
#'   attributes (row 1 northmost). When `path` is given, the written multi-band
#'   `vectra_raster` handle (invisibly).
#'
#' @details Slope and aspect use the Horn (1981) finite-difference gradient over
#'   the 3x3 neighbourhood; `aspect` is degrees clockwise from north (flat cells
#'   return 90). `hillshade` is the cosine of the incidence angle for the given
#'   sun position, clamped at 0. `TPI` is the cell minus the mean of its eight
#'   neighbours; `roughness` is the range over the 3x3; `TRI` is the mean
#'   absolute difference to the eight neighbours. Cells whose 3x3 neighbourhood
#'   touches a nodata value or the raster edge return `NA`.
#'
#' @seealso [focal()] for arbitrary moving windows.
#'
#' @examples
#' # A tilted surface so slope and aspect are well defined.
#' z <- outer(1:8, 1:8, function(r, c) 10 + 2 * c + r)
#' f <- tempfile(fileext = ".vec")
#' vec_write_raster(z, f, dtype = "f64", extent = c(0, 0, 8, 8))
#'
#' slp <- terrain(f, v = "slope")
#' deriv <- terrain(f, v = c("slope", "aspect", "hillshade"))
#' names(deriv)
#' unlink(f)
#'
#' @export
terrain <- function(x, v = c("slope", "aspect", "hillshade",
                             "TPI", "roughness", "TRI"),
                    unit = c("degrees", "radians"),
                    azimuth = 315, altitude = 45,
                    band = 1L, path = NULL, dtype = "f32",
                    compression = c("fast", "balanced", "max")) {
  v <- match.arg(v, several.ok = TRUE)
  codes <- c(slope = 0L, aspect = 1L, hillshade = 2L,
             TPI = 3L, roughness = 4L, TRI = 5L)
  which_codes <- as.integer(unname(codes[v]))
  unit_code <- if (match.arg(unit) == "radians") 1L else 0L
  sun <- c(as.numeric(azimuth), as.numeric(altitude))
  comp_code <- switch(match.arg(compression), fast = 0L, balanced = 1L, max = 2L)

  vh <- .zonal_open(x, "x"); r <- vh$r
  if (vh$close) on.exit(vec_close_raster(r), add = TRUE)
  W <- as.integer(r$width)
  gt <- r$gt
  res <- c(abs(gt[2L]), abs(gt[6L]))
  nout <- length(v)

  compute <- function(vm, in_h, top, out_h)
    .Call(C_terrain_strip, vm, c(in_h, W), which_codes, top, out_h,
          res, unit_code, sun)

  out <- .raster_focal_run(r, as.integer(band), 1L, nout, compute,
                           path, dtype, v, comp_code)
  if (!is.null(path)) return(out)
  names(out) <- v
  if (nout == 1L) out[[1L]] else out
}

# Resolve the warp target grid to list(W, H, gt, epsg). `template` is either a
# vectra_raster / .vec path whose grid is borrowed wholesale, or a list spec
# list(crs=, extent=, res=, dims=). With no `extent` the target extent is the
# source's corners projected into the target CRS (needs sf for a real
# reprojection); `res` or `dims` then sets the spacing.
.warp_grid <- function(template, src) {
  if (inherits(template, "vectra_raster") ||
      (is.character(template) && length(template) == 1L)) {
    th <- .zonal_open(template, "template")
    t  <- th$r
    g  <- list(W = as.integer(t$width), H = as.integer(t$height),
               gt = as.numeric(t$gt),
               epsg = if (!is.null(t$epsg)) as.integer(t$epsg) else 0L)
    if (th$close) vec_close_raster(t)
    return(g)
  }
  if (!is.list(template))
    stop("`template` must be a vectra_raster, a .vec path, or a list(crs=, extent=, res=)")

  epsg <- template$crs
  if (is.null(epsg)) epsg <- if (!is.null(src$epsg)) src$epsg else 0L
  epsg <- as.integer(epsg)
  res <- template$res; extent <- template$extent; dims <- template$dims

  if (is.null(extent)) {
    if (is.null(res))
      stop("a list `template` without `extent` needs `res` to set the grid")
    src_epsg <- if (!is.null(src$epsg)) as.integer(src$epsg) else 0L
    sg <- as.numeric(src$gt); sW <- src$width; sH <- src$height
    cx <- c(0, sW, 0, sW); cy <- c(0, 0, sH, sH)
    X <- sg[1L] + cx * sg[2L] + cy * sg[3L]
    Y <- sg[4L] + cx * sg[5L] + cy * sg[6L]
    if (epsg > 0L && src_epsg > 0L && epsg != src_epsg) {
      .check_sf()
      p <- sf::sf_project(sf::st_crs(src_epsg), sf::st_crs(epsg), cbind(X, Y))
      X <- p[, 1L]; Y <- p[, 2L]
    }
    extent <- c(min(X), min(Y), max(X), max(Y))
  }
  xmin <- extent[1L]; ymin <- extent[2L]; xmax <- extent[3L]; ymax <- extent[4L]

  if (!is.null(res)) {
    xres <- res[1L]; yres <- if (length(res) >= 2L) res[2L] else res[1L]
    W <- max(1L, as.integer(round((xmax - xmin) / xres)))
    H <- max(1L, as.integer(round((ymax - ymin) / yres)))
  } else if (!is.null(dims)) {
    W <- as.integer(dims[1L]); H <- as.integer(dims[2L])
    xres <- (xmax - xmin) / W; yres <- (ymax - ymin) / H
  } else {
    stop("a list `template` needs `res` or `dims`")
  }
  list(W = W, H = H, gt = c(xmin, xres, 0, ymax, 0, -yres), epsg = epsg)
}

#' Resample or reproject a streamed raster onto a target grid
#'
#' Warps a `.vec` raster onto a target grid, walking the *output* one tile-row
#' strip at a time. For each strip the target pixel-centre coordinates are built,
#' projected into the source coordinate reference system when the two CRSs differ
#' (delegated to PROJ via \pkg{sf}), mapped through the source geotransform to
#' fractional source pixels, and sampled from the bounded source window those
#' coordinates fall in. The output is assembled in memory or streamed straight
#' back to a new `.vec`, so the whole output grid is never resident; the source
#' is read in bounded windows rather than held whole.
#'
#' This is the *sort / partition* tier of the spatial toolbox: each output strip
#' reads the source window it projects onto. For a mild reprojection or a plain
#' resample that window is a thin band; a strong reprojection can make it large,
#' but the output stays streamed throughout.
#'
#' Sampling follows the GDAL / \pkg{terra} convention (pixel centres at
#' half-integer coordinates). `"near"` takes the nearest source cell;
#' `"bilinear"` the 2x2 weighted mean; `"cubic"` the 4x4 cubic convolution
#' (Catmull-Rom, a = -0.5). A target cell whose sampling kernel reaches outside
#' the source extent, or touches a nodata cell, comes back `NA`.
#'
#' Reprojection happens only when both rasters carry a known EPSG code and the
#' codes differ; otherwise `warp()` resamples within a shared CRS and needs no
#' \pkg{sf}.
#'
#' @param x A `vectra_raster` (from [vec_open_raster()]) or a path to a `.vec`
#'   raster to warp.
#' @param template The target grid: a `vectra_raster` / `.vec` path whose grid
#'   and CRS are borrowed, or a list `list(crs =, extent =, res =, dims =)`.
#'   With `crs` and `res` but no `extent`, the target extent is the source's
#'   corners projected into `crs`.
#' @param method Resampling method: `"near"`, `"bilinear"`, or `"cubic"`.
#'   Default `"near"`.
#' @param band Band to warp (1-based). Default 1.
#' @param path Optional output `.vec` path. When given the result is streamed to
#'   disk and the opened [vec_open_raster()] handle is returned invisibly; when
#'   `NULL` the result is returned as an in-memory matrix.
#' @param dtype Storage dtype for `.vec` output (see [vec_write_raster()]).
#'   Default `"f32"`.
#' @param compression Compression effort for `.vec` output. Default `"fast"`.
#'
#' @return When `path` is `NULL`, a numeric matrix on the target grid (row 1
#'   northmost) carrying `gt`, `extent`, and `crs` attributes. When `path` is
#'   given, the written `vectra_raster` handle (invisibly).
#'
#' @seealso [rasterize()] to build a raster from streamed vector features,
#'   [focal()] for moving-window statistics.
#'
#' @examples
#' z <- outer(1:8, 1:8, function(r, c) r + 2 * c)
#' f <- tempfile(fileext = ".vec")
#' vec_write_raster(z, f, dtype = "f64", extent = c(0, 0, 8, 8))
#'
#' # Resample onto a finer grid over the same extent.
#' fine <- warp(f, list(extent = c(0, 0, 8, 8), res = 0.5), method = "bilinear")
#' dim(fine)
#' unlink(f)
#'
#' @export
warp <- function(x, template, method = c("near", "bilinear", "cubic"),
                 band = 1L, path = NULL, dtype = "f32",
                 compression = c("fast", "balanced", "max")) {
  method_code <- switch(match.arg(method), near = 0L, bilinear = 1L, cubic = 2L)
  comp_code <- switch(match.arg(compression), fast = 0L, balanced = 1L, max = 2L)

  vh <- .zonal_open(x, "x"); src <- vh$r
  if (vh$close) on.exit(vec_close_raster(src), add = TRUE)
  band <- as.integer(band)

  tg <- .warp_grid(template, src)
  W <- tg$W; H <- tg$H; gt_t <- tg$gt; epsg_t <- tg$epsg
  gt_s <- as.numeric(src$gt)
  sW <- as.integer(src$width); sH <- as.integer(src$height)
  epsg_s <- if (!is.null(src$epsg)) as.integer(src$epsg) else 0L

  reproject <- epsg_t > 0L && epsg_s > 0L && epsg_t != epsg_s
  if (reproject) .check_sf()

  det <- gt_s[2L] * gt_s[6L] - gt_s[3L] * gt_s[5L]
  if (abs(det) < .Machine$double.eps)
    stop("source geotransform is not invertible")
  margin <- method_code   # near = 0, bilinear = 1, cubic = 2 kernel half-width

  TS <- max(1L, as.integer(src$tile_size))
  sink <- .raster_sink(W, H, 1L, gt_t, epsg_t, TS, path, dtype, NULL, comp_code)

  tiles_y <- (H + TS - 1L) %/% TS
  for (ty in seq_len(tiles_y) - 1L) {
    r0 <- ty * TS + 1L
    r1 <- min(r0 + TS - 1L, H)
    out_h <- r1 - r0 + 1L

    # Target pixel centres for this strip, column-major (output row fastest).
    grow <- rep.int(seq.int(0L, out_h - 1L), W)
    gcol <- rep(seq.int(0L, W - 1L), each = out_h)
    gr <- (r0 - 1L) + grow
    Xt <- gt_t[1L] + (gcol + 0.5) * gt_t[2L] + (gr + 0.5) * gt_t[3L]
    Yt <- gt_t[4L] + (gcol + 0.5) * gt_t[5L] + (gr + 0.5) * gt_t[6L]

    if (reproject) {
      p <- sf::sf_project(sf::st_crs(epsg_t), sf::st_crs(epsg_s),
                          cbind(Xt, Yt), keep = TRUE)
      Xs <- p[, 1L]; Ys <- p[, 2L]
    } else {
      Xs <- Xt; Ys <- Yt
    }

    dx <- Xs - gt_s[1L]; dy <- Ys - gt_s[4L]
    sx <- (dx * gt_s[6L] - dy * gt_s[3L]) / det   # fractional source col (edge)
    sy <- (dy * gt_s[2L] - dx * gt_s[5L]) / det   # fractional source row (edge)
    sx[!is.finite(sx)] <- NA_real_
    sy[!is.finite(sy)] <- NA_real_

    fin <- !is.na(sx) & !is.na(sy)
    if (any(fin)) {
      cmin <- max(0L, as.integer(floor(min(sx[fin]) - 0.5)) - margin - 1L)
      cmax <- min(sW - 1L, as.integer(ceiling(max(sx[fin]) - 0.5)) + margin + 1L)
      rmin <- max(0L, as.integer(floor(min(sy[fin]) - 0.5)) - margin - 1L)
      rmax <- min(sH - 1L, as.integer(ceiling(max(sy[fin]) - 0.5)) + margin + 1L)
    } else {
      cmin <- 1L; cmax <- 0L; rmin <- 1L; rmax <- 0L
    }

    if (cmax >= cmin && rmax >= rmin) {
      win <- vec_read_window(src, band = band,
                             cols = c(cmin + 1L, cmax + 1L),
                             rows = c(rmin + 1L, rmax + 1L))
      win_h <- rmax - rmin + 1L; win_w <- cmax - cmin + 1L
      os <- .Call(C_warp_strip, win,
                  c(as.integer(win_h), as.integer(win_w)),
                  c(cmin, rmin), sx, sy, method_code, c(out_h, W))
    } else {
      os <- matrix(NA_real_, out_h, W)
    }
    sink$write(ty, r0, r1, os)
  }

  out <- sink$finish()
  if (!is.null(path)) return(out)
  out[[1L]]
}

#' Materialize a spatial query as an sf object
#'
#' Collects a `vectra_node` (typically the result of [spatial_map()] or
#' [spatial_join()]) into memory and rebuilds an `sf` object from its hex-WKB
#' geometry column. The CRS defaults to the one carried on the node.
#'
#' This is the spatial counterpart to [collect()]: use it when the final result
#' fits in memory as `sf`. For a result still larger than RAM, keep it as a node
#' and write it out with [write_vtr()] (the geometry stays as a WKB string
#' column) or reduce it with [collect_chunked()].
#'
#' @param x A `vectra_node` with a hex-WKB / WKT geometry column, or a
#'   data.frame already collected from one.
#' @param geom Name of the geometry column. Default `"geometry"`.
#' @param crs Override the coordinate reference system. Defaults to the CRS the
#'   node carries, or unknown.
#'
#' @return An `sf` object.
#'
#' @seealso [spatial_map()], [spatial_join()], [collect()].
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(
#'   NAME = nc$NAME,
#'   geometry = sf::st_as_binary(sf::st_geometry(nc), hex = TRUE)
#' ), f)
#' result <- tbl(f) |> spatial_map(~ sf::st_centroid(.x), crs = sf::st_crs(nc))
#' collect_sf(result)
#' unlink(f)
#'
#' @export
collect_sf <- function(x, geom = "geometry", crs = NULL) {
  .check_sf()
  node_crs <- if (inherits(x, "vectra_node")) x$.crs else NULL
  df <- if (inherits(x, "vectra_node")) collect(x) else as.data.frame(x)
  if (is.null(crs)) crs <- node_crs
  crs <- .as_crs(crs)
  if (!geom %in% names(df))
    stop(sprintf("geometry column '%s' not found; pass geom=", geom))
  g <- sf::st_as_sfc(structure(df[[geom]], class = "WKB"), EWKB = FALSE)
  g <- sf::st_set_crs(g, crs)
  rest <- df[setdiff(names(df), geom)]
  sf::st_sf(rest, geometry = g)
}

#' Stream a vectra node's geometry to a vector file
#'
#' An [sf::st_write()] method (also reached through [sf::write_sf()]) for a
#' `vectra_node`: writes the result a batch at a time, appending each, so the
#' whole layer is never held in memory. This is the streaming counterpart to
#' `collect_sf(x) |> sf::st_write(...)` -- that route materializes every feature
#' as an `sf` object first, which for a multi-million-feature result dominates
#' memory; this route's peak is one batch.
#'
#' @param obj A `vectra_node` whose rows carry a hex-WKB geometry column (from
#'   [spatial_overlay()], a grouped [slice_min()] / [slice_max()] resolution, a
#'   `.vtr` scan, ...). It is consumed by the stream.
#' @param dsn Destination data source name (file path).
#' @param layer Layer name. `NULL` lets \pkg{sf} derive it from `dsn`.
#' @param ... Unused; for S3 generic compatibility.
#' @param geom Name of the hex-WKB geometry column. Default `"geometry"`.
#' @param crs CRS to tag the output with. `NULL` takes the CRS carried on the
#'   node.
#' @param delete_dsn If `TRUE`, remove an existing `dsn` before writing.
#' @param quiet Passed to [sf::st_write()].
#'
#' @return The `dsn`, invisibly.
#' @seealso [collect_sf()] to materialize the whole result as one `sf` object.
#' @exportS3Method sf::st_write
st_write.vectra_node <- function(obj, dsn, layer = NULL, ..., geom = "geometry",
                                 crs = NULL, delete_dsn = FALSE, quiet = TRUE) {
  .check_sf()
  if (is.null(crs)) crs <- obj$.crs
  crs <- .as_crs(crs)
  if (isTRUE(delete_dsn) && file.exists(dsn)) unlink(dsn)
  nxt   <- .batch_cursor(obj)
  first <- TRUE
  repeat {
    df <- nxt()
    if (is.null(df)) break
    if (!geom %in% names(df))
      stop(sprintf("geometry column '%s' not found; pass geom=", geom))
    g   <- sf::st_as_sfc(structure(df[[geom]], class = "WKB"), EWKB = FALSE)
    g   <- sf::st_set_crs(g, crs)
    sfb <- sf::st_sf(df[setdiff(names(df), geom)], geometry = g)
    sf::st_write(sfb, dsn, layer = layer, append = !first, quiet = quiet)
    first <- FALSE
  }
  if (first) stop("the query produced no rows to write")
  invisible(dsn)
}

#' Self-overlay a polygon layer into disjoint pieces (QGIS-style Union)
#'
#' Splits a polygon layer along all its own overlaps into disjoint pieces and
#' returns a lazy node with one row per piece per covering polygon: where `k`
#' polygons overlap, that piece appears `k` times, each row carrying one source
#' polygon's attributes. This is the union overlay GIS tools expose as
#' "Union (single layer)", with the overlap retained once per contributing
#' feature rather than dissolved. Resolve the duplicates with a grouped
#' [slice_min()] / [slice_max()] -- for example earliest designation year wins:
#' `group_by(piece_id) |> slice_min(year)`.
#'
#' The topology is done once with \pkg{sf}/GEOS and tiled over connected overlap
#' clusters (disjoint clusters never share a piece, so the tiling is exact and
#' bounded in memory), then the exploded pieces are streamed to a `.vtr` and
#' handed back as a lazy node. Geometry rides through the engine as hex-encoded
#' WKB in a string column; the CRS is carried on the node for [collect_sf()].
#'
#' The overlay runs on a fixed-precision model: coordinates are snapped to a
#' grid derived from their own magnitude so the pieces come out disjoint and
#' their areas reconstruct the union of the inputs, instead of drifting by the
#' fraction of a percent that floating-point sliver artefacts on invalid input
#' otherwise introduce. Inputs are also passed through [sf::st_make_valid()].
#'
#' With a second layer `y`, the same machinery overlays two layers instead of
#' self-unioning one: both layers are noded together into one planar partition,
#' and each piece carries the attributes of the `x` record and the `y` record
#' that cover it. `how` selects which pieces to keep -- the intersection (pieces
#' covered by both), the union (every piece of either), `x` split by `y`
#' (`"identity"`), or the parts in exactly one layer (`"symdiff"`). With
#' `y = NULL` (the default) the function self-unions `x` and `how` is ignored.
#'
#' @param x An `sf` object with polygon or multipolygon geometry, or a single
#'   path to a vector file (e.g. a GeoPackage). A path is read in feature batches
#'   via `layer` / `query`, so the whole layer is never held in memory at once --
#'   peak memory then tracks the cleaned geometry, not the source size, which lets
#'   a larger-than-RAM layer overlay on a modest machine.
#' @param y Optional second layer to overlay `x` against, in the same forms `x`
#'   accepts (an `sf` object or a file path read via `layer_y` / `query_y`). It
#'   must share the CRS of `x`. `NULL` (the default) self-unions `x`.
#' @param vars Character vector of attribute columns of `x` to carry onto each
#'   piece. Default `NULL` keeps them all; name a subset to keep the streamed
#'   output narrow.
#' @param vars_y Character vector of attribute columns of `y` to carry onto each
#'   piece (two-layer overlay only). Default `NULL` keeps them all. A name shared
#'   with an `x` column is disambiguated with a `.x` / `.y` suffix in the output.
#' @param how For a two-layer overlay, which pieces to keep: `"intersection"`
#'   (covered by both layers; the default), `"union"` (every piece of either,
#'   the absent side's attributes filled with `NA`), `"identity"` (all of `x`,
#'   split by `y`, with `y`'s attributes where it covers and `NA` elsewhere), or
#'   `"symdiff"` (pieces in exactly one layer). Ignored when `y = NULL`.
#' @param piece Name of the integer piece-id column added to the output (the key
#'   you group by to resolve overlaps). Default `"piece_id"`.
#' @param geom Name of the output hex-WKB geometry column. Default `"geometry"`.
#' @param grid Fixed-precision snapping grid size in CRS units. Coordinates are
#'   snapped to this grid before noding so near-duplicate shared boundaries merge
#'   into one. `NULL` (the default) derives it from coordinate magnitude
#'   (`max(abs(st_bbox(x))) * 3e-8`), which suits projected layers. Pass a number
#'   to override when that default is too coarse for fine geometry (or too coarse
#'   because an outlier coordinate inflated the magnitude), or `0` to disable
#'   snapping entirely.
#' @param precision Fixed-precision grid size, in CRS units, for noding the
#'   boundary linework. Noding on a fixed grid is deterministic and avoids the
#'   floating noder's repair-and-retry on dense overlapping linework, which is
#'   what makes a large dense layer feasible to overlay. It is far finer than
#'   `grid` so intersection points are not collapsed. `NULL` (the default)
#'   derives it from coordinate magnitude (`max(abs(st_bbox(x))) * 1e-13`); pass
#'   a number to override, or `0` to node in floating precision.
#' @param dedup Overlay one representative per group of byte-identical cleaned
#'   geometries and fan the per-record attributes back onto its pieces afterwards.
#'   Duplicates add no faces, so the result is identical; this only removes the
#'   redundant noding when many records are stacked over one site (common in
#'   WDPA-style data). `TRUE` by default; set `FALSE` to overlay every record.
#' @param flush_rows Exploded rows buffered before a spill flush. Defaults to
#'   `getOption("vectra.spatial_flush", 5e5)`.
#' @param mem_limit Approximate peak working-set budget in bytes, bounding the
#'   per-tile size (`tile_bytes = mem_limit / (threads * 24)`). It is a throughput
#'   knob with an interior optimum, not "bigger is faster": too small replicates
#'   features across many tiles, too large nodes too much linework per tile (a
#'   superlinear cost), and a budget of tens of GB runs slower than the default on
#'   a dense layer. Lower it for tighter memory. Defaults via
#'   `getOption("vectra.overlay_mem_limit", ...)` to a value that scales with
#'   `threads` to hold the per-tile size near its measured optimum.
#' @param threads Number of OpenMP threads for the per-component overlay within a
#'   chunk. `0` (the default, via `getOption("vectra.overlay_threads", 0)`) uses
#'   all available cores.
#' @param quiet If `FALSE`, show a text progress bar over the overlay chunks.
#' @param layer When `x` is a file path, the name of the layer to read. Ignored
#'   for an `sf` `x`. Supply this or `query`.
#' @param query When `x` is a file path, a SQL statement selecting the features to
#'   overlay (read in batches via `LIMIT`/`OFFSET`); use it instead of `layer`
#'   for a subset or join. With `query` and no `layer`, pass `grid` explicitly,
#'   since the layer extent cannot be read from the file metadata.
#' @param layer_y,query_y The `layer` / `query` equivalents for a file-path `y`.
#' @param read_chunk Features per read/parse batch. `NULL` (default) sizes it
#'   from available RAM. Smaller batches lower peak memory; larger ones do fewer
#'   round trips.
#'
#' @return A `vectra_node` over the exploded overlay, backed by temporary `.vtr`
#'   spills removed when the node is garbage-collected, carrying the CRS of `x`
#'   for [collect_sf()]. For a self-union it is one row per piece per covering
#'   polygon; for a two-layer overlay one row per piece per covering
#'   `x`-record / `y`-record pair, with the columns of both layers.
#'
#' @seealso [slice_min()] / [slice_max()] to resolve each piece to one winner,
#'   [collect_sf()] to materialize as `sf`.
#'
#' @examplesIf requireNamespace("sf", quietly = TRUE)
#' # Two overlapping squares designated in different years.
#' sq <- function(a, b) sf::st_polygon(list(rbind(
#'   c(a, 0), c(b, 0), c(b, 1), c(a, 1), c(a, 0))))
#' polys <- sf::st_sf(year = c(1990L, 2010L),
#'                    geometry = sf::st_sfc(sq(0, 2), sq(1, 3)))
#'
#' # Split into disjoint pieces; earliest year wins where they overlap.
#' first <- spatial_overlay(polys) |>
#'   group_by(piece_id) |>
#'   slice_min(year, n = 1, with_ties = FALSE) |>
#'   collect_sf()
#' first
#'
#' # Two-layer overlay: intersect the squares with a zone layer, keeping both
#' # sets of attributes on each overlapping piece.
#' zones <- sf::st_sf(zone = c("A", "B"),
#'                    geometry = sf::st_sfc(sq(0, 1.5), sq(1.5, 3)))
#' inter <- spatial_overlay(polys, zones, how = "intersection") |> collect_sf()
#' inter
#'
#' @export
spatial_overlay <- function(x, y = NULL, vars = NULL, vars_y = NULL,
                            how = c("intersection", "union", "identity", "symdiff"),
                            piece = "piece_id",
                            geom = "geometry", grid = NULL, precision = NULL,
                            dedup = TRUE, flush_rows = NULL,
                            mem_limit = NULL, threads = NULL, quiet = TRUE,
                            layer = NULL, query = NULL,
                            layer_y = NULL, query_y = NULL, read_chunk = NULL) {
  .check_sf()
  how <- match.arg(how)
  two <- !is.null(y)

  # Validate an explicit grid / precision up front; NULL means derive them.
  if (!is.null(grid)) {
    if (!is.numeric(grid) || length(grid) != 1L || !is.finite(grid) || grid < 0)
      stop("`grid` must be a single non-negative number (CRS units), or NULL to derive it")
    grid <- as.double(grid)
  }
  if (!is.null(precision)) {
    if (!is.numeric(precision) || length(precision) != 1L || !is.finite(precision) || precision < 0)
      stop("`precision` must be a single non-negative number (CRS units), or NULL to derive it")
    precision <- as.double(precision)
  }

  # Overlay is CPU-bound and the tiles are load-balanced across the pool, so use
  # every core by default; peak memory is bounded by the per-tile budget times the
  # running threads, not by the thread count.
  nthreads <- threads   %||% getOption("vectra.overlay_threads",
                                       max(parallel::detectCores(), 1L))
  nthreads <- max(as.integer(nthreads), 1L)
  mem      <- mem_limit %||% getOption("vectra.overlay_mem_limit",
                                       .overlay_mem_default(nthreads))

  # Read and parse the input: an in-memory sf object, or a file source (a path
  # with layer= or query=) read in feature batches so the whole layer is never
  # materialized at once. Either way the geometry is repaired, made areal, snapped
  # to the grid, and returned as cleaned WKB plus bounding boxes; geometry stays
  # compact (raw WKB) in R and never touches sf again on the compute boundary. For
  # a two-layer overlay both layers are ingested onto the same grid and stacked, so
  # the downstream noding, components, dedup, and tiling treat them as one set; a
  # per-input `side` tag (1 = x, 2 = y) drives the attribute fan-out at the end.
  ingx  <- .overlay_ingest(x, vars, piece, grid, nthreads,
                           layer = layer, query = query, read_chunk = read_chunk)
  crs   <- ingx$crs; grid <- ingx$grid
  if (two) {
    ingy <- .overlay_ingest(y, vars_y, piece, grid, nthreads,
                            layer = layer_y, query = query_y, read_chunk = read_chunk)
    if (!isTRUE(crs == ingy$crs))
      stop("`x` and `y` must share a CRS; transform one first (e.g. sf::st_transform()).")
    nx <- ingx$n; ny <- ingy$n; n <- nx + ny
    cwkb <- c(ingx$cwkb, ingy$cwkb)
    bbox <- rbind(ingx$bbox, ingy$bbox)
    side_of  <- c(rep.int(1L, nx), rep.int(2L, ny))   # which layer each input is
    local_of <- c(seq_len(nx), seq_len(ny))           # its row within that layer
    # Disambiguate shared column names (dplyr-style .x / .y) and pre-rename the
    # per-layer attribute frames so the fan-out can combine them without collision.
    nm_x <- names(ingx$attrs); nm_y <- names(ingy$attrs)
    ax <- ingx$attrs; if (length(ax)) names(ax) <- ifelse(nm_x %in% nm_y, paste0(nm_x, ".x"), nm_x)
    ay <- ingy$attrs; if (length(ay)) names(ay) <- ifelse(nm_y %in% nm_x, paste0(nm_y, ".y"), nm_y)
    out_template <- ax[0, , drop = FALSE]
    for (nm in names(ay)) out_template[[nm]] <- ay[[nm]][0]
    out_template[[piece]] <- integer(0)
    out_template[[geom]]  <- character(0)
    ingy <- NULL
  } else {
    attrs <- ingx$attrs; n <- ingx$n
    cwkb  <- ingx$cwkb;  bbox <- ingx$bbox
  }
  ingx  <- NULL
  if (!any(!is.na(bbox[, 1L]))) stop("the inputs have no parseable geometries to overlay")

  # Noding precision. Boundaries are noded at a fixed grid (snap rounding): this is
  # deterministic and avoids the floating noder's repair-and-retry on dense overlapping
  # linework, which is what makes a large dense layer feasible to node. The grid is
  # far finer than the cleaning grid so intersection points are not collapsed; it is
  # set well above the floating-point resolution at the layer's extent.
  if (is.null(precision)) {
    mag       <- max(abs(bbox), na.rm = TRUE)
    precision <- if (is.finite(mag) && mag > 0) mag * 1e-13 else 0
  }

  comp   <- .Call(C_overlay_components, bbox)
  wbytes <- as.numeric(lengths(cwkb))

  # Deduplicate identical cleaned geometry before overlaying. After snapping, the
  # many records stacked over one site are byte-identical, and duplicates add no
  # faces -- so the overlay runs on one representative per group and the per-record
  # attributes are fanned back onto its pieces afterwards. The arrangement is
  # unchanged; only the redundant noding/clipping work is removed. `mem_by_dk[[k]]`
  # holds the original rows the k-th distinct input stands for.
  n_orig <- n
  if (isTRUE(dedup)) {
    grp       <- .Call(C_overlay_group, cwkb)
    rep_idx   <- which(!duplicated(grp))
    mem_by_dk <- unname(split(seq_len(n_orig), grp))
  } else {
    rep_idx   <- seq_len(n_orig)
    mem_by_dk <- as.list(seq_len(n_orig))
  }
  comp <- comp[rep_idx]; bbox <- bbox[rep_idx, , drop = FALSE]
  cwkb <- cwkb[rep_idx]; wbytes <- wbytes[rep_idx]; n <- length(rep_idx)

  # Connected components (from bounding boxes). Each component is one overlay job;
  # only the few components too large for the memory budget are tiled over their
  # own extent. Most components are small, so there is no clipping or replication
  # and the fast exact path is taken.
  tile_bytes <- max(mem / (nthreads * .OVERLAY_FACTOR), 1e6)
  no_clip <- rep(NA_real_, 4L)
  # Build each component's jobs into its own list, then flatten once. Growing a
  # single `jobs` list per component is quadratic over the many components a large
  # layer splits into; the per-component lists keep it linear.
  per_comp <- lapply(split(seq_len(n), comp), function(rows) {
    if (length(rows) <= .OVERLAY_FEAT_CAP && sum(wbytes[rows]) <= tile_bytes)
      return(list(list(idx = rows, rect = no_clip)))
    ext <- c(min(bbox[rows, 1L]), min(bbox[rows, 2L]),
             max(bbox[rows, 3L]), max(bbox[rows, 4L]))
    .overlay_tiles(bbox, rows, ext, tile_bytes, .OVERLAY_FEAT_CAP, wbytes, 0L)
  })
  jobs <- unlist(per_comp, recursive = FALSE, use.names = FALSE)
  # Process the heaviest tiles first. A tile's cost tracks its feature bytes, and
  # the dense archipelago tiles cost far more than the rest; running them first
  # keeps every thread busy instead of stranding one on a giant tile at the tail.
  if (length(jobs) > 1L) {
    jcost <- vapply(jobs, function(t) sum(wbytes[t$idx]), numeric(1))
    jobs <- jobs[order(jcost, decreasing = TRUE)]
  }
  if (!quiet)
    message(sprintf(paste0("spatial_overlay: %d inputs (%d distinct), %d components, %d jobs, ",
                           "%d threads, grid=%.4g, noding=%.3g"),
                    n_orig, n, max(comp), length(jobs), nthreads, grid, precision))

  fr        <- flush_rows %||% getOption("vectra.spatial_flush", .SPATIAL_FLUSH)
  acc       <- .run_accumulator(fr)
  # Seed the schema from the combined template so an empty two-layer result (e.g.
  # an intersection with no overlaps) still finishes as a correctly typed node.
  if (two) acc$push(.coerce_for_vtr(out_template))
  piece_off <- 0L
  cov_err   <- 0
  worst     <- NULL                                  # top offending inputs by coverage error

  # Turn one batch's arrangement (pieces x covering inputs) into output rows.
  # Self-union: each piece-row is fanned to one output row per original record the
  # covering distinct input stands for, carrying that record's attributes.
  emit_single <- function(geoms, origin, fid, gi, poff) {
    members <- mem_by_dk[gi[origin]]                 # original rows per piece-row
    mult    <- lengths(members)
    rep_row <- rep.int(seq_along(origin), mult)      # piece-row index per fanned row
    pid     <- poff + fid
    df  <- attrs[unlist(members, use.names = FALSE), , drop = FALSE]
    df[[piece]] <- pid[rep_row]                      # rows of one face share a piece id
    df[[geom]]  <- geoms[rep_row]
    rownames(df) <- NULL
    df
  }

  # Two-layer: group the covering inputs of each face by layer, then combine the
  # x-records and y-records covering a face per `how`. A face's covering rows are
  # the (x record) x (y record) pairs; faces touching only one layer fill the
  # other side with NA. All output rows of a face share its piece id, and the
  # face geometry is taken once (every covering row of a face holds the same one).
  emit_two <- function(geoms, origin, fid, gi, poff) {
    members  <- mem_by_dk[gi[origin]]
    rows     <- unlist(members, use.names = FALSE)   # original rows covering each piece-row
    face_rep <- rep.int(fid, lengths(members))       # face id (batch-local, 1-based) per row
    sd       <- side_of[rows]; loc <- local_of[rows]
    nf   <- max(fid)
    fdup <- !duplicated(fid)
    geom_by_face <- character(nf)
    geom_by_face[fid[fdup]] <- geoms[fdup]           # one representative geometry per face

    ix <- data.frame(face = face_rep[sd == 1L], loc = loc[sd == 1L])  # x coverage
    iy <- data.frame(face = face_rep[sd == 2L], loc = loc[sd == 2L])  # y coverage

    block <- function(face_vec, lx, ly) {
      if (!length(face_vec)) return(NULL)
      m  <- length(face_vec)
      d  <- ax[if (is.null(lx)) rep(NA_integer_, m) else lx, , drop = FALSE]
      dy <- ay[if (is.null(ly)) rep(NA_integer_, m) else ly, , drop = FALSE]
      for (nm in names(dy)) d[[nm]] <- dy[[nm]]
      d[[piece]] <- poff + face_vec
      d[[geom]]  <- geom_by_face[face_vec]
      rownames(d) <- NULL
      d
    }

    out <- list()
    if (how != "symdiff" && nrow(ix) && nrow(iy)) {  # pieces covered by both layers
      mm <- merge(ix, iy, by = "face", suffixes = c(".x", ".y"))
      if (nrow(mm)) out <- c(out, list(block(mm$face, mm$loc.x, mm$loc.y)))
    }
    fx <- unique(ix$face); fy <- unique(iy$face)
    if (how %in% c("union", "identity", "symdiff")) {  # pieces with only x coverage
      xof <- setdiff(fx, fy)
      if (length(xof)) { s <- ix[ix$face %in% xof, ]; out <- c(out, list(block(s$face, s$loc, NULL))) }
    }
    if (how %in% c("union", "symdiff")) {              # pieces with only y coverage
      yof <- setdiff(fy, fx)
      if (length(yof)) { s <- iy[iy$face %in% yof, ]; out <- c(out, list(block(s$face, NULL, s$loc))) }
    }
    out <- out[!vapply(out, is.null, logical(1))]
    if (!length(out)) out_template else do.call(rbind, out)
  }

  emit_fn <- if (two) emit_two else emit_single

  # Overlay one batch of tiles, map pieces back to global rows, stream to spill.
  run_batch <- function(batch) {
    job <- rep.int(seq_along(batch), vapply(batch, function(t) length(t$idx), integer(1)))
    gi  <- unlist(lapply(batch, `[[`, "idx"), use.names = FALSE)
    rct <- unlist(lapply(batch, `[[`, "rect"), use.names = FALSE)
    res <- .Call(C_overlay_run, cwkb[gi], as.integer(job), as.double(rct),
                 as.integer(nthreads), as.double(precision))
    geoms <- res[[1L]]; origin <- res[[2L]]; parea <- res[[3L]]
    iarea <- res[[4L]]; fid <- res[[5L]]
    if (length(geoms)) {
      df <- emit_fn(geoms, origin, fid, gi, piece_off)
      acc$push(.coerce_for_vtr(df))
      piece_off <<- piece_off + max(fid)
      det <- .overlay_coverage_detail(origin, parea, iarea)
      if (nrow(det)) cov_err <<- max(cov_err, max(det$err))
      bad <- det[det$err > 1e-4, , drop = FALSE]
      if (nrow(bad)) {
        bad$row <- rep_idx[gi[bad$src]]               # distinct chunk -> original row
        worst <<- rbind(worst, bad[, c("row", "err", "iarea", "cov")])
        worst <<- worst[utils::head(order(worst$err, decreasing = TRUE), 50L), , drop = FALSE]
      }
    }
  }
  # A batch is overlaid with one thread per tile and load-balanced across the pool
  # only within the batch, so each batch must hold many more tiles than threads, or
  # a batch that gathers several large tiles leaves most threads idle on its tail.
  # Batch by a fixed tile count, not input bytes: a feature spanning many tiles is
  # shared in memory but its bytes recur in every tile, so a byte budget collapses
  # to a handful of tiles wherever such features dominate. Peak working set stays
  # bounded by tile_bytes times the running threads, independent of the batch size.
  batch_tiles <- max(256L, 32L * nthreads)
  pb <- if (!quiet) utils::txtProgressBar(0, length(jobs), style = 3) else NULL

  njob <- length(jobs)
  for (start in seq.int(1L, njob, by = batch_tiles)) {
    last <- min(start + batch_tiles - 1L, njob)
    run_batch(jobs[start:last])
    if (!is.null(pb)) utils::setTxtProgressBar(pb, last)
  }
  if (!is.null(pb)) close(pb)
  if (piece_off == 0L) stop("overlay produced no polygonal pieces")
  if (cov_err > 1e-4) {
    rows <- if (!is.null(worst))
      paste(utils::head(worst$row[order(worst$err, decreasing = TRUE)], 5L), collapse = ", ")
    else "unknown"
    warning(sprintf(paste0("spatial_overlay: coverage rel-err %.2e exceeds 1e-4; %d input(s) did not ",
                           "reconstruct from their pieces (worst rows in `x`: %s). Their geometry is ",
                           "finer than, or invalid after, the snapping grid (%.4g); pass grid= to adjust."),
                    cov_err, if (is.null(worst)) 0L else nrow(worst), rows, grid),
            call. = FALSE)
  }
  attr_node <- acc$finish(crs = crs, empty_geom = geom)
  if (!is.null(worst))
    attr(attr_node, "coverage_offenders") <-
      worst[order(worst$err, decreasing = TRUE), , drop = FALSE]
  attr_node
}
