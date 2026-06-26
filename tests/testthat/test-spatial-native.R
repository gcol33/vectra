# Native libgeos compute paths for spatial_filter / spatial_clip /
# spatial_dissolve, checked head-to-head against the sf path. With crs = NA the
# geometry math is planar on both sides, so the native C path (taken when the
# predicate is recognised and the CRS is planar) must equal the sf path. The sf
# path is forced by wrapping the predicate in a closure that is not identical()
# to the exported sf function, which the native dispatcher therefore skips.

skip_if_not_installed("sf")

sq <- function(xmin, xmax, ymin, ymax)
  sf::st_polygon(list(rbind(c(xmin, ymin), c(xmax, ymin), c(xmax, ymax),
                            c(xmin, ymax), c(xmin, ymin))))

# A stream of polygons with varied relations to a resident locator layer.
stream_polys <- sf::st_sfc(
  sq(0, 1, 0, 1),        # inside A
  sq(0, 2, 0, 2),        # equals A
  sq(0.5, 2.5, 0, 1),    # straddles A's edge
  sq(1.8, 3.2, 0, 1),    # touches/near B
  sq(3, 5, 0, 2),        # equals B
  sq(3.2, 3.8, 0.2, 0.8),# inside B
  sq(6, 7, 6, 7))        # disjoint from both
resident <- sf::st_sf(rid = c("A", "B"),
                      geometry = sf::st_sfc(sq(0, 2, 0, 2), sq(3, 5, 0, 2)))

write_stream <- function() {
  df <- data.frame(pid = seq_along(stream_polys),
                   geometry = sf::st_as_binary(stream_polys, hex = TRUE))
  f <- tempfile(fileext = ".vtr")
  write_vtr(df, f)
  f
}

test_that("native filter equals sf filter for every recognised predicate", {
  f <- write_stream(); on.exit(unlink(f))
  preds <- list(
    intersects  = sf::st_intersects,
    within      = sf::st_within,
    contains    = sf::st_contains,
    overlaps    = sf::st_overlaps,
    covers      = sf::st_covers,
    covered_by  = sf::st_covered_by,
    touches     = sf::st_touches,
    crosses     = sf::st_crosses)
  for (nm in names(preds)) {
    p <- preds[[nm]]
    wrap <- function(a, b) p(a, b)        # not identical() to p -> sf path
    native <- collect(spatial_filter(tbl(f), resident, predicate = p, crs = NA))
    viasf  <- collect(spatial_filter(tbl(f), resident, predicate = wrap, crs = NA))
    expect_equal(sort(native$pid), sort(viasf$pid), info = nm)
    nativeN <- collect(spatial_filter(tbl(f), resident, predicate = p,
                                      negate = TRUE, crs = NA))
    viasfN  <- collect(spatial_filter(tbl(f), resident, predicate = wrap,
                                      negate = TRUE, crs = NA))
    expect_equal(sort(nativeN$pid), sort(viasfN$pid), info = paste0("!", nm))
  }
})

test_that("native join equals sf join, with multi-match attribute duplication", {
  f <- write_stream(); on.exit(unlink(f))
  # Overlapping resident features so a straddling left polygon matches both,
  # exercising the row replication the join performs.
  resj <- sf::st_sf(rid = c("C", "D"),
                    geometry = sf::st_sfc(sq(0, 3, 0, 3), sq(1, 4, 0, 2)))
  key <- function(d)
    sort(paste(d$pid, ifelse(is.na(d$rid), "NA", d$rid), sep = "|"))
  preds <- list(intersects = sf::st_intersects, within = sf::st_within,
                contains = sf::st_contains, covered_by = sf::st_covered_by)
  for (nm in names(preds)) {
    p <- preds[[nm]]
    wrap <- function(a, b) p(a, b)          # not identical() to p -> sf path
    for (lf in c(TRUE, FALSE)) {
      nat <- collect(spatial_join(tbl(f), resj, join = p, crs = NA, left = lf))
      ref <- collect(spatial_join(tbl(f), resj, join = wrap, crs = NA, left = lf))
      expect_equal(key(nat), key(ref), info = paste0(nm, " left=", lf))
      expect_equal(names(nat)[order(names(nat))],
                   names(ref)[order(names(ref))], info = paste0(nm, " cols"))
    }
  }
})

test_that("native clip and erase equal sf, geometry-for-geometry", {
  f <- write_stream(); on.exit(unlink(f))
  mask <- sf::st_sfc(sq(1, 4, 0.5, 1.5))

  clip_nat <- collect_sf(spatial_clip(tbl(f), mask, crs = NA))
  ref <- sf::st_sf(pid = seq_along(stream_polys), geometry = stream_polys)
  clip_ref <- suppressWarnings(sf::st_intersection(ref, sf::st_union(mask)))
  clip_nat <- clip_nat[order(clip_nat$pid), ]
  clip_ref <- clip_ref[order(clip_ref$pid), ]
  expect_equal(clip_nat$pid, clip_ref$pid)
  expect_equal(as.numeric(sf::st_area(clip_nat)),
               as.numeric(sf::st_area(clip_ref)), tolerance = 1e-9)

  erase_nat <- collect_sf(spatial_clip(tbl(f), mask, erase = TRUE, crs = NA))
  erase_ref <- suppressWarnings(sf::st_difference(ref, sf::st_union(mask)))
  erase_nat <- erase_nat[order(erase_nat$pid), ]
  erase_ref <- erase_ref[order(erase_ref$pid), ]
  expect_equal(erase_nat$pid, erase_ref$pid)
  expect_equal(sum(as.numeric(sf::st_area(erase_nat))),
               sum(as.numeric(sf::st_area(erase_ref))), tolerance = 1e-9)
})

test_that("native dissolve equals sf union per group", {
  band <- c(1, 1, 1, 2, 2, 2, 2)
  df <- data.frame(band = band, w = seq_along(band),
                   geometry = sf::st_as_binary(stream_polys, hex = TRUE))
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(df, f)

  got <- collect_sf(spatial_dissolve(tbl(f), by = "band", crs = NA,
                                     .fun = list(wsum = function(d) sum(d$w))))
  got <- got[order(got$band), ]
  ref_area <- vapply(sort(unique(band)), function(b)
    as.numeric(sf::st_area(sf::st_union(stream_polys[band == b]))), numeric(1))
  expect_equal(nrow(got), length(ref_area))
  expect_equal(as.numeric(sf::st_area(got)), ref_area, tolerance = 1e-9)
  expect_equal(got$wsum[order(got$band)],
               c(sum((seq_along(band))[band == 1]),
                 sum((seq_along(band))[band == 2])))
})

test_that("native path is gated to planar; spherical geographic falls back", {
  # planar_ok: NA crs and projected -> native; geographic + s2 on -> sf.
  expect_true(vectra:::.geos_planar_ok(sf::st_crs(NA)))
  expect_true(vectra:::.geos_planar_ok(sf::st_crs(32119)))     # projected
  old <- sf::sf_use_s2(TRUE); on.exit(sf::sf_use_s2(old))
  expect_false(vectra:::.geos_planar_ok(sf::st_crs(4326)))     # lon/lat + s2
  sf::sf_use_s2(FALSE)
  expect_true(vectra:::.geos_planar_ok(sf::st_crs(4326)))      # lon/lat, s2 off
})

test_that("predicate codes map the native set; others are NA (sf fallback)", {
  expect_equal(vectra:::.geos_pred_code(NULL), 0L)
  expect_equal(vectra:::.geos_pred_code(sf::st_within), 1L)
  expect_equal(vectra:::.geos_pred_code(sf::st_equals), 8L)
  expect_equal(vectra:::.geos_pred_code(sf::st_disjoint), 9L)
  expect_equal(vectra:::.geos_pred_code(sf::st_is_within_distance), 10L)
  expect_true(is.na(vectra:::.geos_pred_code(sf::st_nearest_feature)))
  expect_true(is.na(vectra:::.geos_pred_code(function(a, b) NULL)))
})

test_that("native filter equals / disjoint / within_distance match sf", {
  f <- write_stream(); on.exit(unlink(f))
  for (nm in c("equals", "disjoint")) {
    p <- getExportedValue("sf", paste0("st_", nm))
    wrap <- function(a, b) p(a, b)            # not identical() to p -> sf path
    nat <- collect(spatial_filter(tbl(f), resident, predicate = p, crs = NA))
    ref <- collect(spatial_filter(tbl(f), resident, predicate = wrap, crs = NA))
    expect_equal(sort(nat$pid), sort(ref$pid), info = nm)
    natN <- collect(spatial_filter(tbl(f), resident, predicate = p,
                                   negate = TRUE, crs = NA))
    refN <- collect(spatial_filter(tbl(f), resident, predicate = wrap,
                                   negate = TRUE, crs = NA))
    expect_equal(sort(natN$pid), sort(refN$pid), info = paste0("!", nm))
  }
  pw <- sf::st_is_within_distance
  wrapw <- function(a, b, ...) pw(a, b, ...)
  for (d in c(0.1, 0.5, 1.5)) {
    nat <- collect(spatial_filter(tbl(f), resident, predicate = pw,
                                  dist = d, crs = NA))
    ref <- collect(spatial_filter(tbl(f), resident, predicate = wrapw,
                                  dist = d, crs = NA))
    expect_equal(sort(nat$pid), sort(ref$pid), info = paste0("within ", d))
  }
})

test_that("native join equals / within_distance / nearest match sf", {
  f <- write_stream(); on.exit(unlink(f))
  resj <- sf::st_sf(rid = c("C", "D"),
                    geometry = sf::st_sfc(sq(0, 3, 0, 3), sq(1, 4, 0, 2)))
  key <- function(d)
    sort(paste(d$pid, ifelse(is.na(d$rid), "NA", d$rid), sep = "|"))
  for (lf in c(TRUE, FALSE)) {
    pe <- sf::st_equals; wrape <- function(a, b) pe(a, b)
    nat <- collect(spatial_join(tbl(f), resj, join = pe, crs = NA, left = lf))
    ref <- collect(spatial_join(tbl(f), resj, join = wrape, crs = NA, left = lf))
    expect_equal(key(nat), key(ref), info = paste0("equals left=", lf))

    pw <- sf::st_is_within_distance; wrapw <- function(a, b, ...) pw(a, b, ...)
    nat <- collect(spatial_join(tbl(f), resj, join = pw, dist = 0.5,
                                crs = NA, left = lf))
    ref <- collect(spatial_join(tbl(f), resj, join = wrapw, dist = 0.5,
                                crs = NA, left = lf))
    expect_equal(key(nat), key(ref), info = paste0("within left=", lf))
  }
  pn <- sf::st_nearest_feature; wrapn <- function(a, b) pn(a, b)
  nat <- collect(spatial_join(tbl(f), resj, join = pn, crs = NA))
  ref <- collect(spatial_join(tbl(f), resj, join = wrapn, crs = NA))
  expect_equal(key(nat), key(ref))
})

# Raw-coordinate point input: spatial_filter / spatial_join build points in C
# (C_geos_locate_xy) instead of decoding the batch to sf, and the join emits the
# point geometry from C_geos_points_to_hex. Same crs = NA planar gate, same sf
# path forced by a non-identical predicate wrapper.
pt_stream <- data.frame(
  pid = 1:7,
  x = c(0.5, 1.0, 2.5, 4.0, 6.5, 1.5, 3.5),
  y = c(0.5, 1.0, 0.5, 1.0, 6.5, 0.5, 1.0))

write_points <- function() {
  f <- tempfile(fileext = ".vtr"); write_vtr(pt_stream, f); f
}

test_that("native coords filter matches sf for points", {
  f <- write_points(); on.exit(unlink(f))
  for (nm in c("intersects", "within", "covered_by")) {
    p <- getExportedValue("sf", paste0("st_", nm))
    wrap <- function(a, b) p(a, b)
    nat <- collect(spatial_filter(tbl(f), resident, predicate = p,
                                  coords = c("x", "y"), crs = NA))
    ref <- collect(spatial_filter(tbl(f), resident, predicate = wrap,
                                  coords = c("x", "y"), crs = NA))
    expect_equal(sort(nat$pid), sort(ref$pid), info = nm)
  }
  pw <- sf::st_is_within_distance; wrapw <- function(a, b, ...) pw(a, b, ...)
  for (d in c(0.3, 0.6, 2.0)) {
    nat <- collect(spatial_filter(tbl(f), resident, predicate = pw,
                                  dist = d, coords = c("x", "y"), crs = NA))
    ref <- collect(spatial_filter(tbl(f), resident, predicate = wrapw,
                                  dist = d, coords = c("x", "y"), crs = NA))
    expect_equal(sort(nat$pid), sort(ref$pid), info = paste0("within ", d))
  }
})

test_that("native coords join matches sf, point geometry included", {
  f <- write_points(); on.exit(unlink(f))
  resj <- sf::st_sf(rid = c("C", "D"),
                    geometry = sf::st_sfc(sq(0, 3, 0, 3), sq(1, 4, 0, 2)))
  key <- function(d)
    sort(paste(d$pid, ifelse(is.na(d$rid), "NA", d$rid), sep = "|"))
  for (lf in c(TRUE, FALSE)) {
    pj <- sf::st_intersects; wrapj <- function(a, b) pj(a, b)
    nat <- collect(spatial_join(tbl(f), resj, join = pj, coords = c("x", "y"),
                                crs = NA, left = lf))
    ref <- collect(spatial_join(tbl(f), resj, join = wrapj, coords = c("x", "y"),
                                crs = NA, left = lf))
    expect_equal(key(nat), key(ref), info = paste0("intersects left=", lf))
    expect_equal(names(nat)[order(names(nat))],
                 names(ref)[order(names(ref))], info = paste0("cols left=", lf))
  }
  # within-distance and nearest
  pw <- sf::st_is_within_distance; wrapw <- function(a, b, ...) pw(a, b, ...)
  nat <- collect(spatial_join(tbl(f), resj, join = pw, dist = 0.6,
                              coords = c("x", "y"), crs = NA))
  ref <- collect(spatial_join(tbl(f), resj, join = wrapw, dist = 0.6,
                              coords = c("x", "y"), crs = NA))
  expect_equal(key(nat), key(ref), info = "within")
  pn <- sf::st_nearest_feature; wrapn <- function(a, b) pn(a, b)
  nat <- collect(spatial_join(tbl(f), resj, join = pn, coords = c("x", "y"),
                              crs = NA))
  ref <- collect(spatial_join(tbl(f), resj, join = wrapn, coords = c("x", "y"),
                              crs = NA))
  expect_equal(key(nat), key(ref), info = "nearest")

  # the emitted point geometry equals the sf-encoded points, row for row
  o   <- order(paste(nat$pid, nat$rid))
  oR  <- order(paste(ref$pid, ref$rid))
  gn  <- sf::st_as_sfc(structure(nat$geometry[o],  class = "WKB"), EWKB = FALSE)
  gr  <- sf::st_as_sfc(structure(ref$geometry[oR], class = "WKB"), EWKB = FALSE)
  expect_equal(unname(sf::st_coordinates(gn)), unname(sf::st_coordinates(gr)))
})
