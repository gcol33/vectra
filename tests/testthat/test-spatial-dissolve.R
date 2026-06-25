# spatial_dissolve (aggregate geometries by group) -- the streamed partition-tier
# dissolve checked against sf in-memory ground truth. Planar CRS = NA so union
# areas are exact Cartesian and the streamed path must equal the resident path.

skip_if_not_installed("sf")

make_square <- function(xmin, xmax, ymin, ymax) {
  sf::st_polygon(list(rbind(
    c(xmin, ymin), c(xmax, ymin), c(xmax, ymax),
    c(xmin, ymax), c(xmin, ymin))))
}

# Two overlapping squares in band A (union area 3) and two disjoint squares in
# band B (union area 2); a `val` attribute to summarise.
fixture <- function() {
  geoms <- sf::st_sfc(
    make_square(0, 2, 0, 1), make_square(1, 3, 0, 1),   # band A, overlap [1,2]
    make_square(0, 1, 2, 3), make_square(2, 3, 2, 3))   # band B, disjoint
  df <- data.frame(
    band = c("A", "A", "B", "B"),
    val  = c(10, 20, 5, 7),
    geometry = sf::st_as_binary(geoms, hex = TRUE),
    stringsAsFactors = FALSE)
  list(geoms = geoms, df = df)
}

test_that("dissolve unions geometry within each group, matching sf", {
  fx <- fixture()
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(fx$df, f)

  out <- tbl(f) |> spatial_dissolve(by = "band", crs = NA) |> collect_sf()
  out <- out[order(out$band), ]
  expect_equal(out$band, c("A", "B"))
  # one feature per group, geometry = union of that group's squares
  refA <- sf::st_union(fx$geoms[1:2])
  refB <- sf::st_union(fx$geoms[3:4])
  expect_equal(as.numeric(sf::st_area(out)),
               c(as.numeric(sf::st_area(refA)),
                 as.numeric(sf::st_area(refB))), tolerance = 1e-9)
  expect_equal(as.numeric(sf::st_area(out)), c(3, 2), tolerance = 1e-9)
})

test_that(".fun summarises attributes per group", {
  fx <- fixture()
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(fx$df, f)

  out <- tbl(f) |>
    spatial_dissolve(by = "band", crs = NA,
                     .fun = list(total = function(d) sum(d$val),
                                 n = function(d) nrow(d))) |>
    collect_sf()
  out <- out[order(out$band), ]
  expect_equal(sort(names(out)),
               sort(c("band", "total", "n", "geometry")))
  expect_equal(out$total, c(30, 12))   # A: 10+20, B: 5+7
  expect_equal(out$n, c(2, 2))
})

test_that("by = NULL dissolves the whole layer into one feature", {
  fx <- fixture()
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(fx$df, f)

  out <- tbl(f) |> spatial_dissolve(crs = NA) |> collect_sf()
  expect_equal(nrow(out), 1L)
  # bands A and B are disjoint from each other: total area 3 + 2 = 5
  expect_equal(as.numeric(sf::st_area(out)), 5, tolerance = 1e-9)
})

test_that("multi-column by groups on the value combination", {
  geoms <- sf::st_sfc(
    make_square(0, 1, 0, 1), make_square(1, 2, 0, 1),   # r1/c1, r1/c2
    make_square(0, 1, 2, 3), make_square(1, 2, 2, 3))   # r2/c1, r2/c2
  df <- data.frame(
    region = c("r1", "r1", "r2", "r2"),
    cls    = c("c1", "c2", "c1", "c1"),
    geometry = sf::st_as_binary(geoms, hex = TRUE),
    stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(df, f)

  out <- tbl(f) |>
    spatial_dissolve(by = c("region", "cls"), crs = NA) |> collect_sf()
  key <- paste(out$region, out$cls)
  ord <- order(key)
  out <- out[ord, ]; key <- key[ord]
  # three groups: (r1,c1), (r1,c2), (r2,c1 = two squares unioned)
  expect_equal(key, c("r1 c1", "r1 c2", "r2 c1"))
  expect_equal(as.numeric(sf::st_area(out)), c(1, 1, 2), tolerance = 1e-9)
})

test_that("streamed dissolve equals the single-batch resident dissolve", {
  set.seed(31)
  k <- 120L
  # a strip of unit squares, each tagged into one of 4 bands
  geoms <- do.call(sf::st_sfc,
                   lapply(seq_len(k), function(i) make_square(i, i + 1, 0, 1)))
  band  <- sample(letters[1:4], k, replace = TRUE)
  df <- data.frame(band = band,
                   geometry = sf::st_as_binary(geoms, hex = TRUE),
                   stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(df, f, batch_size = 9L)                 # several read batches
  old <- options(vectra.spatial_flush = 13,
                 vectra.partition_budget = 17)      # several routing flushes
  on.exit(options(old), add = TRUE)

  got <- tbl(f) |> spatial_dissolve(by = "band", crs = NA) |> collect_sf()
  got <- got[order(got$band), ]

  resident <- sf::st_sf(band = band, geometry = geoms)
  ref <- lapply(sort(unique(band)), function(b)
    sf::st_union(sf::st_geometry(resident[resident$band == b, ])))
  expect_equal(got$band, sort(unique(band)))
  expect_equal(as.numeric(sf::st_area(got)),
               vapply(ref, function(g) as.numeric(sf::st_area(g)), numeric(1)),
               tolerance = 1e-9)
})

test_that("spatial_dissolve validates inputs", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(band = "A",
                       geometry = "01010000000000000000000000"), f)
  expect_error(spatial_dissolve(42), "must be a vectra_node")
  expect_error(spatial_dissolve(tbl(f), by = 1), "character vector")
  expect_error(spatial_dissolve(tbl(f), .fun = list(function(d) 1)),
               "named list")
  expect_error(spatial_dissolve(tbl(f), by = "nope"), "not found")
})
