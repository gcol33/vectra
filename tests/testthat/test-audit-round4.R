# Regression tests for the round-4 audit fixes (0.11.5).

test_that("streaming roll_min/roll_max do not overflow the deque (long partition, short window)", {
  # Before the fix the min/max monotonic deque used absolute, never-rebased
  # indices while its capacity only grew on value-window overflow, so a long
  # partition with a short window (value window stays compacted) wrote OOB.
  n <- 3000
  t0 <- as.POSIXct("2021-01-01", tz = "UTC")
  t <- t0 + seq_len(n)                     # 1s apart; "2 sec" window holds ~2 rows
  v <- as.numeric(n - seq_len(n))          # strictly decreasing: deque never tail-pops
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(t = t, v = v, id = seq_len(n)), f, batch_size = 128)
  r <- tbl(f) |> mutate(mx = roll_max(v, t, "2 sec"),
                        mn = roll_min(v, t, "2 sec")) |> collect()
  r <- r[order(r$id), ]
  sec <- as.numeric(t)
  refmx <- vapply(seq_len(n), function(i) max(v[sec > sec[i] - 2 & sec <= sec[i]]), numeric(1))
  refmn <- vapply(seq_len(n), function(i) min(v[sec > sec[i] - 2 & sec <= sec[i]]), numeric(1))
  expect_equal(r$mx, refmx)
  expect_equal(r$mn, refmn)
})

test_that("unary math over a double column is correct (no leak, no double free)", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  x <- c(4, 9, 16, 25)
  write_vtr(data.frame(x = x), f)
  r <- tbl(f) |> mutate(s = sqrt(x), a = abs(-x), l = log(x)) |> collect()
  expect_equal(r$s, sqrt(x))
  expect_equal(r$a, x)
  expect_equal(r$l, log(x))
})

test_that("binary-search pushdown keeps matching rows for fractional thresholds on sorted int columns", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  df <- data.frame(x = rep(0:9, each = 3))          # sorted ints, stored int64
  write_vtr(df, f, batch_size = 3)                  # many small groups -> sorted column
  expect_equal(sort((tbl(f) |> filter(x < 2.9) |> collect())$x), rep(0:2, each = 3))
  expect_equal(sort((tbl(f) |> filter(x <= 2.9) |> collect())$x), rep(0:2, each = 3))
  expect_equal(sort((tbl(f) |> filter(x > 6.1) |> collect())$x), rep(7:9, each = 3))
  expect_equal(sort((tbl(f) |> filter(x >= 7.1) |> collect())$x), rep(8:9, each = 3))
  # negative fractional (truncation-toward-zero rounds the wrong way)
  df2 <- data.frame(x = rep(-9:0, each = 3))
  f2 <- tempfile(fileext = ".vtr"); on.exit(unlink(f2), add = TRUE)
  write_vtr(df2, f2, batch_size = 3)
  expect_equal(sort((tbl(f2) |> filter(x > -2.9) |> collect())$x), rep(-2:0, each = 3))
})

test_that("an interior all-NaN row group does not drop matching rows on a sorted double column", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(y = c(1, 2, NaN, NaN, 3, 4)), f, batch_size = 2)
  got <- (tbl(f) |> filter(y < 5) |> collect())$y
  expect_equal(sort(got[!is.nan(got)]), c(1, 2, 3, 4))
})

test_that("fuzzy_join rejects non-string key/block columns instead of crashing", {
  a <- tempfile(fileext = ".vtr"); b <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(a, b)))
  write_vtr(data.frame(id = 1:3, k = c(10, 20, 30)), a)     # numeric key
  write_vtr(data.frame(id = 1:3, k = c(11, 21, 31)), b)
  expect_error(
    fuzzy_join(tbl(a), tbl(b), by = c("k" = "k"), max_dist = 1) |> collect(),
    "must be string"
  )
})

test_that("datetime parts are correct for pre-1970 dates and near-epoch POSIXct", {
  d <- as.Date(c("1965-03-01", "1970-01-02", "2021-02-15"))
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(d = d, id = seq_along(d)), f)
  r <- tbl(f) |> mutate(y = year(d), m = month(d), dd = day(d)) |> collect()
  r <- r[order(r$id), ]
  expect_equal(r$y,  c(1965, 1970, 2021))
  expect_equal(r$m,  c(3, 1, 2))
  expect_equal(r$dd, c(1, 2, 15))

  ts <- as.POSIXct(c("1970-01-01 01:00:00", "2021-06-01 12:30:45",
                     "1969-12-31 23:00:00"), tz = "UTC")
  f2 <- tempfile(fileext = ".vtr"); on.exit(unlink(f2), add = TRUE)
  write_vtr(data.frame(ts = ts, id = seq_along(ts)), f2)
  r2 <- tbl(f2) |> mutate(y = year(ts), h = hour(ts), s = second(ts)) |> collect()
  r2 <- r2[order(r2$id), ]
  expect_equal(r2$y, c(1970, 2021, 1969))   # near-epoch POSIXct no longer read as days
  expect_equal(r2$h, c(1, 12, 23))
  expect_equal(r2$s, c(0, 45, 0))
})

test_that("as.Date rejects invalid dates as NA instead of normalizing", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(s = c("2021-02-30", "2020-02-29", "2021-13-01",
                             "abcd-01-01", "2021-06-15"), id = 1:5), f)
  r <- tbl(f) |> mutate(d = as.Date(s)) |> collect()
  r <- r[order(r$id), ]
  expect_equal(is.na(r$d), c(TRUE, FALSE, TRUE, TRUE, FALSE))
  expect_equal(as.Date(r$d[c(2, 5)], origin = "1970-01-01"),
               as.Date(c("2020-02-29", "2021-06-15")))
})

test_that("GeoTIFF with DEFLATE + horizontal predictor (2) decodes correctly", {
  skip_if_not_installed("terra")
  set.seed(3)
  r <- terra::rast(nrows = 30, ncols = 40,
                   vals = sample(0:60000, 1200), crs = "EPSG:4326")
  terra::ext(r) <- c(0, 40, 0, 30)
  f <- tempfile(fileext = ".tif"); on.exit(unlink(f))
  terra::writeRaster(r, f, overwrite = TRUE, datatype = "INT4U",
                     gdal = c("COMPRESS=DEFLATE", "PREDICTOR=2"))
  v <- tbl_tiff(f) |> collect()
  expect_equal(sort(v$band1), sort(terra::values(terra::rast(f))[, 1]))
})

test_that("ntile front-loads the remainder like dplyr", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(x = 1:10, id = 1:10), f)
  # dplyr::ntile(1:10, n) front-loads the remainder into the first groups
  expect_equal((tbl(f) |> arrange(x) |> mutate(nt = ntile(3)) |> collect())$nt,
               c(1, 1, 1, 1, 2, 2, 2, 3, 3, 3))
  expect_equal((tbl(f) |> arrange(x) |> mutate(nt = ntile(4)) |> collect())$nt,
               c(1, 1, 1, 2, 2, 2, 3, 3, 4, 4))
  expect_equal((tbl(f) |> arrange(x) |> mutate(nt = ntile(7)) |> collect())$nt,
               c(1, 1, 2, 2, 3, 3, 4, 5, 6, 7))
})

test_that("row_number(desc(x)) keeps ties in first-arrival order (grouped)", {
  x <- c(5, 5, 3, 1, 3)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(x = x, g = rep("a", 5), id = seq_along(x)), f)
  r <- tbl(f) |> group_by(g) |> mutate(rn = row_number(desc(x))) |> collect()
  r <- r[order(r$id), ]
  # dplyr::row_number(dplyr::desc(x)): rank descending, ties by first arrival
  expect_equal(r$rn, c(1L, 2L, 3L, 5L, 4L))
})

test_that("logical columns feed numeric/rank windows correctly", {
  x <- c(3, 7, 2, 9, 6)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(x = x, id = seq_along(x)), f)
  r <- tbl(f) |> mutate(c = cumsum(x > 5)) |> collect()
  r <- r[order(r$id), ]
  expect_equal(r$c, cumsum(x > 5))
})

test_that("zone-map pruning keeps rows whose quantized value crosses a predicate boundary", {
  # 4.6 reconstructs to 5.0 under precision=1; the raw max 4.6 must not prune it.
  x <- c(1.6, 2.6, 3.6, 4.6, 5.6)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(x = x, id = seq_along(x)), f,
            quantize = list(x = list(precision = 1, type = "int16")), batch_size = 1)
  res <- tbl(f) |> filter(x >= 5) |> collect()
  expect_equal(sort(res$id), c(4, 5))
})

test_that("int64 above 2^53 warns about precision loss on the fast collect path", {
  skip_if_not_installed("bit64")
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  big <- bit64::as.integer64(2)^60
  write_vtr(data.frame(x = big, id = 1L), f)
  expect_warning(tbl(f) |> collect(), "precision lost")
})

test_that("untyped SQLite columns (BLOB affinity) render numbers instead of all-NA", {
  skip_if_not_installed("RSQLite")
  skip_if_not_installed("DBI")
  f <- tempfile(fileext = ".sqlite"); on.exit(unlink(f))
  con <- DBI::dbConnect(RSQLite::SQLite(), f)
  DBI::dbExecute(con, "CREATE TABLE t (id INTEGER, x, label TEXT)")
  DBI::dbExecute(con, "INSERT INTO t VALUES (1, 42, 'a'), (2, 100, 'b')")
  DBI::dbDisconnect(con)
  r <- tbl_sqlite(f, "t") |> collect()
  expect_false(all(is.na(r$x)))
  expect_equal(r$x, c("42", "100"))
})

test_that("right_join suffixes colliding non-key columns like dplyr", {
  fx <- tempfile(fileext = ".vtr"); fy <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(fx, fy)))
  write_vtr(data.frame(id = 1:3, val = c(10, 20, 30)), fx)
  write_vtr(data.frame(id = 2:4, val = c(200, 300, 400)), fy)
  r <- right_join(tbl(fx), tbl(fy), by = "id") |> collect()
  expect_equal(names(r), c("id", "val.x", "val.y"))
  r <- r[order(r$id), ]
  expect_equal(r$val.x, c(20, 30, NA))
  expect_equal(r$val.y, c(200, 300, 400))
})

test_that("min/max propagate NaN order-independently", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(g = c("a", "a", "b", "b"),
                       x = c(1, NaN, NaN, 2)), f)
  r <- tbl(f) |> group_by(g) |> summarise(mn = min(x), mx = max(x)) |> collect()
  r <- r[order(r$g), ]
  expect_true(is.na(r$mn[1]) && is.na(r$mx[1]))   # group a: {1, NaN}
  expect_true(is.na(r$mn[2]) && is.na(r$mx[2]))   # group b: {NaN, 2}
  # na.rm drops the NaN
  r2 <- tbl(f) |> group_by(g) |> summarise(mn = min(x, na.rm = TRUE)) |> collect()
  r2 <- r2[order(r2$g), ]
  expect_equal(r2$mn, c(1, 2))
})

test_that("NaN double join keys match each other (consistent with grouping)", {
  fx <- tempfile(fileext = ".vtr"); fy <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(fx, fy)))
  write_vtr(data.frame(k = c(1, NaN, 3), vx = c("a", "b", "c")), fx)
  write_vtr(data.frame(k = c(NaN, 3), vy = c("B", "C")), fy)
  r <- inner_join(tbl(fx), tbl(fy), by = "k") |> collect()
  # NaN row of x meets NaN row of y; 3 meets 3
  expect_equal(sort(r$vx), c("b", "c"))
})

test_that("join with more than 16 key columns is rejected", {
  cols <- setNames(as.data.frame(matrix(1L, nrow = 2, ncol = 17)),
                   paste0("k", 1:17))
  fx <- tempfile(fileext = ".vtr"); fy <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(fx, fy)))
  write_vtr(cols, fx); write_vtr(cols, fy)
  expect_error(
    inner_join(tbl(fx), tbl(fy), by = paste0("k", 1:17)) |> collect(),
    "at most 16 key"
  )
})

test_that("%in% returns a logical (never NA) and matches NA via an NA in the set", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(x = c(1, 2, NA, 3), id = 1:4), f)
  r <- tbl(f) |> mutate(a = x %in% c(1, 3),
                        b = x %in% c(1, NA)) |> collect()
  r <- r[order(r$id), ]
  expect_equal(r$a, c(1, 2, NA, 3) %in% c(1, 3))     # NA operand -> FALSE
  expect_equal(r$b, c(1, 2, NA, 3) %in% c(1, NA))    # NA operand -> TRUE
  # strings
  fs <- tempfile(fileext = ".vtr"); on.exit(unlink(fs), add = TRUE)
  write_vtr(data.frame(s = c("a", "b", NA, "c"), id = 1:4), fs)
  rs <- tbl(fs) |> mutate(a = s %in% c("a", "c"),
                          b = s %in% c("a", NA)) |> collect()
  rs <- rs[order(rs$id), ]
  expect_equal(rs$a, c("a", "b", NA, "c") %in% c("a", "c"))
  expect_equal(rs$b, c("a", "b", NA, "c") %in% c("a", NA))
})

test_that("hash join bounds the many-to-many output across batches", {
  K <- 300L; M <- 300L                       # 90000 pairs, > JOIN_PROBE_EMIT_MAX
  fb <- tempfile(fileext = ".vtr"); fp <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(fb, fp)))
  write_vtr(data.frame(k = rep(1L, K), vb = seq_len(K)), fb)
  write_vtr(data.frame(k = rep(1L, M), vp = seq_len(M)), fp, batch_size = 131072)

  r <- inner_join(tbl(fp), tbl(fb), by = "k") |> collect()
  expect_equal(nrow(r), K * M)
  # single shared key => full cross product of (vp, vb)
  expected <- as.vector(outer(seq_len(M) * 10000L, seq_len(K), `+`))
  expect_equal(sort(r$vp * 10000L + r$vb), sort(expected))

  # emitted in bounded chunks, not one giant batch
  n_chunks <- 0L; max_rows <- 0L
  total <- collect_chunked(inner_join(tbl(fp), tbl(fb), by = "k"),
                           function(acc, chunk) {
                             n_chunks <<- n_chunks + 1L
                             max_rows <<- max(max_rows, nrow(chunk))
                             acc + nrow(chunk)
                           }, 0)
  expect_equal(total, K * M)
  expect_gt(n_chunks, 1L)
  expect_lte(max_rows, 65536L)
})

test_that("left join over a hot key resumes correctly and keeps unmatched rows", {
  K <- 300L; M <- 300L
  fb <- tempfile(fileext = ".vtr"); fp <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(fb, fp)))
  write_vtr(data.frame(k = rep(1L, K), vb = seq_len(K)), fb)
  px <- data.frame(k = c(rep(1L, M), 2L, 3L), vp = c(seq_len(M), 999L, 998L))
  write_vtr(px, fp, batch_size = 131072)
  rl <- left_join(tbl(fp), tbl(fb), by = "k") |> collect()
  expect_equal(nrow(rl), K * M + 2L)
  expect_equal(sum(is.na(rl$vb)), 2L)   # k = 2, 3 unmatched
})

test_that("BNL fallback (spilled hot key) also bounds the many-to-many output", {
  old <- options(vectra.mem = 4096)   # tiny budget: spill a single hot key to BNL
  on.exit(options(old))
  K <- 400L; M <- 400L
  fb <- tempfile(fileext = ".vtr"); fp <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(fb, fp)), add = TRUE)
  write_vtr(data.frame(k = rep(7L, K), vb = seq_len(K)), fb, batch_size = 64)
  write_vtr(data.frame(k = rep(7L, M), vp = seq_len(M)), fp, batch_size = 64)

  r <- inner_join(tbl(fp), tbl(fb), by = "k") |> collect()
  expect_equal(nrow(r), K * M)
  # single shared key => full cross product of (vp, vb)
  expected <- as.vector(outer(seq_len(M) * 10000L, seq_len(K), `+`))
  expect_equal(sort(r$vp * 10000L + r$vb), sort(expected))

  max_rows <- 0L
  total <- collect_chunked(inner_join(tbl(fp), tbl(fb), by = "k"),
                           function(acc, chunk) {
                             max_rows <<- max(max_rows, nrow(chunk)); acc + nrow(chunk)
                           }, 0)
  expect_equal(total, K * M)
  expect_lte(max_rows, 65536L)

  # left/full over the spilled hot key keep the unmatched rows
  px <- data.frame(k = c(rep(7L, M), 8L), vp = c(seq_len(M), 111L))
  fp2 <- tempfile(fileext = ".vtr"); on.exit(unlink(fp2), add = TRUE)
  write_vtr(px, fp2, batch_size = 64)
  rl <- left_join(tbl(fp2), tbl(fb), by = "k") |> collect()
  expect_equal(nrow(rl), K * M + 1L)
  expect_equal(sum(is.na(rl$vb)), 1L)
})
