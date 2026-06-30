# Recovery tests for floor_time() and resample(). References use base-R epoch
# arithmetic in UTC (the engine floors in UTC via gmtime).

ref_floor_fixed <- function(secs, bucket) floor(secs / bucket) * bucket

ref_floor_month <- function(t) {
  lt <- as.POSIXlt(t, tz = "UTC")
  lt$mday <- 1; lt$hour <- 0; lt$min <- 0; lt$sec <- 0
  as.numeric(as.POSIXct(lt, tz = "UTC"))
}
ref_floor_year <- function(t) {
  lt <- as.POSIXlt(t, tz = "UTC")
  lt$mon <- 0; lt$mday <- 1; lt$hour <- 0; lt$min <- 0; lt$sec <- 0
  as.numeric(as.POSIXct(lt, tz = "UTC"))
}

make_ts <- function(n, start = "2021-03-14 00:00:00", step = 600) {
  t0 <- as.POSIXct(start, tz = "UTC")
  data.frame(t = t0 + seq.int(0, by = step, length.out = n),
             temp = round(rnorm(n, 15, 5), 2),
             id = seq_len(n))
}

set.seed(11)

test_that("floor_time fixed units match base-R epoch flooring", {
  df <- make_ts(50, step = 137)   # irregular-ish within hours
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f)); write_vtr(df, f)
  secs <- as.numeric(df$t)

  res <- tbl(f) |>
    mutate(fh = floor_time(t, "hour"),
           fq = floor_time(t, "15 min"),
           fd = floor_time(t, "day")) |>
    collect()
  res <- res[order(res$id), ]

  expect_equal(res$fh, ref_floor_fixed(secs, 3600))
  expect_equal(res$fq, ref_floor_fixed(secs, 900))
  expect_equal(res$fd, ref_floor_fixed(secs, 86400))
})

test_that("floor_time calendar units (month, year) match base R", {
  df <- make_ts(40, start = "2020-11-20 08:30:00", step = 86400 * 9)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f)); write_vtr(df, f)

  res <- tbl(f) |>
    mutate(fm = floor_time(t, "month"), fy = floor_time(t, "year")) |>
    collect()
  res <- res[order(res$id), ]

  expect_equal(res$fm, vapply(df$t, ref_floor_month, numeric(1)))
  expect_equal(res$fy, vapply(df$t, ref_floor_year,  numeric(1)))
})

test_that("resample buckets and aggregates like a manual hourly group_by", {
  df <- make_ts(120, step = 300)   # 5-min steps -> 12 per hour
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f)); write_vtr(df, f)

  got <- tbl(f) |>
    resample(t, "1 hour", mean_temp = mean(temp), n = n()) |>
    collect()
  got <- got[order(got$t), ]

  bucket <- ref_floor_fixed(as.numeric(df$t), 3600)
  ref <- aggregate(temp ~ bucket, data.frame(bucket = bucket, temp = df$temp),
                   FUN = mean)
  ref <- ref[order(ref$bucket), ]
  cnt <- as.numeric(table(bucket)[as.character(ref$bucket)])

  expect_equal(nrow(got), nrow(ref))
  expect_equal(got$t, ref$bucket)
  expect_equal(got$mean_temp, ref$temp, tolerance = 1e-6)
  expect_equal(got$n, cnt)
})

test_that("resample errors without a named aggregation", {
  df <- make_ts(10)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f)); write_vtr(df, f)
  expect_error(resample(tbl(f), t, "1 hour"), "named aggregation")
})

test_that("Date columns floor to month in day units", {
  d <- as.Date("2019-01-01") + c(0, 40, 75, 400)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(d = d, id = seq_along(d)), f)

  res <- tbl(f) |> mutate(fm = floor_time(d, "month")) |> collect()
  res <- res[order(res$id), ]
  ref <- as.numeric(as.Date(format(d, "%Y-%m-01")))
  expect_equal(res$fm, ref)
})
