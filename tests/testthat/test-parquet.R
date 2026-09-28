# Fixtures are written by fixtures/parquet/make_fixtures.py (pyarrow); every
# column is a closed-form function of the row index, mirrored here.
pq_dir <- test_path("fixtures", "parquet")
pq_file <- function(name) file.path(pq_dir, name)

pq_expected <- function(i = 0:299) {
  na <- function(x, cond) { x[cond] <- NA; x }
  uuid <- vapply(i, function(r) {
    h <- sprintf("%02x", (r * 7 + 0:15) %% 256)
    paste0(paste(h[1:4], collapse = ""), "-", paste(h[5:6], collapse = ""), "-",
           paste(h[7:8], collapse = ""), "-", paste(h[9:10], collapse = ""), "-",
           paste(h[11:16], collapse = ""))
  }, character(1))
  s <- ifelse(i %% 31 == 0, "",
              ifelse(i %% 29 == 0, "été",
                     paste0("s", i %% 37, strrep("_", i %% 5))))
  tags <- ifelse(i %% 9 == 1, "",
                 paste0("t", i %% 4, ";",
                        ifelse(i %% 5 == 0, "NA", paste0("u", i %% 3))))
  nums <- vapply(i, function(r) paste(seq_len(r %% 4) - 1, collapse = ";"),
                 character(1))
  data.frame(
    id = as.integer(i),
    b = na(i %% 3 == 0, i %% 11 == 0),
    i8 = na(as.integer((i * 37) %% 256 - 128), i %% 7 == 0),
    i16 = na(as.integer((i * 1231) %% 65536 - 32768), i %% 13 == 0),
    i32 = as.integer(na((i * 2654435761) %% 2^32 - 2^31, i %% 5 == 0)),
    i64 = na(i * i * 7919 - 3e9 * (i %% 2), i %% 17 == 0),
    u8 = as.integer((i * 13) %% 256),
    u16 = as.integer((i * 997) %% 65536),
    u32 = na((i * 2654435761) %% 2^32, i %% 19 == 0),
    u64 = i * 1e12,
    f32 = na(i / 8 - 50, i %% 9 == 0),
    f64 = na(i * 0.37 - 100, i %% 10 == 0),
    f16 = (i %% 100) / 4,
    s = na(s, i %% 6 == 0),
    s_long = sprintf("prefix_%05d_%d", i %/% 10, i),
    d = na(as.Date(i * 3 - 1000, origin = "1970-01-01"), i %% 8 == 0),
    ts_ms = na(.POSIXct((1600000000000 + i * 86400123) / 1e3, tz = "UTC"),
               i %% 12 == 0),
    ts_us = .POSIXct((1600000000000000 + i * 3600000001) / 1e6),
    ts_ns = .POSIXct((1.6e18 + i * 1e9) / 1e9, tz = "UTC"),
    t_ms = ((i * 1000003) %% 86400000) / 1e3,
    t_us = ((i * 1000000007) %% 86400000000) / 1e6,
    dec = na((i * 123 - 5000) / 100, i %% 14 == 0),
    dec_big = (i * 1000003 - 7) / 1000,
    uuid = uuid,
    code = na(sprintf("A%02d", i %% 100), i %% 15 == 0),
    tags = na(tags, i %% 9 == 0),
    nums = na(nums, i %% 10 == 3),
    st.a = na(as.integer(i * 2), i %% 7 == 3),
    st.b = na(paste0("b", i), i %% 7 == 3 | i %% 4 == 0),
    check.names = FALSE, stringsAsFactors = FALSE
  )
}

expect_pq_equal <- function(got, want, tz_check = TRUE) {
  expect_identical(names(got), names(want))
  expect_identical(nrow(got), nrow(want))
  for (nm in names(want)) {
    g <- got[[nm]]; w <- want[[nm]]
    if (tz_check) {
      expect_identical(class(g), class(w), label = nm)
      if (inherits(w, "POSIXct"))
        expect_identical(attr(g, "tzone"), attr(w, "tzone"), label = nm)
    }
    if (is.character(w)) expect_identical(g, w, label = nm)
    else expect_equal(as.numeric(g), as.numeric(w), tolerance = 1e-12,
                      label = nm)
  }
}

roundtrip_files <- c("plain_none_v1.parquet", "dict_snappy_v2.parquet",
                     "dict_lz4_v1.parquet", "delta_gzip_v1.parquet",
                     "delta_zstd_v2.parquet")

test_that("every encoding, codec and page version round-trips", {
  want <- pq_expected()
  for (f in roundtrip_files) {
    got <- tbl_parquet(pq_file(f)) |> collect()
    expect_pq_equal(got, want)
  }
})

test_that("INT96 timestamps and integer-stored decimals decode", {
  got <- tbl_parquet(pq_file("plain_zstd_v2.parquet")) |> collect()
  want <- pq_expected()
  expect_pq_equal(got, want, tz_check = FALSE)
  for (nm in c("ts_ms", "ts_us", "ts_ns")) {
    expect_s3_class(got[[nm]], "POSIXct")
    expect_identical(attr(got[[nm]], "tzone"), "UTC")
  }
})

test_that("batch boundaries anywhere in a page give the same rows", {
  want <- pq_expected()
  for (bs in c(1, 7, 64, 99, 1000)) {
    got <- tbl_parquet(pq_file("dict_snappy_v2.parquet"), batch_size = bs) |>
      collect()
    expect_pq_equal(got, want)
  }
})

test_that("list_sep sets the list element separator", {
  got <- tbl_parquet(pq_file("plain_none_v1.parquet"), list_sep = " | ") |>
    select(tags) |> collect()
  want <- pq_expected()$tags
  expect_identical(got$tags, gsub(";", " | ", want, fixed = TRUE))
})

test_that("only the selected columns are read", {
  node <- tbl_parquet(pq_file("delta_zstd_v2.parquet")) |> select(s, f64)
  plan <- paste(capture.output(explain(node)), collapse = "\n")
  expect_match(plan, "streaming parquet, 2/29 cols \\(pruned\\)")
  got <- collect(node)
  want <- pq_expected()
  expect_identical(got$s, want$s)
  expect_equal(got$f64, want$f64)
})

test_that("filters above the scan match an unfiltered collect", {
  want <- pq_expected()
  f <- pq_file("dict_snappy_v2.parquet")
  cases <- list(
    quote(id >= 150 & id < 170),
    quote(id < 5 | id > 290),
    quote(i32 > 0),
    quote(f64 <= -50),
    quote(s == "s1_"),
    quote(s %in% c("s2__", "s4____")),
    quote(u32 > 3e9),
    quote(d > as.Date("1970-06-01")),
    quote(st.a == 20),
    quote(!is.na(tags)),
    quote(b)
  )
  for (cond in cases) {
    got <- eval(bquote(tbl_parquet(f) |> filter(.(cond)) |> collect()))
    keep <- eval(cond, want)
    keep <- !is.na(keep) & keep
    expect_pq_equal(got, want[keep, , drop = FALSE] |> `rownames<-`(NULL))
  }
  plan <- paste(capture.output(explain(tbl_parquet(f) |> filter(id > 250))),
                collapse = "\n")
  expect_match(plan, "predicate pushdown")
})

test_that("row-group statistics skip groups a filter cannot match", {
  f <- pq_file("plain_none_v1.parquet")
  # Row group 2 of 3 is corrupted in a copy; a filter that its statistics rule
  # out never reads it.
  bytes <- readBin(f, "raw", file.size(f))
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp))
  mid <- length(bytes) %/% 2
  bytes[(mid - 200):(mid + 200)] <- as.raw(0xAB)
  writeBin(bytes, tmp)
  got <- tbl_parquet(tmp) |> filter(id < 50) |> select(id) |> collect()
  expect_identical(got$id, 0:49)
  full <- tryCatch(tbl_parquet(tmp) |> collect(), error = function(e) NULL)
  expect_false(identical(full, tbl_parquet(f) |> collect()))
})

test_that("verbs compose over a parquet scan", {
  f <- pq_file("delta_gzip_v1.parquet")
  want <- pq_expected()
  expect_equal(nrow(tbl_parquet(f)), 300)
  expect_true(is.na(nrow(tbl_parquet(f) |> filter(id > 3))))
  got <- tbl_parquet(f) |> mutate(g = u8 %% 3) |> group_by(g) |>
    summarise(n = n(), s = sum(f64)) |> collect()
  got <- got[order(got$g), ]
  exp_g <- (want$u8 %% 3)
  expect_equal(got$n, as.numeric(table(exp_g)))
  expect_equal(got$s, as.numeric(tapply(want$f64, exp_g, sum)))
  cnt <- tbl_parquet(f) |> count() |> collect()
  expect_equal(cnt$n, 300)
})

test_that("a directory or a vector of files reads as one table", {
  want <- pq_expected()
  got <- tbl_parquet(file.path(pq_dir, "dataset")) |> collect()
  expect_pq_equal(got, want)
  files <- file.path(pq_dir, "dataset", c("part=a/000.parquet", "part=b/001.parquet"))
  got2 <- tbl_parquet(files) |> filter(id >= 110 & id < 130) |> collect()
  expect_pq_equal(got2, want[111:130, ] |> `rownames<-`(NULL))
})

test_that("files with different column types are refused", {
  expect_error(tbl_parquet(c(pq_file("plain_none_v1.parquet"),
                             pq_file("nested.parquet"))),
               "missing")
  expect_error(tbl_parquet(c(pq_file("plain_none_v1.parquet"),
                             pq_file("plain_zstd_v2.parquet"))),
               "column 'ts_us'")
})

test_that("unsupported nested columns error when read and can be dropped", {
  f <- pq_file("nested.parquet")
  expect_error(tbl_parquet(f) |> collect(), "column 'deep'.*nested lists")
  got <- tbl_parquet(f) |> select(-deep) |> collect()
  expect_identical(names(got), c("id", "m.key", "m.value"))
  expect_identical(got$m.key, paste0("k", 0:9))
  expect_identical(got$m.value, as.character(0:9))
})

test_that("non-Parquet and damaged files error instead of crashing", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp))
  writeLines("not parquet at all", tmp)
  expect_error(tbl_parquet(tmp), "not a Parquet file")

  src <- readBin(pq_file("delta_zstd_v2.parquet"), "raw",
                 file.size(pq_file("delta_zstd_v2.parquet")))
  for (len in c(8L, 12L, 100L, length(src) %/% 2L, length(src) - 9L)) {
    writeBin(src[seq_len(len)], tmp)
    expect_error(tbl_parquet(tmp) |> collect())
  }

  # Random byte corruption anywhere in the file: every outcome must be either
  # a clean error or a result, never a crash.
  set.seed(42)
  for (f in c("delta_zstd_v2.parquet", "dict_lz4_v1.parquet",
              "plain_none_v1.parquet", "delta_gzip_v1.parquet")) {
    src <- readBin(pq_file(f), "raw", file.size(pq_file(f)))
    for (k in 1:60) {
      bad <- src
      pos <- sample(length(src), sample(1:8, 1))
      bad[pos] <- as.raw(sample(0:255, length(pos), replace = TRUE))
      writeBin(bad, tmp)
      res <- tryCatch(suppressWarnings(nrow(tbl_parquet(tmp) |> collect())),
                      error = function(e) -1L)
      expect_true(is.numeric(res))
    }
  }
})

test_that("the example file opens", {
  f <- system.file("extdata", "example.parquet", package = "vectra")
  skip_if(f == "")
  d <- tbl_parquet(f) |> collect()
  expect_identical(nrow(d), 100L)
})
