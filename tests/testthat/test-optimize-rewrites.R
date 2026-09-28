# Column pruning through projections and predicate pushdown through
# projections / concats: every plan must return what the same query computes on
# the collected data frame.

opt_fixture <- function(n = 5000, rg = 500) {
  set.seed(42)
  df <- data.frame(
    id = seq_len(n),
    k  = rep(seq_len(n / 10), each = 10),
    a  = round(runif(n, 0, 100), 3),
    b  = runif(n),
    sp = sample(c("ant", "bee", "cat", "dog"), n, TRUE),
    stringsAsFactors = FALSE
  )
  df$a[df$id > 1000 & df$id <= 1500] <- NA   # one all-NA row group
  df$b[sample(n, 200)] <- NA
  f <- tempfile(fileext = ".vtr")
  write_vtr(df, f, batch_size = rg)
  list(df = df, f = f)
}

plan_text <- function(node) paste(capture.output(explain(node)), collapse = "\n")

expect_same_rows <- function(got, want) {
  got <- as.data.frame(got)
  want <- as.data.frame(want)
  rownames(want) <- NULL
  expect_equal(names(got), names(want))
  ord_g <- do.call(order, unname(as.list(got)))
  ord_w <- do.call(order, unname(as.list(want)))
  g <- got[ord_g, , drop = FALSE]; rownames(g) <- NULL
  w <- want[ord_w, , drop = FALSE]; rownames(w) <- NULL
  for (nm in names(w)) if (is.integer(w[[nm]])) w[[nm]] <- as.double(w[[nm]])
  expect_equal(g, w)
}

test_that("mutate() before filter() prunes row groups and keeps the rows", {
  fx <- opt_fixture(); on.exit(unlink(fx$f))
  df <- fx$df

  q <- tbl(fx$f) |> mutate(z = a * 2) |> filter(id <= 700)
  expect_match(plan_text(q), "predicate pushdown")
  got <- tbl(fx$f) |> mutate(z = a * 2) |> filter(id <= 700) |> collect()
  want <- transform(df, z = a * 2)[df$id <= 700, ]
  expect_same_rows(got, want)
})

test_that("pushdown renames through rename() and select()", {
  fx <- opt_fixture(); on.exit(unlink(fx$f))
  df <- fx$df

  got <- tbl(fx$f) |> rename(key = id) |> filter(key > 4200) |> collect()
  want <- df[df$id > 4200, ]; names(want)[names(want) == "id"] <- "key"
  expect_same_rows(got, want)

  q <- tbl(fx$f) |> select(sp, key = id) |> filter(key > 4200)
  expect_match(plan_text(q), "predicate pushdown")
  got <- q |> collect()
  want <- data.frame(sp = df$sp, key = df$id)[df$id > 4200, ]
  expect_same_rows(got, want)
})

test_that("a column overwritten by mutate() is never pruned on its stored values", {
  fx <- opt_fixture(); on.exit(unlink(fx$f))
  df <- fx$df

  # id * 0 + 1 is 1 on every row: the stored id statistics must not be used.
  got <- tbl(fx$f) |> mutate(id = id * 0 + 1) |> filter(id == 1) |>
    summarise(n = n()) |> collect()
  expect_equal(got$n, nrow(df))

  got <- tbl(fx$f) |> mutate(a = -a) |> filter(a < -90) |> collect()
  want <- transform(df, a = -a); want <- want[!is.na(want$a) & want$a < -90, ]
  expect_same_rows(got, want)

  # a swap of names through rename-style col refs
  got <- tbl(fx$f) |> select(id = k, k = id) |> filter(id <= 3) |> collect()
  want <- data.frame(id = df$k, k = df$id)[df$k <= 3, ]
  expect_same_rows(got, want)
})

test_that("mixed conjuncts push only the pass-through part; OR with a computed side does not push", {
  fx <- opt_fixture(); on.exit(unlink(fx$f))
  df <- fx$df
  z <- df$b * 10

  got <- tbl(fx$f) |> mutate(z = b * 10) |> filter(z > 5 & id <= 900) |> collect()
  keep <- !is.na(z) & z > 5 & df$id <= 900
  want <- transform(df, z = z)[keep, ]
  expect_same_rows(got, want)

  got <- tbl(fx$f) |> mutate(z = b * 10) |> filter(z > 9 | id <= 900) |> collect()
  keep <- (!is.na(z) & z > 9) | df$id <= 900
  want <- transform(df, z = z)[keep, ]
  expect_same_rows(got, want)

  got <- tbl(fx$f) |> mutate(z = b * 10) |> filter(!(id > 900)) |> collect()
  want <- transform(df, z = z)[df$id <= 900, ]
  expect_same_rows(got, want)
})

test_that("NA and all-NA row groups prune correctly below a projection", {
  fx <- opt_fixture(); on.exit(unlink(fx$f))
  df <- fx$df

  got <- tbl(fx$f) |> mutate(w = b + 1) |> filter(a >= 0) |>
    summarise(n = n()) |> collect()
  expect_equal(got$n, sum(!is.na(df$a)))

  got <- tbl(fx$f) |> mutate(w = b + 1) |> filter(is.na(a)) |>
    summarise(n = n()) |> collect()
  expect_equal(got$n, sum(is.na(df$a)))
})

test_that("string comparisons and %in% push through a projection", {
  fx <- opt_fixture(); on.exit(unlink(fx$f))
  df <- fx$df

  got <- tbl(fx$f) |> mutate(z = a + b) |> filter(sp == "cat") |> collect()
  want <- transform(df, z = a + b)[df$sp == "cat", ]
  expect_same_rows(got, want)

  got <- tbl(fx$f) |> mutate(z = a + b) |> filter(sp %in% c("ant", "dog")) |>
    collect()
  want <- transform(df, z = a + b)[df$sp %in% c("ant", "dog"), ]
  expect_same_rows(got, want)

  got <- tbl(fx$f) |> mutate(z = a + b) |> filter(k %in% c(5, 9, 400)) |>
    collect()
  want <- transform(df, z = a + b)[df$k %in% c(5, 9, 400), ]
  expect_same_rows(got, want)
})

test_that("indexed stores answer the same through a projection", {
  fx <- opt_fixture(); on.exit(unlink(c(fx$f, paste0(fx$f, "*"))))
  df <- fx$df
  create_index(fx$f, "k")
  create_index(fx$f, "sp")

  for (key in c(1, 250, 499, 500)) {
    got <- tbl(fx$f) |> mutate(z = a * 2) |> filter(k == key) |> collect()
    want <- transform(df, z = a * 2)[df$k == key, ]
    expect_same_rows(got, want)
  }
  got <- tbl(fx$f) |> select(sp, k, a) |> filter(k %in% c(3, 77, 480)) |> collect()
  want <- df[df$k %in% c(3, 77, 480), c("sp", "k", "a")]
  expect_same_rows(got, want)

  got <- tbl(fx$f) |> rename(kk = k) |> filter(kk == 42 & sp == "bee") |> collect()
  want <- df[df$k == 42 & df$sp == "bee", ]; names(want)[names(want) == "k"] <- "kk"
  expect_same_rows(got, want)
})

test_that("bind_rows() pushes the filter into every input", {
  fx <- opt_fixture(); on.exit(unlink(fx$f))
  df <- fx$df

  q <- bind_rows(tbl(fx$f), tbl(fx$f), tbl(fx$f)) |> filter(id <= 600)
  txt <- plan_text(q)
  expect_equal(lengths(regmatches(txt, gregexpr("predicate pushdown", txt))), 3)
  got <- bind_rows(tbl(fx$f), tbl(fx$f), tbl(fx$f)) |> filter(id <= 600) |>
    summarise(n = n(), s = sum(a, na.rm = TRUE)) |> collect()
  keep <- df$id <= 600
  expect_equal(got$n, 3 * sum(keep))
  expect_equal(got$s, 3 * sum(df$a[keep], na.rm = TRUE))

  many <- lapply(1:20, function(i) tbl(fx$f))
  got <- do.call(bind_rows, many) |> filter(id > 4900) |> summarise(n = n()) |>
    collect()
  expect_equal(got$n, 20 * 100)
})

test_that("bind_rows() of inputs with different column types stays correct", {
  f1 <- tempfile(fileext = ".vtr"); f2 <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f1, f2)))
  write_vtr(data.frame(x = 1:3000, y = 1), f1, batch_size = 500)
  write_vtr(data.frame(x = seq(0.5, 2999.5, by = 1), y = 2), f2, batch_size = 500)

  got <- bind_rows(tbl(f1), tbl(f2)) |> filter(x > 2999) |> collect()
  expect_equal(sort(got$x), c(2999.5, 3000))
  got <- bind_rows(tbl(f1), tbl(f2)) |> mutate(z = y) |> filter(x <= 1.5) |>
    collect()
  expect_equal(sort(got$x), c(0.5, 1, 1.5))
  expect_type(got$x, "double")
})

test_that("pushdown does not cross limits, windows or aggregates", {
  fx <- opt_fixture(); on.exit(unlink(fx$f))
  df <- fx$df

  got <- tbl(fx$f) |> slice_head(n = 50) |> filter(id > 40) |> collect()
  expect_equal(got$id, 41:50)

  got <- tbl(fx$f) |> mutate(r = row_number()) |> filter(id > 4990) |> collect()
  expect_equal(got$r, as.double(4991:5000))

  got <- tbl(fx$f) |> group_by(k) |> summarise(n = n()) |> filter(k > 495) |>
    collect()
  expect_equal(sort(got$k), 496:500)
  expect_true(all(got$n == 10))
})

test_that("arrange() then filter() prunes below the sort and keeps order", {
  fx <- opt_fixture(); on.exit(unlink(fx$f))
  df <- fx$df
  got <- tbl(fx$f) |> arrange(desc(b)) |> filter(id <= 300) |> collect()
  want <- df[df$id <= 300, ]
  want <- want[order(-want$b, na.last = TRUE), ]
  expect_equal(got$id, as.double(want$id))
})

test_that("mutate() no longer forces every column below it to be read", {
  fx <- opt_fixture(); on.exit(unlink(fx$f))
  df <- fx$df

  q <- tbl(fx$f) |> mutate(z = a * 2) |> group_by(k) |> summarise(s = sum(z))
  expect_match(plan_text(q), "2/5 cols \\(pruned\\)")
  got <- tbl(fx$f) |> mutate(z = a * 2) |> group_by(k) |>
    summarise(s = sum(z, na.rm = TRUE)) |> collect()
  want <- aggregate(z ~ k, transform(df, z = a * 2), sum, na.action = na.pass,
                    na.rm = TRUE)
  expect_equal(got$s[order(got$k)], want$z[order(want$k)])

  got <- tbl(fx$f) |> mutate(z = a * 2, w = b + 1) |> summarise(n = n()) |> collect()
  expect_equal(got$n, nrow(df))

  got <- tbl(fx$f) |> mutate(a2 = a * 2) |> mutate(a4 = a2 * 2, junk = sp) |>
    select(id, a4) |> collect()
  expect_equal(got$a4, df$a * 4)
  expect_equal(names(got), c("id", "a4"))

  got <- tbl(fx$f) |> transmute(id = id, s2 = paste0(sp, "!")) |> filter(id <= 5) |>
    select(s2) |> collect()
  expect_equal(got$s2, paste0(df$sp[1:5], "!"))

  got <- tbl(fx$f) |> mutate(z = a * 2) |> mutate(r = cumsum(id)) |>
    select(r) |> collect()
  expect_equal(got$r, cumsum(as.double(df$id)))
})

test_that("explain() before collect() does not change the result", {
  fx <- opt_fixture(); on.exit(unlink(fx$f))
  df <- fx$df
  q <- tbl(fx$f) |> mutate(z = a * 2) |> filter(id <= 700 & sp == "ant")
  invisible(capture.output(explain(q)))
  invisible(capture.output(explain(q)))
  got <- q |> collect()
  want <- transform(df, z = a * 2)[df$id <= 700 & df$sp == "ant", ]
  expect_same_rows(got, want)
})
