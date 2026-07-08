# Grouped slice_min/slice_max (n = 1) is bounded: it routes through the external
# sort (keyed by group + row-id) and keeps only the running champion. A tiny
# vectra.memory plus a small row-group size forces that sort to spill, so these
# check the winners stay exact -- including input-order tie-breaking -- when the
# sort spills and merges rather than sorting in RAM.

topn_ref <- function(df, gcol, ocol, desc = FALSE) {
  ord <- df[[ocol]]; rn <- seq_len(nrow(df))
  win <- vapply(split(rn, df[[gcol]]), function(idx) {
    v <- ord[idx]; ok <- !is.na(v)
    if (!any(ok)) return(idx[1L])
    iv <- idx[ok]; vv <- v[ok]
    iv[if (desc) which.max(vv) else which.min(vv)]   # first appearance on ties
  }, integer(1))
  df[sort(win), , drop = FALSE]
}

test_that("grouped slice winners are exact when the sort spills", {
  set.seed(201)
  n <- 60000L
  d <- data.frame(
    g   = sample.int(4000L, n, replace = TRUE),
    ord = sample.int(999L, n, replace = TRUE),
    dbl = rnorm(n),
    tag = sprintf("wkb%07d%s", seq_len(n), strrep("q", 25)),
    stringsAsFactors = FALSE)
  d$ord[sample(n, n %/% 10)] <- NA_integer_
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(d, f, batch_size = 500L)                 # 120 row groups

  old <- options(vectra.memory = 65536); on.exit(options(old), add = TRUE)
  for (desc in c(FALSE, TRUE)) {
    fn  <- if (desc) slice_max else slice_min
    got <- tbl(f) |> group_by(g) |> fn(ord, n = 1, with_ties = FALSE) |> collect()
    got <- got[order(got$g), ]
    ref <- topn_ref(d, "g", "ord", desc = desc)
    ref <- ref[order(ref$g), ]
    expect_equal(nrow(got), nrow(ref))
    expect_equal(as.integer(got$ord), as.integer(ref$ord))
    expect_equal(got$dbl, ref$dbl)                   # winning row travels intact
    expect_equal(got$tag, ref$tag)                   # wide string column intact
  }
})

test_that("spilled grouped slice matches the in-RAM result exactly", {
  set.seed(202)
  n <- 30000L
  d <- data.frame(g = sample.int(2500L, n, replace = TRUE),
                  ord = rnorm(n), tag = sprintf("t%06d", seq_len(n)),
                  stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(d, f, batch_size = 400L)

  old <- options(vectra.memory = "4GB")
  ref <- tbl(f) |> group_by(g) |> slice_min(ord, n = 1, with_ties = FALSE) |>
    collect()
  ref <- ref[order(ref$g), ]
  options(old)

  old <- options(vectra.memory = 65536); on.exit(options(old), add = TRUE)
  got <- tbl(f) |> group_by(g) |> slice_min(ord, n = 1, with_ties = FALSE) |>
    collect()
  got <- got[order(got$g), ]
  expect_equal(got, ref)
})

test_that("ties break to the first input row even when the sort spills", {
  # Every row of a group shares one ord value, so all rows tie; the winner must
  # be the earliest input row of the group regardless of spilling.
  set.seed(203)
  n <- 50000L
  g <- sample.int(3000L, n, replace = TRUE)
  d <- data.frame(g = g, ord = 7L,                    # constant -> universal tie
                  seqno = seq_len(n),
                  tag = sprintf("row%07d", seq_len(n)),
                  stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(d, f, batch_size = 500L)

  old <- options(vectra.memory = 65536); on.exit(options(old), add = TRUE)
  got <- tbl(f) |> group_by(g) |> slice_min(ord, n = 1, with_ties = FALSE) |>
    collect()
  got <- got[order(got$g), ]

  first_seqno <- tapply(d$seqno, d$g, min)             # earliest input row / group
  expect_equal(nrow(got), length(first_seqno))
  expect_equal(as.integer(got$seqno),
               as.integer(first_seqno[as.character(got$g)]))
})

test_that("a few huge groups keep memory to one champion under a tiny budget", {
  # 3 groups over many rows: the champion store never exceeds the group count,
  # while the sort spills; the winner is still the true per-group min.
  set.seed(204)
  n <- 90000L
  d <- data.frame(g = sample(1:3, n, replace = TRUE),
                  ord = sample.int(1e6L, n, replace = TRUE),
                  tag = sprintf("v%07d", seq_len(n)), stringsAsFactors = FALSE)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(d, f, batch_size = 1000L)

  old <- options(vectra.memory = 65536); on.exit(options(old), add = TRUE)
  got <- tbl(f) |> group_by(g) |> slice_min(ord, n = 1, with_ties = FALSE) |>
    collect()
  got <- got[order(got$g), ]
  ref <- topn_ref(d, "g", "ord")
  ref <- ref[order(ref$g), ]
  expect_equal(as.integer(got$ord), as.integer(ref$ord))
  expect_equal(got$tag, ref$tag)
})
