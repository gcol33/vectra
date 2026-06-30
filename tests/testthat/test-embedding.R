# Recovery tests for embedding distance ops. References are computed in R on
# float32-rounded values (the precision vectra stores) so they match the C
# kernel to floating-point tolerance.

# Round a numeric vector through float32, the storage precision of embeddings.
f32 <- function(v) {
  readBin(writeBin(as.double(v), raw(), size = 4L, endian = "little"),
          "double", n = length(v), size = 4L, endian = "little")
}

ref_cosine <- function(a, b) {
  a <- f32(a); b <- f32(b)
  1 - sum(a * b) / (sqrt(sum(a * a)) * sqrt(sum(b * b)))
}
ref_l2  <- function(a, b) { a <- f32(a); b <- f32(b); sqrt(sum((a - b)^2)) }
ref_dot <- function(a, b) { a <- f32(a); b <- f32(b); sum(a * b) }

make_emb_tbl <- function(m) {
  f <- tempfile(fileext = ".vtr")
  df <- data.frame(id = seq_len(nrow(m)), emb = as_embedding(m),
                   stringsAsFactors = FALSE)
  write_vtr(df, f)
  list(path = f, df = df, m = m)
}

set.seed(7)

test_that("cosine, l2, dot against a constant query match R references", {
  dim <- 16; n <- 30
  m <- matrix(rnorm(n * dim), nrow = n)
  q <- rnorm(dim)
  t <- make_emb_tbl(m); on.exit(unlink(t$path))

  res <- tbl(t$path) |>
    mutate(dc = cosine(emb, q), de = l2(emb, q), dd = dot(emb, q)) |>
    collect()
  res <- res[order(res$id), ]

  exp_c <- vapply(seq_len(n), function(i) ref_cosine(m[i, ], q), numeric(1))
  exp_e <- vapply(seq_len(n), function(i) ref_l2(m[i, ], q),  numeric(1))
  exp_d <- vapply(seq_len(n), function(i) ref_dot(m[i, ], q), numeric(1))

  expect_equal(res$dc, exp_c, tolerance = 1e-5)
  expect_equal(res$de, exp_e, tolerance = 1e-5)
  expect_equal(res$dd, exp_d, tolerance = 1e-5)
})

test_that("column-to-column distance matches R references", {
  dim <- 8; n <- 20
  a <- matrix(rnorm(n * dim), nrow = n)
  b <- matrix(rnorm(n * dim), nrow = n)
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(id = seq_len(n),
                       ea = as_embedding(a), eb = as_embedding(b),
                       stringsAsFactors = FALSE), f)

  res <- tbl(f) |> mutate(d = l2(ea, eb)) |> collect()
  res <- res[order(res$id), ]
  exp_d <- vapply(seq_len(n), function(i) ref_l2(a[i, ], b[i, ]), numeric(1))
  expect_equal(res$d, exp_d, tolerance = 1e-5)
})

test_that("slice_min on cosine distance retrieves the true nearest neighbours", {
  dim <- 32; n <- 200; k <- 5
  m <- matrix(rnorm(n * dim), nrow = n)
  q <- m[42, ] + rnorm(dim, sd = 0.01)   # query near row 42
  t <- make_emb_tbl(m); on.exit(unlink(t$path))

  got <- tbl(t$path) |>
    mutate(d = cosine(emb, q)) |>
    slice_min(d, n = k) |>
    collect()

  d_all <- vapply(seq_len(n), function(i) ref_cosine(m[i, ], q), numeric(1))
  expect_setequal(got$id, order(d_all)[seq_len(k)])
  expect_equal(got$d[order(got$d)], sort(d_all)[seq_len(k)], tolerance = 1e-5)
})

test_that("dimension mismatch and NA cells yield NA distance", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  emb <- as_embedding(matrix(rnorm(3 * 4), nrow = 3))
  emb[2] <- NA                       # missing cell
  emb[3] <- as_embedding(rnorm(5))   # wrong dimension
  write_vtr(data.frame(id = 1:3, emb = emb, stringsAsFactors = FALSE), f)

  res <- tbl(f) |> mutate(d = l2(emb, rnorm(4))) |> collect()
  res <- res[order(res$id), ]
  expect_false(is.na(res$d[1]))
  expect_true(is.na(res$d[2]))
  expect_true(is.na(res$d[3]))
})

test_that("zero vector gives NA cosine but finite l2 and dot", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  m <- rbind(rep(0, 4), rnorm(4))
  write_vtr(data.frame(id = 1:2, emb = as_embedding(m),
                       stringsAsFactors = FALSE), f)
  q <- rnorm(4)
  res <- tbl(f) |>
    mutate(dc = cosine(emb, q), de = l2(emb, q)) |> collect()
  res <- res[order(res$id), ]
  expect_true(is.na(res$dc[1]))      # cosine undefined for zero vector
  expect_false(is.na(res$de[1]))     # l2 still defined
})
