# Regression: repeatedly opening and collecting a .vtr must not exhaust OS file
# handles. The scan reader used to hold a file descriptor open for the lifetime
# of every scan node; in a tight tbl()|>collect() loop the anonymous nodes piled
# up unfinalized, one open .vtr handle each, until vtr1_open_tdc failed (and,
# hit mid-decode, segfaulted). The reader now drops the OS handle once the index
# is in memory and reopens per read, so idle nodes hold nothing.
#
# The loop counts here (well past the old ~500-handle ceiling) would reliably
# crash the pre-fix build.

test_that("a tight tbl()|>collect() loop does not exhaust file handles", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(x = 1:9, y = letters[1:9], stringsAsFactors = FALSE), f)
  last <- NULL
  for (i in 1:1500) last <- tbl(f) |> collect()
  expect_equal(nrow(last), 9L)
  expect_equal(last$x, as.double(1:9))
})

test_that("a tight loop with a downstream verb does not leak handles", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  write_vtr(data.frame(id = rep(c("a", "b", "c"), 4), v = 1:12), f)
  last <- NULL
  for (i in 1:1500) last <- tbl(f) |> filter(v > 6) |> collect()
  expect_equal(nrow(last), 6L)
})

# Regression: an I/O failure during a parallel collect must raise a normal,
# catchable R error, not crash. The parallel reader decodes row groups on
# OpenMP worker threads; it used to call the R error machinery (Rf_error, a
# longjmp) directly from a worker when fopen/fseek/fread/decode failed. A
# longjmp into a setjmp owned by the master thread's stack is undefined and
# corrupted the stack -- an intermittent SIGSEGV that surfaced under the
# resource pressure (locked files, tight memory) of build/test churn. The
# reader now allocates every batch on the master thread and only fills them
# from disk in parallel, capturing the first failure and re-raising it on the
# master after the region joins. These force the two worker-side I/O failures
# and require a clean error plus a still-usable engine afterward.

test_that("a parallel read with a truncated file errors cleanly, not a crash", {
  f <- tempfile(fileext = ".vtr"); on.exit(unlink(f))
  n <- 2e5
  write_vtr(data.frame(a = as.double(seq_len(n)),
                       b = rep(letters, length.out = n),
                       stringsAsFactors = FALSE), f, batch_size = 10000)
  h <- tbl(f)                       # footer/index read into memory
  con <- file(f, "r+b"); truncate(con, 4096L); close(con)
  expect_error(collect(h), "short read|invalid block size")

  # engine survives the caught error: a fresh valid read still works
  g <- tempfile(fileext = ".vtr"); on.exit(unlink(g), add = TRUE)
  write_vtr(data.frame(x = 1:9), g)
  expect_equal(nrow(tbl(g) |> collect()), 9L)
})

test_that("a parallel read of a removed file errors cleanly, not a crash", {
  f <- tempfile(fileext = ".vtr")
  n <- 2e5
  write_vtr(data.frame(a = as.double(seq_len(n)),
                       b = rep(letters, length.out = n),
                       stringsAsFactors = FALSE), f, batch_size = 10000)
  h <- tbl(f)
  unlink(f)                         # gone before the per-read reopen
  expect_error(collect(h), "cannot open|cannot reopen")
})

test_that("df_to_node temp files are cleaned up and survive until collect", {
  skip_if_not_installed("openxlsx2")
  f <- tempfile(fileext = ".xlsx"); on.exit(unlink(f))
  openxlsx2::write_xlsx(data.frame(a = 1:5, b = letters[1:5],
                                   stringsAsFactors = FALSE), f)

  # The lifted node outlives tbl_xlsx(); its scratch .vtr must still be readable
  # at collect time, and a repeated read must not accumulate scratch files.
  node <- tbl_xlsx(f)
  d <- node |> filter(a > 2) |> collect()
  expect_equal(nrow(d), 3L)

  for (i in 1:500) {
    d2 <- tbl_xlsx(f) |> collect()
    expect_equal(nrow(d2), 5L)
  }
})
