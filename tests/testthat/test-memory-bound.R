# vectra_mem() is a plan-level bound on what the engine allocates: a join feeding
# a grouped aggregate, and an arrange(), must keep the process peak near the
# budget. Peak RSS is monotone within a process, so each workload runs in a
# fresh R process that reports its own peak.

run_peak <- function(expr_text, budget, files) {
  rscript <- file.path(R.home("bin"),
                       if (.Platform$OS.type == "windows") "Rscript.exe" else "Rscript")
  # A source tree (devtools::test) is reloaded from its compiled src/; an
  # installed package (R CMD check) is attached from the same library.
  ns_path <- getNamespaceInfo("vectra", "path")
  loader <- if (dir.exists(file.path(ns_path, "src"))) {
    sprintf("suppressMessages(pkgload::load_all(%s, compile = FALSE, quiet = TRUE, helpers = FALSE))",
            deparse(ns_path))
  } else {
    sprintf(".libPaths(%s); suppressMessages(library(vectra))",
            paste(deparse(.libPaths()), collapse = ""))
  }
  script <- tempfile(fileext = ".R")
  on.exit(unlink(script))
  writeLines(c(
    loader,
    sprintf("options(vectra.memory = %s)", deparse(budget)),
    sprintf("f <- %s; d <- %s", deparse(files[1]), deparse(files[2])),
    "invisible(gc())",
    "base <- vectra:::.peak_rss()",
    sprintf("r <- %s", expr_text),
    "cat(base, vectra:::.peak_rss(), '\n')"
  ), script)
  out <- system2(rscript, c("--vanilla", shQuote(script)), stdout = TRUE, stderr = FALSE)
  as.numeric(strsplit(trimws(tail(out, 1)), " +")[[1]])
}

test_that("a join feeding a grouped aggregate stays within vectra_mem()", {
  skip_on_cran()
  skip_if_not(vectra:::.peak_rss() > 0, "peak RSS is not reported on this platform")
  skip_if(nzchar(Sys.getenv("ASAN_OPTIONS")),
          "peak RSS under AddressSanitizer includes its shadow memory and quarantine")

  set.seed(1)
  n <- 4e6
  f <- tempfile(fileext = ".vtr"); d <- tempfile(fileext = ".vtr")
  on.exit(unlink(c(f, d)))
  write_vtr(data.frame(id = seq_len(n), year = sample(1950:2024, n, TRUE),
                       v = runif(n)), f)
  write_vtr(data.frame(id = seq_len(n), w = runif(n)), d)

  budget <- 64 * 1024^2
  # R, the batches in flight and the (tiny) result: fixed, independent of n.
  allowance <- 48 * 1024^2

  join <- run_peak(paste(
    "tbl(f) |> inner_join(tbl(d), by = 'id') |> group_by(year) |>",
    "summarise(n = n(), sw = sum(w)) |> collect()"), budget, c(f, d))
  expect_lte(join[2] - join[1], budget + allowance)

  srt <- run_peak(
    "tbl(f) |> arrange(v) |> summarise(n = n()) |> collect()", budget, c(f, d))
  expect_lte(srt[2] - srt[1], budget + allowance)
})
