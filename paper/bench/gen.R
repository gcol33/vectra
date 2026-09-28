`%||%` <- function(a, b) if (is.null(a)) b else a
# Synthetic occurrence-like table, written in 10 chunks of 1e7 rows to three
# prepared formats: .vtr (vectra), Parquet (arrow, duckdb), fst (data.table).
suppressPackageStartupMessages({library(vectra); library(arrow); library(fst); library(data.table)})
set_cpu_count(2); setDTthreads(2)
n_chunk <- 1e7; n_chunks <- as.integer(commandArgs(TRUE)[1] %||% 10)
dir.create("data/vtr", FALSE); dir.create("data/pq", FALSE); dir.create("data/fst", FALSE)
sp_levels <- sprintf("Species_%05d", 1:1e4)
for (i in seq_len(n_chunks)) {
  set.seed(1000 + i)
  n <- n_chunk
  df <- data.frame(
    id   = as.integer((i - 1) * n + seq_len(n)),
    year = sample(1950:2024, n, TRUE),
    k3   = sample.int(1e3, n, TRUE),
    k5   = sample.int(1e5, n, TRUE),
    k6   = sample.int(1e6, n, TRUE),
    k7   = sample.int(1e7, n, TRUE),
    sp   = sp_levels[sample.int(1e4, n, TRUE)],
    v1   = rnorm(n),
    v2   = rexp(n),
    x    = runif(n, -10, 30),
    y    = runif(n, 36, 70),
    stringsAsFactors = FALSE)
  f <- sprintf("%02d", i)
  write_vtr(df, file.path("data/vtr", paste0("chunk", f, ".vtr")))
  write_parquet(df, file.path("data/pq", paste0("chunk", f, ".parquet")))
  write_fst(df, file.path("data/fst", paste0("chunk", f, ".fst")), compress = 50)
  message("chunk ", i); rm(df); gc()
}
