# Core benchmarks for vectra regression testing
#
# Usage:
#   Rscript inst/benchmarks/bench_core.R              # run and print
#   Rscript inst/benchmarks/bench_core.R --save       # run, print, save baseline
#   Rscript inst/benchmarks/bench_core.R --check      # run and compare to baseline
#
# Measures runtime for key operations. Not comparative (no dplyr/arrow),
# just a baseline for tracking regressions as features grow.
#
# Exit code 1 if any benchmark exceeds its slowdown threshold (--check mode).

library(vectra)

# --- Configuration ---
SLOWDOWN_THRESHOLD <- 1.5  # flag if > 50% slower than baseline
# Locate baseline file: installed package or dev source tree
BASELINE_FILE <- tryCatch({
  d <- system.file("benchmarks", package = "vectra", mustWork = TRUE)
  file.path(d, "baseline.rds")
}, error = function(e) {
  # Dev mode: look relative to script or working directory
  candidates <- c(
    if (!is.null(sys.frame(1)$ofile)) file.path(dirname(sys.frame(1)$ofile), "baseline.rds"),
    file.path("inst", "benchmarks", "baseline.rds")
  )
  for (p in candidates) if (file.exists(p)) return(p)
  candidates[length(candidates)]  # default even if doesn't exist yet
})

args <- commandArgs(trailingOnly = TRUE)
save_baseline <- "--save" %in% args
check_baseline <- "--check" %in% args

# --- Timing helper ---
bench <- function(name, expr) {
  gc(FALSE)
  t0 <- proc.time()
  result <- eval(expr)
  elapsed <- (proc.time() - t0)[3]
  nr <- if (is.data.frame(result)) nrow(result) else NA
  list(name = name, elapsed = elapsed, nrow = nr)
}

# --- Setup ---
set.seed(42)
n <- 1e6
n_groups <- 1000
n_groups_high <- 50000

cat(sprintf("vectra benchmark suite | %d rows, %d groups\n", n, n_groups))
cat(sprintf("R %s | %s | %s\n", R.version.string,
            .Platform$OS.type, Sys.info()["machine"]))
cat("---\n")

wide <- data.frame(
  id = seq_len(n) + 0.0,
  g = sample(paste0("g", seq_len(n_groups)), n, replace = TRUE),
  x = rnorm(n),
  y = rnorm(n),
  z = rnorm(n),
  a = rnorm(n),
  b = rnorm(n),
  c = rnorm(n),
  d = rnorm(n),
  e = rnorm(n),
  stringsAsFactors = FALSE
)

# Long string keys for join stress test
long_keys <- data.frame(
  key = paste0("very_long_key_name_for_hashing_stress_", seq_len(n)),
  val = rnorm(n),
  stringsAsFactors = FALSE
)

# High-cardinality group data
hc <- data.frame(
  g = sample(paste0("hc", seq_len(n_groups_high)), n, replace = TRUE),
  x = rnorm(n),
  stringsAsFactors = FALSE
)

# Many-to-many join data
m2m_left <- data.frame(
  key = sample(seq_len(100), 10000, replace = TRUE) + 0.0,
  lx = rnorm(10000),
  stringsAsFactors = FALSE
)
m2m_right <- data.frame(
  key = sample(seq_len(100), 10000, replace = TRUE) + 0.0,
  ry = rnorm(10000),
  stringsAsFactors = FALSE
)

f <- tempfile(fileext = ".vtr")
f2 <- tempfile(fileext = ".vtr")
f3 <- tempfile(fileext = ".vtr")
f4 <- tempfile(fileext = ".vtr")
f5 <- tempfile(fileext = ".vtr")
f6 <- tempfile(fileext = ".vtr")
f7 <- tempfile(fileext = ".vtr")
on.exit(unlink(c(f, f2, f3, f4, f5, f6, f7)))

results <- list()

# --- B00: write_vtr ---
r <- bench("write_vtr", quote({
  write_vtr(wide, f)
  data.frame(size = file.size(f))
}))
cat(sprintf("%-30s %.3fs\n", r$name, r$elapsed))
results <- c(results, list(r))

# --- B01: filter + select (selective) ---
r <- bench("filter_select", quote(
  tbl(f) |> filter(x > 1.5) |> select(id, x, y) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B02: filter + select (broad) ---
r <- bench("filter_select_broad", quote(
  tbl(f) |> filter(x > 0) |> select(id, x, y) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B03: wide select (3 of 10 columns) ---
r <- bench("wide_select", quote(
  tbl(f) |> select(id, x, e) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B04: group_by + summarise (1K groups) ---
r <- bench("group_summarise_1k", quote(
  tbl(f) |> group_by(g) |>
    summarise(sx = sum(x), mx = mean(y), n = n()) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d groups)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B05: group_by + summarise (50K groups) ---
write_vtr(hc, f3)
r <- bench("group_summarise_50k", quote(
  tbl(f3) |> group_by(g) |>
    summarise(sx = sum(x), n = n()) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d groups)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B06: filter -> group_by -> summarise ---
r <- bench("filter_group_summarise", quote(
  tbl(f) |> filter(x > 0) |> group_by(g) |>
    summarise(sx = sum(x), n = n()) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d groups)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B07: arrange (full sort) ---
r <- bench("arrange_full", quote(
  tbl(f) |> arrange(x) |> slice_head(n = 10) |> collect()
))
cat(sprintf("%-30s %.3fs\n", r$name, r$elapsed))
results <- c(results, list(r))

# --- B08: slice_min (top-N, no full sort) ---
r <- bench("slice_min_100", quote(
  tbl(f) |> slice_min(order_by = x, n = 100) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B09: left_join (1M x 1K lookup) ---
lookup <- data.frame(
  g = paste0("g", seq_len(n_groups)),
  label = paste0("label_", seq_len(n_groups)),
  stringsAsFactors = FALSE
)
write_vtr(lookup, f2)
r <- bench("left_join_lookup", quote(
  tbl(f) |> left_join(tbl(f2), by = "g") |>
    select(id, g, x, label) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B10: filter -> left_join ---
r <- bench("filter_join", quote(
  tbl(f) |> filter(x > 1) |> left_join(tbl(f2), by = "g") |>
    select(id, g, x, label) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B11: many-to-many inner_join ---
write_vtr(m2m_left, f4)
write_vtr(m2m_right, f5)
r <- bench("inner_join_m2m", quote(
  inner_join(tbl(f4), tbl(f5), by = "key") |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B12: long string key join ---
write_vtr(long_keys, f6)
lk_right <- data.frame(
  key = sample(long_keys$key, 10000),
  extra = rnorm(10000),
  stringsAsFactors = FALSE
)
write_vtr(lk_right, f7)
r <- bench("join_long_string_key", quote(
  inner_join(tbl(f6), tbl(f7), by = "key") |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B13: window functions (grouped) ---
r <- bench("window_grouped", quote(
  tbl(f) |> group_by(g) |>
    mutate(rn = row_number(), cs = cumsum(x)) |>
    select(id, g, x, rn, cs) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B14: window mutate (ungrouped lag/lead) ---
r <- bench("window_lag_lead", quote(
  tbl(f) |> mutate(x_lag = lag(x, 1), x_lead = lead(x, 1)) |>
    select(id, x, x_lag, x_lead) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B15: streaming bind_rows + filter ---
r <- bench("bind_rows_filter", quote(
  bind_rows(tbl(f), tbl(f)) |> filter(x > 1.5) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B16: mutate (arithmetic) ---
r <- bench("mutate_arithmetic", quote(
  tbl(f) |> mutate(w = (x + y) * z - a / (b + 1)) |>
    select(id, w) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B17: nested aggregation expression ---
r <- bench("nested_agg_expr", quote(
  tbl(f) |> group_by(g) |>
    summarise(s = sum(x + y), m = mean(x * y)) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d groups)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B18: string operations ---
str_df <- data.frame(
  s = paste0("item_", sample(letters, n, replace = TRUE),
             sample(1000:9999, n, replace = TRUE)),
  x = rnorm(n),
  stringsAsFactors = FALSE
)
f_str <- tempfile(fileext = ".vtr")
write_vtr(str_df, f_str)

r <- bench("string_nchar_substr", quote(
  tbl(f_str) |>
    mutate(len = nchar(s), prefix = substr(s, 1, 6)) |>
    select(len, prefix) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B19: grepl filter ---
r <- bench("string_grepl_filter", quote(
  tbl(f_str) |> filter(grepl("item_a", s)) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

# --- B20: rank + dense_rank ---
r <- bench("rank_dense_rank", quote(
  tbl(f) |> mutate(r = rank(x), dr = dense_rank(x)) |>
    select(id, r, dr) |> collect()
))
cat(sprintf("%-30s %.3fs  (%d rows)\n", r$name, r$elapsed, r$nrow))
results <- c(results, list(r))

unlink(f_str)

# --- Build results table ---
cat("\n=== Summary ===\n")
res_df <- data.frame(
  benchmark = vapply(results, `[[`, character(1), "name"),
  seconds = vapply(results, `[[`, double(1), "elapsed"),
  stringsAsFactors = FALSE
)

# --- Save baseline ---
if (save_baseline) {
  saveRDS(res_df, BASELINE_FILE)
  cat(sprintf("Baseline saved to %s\n", BASELINE_FILE))
}

# --- Check against baseline ---
if (check_baseline) {
  if (!file.exists(BASELINE_FILE)) {
    cat("No baseline found. Run with --save first.\n")
    quit(status = 1)
  }
  baseline <- readRDS(BASELINE_FILE)
  cat("\n%-30s %8s %8s %8s  %s\n" |> sprintf("benchmark", "base", "now", "ratio", "status"))
  cat(strrep("-", 72), "\n")
  any_fail <- FALSE
  for (i in seq_len(nrow(res_df))) {
    bm <- res_df$benchmark[i]
    bi <- match(bm, baseline$benchmark)
    if (is.na(bi)) {
      cat(sprintf("%-30s %8s %8.3f %8s  NEW\n", bm, "---", res_df$seconds[i], "---"))
      next
    }
    base_t <- baseline$seconds[bi]
    now_t <- res_df$seconds[i]
    ratio <- now_t / base_t
    status <- if (ratio > SLOWDOWN_THRESHOLD) {
      any_fail <- TRUE
      "SLOW"
    } else if (ratio < 1 / SLOWDOWN_THRESHOLD) {
      "FAST"
    } else {
      "ok"
    }
    cat(sprintf("%-30s %8.3f %8.3f %7.2fx  %s\n", bm, base_t, now_t, ratio, status))
  }
  if (any_fail) {
    cat("\nFAILED: one or more benchmarks exceeded the slowdown threshold.\n")
    quit(status = 1)
  } else {
    cat("\nPASSED: all benchmarks within threshold.\n")
  }
}
