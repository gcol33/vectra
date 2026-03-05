# Core benchmarks for vectra regression testing
#
# Usage: Rscript inst/benchmarks/bench_core.R
#
# Measures runtime for key operations. Not comparative (no dplyr/arrow),
# just a baseline for tracking regressions as features grow.

library(vectra)

# --- Setup ---
set.seed(42)
n <- 1e6
n_groups <- 1000

cat(sprintf("Generating test data: %d rows, %d groups\n", n, n_groups))
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

f <- tempfile(fileext = ".vtr")
f2 <- tempfile(fileext = ".vtr")
on.exit(unlink(c(f, f2)))

t0 <- proc.time()
write_vtr(wide, f)
t_write <- (proc.time() - t0)[3]
cat(sprintf("write_vtr:           %.3fs  (%s)\n", t_write,
            format(file.size(f), big.mark = ",")))

# --- Benchmark 1: filter + select ---
t0 <- proc.time()
result <- tbl(f) |> filter(x > 0) |> select(id, x, y) |> collect()
t1 <- (proc.time() - t0)[3]
cat(sprintf("filter + select:     %.3fs  (%d rows)\n", t1, nrow(result)))

# --- Benchmark 2: group_by + summarise (high cardinality) ---
t0 <- proc.time()
result <- tbl(f) |> group_by(g) |>
  summarise(sx = sum(x), mx = mean(y), n = n()) |> collect()
t2 <- (proc.time() - t0)[3]
cat(sprintf("group_by + summarise: %.3fs  (%d groups)\n", t2, nrow(result)))

# --- Benchmark 3: arrange ---
t0 <- proc.time()
result <- tbl(f) |> arrange(x) |> slice_head(n = 10) |> collect()
t3 <- (proc.time() - t0)[3]
cat(sprintf("arrange + head:      %.3fs\n", t3))

# --- Benchmark 4: left_join ---
lookup <- data.frame(
  g = paste0("g", seq_len(n_groups)),
  label = paste0("label_", seq_len(n_groups)),
  stringsAsFactors = FALSE
)
write_vtr(lookup, f2)

t0 <- proc.time()
result <- tbl(f) |> left_join(tbl(f2), by = "g") |>
  select(id, g, x, label) |> collect()
t4 <- (proc.time() - t0)[3]
cat(sprintf("left_join:           %.3fs  (%d rows)\n", t4, nrow(result)))

# --- Benchmark 5: window functions (grouped) ---
t0 <- proc.time()
result <- tbl(f) |> group_by(g) |>
  mutate(rn = row_number(), cs = cumsum(x)) |>
  select(id, g, x, rn, cs) |> collect()
t5 <- (proc.time() - t0)[3]
cat(sprintf("window (grouped):    %.3fs  (%d rows)\n", t5, nrow(result)))

# --- Benchmark 6: streaming bind_rows ---
t0 <- proc.time()
result <- bind_rows(tbl(f), tbl(f)) |> filter(x > 1.5) |> collect()
t6 <- (proc.time() - t0)[3]
cat(sprintf("bind_rows + filter:  %.3fs  (%d rows)\n", t6, nrow(result)))

# --- Summary ---
cat("\n--- Summary ---\n")
cat(sprintf("  write:       %.3fs\n", t_write))
cat(sprintf("  filter+sel:  %.3fs\n", t1))
cat(sprintf("  group+agg:   %.3fs\n", t2))
cat(sprintf("  sort+head:   %.3fs\n", t3))
cat(sprintf("  left_join:   %.3fs\n", t4))
cat(sprintf("  window:      %.3fs\n", t5))
cat(sprintf("  bind+filter: %.3fs\n", t6))
