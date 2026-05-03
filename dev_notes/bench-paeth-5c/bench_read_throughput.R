# Phase 5c bench — measure VECR read throughput for representative rasters.
#
# Question: does PAETH decode dominate any realistic vectra raster read path?
# Upstream tdc benchmarks already show u16 PAETH at ~480 MB/s decode on
# isolated tiles. This bench measures the *full* vec_read_window path (mmap
# + tdc decode + dtype cast back to double) on rasters big enough that
# per-call overhead is amortised, across dtypes & sizes.
#
# If full-stack throughput is well below the upstream PAETH ceiling, the
# PAETH inverse is not the bottleneck and Phase 5c (SIMD PAETH) buys little.
# If it's at-or-near the ceiling and dominated by decode, 5c could matter.

suppressPackageStartupMessages({
  devtools::load_all(".", quiet = TRUE)
})

# ---- helpers ----

bytes_per <- c(u8 = 1, u16 = 2, i16 = 2, i32 = 4,
               f32 = 4, f64 = 8)

mk_raster <- function(W, H, nb, dtype, kind = c("smooth", "noisy")) {
  kind <- match.arg(kind)
  if (kind == "smooth") {
    # Smooth gradient — friendly to PAETH (low residual energy).
    x <- matrix(rep(seq_len(W) / W, each = H) +
                rep(seq_len(H) / H, times = W), nrow = H, ncol = W)
  } else {
    # White noise — PAETH residuals ~ raw signal; UP usually wins.
    x <- matrix(runif(W * H), nrow = H, ncol = W)
  }
  rng <- switch(dtype,
                u8 = c(0, 250),
                u16 = c(0, 30000),
                i16 = c(-15000, 15000),
                i32 = c(-1e6, 1e6),
                f32 = c(-1e3, 1e3),
                f64 = c(-1e3, 1e3))
  x <- rng[1] + (rng[2] - rng[1]) * (x - min(x)) / diff(range(x))
  if (nb == 1L) x else array(rep(x, nb), dim = c(H, W, nb))
}

bench_one <- function(W, H, nb, dtype, kind, tile_size, comp, n_reads = 5L) {
  arr <- mk_raster(W, H, nb, dtype, kind)
  path <- tempfile(fileext = ".vec")
  vec_write_raster(arr, path, dtype = dtype, tile_size = tile_size,
                   compression = comp)
  on.exit(unlink(path), add = TRUE)

  fsize <- file.info(path)$size
  raw_bytes <- as.numeric(W) * H * nb * bytes_per[[dtype]]

  r <- vec_open_raster(path)
  on.exit(vec_close_raster(r), add = TRUE)

  # Warmup
  invisible(vec_read_window(r, band = 1L))
  # Time
  t0 <- proc.time()[3]
  for (i in seq_len(n_reads)) invisible(vec_read_window(r, band = 1L))
  t1 <- proc.time()[3]

  read_s <- (t1 - t0) / n_reads
  data.frame(
    W = W, H = H, nb = nb, dtype = dtype, kind = kind,
    tile = tile_size, compression = comp,
    file_mb = round(fsize / 1e6, 2),
    raw_mb = round(raw_bytes / 1e6, 2),
    ratio = round(raw_bytes / fsize, 2),
    read_ms = round(read_s * 1000, 1),
    raw_throughput_mbs = round((raw_bytes / 1e6) / read_s, 0)
  )
}

# ---- bench grid ----

cat("Phase 5c bench — full-stack vec_read_window throughput\n")
cat("Goal: locate vectra raster reads relative to the ~480 MB/s u16-PAETH\n")
cat("upstream ceiling. If we're well under it, PAETH inverse is NOT the\n")
cat("bottleneck even on PAETH-heavy data.\n\n")

set.seed(42)
W <- 4096L; H <- 4096L
results <- list()

for (dtype in c("u16", "f32", "u8")) {
  for (kind in c("smooth", "noisy")) {
    for (comp in c("balanced", "max")) {
      r <- bench_one(W, H, nb = 1L, dtype = dtype, kind = kind,
                     tile_size = 256L, comp = comp, n_reads = 5L)
      results[[length(results) + 1L]] <- r
      cat(sprintf("  %s %s %s/%s  %4d ms  %4d MB/s (raw)  ratio %.1fx\n",
                  dtype, kind, "256", comp, r$read_ms,
                  r$raw_throughput_mbs, r$ratio))
    }
  }
}

df <- do.call(rbind, results)
out_path <- file.path("dev_notes", "bench-paeth-5c",
                      "bench_results.csv")
write.csv(df, out_path, row.names = FALSE)
cat(sprintf("\nWrote %s\n", out_path))

cat("\n--- Reference: upstream tdc bench (../tdc/bench/RESULTS.md, the-beast) ---\n")
cat("  u16 PAETH decode (isolated tile):  ~378-468 MB/s\n")
cat("  u16 UP   decode (isolated tile):   ~608 MB/s\n")
cat("  4-row SSE2 wavefront PAETH already in upstream tdc.\n")
cat("\nIf full-stack throughput here is well below the PAETH ceiling,\n")
cat("PAETH SIMD is not the limiting factor in vectra raster reads.\n")
