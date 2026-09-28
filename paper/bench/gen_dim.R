suppressPackageStartupMessages({library(vectra); library(arrow); library(fst)})
dir.create("data/dim", FALSE)
for (p in c("1e3", "1e5", "1e6", "1e7", "5e7")) {
  M <- as.numeric(p); set.seed(7)
  key <- c("1e3" = "k3", "1e5" = "k5", "1e6" = "k6", "1e7" = "k7", "5e7" = "id")[[p]]
  d <- data.frame(k = seq_len(M), w = runif(M)); names(d)[1] <- key
  f <- file.path("data/dim", paste0("dim_", p))
  write_vtr(d, paste0(f, ".vtr")); write_parquet(d, paste0(f, ".parquet")); write_fst(d, paste0(f, ".fst"))
}
