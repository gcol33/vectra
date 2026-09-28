# Stage 1: Parquet -> one .vtr store. vectra has no Parquet reader, so each
# local Parquet file is read with arrow and appended as new row groups;
# append_vtr() never rereads existing row groups, so the cost is linear.
suppressPackageStartupMessages({library(vectra); library(arrow)})
set_cpu_count(2)
files <- sort(list.files("raw", "\\.parquet$", full.names = TRUE))
store <- "records.vtr"; unlink(store)
n <- 0
for (i in seq_along(files)) {
  d <- as.data.frame(read_parquet(files[i]))
  d$year <- as.integer(d$year)
  if (i == 1) write_vtr(d, store) else append_vtr(d, store)
  n <- n + nrow(d); rm(d); invisible(gc(FALSE))
}
cat("RESULT rows=", n, " rowgroups_file_bytes=", file.size(store), "\n", sep = "")
