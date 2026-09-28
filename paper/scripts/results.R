# Loads the measured results used in the manuscript and defines helpers.
# Benchmark runs: data/bench_runs.csv (parsed from the harness's runs.jsonl).
# Case study:     data/case_stages.csv, data/case_fits.csv, data/case_facts.rds

.dir <- normalizePath(if (dir.exists("data")) "data" else file.path("..", "data"))
runs <- read.csv(file.path(.dir, "bench_runs.csv"), stringsAsFactors = FALSE)
runs$param_num <- suppressWarnings(as.numeric(runs$param))

tool_lab <- c(vectra = "vectra", vectra1g = "vectra (1 GB budget)", arrow = "arrow",
              duckdb = "DuckDB", datatable = "data.table", sf = "sf")
tool_col <- c(vectra = "#2a78d6", vectra1g = "#eb6834", arrow = "#1baf7a",
              duckdb = "#eda100", datatable = "#e87ba4", sf = "#1baf7a")
tool_shape <- c(vectra = 16, vectra1g = 17, arrow = 15, duckdb = 18, datatable = 4, sf = 15)

# Summary over repetitions: median of successful runs, range, success count.
summ <- do.call(rbind, lapply(split(runs, list(runs$tool, runs$workload, runs$param), drop = TRUE),
  function(d) {
    ok <- d[d$ok, ]
    data.frame(tool = d$tool[1], workload = d$workload[1], param = d$param[1],
               param_num = d$param_num[1], n_runs = nrow(d), n_ok = nrow(ok),
               wall = if (nrow(ok)) median(ok$wall_s) else NA,
               wall_min = if (nrow(ok)) min(ok$wall_s) else NA,
               wall_max = if (nrow(ok)) max(ok$wall_s) else NA,
               inner = if (nrow(ok)) median(ok$inner_s) else NA,
               rss = if (nrow(ok)) median(ok$rss_mb) else median(d$rss_mb),
               tmp = if (nrow(ok)) median(ok$tmp_mb) else NA,
               out = if (nrow(ok)) median(ok$out_mb) else NA,
               rchar = if (nrow(ok)) median(ok$rchar_mb) else NA,
               t_prep = if (nrow(ok)) median(ok$t_prep) else NA,
               iter = if (nrow(ok)) median(ok$iter) else NA,
               check_agree = length(unique(ok$check)) <= 1,
               check = if (nrow(ok)) ok$check[1] else NA,
               stringsAsFactors = FALSE)
  }))
rownames(summ) <- NULL

B <- function(tool, workload, param, stat = "wall") {
  r <- summ[summ$tool == tool & summ$workload == workload & summ$param == param, stat]
  if (!length(r)) NA else r
}
fmt <- function(x, digits = 1) {
  if (length(x) != 1 || is.na(x)) return("n/a")
  formatC(x, format = "f", digits = digits, big.mark = ",")
}
gb <- function(mb, digits = 1) fmt(mb / 1024, digits)
ratio <- function(a, b, digits = 1) fmt(a / b, digits)

# Equivalence: for every configuration, all tools that completed returned the
# same checksum.
equiv <- do.call(rbind, lapply(split(summ, list(summ$workload, summ$param), drop = TRUE),
  function(d) data.frame(workload = d$workload[1], param = d$param[1],
                         n_tools = sum(d$n_ok > 0),
                         agree = length(unique(signif(d$check[d$n_ok > 0], 10))) <= 1)))

meta_file <- file.path(.dir, "versions.rds")
if (file.exists(meta_file)) {
  v <- readRDS(meta_file)
  for (nm in names(v)) assign(nm, v[[nm]])
}

cs_file <- file.path(.dir, "case_facts.rds")
CS <- if (file.exists(cs_file)) readRDS(cs_file) else list(n_records = NA, shard_max_rows = NA)

# Tables: in PDF, wrap text columns at given widths and shrink wide tables to
# the text width; in HTML, a plain table.
ktab <- function(df, caption, widths = NULL, font_size = 8) {
  if (knitr::is_latex_output()) {
    k <- kableExtra::kbl(df, format = "latex", booktabs = TRUE, caption = caption,
                         row.names = FALSE, linesep = "")
    k <- kableExtra::kable_styling(k, font_size = font_size,
                                   latex_options = c("hold_position", if (is.null(widths)) "scale_down"))
    if (!is.null(widths))
      for (i in seq_along(widths)) if (!is.na(widths[i]))
        k <- kableExtra::column_spec(k, i, width = widths[i])
    k
  } else {
    knitr::kable(df, format = "html", caption = caption, row.names = FALSE)
  }
}
