# Parse the benchmark harness output (results/runs.jsonl) into one row per run.
parse_runs <- function(path) {
  L <- readLines(path)
  rows <- lapply(L, function(l) {
    d <- jsonlite::fromJSON(l, simplifyVector = FALSE)
    tm <- strsplit(sub("^TIME ", "", d$time), " ")[[1]]
    r <- d$res
    ex <- if (!is.null(r$extra)) r$extra else list()
    data.frame(
      id = d$id, tool = if (!is.null(r)) r$tool else sub("_.*", "", d$id),
      workload = if (!is.null(r)) r$workload else NA_character_,
      param = if (!is.null(r)) r$param else NA_character_,
      rep = d$rep,
      exit = as.integer(sub("EXIT ", "", d$exit)),
      wall_s = suppressWarnings(as.numeric(tm[1])),
      rss_mb = suppressWarnings(as.numeric(tm[2])) / 1024,
      tmp_mb = d$maxtmp / 1e6, out_mb = d$outsz / 1e6,
      inner_s = if (!is.null(r)) r$inner_s else NA_real_,
      check = if (!is.null(r) && !is.null(r$check)) as.numeric(r$check) else NA_real_,
      rchar_mb = if (!is.null(r)) r$rchar / 1e6 else NA_real_,
      read_mb = if (!is.null(r)) r$read_bytes / 1e6 else NA_real_,
      t_prep = if (!is.null(ex$t_prep)) ex$t_prep else NA_real_,
      iter = if (!is.null(ex$iter)) ex$iter else NA_real_,
      stringsAsFactors = FALSE)
  })
  x <- do.call(rbind, rows)
  # recover tool/workload/param for failed runs from the id
  miss <- is.na(x$workload)
  if (any(miss)) {
    parts <- strsplit(x$id[miss], "_")
    x$tool[miss] <- vapply(parts, `[`, "", 1)
    x$param[miss] <- vapply(parts, function(p) p[length(p) - 1], "")
    x$workload[miss] <- vapply(parts, function(p) paste(p[2:(length(p) - 2)], collapse = "_"), "")
  }
  x <- x[x$rep > 0, ]
  x$ok <- x$exit == 0 & !is.na(x$inner_s)
  x
}
