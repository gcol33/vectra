# One benchmark run in a fresh R process.
#   Rscript bench_one.R <tool> <workload> <param> <outdir>
# Prints one line "RESULT <json>" with the inner elapsed time, a result
# checksum used to verify equivalence across tools, and the process I/O
# counters from /proc/self/io. Peak RSS and wall time are measured outside
# by GNU time; temporary disk by a poller on $TMPDIR.

args <- commandArgs(TRUE)
label <- args[1]; tool <- sub("1g$", "", label); if (label != tool) options(vectra.memory = "1GB")
workload <- args[2]; param <- args[3]; outdir <- args[4]
threads <- 2L
Sys.setenv(OMP_NUM_THREADS = threads)
DATA <- normalizePath("data")
chunks <- function(n_rows) seq_len(round(as.numeric(n_rows) / 1e7))
files <- function(fmt, n_rows) {
  ext <- c(vtr = "vtr", pq = "parquet", fst = "fst")[[fmt]]
  file.path(DATA, fmt, sprintf("chunk%02d.%s", chunks(n_rows), ext))
}
io <- function() {
  l <- readLines("/proc/self/io")
  v <- as.numeric(sub(".*: ", "", l)); names(v) <- sub(":.*", "", l); v
}
out_file <- function(ext) file.path(outdir, paste0("out.", ext))

suppressPackageStartupMessages({
  if (tool == "vectra") library(vectra)
  if (tool %in% c("arrow")) { library(arrow); library(dplyr) }
  if (tool == "duckdb") { library(DBI); library(duckdb) }
  if (tool == "datatable") { library(data.table); library(fst) }
  if (tool == "sf") library(sf)
  if (tool == "terra") library(terra)
})
if (tool == "arrow") arrow::set_cpu_count(threads)
if (tool == "datatable") setDTthreads(threads)
if (tool == "duckdb") {
  con <- dbConnect(duckdb::duckdb())
  dbExecute(con, sprintf("SET threads = %d", threads))
  dbExecute(con, sprintf("SET temp_directory = '%s'", Sys.getenv("TMPDIR")))
  dbExecute(con, "SET preserve_insertion_order = false")
  dbExecute(con, "SET extension_directory = '/home/claude/duckdb_ext'")
}
pq_list <- function(n) paste0("[", paste0("'", files("pq", n), "'", collapse = ","), "]")
vtr_src <- function(n) {
  f <- files("vtr", n)
  if (length(f) == 1) tbl(f) else do.call(bind_rows, lapply(f, tbl))
}
dt_src <- function(n, cols) rbindlist(lapply(files("fst", n), read_fst,
                                             columns = cols, as.data.table = TRUE))

if (workload == "noop") { check <- 0 }
io0 <- io(); t0 <- proc.time()[["elapsed"]]
res <- NULL; check <- NA_real_; extra <- list()

# ---------------------------------------------------------------- Q1 scan
# filter -> mutate -> group by year (75 groups) -> summarise, N rows varies
if (workload == "scan") {
  N <- param
  if (tool == "vectra") {
    res <- vtr_src(N) |> filter(year >= 1990, v1 > 0) |> mutate(z = v1 * v2) |>
      group_by(year) |> summarise(n = n(), mz = mean(z), sv = sum(v2)) |> collect()
  } else if (tool == "arrow") {
    res <- open_dataset(files("pq", N)) |> filter(year >= 1990, v1 > 0) |>
      mutate(z = v1 * v2) |> group_by(year) |>
      summarise(n = n(), mz = mean(z), sv = sum(v2)) |> collect()
  } else if (tool == "duckdb") {
    res <- dbGetQuery(con, sprintf(
      "SELECT year, count(*) AS n, avg(v1 * v2) AS mz, sum(v2) AS sv
         FROM read_parquet(%s) WHERE year >= 1990 AND v1 > 0 GROUP BY year", pq_list(N)))
  } else if (tool == "datatable") {
    d <- dt_src(N, c("year", "v1", "v2"))
    res <- d[year >= 1990 & v1 > 0, .(n = .N, mz = mean(v1 * v2), sv = sum(v2)), by = year]
  }
  check <- sum(res$n) + round(sum(res$sv), 0)
}

# ---------------------------------------------------------------- Q2 groups
# group by a key of cardinality G at N = 5e7, result written to disk
if (workload == "groups") {
  N <- 5e7; key <- c("1e3" = "k3", "1e5" = "k5", "1e6" = "k6", "1e7" = "k7")[[param]]
  if (tool == "vectra") {
    do.call(group_by, list(vtr_src(N), as.name(key))) |>
      summarise(n = n(), m = mean(v1), s = sum(v2)) |> write_vtr(out_file("vtr"))
    res <- tbl(out_file("vtr")) |> summarise(g = n(), n = sum(n), s = sum(s)) |> collect()
  } else if (tool == "arrow") {
    open_dataset(files("pq", N)) |> group_by(!!rlang::sym(key)) |>
      summarise(n = n(), m = mean(v1), s = sum(v2)) |>
      write_dataset(file.path(outdir, "out_pq"))
    res <- open_dataset(file.path(outdir, "out_pq")) |>
      summarise(g = n(), n = sum(n), s = sum(s)) |> collect()
  } else if (tool == "duckdb") {
    dbExecute(con, sprintf(
      "COPY (SELECT %s, count(*) AS n, avg(v1) AS m, sum(v2) AS s FROM read_parquet(%s) GROUP BY %s)
       TO '%s' (FORMAT parquet)", key, pq_list(N), key, out_file("parquet")))
    res <- dbGetQuery(con, sprintf("SELECT count(*) g, sum(n) n, sum(s) s FROM '%s'", out_file("parquet")))
  } else if (tool == "datatable") {
    d <- dt_src(N, c(key, "v1", "v2"))
    r <- d[, .(n = .N, m = mean(v1), s = sum(v2)), by = key]
    write_fst(r, out_file("fst"))
    res <- data.frame(g = nrow(r), n = sum(r$n), s = sum(r$s))
  }
  check <- res$g + res$n + round(res$s, 0)
}

# ---------------------------------------------------------------- Q3 join
# inner join a 5e7-row fact table to a build side of M rows, then aggregate
if (workload == "join") {
  N <- 5e7
  key <- c("1e3" = "k3", "1e5" = "k5", "1e6" = "k6", "1e7" = "k7", "5e7" = "id")[[param]]
  M <- as.numeric(param)
  dimf <- file.path(DATA, "dim", sprintf("dim_%s", param))
  if (tool == "vectra") {
    res <- vtr_src(N) |> select(all_of(c(key, "year"))) |>
      inner_join(tbl(paste0(dimf, ".vtr")), by = key) |>
      group_by(year) |> summarise(n = n(), sw = sum(w)) |> collect()
  } else if (tool == "arrow") {
    res <- open_dataset(files("pq", N)) |> select(all_of(c(key, "year"))) |>
      inner_join(open_dataset(paste0(dimf, ".parquet")), by = key) |>
      group_by(year) |> summarise(n = n(), sw = sum(w)) |> collect()
  } else if (tool == "duckdb") {
    res <- dbGetQuery(con, sprintf(
      "SELECT f.year, count(*) AS n, sum(d.w) AS sw FROM read_parquet(%s) f
         JOIN '%s.parquet' d ON f.%s = d.%s GROUP BY f.year", pq_list(N), dimf, key, key))
  } else if (tool == "datatable") {
    d <- dt_src(N, c(key, "year"))
    dm <- as.data.table(read_fst(paste0(dimf, ".fst")))
    r <- dm[d, on = key, nomatch = NULL]
    res <- r[, .(n = .N, sw = sum(w)), by = year]
  }
  check <- sum(res$n) + round(sum(res$sw), 0)
}

# ---------------------------------------------------------------- Q4 sort
# full sort of all columns by v1, written to disk, N varies
if (workload == "sort") {
  N <- param
  if (tool == "vectra") {
    vtr_src(N) |> arrange(v1) |> write_vtr(out_file("vtr"))
    res <- tbl(out_file("vtr")) |> slice_head(n = 3) |> collect()
    nr <- nrow(tbl(out_file("vtr")))
  } else if (tool == "arrow") {
    open_dataset(files("pq", N)) |> arrange(v1) |> write_dataset(file.path(outdir, "out_pq"))
    res <- open_dataset(file.path(outdir, "out_pq")) |> arrange(v1) |> head(3) |> collect()
    nr <- open_dataset(file.path(outdir, "out_pq")) |> count() |> collect() |> pull(n)
  } else if (tool == "duckdb") {
    dbExecute(con, sprintf("COPY (SELECT * FROM read_parquet(%s) ORDER BY v1) TO '%s' (FORMAT parquet)",
                           pq_list(N), out_file("parquet")))
    res <- dbGetQuery(con, sprintf("SELECT * FROM '%s' ORDER BY v1 LIMIT 3", out_file("parquet")))
    nr <- dbGetQuery(con, sprintf("SELECT count(*) n FROM '%s'", out_file("parquet")))$n
  } else if (tool == "datatable") {
    d <- dt_src(N, NULL); setorder(d, v1); write_fst(d, out_file("fst"))
    res <- d[1:3]; nr <- nrow(d)
  }
  check <- nr + round(sum(res$v1) * 1e6)
}

# ---------------------------------------------------------------- Q8 collect
# materialising a result in R: filter to a fraction s of N = 5e7 rows,
# then collect() it or stream it to a file
if (workload %in% c("collect", "sink")) {
  N <- 5e7; s <- as.numeric(param); thr <- qnorm(1 - s)
  if (tool == "vectra") {
    q <- vtr_src(N) |> filter(v1 > thr) |> select(id, sp, v1, v2)
    if (workload == "collect") { r <- collect(q); check <- nrow(r) }
    else { write_vtr(q, out_file("vtr")); check <- nrow(tbl(out_file("vtr"))) }
  } else if (tool == "arrow") {
    q <- open_dataset(files("pq", N)) |> filter(v1 > thr) |> select(id, sp, v1, v2)
    if (workload == "collect") { r <- collect(q); check <- nrow(r) }
    else { write_dataset(q, file.path(outdir, "out_pq"))
           check <- open_dataset(file.path(outdir, "out_pq")) |> count() |> collect() |> pull(n) }
  } else if (tool == "duckdb") {
    q <- sprintf("SELECT id, sp, v1, v2 FROM read_parquet(%s) WHERE v1 > %.17g", pq_list(N), thr)
    if (workload == "collect") { r <- dbGetQuery(con, q); check <- nrow(r) }
    else { dbExecute(con, sprintf("COPY (%s) TO '%s' (FORMAT parquet)", q, out_file("parquet")))
           check <- dbGetQuery(con, sprintf("SELECT count(*) n FROM '%s'", out_file("parquet")))$n }
  }
}

# ---------------------------------------------------------------- A: ablations
# vectra only, single 5e7-row store (data/abl/f5e7.vtr, 381 row groups)
if (workload == "abl") {
  f <- file.path(DATA, "abl", "f5e7.vtr"); fi <- file.path(DATA, "abl", "f5e7_idx.vtr")
  r <- switch(param,
    # zone maps: two filters of equal selectivity (1%), on a column whose row
    # groups are ordered (id) and on one that is random (k7)
    zm_sorted   = tbl(f) |> filter(id > 2e7, id <= 2.05e7) |> summarise(n = n(), s = sum(v1)) |> collect(),
    zm_random   = tbl(f) |> filter(k7 <= 1e5) |> summarise(n = n(), s = sum(v1)) |> collect(),
    # column pruning: one column versus all numeric columns
    cols_1      = tbl(f) |> summarise(s = sum(v1)) |> collect(),
    cols_all    = tbl(f) |> summarise(s = sum(v1), a = sum(v2), b = sum(x), c = sum(y), d = sum(id),
                                      e = sum(k3), g = sum(k5), h = sum(k6), i = sum(k7), j = sum(year)) |> collect(),
    # hash index: point lookup with and without a .vtri sidecar
    idx_none    = tbl(f)  |> filter(k7 == 4242424) |> collect(),
    idx_vtri    = tbl(fi) |> filter(k7 == 4242424) |> collect(),
    # pushdown through a mutate (the optimiser currently stops at it)
    mut_before  = tbl(f) |> mutate(z = v1 * 2) |> filter(id <= 5e5) |> summarise(n = n()) |> collect(),
    mut_after   = tbl(f) |> filter(id <= 5e5) |> mutate(z = v1 * 2) |> summarise(n = n()) |> collect())
  check <- sum(sapply(r, function(v) if (is.numeric(v)) sum(v) else 0))
  extra$nrow <- nrow(r)
}

# ---------------------------------------------------------------- M: models
# logistic regression by IRLS (biglm::bigglm) on a prepared query that joins
# the fact table to dim_1e6 and derives the response; N rows varies
if (workload %in% c("model_rebuild", "model_offload", "model_glm")) {
  N <- param
  suppressPackageStartupMessages(library(biglm))
  prep <- function() vtr_src(N) |> filter(year >= 1970) |>
    inner_join(tbl(file.path(DATA, "dim", "dim_1e6.vtr")), by = "k6") |>
    mutate(pres = if_else(v1 + v2 - 1 > 0.8 * w - 0.02 * (y - 53), 1, 0),
           ys = (y - 53) / 10) |>
    select(pres, w, ys)
  t_prep <- 0
  if (workload == "model_rebuild") {
    fit <- bigglm(pres ~ w + ys, data = chunk_feeder(prep), family = binomial(), maxit = 20)
  } else if (workload == "model_offload") {
    tp <- proc.time()[["elapsed"]]; s <- offload(prep()); t_prep <- proc.time()[["elapsed"]] - tp
    fit <- bigglm(pres ~ w + ys, data = chunk_feeder(s), family = binomial(), maxit = 20)
  } else {
    tp <- proc.time()[["elapsed"]]; d <- collect(prep()); t_prep <- proc.time()[["elapsed"]] - tp
    fit <- glm(pres ~ w + ys, data = d, family = binomial())
  }
  cf <- coef(fit)
  extra <- list(t_prep = t_prep, iter = if (!is.null(fit$iterations)) fit$iterations else fit$iter,
                coef = unname(cf))
  check <- round(sum(cf), 4)
}

# ---------------------------------------------------------------- S: spatial
# point-in-polygon tagging of N points against 53 European country polygons
# (Natural Earth 1:10m, clipped), then a count per country
if (workload == "pip") {
  N <- as.numeric(param)
  suppressPackageStartupMessages(library(sf)); sf_use_s2(FALSE)
  eu <- readRDS("/home/claude/geo/eu_bbox.rds")[, "adm0"]
  pts_src <- function() {
    s <- vtr_src(max(N, 1e7))
    if (N < 1e7) s <- s |> filter(id <= N)
    s |> select(id, x, y)
  }
  if (tool == "vectra") {
    suppressPackageStartupMessages(library(sf)); sf_use_s2(FALSE)
    res <- pts_src() |> spatial_join(eu, coords = c("x", "y"), crs = 4326, left = FALSE) |>
      count(adm0) |> collect()
  } else if (tool == "sf") {
    sf_use_s2(FALSE)
    suppressPackageStartupMessages(library(vectra))
    d <- collect(pts_src())                       # vectra only to read the same points
    p <- st_as_sf(d, coords = c("x", "y"), crs = 4326)
    j <- st_join(p, eu, join = st_intersects, left = FALSE)
    res <- as.data.frame(table(adm0 = j$adm0), stringsAsFactors = FALSE); names(res)[2] <- "n"
  } else if (tool == "terra") {
    suppressPackageStartupMessages(library(vectra))
    d <- collect(pts_src())
    v <- vect(eu)
    e <- terra::extract(v, as.matrix(d[, c("x", "y")]))
    e <- e[!is.na(e$adm0), ]
    res <- as.data.frame(table(adm0 = e$adm0), stringsAsFactors = FALSE); names(res)[2] <- "n"
  } else if (tool == "duckdb") {
    dbExecute(con, "INSTALL spatial; LOAD spatial;")
    sf::st_write(eu, file.path(outdir, "eu.gpkg"), quiet = TRUE)
    dbExecute(con, sprintf("CREATE TABLE eu AS SELECT adm0, geom FROM ST_Read('%s')", file.path(outdir, "eu.gpkg")))
    lim <- if (N < 1e7) sprintf("WHERE id <= %d", as.integer(N)) else ""
    res <- dbGetQuery(con, sprintf(
      "SELECT e.adm0, count(*) AS n FROM (SELECT x, y FROM read_parquet(%s) %s) p
         JOIN eu e ON ST_Intersects(e.geom, ST_Point(p.x, p.y)) GROUP BY e.adm0", pq_list(max(N, 1e7)), lim))
  }
  check <- sum(res$n)
  extra$n_countries <- nrow(res)
}

el <- proc.time()[["elapsed"]] - t0
io1 <- io()
cat("RESULT", jsonlite::toJSON(list(tool = label, workload = workload, param = param,
    inner_s = el, check = check, rchar = io1[["rchar"]] - io0[["rchar"]],
    read_bytes = io1[["read_bytes"]] - io0[["read_bytes"]],
    write_bytes = io1[["write_bytes"]] - io0[["write_bytes"]], extra = extra), auto_unbox = TRUE, digits = NA), "\n")
