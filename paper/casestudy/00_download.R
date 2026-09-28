# Stage 0 (acquisition, outside vectra): European vascular-plant records from
# the GBIF monthly snapshot of 1 September 2026 (doi:10.15468/dl.e9fsnq) on the
# GBIF AWS open-data bucket. Only the 6077 of 9898 Parquet parts that hold
# Tracheophyta records from European countries are read; each group of 50
# parts is written to one local Parquet file. This stands in for a GBIF
# SIMPLE_PARQUET download requested with rgbif::occ_download().
library(DBI)
con <- dbConnect(duckdb::duckdb())
dbExecute(con, "SET extension_directory = '/home/claude/duckdb_ext'; LOAD httpfs;")
dbExecute(con, sprintf("SET http_proxy='%s'; SET ca_cert_file='/root/.ccr/ca-bundle.crt'; SET threads=8; SET preserve_insertion_order=false;", sub('^https?://', '', Sys.getenv('HTTPS_PROXY'))))
eu <- c("AL","AD","AT","BY","BE","BA","BG","HR","CY","CZ","DK","EE","FO","FI","FR","DE","GI","GR","GG","HU","IS","IE","IM","IT","JE","XK","LV","LI","LT","LU","MT","MD","MC","ME","NL","MK","NO","PL","PT","RO","RU","SM","RS","SK","SI","ES","SJ","SE","CH","UA","GB","VA","AX")
inl <- paste0("'", eu, "'", collapse = ",")
f <- readRDS("/home/claude/gbif/eu_files.rds")$filename
f <- sort(f)
dir.create("raw", FALSE)
grp <- split(f, ceiling(seq_along(f) / 50))
for (i in seq_along(grp)) {
  out <- sprintf("raw/part%03d.parquet", i)
  if (file.exists(out)) next
  q <- sprintf("COPY (SELECT specieskey, species, family,
      decimallongitude AS lon, decimallatitude AS lat, coordinateuncertaintyinmeters AS unc,
      year, basisofrecord, occurrencestatus, taxonrank, countrycode, datasetkey
    FROM read_parquet([%s]) WHERE phylum = 'Tracheophyta' AND countrycode IN (%s))
    TO '%s.tmp' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)",
    paste0("'", grp[[i]], "'", collapse = ","), inl, out)
  ok <- tryCatch({dbExecute(con, q); TRUE}, error = function(e) {message(conditionMessage(e)); FALSE})
  if (ok) file.rename(paste0(out, ".tmp"), out)
  message(i, "/", length(grp), " ", Sys.time())
}
