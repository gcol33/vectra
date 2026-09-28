# Stage 2: record filters and the grid-cell key, streamed to disk.
suppressPackageStartupMessages(library(vectra))
g <- readRDS("grid.rds")   # WorldClim 10' grid geometry: xmin, ymax, res, ncol
keep_basis <- c("HUMAN_OBSERVATION", "PRESERVED_SPECIMEN", "OCCURRENCE",
                "OBSERVATION", "MATERIAL_SAMPLE", "MACHINE_OBSERVATION")
q <- tbl("records.vtr") |>
  filter(occurrencestatus == "PRESENT", taxonrank == "SPECIES",
         !is.na(specieskey), !is.na(lon), !is.na(lat),
         is.na(unc) | unc <= 10000, year >= 1950,
         basisofrecord %in% keep_basis,
         lon >= g$xmin, lon < g$xmax, lat > g$ymin, lat <= g$ymax) |>
  mutate(cell = floor((g$ymax - lat) / g$res) * g$ncol + floor((lon - g$xmin) / g$res)) |>
  select(specieskey, cell, year, datasetkey)
print(explain(q))
write_vtr(q, "clean.vtr")
cat("RESULT rows=", nrow(tbl("clean.vtr")), "\n", sep = "")
