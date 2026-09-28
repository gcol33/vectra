# Stage 4: species x cell occupancy with the cell attributes attached.
# The record table is reduced to one row per species and cell, then joined to
# the (small) cell table; both steps stream to disk.
suppressPackageStartupMessages(library(vectra))
occ <- tbl("clean.vtr") |>
  group_by(specieskey, cell) |>
  summarise(n = n(), n_ds = n_distinct(datasetkey), first = min(year), last = max(year))
write_vtr(occ, "occ.vtr")
tbl("occ.vtr") |> inner_join(tbl("cells.vtr"), by = "cell") |> write_vtr("occ_env.vtr")
cat("RESULT occ=", nrow(tbl("occ.vtr")), " occ_env=", nrow(tbl("occ_env.vtr")), "\n", sep = "")
