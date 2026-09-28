# Stage 3: the grid-cell table. Climate comes from two WorldClim 2.1 layers read
# as tables (one row per pixel) and joined on the cell key; the country and
# European subregion of each cell come from a streamed point-in-polygon join of
# the cell centres against Natural Earth country polygons.
suppressPackageStartupMessages({library(vectra); library(sf)})
sf_use_s2(FALSE)
g <- readRDS("grid.rds")
cellkey <- function(node) node |>
  filter(!is.na(band1)) |>
  mutate(cell = floor((g$ymax - y) / g$res) * g$ncol + floor((x - g$xmin) / g$res))
bio1  <- tbl_tiff("/home/claude/geo/bio1_eu_deflate.tif")  |> cellkey() |> select(cell, x, y, bio1 = band1)
bio12 <- tbl_tiff("/home/claude/geo/bio12_eu_deflate.tif") |> cellkey() |> select(cell, bio12 = band1)
write_vtr(inner_join(bio1, bio12, by = "cell"), "clim.vtr")
countries <- readRDS("/home/claude/geo/eu_countries.rds")   # sf, adm0 + region
# The cell table is small (one row per land cell), so it is finished in R:
# vectra's expression evaluator has no trigonometric functions.
cells <- tbl("clim.vtr") |>
  spatial_join(countries, coords = c("x", "y"), crs = 4326, left = FALSE) |>
  select(cell, x, y, bio1, bio12, adm0, region) |>
  collect()
cells$lp <- log1p(cells$bio12)
cells$area_km2 <- (g$res * 111.32)^2 * cos(cells$y * pi / 180)
write_vtr(cells, "cells.vtr")
cat("RESULT cells=", nrow(tbl("cells.vtr")), "\n", sep = "")
