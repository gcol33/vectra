# Geographic inputs: WorldClim 2.1 BIO1 and BIO12 at 10', cropped to Europe and
# re-encoded with DEFLATE (vectra's GeoTIFF reader does not decode LZW, the
# compression WorldClim distributes); Natural Earth 1:10m countries.
suppressMessages({library(sf); library(terra)})
sf_use_s2(FALSE)
geo <- Sys.getenv("GEO_DIR", "geo"); dir.create(geo, FALSE)
# wc2.1_10m_bio.zip from https://geodata.ucdavis.edu/climate/worldclim/2_1/base/
# ne_10m_admin_0_countries.* from https://github.com/nvkelso/natural-earth-vector
for (b in c(1, 12)) {
  r <- crop(rast(file.path(geo, sprintf("wc2.1_10m_bio_%d.tif", b))), ext(-25, 45, 34, 72))
  writeRaster(r, file.path(geo, sprintf("bio%d_eu_deflate.tif", b)), overwrite = TRUE,
              gdal = c("COMPRESS=DEFLATE"))
}
r <- rast(file.path(geo, "bio1_eu_deflate.tif")); e <- ext(r)
g <- lapply(list(xmin = e[1], xmax = e[2], ymin = e[3], ymax = e[4], res = res(r)[1],
                 ncol = ncol(r), nrow = nrow(r)), unname)
saveRDS(g, "grid.rds")
w <- st_read(file.path(geo, "ne_10m_admin_0_countries.shp"), quiet = TRUE)
# case study: European countries (plus Cyprus), cropped to the climate grid
eu <- w[w$CONTINENT == "Europe" | w$ADM0_A3 %in% c("CYP", "CYN"), c("ADM0_A3", "NAME", "SUBREGION")]
bb <- st_as_sfc(st_bbox(c(xmin = g$xmin, ymin = g$ymin, xmax = g$xmax, ymax = g$ymax), crs = 4326))
eu <- st_cast(suppressWarnings(st_intersection(eu, bb)), "MULTIPOLYGON")
names(eu)[1:3] <- c("adm0", "name", "region")
eu$region[eu$adm0 %in% c("CYP", "CYN")] <- "Southern Europe"
saveRDS(eu, file.path(geo, "eu_countries.rds"))
# benchmark: all countries clipped to 10W-30E, 36N-70N
b2 <- w[, c("ADM0_A3", "ISO_A2_EH", "NAME", "CONTINENT", "SUBREGION")]
bb2 <- st_as_sfc(st_bbox(c(xmin = -10, ymin = 36, xmax = 30, ymax = 70), crs = 4326))
b2 <- suppressWarnings(st_intersection(b2, bb2)); b2 <- st_cast(b2[!st_is_empty(b2), ], "MULTIPOLYGON")
names(b2)[1] <- "adm0"
saveRDS(b2, file.path(geo, "eu_bbox.rds"))
