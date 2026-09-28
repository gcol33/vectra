# Stage 5: range size against climatic niche breadth. The occupancy table is
# split on disk by European region; each shard is read into memory on its own,
# reduced to one row per species, and modelled. A Europe-wide species table is
# computed in the engine for comparison.
suppressPackageStartupMessages(library(vectra))
# One row per species from a shard of the occupancy table. Range size is the
# summed area of occupied cells. Niche breadth is the standard deviation of the
# cells' climate, rarefied to 10 cells (mean over 20 random draws) so that it is
# measured at the same sample size for every species.
species_tab <- function(d, k = 10, draws = 20) {
  set.seed(1)
  idx <- split(seq_len(nrow(d)), d$specieskey)
  idx <- idx[lengths(idx) >= k]
  rare_sd <- function(v) mean(vapply(seq_len(draws), function(i) sd(sample(v, k)), 0))
  data.frame(
    specieskey = names(idx),
    n_rec     = vapply(idx, function(i) sum(d$n[i]), 0),
    n_cells   = lengths(idx),
    range_km2 = vapply(idx, function(i) sum(d$area_km2[i]), 0),
    br_T      = vapply(idx, function(i) rare_sd(d$bio1[i]), 0),
    br_P      = vapply(idx, function(i) rare_sd(d$lp[i]), 0),
    row.names = NULL)
}
fit_one <- function(sp) {
  sp <- sp[sp$n_rec >= 50 & sp$n_cells >= 10 & sp$br_T > 0 & sp$br_P > 0, ]
  if (nrow(sp) < 30) return(data.frame(term = NA_character_, estimate = NA, se = NA,
                                       n_species = nrow(sp), r2 = NA))
  m <- lm(log(range_km2) ~ log(br_T) + log(br_P) + log(n_rec), data = sp)
  cf <- summary(m)$coefficients
  data.frame(term = rownames(cf), estimate = cf[, 1], se = cf[, 2],
             n_species = nrow(sp), r2 = summary(m)$r.squared, row.names = NULL)
}
p <- offload(tbl("occ_env.vtr") |> select(specieskey, n, area_km2, bio1, lp, region), by = "region")
print(p)
by_region <- group_modify(p, function(d, region) fit_one(species_tab(d)))
saveRDS(by_region, "fits_region.rds")
# Europe-wide: species summary computed in the engine (sd as breadth), then lm()
eu_sp <- tbl("occ_env.vtr") |>
  group_by(specieskey) |>
  summarise(n_rec = sum(n), n_cells = n(), range_km2 = sum(area_km2),
            sd_T = sd(bio1), sd_P = sd(lp)) |>
  collect()
saveRDS(eu_sp, "species_europe.rds")
eu_sp <- eu_sp[eu_sp$n_rec >= 50 & eu_sp$n_cells >= 10 & eu_sp$sd_T > 0 & eu_sp$sd_P > 0, ]
m <- lm(log(range_km2) ~ log(sd_T) + log(sd_P) + log(n_rec), data = eu_sp)
saveRDS(m, "fit_europe.rds")
print(summary(m)); print(by_region)
cat("RESULT species=", nrow(eu_sp), "\n", sep = "")
