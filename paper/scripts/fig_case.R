# Case-study figure: niche-breadth elasticities of range size by region.
# Sourced from the case-study child document (CS in scope), or run directly.
suppressPackageStartupMessages(library(ggplot2))
if (!exists("CS")) { source("scripts/results.R") }
f <- CS$fits
f <- f[f$term %in% c("log(br_T)", "log(br_P)", "log(sd_T)", "log(sd_P)"), ]
f$predictor <- ifelse(grepl("_T", f$term), "Temperature breadth", "Precipitation breadth")
f$predictor <- factor(f$predictor, c("Temperature breadth", "Precipitation breadth"))
f$lo <- f$estimate - 1.96 * f$se; f$hi <- f$estimate + 1.96 * f$se
lev <- c(sort(unique(f$region[f$region != "Europe (unrarefied)"])), "Europe (unrarefied)")
f$region <- factor(f$region, rev(lev))
f$kind <- ifelse(f$region == "Europe (unrarefied)", "Europe-wide, breadth not rarefied",
                 "Region, breadth rarefied to 10 cells")
p <- ggplot(f, aes(estimate, region, shape = kind)) +
  geom_vline(xintercept = 0, colour = "grey60", linewidth = 0.3) +
  geom_errorbarh(aes(xmin = lo, xmax = hi), height = 0, colour = "#2a78d6", linewidth = 0.6) +
  geom_point(size = 2.2, colour = "#2a78d6", fill = "white") +
  scale_shape_manual(values = c("Region, breadth rarefied to 10 cells" = 16,
                                "Europe-wide, breadth not rarefied" = 21)) +
  facet_wrap(~ predictor, nrow = 1) +
  labs(x = "Elasticity of range size (with 95% confidence interval)", y = NULL) +
  theme_minimal(base_size = 9) +
  theme(panel.grid.minor = element_blank(), panel.grid.major.y = element_blank(),
        legend.position = "bottom", legend.title = element_blank(),
        strip.text = element_text(face = "bold"))
if (!interactive() && is.null(knitr::current_input())) {
  ggsave("figures/case.pdf", p, width = 6.5, height = 2.8)
  ggsave("figures/case.png", p, width = 6.5, height = 2.8, dpi = 200)
} else print(p)
