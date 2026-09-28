# Benchmark figures. Run from the paper directory: Rscript scripts/figures.R
suppressPackageStartupMessages({library(ggplot2); library(patchwork)})
source("scripts/results.R")

theme_rj <- function() theme_minimal(base_size = 9) +
  theme(panel.grid.minor = element_blank(),
        panel.grid.major = element_line(colour = "grey90", linewidth = 0.3),
        axis.line = element_line(colour = "grey40", linewidth = 0.3),
        axis.ticks = element_line(colour = "grey40", linewidth = 0.3),
        legend.position = "bottom", legend.title = element_blank(),
        plot.title = element_text(size = 9, face = "bold"),
        strip.text = element_text(size = 8, face = "bold"))

tools5 <- c("vectra", "vectra1g", "arrow", "duckdb", "datatable")
sc_col <- scale_colour_manual(values = tool_col, labels = tool_lab, breaks = names(tool_lab))
sc_shp <- scale_shape_manual(values = tool_shape, labels = tool_lab, breaks = names(tool_lab))
short <- function(x) ifelse(x >= 1e6, paste0(x / 1e6, "M"), ifelse(x >= 1e3, paste0(x / 1e3, "k"), x))
log_x <- function(lab, br = NULL) scale_x_log10(breaks = br, labels = short, name = lab)
mem_line <- function() list(geom_hline(yintercept = 8032 / 1024, linetype = "dashed", colour = "grey40", linewidth = 0.3), annotate("text", x = -Inf, y = 8032 / 1024, label = "physical memory", hjust = -0.05, vjust = -0.4, size = 2.5, colour = "grey30"))

# Runs that failed are drawn as open symbols at the memory they had reached.
panel <- function(w, y, ylab, xlab, tools = tools5, log_y = TRUE, title = NULL) {
  d <- summ[summ$workload == w & summ$tool %in% tools, ]
  d$y <- if (y == "rss") d$rss / 1024 else d[[y]]
  d$failed <- d$n_ok == 0
  ok <- d[!d$failed, ]
  p <- ggplot(ok, aes(param_num, y, colour = tool, shape = tool, group = tool)) +
    geom_line(linewidth = 0.5) + geom_point(size = 2) +
    sc_col + sc_shp + log_x(xlab, sort(unique(d$param_num))) + labs(y = ylab, title = title) + theme_rj()
  if (y == "rss" && any(d$failed))
    p <- p + geom_point(data = d[d$failed, ], shape = 4, size = 2.5, stroke = 0.9, show.legend = FALSE)
  if (y == "rss") p <- p + mem_line()
  if (log_y) p <- p + scale_y_log10() else p <- p + expand_limits(y = 0)
  p
}

# Figure: scaling of the scan-filter-aggregate workload with input size
f1 <- panel("scan", "wall", "Elapsed time (s)", "Input rows", title = "A  Time") +
      panel("scan", "rss", "Peak memory (GB)", "Input rows", log_y = FALSE, title = "B  Peak memory") +
      plot_layout(guides = "collect") & theme(legend.position = "bottom")
ggsave("figures/scan.pdf", f1, width = 6.5, height = 2.9)
ggsave("figures/scan.png", f1, width = 6.5, height = 2.9, dpi = 200)

# Figure: stateful operations at N = 5e7
f2 <- (panel("groups", "wall", "Elapsed time (s)", "Distinct groups", title = "A  Grouping: time") |
       panel("groups", "rss", "Peak memory (GB)", "Distinct groups", log_y = FALSE, title = "B  Grouping: memory")) /
      (panel("join", "wall", "Elapsed time (s)", "Build-side rows", title = "C  Join: time") |
       panel("join", "rss", "Peak memory (GB)", "Build-side rows", log_y = FALSE, title = "D  Join: memory")) +
      plot_layout(guides = "collect") & theme(legend.position = "bottom")
ggsave("figures/stateful.pdf", f2, width = 6.5, height = 5.4)
ggsave("figures/stateful.png", f2, width = 6.5, height = 5.4, dpi = 200)

# Figure: materialising in R (collect) versus streaming to a file (sink)
d <- summ[summ$workload %in% c("collect", "sink") & summ$tool %in% c("vectra", "arrow", "duckdb"), ]
d$rows <- d$param_num * 5e7
d$mode <- ifelse(d$workload == "collect", "collect() into R", "stream to file")
f3 <- ggplot(d, aes(rows, rss / 1024, colour = tool, shape = tool, linetype = mode,
                    group = interaction(tool, mode))) +
  geom_line(linewidth = 0.5) + geom_point(size = 2) + sc_col + sc_shp +
  scale_linetype_manual(values = c("collect() into R" = "solid", "stream to file" = "22")) +
  log_x("Rows in result", sort(unique(d$rows))) + labs(y = "Peak memory (GB)") + expand_limits(y = 0) + theme_rj() +
  guides(linetype = guide_legend(nrow = 2), colour = guide_legend(nrow = 1), shape = guide_legend(nrow = 1))
ggsave("figures/collect.pdf", f3, width = 4.5, height = 3.1)
ggsave("figures/collect.png", f3, width = 4.5, height = 3.1, dpi = 200)

# Figure: point-in-polygon tagging
f4 <- panel("pip", "wall", "Elapsed time (s)", "Points", tools = c("vectra", "sf", "duckdb"), title = "A  Time") +
      panel("pip", "rss", "Peak memory (GB)", "Points", tools = c("vectra", "sf", "duckdb"), log_y = FALSE, title = "B  Peak memory") +
      plot_layout(guides = "collect") & theme(legend.position = "bottom")
ggsave("figures/pip.pdf", f4, width = 6.5, height = 2.9)
ggsave("figures/pip.png", f4, width = 6.5, height = 2.9, dpi = 200)
cat("figures written\n")
