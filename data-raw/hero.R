# Build the README banner man/figures/vectra-hero.png.
#
# A .vtr file of eight years of observations, written in year order with 8 row
# groups per year and sorted by station within each year. Each block is one row
# group of that file. The per-row-group min/max statistics stored in the file
# decide which row groups the query
#   filter(year >= 2021, station >= 65)
# reads; the other row groups are skipped without being decoded.
#
# Proportions follow the golden ratio phi: a block is phi times as wide as it is
# tall, the gap between blocks (and the margin) is 1/phi^2 of the block height,
# the corner radius is 1/phi^3 of it, the grid is phi times as wide as the label
# panel beside it, and the read region covers 3 of 8 years and 3 of 8 columns.
# The labels stand in the panel next to the blocks they name.
#
# Run from the package root:  Rscript data-raw/hero.R
# Needs ggplot2 and ragg only here; they are not package dependencies.

suppressMessages({
  library(ggplot2)
  library(vectra)
})

phi <- (1 + sqrt(5)) / 2

years <- 2016:2023
per_year <- 8L
rows_per_group <- 150000L
set.seed(1)

d <- do.call(rbind, lapply(years, function(y) {
  n <- per_year * rows_per_group
  data.frame(year = y,
             station = sort(sample.int(100L, n, replace = TRUE)),
             temperature = rnorm(n, 15, 8))
}))

f <- tempfile(fileext = ".vtr")
write_vtr(d, f, batch_size = rows_per_group)

stats <- .Call(vectra:::C_read_vtr_tdc_stats, f)
yr <- vapply(stats, function(g) g[[1]][2:3], numeric(2))
st <- vapply(stats, function(g) g[[2]][2:3], numeric(2))
stopifnot(length(stats) == length(years) * per_year)

# A row group is read when its [min, max] range can hold a matching row.
y_min <- 2021
s_min <- 65
read <- yr[2, ] >= y_min & st[2, ] >= s_min

# The engine returns the rows a plain data.frame filter returns.
res <- tbl(f) |> filter(year >= y_min, station >= s_min) |> collect()
stopifnot(nrow(res) == sum(d$year >= y_min & d$station >= s_min))
unlink(f)

n_rg <- length(read)
message(sum(read), " of ", n_rg, " row groups read")

# Geometry in units u of the block height: block phi x 1, gap 1/phi^2 (also the
# margin). The grid is phi times as wide as the label panel to its right.
n_row <- length(years)
bh_u <- 1
bw_u <- phi
gap_u <- 1 / phi^2
grid_u <- per_year * (bw_u + gap_u) + gap_u
panel_u <- grid_u / phi
W <- 1628
u <- W / (grid_u + panel_u)
H <- round(u * (n_row * (bh_u + gap_u) + gap_u))
bw <- bw_u * u
bh <- bh_u * u
gap <- gap_u * u
grid_right <- grid_u * u

rrect <- function(x0, y0, x1, y1, r, n = 8) {
  corner <- function(cx, cy, a0) {
    a <- seq(a0, a0 + pi / 2, length.out = n)
    cbind(cx + r * cos(a), cy + r * sin(a))
  }
  rbind(corner(x1 - r, y1 - r, 0), corner(x0 + r, y1 - r, pi / 2),
        corner(x0 + r, y0 + r, pi), corner(x1 - r, y0 + r, 3 * pi / 2))
}

idx <- seq_len(n_rg) - 1L
col <- idx %% per_year
row <- idx %/% per_year
x_left <- function(c) gap + c * (bw + gap)
y_mid <- function(r) H - gap - r * (bh + gap) - bh / 2

blocks <- do.call(rbind, lapply(seq_len(n_rg), function(i) {
  p <- rrect(x_left(col[i]), y_mid(row[i]) - bh / 2,
             x_left(col[i]) + bw, y_mid(row[i]) + bh / 2, r = bh / phi^3)
  data.frame(x = p[, 1], y = p[, 2], id = i, read = read[i])
}))

blue <- "#2E6F9E"
grey <- "#5C6166"

# The labels stand in the panel to the right of the grid, level with the blocks
# they name: "skipped" beside the middle of the all-skipped top rows, "read"
# beside the middle of the read rows.
r_skip <- median(unique(row[!read & row < min(row[read])]))
r_read <- median(unique(row[read]))
key <- data.frame(
  x = grid_right + bh,
  y = c(y_mid(r_skip), y_mid(r_read)),
  label = c("skipped", "read"),
  group = c("skipped", "read")
)

p <- ggplot() +
  geom_polygon(data = blocks[!blocks$read, ], aes(x, y, group = id),
               fill = alpha(blue, 0.10), colour = alpha(blue, 0.35), linewidth = 0.5) +
  geom_polygon(data = blocks[blocks$read, ], aes(x, y, group = id),
               fill = alpha(blue, 0.78), colour = NA) +
  geom_text(data = key, aes(x, y, label = label, colour = group), size = 22,
            fontface = "bold", hjust = 0) +
  scale_colour_manual(values = c(skipped = grey, read = blue), guide = "none") +
  coord_equal(xlim = c(0, W), ylim = c(0, H), expand = FALSE) +
  theme_void() +
  theme(plot.background = element_rect(fill = "white", colour = NA),
        plot.margin = margin(0, 0, 0, 0))

dir.create("man/figures", recursive = TRUE, showWarnings = FALSE)
ggsave("man/figures/vectra-hero.png", p, width = W / 120, height = H / 120, dpi = 120,
       bg = "white", device = ragg::agg_png)
