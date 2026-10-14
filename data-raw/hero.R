# Build the README banner man/figures/vectra-hero.png.
#
# A .vtr file of eight years of observations, written in year order with 24 row
# groups per year and sorted by station within each year. Each block is one row
# group of that file. The per-row-group min/max statistics stored in the file
# decide which row groups the query
#   filter(year >= 2021, station <= 30)
# reads; the other row groups are skipped without being decoded.
#
# Run from the package root:  Rscript data-raw/hero.R
# Needs ggplot2 and ragg only here; they are not package dependencies.

suppressMessages({
  library(ggplot2)
  library(vectra)
})

years <- 2016:2023
per_year <- 24L
rows_per_group <- 50000L
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
s_max <- 30
read <- yr[2, ] >= y_min & st[1, ] <= s_max

# The engine returns the rows a plain data.frame filter returns.
res <- tbl(f) |> filter(year >= y_min, station <= s_max) |> collect()
stopifnot(nrow(res) == sum(d$year >= y_min & d$station <= s_max))
unlink(f)

n_rg <- length(read)
n_read <- sum(read)
message(n_read, " of ", n_rg, " row groups read")

# Layout in pixels of the 1628 x 720 banner (120 dpi): one row per year.
W <- 1628
H <- 720
pad <- 8
pitch_x <- (W - 2 * pad) / per_year
pitch_y <- (H - 2 * pad) / length(years)
bw <- 0.84 * pitch_x
bh <- 0.76 * pitch_y

rrect <- function(x0, y0, x1, y1, r = 9, n = 6) {
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
blocks <- do.call(rbind, lapply(seq_len(n_rg), function(i) {
  cx <- pad + (col[i] + 0.5) * pitch_x
  cy <- H - pad - (row[i] + 0.5) * pitch_y
  p <- rrect(cx - bw / 2, cy - bh / 2, cx + bw / 2, cy + bh / 2)
  data.frame(x = p[, 1], y = p[, 2], id = i, read = read[i])
}))

blue <- "#2E6F9E"
grey <- "#5C6166"

# Labels sit over the skipped blocks, each under a soft white glow that traces
# the letters (translucent copies on circles of growing radius).
key <- data.frame(
  x = c(0.62 * W, 0.47 * W),
  y = c(H - pad - 2.5 * pitch_y, H - pad - 6.5 * pitch_y),
  label = c(paste0(n_rg - n_read, " skipped"), paste0(n_read, " read")),
  group = c("skipped", "read")
)

halo_layers <- function(key, radii = seq(2, 12, by = 2), n = 16, alpha = 0.07) {
  a <- seq(0, 2 * pi, length.out = n + 1L)[-1L]
  unlist(lapply(radii, function(r) lapply(a, function(t)
    geom_text(data = transform(key, x = x + r * cos(t), y = y + r * sin(t)),
              aes(x, y, label = label), colour = "white", alpha = alpha, size = 11,
              fontface = "bold", hjust = 0.5))),
    recursive = FALSE)
}

p <- ggplot() +
  geom_polygon(data = blocks[!blocks$read, ], aes(x, y, group = id),
               fill = alpha(blue, 0.10), colour = alpha(blue, 0.35), linewidth = 0.5) +
  geom_polygon(data = blocks[blocks$read, ], aes(x, y, group = id),
               fill = alpha(blue, 0.78), colour = NA) +
  halo_layers(key) +
  geom_text(data = key, aes(x, y, label = label, colour = group), size = 11,
            fontface = "bold", hjust = 0.5) +
  scale_colour_manual(values = c(skipped = grey, read = blue), guide = "none") +
  coord_equal(xlim = c(0, W), ylim = c(0, H), expand = FALSE) +
  theme_void() +
  theme(plot.background = element_rect(fill = "white", colour = NA),
        plot.margin = margin(0, 0, 0, 0))

dir.create("man/figures", recursive = TRUE, showWarnings = FALSE)
ggsave("man/figures/vectra-hero.png", p, width = W / 120, height = H / 120, dpi = 120,
       bg = "white", device = ragg::agg_png)
