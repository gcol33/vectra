# Builds man/figures/logo.svg: columns of row groups drifting upward through the hex.
# Filled = row group read, teal = row group selected by a predicate, outline = empty.
# Each column repeats every PERIOD rows, so a drift of PERIOD rows is an exact loop.

pitch_x <- 70
pitch_y <- 44
w <- 56
h <- 32
ncol <- 7L
period <- 4L
x0 <- 260 - ncol * pitch_x / 2 + (pitch_x - w) / 2

pattern <- rbind(
  c(1, 0, 1, 0),
  c(0, 1, 1, 0),
  c(1, 0, 0, 2),
  c(0, 2, 1, 0),
  c(1, 1, 0, 0),
  c(0, 0, 1, 2),
  c(1, 0, 1, 0)
)

blue <- "#2E6F9E"
teal <- "#2A7A7A"
paper <- "#F5F6F8"
border <- "#2B3A4A"

hex_points <- function(r) {
  a <- (-90 + 60 * 0:5) * pi / 180
  paste(sprintf("%.1f,%.1f", 260 + r * cos(a), 300 + r * sin(a)), collapse = " ")
}

block <- function(x, y, k) {
  if (k == 0) {
    sprintf(
      '<rect x="%g" y="%g" width="%g" height="%g" rx="7" fill="%s" fill-opacity=".10" stroke="%s" stroke-opacity=".35" stroke-width="2.5"/>',
      x, y, w, h, blue, blue
    )
  } else {
    sprintf(
      '<rect x="%g" y="%g" width="%g" height="%g" rx="7" fill="%s" fill-opacity="%s"/>',
      x, y, w, h, if (k == 1) blue else teal, if (k == 1) ".78" else ".95"
    )
  }
}

blocks <- function(y_start, y_end) {
  ys <- seq(y_start, y_end - 1e-9, by = pitch_y)
  out <- character()
  for (c in seq_len(ncol)) {
    for (r in seq_along(ys)) {
      k <- pattern[c, ((r - 1L) %% period) + 1L]
      out <- c(out, block(x0 + (c - 1L) * pitch_x, ys[r], k))
    }
  }
  paste(out, collapse = "\n")
}

shift <- period * pitch_y
y_top <- -shift - pitch_y
y_bot <- 470

svg <- sprintf('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 520 600" role="img" aria-label="vectra logo">
<title>vectra logo</title>
<defs>
<clipPath id="clip"><polygon points="%s"/></clipPath>
<linearGradient id="fade" x1="0" y1="0" x2="0" y2="600" gradientUnits="userSpaceOnUse">
<stop offset="0.52" stop-color="#fff"/><stop offset="0.70" stop-color="#000"/>
</linearGradient>
<mask id="mask" maskUnits="userSpaceOnUse" x="0" y="0" width="520" height="600">
<rect width="520" height="600" fill="url(#fade)"/>
</mask>
</defs>
<style>
.still { display: none; }
@media (prefers-reduced-motion: reduce) { .moving { display: none; } .still { display: inline; } }
</style>
<polygon points="%s" fill="%s"/>
<g clip-path="url(#clip)">
<g mask="url(#mask)">
<g class="moving">
<g>
<animateTransform attributeName="transform" type="translate" from="0 0" to="0 %g" dur="12s" repeatCount="indefinite"/>
%s
</g>
</g>
<g class="still">
%s
</g>
</g>
</g>
<text x="260" y="472" text-anchor="middle" font-family="Georgia, \'Times New Roman\', serif" font-weight="700" font-style="italic" font-size="78" fill="%s" stroke="%s" stroke-width="10" paint-order="stroke" stroke-linejoin="round">vectra</text>
<polygon points="%s" fill="none" stroke="%s" stroke-width="18" stroke-linejoin="round"/>
</svg>
', hex_points(288), hex_points(288), paper, shift,
  blocks(y_top, y_bot), blocks(y_top, y_bot),
  blue, paper, hex_points(291), border)

dir.create("man/figures", recursive = TRUE, showWarnings = FALSE)
writeLines(svg, "man/figures/logo.svg", useBytes = TRUE)
