/*
 * warp.c — resample/reproject sampler kernel over a VECR raster.
 *
 * The R driver (warp() in R/spatial.R) walks the OUTPUT grid one tile-row
 * strip at a time. For each strip it builds the target pixel-centre
 * coordinates, projects them into the source CRS (sf::sf_project when the two
 * CRSs differ), maps them through the source inverse geotransform to
 * fractional source pixel coordinates, reads the bounded source window those
 * coordinates fall in, and hands the window plus the per-pixel source
 * coordinates here. This kernel does only the sampling, so projection stays in
 * PROJ (via sf) and the raster interpolation stays native C.
 *
 * Coordinate convention. `sx`/`sy` are source EDGE coordinates in the full
 * source grid (0 = the left/top edge of pixel 0, so j + 0.5 is the centre of
 * source column j). The window's top-left sample win[0,0] is full-source pixel
 * (origin_col, origin_row). A tap whose source pixel lies outside the read
 * window is a real source edge and samples as NA; any NA tap makes the output
 * pixel NA (the conservative terra::project border behaviour).
 */

#include <R.h>
#include <Rinternals.h>

#include <math.h>
#include <stddef.h>

#include "vec_omp.h"

#define WARP_NEAR     0
#define WARP_BILINEAR 1
#define WARP_CUBIC    2

/* Catmull-Rom (cubic convolution, a = -0.5) across four samples. */
static inline double cubic1(double pm1, double p0, double p1, double p2,
                            double f) {
    return 0.5 * (2.0 * p0
                  + (-pm1 + p1) * f
                  + (2.0 * pm1 - 5.0 * p0 + 4.0 * p1 - p2) * f * f
                  + (-pm1 + 3.0 * p0 - 3.0 * p1 + p2) * f * f * f);
}

/*
 * C_warp_strip(win, win_dims, origin, sx, sy, method, out_dims)
 *
 *   win      : REALSXP matrix win_h x win_w (column-major; NA = nodata)
 *   win_dims : integer c(win_h, win_w)
 *   origin   : integer c(origin_col, origin_row) — 0-based full-source pixel
 *              coordinates of win[0,0]
 *   sx, sy   : REALSXP length out_h*out_w, fractional source edge coordinates
 *              in the full source grid, column-major (output row varies
 *              fastest); NA where the target coordinate did not project
 *   method   : integer WARP_* code
 *   out_dims : integer c(out_h, out_w)
 *
 * Returns a REALSXP matrix out_h x out_w (column-major).
 */
SEXP C_warp_strip(SEXP win_sexp, SEXP win_dims_sexp, SEXP origin_sexp,
                  SEXP sx_sexp, SEXP sy_sexp, SEXP method_sexp,
                  SEXP out_dims_sexp) {
    const double *win = REAL(win_sexp);
    int win_h = INTEGER(win_dims_sexp)[0];
    int win_w = INTEGER(win_dims_sexp)[1];
    int col0  = INTEGER(origin_sexp)[0];
    int row0  = INTEGER(origin_sexp)[1];
    const double *sx = REAL(sx_sexp);
    const double *sy = REAL(sy_sexp);
    int method = Rf_asInteger(method_sexp);
    int out_h  = INTEGER(out_dims_sexp)[0];
    int out_w  = INTEGER(out_dims_sexp)[1];
    R_xlen_t n = (R_xlen_t)out_h * out_w;

    SEXP out = PROTECT(Rf_allocMatrix(REALSXP, out_h, out_w));
    double *o = REAL(out);

    int parallel = (double)n > 20000.0;

    #ifdef _OPENMP
    #pragma omp parallel for if(parallel) schedule(static)
    #endif
    for (R_xlen_t k = 0; k < n; ++k) {
        double x = sx[k], y = sy[k];
        if (ISNAN(x) || ISNAN(y)) { o[k] = NA_REAL; continue; }

        /* Centre coordinates: integer u is the centre of a source column. */
        double u = x - 0.5, v = y - 0.5;
        double val = NA_REAL;

        if (method == WARP_NEAR) {
            int j = (int)floor(u + 0.5) - col0;
            int i = (int)floor(v + 0.5) - row0;
            if (j >= 0 && j < win_w && i >= 0 && i < win_h)
                val = win[(size_t)j * win_h + i];
        } else if (method == WARP_BILINEAR) {
            int j0 = (int)floor(u), i0 = (int)floor(v);
            double fx = u - j0, fy = v - i0;
            int lj = j0 - col0, li = i0 - row0;
            if (lj >= 0 && lj + 1 < win_w && li >= 0 && li + 1 < win_h) {
                double a = win[(size_t)lj       * win_h + li];
                double b = win[(size_t)(lj + 1) * win_h + li];
                double c = win[(size_t)lj       * win_h + li + 1];
                double d = win[(size_t)(lj + 1) * win_h + li + 1];
                if (!(ISNAN(a) || ISNAN(b) || ISNAN(c) || ISNAN(d))) {
                    double top = a + (b - a) * fx;
                    double bot = c + (d - c) * fx;
                    val = top + (bot - top) * fy;
                }
            }
        } else {  /* WARP_CUBIC */
            int j0 = (int)floor(u), i0 = (int)floor(v);
            double fx = u - j0, fy = v - i0;
            int lj = j0 - col0, li = i0 - row0;
            if (lj - 1 >= 0 && lj + 2 < win_w &&
                li - 1 >= 0 && li + 2 < win_h) {
                double rows4[4];
                int ok = 1;
                for (int rr = -1; rr <= 2 && ok; ++rr) {
                    const double *col = win + (size_t)(li + rr);
                    double p[4];
                    for (int cc = -1; cc <= 2; ++cc) {
                        double s = col[(size_t)(lj + cc) * win_h];
                        if (ISNAN(s)) { ok = 0; break; }
                        p[cc + 1] = s;
                    }
                    if (ok) rows4[rr + 1] = cubic1(p[0], p[1], p[2], p[3], fx);
                }
                if (ok)
                    val = cubic1(rows4[0], rows4[1], rows4[2], rows4[3], fy);
            }
        }
        o[k] = val;
    }

    UNPROTECT(1);
    return out;
}
