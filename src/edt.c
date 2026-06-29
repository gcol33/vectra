/*
 * edt.c — exact Euclidean distance transform kernel over raster strips.
 *
 * The separable distance transform (Felzenszwalb and Huttenlocher 2012) runs a
 * one-dimensional lower-envelope-of-parabolas transform along each line, linear
 * in the line length. C_edt_strip applies it to every row of a column-major
 * strip; the R driver (proximity() in R/spatial_more.R) composes a row pass, an
 * out-of-core transpose, a second row pass, and a transpose back to obtain the
 * full two-dimensional transform without ever holding the whole grid resident.
 *
 * The 1D transform takes a sampled cost f over n points with physical spacing
 * `scale` between adjacent samples and returns, at each point q, the minimum of
 * scale^2 * (q - q')^2 + f[q'] over all q'. A finite sentinel marks "no feature
 * here": the squared distance between distant points stays well inside double
 * range for realistic grids, so finite arithmetic replaces infinity in the
 * parabola intersections.
 */

#include <R.h>
#include <Rinternals.h>

#include <math.h>
#include <stddef.h>
#include <stdlib.h>

#include "vec_omp.h"

/* One line of the lower-envelope transform. `f` holds n sampled costs, `s2` is
 * the squared physical spacing. `d` receives the transformed line; `v` and `z`
 * are caller-supplied workspace of length n and n + 1. */
static void edt1d(const double *f, int n, double s2,
                  double *d, int *v, double *z) {
    int k = 0;
    v[0] = 0;
    z[0] = -INFINITY;
    z[1] = INFINITY;
    for (int q = 1; q < n; ++q) {
        double s;
        for (;;) {
            double num = (f[q] + s2 * (double)q * q) -
                         (f[v[k]] + s2 * (double)v[k] * v[k]);
            double den = 2.0 * s2 * (double)(q - v[k]);
            s = num / den;
            if (k > 0 && s <= z[k]) { --k; continue; }
            break;
        }
        ++k;
        v[k] = q;
        z[k] = s;
        z[k + 1] = INFINITY;
    }
    k = 0;
    for (int q = 0; q < n; ++q) {
        while (z[k + 1] < (double)q) ++k;
        double dq = (double)(q - v[k]);
        d[q] = s2 * dq * dq + f[v[k]];
    }
}

/*
 * C_edt_strip(mat, dims, scale)
 *
 *   mat   : REALSXP matrix in_h x W (column-major)
 *   dims  : integer c(in_h, W)
 *   scale : numeric(1) physical spacing between adjacent columns
 *
 * Transforms every row of the strip (a raster row, fixed y over varying x):
 * for each row, the lower-envelope transform of the W column costs with spacing
 * `scale`. Returns a REALSXP matrix in_h x W (column-major).
 */
SEXP C_edt_strip(SEXP mat_sexp, SEXP dims_sexp, SEXP scale_sexp) {
    const double *in = REAL(mat_sexp);
    int in_h = INTEGER(dims_sexp)[0];
    int W    = INTEGER(dims_sexp)[1];
    double scale = Rf_asReal(scale_sexp);
    double s2 = scale * scale;

    SEXP out = PROTECT(Rf_allocMatrix(REALSXP, in_h, W));
    double *o = REAL(out);

    int parallel = (double)in_h * W > 20000.0;

    #ifdef _OPENMP
    #pragma omp parallel if(parallel)
    #endif
    {
        double *f = (double *)malloc((size_t)W * sizeof(double));
        double *d = (double *)malloc((size_t)W * sizeof(double));
        int    *v = (int    *)malloc((size_t)W * sizeof(int));
        double *z = (double *)malloc(((size_t)W + 1) * sizeof(double));
        if (f && d && v && z) {
            #ifdef _OPENMP
            #pragma omp for schedule(static)
            #endif
            for (int i = 0; i < in_h; ++i) {
                for (int c = 0; c < W; ++c) f[c] = in[(size_t)c * in_h + i];
                edt1d(f, W, s2, d, v, z);
                for (int c = 0; c < W; ++c) o[(size_t)c * in_h + i] = d[c];
            }
        }
        free(f); free(d); free(v); free(z);
    }

    UNPROTECT(1);
    return out;
}
