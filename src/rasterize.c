/*
 * rasterize.c — streaming vector-to-raster accumulator.
 *
 * The monoid-fold behind rasterize(): a fixed grid is held resident in RAM
 * while an arbitrarily large point stream flows past one batch at a time. Each
 * point's (x, y) is mapped to a cell via the raster's north-up geotransform and
 * the per-cell accumulator is updated. The grid fits in memory; the points do
 * not.
 *
 * The R side drives the batch cursor and calls three entry points across the
 * stream:
 *   C_rasterize_new(dims, gt, fun)  -> externalptr holding the grid state
 *   C_rasterize_push(ptr, x, y, v)  -> fold one batch into the grid
 *   C_rasterize_finish(ptr, bg)     -> the grid as an R matrix, background-filled
 *
 * Two double grids back every reduction: `cnt` counts the contributing points
 * per cell (so an untouched cell is exactly cnt == 0, distinct from a cell that
 * legitimately summed to zero) and `acc` carries the running sum / min / max.
 * count needs only cnt; the rest need both. mean is acc / cnt at finish.
 */

#include <R.h>
#include <Rinternals.h>

#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "error.h"

/* Reduction codes — kept in lockstep with the R front door's fun switch. */
enum {
    RAST_COUNT = 0,
    RAST_SUM   = 1,
    RAST_MEAN  = 2,
    RAST_MIN   = 3,
    RAST_MAX   = 4
};

typedef struct {
    int      width;     /* cells across (cols) */
    int      height;    /* cells down (rows) */
    double   gt[6];     /* GDAL geotransform; north-up (gt[2]=gt[4]=0) assumed */
    int      fun;       /* RAST_* reduction */
    double  *acc;       /* running sum/min/max, NULL in count mode */
    double  *cnt;       /* contributing-point count per cell */
} RasterAcc;

/* ---------- xptr management --------------------------------------------- */

static void rasterize_xptr_finalize(SEXP x) {
    RasterAcc *a = (RasterAcc *)R_ExternalPtrAddr(x);
    if (a) {
        free(a->acc);
        free(a->cnt);
        free(a);
        R_ClearExternalPtr(x);
    }
}

static RasterAcc *unwrap_rasterize(SEXP xptr) {
    if (TYPEOF(xptr) != EXTPTRSXP)
        Rf_error("expected an external pointer to a rasterize accumulator");
    RasterAcc *a = (RasterAcc *)R_ExternalPtrAddr(xptr);
    if (!a) Rf_error("rasterize accumulator is closed");
    return a;
}

/* ---------- C_rasterize_new --------------------------------------------- */

/*
 * C_rasterize_new(dims, gt, fun)
 *   dims : integer(2) = c(width, height)
 *   gt   : numeric(6) GDAL geotransform (north-up; rotation terms ignored)
 *   fun  : integer(1) RAST_* code
 */
SEXP C_rasterize_new(SEXP dims_sexp, SEXP gt_sexp, SEXP fun_sexp) {
    int *dims = INTEGER(dims_sexp);
    int width  = dims[0];
    int height = dims[1];
    if (width <= 0 || height <= 0)
        vectra_error("rasterize: grid dimensions must be positive");
    if (TYPEOF(gt_sexp) != REALSXP || Rf_xlength(gt_sexp) != 6)
        vectra_error("rasterize: gt must be numeric(6)");
    const double *gt = REAL(gt_sexp);
    if (gt[1] == 0.0 || gt[5] == 0.0)
        vectra_error("rasterize: geotransform has a zero pixel size");

    int fun = Rf_asInteger(fun_sexp);
    if (fun < RAST_COUNT || fun > RAST_MAX)
        vectra_error("rasterize: unknown reduction code %d", fun);

    int64_t n = (int64_t)width * (int64_t)height;

    RasterAcc *a = (RasterAcc *)calloc(1, sizeof(RasterAcc));
    if (!a) vectra_error("rasterize: alloc failed");
    a->width = width; a->height = height; a->fun = fun;
    memcpy(a->gt, gt, 6 * sizeof(double));

    a->cnt = (double *)calloc((size_t)n, sizeof(double));
    if (!a->cnt) { free(a); vectra_error("rasterize: grid alloc failed"); }
    if (fun != RAST_COUNT) {
        a->acc = (double *)calloc((size_t)n, sizeof(double));
        if (!a->acc) { free(a->cnt); free(a);
                       vectra_error("rasterize: grid alloc failed"); }
    }

    SEXP tag  = PROTECT(Rf_install("rasterize_acc"));
    SEXP xptr = PROTECT(R_MakeExternalPtr(a, tag, R_NilValue));
    R_RegisterCFinalizerEx(xptr, rasterize_xptr_finalize, TRUE);
    UNPROTECT(2);
    return xptr;
}

/* ---------- C_rasterize_push -------------------------------------------- */

/*
 * C_rasterize_push(ptr, x, y, value)
 *   x, y  : numeric vectors of equal length (one point each)
 *   value : numeric vector of the same length, or NULL in count mode
 *
 * A point outside the grid, or with a non-finite coordinate, is skipped. In
 * value-bearing modes a point with NA value is skipped too (na.rm semantics).
 */
SEXP C_rasterize_push(SEXP ptr_sexp, SEXP x_sexp, SEXP y_sexp, SEXP val_sexp) {
    RasterAcc *a = unwrap_rasterize(ptr_sexp);
    int64_t n = Rf_xlength(x_sexp);
    if (Rf_xlength(y_sexp) != n)
        vectra_error("rasterize: x and y length mismatch");
    const double *x = REAL(x_sexp);
    const double *y = REAL(y_sexp);

    const double *v = NULL;
    if (a->fun != RAST_COUNT) {
        if (val_sexp == R_NilValue)
            vectra_error("rasterize: a field is required for this reduction");
        if (Rf_xlength(val_sexp) != n)
            vectra_error("rasterize: value length must match coordinates");
        v = REAL(val_sexp);
    }

    int     w  = a->width;
    int     h  = a->height;
    double  ox = a->gt[0], px = a->gt[1];   /* origin x, pixel width  */
    double  oy = a->gt[3], py = a->gt[5];   /* origin y, pixel height (<0) */
    double *cnt = a->cnt;
    double *acc = a->acc;

    for (int64_t i = 0; i < n; ++i) {
        double xi = x[i], yi = y[i];
        if (!R_FINITE(xi) || !R_FINITE(yi)) continue;
        double fc = (xi - ox) / px;
        double fr = (yi - oy) / py;
        if (fc < 0.0 || fr < 0.0) continue;
        int64_t col = (int64_t)floor(fc);
        int64_t row = (int64_t)floor(fr);
        if (col >= w || row >= h) continue;
        int64_t k = row * (int64_t)w + col;

        switch (a->fun) {
        case RAST_COUNT:
            cnt[k] += 1.0;
            break;
        case RAST_SUM:
        case RAST_MEAN: {
            double vi = v[i];
            if (ISNAN(vi)) break;
            acc[k] += vi;
            cnt[k] += 1.0;
            break;
        }
        case RAST_MIN: {
            double vi = v[i];
            if (ISNAN(vi)) break;
            if (cnt[k] == 0.0 || vi < acc[k]) acc[k] = vi;
            cnt[k] += 1.0;
            break;
        }
        case RAST_MAX: {
            double vi = v[i];
            if (ISNAN(vi)) break;
            if (cnt[k] == 0.0 || vi > acc[k]) acc[k] = vi;
            cnt[k] += 1.0;
            break;
        }
        }
    }
    return R_NilValue;
}

/* ---------- C_rasterize_finish ------------------------------------------ */

/*
 * C_rasterize_finish(ptr, background) -> numeric matrix c(height, width)
 *
 * Row 1 is the northernmost row (matching vec_write_raster's and terra's
 * top-of-y-axis convention). Untouched cells (cnt == 0) take the background
 * value; touched cells take count / sum / mean / min / max.
 */
SEXP C_rasterize_finish(SEXP ptr_sexp, SEXP bg_sexp) {
    RasterAcc *a = unwrap_rasterize(ptr_sexp);
    int w = a->width;
    int h = a->height;
    double bg = Rf_asReal(bg_sexp);
    double *cnt = a->cnt;
    double *acc = a->acc;
    int fun = a->fun;

    SEXP out = PROTECT(Rf_allocMatrix(REALSXP, h, w));
    double *dst = REAL(out);

    /* Internal grids are row-major (k = row*w + col); R matrices are
     * column-major (dst[col*h + row]). Transpose during the copy. */
    for (int64_t row = 0; row < h; ++row) {
        for (int64_t col = 0; col < w; ++col) {
            int64_t k = row * (int64_t)w + col;
            double out_v;
            if (cnt[k] == 0.0) {
                out_v = bg;
            } else {
                switch (fun) {
                case RAST_COUNT: out_v = cnt[k];          break;
                case RAST_SUM:   out_v = acc[k];          break;
                case RAST_MEAN:  out_v = acc[k] / cnt[k]; break;
                default:         out_v = acc[k];          break; /* min/max */
                }
            }
            dst[col * (int64_t)h + row] = out_v;
        }
    }
    UNPROTECT(1);
    return out;
}
