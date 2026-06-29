/*
 * focal.c — moving-window (focal) and DEM-derivative (terrain) kernels over
 * VECR raster strips, plus a streaming tile-row writer bridge.
 *
 * The R driver (focal() / terrain() in R/spatial.R) reads the input one
 * haloed tile-row strip at a time via vec_read_window (so the whole input
 * grid is never resident), hands each strip to the kernels here, and either
 * assembles an in-memory matrix or streams the output tile-row to a .vec via
 * the writer bridge — never holding the whole output band either.
 *
 * Strip geometry. An input strip is a column-major R matrix of in_h x W
 * doubles (NA = nodata). `top` is the number of halo rows above the first
 * output row (0 at the raster's north edge). Output local row i maps to input
 * strip row `top + i`; a window neighbour outside [0, in_h) x [0, W) is a real
 * raster edge and is treated as NA.
 */

#include <R.h>
#include <Rinternals.h>
#include "vec_raster.h"
#include "error.h"

#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "vec_omp.h"

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

/* fun codes shared with R/spatial.R */
#define FOCAL_SUM    0
#define FOCAL_MEAN   1
#define FOCAL_MIN    2
#define FOCAL_MAX    3
#define FOCAL_SD     4
#define FOCAL_MEDIAN 5

static int dbl_cmp(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

/* Reduce the gathered in-window values/weights to one statistic. `vals` holds
 * the `m` non-NA values (for min/max/sd/median), `sw`/`swv` the running weight
 * and weighted-value sums (for sum/mean). `tainted` is set when na.rm = FALSE
 * met an NA inside the window. */
static double focal_reduce(int fun, double *vals, int m,
                           double sw, double swv, int tainted) {
    if (tainted) return NA_REAL;
    if (m == 0) return NA_REAL;
    switch (fun) {
    case FOCAL_SUM:  return swv;
    case FOCAL_MEAN: return sw != 0.0 ? swv / sw : NA_REAL;
    case FOCAL_MIN: {
        double r = vals[0];
        for (int i = 1; i < m; ++i) if (vals[i] < r) r = vals[i];
        return r;
    }
    case FOCAL_MAX: {
        double r = vals[0];
        for (int i = 1; i < m; ++i) if (vals[i] > r) r = vals[i];
        return r;
    }
    case FOCAL_SD: {
        if (m < 2) return NA_REAL;
        double mean = 0.0;
        for (int i = 0; i < m; ++i) mean += vals[i];
        mean /= m;
        double ss = 0.0;
        for (int i = 0; i < m; ++i) { double d = vals[i] - mean; ss += d * d; }
        return sqrt(ss / (m - 1));
    }
    case FOCAL_MEDIAN: {
        qsort(vals, m, sizeof(double), dbl_cmp);
        return (m & 1) ? vals[m / 2]
                       : 0.5 * (vals[m / 2 - 1] + vals[m / 2]);
    }
    }
    return NA_REAL;
}

/*
 * C_focal_strip(in, dims, w, kdims, fun, na_rm, top, out_h)
 *
 *   in    : REALSXP matrix in_h x W (column-major; NA = nodata)
 *   dims  : integer c(in_h, W)
 *   w     : REALSXP length kh*kw, row-major (north row first); NA = outside
 *           the window. For sum/mean the magnitude is the weight; for
 *           min/max/sd/median a finite weight only marks membership.
 *   kdims : integer c(kh, kw) (both odd)
 *   fun   : integer FOCAL_* code
 *   na_rm : logical(1)
 *   top   : integer halo rows above the first output row
 *   out_h : integer number of output rows
 *
 * Returns a REALSXP matrix out_h x W (column-major).
 */
SEXP C_focal_strip(SEXP in_sexp, SEXP dims_sexp, SEXP w_sexp, SEXP kdims_sexp,
                   SEXP fun_sexp, SEXP na_rm_sexp, SEXP top_sexp,
                   SEXP out_h_sexp) {
    const double *in = REAL(in_sexp);
    int in_h = INTEGER(dims_sexp)[0];
    int W    = INTEGER(dims_sexp)[1];
    const double *w = REAL(w_sexp);
    int kh = INTEGER(kdims_sexp)[0];
    int kw = INTEGER(kdims_sexp)[1];
    int fun   = Rf_asInteger(fun_sexp);
    int na_rm = Rf_asLogical(na_rm_sexp);
    int top   = Rf_asInteger(top_sexp);
    int out_h = Rf_asInteger(out_h_sexp);
    int radh = (kh - 1) / 2;
    int radw = (kw - 1) / 2;

    SEXP out = PROTECT(Rf_allocMatrix(REALSXP, out_h, W));
    double *o = REAL(out);

    int parallel = (double)out_h * W * kh * kw > 50000.0;

    #ifdef _OPENMP
    #pragma omp parallel for if(parallel) schedule(static)
    #endif
    for (int i = 0; i < out_h; ++i) {
        double *vals = (double *)malloc((size_t)kh * kw * sizeof(double));
        int ir0 = top + i;
        for (int c = 0; c < W; ++c) {
            int m = 0, tainted = 0;
            double sw = 0.0, swv = 0.0;
            for (int wr = 0; wr < kh; ++wr) {
                int ir = ir0 + (wr - radh);
                for (int wc = 0; wc < kw; ++wc) {
                    double wgt = w[wr * kw + wc];
                    if (ISNAN(wgt)) continue;             /* outside the window */
                    int ic = c + (wc - radw);
                    double v;
                    if (ir < 0 || ir >= in_h || ic < 0 || ic >= W)
                        v = NA_REAL;                      /* raster edge */
                    else
                        v = in[(size_t)ic * in_h + ir];
                    if (ISNAN(v)) {
                        if (!na_rm) { tainted = 1; }
                        continue;
                    }
                    vals[m++] = v;
                    sw  += wgt;
                    swv += wgt * v;
                }
            }
            o[(size_t)c * out_h + i] = focal_reduce(fun, vals, m, sw, swv, tainted);
        }
        free(vals);
    }

    UNPROTECT(1);
    return out;
}

/* terrain derivative codes shared with R/spatial.R */
#define TER_SLOPE     0
#define TER_ASPECT    1
#define TER_HILLSHADE 2
#define TER_TPI       3
#define TER_ROUGHNESS 4
#define TER_TRI       5

/*
 * C_terrain_strip(in, dims, which, top, out_h, res, unit, sun)
 *
 *   in    : REALSXP matrix in_h x W (column-major; NA = nodata)
 *   dims  : integer c(in_h, W)
 *   which : integer vector of TER_* codes, length nout
 *   top   : integer halo rows above the first output row
 *   out_h : integer number of output rows
 *   res   : numeric c(xres, yres) cell sizes (positive)
 *   unit  : integer 0 = degrees, 1 = radians (slope/aspect)
 *   sun   : numeric c(azimuth_deg, altitude_deg) for hillshade
 *
 * Horn's 3x3 method (matches terra::terrain / terra::shade). A cell with any
 * NA in its 3x3 neighbourhood (including raster edges) yields NA for every
 * derivative, matching terra's border handling.
 *
 * Returns a REALSXP matrix out_h x (W*nout); derivative k occupies columns
 * [k*W, (k+1)*W).
 */
SEXP C_terrain_strip(SEXP in_sexp, SEXP dims_sexp, SEXP which_sexp,
                     SEXP top_sexp, SEXP out_h_sexp, SEXP res_sexp,
                     SEXP unit_sexp, SEXP sun_sexp) {
    const double *in = REAL(in_sexp);
    int in_h = INTEGER(dims_sexp)[0];
    int W    = INTEGER(dims_sexp)[1];
    const int *which = INTEGER(which_sexp);
    int nout = Rf_length(which_sexp);
    int top   = Rf_asInteger(top_sexp);
    int out_h = Rf_asInteger(out_h_sexp);
    double xres = REAL(res_sexp)[0];
    double yres = REAL(res_sexp)[1];
    int unit = Rf_asInteger(unit_sexp);
    double az  = REAL(sun_sexp)[0] * M_PI / 180.0;
    double alt = REAL(sun_sexp)[1] * M_PI / 180.0;
    double zenith = M_PI / 2.0 - alt;
    double to_deg = 180.0 / M_PI;

    SEXP out = PROTECT(Rf_allocMatrix(REALSXP, out_h, W * nout));
    double *o = REAL(out);

    int parallel = (double)out_h * W > 20000.0;

    #ifdef _OPENMP
    #pragma omp parallel for if(parallel) schedule(static)
    #endif
    for (int i = 0; i < out_h; ++i) {
        int rc = top + i;
        for (int c = 0; c < W; ++c) {
            /* Gather the 3x3 neighbourhood: row index increases southward. */
            double nb[9]; int ok = 1;
            for (int dr = -1; dr <= 1 && ok; ++dr) {
                int ir = rc + dr;
                for (int dc = -1; dc <= 1; ++dc) {
                    int ic = c + dc;
                    double v;
                    if (ir < 0 || ir >= in_h || ic < 0 || ic >= W)
                        v = NA_REAL;
                    else
                        v = in[(size_t)ic * in_h + ir];
                    if (ISNAN(v)) { ok = 0; break; }
                    nb[(dr + 1) * 3 + (dc + 1)] = v;
                }
            }
            double za, zb, zcc, zd, ze, zf, zg, zh, zi;
            double zx = 0, zy = 0, slope = 0, aspect = 0;
            int gradient_done = 0;
            if (ok) {
                za = nb[0]; zb = nb[1]; zcc = nb[2];   /* NW N NE */
                zd = nb[3]; ze = nb[4]; zf  = nb[5];   /* W  C E  */
                zg = nb[6]; zh = nb[7]; zi  = nb[8];   /* SW S SE */
            } else {
                za = zb = zcc = zd = ze = zf = zg = zh = zi = NA_REAL;
            }
            for (int k = 0; k < nout; ++k) {
                double val = NA_REAL;
                if (ok) {
                    switch (which[k]) {
                    case TER_SLOPE:
                    case TER_ASPECT:
                    case TER_HILLSHADE:
                        if (!gradient_done) {
                            zx = ((zcc + 2*zf + zi) - (za + 2*zd + zg)) / (8.0 * xres);
                            zy = ((zg + 2*zh + zi) - (za + 2*zb + zcc)) / (8.0 * yres);
                            slope = atan(sqrt(zx * zx + zy * zy));
                            aspect = to_deg * atan2(zx, zy);   /* deg from south */
                            aspect = fmod(360.0 - aspect, 360.0);
                            if (aspect < 0.0) aspect += 360.0;
                            if (zx == 0.0 && zy == 0.0) aspect = 90.0;  /* flat */
                            gradient_done = 1;
                        }
                        if (which[k] == TER_SLOPE) {
                            val = (unit == 1) ? slope : slope * to_deg;
                        } else if (which[k] == TER_ASPECT) {
                            val = (unit == 1) ? aspect * M_PI / 180.0 : aspect;
                        } else {  /* hillshade */
                            double ar = aspect * M_PI / 180.0;
                            double hs = cos(zenith) * cos(slope) +
                                        sin(zenith) * sin(slope) * cos(az - ar);
                            val = hs < 0.0 ? 0.0 : hs;
                        }
                        break;
                    case TER_TPI:
                        val = ze - (za + zb + zcc + zd + zf + zg + zh + zi) / 8.0;
                        break;
                    case TER_ROUGHNESS: {
                        double mn = za, mx = za;
                        double vv[9] = {za, zb, zcc, zd, ze, zf, zg, zh, zi};
                        for (int t = 1; t < 9; ++t) {
                            if (vv[t] < mn) mn = vv[t];
                            if (vv[t] > mx) mx = vv[t];
                        }
                        val = mx - mn;
                        break;
                    }
                    case TER_TRI: {
                        double s = fabs(ze - za) + fabs(ze - zb) + fabs(ze - zcc) +
                                   fabs(ze - zd) + fabs(ze - zf) + fabs(ze - zg) +
                                   fabs(ze - zh) + fabs(ze - zi);
                        val = s / 8.0;
                        break;
                    }
                    }
                }
                o[((size_t)k * W + c) * out_h + i] = val;
            }
        }
    }

    UNPROTECT(1);
    return out;
}

/* ====================================================================== */
/*  Streaming tile-row writer bridge                                       */
/* ====================================================================== */

static void vecr_writer_xptr_finalize(SEXP x) {
    VecrWriter *w = (VecrWriter *)R_ExternalPtrAddr(x);
    if (w) {
        vecr_writer_close(w);
        R_ClearExternalPtr(x);
    }
}

static VecrWriter *unwrap_vecr_writer(SEXP x) {
    if (TYPEOF(x) != EXTPTRSXP)
        Rf_error("expected an external pointer to a vecr_writer");
    VecrWriter *w = (VecrWriter *)R_ExternalPtrAddr(x);
    if (!w) Rf_error("vecr_writer handle is closed");
    return w;
}

/*
 * C_vecr_writer_open(path, dims, dtype, tile_size, gt, epsg, nodata,
 *                    band_names, compression) -> externalptr
 *
 *   dims : integer c(width, height, n_bands)
 */
SEXP C_vecr_writer_open(SEXP path_sexp, SEXP dims_sexp, SEXP dtype_sexp,
                        SEXP tile_size_sexp, SEXP gt_sexp, SEXP epsg_sexp,
                        SEXP nodata_sexp, SEXP band_names_sexp,
                        SEXP compression_sexp) {
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    int *dims = INTEGER(dims_sexp);
    int64_t width  = (int64_t)dims[0];
    int64_t height = (int64_t)dims[1];
    int     n_bands = dims[2];

    uint8_t dt = vecr_dtype_from_string(CHAR(STRING_ELT(dtype_sexp, 0)));
    if (dt == 0) vectra_error("vecr_writer_open: unknown dtype '%s'",
                              CHAR(STRING_ELT(dtype_sexp, 0)));
    uint16_t tile_size = (uint16_t)Rf_asInteger(tile_size_sexp);
    int32_t  epsg = (int32_t)Rf_asInteger(epsg_sexp);
    double   nodata = Rf_asReal(nodata_sexp);

    const double *gt = NULL;
    if (TYPEOF(gt_sexp) == REALSXP && Rf_xlength(gt_sexp) == 6)
        gt = REAL(gt_sexp);

    char **band_names = NULL;
    if (TYPEOF(band_names_sexp) == STRSXP &&
        Rf_xlength(band_names_sexp) == n_bands) {
        band_names = (char **)calloc((size_t)n_bands, sizeof(char *));
        if (!band_names) vectra_error("alloc failed for band names");
        for (int i = 0; i < n_bands; ++i)
            band_names[i] = (char *)CHAR(STRING_ELT(band_names_sexp, i));
    }

    VecrWriter *w = NULL;
    if (vecr_writer_open(path, width, height, n_bands, tile_size, dt,
                         gt, epsg, nodata,
                         (const char *const *)band_names, &w) != 0) {
        const char *m = w ? vecr_writer_errmsg(w) : "open failed";
        vecr_writer_close(w); free(band_names);
        vectra_error("vecr_writer_open: %s", m);
    }
    free(band_names);
    vecr_writer_set_compression(w, Rf_asInteger(compression_sexp));

    SEXP tag = PROTECT(Rf_install("vecr_writer"));
    SEXP xptr = PROTECT(R_MakeExternalPtr(w, tag, R_NilValue));
    R_RegisterCFinalizerEx(xptr, vecr_writer_xptr_finalize, TRUE);
    UNPROTECT(2);
    return xptr;
}

/*
 * C_vecr_writer_write_strip(ptr, band, ty, strip)
 *
 *   band  : integer(1) 1-based band index
 *   ty    : integer(1) 0-based tile-row index
 *   strip : REALSXP matrix strip_h x W (column-major; NA = nodata)
 *
 * Transposes the column-major strip to row-major, casts to the file dtype,
 * and emits the ty-th tile-row.
 */
SEXP C_vecr_writer_write_strip(SEXP ptr_sexp, SEXP band_sexp, SEXP ty_sexp,
                               SEXP strip_sexp) {
    VecrWriter *w = unwrap_vecr_writer(ptr_sexp);
    int band = Rf_asInteger(band_sexp) - 1;
    int64_t ty = (int64_t)Rf_asInteger(ty_sexp);

    SEXP d = Rf_getAttrib(strip_sexp, R_DimSymbol);
    if (d == R_NilValue || Rf_length(d) != 2)
        vectra_error("vecr_writer_write_strip: strip must be a matrix");
    int strip_h = INTEGER(d)[0];
    int W       = INTEGER(d)[1];
    const double *s = REAL(strip_sexp);

    /* Transpose the column-major strip to row-major doubles, then cast to the
     * file's sample dtype. */
    double *row = (double *)malloc((size_t)strip_h * W * sizeof(double));
    if (!row) vectra_error("alloc failed for strip transpose");
    for (int r = 0; r < strip_h; ++r)
        for (int c = 0; c < W; ++c)
            row[(size_t)r * W + c] = s[(size_t)c * strip_h + r];

    uint8_t fdt = vecr_writer_dtype(w);
    size_t esz = vecr_dtype_size(fdt);
    void *buf = malloc((size_t)strip_h * W * esz);
    if (!buf) { free(row); vectra_error("alloc failed for strip cast"); }
    vecr_cast_doubles_to_dtype(row, (int64_t)strip_h * W, fdt, buf);
    free(row);

    int rc = vecr_writer_write_tile_row(w, band, ty, buf, strip_h);
    free(buf);
    if (rc != 0)
        vectra_error("vecr_writer_write_strip: %s", vecr_writer_errmsg(w));
    return R_NilValue;
}

/* C_vecr_writer_finish(ptr) -> finalize index + close, clear the handle. */
SEXP C_vecr_writer_finish(SEXP ptr_sexp) {
    VecrWriter *w = unwrap_vecr_writer(ptr_sexp);
    int rc = vecr_writer_finish(w);
    const char *msg = rc != 0 ? vecr_writer_errmsg(w) : NULL;
    vecr_writer_close(w);
    R_ClearExternalPtr(ptr_sexp);
    if (rc != 0) vectra_error("vecr_writer_finish: %s", msg ? msg : "failed");
    return R_NilValue;
}
