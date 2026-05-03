/*
 * r_bridge_raster.c — R entry points for the VECR raster format.
 *
 * Four entry points (mirroring the Phase 1 R API):
 *   C_vec_write_raster  — write a single-band-or-multi-band matrix/array
 *   C_vec_open_raster   — open a .vec raster, return metadata + xptr handle
 *   C_vec_read_window   — decode a (col_min, row_min, col_max, row_max)
 *                         window of a chosen band into an R numeric matrix
 *   C_vec_extract_points— pointwise sampling, mirrors tiff_extract_points
 *
 * All sample dtype handling lives behind R: we accept R numeric (double)
 * matrices/arrays at the boundary and let the user pick a storage dtype
 * via a `dtype` string. The bridge converts to the storage representation
 * once before handing buffers to vec_raster.c.
 */

#include <R.h>
#include <Rinternals.h>
#include "vec_raster.h"
#include "error.h"

#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ---------- dtype string -> code ---------------------------------------- */

static uint8_t dtype_from_string(const char *s) {
    if (!s) return VECR_DT_F64;
    if (strcmp(s, "f64") == 0 || strcmp(s, "double")  == 0) return VECR_DT_F64;
    if (strcmp(s, "f32") == 0 || strcmp(s, "float")   == 0) return VECR_DT_F32;
    if (strcmp(s, "i8")  == 0)                              return VECR_DT_I8;
    if (strcmp(s, "u8")  == 0)                              return VECR_DT_U8;
    if (strcmp(s, "i16") == 0)                              return VECR_DT_I16;
    if (strcmp(s, "u16") == 0)                              return VECR_DT_U16;
    if (strcmp(s, "i32") == 0)                              return VECR_DT_I32;
    if (strcmp(s, "u32") == 0)                              return VECR_DT_U32;
    if (strcmp(s, "i64") == 0)                              return VECR_DT_I64;
    if (strcmp(s, "u64") == 0)                              return VECR_DT_U64;
    return 0;  /* invalid */
}

static const char *dtype_to_string(uint8_t dt) {
    switch (dt) {
    case VECR_DT_F64: return "f64";
    case VECR_DT_F32: return "f32";
    case VECR_DT_I8:  return "i8";
    case VECR_DT_U8:  return "u8";
    case VECR_DT_I16: return "i16";
    case VECR_DT_U16: return "u16";
    case VECR_DT_I32: return "i32";
    case VECR_DT_U32: return "u32";
    case VECR_DT_I64: return "i64";
    case VECR_DT_U64: return "u64";
    }
    return "unknown";
}

/* Cast a row-major double buffer into the target dtype's buffer.
 * dst must be allocated of size n * vecr_dtype_size(dt). */
static void cast_doubles_to_dtype(const double *src, int64_t n,
                                  uint8_t dt, void *dst) {
    if (dt == VECR_DT_F64) { memcpy(dst, src, (size_t)n * sizeof(double)); return; }
    if (dt == VECR_DT_F32) {
        float *p = (float *)dst;
        for (int64_t i = 0; i < n; ++i) p[i] = (float)src[i];
        return;
    }
    /* Integer dtypes: NA_REAL maps to nodata (caller is responsible for
     * setting a nodata value that matches), otherwise round-to-nearest. */
    for (int64_t i = 0; i < n; ++i) {
        double v = src[i];
        int64_t iv = ISNAN(v) ? 0 : (int64_t)llround(v);
        switch (dt) {
        case VECR_DT_I8:  ((int8_t  *)dst)[i] = (int8_t) iv; break;
        case VECR_DT_U8:  ((uint8_t *)dst)[i] = (uint8_t)iv; break;
        case VECR_DT_I16: ((int16_t *)dst)[i] = (int16_t)iv; break;
        case VECR_DT_U16: ((uint16_t*)dst)[i] = (uint16_t)iv; break;
        case VECR_DT_I32: ((int32_t *)dst)[i] = (int32_t)iv; break;
        case VECR_DT_U32: ((uint32_t*)dst)[i] = (uint32_t)iv; break;
        case VECR_DT_I64: ((int64_t *)dst)[i] = iv;          break;
        case VECR_DT_U64: ((uint64_t*)dst)[i] = (uint64_t)iv; break;
        }
    }
}

/* Reverse: dtype buffer -> doubles. NaN passthrough for floats; integer
 * buffers are widened. The caller can post-process nodata to NA_REAL. */
static void cast_dtype_to_doubles(const void *src, int64_t n,
                                  uint8_t dt, double *dst) {
    switch (dt) {
    case VECR_DT_F64: memcpy(dst, src, (size_t)n * sizeof(double)); break;
    case VECR_DT_F32: {
        const float *p = (const float *)src;
        for (int64_t i = 0; i < n; ++i) dst[i] = (double)p[i];
        break;
    }
    case VECR_DT_I8:  { const int8_t   *p = (const int8_t  *)src;
        for (int64_t i = 0; i < n; ++i) dst[i] = (double)p[i]; break; }
    case VECR_DT_U8:  { const uint8_t  *p = (const uint8_t *)src;
        for (int64_t i = 0; i < n; ++i) dst[i] = (double)p[i]; break; }
    case VECR_DT_I16: { const int16_t  *p = (const int16_t *)src;
        for (int64_t i = 0; i < n; ++i) dst[i] = (double)p[i]; break; }
    case VECR_DT_U16: { const uint16_t *p = (const uint16_t*)src;
        for (int64_t i = 0; i < n; ++i) dst[i] = (double)p[i]; break; }
    case VECR_DT_I32: { const int32_t  *p = (const int32_t *)src;
        for (int64_t i = 0; i < n; ++i) dst[i] = (double)p[i]; break; }
    case VECR_DT_U32: { const uint32_t *p = (const uint32_t*)src;
        for (int64_t i = 0; i < n; ++i) dst[i] = (double)p[i]; break; }
    case VECR_DT_I64: { const int64_t  *p = (const int64_t *)src;
        for (int64_t i = 0; i < n; ++i) dst[i] = (double)p[i]; break; }
    case VECR_DT_U64: { const uint64_t *p = (const uint64_t*)src;
        for (int64_t i = 0; i < n; ++i) dst[i] = (double)p[i]; break; }
    }
}

/* ---------- Reader xptr management -------------------------------------- */

static void vecr_reader_xptr_finalize(SEXP x) {
    VecrReader *r = (VecrReader *)R_ExternalPtrAddr(x);
    if (r) {
        vecr_reader_close(r);
        R_ClearExternalPtr(x);
    }
}

static SEXP wrap_vecr_reader(VecrReader *r) {
    SEXP tag = PROTECT(Rf_install("vecr_reader"));
    SEXP xptr = PROTECT(R_MakeExternalPtr(r, tag, R_NilValue));
    R_RegisterCFinalizerEx(xptr, vecr_reader_xptr_finalize, TRUE);
    UNPROTECT(2);
    return xptr;
}

static VecrReader *unwrap_vecr_reader(SEXP xptr) {
    if (TYPEOF(xptr) != EXTPTRSXP)
        Rf_error("expected an external pointer to a vecr_reader");
    VecrReader *r = (VecrReader *)R_ExternalPtrAddr(xptr);
    if (!r) Rf_error("vecr_reader handle is closed");
    return r;
}

/* ====================================================================== */
/*  C_vec_write_raster                                                     */
/* ====================================================================== */

/* ====================================================================== */
/*  C_vec_write_time_cube                                                  */
/* ====================================================================== */

/* Write a 4D array (rows, cols, bands, time) as a stack of band slices,
 * each slice tagged with its time stamp. The on-disk format is identical
 * to a regular .vec raster — only the per-tile time field differs. */
SEXP C_vec_write_time_cube(SEXP path_sexp, SEXP data_sexp, SEXP dims_sexp,
                           SEXP times_sexp, SEXP dtype_sexp,
                           SEXP tile_size_sexp,
                           SEXP gt_sexp, SEXP epsg_sexp, SEXP nodata_sexp,
                           SEXP band_names_sexp, SEXP compression_sexp) {
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    int *dims = INTEGER(dims_sexp);
    int64_t width   = (int64_t)dims[0];
    int64_t height  = (int64_t)dims[1];
    int     n_bands = dims[2];
    int     n_time  = dims[3];

    int64_t expected = width * height * (int64_t)n_bands * (int64_t)n_time;
    if (Rf_xlength(data_sexp) != expected)
        vectra_error("vec_write_time_cube: data length mismatch");
    if (Rf_xlength(times_sexp) != n_time)
        vectra_error("vec_write_time_cube: times length must match n_time");

    uint8_t dt = dtype_from_string(CHAR(STRING_ELT(dtype_sexp, 0)));
    if (dt == 0)
        vectra_error("vec_write_time_cube: unknown dtype");

    uint16_t tile_size = (uint16_t)Rf_asInteger(tile_size_sexp);
    int32_t  epsg = (int32_t)Rf_asInteger(epsg_sexp);
    double   nodata = Rf_asReal(nodata_sexp);

    const double *gt = NULL;
    if (TYPEOF(gt_sexp) == REALSXP && Rf_xlength(gt_sexp) == 6) {
        gt = REAL(gt_sexp);
    }

    char **band_names = NULL;
    if (TYPEOF(band_names_sexp) == STRSXP &&
        Rf_xlength(band_names_sexp) == n_bands) {
        band_names = (char **)calloc((size_t)n_bands, sizeof(char *));
        if (!band_names) vectra_error("alloc failed");
        for (int i = 0; i < n_bands; ++i)
            band_names[i] = (char *)CHAR(STRING_ELT(band_names_sexp, i));
    }

    VecrWriter *w = NULL;
    if (vecr_writer_open(path, width, height, n_bands, tile_size, dt,
                         gt, epsg, nodata,
                         (const char *const *)band_names, &w) != 0) {
        const char *m = w ? vecr_writer_errmsg(w) : "open failed";
        vecr_writer_close(w); free(band_names);
        vectra_error("vec_write_time_cube: open failed: %s", m);
    }
    free(band_names);
    vecr_writer_set_compression(w, Rf_asInteger(compression_sexp));

    int64_t band_n = width * height;
    size_t  esz    = vecr_dtype_size(dt);
    void   *band_buf = malloc((size_t)band_n * esz);
    if (!band_buf) {
        vecr_writer_close(w);
        vectra_error("alloc failed for band buffer");
    }

    const double *src = REAL(data_sexp);
    /* R array layout: a[row, col, band, time]: index =
     *   row + col*rows + band*rows*cols + time*rows*cols*bands.
     * Our writer expects band-major then row-major. We iterate
     * (time, band) outer, casting one slice at a time. */
    int64_t per_slice = band_n;
    int64_t per_band  = per_slice;          /* one band at one time */
    int64_t stride_band = per_band;         /* doubles between band 0 and band 1 */
    int64_t stride_time = per_band * (int64_t)n_bands;

    const double *times = REAL(times_sexp);
    for (int t = 0; t < n_time; ++t) {
        int64_t tval = (int64_t)times[t];
        if (tval == 0) tval = 1;  /* avoid 0 = "untimed" sentinel */
        vecr_writer_set_time(w, tval);
        for (int b = 0; b < n_bands; ++b) {
            const double *slice = src + (int64_t)t * stride_time + (int64_t)b * stride_band;
            cast_doubles_to_dtype(slice, per_slice, dt, band_buf);
            if (vecr_writer_write_band(w, b, band_buf) != 0) {
                const char *m = vecr_writer_errmsg(w);
                free(band_buf); vecr_writer_close(w);
                vectra_error("vec_write_time_cube: write failed: %s", m);
            }
        }
    }
    free(band_buf);

    if (vecr_writer_finish(w) != 0) {
        const char *m = vecr_writer_errmsg(w);
        vecr_writer_close(w);
        vectra_error("vec_write_time_cube: finish failed: %s", m);
    }
    vecr_writer_close(w);
    return R_NilValue;
}

/*
 * C_vec_read_time_slice(ptr, band, level, time, col_min, row_min, col_max, row_max)
 *
 * Like C_vec_read_window but matches tiles where index entry .time equals
 * the supplied time value. Returns a numeric matrix with NA for nodata.
 */
SEXP C_vec_read_time_slice(SEXP ptr_sexp, SEXP band_sexp, SEXP level_sexp,
                           SEXP time_sexp,
                           SEXP col_min_sexp, SEXP row_min_sexp,
                           SEXP col_max_sexp, SEXP row_max_sexp) {
    VecrReader *r = unwrap_vecr_reader(ptr_sexp);
    int     band  = Rf_asInteger(band_sexp) - 1;
    int     level = Rf_asInteger(level_sexp);
    int64_t time  = (int64_t)Rf_asReal(time_sexp);
    int64_t col_min = (int64_t)Rf_asInteger(col_min_sexp) - 1;
    int64_t row_min = (int64_t)Rf_asInteger(row_min_sexp) - 1;
    int64_t col_max = (int64_t)Rf_asInteger(col_max_sexp) - 1;
    int64_t row_max = (int64_t)Rf_asInteger(row_max_sexp) - 1;

    int64_t out_w = col_max - col_min + 1;
    int64_t out_h = row_max - row_min + 1;
    int64_t out_n = out_w * out_h;

    uint8_t dt  = vecr_reader_dtype(r);
    size_t  esz = vecr_dtype_size(dt);

    void *raw = malloc((size_t)out_n * esz);
    if (!raw) vectra_error("alloc failed");

    if (vecr_reader_read_window_t(r, band, (uint8_t)level, time,
                                   col_min, row_min, col_max, row_max,
                                   raw) != 0) {
        const char *msg = vecr_reader_errmsg(r);
        free(raw);
        vectra_error("vec_read_time_slice: %s", msg);
    }

    SEXP out = PROTECT(Rf_allocMatrix(REALSXP, (int)out_h, (int)out_w));
    double *dst = REAL(out);
    int has_nd = vecr_reader_has_nodata(r);
    double nd = vecr_reader_nodata(r);

    double *row_buf = (double *)malloc((size_t)out_w * sizeof(double));
    if (!row_buf) { free(raw); vectra_error("alloc failed for row buf"); }
    for (int64_t rr = 0; rr < out_h; ++rr) {
        const uint8_t *src_row = (const uint8_t *)raw + (size_t)(rr * out_w) * esz;
        cast_dtype_to_doubles(src_row, out_w, dt, row_buf);
        for (int64_t cc = 0; cc < out_w; ++cc) {
            double v = row_buf[cc];
            if (has_nd) {
                if (dt == VECR_DT_F64 || dt == VECR_DT_F32) {
                    if (isnan(nd) ? isnan(v) : v == nd) v = NA_REAL;
                } else if (v == nd) v = NA_REAL;
            } else if ((dt == VECR_DT_F64 || dt == VECR_DT_F32) && isnan(v)) {
                v = NA_REAL;
            }
            dst[(int64_t)cc * out_h + rr] = v;
        }
    }
    free(row_buf);
    free(raw);
    UNPROTECT(1);
    return out;
}

/*
 * C_vec_write_raster(path, data, dims, dtype, tile_size, gt, epsg, nodata,
 *                     band_names, compression)
 *
 *   path        : character(1)
 *   data        : numeric vector of length width*height*n_bands (band-major:
 *                 band 1 first, then band 2, ...). Each band is row-major.
 *   dims        : integer(3) = c(width, height, n_bands)
 *   dtype       : character(1) — "f32"/"f64"/"i16"/etc.
 *   tile_size   : integer(1) — 0 means default 512
 *   gt          : numeric(6) — affine transform (NULL allowed)
 *   epsg        : integer(1) — 0 = no CRS recorded
 *   nodata      : numeric(1) — NA_real_ = none recorded
 *   band_names  : character(n_bands) or NULL
 *   compression : integer(1) — VECR_COMPRESS_FAST/BALANCED/MAX
 */
SEXP C_vec_write_raster(SEXP path_sexp, SEXP data_sexp, SEXP dims_sexp,
                        SEXP dtype_sexp, SEXP tile_size_sexp,
                        SEXP gt_sexp, SEXP epsg_sexp, SEXP nodata_sexp,
                        SEXP band_names_sexp, SEXP compression_sexp) {
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    int *dims = INTEGER(dims_sexp);
    int64_t width   = (int64_t)dims[0];
    int64_t height  = (int64_t)dims[1];
    int     n_bands = dims[2];

    if (TYPEOF(data_sexp) != REALSXP)
        vectra_error("vec_write_raster: data must be numeric");
    int64_t expected = width * height * (int64_t)n_bands;
    if (Rf_xlength(data_sexp) != expected)
        vectra_error("vec_write_raster: data length %lld != width*height*n_bands (%lld)",
                     (long long)Rf_xlength(data_sexp), (long long)expected);

    uint8_t dt = dtype_from_string(CHAR(STRING_ELT(dtype_sexp, 0)));
    if (dt == 0)
        vectra_error("vec_write_raster: unknown dtype '%s'",
                     CHAR(STRING_ELT(dtype_sexp, 0)));

    uint16_t tile_size = (uint16_t)Rf_asInteger(tile_size_sexp);
    int32_t  epsg = (int32_t)Rf_asInteger(epsg_sexp);
    double   nodata = Rf_asReal(nodata_sexp);

    const double *gt = NULL;
    if (TYPEOF(gt_sexp) == REALSXP && Rf_xlength(gt_sexp) == 6) {
        gt = REAL(gt_sexp);
    }

    /* Band names */
    char **band_names = NULL;
    if (TYPEOF(band_names_sexp) == STRSXP &&
        Rf_xlength(band_names_sexp) == n_bands) {
        band_names = (char **)calloc((size_t)n_bands, sizeof(char *));
        if (!band_names) vectra_error("alloc failed for band names");
        for (int i = 0; i < n_bands; ++i) {
            SEXP s = STRING_ELT(band_names_sexp, i);
            band_names[i] = (char *)CHAR(s);
        }
    }

    VecrWriter *w = NULL;
    if (vecr_writer_open(path, width, height, n_bands, tile_size, dt,
                         gt, epsg, nodata,
                         (const char *const *)band_names, &w) != 0) {
        const char *msg = w ? vecr_writer_errmsg(w) : "unknown";
        vecr_writer_close(w);
        free(band_names);
        vectra_error("vec_write_raster: open failed: %s", msg);
    }
    free(band_names);
    vecr_writer_set_compression(w, Rf_asInteger(compression_sexp));

    /* Cast and write band-by-band. */
    int64_t band_n = width * height;
    size_t  esz    = vecr_dtype_size(dt);
    void   *band_buf = malloc((size_t)band_n * esz);
    if (!band_buf) {
        vecr_writer_close(w);
        vectra_error("alloc failed for band buffer");
    }

    const double *src = REAL(data_sexp);
    for (int b = 0; b < n_bands; ++b) {
        cast_doubles_to_dtype(src + b * band_n, band_n, dt, band_buf);
        if (vecr_writer_write_band(w, b, band_buf) != 0) {
            const char *msg = vecr_writer_errmsg(w);
            free(band_buf);
            vecr_writer_close(w);
            vectra_error("vec_write_raster: write band %d failed: %s", b, msg);
        }
    }
    free(band_buf);

    if (vecr_writer_finish(w) != 0) {
        const char *msg = vecr_writer_errmsg(w);
        vecr_writer_close(w);
        vectra_error("vec_write_raster: finish failed: %s", msg);
    }
    vecr_writer_close(w);
    return R_NilValue;
}

/* ====================================================================== */
/*  C_vec_open_raster                                                      */
/* ====================================================================== */

/*
 * Returns a list with elements:
 *   $ptr        : externalptr (auto-finalizer installed)
 *   $width      : integer
 *   $height     : integer
 *   $n_bands    : integer
 *   $tile_size  : integer
 *   $dtype      : character (one of "f32"/"f64"/"i16"/...)
 *   $gt         : numeric(6)
 *   $epsg       : integer (0 if not set)
 *   $nodata     : numeric (NA_real_ if not set)
 *   $band_names : character(n_bands) or NULL
 */
SEXP C_vec_open_raster(SEXP path_sexp) {
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    VecrReader *r = NULL;
    if (vecr_reader_open(path, &r) != 0) {
        const char *msg = r ? vecr_reader_errmsg(r) : "unknown";
        vecr_reader_close(r);
        vectra_error("vec_open_raster: %s", msg);
    }

    SEXP ptr = PROTECT(wrap_vecr_reader(r));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, 11));
    SEXP out   = PROTECT(Rf_allocVector(VECSXP, 11));

    SET_VECTOR_ELT(out, 0, ptr);
    SET_STRING_ELT(names, 0, Rf_mkChar("ptr"));

    SET_VECTOR_ELT(out, 1, Rf_ScalarInteger((int)vecr_reader_width(r)));
    SET_STRING_ELT(names, 1, Rf_mkChar("width"));

    SET_VECTOR_ELT(out, 2, Rf_ScalarInteger((int)vecr_reader_height(r)));
    SET_STRING_ELT(names, 2, Rf_mkChar("height"));

    SET_VECTOR_ELT(out, 3, Rf_ScalarInteger(vecr_reader_nbands(r)));
    SET_STRING_ELT(names, 3, Rf_mkChar("n_bands"));

    SET_VECTOR_ELT(out, 4, Rf_ScalarInteger((int)vecr_reader_tile_size(r)));
    SET_STRING_ELT(names, 4, Rf_mkChar("tile_size"));

    SET_VECTOR_ELT(out, 5,
        Rf_mkString(dtype_to_string(vecr_reader_dtype(r))));
    SET_STRING_ELT(names, 5, Rf_mkChar("dtype"));

    SEXP gt = PROTECT(Rf_allocVector(REALSXP, 6));
    memcpy(REAL(gt), vecr_reader_geotransform(r), 6 * sizeof(double));
    SET_VECTOR_ELT(out, 6, gt);
    SET_STRING_ELT(names, 6, Rf_mkChar("gt"));
    UNPROTECT(1);

    SET_VECTOR_ELT(out, 7, Rf_ScalarInteger((int)vecr_reader_epsg(r)));
    SET_STRING_ELT(names, 7, Rf_mkChar("epsg"));

    double nd = vecr_reader_has_nodata(r) ? vecr_reader_nodata(r) : NA_REAL;
    SET_VECTOR_ELT(out, 8, Rf_ScalarReal(nd));
    SET_STRING_ELT(names, 8, Rf_mkChar("nodata"));

    int nb = vecr_reader_nbands(r);
    int has_names = 0;
    for (int b = 0; b < nb; ++b) {
        if (vecr_reader_band_name(r, b)) { has_names = 1; break; }
    }
    if (has_names) {
        SEXP bn = PROTECT(Rf_allocVector(STRSXP, nb));
        for (int b = 0; b < nb; ++b) {
            const char *s = vecr_reader_band_name(r, b);
            SET_STRING_ELT(bn, b, s ? Rf_mkChar(s) : NA_STRING);
        }
        SET_VECTOR_ELT(out, 9, bn);
        UNPROTECT(1);
    } else {
        SET_VECTOR_ELT(out, 9, R_NilValue);
    }
    SET_STRING_ELT(names, 9, Rf_mkChar("band_names"));

    SET_VECTOR_ELT(out, 10, Rf_ScalarInteger(vecr_reader_n_levels(r)));
    SET_STRING_ELT(names, 10, Rf_mkChar("n_levels"));

    Rf_setAttrib(out, R_NamesSymbol, names);
    UNPROTECT(3);
    return out;
}

/* ====================================================================== */
/*  C_vec_read_window                                                      */
/* ====================================================================== */

/*
 * C_vec_read_window(ptr, band, level, col_min, row_min, col_max, row_max)
 *   ptr     : externalptr from open_raster
 *   band    : integer(1) 1-based
 *   level   : integer(1)
 *   col/row : integer(1) — 1-based, inclusive
 *
 * Returns a numeric matrix of dim c(rows, cols) with NA for nodata pixels.
 */
SEXP C_vec_read_window(SEXP ptr_sexp, SEXP band_sexp, SEXP level_sexp,
                       SEXP col_min_sexp, SEXP row_min_sexp,
                       SEXP col_max_sexp, SEXP row_max_sexp) {
    VecrReader *r = unwrap_vecr_reader(ptr_sexp);
    int  band  = Rf_asInteger(band_sexp) - 1;
    int  level = Rf_asInteger(level_sexp);
    int64_t col_min = (int64_t)Rf_asInteger(col_min_sexp) - 1;
    int64_t row_min = (int64_t)Rf_asInteger(row_min_sexp) - 1;
    int64_t col_max = (int64_t)Rf_asInteger(col_max_sexp) - 1;
    int64_t row_max = (int64_t)Rf_asInteger(row_max_sexp) - 1;

    int64_t out_w = col_max - col_min + 1;
    int64_t out_h = row_max - row_min + 1;
    if (out_w <= 0 || out_h <= 0)
        vectra_error("vec_read_window: empty window");
    int64_t out_n = out_w * out_h;

    uint8_t dt  = vecr_reader_dtype(r);
    size_t  esz = vecr_dtype_size(dt);

    void *raw = malloc((size_t)out_n * esz);
    if (!raw) vectra_error("alloc failed for window buffer");

    if (vecr_reader_read_window(r, band, (uint8_t)level,
                                col_min, row_min, col_max, row_max,
                                raw) != 0) {
        const char *msg = vecr_reader_errmsg(r);
        free(raw);
        vectra_error("vec_read_window: %s", msg);
    }

    /* Build a numeric matrix with rows = out_h, cols = out_w. R matrices
     * are column-major; the C buffer is row-major. Transpose during cast. */
    SEXP out = PROTECT(Rf_allocMatrix(REALSXP, (int)out_h, (int)out_w));
    double *dst = REAL(out);

    int has_nd = vecr_reader_has_nodata(r);
    double nd = vecr_reader_nodata(r);

    /* Fast path: f64 with no nodata recorded. */
    if (dt == VECR_DT_F64 && !has_nd) {
        const double *p = (const double *)raw;
        for (int64_t rr = 0; rr < out_h; ++rr)
            for (int64_t cc = 0; cc < out_w; ++cc)
                dst[(int64_t)cc * out_h + rr] = p[rr * out_w + cc];
        free(raw);
        UNPROTECT(1);
        return out;
    }

    /* General path: cast to double row-by-row, transpose, NA-mark nodata. */
    double *row_buf = (double *)malloc((size_t)out_w * sizeof(double));
    if (!row_buf) { free(raw); vectra_error("alloc failed for row buf"); }

    for (int64_t rr = 0; rr < out_h; ++rr) {
        const uint8_t *src_row = (const uint8_t *)raw + (size_t)(rr * out_w) * esz;
        cast_dtype_to_doubles(src_row, out_w, dt, row_buf);
        for (int64_t cc = 0; cc < out_w; ++cc) {
            double v = row_buf[cc];
            if (has_nd) {
                if (dt == VECR_DT_F64 || dt == VECR_DT_F32) {
                    if (isnan(nd) ? isnan(v) : v == nd) v = NA_REAL;
                } else {
                    if (v == nd) v = NA_REAL;
                }
            } else if ((dt == VECR_DT_F64 || dt == VECR_DT_F32) && isnan(v)) {
                v = NA_REAL;
            }
            dst[(int64_t)cc * out_h + rr] = v;
        }
    }
    free(row_buf);
    free(raw);
    UNPROTECT(1);
    return out;
}

/* ====================================================================== */
/*  C_vec_extract_points                                                   */
/* ====================================================================== */

/*
 * C_vec_extract_points(ptr, x, y) -> data.frame with columns
 *   x, y, band1, band2, ...
 * (matches the tiff_extract_points convention).
 */
SEXP C_vec_extract_points(SEXP ptr_sexp, SEXP x_sexp, SEXP y_sexp) {
    VecrReader *r = unwrap_vecr_reader(ptr_sexp);

    int64_t n = Rf_xlength(x_sexp);
    if (Rf_xlength(y_sexp) != n)
        vectra_error("vec_extract_points: x and y length mismatch");

    int nb = vecr_reader_nbands(r);
    int has_nd = vecr_reader_has_nodata(r);
    double nd = vecr_reader_nodata(r);
    uint8_t dt = vecr_reader_dtype(r);

    int n_cols = 2 + nb;
    SEXP out   = PROTECT(Rf_allocVector(VECSXP, n_cols));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, n_cols));

    SEXP x_out = PROTECT(Rf_allocVector(REALSXP, n));
    memcpy(REAL(x_out), REAL(x_sexp), (size_t)n * sizeof(double));
    SET_VECTOR_ELT(out, 0, x_out);
    SET_STRING_ELT(names, 0, Rf_mkChar("x"));
    UNPROTECT(1);

    SEXP y_out = PROTECT(Rf_allocVector(REALSXP, n));
    memcpy(REAL(y_out), REAL(y_sexp), (size_t)n * sizeof(double));
    SET_VECTOR_ELT(out, 1, y_out);
    SET_STRING_ELT(names, 1, Rf_mkChar("y"));
    UNPROTECT(1);

    for (int b = 0; b < nb; ++b) {
        SEXP col = PROTECT(Rf_allocVector(REALSXP, n));
        double *dst = REAL(col);
        if (vecr_reader_extract_points(r, b, n, REAL(x_sexp), REAL(y_sexp),
                                       dst) != 0) {
            const char *msg = vecr_reader_errmsg(r);
            UNPROTECT(3);  /* col, names, out */
            vectra_error("vec_extract_points: %s", msg);
        }
        for (int64_t i = 0; i < n; ++i) {
            double v = dst[i];
            if (isnan(v)) { dst[i] = NA_REAL; continue; }
            if (has_nd) {
                if (dt == VECR_DT_F64 || dt == VECR_DT_F32) {
                    if (isnan(nd) ? isnan(v) : v == nd) dst[i] = NA_REAL;
                } else if (v == nd) {
                    dst[i] = NA_REAL;
                }
            }
        }
        SET_VECTOR_ELT(out, 2 + b, col);

        char bname[32];
        const char *sname = vecr_reader_band_name(r, b);
        if (sname && sname[0]) {
            SET_STRING_ELT(names, 2 + b, Rf_mkChar(sname));
        } else {
            snprintf(bname, sizeof(bname), "band%d", b + 1);
            SET_STRING_ELT(names, 2 + b, Rf_mkChar(bname));
        }
        UNPROTECT(1);
    }

    Rf_setAttrib(out, R_NamesSymbol, names);
    Rf_setAttrib(out, R_ClassSymbol, Rf_mkString("data.frame"));
    SEXP rn = PROTECT(Rf_allocVector(INTSXP, 2));
    INTEGER(rn)[0] = NA_INTEGER;
    INTEGER(rn)[1] = -(int)n;
    Rf_setAttrib(out, R_RowNamesSymbol, rn);
    UNPROTECT(1);

    UNPROTECT(2);
    return out;
}

/* ====================================================================== */
/*  C_vec_close_raster                                                     */
/* ====================================================================== */

/* Idempotent close; the finalizer will also clean up if the user forgets. */
SEXP C_vec_close_raster(SEXP ptr_sexp) {
    if (TYPEOF(ptr_sexp) != EXTPTRSXP) return R_NilValue;
    VecrReader *r = (VecrReader *)R_ExternalPtrAddr(ptr_sexp);
    if (r) {
        vecr_reader_close(r);
        R_ClearExternalPtr(ptr_sexp);
    }
    return R_NilValue;
}

/* ====================================================================== */
/*  C_vec_build_overviews                                                  */
/* ====================================================================== */

static int resampling_from_string(const char *s) {
    if (!s) return VECR_RESAMPLE_AVERAGE;
    if (strcmp(s, "nearest")  == 0) return VECR_RESAMPLE_NEAREST;
    if (strcmp(s, "average")  == 0) return VECR_RESAMPLE_AVERAGE;
    if (strcmp(s, "bilinear") == 0) return VECR_RESAMPLE_BILINEAR;
    if (strcmp(s, "mode")     == 0) return VECR_RESAMPLE_MODE;
    if (strcmp(s, "gauss")    == 0) return VECR_RESAMPLE_GAUSS;
    return -1;
}

SEXP C_vec_build_overviews(SEXP path_sexp, SEXP n_levels_sexp,
                           SEXP resampling_sexp, SEXP compression_sexp) {
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    int n_levels = Rf_asInteger(n_levels_sexp);
    int resampling = resampling_from_string(CHAR(STRING_ELT(resampling_sexp, 0)));
    if (resampling < 0)
        vectra_error("vec_build_overviews: unknown resampling '%s'",
                     CHAR(STRING_ELT(resampling_sexp, 0)));
    int compression = Rf_asInteger(compression_sexp);

    char errbuf[256] = {0};
    if (vecr_build_overviews(path, n_levels, resampling, compression,
                              errbuf, sizeof(errbuf)) != 0) {
        vectra_error("vec_build_overviews: %s",
                     errbuf[0] ? errbuf : "unknown error");
    }
    return R_NilValue;
}

/* ====================================================================== */
/*  C_vec_to_tiff                                                          */
/* ====================================================================== */

#include "tiff_format.h"

static int pixel_type_from_dtype(uint8_t dt) {
    switch (dt) {
    case VECR_DT_F64: return TIFF_PIXEL_FLOAT64;
    case VECR_DT_F32: return TIFF_PIXEL_FLOAT32;
    case VECR_DT_I16: return TIFF_PIXEL_INT16;
    case VECR_DT_I32: return TIFF_PIXEL_INT32;
    case VECR_DT_U8:  return TIFF_PIXEL_UINT8;
    case VECR_DT_U16: return TIFF_PIXEL_UINT16;
    }
    /* Other dtypes (i8/u32/i64/u64) widen to f64 in TIFF output. */
    return TIFF_PIXEL_FLOAT64;
}

/*
 * C_vec_to_tiff(vec_path, tiff_path, compression)
 *
 *   vec_path     : character(1) — source .vec raster
 *   tiff_path    : character(1) — output .tif
 *   compression  : integer(1)   — TIFF_COMPRESS_NONE / DEFLATE / LZW
 *                                 (0 / 1 / 2; mirrors the public enum)
 *
 * The exported TIFF uses the same dtype as the source .vec raster (with
 * a fallback to f64 for dtypes TIFF doesn't natively carry — i8/u32/i64).
 * Geotransform, EPSG, and nodata are forwarded from the .vec header.
 */
SEXP C_vec_to_tiff(SEXP vec_path_sexp, SEXP tiff_path_sexp,
                   SEXP compression_sexp) {
    const char *vpath = CHAR(STRING_ELT(vec_path_sexp,  0));
    const char *tpath = CHAR(STRING_ELT(tiff_path_sexp, 0));
    int compression = Rf_asInteger(compression_sexp);
    if (compression != TIFF_COMPRESS_NONE &&
        compression != TIFF_COMPRESS_DEFLATE &&
        compression != TIFF_COMPRESS_LZW) {
        vectra_error("vec_to_tiff: invalid compression code %d", compression);
    }

    VecrReader *r = NULL;
    if (vecr_reader_open(vpath, &r) != 0) {
        const char *m = r ? vecr_reader_errmsg(r) : "open failed";
        vecr_reader_close(r);
        vectra_error("vec_to_tiff: %s", m);
    }

    int64_t width  = vecr_reader_width(r);
    int64_t height = vecr_reader_height(r);
    int     n_bands = vecr_reader_nbands(r);
    uint8_t dt = vecr_reader_dtype(r);
    int pix = pixel_type_from_dtype(dt);
    double nodata = vecr_reader_has_nodata(r) ? vecr_reader_nodata(r) : NAN;

    TiffWriter *w = NULL;
    if (tiff_writer_open(tpath, &w, width, height, n_bands,
                         vecr_reader_geotransform(r), nodata,
                         compression, pix) != 0) {
        const char *m = w ? tiff_writer_errmsg(w) : "open failed";
        tiff_writer_close(w);
        vecr_reader_close(r);
        vectra_error("vec_to_tiff: %s", m);
    }

    int32_t epsg = vecr_reader_epsg(r);
    if (epsg > 0) {
        /* The TIFF writer takes geographic vs projected as separate args.
         * We can't classify EPSG by code alone (3xxx may still be projected
         * via a non-standard CRS), so default to projected — that matches
         * the typical climate / DEM / land-cover use cases. */
        tiff_writer_set_crs(w, 0, epsg, NULL);
    }

    /* Decode each band as doubles and hand to the TIFF writer in one shot. */
    int64_t n_pixels = width * height;
    size_t  esz = vecr_dtype_size(dt);

    double **bands = (double **)malloc((size_t)n_bands * sizeof(double *));
    if (!bands) {
        tiff_writer_close(w); vecr_reader_close(r);
        vectra_error("alloc failed for band table");
    }
    void *raw = malloc((size_t)n_pixels * esz);
    if (!raw) {
        free(bands);
        tiff_writer_close(w); vecr_reader_close(r);
        vectra_error("alloc failed for raw buffer");
    }

    int rc = 0;
    int has_nd = vecr_reader_has_nodata(r);
    double nd = vecr_reader_nodata(r);

    for (int b = 0; b < n_bands; ++b) {
        bands[b] = (double *)malloc((size_t)n_pixels * sizeof(double));
        if (!bands[b]) { rc = -1; break; }

        if (vecr_reader_read_window(r, b, 0, 0, 0, width - 1, height - 1, raw)
              != 0) { rc = -1; break; }

        cast_dtype_to_doubles(raw, n_pixels, dt, bands[b]);

        /* For float dtypes the reader may have left NaN in nodata cells;
         * the TIFF writer maps NaN to nodata via the writer's tag. For
         * integer dtypes we leave the cast value and rely on TIFF's
         * GDAL_NODATA tag to flag it on read. NaN-fill nodata cells when
         * writing to a float TIFF so terra etc. recognise them. */
        if (has_nd && (pix == TIFF_PIXEL_FLOAT32 || pix == TIFF_PIXEL_FLOAT64)) {
            for (int64_t i = 0; i < n_pixels; ++i) {
                if (bands[b][i] == nd) bands[b][i] = NAN;
            }
        }
    }
    free(raw);

    if (rc != 0) {
        for (int b = 0; b < n_bands; ++b) free(bands[b]);
        free(bands);
        tiff_writer_close(w); vecr_reader_close(r);
        vectra_error("vec_to_tiff: failed to decode bands");
    }

    if (tiff_writer_write_rows(w, 0, height,
                                (const double *const *)bands) != 0) {
        const char *m = tiff_writer_errmsg(w);
        for (int b = 0; b < n_bands; ++b) free(bands[b]);
        free(bands);
        tiff_writer_close(w); vecr_reader_close(r);
        vectra_error("vec_to_tiff write error: %s", m);
    }
    if (tiff_writer_finish(w) != 0) {
        const char *m = tiff_writer_errmsg(w);
        for (int b = 0; b < n_bands; ++b) free(bands[b]);
        free(bands);
        tiff_writer_close(w); vecr_reader_close(r);
        vectra_error("vec_to_tiff finish error: %s", m);
    }

    for (int b = 0; b < n_bands; ++b) free(bands[b]);
    free(bands);
    tiff_writer_close(w);
    vecr_reader_close(r);
    return R_NilValue;
}

/* ====================================================================== */
/*  Phase 6b — pixel-time-series transpose layout                          */
/* ====================================================================== */

/*
 * C_vec_write_pixel_cube(path, data, dims, times, dtype, tile_size,
 *                        gt, epsg, nodata, band_names, compression)
 *
 *   data : numeric vector of length width*height*n_bands*n_time, ordered
 *          [time][band][row][col] (col fastest) — same layout the
 *          time-cube R wrapper produces.
 *   dims : integer(4) = c(width, height, n_bands, n_time)
 *   times: numeric(n_time) — int64-friendly (truncated via (int64_t)).
 *
 * Writes a pixel-major .vec raster: each spatial tile holds the full
 * (tw*th, n_time) time stack contiguously.
 */
SEXP C_vec_write_pixel_cube(SEXP path_sexp, SEXP data_sexp, SEXP dims_sexp,
                            SEXP times_sexp, SEXP dtype_sexp,
                            SEXP tile_size_sexp,
                            SEXP gt_sexp, SEXP epsg_sexp, SEXP nodata_sexp,
                            SEXP band_names_sexp, SEXP compression_sexp) {
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    int *dims = INTEGER(dims_sexp);
    int64_t width   = (int64_t)dims[0];
    int64_t height  = (int64_t)dims[1];
    int     n_bands = dims[2];
    int     n_time  = dims[3];

    int64_t expected = width * height * (int64_t)n_bands * (int64_t)n_time;
    if (Rf_xlength(data_sexp) != expected)
        vectra_error("vec_write_pixel_cube: data length mismatch");
    if (Rf_xlength(times_sexp) != n_time)
        vectra_error("vec_write_pixel_cube: times length must match n_time");

    uint8_t dt = dtype_from_string(CHAR(STRING_ELT(dtype_sexp, 0)));
    if (dt == 0)
        vectra_error("vec_write_pixel_cube: unknown dtype");

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
        if (!band_names) vectra_error("alloc failed");
        for (int i = 0; i < n_bands; ++i)
            band_names[i] = (char *)CHAR(STRING_ELT(band_names_sexp, i));
    }

    /* Cast doubles -> sample dtype once for the whole array. */
    int64_t total = expected;
    size_t  esz   = vecr_dtype_size(dt);
    void   *buf   = malloc((size_t)total * esz);
    if (!buf) {
        free(band_names);
        vectra_error("alloc failed for cube buffer");
    }
    cast_doubles_to_dtype(REAL(data_sexp), total, dt, buf);

    /* Times -> int64. */
    int64_t *times = (int64_t *)malloc((size_t)n_time * sizeof(int64_t));
    if (!times) {
        free(buf); free(band_names);
        vectra_error("alloc failed for times");
    }
    const double *tsrc = REAL(times_sexp);
    for (int t = 0; t < n_time; ++t) times[t] = (int64_t)tsrc[t];

    char errbuf[256] = {0};
    int rc = vecr_write_pixel_cube(path, width, height, n_bands, tile_size, dt,
                                   gt, epsg, nodata,
                                   (const char *const *)band_names,
                                   times, n_time, buf,
                                   Rf_asInteger(compression_sexp),
                                   errbuf, sizeof(errbuf));
    free(times); free(buf); free(band_names);
    if (rc != 0) {
        vectra_error("vec_write_pixel_cube: %s",
                     errbuf[0] ? errbuf : "unknown");
    }
    return R_NilValue;
}

/*
 * C_vec_read_pixel_series(ptr, col, row, band, level)
 *
 * Returns a numeric vector of length n_time. For pixel-major files this
 * is one tile decode; for image-major files it scans every distinct
 * .time stamp matching (band, level) and decodes the spatial tile
 * containing the pixel for each.
 */
SEXP C_vec_read_pixel_series(SEXP ptr_sexp,
                             SEXP col_sexp, SEXP row_sexp,
                             SEXP band_sexp, SEXP level_sexp) {
    VecrReader *r = unwrap_vecr_reader(ptr_sexp);
    int64_t col   = (int64_t)Rf_asInteger(col_sexp) - 1;
    int64_t row   = (int64_t)Rf_asInteger(row_sexp) - 1;
    int     band  = Rf_asInteger(band_sexp) - 1;
    int     level = Rf_asInteger(level_sexp);

    int n_time = vecr_reader_distinct_times(r, band, (uint8_t)level, NULL, 0);
    if (n_time <= 0)
        vectra_error("vec_read_pixel_series: no tiles match band/level");

    uint8_t dt  = vecr_reader_dtype(r);
    size_t  esz = vecr_dtype_size(dt);

    void *raw = malloc((size_t)n_time * esz);
    if (!raw) vectra_error("alloc failed");

    if (vecr_reader_read_pixel_series(r, col, row, band, (uint8_t)level, raw) != 0) {
        const char *msg = vecr_reader_errmsg(r);
        free(raw);
        vectra_error("vec_read_pixel_series: %s", msg);
    }

    SEXP out = PROTECT(Rf_allocVector(REALSXP, n_time));
    cast_dtype_to_doubles(raw, n_time, dt, REAL(out));
    int has_nd = vecr_reader_has_nodata(r);
    double nd  = vecr_reader_nodata(r);
    double *p  = REAL(out);
    for (int t = 0; t < n_time; ++t) {
        double v = p[t];
        if (has_nd) {
            if (dt == VECR_DT_F64 || dt == VECR_DT_F32) {
                if (isnan(nd) ? isnan(v) : v == nd) p[t] = NA_REAL;
            } else if (v == nd) p[t] = NA_REAL;
        } else if ((dt == VECR_DT_F64 || dt == VECR_DT_F32) && isnan(v)) {
            p[t] = NA_REAL;
        }
    }
    free(raw);
    UNPROTECT(1);
    return out;
}

/*
 * C_vec_raster_times(ptr, band, level) -> numeric(n_time) of distinct
 * time stamps for the given band/level. Pixel-major files always have
 * one consolidated table; image-major files derive theirs from the
 * tile index. Returns NULL when there are no tiles or no time stamps.
 */
SEXP C_vec_raster_times(SEXP ptr_sexp, SEXP band_sexp, SEXP level_sexp) {
    VecrReader *r = unwrap_vecr_reader(ptr_sexp);
    int band  = Rf_asInteger(band_sexp) - 1;
    int level = Rf_asInteger(level_sexp);
    if (band < 0) band = 0;
    if (level < 0) level = 0;

    int n = vecr_reader_distinct_times(r, band, (uint8_t)level, NULL, 0);
    if (n <= 0) return R_NilValue;
    int64_t *buf = (int64_t *)R_alloc(n, sizeof(int64_t));
    int got = vecr_reader_distinct_times(r, band, (uint8_t)level, buf, n);
    if (got <= 0) return R_NilValue;
    SEXP out = PROTECT(Rf_allocVector(REALSXP, got));
    double *p = REAL(out);
    for (int i = 0; i < got; ++i) p[i] = (double)buf[i];
    UNPROTECT(1);
    return out;
}

/* C_vec_raster_layout(ptr) -> character(1) "image" or "pixel". */
SEXP C_vec_raster_layout(SEXP ptr_sexp) {
    VecrReader *r = unwrap_vecr_reader(ptr_sexp);
    return Rf_mkString(vecr_reader_layout(r) == VECR_LAYOUT_PIXEL
                       ? "pixel" : "image");
}
