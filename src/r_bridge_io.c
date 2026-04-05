#include "r_bridge.h"
#include "r_bridge_internal.h"
#include "types.h"
#include "csv_write.h"
#include "csv_scan.h"
#include "sql_scan.h"
#include "sql_write.h"
#include "tiff_scan.h"
#include "tiff_write.h"
#include "vtr_write.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* ---- shared helpers ---- */

/* Scan helper: path + batch_size -> format-specific constructor -> wrapped node.
 * Covers the csv and tiff scan nodes which share identical structure. */
typedef VecNode *(*ScanCreateFn)(const char *path, int64_t batch_size);

static SEXP scan_node_create(SEXP path_sexp, SEXP batch_size_sexp,
                             ScanCreateFn create_fn) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));
    int64_t batch_size = (int64_t)Rf_asReal(batch_size_sexp);
    VecNode *sn = create_fn(fpath, batch_size);
    return wrap_node(sn);
}

/* Write helper: unwrap node, extract path, call format-specific writer, free node.
 * The writer receives (node, path, ctx) where ctx carries any extra parameters. */
typedef void (*WriteNodeFn)(VecNode *node, const char *path, void *ctx);

static SEXP write_node_dispatch(SEXP node_xptr, SEXP path_sexp,
                                WriteNodeFn write_fn, void *ctx) {
    VecNode *node = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    write_fn(node, path, ctx);
    node->free_node(node);
    return R_NilValue;
}

/* Format-specific writer adapters (bridge void* ctx to real signatures) */

static void csv_writer(VecNode *node, const char *path, void *ctx) {
    (void)ctx;
    csv_write_node(node, path);
}

static void sql_writer(VecNode *node, const char *path, void *ctx) {
    const char *table = (const char *)ctx;
    sql_write_node(node, path, table);
}

static void tiff_writer(VecNode *node, const char *path, void *ctx) {
    int use_deflate = *(int *)ctx;
    tiff_write_node(node, path, use_deflate);
}

typedef struct {
    int use_deflate;
    int pixel_type;
    const char *metadata_xml;
} TiffTypedCtx;

static void tiff_typed_writer(VecNode *node, const char *path, void *ctx) {
    TiffTypedCtx *tc = (TiffTypedCtx *)ctx;
    tiff_write_node_typed(node, path, tc->use_deflate, tc->pixel_type,
                          tc->metadata_xml);
}

typedef struct {
    int64_t batch_size;
} VtrWriteCtx;

static void vtr_writer(VecNode *node, const char *path, void *ctx) {
    VtrWriteCtx *wctx = (VtrWriteCtx *)ctx;
    if (wctx->batch_size > 0)
        vtr_write_node_batched(node, path, wctx->batch_size);
    else
        vtr_write_node(node, path);
}

/* ---- scan entry points ---- */

static VecNode *csv_scan_adapter(const char *path, int64_t bs) {
    return (VecNode *)csv_scan_node_create(path, bs);
}

static VecNode *tiff_scan_adapter(const char *path, int64_t bs) {
    return (VecNode *)tiff_scan_node_create(path, bs);
}

SEXP C_csv_scan_node(SEXP path_sexp, SEXP batch_size_sexp) {
    return scan_node_create(path_sexp, batch_size_sexp, csv_scan_adapter);
}

SEXP C_sql_scan_node(SEXP path_sexp, SEXP table_sexp, SEXP batch_size_sexp) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));
    const char *table = CHAR(STRING_ELT(table_sexp, 0));
    int64_t batch_size = (int64_t)Rf_asReal(batch_size_sexp);
    SqlScanNode *sn = sql_scan_node_create(fpath, table, batch_size);
    return wrap_node((VecNode *)sn);
}

SEXP C_tiff_scan_node(SEXP path_sexp, SEXP batch_size_sexp) {
    return scan_node_create(path_sexp, batch_size_sexp, tiff_scan_adapter);
}

/* ---- write entry points ---- */

SEXP C_write_csv(SEXP node_xptr, SEXP path_sexp) {
    return write_node_dispatch(node_xptr, path_sexp, csv_writer, NULL);
}

SEXP C_write_sqlite(SEXP node_xptr, SEXP path_sexp, SEXP table_sexp) {
    const char *table = CHAR(STRING_ELT(table_sexp, 0));
    return write_node_dispatch(node_xptr, path_sexp, sql_writer, (void *)table);
}

SEXP C_write_tiff(SEXP node_xptr, SEXP path_sexp, SEXP compress_sexp) {
    int use_deflate = Rf_asLogical(compress_sexp);
    return write_node_dispatch(node_xptr, path_sexp, tiff_writer, &use_deflate);
}

SEXP C_write_vtr_node(SEXP node_xptr, SEXP path_sexp, SEXP batch_size_sexp) {
    VtrWriteCtx ctx = {
        .batch_size = (batch_size_sexp == R_NilValue) ? 0 : (int64_t)Rf_asReal(batch_size_sexp)
    };
    return write_node_dispatch(node_xptr, path_sexp, vtr_writer, &ctx);
}

/* --- C_tiff_extract_points --- */

SEXP C_tiff_extract_points(SEXP path_sexp, SEXP x_sexp, SEXP y_sexp) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));
    int64_t n = Rf_xlength(x_sexp);

    TiffReader *reader = NULL;
    if (tiff_reader_open(fpath, &reader) != 0) {
        const char *msg = reader ? tiff_reader_errmsg(reader) : "unknown";
        tiff_reader_close(reader);
        vectra_error("cannot open GeoTIFF: %s", msg);
    }

    int nb = tiff_reader_nbands(reader);

    /* Allocate output band arrays */
    double **bands = (double **)malloc((size_t)nb * sizeof(double *));
    if (!bands) {
        tiff_reader_close(reader);
        vectra_error("alloc failed for point extraction");
    }
    for (int b = 0; b < nb; b++) {
        bands[b] = (double *)malloc((size_t)n * sizeof(double));
        if (!bands[b]) {
            for (int j = 0; j < b; j++) free(bands[j]);
            free(bands);
            tiff_reader_close(reader);
            vectra_error("alloc failed for band data");
        }
    }

    /* Extract */
    if (tiff_reader_extract_points(reader, n, REAL(x_sexp), REAL(y_sexp),
                                    bands) != 0) {
        const char *msg = tiff_reader_errmsg(reader);
        for (int b = 0; b < nb; b++) free(bands[b]);
        free(bands);
        tiff_reader_close(reader);
        vectra_error("TIFF extract error: %s", msg);
    }

    tiff_reader_close(reader);

    /* Build R data.frame: x, y, band1, band2, ... */
    int n_cols = 2 + nb;
    SEXP result = PROTECT(Rf_allocVector(VECSXP, n_cols));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, n_cols));

    /* x column (copy input) */
    SEXP x_out = PROTECT(Rf_allocVector(REALSXP, n));
    memcpy(REAL(x_out), REAL(x_sexp), (size_t)n * sizeof(double));
    SET_VECTOR_ELT(result, 0, x_out);
    SET_STRING_ELT(names, 0, Rf_mkChar("x"));
    UNPROTECT(1); /* x_out */

    /* y column (copy input) */
    SEXP y_out = PROTECT(Rf_allocVector(REALSXP, n));
    memcpy(REAL(y_out), REAL(y_sexp), (size_t)n * sizeof(double));
    SET_VECTOR_ELT(result, 1, y_out);
    SET_STRING_ELT(names, 1, Rf_mkChar("y"));
    UNPROTECT(1); /* y_out */

    /* Band columns */
    for (int b = 0; b < nb; b++) {
        SEXP col = PROTECT(Rf_allocVector(REALSXP, n));
        double *dst = REAL(col);
        for (int64_t i = 0; i < n; i++) {
            dst[i] = isnan(bands[b][i]) ? NA_REAL : bands[b][i];
        }
        SET_VECTOR_ELT(result, 2 + b, col);
        char bname[16];
        snprintf(bname, 16, "band%d", b + 1);
        SET_STRING_ELT(names, 2 + b, Rf_mkChar(bname));
        UNPROTECT(1); /* col */
        free(bands[b]);
    }
    free(bands);

    /* Set as data.frame */
    Rf_setAttrib(result, R_NamesSymbol, names);
    SEXP rownames = PROTECT(Rf_allocVector(INTSXP, 2));
    INTEGER(rownames)[0] = NA_INTEGER;
    INTEGER(rownames)[1] = -(int)n;
    Rf_setAttrib(result, R_RowNamesSymbol, rownames);
    Rf_setAttrib(result, R_ClassSymbol, Rf_mkString("data.frame"));

    UNPROTECT(3); /* result, names, rownames */
    return result;
}

/* --- C_write_tiff_typed --- */

SEXP C_write_tiff_typed(SEXP node_xptr, SEXP path_sexp,
                        SEXP compress_sexp, SEXP pixel_type_sexp,
                        SEXP metadata_sexp) {
    TiffTypedCtx ctx;
    ctx.use_deflate = Rf_asLogical(compress_sexp);
    ctx.pixel_type = Rf_asInteger(pixel_type_sexp);
    ctx.metadata_xml = (metadata_sexp == R_NilValue)
                        ? NULL : CHAR(STRING_ELT(metadata_sexp, 0));
    return write_node_dispatch(node_xptr, path_sexp, tiff_typed_writer, &ctx);
}

/* --- C_tiff_read_metadata --- */

SEXP C_tiff_read_metadata(SEXP path_sexp) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));

    TiffReader *reader = NULL;
    if (tiff_reader_open(fpath, &reader) != 0) {
        const char *msg = reader ? tiff_reader_errmsg(reader) : "unknown";
        tiff_reader_close(reader);
        vectra_error("cannot open GeoTIFF: %s", msg);
    }

    const char *meta = tiff_reader_metadata(reader);
    SEXP result;
    if (meta) {
        result = PROTECT(Rf_mkString(meta));
    } else {
        result = PROTECT(Rf_ScalarString(NA_STRING));
    }

    tiff_reader_close(reader);
    UNPROTECT(1);
    return result;
}

/* --- C_tiff_scan_meta --- */

SEXP C_tiff_scan_meta(SEXP node_xptr) {
    VecNode *node = unwrap_node(node_xptr);
    if (!node->kind || strcmp(node->kind, "TiffScanNode") != 0)
        vectra_error("not a TiffScanNode");
    TiffScanNode *sn = (TiffScanNode *)node;
    TiffReader *r = sn->reader;

    SEXP result = PROTECT(Rf_allocVector(VECSXP, 5));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, 5));

    SET_STRING_ELT(names, 0, Rf_mkChar("width"));
    SET_STRING_ELT(names, 1, Rf_mkChar("height"));
    SET_STRING_ELT(names, 2, Rf_mkChar("nbands"));
    SET_STRING_ELT(names, 3, Rf_mkChar("gt"));
    SET_STRING_ELT(names, 4, Rf_mkChar("nodata"));

    SET_VECTOR_ELT(result, 0, Rf_ScalarReal((double)tiff_reader_width(r)));
    SET_VECTOR_ELT(result, 1, Rf_ScalarReal((double)tiff_reader_height(r)));
    SET_VECTOR_ELT(result, 2, Rf_ScalarInteger(tiff_reader_nbands(r)));

    SEXP gt_sexp = PROTECT(Rf_allocVector(REALSXP, 6));
    const double *gt = tiff_reader_geotransform(r);
    memcpy(REAL(gt_sexp), gt, 6 * sizeof(double));
    SET_VECTOR_ELT(result, 3, gt_sexp);

    if (tiff_reader_has_nodata(r))
        SET_VECTOR_ELT(result, 4, Rf_ScalarReal(tiff_reader_nodata(r)));
    else
        SET_VECTOR_ELT(result, 4, Rf_ScalarReal(NA_REAL));

    Rf_setAttrib(result, R_NamesSymbol, names);
    UNPROTECT(3);
    return result;
}

