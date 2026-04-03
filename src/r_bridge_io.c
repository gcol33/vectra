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

