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

/* --- C_csv_scan_node --- */

SEXP C_csv_scan_node(SEXP path_sexp, SEXP batch_size_sexp) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));
    int64_t batch_size = (int64_t)Rf_asReal(batch_size_sexp);
    CsvScanNode *sn = csv_scan_node_create(fpath, batch_size);
    return wrap_node((VecNode *)sn);
}

/* --- C_write_csv --- */

SEXP C_write_csv(SEXP node_xptr, SEXP path_sexp) {
    VecNode *node = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    csv_write_node(node, path);
    node->free_node(node);
    return R_NilValue;
}

/* --- C_sql_scan_node --- */

SEXP C_sql_scan_node(SEXP path_sexp, SEXP table_sexp, SEXP batch_size_sexp) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));
    const char *table = CHAR(STRING_ELT(table_sexp, 0));
    int64_t batch_size = (int64_t)Rf_asReal(batch_size_sexp);
    SqlScanNode *sn = sql_scan_node_create(fpath, table, batch_size);
    return wrap_node((VecNode *)sn);
}

/* --- C_write_sqlite --- */

SEXP C_write_sqlite(SEXP node_xptr, SEXP path_sexp, SEXP table_sexp) {
    VecNode *node = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    const char *table = CHAR(STRING_ELT(table_sexp, 0));
    sql_write_node(node, path, table);
    node->free_node(node);
    return R_NilValue;
}

/* --- C_tiff_scan_node --- */

SEXP C_tiff_scan_node(SEXP path_sexp, SEXP batch_size_sexp) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));
    int64_t batch_size = (int64_t)Rf_asReal(batch_size_sexp);
    TiffScanNode *sn = tiff_scan_node_create(fpath, batch_size);
    return wrap_node((VecNode *)sn);
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

/* --- C_write_tiff --- */

SEXP C_write_tiff(SEXP node_xptr, SEXP path_sexp, SEXP compress_sexp) {
    VecNode *node = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    int use_deflate = Rf_asLogical(compress_sexp);
    tiff_write_node(node, path, use_deflate);
    node->free_node(node);
    return R_NilValue;
}

/* --- C_write_vtr_node (streaming write) --- */

SEXP C_write_vtr_node(SEXP node_xptr, SEXP path_sexp) {
    VecNode *node = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    vtr_write_node(node, path);
    node->free_node(node);
    return R_NilValue;
}
