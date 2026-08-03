#include "r_bridge.h"
#include "r_bridge_internal.h"
#include "vtri.h"
#include "vtr1_tdc.h"
#include "schema.h"
#include "error.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Total rows and row groups of a .vtr store, for verifying an index stamp. */
static int store_stamp(const char *vtr_path, int64_t *n_rows,
                       int64_t *n_rowgroups) {
    Vtr1TdcFile *file = vtr1_open_tdc(vtr_path);
    if (!file) return 0;
    uint32_t n_rg = vtr1_tdc_n_rowgroups(file);
    int64_t total = 0;
    for (uint32_t rg = 0; rg < n_rg; rg++)
        total += vtr1_tdc_rowgroup_n_rows(file, rg);
    vtr1_close_tdc(file);
    *n_rows = total;
    *n_rowgroups = (int64_t)n_rg;
    return 1;
}

/* Canonical .vtri path for a set of column names: schema order, so that
   create_index(), has_index(), and the scan-side probe all name the same file. */
static char *canonical_index_path(const char *vtr_path, SEXP col_name) {
    int n_cols = Rf_length(col_name);
    if (n_cols < 1 || n_cols > VTRI_MAX_COLS) return NULL;

    Vtr1TdcFile *file = vtr1_open_tdc(vtr_path);
    if (!file) return NULL;

    const char *in_names[VTRI_MAX_COLS];
    for (int i = 0; i < n_cols; i++)
        in_names[i] = CHAR(STRING_ELT(col_name, i));

    int col_idx[VTRI_MAX_COLS];
    const char *sorted[VTRI_MAX_COLS];
    char *path = NULL;
    if (vtri_resolve_cols(vtr1_tdc_schema(file), in_names, n_cols,
                          col_idx, sorted, NULL, NULL))
        path = vtri_make_path_composite(vtr_path, sorted, n_cols);

    vtr1_close_tdc(file);
    return path;
}

/* --- C_create_index(path, col_name, ci, mem) ---
   mem is the sort budget the entries spill past, so building an index over a
   store larger than memory costs disk rather than RAM. */

SEXP C_create_index(SEXP path, SEXP col_name, SEXP ci, SEXP mem) {
    const char *vtr_path = CHAR(STRING_ELT(path, 0));
    int ci_flag = Rf_asLogical(ci);
    int n_cols = Rf_length(col_name);

    if (n_cols < 1 || n_cols > VTRI_MAX_COLS)
        vectra_error("an index spans 1 to %d columns, got %d",
                     VTRI_MAX_COLS, n_cols);

    const char *col_names[VTRI_MAX_COLS];
    for (int i = 0; i < n_cols; i++)
        col_names[i] = CHAR(STRING_ELT(col_name, i));

    vtri_build(vtr_path, col_names, n_cols, ci_flag,
               (int64_t)Rf_asReal(mem), get_r_tempdir());
    return R_NilValue;
}

/* --- C_extend_index(path, vtri_path, mem) ---
   Bring one sidecar up to date with a store that has just gained row groups,
   reading only the appended ones. TRUE when it was extended; FALSE when it
   cannot be (unreadable, or built against a store this one is not an extension
   of), which tells the caller to rebuild it instead. */

SEXP C_extend_index(SEXP path, SEXP vtri_path, SEXP mem) {
    const char *vtr_p  = CHAR(STRING_ELT(path, 0));
    const char *vtri_p = CHAR(STRING_ELT(vtri_path, 0));
    return Rf_ScalarLogical(vtri_extend(vtr_p, vtri_p,
                                        (int64_t)Rf_asReal(mem),
                                        get_r_tempdir()));
}

/* --- C_has_index(path, col_name) ---
   TRUE only when the index can actually be used: present, in the current
   format, and matching the store as it is now. */

SEXP C_has_index(SEXP path, SEXP col_name) {
    const char *vtr_path = CHAR(STRING_ELT(path, 0));

    char *vtri_path = canonical_index_path(vtr_path, col_name);
    if (!vtri_path) return Rf_ScalarLogical(0);

    int64_t n_rows = -1, n_rgs = -1;
    if (!store_stamp(vtr_path, &n_rows, &n_rgs)) {
        free(vtri_path);
        return Rf_ScalarLogical(0);
    }

    VtrIndex *idx = vtri_open(vtri_path, NULL, n_rows, n_rgs);
    free(vtri_path);
    if (!idx) return Rf_ScalarLogical(0);
    vtri_close(idx);
    return Rf_ScalarLogical(1);
}

/* --- C_index_spec(path, vtri_path) ---
   The column names a .vtri file indexes, read from its header for any format
   version, so an index can be rebuilt from what it was built on. Returns NULL
   if the file is not readable as an index or names columns this store lacks. */

SEXP C_index_spec(SEXP path, SEXP vtri_path) {
    const char *vtr_path = CHAR(STRING_ELT(path, 0));
    const char *ix_path  = CHAR(STRING_ELT(vtri_path, 0));

    uint16_t col_indices[VTRI_MAX_COLS];
    int ci = 0;
    int n_cols = vtri_read_spec(ix_path, col_indices, &ci);
    if (n_cols < 1) return R_NilValue;

    Vtr1TdcFile *file = vtr1_open_tdc(vtr_path);
    if (!file) return R_NilValue;
    const VecSchema *schema = vtr1_tdc_schema(file);

    for (int c = 0; c < n_cols; c++) {
        if (col_indices[c] >= (uint16_t)schema->n_cols) {
            vtr1_close_tdc(file);
            return R_NilValue;
        }
    }

    SEXP cols = PROTECT(Rf_allocVector(STRSXP, n_cols));
    for (int c = 0; c < n_cols; c++)
        SET_STRING_ELT(cols, c, Rf_mkChar(schema->col_names[col_indices[c]]));
    vtr1_close_tdc(file);

    SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));
    SET_VECTOR_ELT(out, 0, cols);
    SET_VECTOR_ELT(out, 1, Rf_ScalarLogical(ci));
    SEXP nms = PROTECT(Rf_allocVector(STRSXP, 2));
    SET_STRING_ELT(nms, 0, Rf_mkChar("columns"));
    SET_STRING_ELT(nms, 1, Rf_mkChar("ci"));
    Rf_setAttrib(out, R_NamesSymbol, nms);
    UNPROTECT(3);
    return out;
}
