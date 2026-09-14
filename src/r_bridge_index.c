#include "r_bridge.h"
#include "r_bridge_internal.h"
#include "vtri.h"
#include "vtr1_tdc.h"
#include "schema.h"
#include "error.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* A fingerprint crosses into R as 16 hex digits: R has no unsigned 64-bit
   type, and a string round-trips every bit. */
static SEXP fingerprint_to_r(uint64_t fp) {
    char buf[17];
    snprintf(buf, sizeof(buf), "%016llx", (unsigned long long)fp);
    return Rf_mkString(buf);
}

static int fingerprint_from_r(SEXP x, uint64_t *out) {
    if (TYPEOF(x) != STRSXP || Rf_length(x) != 1 || STRING_ELT(x, 0) == NA_STRING)
        return 0;
    const char *s = CHAR(STRING_ELT(x, 0));
    if (strlen(s) != 16) return 0;
    char *end = NULL;
    unsigned long long v = strtoull(s, &end, 16);
    if (!end || *end != '\0') return 0;
    *out = (uint64_t)v;
    return 1;
}

/* --- C_store_fingerprint(path) ---
   The store's fingerprint as it now stands, taken before a store is grown in
   place so its indexes can be carried over only if they described it. NULL
   when the store cannot be opened. */

SEXP C_store_fingerprint(SEXP path) {
    Vtr1TdcFile *file = vtr1_open_tdc(CHAR(STRING_ELT(path, 0)));
    if (!file) return R_NilValue;
    uint64_t fp = vtr1_tdc_fingerprint(file);
    vtr1_close_tdc(file);
    return fingerprint_to_r(fp);
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

/* --- C_extend_index(path, vtri_path, pre_fingerprint, mem) ---
   Bring one sidecar up to date with a store that has just been grown in place,
   reading only the appended row groups. pre_fingerprint is the store's
   fingerprint before the append (C_store_fingerprint). TRUE when it was
   extended; FALSE when it cannot be (unreadable, or not built against the store
   as it was before the append), which tells the caller to rebuild it instead. */

SEXP C_extend_index(SEXP path, SEXP vtri_path, SEXP pre_fingerprint, SEXP mem) {
    const char *vtr_p  = CHAR(STRING_ELT(path, 0));
    const char *vtri_p = CHAR(STRING_ELT(vtri_path, 0));
    uint64_t pre = 0;
    if (!fingerprint_from_r(pre_fingerprint, &pre)) return Rf_ScalarLogical(0);
    return Rf_ScalarLogical(vtri_extend(vtr_p, vtri_p, pre,
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

    Vtr1TdcFile *file = vtr1_open_tdc(vtr_path);
    if (!file) {
        free(vtri_path);
        return Rf_ScalarLogical(0);
    }
    VtriStamp stamp;
    vtri_store_stamp(file, &stamp);
    vtr1_close_tdc(file);

    VtrIndex *idx = vtri_open(vtri_path, NULL, &stamp);
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
