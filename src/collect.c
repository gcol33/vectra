#include "collect.h"
#include "builder.h"
#include "array.h"
#include "batch.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* Check if bit64 int64 mode is requested */
static int use_bit64(void) {
    SEXP opt = Rf_GetOption1(Rf_install("vectra.int64"));
    if (opt == R_NilValue) return 0;
    if (TYPEOF(opt) == STRSXP && LENGTH(opt) == 1) {
        const char *s = CHAR(STRING_ELT(opt, 0));
        if (strcmp(s, "bit64") == 0) return 1;
    }
    return 0;
}

/* Convert a VecArray to an R SEXP column */
static SEXP array_to_sexp(const VecArray *arr, int want_bit64) {
    SEXP col;
    int64_t n = arr->length;

    switch (arr->type) {
    case VEC_INT64: {
        if (want_bit64) {
            /* Return as bit64::integer64 (raw doubles reinterpreted) */
            col = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t)n));
            double *out = REAL(col);
            for (int64_t i = 0; i < n; i++) {
                if (!vec_array_is_valid(arr, i)) {
                    /* NA for integer64 is INT64_MIN stored as double bits */
                    int64_t na_val = INT64_MIN;
                    memcpy(&out[i], &na_val, sizeof(double));
                } else {
                    memcpy(&out[i], &arr->buf.i64[i], sizeof(double));
                }
            }
            Rf_setAttrib(col, R_ClassSymbol, Rf_mkString("integer64"));
            UNPROTECT(1);
        } else {
            /* Convert to R double, warn if precision loss */
            col = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t)n));
            double *out = REAL(col);
            int warned = 0;
            for (int64_t i = 0; i < n; i++) {
                if (!vec_array_is_valid(arr, i)) {
                    out[i] = NA_REAL;
                } else {
                    int64_t v = arr->buf.i64[i];
                    out[i] = (double)v;
                    if (!warned && (v > (int64_t)1 << 53 || v < -((int64_t)1 << 53))) {
                        Rf_warning("int64 value exceeds 2^53; precision lost. "
                                   "Use options(vectra.int64 = \"bit64\") for exact representation.");
                        warned = 1;
                    }
                }
            }
            UNPROTECT(1);
        }
        return col;
    }
    case VEC_DOUBLE: {
        col = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t)n));
        double *out = REAL(col);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i))
                out[i] = NA_REAL;
            else
                out[i] = arr->buf.dbl[i];
        }
        UNPROTECT(1);
        return col;
    }
    case VEC_BOOL: {
        col = PROTECT(Rf_allocVector(LGLSXP, (R_xlen_t)n));
        int *out = LOGICAL(col);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i))
                out[i] = NA_LOGICAL;
            else
                out[i] = arr->buf.bln[i] ? 1 : 0;
        }
        UNPROTECT(1);
        return col;
    }
    case VEC_STRING: {
        col = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t)n));
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i)) {
                SET_STRING_ELT(col, (R_xlen_t)i, NA_STRING);
            } else {
                int64_t start = arr->buf.str.offsets[i];
                int64_t end = arr->buf.str.offsets[i + 1];
                int64_t slen = end - start;
                SET_STRING_ELT(col, (R_xlen_t)i,
                    Rf_mkCharLenCE(arr->buf.str.data + start, (int)slen, CE_UTF8));
            }
        }
        UNPROTECT(1);
        return col;
    }
    }
    return R_NilValue;
}

SEXP vec_collect(VecNode *root) {
    const VecSchema *schema = &root->output_schema;
    int n_cols = schema->n_cols;
    int want_bit64 = use_bit64();

    /* Initialize builders */
    VecArrayBuilder *builders = (VecArrayBuilder *)calloc(
        (size_t)n_cols, sizeof(VecArrayBuilder));
    if (!builders) vectra_error("alloc failed for builders");

    for (int i = 0; i < n_cols; i++)
        builders[i] = vec_builder_init(schema->col_types[i]);

    /* Pull batches */
    VecBatch *batch;
    while ((batch = root->next_batch(root)) != NULL) {
        for (int i = 0; i < n_cols; i++)
            vec_builder_append_array(&builders[i], &batch->columns[i]);
        vec_batch_free(batch);
    }

    /* Finish builders -> arrays -> R columns */
    SEXP df = PROTECT(Rf_allocVector(VECSXP, n_cols));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, n_cols));
    int64_t total_rows = 0;

    for (int i = 0; i < n_cols; i++) {
        VecArray arr = vec_builder_finish(&builders[i]);
        if (i == 0) total_rows = arr.length;
        SET_VECTOR_ELT(df, i, array_to_sexp(&arr, want_bit64));
        SET_STRING_ELT(names, i,
            Rf_mkCharCE(schema->col_names[i], CE_UTF8));
        vec_array_free(&arr);
    }

    free(builders);

    /* Set data.frame attributes */
    Rf_setAttrib(df, R_NamesSymbol, names);

    SEXP row_names = PROTECT(Rf_allocVector(INTSXP, 2));
    INTEGER(row_names)[0] = NA_INTEGER;
    INTEGER(row_names)[1] = -(int)total_rows;
    Rf_setAttrib(df, R_RowNamesSymbol, row_names);
    Rf_setAttrib(df, R_ClassSymbol, Rf_mkString("data.frame"));

    UNPROTECT(3);
    return df;
}
