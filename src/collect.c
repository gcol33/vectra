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

/* Convert VecArray to R SEXP, then apply annotation (Date/POSIXct/factor).
   Returns a new SEXP if factor, or the same col with class attrs set. */
static SEXP apply_annotation(SEXP col, const char *ann) {
    if (!ann) return col;

    if (strcmp(ann, "Date") == 0) {
        Rf_setAttrib(col, R_ClassSymbol, Rf_mkString("Date"));
        return col;
    }
    if (strncmp(ann, "POSIXct|", 8) == 0) {
        const char *tz = ann + 8;
        SEXP cls = PROTECT(Rf_allocVector(STRSXP, 2));
        SET_STRING_ELT(cls, 0, Rf_mkChar("POSIXct"));
        SET_STRING_ELT(cls, 1, Rf_mkChar("POSIXt"));
        Rf_setAttrib(col, R_ClassSymbol, cls);
        if (tz[0] != '\0')
            Rf_setAttrib(col, Rf_install("tzone"), Rf_mkString(tz));
        UNPROTECT(1);
        return col;
    }
    if (strncmp(ann, "factor", 6) == 0) {
        /* "factor|lev1|lev2|..." -> convert string column to factor */
        R_xlen_t n = XLENGTH(col);
        /* Parse levels */
        int n_levels = 0;
        const char *p = ann + 6;
        while (*p == '|') { n_levels++; p++; while (*p && *p != '|') p++; }
        SEXP levels = PROTECT(Rf_allocVector(STRSXP, n_levels));
        p = ann + 6;
        for (int i = 0; i < n_levels; i++) {
            p++; /* skip '|' */
            const char *start = p;
            while (*p && *p != '|') p++;
            SET_STRING_ELT(levels, i, Rf_mkCharLen(start, (int)(p - start)));
        }
        /* Convert strings to integer codes */
        SEXP icol = PROTECT(Rf_allocVector(INTSXP, n));
        int *ip = INTEGER(icol);
        for (R_xlen_t i = 0; i < n; i++) {
            if (STRING_ELT(col, i) == NA_STRING) {
                ip[i] = NA_INTEGER;
            } else {
                const char *val = CHAR(STRING_ELT(col, i));
                ip[i] = NA_INTEGER;
                for (int j = 0; j < n_levels; j++) {
                    if (strcmp(val, CHAR(STRING_ELT(levels, j))) == 0) {
                        ip[i] = j + 1;
                        break;
                    }
                }
            }
        }
        Rf_setAttrib(icol, R_LevelsSymbol, levels);
        Rf_setAttrib(icol, R_ClassSymbol, Rf_mkString("factor"));
        UNPROTECT(2);
        return icol;
    }
    return col;
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

    /* Pull batches (sel-aware) */
    VecBatch *batch;
    while ((batch = root->next_batch(root)) != NULL) {
        if (!batch->sel) {
            /* Fast path: no selection vector, bulk append */
            for (int i = 0; i < n_cols; i++)
                vec_builder_append_array(&builders[i], &batch->columns[i]);
        } else {
            /* Selection vector: append selected rows one by one */
            int64_t n_logical = vec_batch_logical_rows(batch);
            for (int i = 0; i < n_cols; i++)
                vec_builder_reserve(&builders[i], n_logical);
            for (int64_t li = 0; li < n_logical; li++) {
                int64_t pi = vec_batch_physical_row(batch, li);
                for (int i = 0; i < n_cols; i++)
                    vec_builder_append_one(&builders[i],
                                           &batch->columns[i], pi);
            }
        }
        vec_batch_free(batch);
    }

    /* Finish builders -> arrays -> R columns */
    SEXP df = PROTECT(Rf_allocVector(VECSXP, n_cols));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, n_cols));
    int64_t total_rows = 0;

    for (int i = 0; i < n_cols; i++) {
        VecArray arr = vec_builder_finish(&builders[i]);
        if (i == 0) total_rows = arr.length;
        SEXP col = array_to_sexp(&arr, want_bit64);
        /* Apply type annotation (Date, POSIXct, factor) */
        const char *ann = (schema->col_annotations)
                          ? schema->col_annotations[i] : NULL;
        col = apply_annotation(col, ann);
        SET_VECTOR_ELT(df, i, col);
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
