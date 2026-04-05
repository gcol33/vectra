#include "collect.h"
#include "vec_omp.h"
#include "optimize.h"
#include "scan.h"
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

/* Find the 1-based factor level code for a string value, or NA_INTEGER. */
static int find_factor_code(const char *val, SEXP levels, int n_levels) {
    for (int j = 0; j < n_levels; j++) {
        if (strcmp(val, CHAR(STRING_ELT(levels, j))) == 0)
            return j + 1;
    }
    return NA_INTEGER;
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
            if (STRING_ELT(col, i) == NA_STRING)
                ip[i] = NA_INTEGER;
            else
                ip[i] = find_factor_code(CHAR(STRING_ELT(col, i)),
                                         levels, n_levels);
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
            if (vec_array_all_valid(arr)) {
                int warned = 0;
                for (int64_t i = 0; i < n; i++) {
                    int64_t v = arr->buf.i64[i];
                    out[i] = (double)v;
                    if (!warned && (v > (int64_t)1 << 53 || v < -((int64_t)1 << 53))) {
                        Rf_warning("int64 value exceeds 2^53; precision lost. "
                                   "Use options(vectra.int64 = \"bit64\") for exact representation.");
                        warned = 1;
                    }
                }
            } else {
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
            }
            UNPROTECT(1);
        }
        return col;
    }
    case VEC_DOUBLE: {
        col = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t)n));
        double *out = REAL(col);
        if (vec_array_all_valid(arr)) {
            memcpy(out, arr->buf.dbl, (size_t)n * sizeof(double));
        } else {
            for (int64_t i = 0; i < n; i++) {
                if (!vec_array_is_valid(arr, i))
                    out[i] = NA_REAL;
                else
                    out[i] = arr->buf.dbl[i];
            }
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

        /* CHARSXP cache: avoids repeated Rf_mkCharLenCE hash lookups for
           columns with many duplicate strings (common with dictionary encoding).
           Open-addressing hash table keyed on string content. */
        #define STR_CACHE_BITS 13
        #define STR_CACHE_SIZE (1 << STR_CACHE_BITS)
        #define STR_CACHE_MASK (STR_CACHE_SIZE - 1)
        typedef struct { uint32_t hash; int len; const char *ptr; SEXP sexp; } StrCacheSlot;
        int use_cache = (n > 256);
        StrCacheSlot *cache = NULL;
        if (use_cache) {
            cache = (StrCacheSlot *)calloc(STR_CACHE_SIZE, sizeof(StrCacheSlot));
            if (!cache) use_cache = 0;
        }

        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i)) {
                SET_STRING_ELT(col, (R_xlen_t)i, NA_STRING);
            } else {
                int64_t start = arr->buf.str.offsets[i];
                int64_t end = arr->buf.str.offsets[i + 1];
                int slen = (int)(end - start);
                const char *sptr = arr->buf.str.data + start;

                SEXP cs = R_NilValue;
                if (use_cache) {
                    /* FNV-1a content hash */
                    uint32_t h = 2166136261u;
                    for (int j = 0; j < slen; j++) {
                        h ^= (uint8_t)sptr[j];
                        h *= 16777619u;
                    }
                    h |= 1u; /* non-zero sentinel */
                    uint32_t slot = h & STR_CACHE_MASK;

                    for (int p = 0; p < 4; p++) {
                        uint32_t si = (slot + p) & STR_CACHE_MASK;
                        if (!cache[si].hash) {
                            cs = Rf_mkCharLenCE(sptr, slen, CE_UTF8);
                            cache[si].hash = h;
                            cache[si].len = slen;
                            cache[si].ptr = sptr;
                            cache[si].sexp = cs;
                            break;
                        }
                        if (cache[si].hash == h && cache[si].len == slen &&
                            memcmp(cache[si].ptr, sptr, (size_t)slen) == 0) {
                            cs = cache[si].sexp;
                            break;
                        }
                    }
                    if (cs == R_NilValue)
                        cs = Rf_mkCharLenCE(sptr, slen, CE_UTF8);
                } else {
                    cs = Rf_mkCharLenCE(sptr, slen, CE_UTF8);
                }
                SET_STRING_ELT(col, (R_xlen_t)i, cs);
            }
        }

        free(cache);
        #undef STR_CACHE_BITS
        #undef STR_CACHE_SIZE
        #undef STR_CACHE_MASK
        UNPROTECT(1);
        return col;
    }
    }
    return R_NilValue;
}

/* Copy batch column data directly into a pre-allocated R SEXP vector at offset.
   Returns the number of rows copied. */
static int64_t batch_to_sexp_direct(const VecBatch *batch, int col_idx,
                                    SEXP col, int64_t offset, int want_bit64,
                                    VecType type) {
    const VecArray *arr = &batch->columns[col_idx];
    int64_t n = arr->length;

    switch (type) {
    case VEC_INT64: {
        double *out = REAL(col) + offset;
        if (want_bit64) {
            if (vec_array_all_valid(arr)) {
                memcpy(out, arr->buf.i64, (size_t)n * sizeof(double));
            } else {
                for (int64_t i = 0; i < n; i++) {
                    if (!vec_array_is_valid(arr, i)) {
                        int64_t na_val = INT64_MIN;
                        memcpy(&out[i], &na_val, sizeof(double));
                    } else {
                        memcpy(&out[i], &arr->buf.i64[i], sizeof(double));
                    }
                }
            }
        } else {
            if (vec_array_all_valid(arr)) {
                for (int64_t i = 0; i < n; i++)
                    out[i] = (double)arr->buf.i64[i];
            } else {
                for (int64_t i = 0; i < n; i++) {
                    if (!vec_array_is_valid(arr, i))
                        out[i] = NA_REAL;
                    else
                        out[i] = (double)arr->buf.i64[i];
                }
            }
        }
        break;
    }
    case VEC_DOUBLE: {
        double *out = REAL(col) + offset;
        if (vec_array_all_valid(arr)) {
            memcpy(out, arr->buf.dbl, (size_t)n * sizeof(double));
        } else {
            for (int64_t i = 0; i < n; i++) {
                if (!vec_array_is_valid(arr, i))
                    out[i] = NA_REAL;
                else
                    out[i] = arr->buf.dbl[i];
            }
        }
        break;
    }
    case VEC_BOOL: {
        int *out = LOGICAL(col) + (int)offset;
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i))
                out[i] = NA_LOGICAL;
            else
                out[i] = arr->buf.bln[i] ? 1 : 0;
        }
        break;
    }
    case VEC_STRING:
        /* Strings are handled inline in the fast path below */
        break;
    }
    return n;
}

SEXP vec_collect(VecNode *root) {
    /* Optimize plan tree before execution */
    vec_optimize(root);

    const VecSchema *schema = &root->output_schema;
    int n_cols = schema->n_cols;
    int want_bit64 = use_bit64();
    int64_t hint = root->row_count_hint;

    /* ============================================================
     * FAST PATH: direct-to-R when row count is known.
     * Pre-allocate R vectors, copy batch data directly into them.
     * Falls back to builder path on selection vectors.
     * ============================================================ */
    if (hint > 0) {
        SEXP df = PROTECT(Rf_allocVector(VECSXP, n_cols));
        SEXP names_vec = PROTECT(Rf_allocVector(STRSXP, n_cols));

        /* Pre-allocate R column vectors */
        SEXP *cols = (SEXP *)malloc((size_t)n_cols * sizeof(SEXP));
        if (!cols) vectra_error("alloc failed");
        for (int i = 0; i < n_cols; i++) {
            VecType t = schema->col_types[i];
            if (t == VEC_INT64 || t == VEC_DOUBLE)
                cols[i] = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t)hint));
            else if (t == VEC_BOOL)
                cols[i] = PROTECT(Rf_allocVector(LGLSXP, (R_xlen_t)hint));
            else /* VEC_STRING */
                cols[i] = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t)hint));
        }

        /* CHARSXP cache for strings (shared across all string columns) */
        #define STR_CACHE_BITS 13
        #define STR_CACHE_SIZE (1 << STR_CACHE_BITS)
        #define STR_CACHE_MASK (STR_CACHE_SIZE - 1)
        typedef struct { uint32_t hash; int len; const char *ptr; SEXP sexp; } StrCacheSlot;
        StrCacheSlot *str_cache = NULL;
        int has_strings = 0;
        for (int i = 0; i < n_cols; i++) {
            if (schema->col_types[i] == VEC_STRING) { has_strings = 1; break; }
        }
        if (has_strings) {
            str_cache = (StrCacheSlot *)calloc(STR_CACHE_SIZE, sizeof(StrCacheSlot));
        }

        int64_t offset = 0;
        int fell_back = 0;
        VecBatch *batch;

        /* === PARALLEL I/O PATH ===
           When root is a plain ScanNode with multiple row groups and no
           predicates/tombstones, read all row groups in parallel using
           thread-local FILE handles, then fill R vectors sequentially. */
        int used_parallel = 0;
        if (scan_node_is_parallel_safe(root)) {
            const char *path = scan_node_get_path(root);
            Vtr1File *file = scan_node_get_file(root);
            const int *col_mask = scan_node_get_col_mask(root);
            uint32_t n_batches = 0;

            VecBatch **batches = vtr1_read_parallel(file, col_mask, path,
                                                    &n_batches);
            used_parallel = 1;

            for (uint32_t bi = 0; bi < n_batches; bi++) {
                batch = batches[bi];
                if (!batch) continue;
                int64_t n = batch->n_rows;

                for (int i = 0; i < n_cols; i++) {
                    VecType t = schema->col_types[i];
                    if (t != VEC_STRING) {
                        batch_to_sexp_direct(batch, i, cols[i], offset,
                                             want_bit64, t);
                    } else {
                        const VecArray *arr = &batch->columns[i];
                        for (int64_t j = 0; j < n; j++) {
                            int64_t ri = offset + j;
                            if (!vec_array_is_valid(arr, j)) {
                                SET_STRING_ELT(cols[i], (R_xlen_t)ri, NA_STRING);
                            } else {
                                int64_t start = arr->buf.str.offsets[j];
                                int64_t end = arr->buf.str.offsets[j + 1];
                                int slen = (int)(end - start);
                                const char *sptr = arr->buf.str.data + start;

                                SEXP cs = R_NilValue;
                                if (str_cache) {
                                    uint32_t h = 2166136261u;
                                    for (int k = 0; k < slen; k++) {
                                        h ^= (uint8_t)sptr[k];
                                        h *= 16777619u;
                                    }
                                    h |= 1u;
                                    uint32_t slot = h & STR_CACHE_MASK;
                                    for (int p = 0; p < 4; p++) {
                                        uint32_t si = (slot + p) & STR_CACHE_MASK;
                                        if (!str_cache[si].hash) {
                                            cs = Rf_mkCharLenCE(sptr, slen, CE_UTF8);
                                            str_cache[si].hash = h;
                                            str_cache[si].len = slen;
                                            str_cache[si].ptr = sptr;
                                            str_cache[si].sexp = cs;
                                            break;
                                        }
                                        if (str_cache[si].hash == h &&
                                            str_cache[si].len == slen &&
                                            memcmp(str_cache[si].ptr, sptr, (size_t)slen) == 0) {
                                            cs = str_cache[si].sexp;
                                            break;
                                        }
                                    }
                                    if (cs == R_NilValue)
                                        cs = Rf_mkCharLenCE(sptr, slen, CE_UTF8);
                                } else {
                                    cs = Rf_mkCharLenCE(sptr, slen, CE_UTF8);
                                }
                                SET_STRING_ELT(cols[i], (R_xlen_t)ri, cs);
                            }
                        }
                    }
                }
                offset += n;
                vec_batch_free(batch);
            }
            free(batches);
        }

        /* === SEQUENTIAL PATH === */
        if (!used_parallel) {
        while ((batch = root->next_batch(root)) != NULL) {
            if (batch->sel) {
                /* Selection vector present — can't do direct copy.
                   Fall back to builder path for remaining data. */
                fell_back = 1;
                /* We'll handle this batch and all remaining below */
                break;
            }

            int64_t n = batch->n_rows;

            /* Copy numeric columns directly into R vectors */
            for (int i = 0; i < n_cols; i++) {
                VecType t = schema->col_types[i];
                if (t != VEC_STRING) {
                    batch_to_sexp_direct(batch, i, cols[i], offset, want_bit64, t);
                } else {
                    /* Strings: Rf_mkCharLenCE with CHARSXP cache */
                    const VecArray *arr = &batch->columns[i];
                    for (int64_t j = 0; j < n; j++) {
                        int64_t ri = offset + j;
                        if (!vec_array_is_valid(arr, j)) {
                            SET_STRING_ELT(cols[i], (R_xlen_t)ri, NA_STRING);
                        } else {
                            int64_t start = arr->buf.str.offsets[j];
                            int64_t end = arr->buf.str.offsets[j + 1];
                            int slen = (int)(end - start);
                            const char *sptr = arr->buf.str.data + start;

                            SEXP cs = R_NilValue;
                            if (str_cache) {
                                uint32_t h = 2166136261u;
                                for (int k = 0; k < slen; k++) {
                                    h ^= (uint8_t)sptr[k];
                                    h *= 16777619u;
                                }
                                h |= 1u;
                                uint32_t slot = h & STR_CACHE_MASK;
                                for (int p = 0; p < 4; p++) {
                                    uint32_t si = (slot + p) & STR_CACHE_MASK;
                                    if (!str_cache[si].hash) {
                                        cs = Rf_mkCharLenCE(sptr, slen, CE_UTF8);
                                        str_cache[si].hash = h;
                                        str_cache[si].len = slen;
                                        str_cache[si].ptr = sptr;
                                        str_cache[si].sexp = cs;
                                        break;
                                    }
                                    if (str_cache[si].hash == h &&
                                        str_cache[si].len == slen &&
                                        memcmp(str_cache[si].ptr, sptr, (size_t)slen) == 0) {
                                        cs = str_cache[si].sexp;
                                        break;
                                    }
                                }
                                if (cs == R_NilValue)
                                    cs = Rf_mkCharLenCE(sptr, slen, CE_UTF8);
                            } else {
                                cs = Rf_mkCharLenCE(sptr, slen, CE_UTF8);
                            }
                            SET_STRING_ELT(cols[i], (R_xlen_t)ri, cs);
                        }
                    }
                }
            }
            offset += n;
            vec_batch_free(batch);
        }
        } /* end !used_parallel */

        free(str_cache);

        if (fell_back) {
            /* Selection vector appeared — use builders for remaining data.
               This should be rare for scan-only pipelines. */
            VecArrayBuilder *builders = (VecArrayBuilder *)calloc(
                (size_t)n_cols, sizeof(VecArrayBuilder));
            if (!builders) vectra_error("alloc failed");
            for (int i = 0; i < n_cols; i++)
                builders[i] = vec_builder_init(schema->col_types[i]);

            /* Process the batch that triggered fallback */
            if (batch) {
                int64_t n_logical = vec_batch_logical_rows(batch);
                for (int i = 0; i < n_cols; i++)
                    vec_builder_reserve(&builders[i], n_logical);
                for (int64_t li = 0; li < n_logical; li++) {
                    int64_t pi = vec_batch_physical_row(batch, li);
                    for (int i = 0; i < n_cols; i++)
                        vec_builder_append_one(&builders[i],
                                               &batch->columns[i], pi);
                }
                vec_batch_free(batch);
            }
            /* Process remaining batches */
            while ((batch = root->next_batch(root)) != NULL) {
                if (!batch->sel) {
                    for (int i = 0; i < n_cols; i++)
                        vec_builder_append_array(&builders[i], &batch->columns[i]);
                } else {
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

            /* Rebuild R vectors with correct size = offset + builder length */
            for (int i = 0; i < n_cols; i++) {
                VecArray arr = vec_builder_finish(&builders[i]);
                int64_t tail_n = arr.length;
                /* Append builder data into pre-allocated R vectors */
                VecType t = schema->col_types[i];
                if (t == VEC_DOUBLE) {
                    double *out = REAL(cols[i]) + offset;
                    if (vec_array_all_valid(&arr))
                        memcpy(out, arr.buf.dbl, (size_t)tail_n * sizeof(double));
                    else
                        for (int64_t j = 0; j < tail_n; j++)
                            out[j] = vec_array_is_valid(&arr, j) ? arr.buf.dbl[j] : NA_REAL;
                } else if (t == VEC_INT64) {
                    double *out = REAL(cols[i]) + offset;
                    for (int64_t j = 0; j < tail_n; j++) {
                        if (!vec_array_is_valid(&arr, j))
                            out[j] = NA_REAL;
                        else
                            out[j] = (double)arr.buf.i64[j];
                    }
                } else if (t == VEC_BOOL) {
                    int *out = LOGICAL(cols[i]) + (int)offset;
                    for (int64_t j = 0; j < tail_n; j++)
                        out[j] = vec_array_is_valid(&arr, j) ? (arr.buf.bln[j] ? 1 : 0) : NA_LOGICAL;
                } else {
                    for (int64_t j = 0; j < tail_n; j++) {
                        int64_t ri = offset + j;
                        if (!vec_array_is_valid(&arr, j))
                            SET_STRING_ELT(cols[i], (R_xlen_t)ri, NA_STRING);
                        else {
                            int64_t start = arr.buf.str.offsets[j];
                            int64_t end = arr.buf.str.offsets[j + 1];
                            SET_STRING_ELT(cols[i], (R_xlen_t)ri,
                                Rf_mkCharLenCE(arr.buf.str.data + start,
                                               (int)(end - start), CE_UTF8));
                        }
                    }
                }
                offset += tail_n;
                vec_array_free(&arr);
            }
            free(builders);
        }

        int64_t total_rows = offset;

        /* Shrink R vectors if actual rows < hint (e.g. after filter pruning) */
        if (total_rows < hint) {
            for (int i = 0; i < n_cols; i++) {
                SEXP old_col = cols[i];
                VecType t = schema->col_types[i];
                SEXP new_col;
                if (t == VEC_INT64 || t == VEC_DOUBLE) {
                    new_col = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t)total_rows));
                    memcpy(REAL(new_col), REAL(old_col), (size_t)total_rows * sizeof(double));
                } else if (t == VEC_BOOL) {
                    new_col = PROTECT(Rf_allocVector(LGLSXP, (R_xlen_t)total_rows));
                    memcpy(LOGICAL(new_col), LOGICAL(old_col), (size_t)total_rows * sizeof(int));
                } else {
                    new_col = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t)total_rows));
                    for (int64_t j = 0; j < total_rows; j++)
                        SET_STRING_ELT(new_col, (R_xlen_t)j, STRING_ELT(old_col, (R_xlen_t)j));
                }
                cols[i] = new_col;
                /* old_col protected, will be UNPROTECT'd with the batch */
            }
            /* Extra protects for new columns */
            /* Adjust: unprotect old columns (n_cols), new columns already protected */
        }

        /* Apply annotations and build data.frame */
        for (int i = 0; i < n_cols; i++) {
            SEXP col = cols[i];
            if (want_bit64 && schema->col_types[i] == VEC_INT64)
                Rf_setAttrib(col, R_ClassSymbol, Rf_mkString("integer64"));
            const char *ann = (schema->col_annotations)
                              ? schema->col_annotations[i] : NULL;
            col = apply_annotation(col, ann);
            SET_VECTOR_ELT(df, i, col);
            SET_STRING_ELT(names_vec, i,
                Rf_mkCharCE(schema->col_names[i], CE_UTF8));
        }
        free(cols);

        Rf_setAttrib(df, R_NamesSymbol, names_vec);
        SEXP row_names = PROTECT(Rf_allocVector(INTSXP, 2));
        INTEGER(row_names)[0] = NA_INTEGER;
        INTEGER(row_names)[1] = -(int)total_rows;
        Rf_setAttrib(df, R_RowNamesSymbol, row_names);
        Rf_setAttrib(df, R_ClassSymbol, Rf_mkString("data.frame"));

        /* Unprotect: df + names_vec + n_cols columns + row_names
           (+ n_cols extra if shrunk, but those replaced the originals) */
        int n_protect = 3 + n_cols;
        if (total_rows < hint) n_protect += n_cols;
        UNPROTECT(n_protect);
        return df;
    }

    /* ============================================================
     * BUILDER PATH: fallback when row count is unknown.
     * ============================================================ */

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
            #pragma omp parallel for if(n_cols > 8) schedule(static)
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
