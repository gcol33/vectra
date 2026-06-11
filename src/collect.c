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

/* CHARSXP cache: avoids repeated Rf_mkCharLenCE hash lookups for columns with
   many duplicate strings (common with dictionary encoding). Open-addressing
   hash table keyed on string content. Shared by all string-fill paths. */
#define STR_CACHE_BITS 13
#define STR_CACHE_SIZE (1 << STR_CACHE_BITS)
#define STR_CACHE_MASK (STR_CACHE_SIZE - 1)
/* Cache slot for de-duplicating CHARSXP creation across batches.
 * We intentionally do NOT store a raw pointer into the decoder's heap
 * buffer: that buffer belongs to a VecBatch and is freed between
 * consumer iterations, which would make any cross-batch memcmp
 * dereference freed memory (classic use-after-free). Instead we keep
 * the already-interned CHARSXP and read its body via CHAR() when we
 * need to verify a hash match. The CHARSXP is kept alive by the output
 * STRSXP (which was PROTECTed before any SET_STRING_ELT), so its body
 * pointer is stable for the lifetime of this collect. */
typedef struct { uint32_t hash; int len; SEXP sexp; } StrCacheSlot;

/* Fill a slice of an R STRSXP from a VecArray's flat string buffer. With
 * str_cache != NULL, duplicate values skip R's CHARSXP hash via a content-hash
 * cache; with str_cache == NULL, every row pays a plain Rf_mkCharLenCE. */
static void fill_string_col_from_batch(SEXP col, int64_t offset,
                                       const VecArray *arr, int64_t n,
                                       StrCacheSlot *str_cache) {
    for (int64_t j = 0; j < n; j++) {
        int64_t ri = offset + j;
        if (!vec_array_is_valid(arr, j)) {
            SET_STRING_ELT(col, (R_xlen_t)ri, NA_STRING);
            continue;
        }
        int64_t start = arr->buf.str.offsets[j];
        int64_t end = arr->buf.str.offsets[j + 1];
        int slen = (int)(end - start);

        /* Empty strings: shortcut to R's interned empty CHARSXP, skipping
         * the cache and Rf_mkCharLenCE entirely. arr->buf.str.data may be
         * NULL when every row in the batch is empty/NA, and feeding NULL
         * into memcmp / Rf_mkCharLenCE trips UBSAN's nonnull check even
         * though the length is zero. */
        if (slen == 0) {
            SET_STRING_ELT(col, (R_xlen_t)ri, R_BlankString);
            continue;
        }

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
                    str_cache[si].sexp = cs;
                    break;
                }
                if (str_cache[si].hash == h && str_cache[si].len == slen &&
                    memcmp(CHAR(str_cache[si].sexp), sptr, (size_t)slen) == 0) {
                    cs = str_cache[si].sexp;
                    break;
                }
            }
            if (cs == R_NilValue)
                cs = Rf_mkCharLenCE(sptr, slen, CE_UTF8);
        } else {
            cs = Rf_mkCharLenCE(sptr, slen, CE_UTF8);
        }
        SET_STRING_ELT(col, (R_xlen_t)ri, cs);
    }
}

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
        SEXP cls = PROTECT(Rf_mkString("Date"));
        Rf_setAttrib(col, R_ClassSymbol, cls);
        UNPROTECT(1);
        return col;
    }
    if (strncmp(ann, "POSIXct|", 8) == 0) {
        const char *tz = ann + 8;
        SEXP cls = PROTECT(Rf_allocVector(STRSXP, 2));
        SET_STRING_ELT(cls, 0, Rf_mkChar("POSIXct"));
        SET_STRING_ELT(cls, 1, Rf_mkChar("POSIXt"));
        Rf_setAttrib(col, R_ClassSymbol, cls);
        if (tz[0] != '\0') {
            SEXP tzv = PROTECT(Rf_mkString(tz));
            Rf_setAttrib(col, Rf_install("tzone"), tzv);
            UNPROTECT(1);
        }
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
        SEXP fcls = PROTECT(Rf_mkString("factor"));
        Rf_setAttrib(icol, R_ClassSymbol, fcls);
        UNPROTECT(3);
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
    case VEC_INT32:
    case VEC_INT16:
    case VEC_INT8: {
        col = PROTECT(Rf_allocVector(INTSXP, (R_xlen_t)n));
        int *out = INTEGER(col);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i))
                out[i] = NA_INTEGER;
            else {
                switch (arr->type) {
                case VEC_INT32: out[i] = (int)arr->buf.i32[i]; break;
                case VEC_INT16: out[i] = (int)arr->buf.i16[i]; break;
                case VEC_INT8:  out[i] = (int)arr->buf.i8[i]; break;
                default: break;
                }
            }
        }
        UNPROTECT(1);
        return col;
    }
    case VEC_DOUBLE: {
        col = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t)n));
        double *out = REAL(col);
        if (vec_array_all_valid(arr)) {
            if (n > 0 && arr->buf.dbl != NULL)
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
        StrCacheSlot *cache = NULL;
        if (n > 256)
            cache = (StrCacheSlot *)calloc(STR_CACHE_SIZE, sizeof(StrCacheSlot));
        fill_string_col_from_batch(col, 0, arr, n, cache);
        free(cache);
        UNPROTECT(1);
        return col;
    }
    }
    return R_NilValue;
}

/* Patch NA sentinels into a slice of a REALSXP that the .vtr decoder filled
 * via the direct-write fast path. The decoder writes raw element bytes only;
 * positions whose validity bit is 0 still hold whatever was on disk and must
 * be overwritten with the type's R-side NA value.
 *
 *   t == VEC_DOUBLE              : NA = NA_REAL.
 *   t == VEC_INT64 (bit64 mode)  : NA = INT64_MIN, written via memcpy into
 *                                  the REALSXP storage to dodge type-pun UB.
 *
 * No-op when the array has no NAs. */
static void patch_na_into_direct_real(SEXP col, int64_t offset,
                                      const VecArray *arr, VecType t) {
    if (vec_array_all_valid(arr)) return;
    int64_t n = arr->length;
    double *out = REAL(col) + offset;
    if (t == VEC_DOUBLE) {
        for (int64_t j = 0; j < n; j++) {
            if (!vec_array_is_valid(arr, j))
                out[j] = NA_REAL;
        }
    } else {
        const int64_t na_val = INT64_MIN;
        for (int64_t j = 0; j < n; j++) {
            if (!vec_array_is_valid(arr, j))
                memcpy(&out[j], &na_val, sizeof(double));
        }
    }
}

/* One-shot debug warning when a column for which the caller supplied a direct
 * write buffer came back with data_borrowed == 0 — i.e. the decoder fell
 * through to its own allocation, forcing us to memcpy on the way out. This
 * is correct but slow, and is the canary that fires when a new encoding is
 * added without wiring up the direct-write path.
 *
 * Suppressed unless the VTR_DEBUG_DIRECT environment variable is set, so
 * normal users never see it. Keyed by VecType so each unsupported type warns
 * at most once per session. */
static void warn_direct_buf_fallback_once(VecType t) {
    static int seen[8] = {0};
    int idx = (int)t;
    if (idx < 0 || idx >= 8 || seen[idx]) return;
    const char *env = getenv("VTR_DEBUG_DIRECT");
    if (!env || env[0] == '\0') return;
    seen[idx] = 1;
    REprintf("[vectra] direct-write fallback for type %s: decoder allocated "
             "its own buffer, copy required. Add direct-write support in "
             "src/vtr1.c to recover the fast path.\n", vec_type_name(t));
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
                if (n > 0 && arr->buf.i64 != NULL)
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
    case VEC_INT32: {
        int *out = INTEGER(col) + (int)offset;
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i))
                out[i] = NA_INTEGER;
            else
                out[i] = (int)arr->buf.i32[i];
        }
        break;
    }
    case VEC_INT16: {
        int *out = INTEGER(col) + (int)offset;
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i))
                out[i] = NA_INTEGER;
            else
                out[i] = (int)arr->buf.i16[i];
        }
        break;
    }
    case VEC_INT8: {
        int *out = INTEGER(col) + (int)offset;
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i))
                out[i] = NA_INTEGER;
            else
                out[i] = (int)arr->buf.i8[i];
        }
        break;
    }
    case VEC_DOUBLE: {
        double *out = REAL(col) + offset;
        if (vec_array_all_valid(arr)) {
            if (n > 0 && arr->buf.dbl != NULL)
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

/* Convert one VecBatch into a standalone R data.frame, applying the schema's
   per-column annotations (Date/POSIXct/factor). A selection vector, if present,
   is compacted away first so the result is dense. Per-column conversion reuses
   array_to_sexp() — the same converter the builder path uses — so the row-group
   and chunk paths share one source of truth. The batch is consumed (freed)
   before returning; the data.frame holds independent copies of every column. */
static SEXP batch_to_dataframe(VecBatch *batch, const VecSchema *schema,
                               int want_bit64) {
    batch = vec_batch_compact(batch);   /* dense; frees+replaces if sel set */
    int n_cols = schema->n_cols;
    int64_t n_rows = batch->n_rows;

    SEXP df = PROTECT(Rf_allocVector(VECSXP, n_cols));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, n_cols));

    for (int i = 0; i < n_cols; i++) {
        /* Protect across apply_annotation: it allocates (and the factor path
           returns a fresh SEXP), which could otherwise collect this column. */
        SEXP col = PROTECT(array_to_sexp(&batch->columns[i], want_bit64));
        const char *ann = (schema->col_annotations)
                          ? schema->col_annotations[i] : NULL;
        col = apply_annotation(col, ann);
        SET_VECTOR_ELT(df, i, col);
        SET_STRING_ELT(names, i, Rf_mkCharCE(schema->col_names[i], CE_UTF8));
        UNPROTECT(1);   /* col — now reachable through df */
    }

    Rf_setAttrib(df, R_NamesSymbol, names);
    SEXP row_names = PROTECT(Rf_allocVector(INTSXP, 2));
    INTEGER(row_names)[0] = NA_INTEGER;
    INTEGER(row_names)[1] = -(int)n_rows;
    Rf_setAttrib(df, R_RowNamesSymbol, row_names);
    Rf_setAttrib(df, R_ClassSymbol, Rf_mkString("data.frame"));

    vec_batch_free(batch);
    UNPROTECT(3);   /* df, names, row_names */
    return df;
}

/* Pull the next non-empty batch from an already-optimized plan and return it
   as an R data.frame, or R_NilValue at end of stream. Empty batches (e.g. an
   input batch whose rows were all filtered out) are skipped rather than
   returned, so consumers that treat a zero-row frame as end-of-stream — such
   as biglm::bigglm's data() protocol — are never tripped mid-stream.

   The caller must run vec_optimize() once before the first call; thereafter
   each call advances the plan's pull cursor by one batch. */
SEXP vec_collect_next(VecNode *root) {
    int want_bit64 = use_bit64();
    VecBatch *batch;
    while ((batch = root->next_batch(root)) != NULL) {
        if (vec_batch_logical_rows(batch) > 0)
            return batch_to_dataframe(batch, &root->output_schema, want_bit64);
        vec_batch_free(batch);
    }
    return R_NilValue;
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
            else if (t == VEC_INT32 || t == VEC_INT16 || t == VEC_INT8)
                cols[i] = PROTECT(Rf_allocVector(INTSXP, (R_xlen_t)hint));
            else if (t == VEC_BOOL)
                cols[i] = PROTECT(Rf_allocVector(LGLSXP, (R_xlen_t)hint));
            else /* VEC_STRING */
                cols[i] = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t)hint));
        }

        /* CHARSXP cache shared across all string columns in this collect. */
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
           thread-local FILE handles. For columns whose R storage element
           size matches the on-disk decoded element size (DOUBLE, and INT64
           with bit64), each thread decodes straight into the pre-allocated
           R vector at its row offset — no intermediate malloc, no fill copy.
           Other columns (strings, narrow ints, plain int64) fall back to
           the per-batch sequential fill below. */
        int used_parallel = 0;
        if (scan_node_is_parallel_safe(root)) {
            const char *path = scan_node_get_path(root);
            Vtr1TdcFile *file = scan_node_get_file(root);
            const int *col_mask = scan_node_get_col_mask(root);
            uint32_t n_batches = 0;

            void **col_bases = (void **)calloc((size_t)n_cols, sizeof(void *));
            size_t *col_elem_sizes = (size_t *)calloc((size_t)n_cols,
                                                      sizeof(size_t));
            if (!col_bases || !col_elem_sizes)
                vectra_error("alloc failed for direct buffers");
            int *col_direct = (int *)calloc((size_t)n_cols, sizeof(int));
            if (!col_direct) vectra_error("alloc failed for col_direct");
            for (int i = 0; i < n_cols; i++) {
                VecType t = schema->col_types[i];
                if (t == VEC_DOUBLE || (t == VEC_INT64 && want_bit64)) {
                    col_bases[i] = REAL(cols[i]);
                    col_elem_sizes[i] = sizeof(double);
                    col_direct[i] = 1;
                }
                /* VEC_STRING: leave col_bases[i] NULL so the decoder
                 * allocates a heap buffer and the consumer loop walks it
                 * via fill_string_col_from_batch. */
            }

            VecBatch **batches = vtr1_read_parallel_tdc_into(file, col_mask, path,
                                                             col_bases,
                                                             col_elem_sizes,
                                                             n_cols, &n_batches);
            used_parallel = 1;

            for (uint32_t bi = 0; bi < n_batches; bi++) {
                batch = batches[bi];
                if (!batch) continue;
                int64_t n = batch->n_rows;

                for (int i = 0; i < n_cols; i++) {
                    VecType t = schema->col_types[i];
                    if (col_direct[i]) {
                        /* Caller asked for the direct-write fast path. The
                           decoder honors it only for the PLAIN+fixed paths;
                           on success the resulting VecArray has
                           data_borrowed == 1 and we just patch NAs.
                           Otherwise (DICT/DELTA/DIFF/QUANTIZE/SPATIAL/...)
                           the decoder allocated its own buffer — fall back
                           to the general copy path and warn in debug builds
                           so a regression here is hard to miss. */
                        const VecArray *arr = &batch->columns[i];
                        if (arr->data_borrowed) {
                            patch_na_into_direct_real(cols[i], offset, arr, t);
                        } else {
                            warn_direct_buf_fallback_once(t);
                            batch_to_sexp_direct(batch, i, cols[i], offset,
                                                 want_bit64, t);
                        }
                        continue;
                    }
                    if (t != VEC_STRING) {
                        batch_to_sexp_direct(batch, i, cols[i], offset,
                                             want_bit64, t);
                    } else {
                        fill_string_col_from_batch(cols[i], offset,
                                                   &batch->columns[i], n,
                                                   str_cache);
                    }
                }
                offset += n;
                vec_batch_free(batch);
            }
            free(batches);
            free(col_bases);
            free(col_elem_sizes);
            free(col_direct);
        }

        /* === DIRECT-READ PATH ===
           When root is a plain ScanNode (no predicates/tombstones), bypass
           the intermediate malloc+memcpy+free by reading directly into R
           vectors via vtr1_read_rowgroup_tdc_ex with pre-allocated target
           buffers. Numeric columns whose R element size matches the on-disk
           decoded element size (DOUBLE, INT64+bit64) get a real direct
           pointer; VEC_STRING columns pass NULL so the decoder allocates
           its own heap and the consumer loop walks it via
           fill_string_col_from_batch. Other column types disable the fast
           path entirely. */
        int used_direct = 0;
        if (!used_parallel && root->kind &&
            strcmp(root->kind, "ScanNode") == 0) {
            ScanNode *sn = (ScanNode *)root;
            if (!sn->predicate && !sn->tombstone && !sn->rg_bitmap) {
                int can_direct = 1;
                for (int i = 0; i < n_cols; i++) {
                    VecType t = schema->col_types[i];
                    if (t == VEC_DOUBLE || (t == VEC_INT64 && want_bit64) ||
                        t == VEC_STRING)
                        continue;
                    can_direct = 0;
                    break;
                }
                if (can_direct) {
                    Vtr1TdcFile *file = sn->file;
                    const int *col_mask = sn->col_mask;
                    uint32_t n_rgs = vtr1_tdc_n_rowgroups(file);
                    void **direct_bufs = (void **)malloc(
                        (size_t)n_cols * sizeof(void *));
                    if (direct_bufs) {
                        used_direct = 1;
                        for (uint32_t rg = 0; rg < n_rgs; rg++) {
                            int64_t rg_rows =
                                vtr1_tdc_rowgroup_n_rows(file, rg);
                            for (int i = 0; i < n_cols; i++) {
                                /* Strings: NULL -> decoder allocates heap
                                 * buffer consumed by fill_string_col_from_batch. */
                                if (schema->col_types[i] == VEC_STRING)
                                    direct_bufs[i] = NULL;
                                else
                                    direct_bufs[i] = REAL(cols[i]) + offset;
                            }
                            batch = vtr1_read_rowgroup_tdc_ex(
                                file, rg, col_mask, direct_bufs);
                            /* Same direct-write contract as the parallel
                               path: for numeric cols, data_borrowed == 1
                               means the decoder wrote into the R vector and
                               we only need to patch NAs; otherwise it
                               allocated its own buffer and we copy. String
                               cols are filled via fill_string_col_from_batch
                               from the decoder's flat heap buffer. */
                            for (int i = 0; i < n_cols; i++) {
                                const VecArray *arr = &batch->columns[i];
                                VecType t = schema->col_types[i];
                                if (t == VEC_STRING) {
                                    fill_string_col_from_batch(
                                        cols[i], offset, arr, rg_rows,
                                        str_cache);
                                } else if (arr->data_borrowed) {
                                    patch_na_into_direct_real(cols[i], offset,
                                                              arr, t);
                                } else {
                                    warn_direct_buf_fallback_once(t);
                                    batch_to_sexp_direct(batch, i, cols[i],
                                                         offset, want_bit64,
                                                         t);
                                }
                            }
                            offset += rg_rows;
                            vec_batch_free(batch);
                        }
                        free(direct_bufs);
                    }
                }
            }
        }

        /* === SEQUENTIAL PATH === */
        if (!used_parallel && !used_direct) {
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
                    fill_string_col_from_batch(cols[i], offset,
                                               &batch->columns[i], n,
                                               str_cache);
                }
            }
            offset += n;
            vec_batch_free(batch);
        }
        } /* end !used_parallel && !used_direct */

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
            int64_t tail_n = 0;
            for (int i = 0; i < n_cols; i++) {
                VecArray arr = vec_builder_finish(&builders[i]);
                tail_n = arr.length;
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
                } else if (t == VEC_INT32 || t == VEC_INT16 || t == VEC_INT8) {
                    int *out = INTEGER(cols[i]) + (int)offset;
                    for (int64_t j = 0; j < tail_n; j++) {
                        if (!vec_array_is_valid(&arr, j))
                            out[j] = NA_INTEGER;
                        else {
                            switch (t) {
                            case VEC_INT32: out[j] = (int)arr.buf.i32[j]; break;
                            case VEC_INT16: out[j] = (int)arr.buf.i16[j]; break;
                            case VEC_INT8:  out[j] = (int)arr.buf.i8[j]; break;
                            default: break;
                            }
                        }
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
                            int slen = (int)(end - start);
                            /* Skip Rf_mkCharLenCE on a NULL+0 pointer when
                             * the batch only holds empty/NA strings. */
                            if (slen == 0) {
                                SET_STRING_ELT(cols[i], (R_xlen_t)ri, R_BlankString);
                            } else {
                                SET_STRING_ELT(cols[i], (R_xlen_t)ri,
                                    Rf_mkCharLenCE(arr.buf.str.data + start,
                                                   slen, CE_UTF8));
                            }
                        }
                    }
                }
                vec_array_free(&arr);
            }
            offset += tail_n;
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
                } else if (t == VEC_INT32 || t == VEC_INT16 || t == VEC_INT8) {
                    new_col = PROTECT(Rf_allocVector(INTSXP, (R_xlen_t)total_rows));
                    memcpy(INTEGER(new_col), INTEGER(old_col), (size_t)total_rows * sizeof(int));
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
