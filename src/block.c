#include "block.h"
#include "builder.h"
#include "array.h"
#include "batch.h"
#include "optimize.h"
#include "collect.h"
#include "error.h"
#include "string_distance.h"
#include "fuzzy_join.h"
#include <R.h>
#include <Rinternals.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <math.h>

#ifdef _OPENMP
#include <omp.h>
#endif

/* ------------------------------------------------------------------ */
/* FNV-1a hashing (same constants as hash.c)                          */
/* ------------------------------------------------------------------ */

#define FNV_OFFSET 0xcbf29ce484222325ULL
#define FNV_PRIME  0x00000100000001B3ULL

static inline uint64_t fnv1a_bytes(const char *s, int64_t len) {
    uint64_t h = FNV_OFFSET;
    for (int64_t i = 0; i < len; i++) {
        h ^= (uint8_t)s[i];
        h *= FNV_PRIME;
    }
    return h;
}

static inline uint64_t fnv1a_bytes_ci(const char *s, int64_t len) {
    uint64_t h = FNV_OFFSET;
    for (int64_t i = 0; i < len; i++) {
        h ^= (uint8_t)tolower((unsigned char)s[i]);
        h *= FNV_PRIME;
    }
    return h;
}

/* Compare offset-based string with (ptr, len) pair */
static inline int str_eq(const VecArray *col, int64_t row,
                         const char *s, int64_t slen, int ci) {
    int64_t start = col->buf.str.offsets[row];
    int64_t end   = col->buf.str.offsets[row + 1];
    int64_t clen  = end - start;
    if (clen != slen) return 0;
    const char *d = col->buf.str.data + start;
    if (ci) {
        for (int64_t i = 0; i < slen; i++) {
            if (tolower((unsigned char)d[i]) !=
                tolower((unsigned char)s[i])) return 0;
        }
        return 1;
    }
    return memcmp(d, s, (size_t)slen) == 0;
}

/* Next power of 2 >= n, minimum 16 */
static int64_t next_pow2(int64_t n) {
    int64_t p = 16;
    while (p < n) p <<= 1;
    return p;
}


/* ------------------------------------------------------------------ */
/* block_materialize                                                   */
/* ------------------------------------------------------------------ */

ColumnBlock *block_materialize(VecNode *node) {
    vec_optimize(node);

    const VecSchema *schema = &node->output_schema;
    int n_cols = schema->n_cols;

    VecArrayBuilder *builders = (VecArrayBuilder *)calloc(
        (size_t)n_cols, sizeof(VecArrayBuilder));
    if (!builders) vectra_error("alloc failed in block_materialize");

    for (int i = 0; i < n_cols; i++)
        builders[i] = vec_builder_init(schema->col_types[i]);

    /* Pull all batches */
    VecBatch *batch;
    while ((batch = node->next_batch(node)) != NULL) {
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

    /* Build the block */
    ColumnBlock *blk = (ColumnBlock *)calloc(1, sizeof(ColumnBlock));
    if (!blk) vectra_error("alloc failed for ColumnBlock");

    blk->n_cols = n_cols;
    blk->columns = (VecArray *)calloc((size_t)n_cols, sizeof(VecArray));
    if (!blk->columns) vectra_error("alloc failed for block columns");

    for (int i = 0; i < n_cols; i++) {
        blk->columns[i] = vec_builder_finish(&builders[i]);
    }
    blk->n_rows = (n_cols > 0) ? blk->columns[0].length : 0;

    /* Copy schema */
    blk->schema.n_cols = n_cols;
    blk->schema.col_names = (char **)calloc((size_t)n_cols, sizeof(char *));
    blk->schema.col_types = (VecType *)calloc((size_t)n_cols, sizeof(VecType));
    blk->schema.col_annotations = (char **)calloc((size_t)n_cols, sizeof(char *));
    for (int i = 0; i < n_cols; i++) {
        blk->schema.col_names[i] = strdup(schema->col_names[i]);
        blk->schema.col_types[i] = schema->col_types[i];
        blk->schema.col_annotations[i] = schema->col_annotations[i]
            ? strdup(schema->col_annotations[i]) : NULL;
    }

    /* Init index caches */
    blk->indices    = (BlockIndex **)calloc((size_t)n_cols, sizeof(BlockIndex *));
    blk->ci_indices = (BlockIndex **)calloc((size_t)n_cols, sizeof(BlockIndex *));

    free(builders);
    return blk;
}


/* ------------------------------------------------------------------ */
/* block_free                                                          */
/* ------------------------------------------------------------------ */

static void block_index_free(BlockIndex *idx) {
    if (!idx) return;
    free(idx->heads);
    free(idx->entry_hash);
    free(idx->entry_row);
    free(idx->entry_next);
    free(idx);
}

void block_free(ColumnBlock *blk) {
    if (!blk) return;
    for (int i = 0; i < blk->n_cols; i++) {
        vec_array_free(&blk->columns[i]);
        if (blk->indices)    block_index_free(blk->indices[i]);
        if (blk->ci_indices) block_index_free(blk->ci_indices[i]);
        free(blk->schema.col_names[i]);
        free(blk->schema.col_annotations[i]);
    }
    free(blk->columns);
    free(blk->indices);
    free(blk->ci_indices);
    free(blk->schema.col_names);
    free(blk->schema.col_types);
    free(blk->schema.col_annotations);
    free(blk);
}


/* ------------------------------------------------------------------ */
/* block_get_index: build or retrieve cached hash index                */
/* ------------------------------------------------------------------ */

BlockIndex *block_get_index(ColumnBlock *blk, int col_idx, int ci) {
    BlockIndex **cache = ci ? blk->ci_indices : blk->indices;
    if (cache[col_idx]) return cache[col_idx];

    const VecArray *col = &blk->columns[col_idx];
    if (col->type != VEC_STRING)
        vectra_error("block_get_index: column %d is not a string column",
                     col_idx);

    int64_t n = blk->n_rows;
    int64_t n_slots = next_pow2(n * 2);  /* load factor ~0.5 */

    BlockIndex *idx = (BlockIndex *)calloc(1, sizeof(BlockIndex));
    if (!idx) vectra_error("alloc failed for BlockIndex");

    idx->n_slots   = n_slots;
    idx->n_entries = n;
    idx->col       = col;
    idx->ci        = ci;

    idx->heads      = (int64_t *)malloc((size_t)n_slots * sizeof(int64_t));
    idx->entry_hash = (uint64_t *)malloc((size_t)n * sizeof(uint64_t));
    idx->entry_row  = (int64_t *)malloc((size_t)n * sizeof(int64_t));
    idx->entry_next = (int64_t *)malloc((size_t)n * sizeof(int64_t));
    if (!idx->heads || !idx->entry_hash || !idx->entry_row || !idx->entry_next)
        vectra_error("alloc failed for BlockIndex arrays");

    /* Initialize heads to -1 */
    for (int64_t s = 0; s < n_slots; s++)
        idx->heads[s] = -1;

    /* Insert all rows */
    const int64_t *offsets = col->buf.str.offsets;
    const char    *data    = col->buf.str.data;
    int64_t mask = n_slots - 1;

    for (int64_t r = 0; r < n; r++) {
        /* Skip NULL strings */
        if (col->validity && !vec_array_is_valid(col, r)) {
            idx->entry_hash[r] = 0;
            idx->entry_row[r]  = r;
            idx->entry_next[r] = -2;  /* sentinel: not in any chain */
            continue;
        }

        int64_t start = offsets[r];
        int64_t slen  = offsets[r + 1] - start;

        uint64_t h = ci ? fnv1a_bytes_ci(data + start, slen)
                        : fnv1a_bytes(data + start, slen);

        int64_t slot = (int64_t)(h & (uint64_t)mask);

        idx->entry_hash[r] = h;
        idx->entry_row[r]  = r;
        idx->entry_next[r] = idx->heads[slot];
        idx->heads[slot]   = r;
    }

    cache[col_idx] = idx;
    return idx;
}


/* ------------------------------------------------------------------ */
/* block_probe                                                         */
/* ------------------------------------------------------------------ */

int64_t block_probe(const BlockIndex *idx,
                    const char **keys, const int *key_lens, int64_t n_keys,
                    int64_t **out_query_idx, int64_t **out_block_row) {

    int64_t mask = idx->n_slots - 1;
    int ci = idx->ci;

    /* First pass: count total matches for allocation */
    int64_t total = 0;
    for (int64_t q = 0; q < n_keys; q++) {
        if (!keys[q]) continue;
        int64_t slen = key_lens[q];
        uint64_t h = ci ? fnv1a_bytes_ci(keys[q], slen)
                        : fnv1a_bytes(keys[q], slen);
        int64_t slot = (int64_t)(h & (uint64_t)mask);
        int64_t e = idx->heads[slot];
        while (e >= 0) {
            if (idx->entry_hash[e] == h &&
                str_eq(idx->col, idx->entry_row[e], keys[q], slen, ci)) {
                total++;
            }
            e = idx->entry_next[e];
        }
    }

    if (total == 0) {
        *out_query_idx = NULL;
        *out_block_row = NULL;
        return 0;
    }

    /* Second pass: collect matches */
    *out_query_idx = (int64_t *)malloc((size_t)total * sizeof(int64_t));
    *out_block_row = (int64_t *)malloc((size_t)total * sizeof(int64_t));
    if (!*out_query_idx || !*out_block_row)
        vectra_error("alloc failed in block_probe");

    int64_t pos = 0;
    for (int64_t q = 0; q < n_keys; q++) {
        if (!keys[q]) continue;
        int64_t slen = key_lens[q];
        uint64_t h = ci ? fnv1a_bytes_ci(keys[q], slen)
                        : fnv1a_bytes(keys[q], slen);
        int64_t slot = (int64_t)(h & (uint64_t)mask);
        int64_t e = idx->heads[slot];
        while (e >= 0) {
            if (idx->entry_hash[e] == h &&
                str_eq(idx->col, idx->entry_row[e], keys[q], slen, ci)) {
                (*out_query_idx)[pos] = q;
                (*out_block_row)[pos] = idx->entry_row[e];
                pos++;
            }
            e = idx->entry_next[e];
        }
    }

    return total;
}


/* ------------------------------------------------------------------ */
/* R bridge: C_block_materialize                                       */
/* ------------------------------------------------------------------ */

static void block_finalizer(SEXP xptr) {
    ColumnBlock *blk = (ColumnBlock *)R_ExternalPtrAddr(xptr);
    if (blk) {
        block_free(blk);
        R_ClearExternalPtr(xptr);
    }
}

SEXP C_block_materialize(SEXP node_xptr) {
    VecNode *node = (VecNode *)R_ExternalPtrAddr(node_xptr);
    if (!node) Rf_error("vectra node has been freed or collected");

    ColumnBlock *blk = block_materialize(node);

    /* Consume the node: clear the pointer so R's finalizer won't double-free */
    node->free_node(node);
    R_ClearExternalPtr(node_xptr);

    SEXP xptr = PROTECT(R_MakeExternalPtr(blk, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(xptr, block_finalizer, TRUE);
    UNPROTECT(1);
    return xptr;
}


/* ------------------------------------------------------------------ */
/* R bridge: C_block_lookup                                            */
/*                                                                     */
/* Given a block, column name, and R character vector of keys,         */
/* returns a data.frame with query_idx (1-based) + all block columns   */
/* for matching rows.                                                  */
/* ------------------------------------------------------------------ */

/* Forward decl from collect.c — we reuse array_to_sexp */
/* Since it's static in collect.c, we duplicate the minimal logic here */
static SEXP block_array_gather_sexp(const VecArray *arr,
                                    const int64_t *rows, int64_t n) {
    SEXP col;
    switch (arr->type) {
    case VEC_INT64: {
        col = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t)n));
        double *out = REAL(col);
        for (int64_t i = 0; i < n; i++) {
            int64_t r = rows[i];
            if (!vec_array_is_valid(arr, r))
                out[i] = NA_REAL;
            else
                out[i] = (double)arr->buf.i64[r];
        }
        UNPROTECT(1);
        return col;
    }
    case VEC_INT8: {
        col = PROTECT(Rf_allocVector(INTSXP, (R_xlen_t)n));
        int *out = INTEGER(col);
        for (int64_t i = 0; i < n; i++) {
            int64_t r = rows[i];
            if (!vec_array_is_valid(arr, r))
                out[i] = NA_INTEGER;
            else
                out[i] = (int)arr->buf.i8[r];
        }
        UNPROTECT(1);
        return col;
    }
    case VEC_INT16: {
        col = PROTECT(Rf_allocVector(INTSXP, (R_xlen_t)n));
        int *out = INTEGER(col);
        for (int64_t i = 0; i < n; i++) {
            int64_t r = rows[i];
            if (!vec_array_is_valid(arr, r))
                out[i] = NA_INTEGER;
            else
                out[i] = (int)arr->buf.i16[r];
        }
        UNPROTECT(1);
        return col;
    }
    case VEC_INT32: {
        col = PROTECT(Rf_allocVector(INTSXP, (R_xlen_t)n));
        int *out = INTEGER(col);
        for (int64_t i = 0; i < n; i++) {
            int64_t r = rows[i];
            if (!vec_array_is_valid(arr, r))
                out[i] = NA_INTEGER;
            else
                out[i] = arr->buf.i32[r];
        }
        UNPROTECT(1);
        return col;
    }
    case VEC_DOUBLE: {
        col = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t)n));
        double *out = REAL(col);
        for (int64_t i = 0; i < n; i++) {
            int64_t r = rows[i];
            if (!vec_array_is_valid(arr, r))
                out[i] = NA_REAL;
            else
                out[i] = arr->buf.dbl[r];
        }
        UNPROTECT(1);
        return col;
    }
    case VEC_BOOL: {
        col = PROTECT(Rf_allocVector(LGLSXP, (R_xlen_t)n));
        int *out = LOGICAL(col);
        for (int64_t i = 0; i < n; i++) {
            int64_t r = rows[i];
            if (!vec_array_is_valid(arr, r))
                out[i] = NA_LOGICAL;
            else
                out[i] = arr->buf.bln[r] ? 1 : 0;
        }
        UNPROTECT(1);
        return col;
    }
    case VEC_STRING: {
        col = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t)n));
        for (int64_t i = 0; i < n; i++) {
            int64_t r = rows[i];
            if (!vec_array_is_valid(arr, r)) {
                SET_STRING_ELT(col, (R_xlen_t)i, NA_STRING);
            } else {
                int64_t start = arr->buf.str.offsets[r];
                int64_t end   = arr->buf.str.offsets[r + 1];
                SET_STRING_ELT(col, (R_xlen_t)i,
                    Rf_mkCharLenCE(arr->buf.str.data + start,
                                   (int)(end - start), CE_UTF8));
            }
        }
        UNPROTECT(1);
        return col;
    }
    }
    return R_NilValue;
}


SEXP C_block_lookup(SEXP block_xptr, SEXP col_name, SEXP keys, SEXP ci_sexp) {
    ColumnBlock *blk = (ColumnBlock *)R_ExternalPtrAddr(block_xptr);
    if (!blk) Rf_error("block has been freed");

    const char *cname = CHAR(STRING_ELT(col_name, 0));
    int ci = Rf_asLogical(ci_sexp);

    /* Find column index */
    int col_idx = -1;
    for (int i = 0; i < blk->n_cols; i++) {
        if (strcmp(blk->schema.col_names[i], cname) == 0) {
            col_idx = i;
            break;
        }
    }
    if (col_idx < 0)
        Rf_error("column '%s' not found in block", cname);
    if (blk->schema.col_types[col_idx] != VEC_STRING)
        Rf_error("column '%s' is not a string column", cname);

    /* Build or retrieve cached index */
    BlockIndex *idx = block_get_index(blk, col_idx, ci);

    /* Extract keys from R character vector */
    int64_t n_keys = XLENGTH(keys);
    const char **key_ptrs = (const char **)malloc(
        (size_t)n_keys * sizeof(const char *));
    int *key_lens = (int *)malloc((size_t)n_keys * sizeof(int));
    if (!key_ptrs || !key_lens) Rf_error("alloc failed");

    for (int64_t i = 0; i < n_keys; i++) {
        SEXP s = STRING_ELT(keys, (R_xlen_t)i);
        if (s == NA_STRING) {
            key_ptrs[i] = NULL;
            key_lens[i] = 0;
        } else {
            key_ptrs[i] = CHAR(s);
            key_lens[i] = (int)LENGTH(s);
        }
    }

    /* Probe */
    int64_t *query_idx = NULL, *block_row = NULL;
    int64_t n_matches = block_probe(idx, key_ptrs, key_lens, n_keys,
                                    &query_idx, &block_row);

    free(key_ptrs);
    free(key_lens);

    /* Build output data.frame: query_idx + all block columns */
    int out_ncols = 1 + blk->n_cols;
    SEXP df    = PROTECT(Rf_allocVector(VECSXP, out_ncols));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, out_ncols));

    /* Column 0: query_idx (1-based for R) */
    {
        SEXP qcol = PROTECT(Rf_allocVector(INTSXP, (R_xlen_t)n_matches));
        int *qp = INTEGER(qcol);
        for (int64_t i = 0; i < n_matches; i++)
            qp[i] = (int)(query_idx[i] + 1);
        SET_VECTOR_ELT(df, 0, qcol);
        SET_STRING_ELT(names, 0, Rf_mkChar("query_idx"));
        UNPROTECT(1);
    }

    /* Columns 1..n_cols: gathered block columns */
    for (int c = 0; c < blk->n_cols; c++) {
        SEXP col = block_array_gather_sexp(&blk->columns[c],
                                           block_row, n_matches);
        SET_VECTOR_ELT(df, c + 1, col);
        SET_STRING_ELT(names, c + 1,
            Rf_mkCharCE(blk->schema.col_names[c], CE_UTF8));
    }

    /* data.frame attributes */
    Rf_setAttrib(df, R_NamesSymbol, names);
    SEXP row_names = PROTECT(Rf_allocVector(INTSXP, 2));
    INTEGER(row_names)[0] = NA_INTEGER;
    INTEGER(row_names)[1] = -(int)n_matches;
    Rf_setAttrib(df, R_RowNamesSymbol, row_names);
    Rf_setAttrib(df, R_ClassSymbol, Rf_mkString("data.frame"));

    free(query_idx);
    free(block_row);

    UNPROTECT(3);
    return df;
}


/* ------------------------------------------------------------------ */
/* String access helpers (local copies — static in fuzzy_join.c)      */
/* ------------------------------------------------------------------ */

static inline const char *blk_str_ptr(const VecArray *arr, int64_t row) {
    return arr->buf.str.data + arr->buf.str.offsets[row];
}

static inline int64_t blk_str_len(const VecArray *arr, int64_t row) {
    return arr->buf.str.offsets[row + 1] - arr->buf.str.offsets[row];
}

/* ------------------------------------------------------------------ */
/* Compute normalized distance (local copy — static in fuzzy_join.c)  */
/* ------------------------------------------------------------------ */

static inline double blk_compute_dist(FuzzyMethod method,
                                      const char *a, int64_t la,
                                      const char *b, int64_t lb,
                                      double max_dist) {
    if (method == FUZZY_JW) {
        double sim = strdist_jaro_winkler(a, la, b, lb);
        return 1.0 - sim;
    }
    int64_t max_len = la > lb ? la : lb;
    if (max_len == 0) return 0.0;
    int64_t max_raw = (int64_t)ceil(max_dist * (double)max_len);
    int64_t raw;
    if (method == FUZZY_DL) {
        raw = strdist_dl(a, la, b, lb, max_raw);
    } else {
        raw = strdist_levenshtein(a, la, b, lb, max_raw);
    }
    if (raw > max_raw) return max_dist + 1.0;
    return (double)raw / (double)max_len;
}

/* ------------------------------------------------------------------ */
/* Thread-local growable buffer for fuzzy lookup matches               */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t *query_idx;
    int64_t *block_row;
    double  *dist;
    int64_t  count;
    int64_t  capacity;
} FuzzyLookupBuf;

static void flbuf_init(FuzzyLookupBuf *buf, int64_t cap) {
    buf->query_idx = (int64_t *)malloc((size_t)cap * sizeof(int64_t));
    buf->block_row = (int64_t *)malloc((size_t)cap * sizeof(int64_t));
    buf->dist      = (double *)malloc((size_t)cap * sizeof(double));
    buf->count = 0;
    buf->capacity = cap;
    if (!buf->query_idx || !buf->block_row || !buf->dist)
        vectra_error("alloc failed for FuzzyLookupBuf");
}

static void flbuf_push(FuzzyLookupBuf *buf, int64_t qi, int64_t br, double d) {
    if (buf->count >= buf->capacity) {
        buf->capacity *= 2;
        buf->query_idx = (int64_t *)realloc(buf->query_idx,
            (size_t)buf->capacity * sizeof(int64_t));
        buf->block_row = (int64_t *)realloc(buf->block_row,
            (size_t)buf->capacity * sizeof(int64_t));
        buf->dist = (double *)realloc(buf->dist,
            (size_t)buf->capacity * sizeof(double));
        if (!buf->query_idx || !buf->block_row || !buf->dist)
            vectra_error("realloc failed for FuzzyLookupBuf");
    }
    buf->query_idx[buf->count] = qi;
    buf->block_row[buf->count] = br;
    buf->dist[buf->count]      = d;
    buf->count++;
}

static void flbuf_free(FuzzyLookupBuf *buf) {
    free(buf->query_idx);
    free(buf->block_row);
    free(buf->dist);
}


/* ------------------------------------------------------------------ */
/* R bridge: C_block_fuzzy_lookup                                      */
/* ------------------------------------------------------------------ */

SEXP C_block_fuzzy_lookup(SEXP block_xptr, SEXP match_col_name, SEXP keys,
                          SEXP method_sexp, SEXP max_dist_sexp,
                          SEXP block_col_name, SEXP block_keys,
                          SEXP n_threads_sexp) {

    ColumnBlock *blk = (ColumnBlock *)R_ExternalPtrAddr(block_xptr);
    if (!blk) Rf_error("block has been freed");

    const char *mcname = CHAR(STRING_ELT(match_col_name, 0));
    FuzzyMethod method = (FuzzyMethod)Rf_asInteger(method_sexp);
    double max_dist = Rf_asReal(max_dist_sexp);
    int n_threads = Rf_asInteger(n_threads_sexp);
    if (n_threads < 1) n_threads = 1;

    /* Find match column index */
    int match_col_idx = -1;
    for (int i = 0; i < blk->n_cols; i++) {
        if (strcmp(blk->schema.col_names[i], mcname) == 0) {
            match_col_idx = i;
            break;
        }
    }
    if (match_col_idx < 0)
        Rf_error("column '%s' not found in block", mcname);
    if (blk->schema.col_types[match_col_idx] != VEC_STRING)
        Rf_error("column '%s' is not a string column", mcname);

    const VecArray *match_arr = &blk->columns[match_col_idx];

    /* Blocking column (optional) */
    int use_blocking = (block_col_name != R_NilValue);
    int block_col_idx = -1;
    BlockIndex *blk_idx = NULL;

    if (use_blocking) {
        const char *bcname = CHAR(STRING_ELT(block_col_name, 0));
        for (int i = 0; i < blk->n_cols; i++) {
            if (strcmp(blk->schema.col_names[i], bcname) == 0) {
                block_col_idx = i;
                break;
            }
        }
        if (block_col_idx < 0)
            Rf_error("blocking column '%s' not found in block", bcname);
        if (blk->schema.col_types[block_col_idx] != VEC_STRING)
            Rf_error("blocking column '%s' is not a string column",
                     CHAR(STRING_ELT(block_col_name, 0)));
        if (block_keys == R_NilValue)
            Rf_error("block_keys required when block_col is provided");
        if (XLENGTH(block_keys) != XLENGTH(keys))
            Rf_error("block_keys must have same length as keys");

        blk_idx = block_get_index(blk, block_col_idx, 0);
    }

    /* Extract query keys */
    int64_t n_keys = XLENGTH(keys);

    /* Allocate thread-local buffers */
#ifdef _OPENMP
    if (n_threads > omp_get_max_threads())
        n_threads = omp_get_max_threads();
#else
    n_threads = 1;
#endif

    FuzzyLookupBuf *tbufs = (FuzzyLookupBuf *)malloc(
        (size_t)n_threads * sizeof(FuzzyLookupBuf));
    if (!tbufs) Rf_error("alloc failed for thread buffers");
    for (int t = 0; t < n_threads; t++)
        flbuf_init(&tbufs[t], 256);

    int64_t blk_nrows = blk->n_rows;

    if (use_blocking) {
        /* ---- Blocked fuzzy lookup ---- */
        int64_t idx_mask = blk_idx->n_slots - 1;

        #ifdef _OPENMP
        #pragma omp parallel for num_threads(n_threads) schedule(dynamic, 64)
        #endif
        for (int64_t q = 0; q < n_keys; q++) {
            #ifdef _OPENMP
            int tid = omp_get_thread_num();
            #else
            int tid = 0;
            #endif

            SEXP ks = STRING_ELT(keys, (R_xlen_t)q);
            if (ks == NA_STRING) continue;
            const char *key = CHAR(ks);
            int64_t key_len = (int64_t)strlen(key);

            /* Get blocking key for this query */
            SEXP bks = STRING_ELT(block_keys, (R_xlen_t)q);
            if (bks == NA_STRING) continue;
            const char *bkey = CHAR(bks);
            int64_t bkey_len = (int64_t)strlen(bkey);

            /* Probe the block index for candidate rows */
            uint64_t h = fnv1a_bytes(bkey, bkey_len);
            int64_t slot = (int64_t)(h & (uint64_t)idx_mask);
            int64_t e = blk_idx->heads[slot];

            while (e >= 0) {
                if (blk_idx->entry_hash[e] == h &&
                    str_eq(blk_idx->col, blk_idx->entry_row[e],
                           bkey, bkey_len, 0)) {
                    int64_t r = blk_idx->entry_row[e];
                    /* Skip NULL in match column */
                    if (match_arr->validity &&
                        !vec_array_is_valid(match_arr, r)) {
                        e = blk_idx->entry_next[e];
                        continue;
                    }
                    const char *bstr = blk_str_ptr(match_arr, r);
                    int64_t blen = blk_str_len(match_arr, r);
                    double d = blk_compute_dist(method, key, key_len,
                                                bstr, blen, max_dist);
                    if (d <= max_dist)
                        flbuf_push(&tbufs[tid], q, r, d);
                }
                e = blk_idx->entry_next[e];
            }
        }
    } else {
        /* ---- Unblocked fuzzy lookup: compare against all rows ---- */
        #ifdef _OPENMP
        #pragma omp parallel for num_threads(n_threads) schedule(dynamic, 16)
        #endif
        for (int64_t q = 0; q < n_keys; q++) {
            #ifdef _OPENMP
            int tid = omp_get_thread_num();
            #else
            int tid = 0;
            #endif

            SEXP ks = STRING_ELT(keys, (R_xlen_t)q);
            if (ks == NA_STRING) continue;
            const char *key = CHAR(ks);
            int64_t key_len = (int64_t)strlen(key);

            for (int64_t r = 0; r < blk_nrows; r++) {
                if (match_arr->validity &&
                    !vec_array_is_valid(match_arr, r))
                    continue;
                const char *bstr = blk_str_ptr(match_arr, r);
                int64_t blen = blk_str_len(match_arr, r);
                double d = blk_compute_dist(method, key, key_len,
                                            bstr, blen, max_dist);
                if (d <= max_dist)
                    flbuf_push(&tbufs[tid], q, r, d);
            }
        }
    }

    /* Merge thread-local buffers */
    int64_t total = 0;
    for (int t = 0; t < n_threads; t++)
        total += tbufs[t].count;

    int64_t *merged_qi = (int64_t *)malloc((size_t)(total + 1) * sizeof(int64_t));
    int64_t *merged_br = (int64_t *)malloc((size_t)(total + 1) * sizeof(int64_t));
    double  *merged_d  = (double *)malloc((size_t)(total + 1) * sizeof(double));
    if (!merged_qi || !merged_br || !merged_d)
        Rf_error("alloc failed for merged fuzzy results");

    int64_t pos = 0;
    for (int t = 0; t < n_threads; t++) {
        if (tbufs[t].count > 0) {
            memcpy(merged_qi + pos, tbufs[t].query_idx,
                   (size_t)tbufs[t].count * sizeof(int64_t));
            memcpy(merged_br + pos, tbufs[t].block_row,
                   (size_t)tbufs[t].count * sizeof(int64_t));
            memcpy(merged_d + pos, tbufs[t].dist,
                   (size_t)tbufs[t].count * sizeof(double));
            pos += tbufs[t].count;
        }
        flbuf_free(&tbufs[t]);
    }
    free(tbufs);

    /* Build output data.frame: query_idx + fuzzy_dist + all block columns */
    int out_ncols = 2 + blk->n_cols;
    SEXP df    = PROTECT(Rf_allocVector(VECSXP, out_ncols));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, out_ncols));

    /* Column 0: query_idx (1-based) */
    {
        SEXP qcol = PROTECT(Rf_allocVector(INTSXP, (R_xlen_t)total));
        int *qp = INTEGER(qcol);
        for (int64_t i = 0; i < total; i++)
            qp[i] = (int)(merged_qi[i] + 1);
        SET_VECTOR_ELT(df, 0, qcol);
        SET_STRING_ELT(names, 0, Rf_mkChar("query_idx"));
        UNPROTECT(1);
    }

    /* Column 1: fuzzy_dist */
    {
        SEXP dcol = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t)total));
        double *dp = REAL(dcol);
        memcpy(dp, merged_d, (size_t)total * sizeof(double));
        SET_VECTOR_ELT(df, 1, dcol);
        SET_STRING_ELT(names, 1, Rf_mkChar("fuzzy_dist"));
        UNPROTECT(1);
    }

    /* Columns 2..n_cols+1: gathered block columns */
    for (int c = 0; c < blk->n_cols; c++) {
        SEXP col = block_array_gather_sexp(&blk->columns[c],
                                           merged_br, total);
        SET_VECTOR_ELT(df, c + 2, col);
        SET_STRING_ELT(names, c + 2,
            Rf_mkCharCE(blk->schema.col_names[c], CE_UTF8));
    }

    /* data.frame attributes */
    Rf_setAttrib(df, R_NamesSymbol, names);
    SEXP row_names = PROTECT(Rf_allocVector(INTSXP, 2));
    INTEGER(row_names)[0] = NA_INTEGER;
    INTEGER(row_names)[1] = -(int)total;
    Rf_setAttrib(df, R_RowNamesSymbol, row_names);
    Rf_setAttrib(df, R_ClassSymbol, Rf_mkString("data.frame"));

    free(merged_qi);
    free(merged_br);
    free(merged_d);

    UNPROTECT(3);
    return df;
}
