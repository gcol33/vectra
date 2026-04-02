#include "vtr_diff.h"
#include "vtr1.h"
#include "scan.h"
#include "schema.h"
#include "hash.h"
#include "array.h"
#include "batch.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* ------------------------------------------------------------------ */
/*  Key arena: stores one copy of each unique key (single-key diff)   */
/* ------------------------------------------------------------------ */

typedef struct {
    VecType  key_type;
    int64_t  capacity;
    int64_t  length;
    VecArray arena;      /* one VecArray for the single key column */
    /* For VEC_STRING: separate owned string buffer (arena.buf.str.data
       is a borrowed pointer into str_data). */
    char    *str_data;
    int64_t  str_data_len;
    int64_t  str_data_cap;
} DiffKeyArena;

static void dka_init(DiffKeyArena *ka, VecType key_type) {
    ka->key_type     = key_type;
    ka->capacity     = 64;
    ka->length       = 0;
    ka->str_data     = NULL;
    ka->str_data_len = 0;
    ka->str_data_cap = 0;
    ka->arena = vec_array_alloc(key_type, ka->capacity);
    if (key_type == VEC_STRING)
        ka->arena.owns_data = 0;  /* we own str_data separately */
}

static void dka_ensure(DiffKeyArena *ka, int64_t n) {
    if (n <= ka->capacity) return;
    int64_t new_cap = ka->capacity;
    while (new_cap < n) new_cap *= 2;

    VecArray old = ka->arena;
    VecArray new_arr = vec_array_alloc(ka->key_type, new_cap);
    /* Copy validity bits */
    memcpy(new_arr.validity, old.validity,
           (size_t)((old.length + 7) / 8));
    switch (ka->key_type) {
    case VEC_INT64:
        memcpy(new_arr.buf.i64, old.buf.i64,
               (size_t)old.length * sizeof(int64_t));
        break;
    case VEC_DOUBLE:
        memcpy(new_arr.buf.dbl, old.buf.dbl,
               (size_t)old.length * sizeof(double));
        break;
    case VEC_BOOL:
        memcpy(new_arr.buf.bln, old.buf.bln, (size_t)old.length);
        break;
    case VEC_STRING:
        memcpy(new_arr.buf.str.offsets, old.buf.str.offsets,
               (size_t)(old.length + 1) * sizeof(int64_t));
        free(new_arr.buf.str.data);   /* free the 1-byte alloc */
        new_arr.buf.str.data     = ka->str_data;
        new_arr.buf.str.data_len = ka->str_data_len;
        new_arr.owns_data        = 0;
        break;
    }
    new_arr.length = old.length;
    vec_array_free(&old);
    ka->arena    = new_arr;
    ka->capacity = new_cap;
}

static void dka_append(DiffKeyArena *ka, const VecArray *col, int64_t row) {
    int64_t pos = ka->length;
    dka_ensure(ka, pos + 1);

    VecArray *a = &ka->arena;
    a->length = pos + 1;

    if (!vec_array_is_valid(col, row)) {
        vec_array_set_null(a, pos);
        if (ka->key_type == VEC_STRING) {
            int64_t cur = ka->str_data_len;
            a->buf.str.offsets[pos]     = cur;
            a->buf.str.offsets[pos + 1] = cur;
            a->buf.str.data             = ka->str_data;
            a->buf.str.data_len         = cur;
        }
    } else {
        vec_array_set_valid(a, pos);
        switch (ka->key_type) {
        case VEC_INT64:
            a->buf.i64[pos] = col->buf.i64[row];
            break;
        case VEC_DOUBLE:
            a->buf.dbl[pos] = col->buf.dbl[row];
            break;
        case VEC_BOOL:
            a->buf.bln[pos] = col->buf.bln[row];
            break;
        case VEC_STRING: {
            int64_t s    = col->buf.str.offsets[row];
            int64_t e    = col->buf.str.offsets[row + 1];
            int64_t slen = e - s;
            int64_t needed = ka->str_data_len + slen;
            if (needed > ka->str_data_cap) {
                int64_t nc = (ka->str_data_cap == 0) ? 256 : ka->str_data_cap;
                while (nc < needed) nc *= 2;
                ka->str_data     = (char *)realloc(ka->str_data, (size_t)nc);
                if (!ka->str_data) vectra_error("dka_append: alloc failed");
                ka->str_data_cap = nc;
            }
            a->buf.str.offsets[pos] = ka->str_data_len;
            if (slen > 0)
                memcpy(ka->str_data + ka->str_data_len,
                       col->buf.str.data + s, (size_t)slen);
            ka->str_data_len += slen;
            a->buf.str.offsets[pos + 1] = ka->str_data_len;
            a->buf.str.data             = ka->str_data;
            a->buf.str.data_len         = ka->str_data_len;
            break;
        }
        }
    }
    ka->length = pos + 1;
}

static void dka_free(DiffKeyArena *ka) {
    if (ka->key_type == VEC_STRING) {
        /* arena.buf.str.data is borrowed; free str_data directly */
        ka->arena.owns_data = 0;
        free(ka->str_data);
        ka->str_data = NULL;
    }
    vec_array_free(&ka->arena);
}

/* ------------------------------------------------------------------ */
/*  Convert a single VecArray column (may have sel) to an R vector    */
/* ------------------------------------------------------------------ */

/* Convert dense VecArray (length n) to an R vector.
   For int64: returns REALSXP (double) matching collect.c behaviour. */
static SEXP array_col_to_sexp(const VecArray *arr) {
    int64_t n = arr->length;
    SEXP out;
    switch (arr->type) {
    case VEC_INT64: {
        out = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t)n));
        double *p = REAL(out);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i))
                p[i] = NA_REAL;
            else
                p[i] = (double)arr->buf.i64[i];
        }
        UNPROTECT(1);
        return out;
    }
    case VEC_DOUBLE: {
        out = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t)n));
        double *p = REAL(out);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i))
                p[i] = NA_REAL;
            else
                p[i] = arr->buf.dbl[i];
        }
        UNPROTECT(1);
        return out;
    }
    case VEC_BOOL: {
        out = PROTECT(Rf_allocVector(LGLSXP, (R_xlen_t)n));
        int *p = LOGICAL(out);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i))
                p[i] = NA_LOGICAL;
            else
                p[i] = arr->buf.bln[i] ? 1 : 0;
        }
        UNPROTECT(1);
        return out;
    }
    case VEC_STRING: {
        out = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t)n));
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i)) {
                SET_STRING_ELT(out, (R_xlen_t)i, NA_STRING);
            } else {
                int64_t s    = arr->buf.str.offsets[i];
                int64_t e    = arr->buf.str.offsets[i + 1];
                int64_t slen = e - s;
                SET_STRING_ELT(out, (R_xlen_t)i,
                    Rf_mkCharLenCE(arr->buf.str.data + s,
                                   (int)slen, CE_UTF8));
            }
        }
        UNPROTECT(1);
        return out;
    }
    }
    return R_NilValue;
}

/* ------------------------------------------------------------------ */
/*  Growing int64 result buffer for added-key indices or values       */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t *data;
    int64_t  len;
    int64_t  cap;
} I64Vec;

static void i64vec_init(I64Vec *v) {
    v->data = NULL;
    v->len  = 0;
    v->cap  = 0;
}

static void i64vec_push(I64Vec *v, int64_t val) {
    if (v->len == v->cap) {
        int64_t nc = (v->cap == 0) ? 64 : v->cap * 2;
        v->data = (int64_t *)realloc(v->data, (size_t)nc * sizeof(int64_t));
        if (!v->data) vectra_error("i64vec_push: alloc failed");
        v->cap = nc;
    }
    v->data[v->len++] = val;
}

static void i64vec_free(I64Vec *v) {
    free(v->data);
    v->data = NULL;
    v->len = v->cap = 0;
}

/* Same but for string keys accumulated during B scan */
typedef struct {
    char    *data;
    int64_t  data_len;
    int64_t  data_cap;
    int64_t *offsets;     /* len+1 entries */
    int64_t  len;
    int64_t  cap;         /* capacity for offsets/lengths arrays */
} StrVec;

static void strvec_init(StrVec *v) {
    memset(v, 0, sizeof(*v));
}

static void strvec_push(StrVec *v, const char *s, int64_t slen) {
    /* grow offsets/len array */
    if (v->len >= v->cap) {
        int64_t nc = (v->cap == 0) ? 64 : v->cap * 2;
        v->offsets = (int64_t *)realloc(v->offsets,
                                        (size_t)(nc + 1) * sizeof(int64_t));
        if (!v->offsets) vectra_error("strvec_push: alloc failed (offsets)");
        if (v->len == 0) v->offsets[0] = 0;
        v->cap = nc;
    }
    /* grow data buffer */
    int64_t needed = v->data_len + slen;
    if (needed > v->data_cap) {
        int64_t nc = (v->data_cap == 0) ? 256 : v->data_cap;
        while (nc < needed) nc *= 2;
        v->data = (char *)realloc(v->data, (size_t)nc);
        if (!v->data) vectra_error("strvec_push: alloc failed (data)");
        v->data_cap = nc;
    }
    if (slen > 0)
        memcpy(v->data + v->data_len, s, (size_t)slen);
    v->data_len += slen;
    v->offsets[v->len + 1] = v->data_len;
    v->len++;
}

static void strvec_free(StrVec *v) {
    free(v->data);
    free(v->offsets);
    memset(v, 0, sizeof(*v));
}

/* ------------------------------------------------------------------ */
/*  Main diff implementation                                           */
/* ------------------------------------------------------------------ */

SEXP C_diff_vtr(SEXP path_a_sexp, SEXP path_b_sexp, SEXP key_col_sexp) {
    const char *path_a  = CHAR(STRING_ELT(path_a_sexp, 0));
    const char *path_b  = CHAR(STRING_ELT(path_b_sexp, 0));
    const char *key_col = CHAR(STRING_ELT(key_col_sexp, 0));

    /* ---- Validate key column exists in both files ---- */
    Vtr1File *fa = vtr1_open(path_a);
    int key_idx_a = vec_schema_find_col(&fa->header.schema, key_col);
    if (key_idx_a < 0) {
        vtr1_close(fa);
        vectra_error("key_col '%s' not found in old_path", key_col);
    }
    VecType key_type = fa->header.schema.col_types[key_idx_a];
    vtr1_close(fa);

    Vtr1File *fb = vtr1_open(path_b);
    int key_idx_b = vec_schema_find_col(&fb->header.schema, key_col);
    if (key_idx_b < 0) {
        vtr1_close(fb);
        vectra_error("key_col '%s' not found in new_path", key_col);
    }
    VecType key_type_b = fb->header.schema.col_types[key_idx_b];
    vtr1_close(fb);

    if (key_type != key_type_b)
        vectra_error("key_col '%s' has different types in old_path and new_path",
                     key_col);

    /* ---- Pass 1: stream A, build hash set of all keys ---- */
    DiffKeyArena arena;
    dka_init(&arena, key_type);

    VecHashTable ht = vec_ht_create(256);

    /* Scan A with only the key column */
    int col_idx_a[1] = { key_idx_a };
    ScanNode *scan_a = scan_node_create(path_a, col_idx_a, 1);
    VecNode  *node_a = (VecNode *)scan_a;

    VecBatch *batch;
    while ((batch = node_a->next_batch(node_a)) != NULL) {
        int64_t n_logical = vec_batch_logical_rows(batch);
        /* The key is always in column 0 of the single-column batch */
        const VecArray *key_arr = &batch->columns[0];

        for (int64_t li = 0; li < n_logical; li++) {
            int64_t pi = vec_batch_physical_row(batch, li);
            uint64_t h = vec_hash_value(key_arr, pi);
            int was_new = 0;
            vec_ht_find_or_insert(&ht, h,
                                   key_arr, 1, pi,
                                   &arena.arena, arena.length,
                                   &was_new);
            if (was_new)
                dka_append(&arena, key_arr, pi);
        }
        vec_batch_free(batch);
    }
    node_a->free_node(node_a);

    int64_t n_a_keys = arena.length;

    /* ---- Allocate seen_in_b flags for each key in A ---- */
    uint8_t *seen_in_b = (uint8_t *)calloc((size_t)(n_a_keys > 0 ? n_a_keys : 1),
                                            sizeof(uint8_t));
    if (!seen_in_b) vectra_error("C_diff_vtr: alloc failed for seen_in_b");

    /* ---- Pass 2: stream B, classify each key ---- */
    /* Collect added keys (type-specific growing buffers) */
    I64Vec added_i64;
    StrVec added_str;
    i64vec_init(&added_i64);
    strvec_init(&added_str);

    int col_idx_b[1] = { key_idx_b };
    ScanNode *scan_b = scan_node_create(path_b, col_idx_b, 1);
    VecNode  *node_b = (VecNode *)scan_b;

    while ((batch = node_b->next_batch(node_b)) != NULL) {
        int64_t n_logical = vec_batch_logical_rows(batch);
        const VecArray *key_arr = &batch->columns[0];

        for (int64_t li = 0; li < n_logical; li++) {
            int64_t pi = vec_batch_physical_row(batch, li);
            uint64_t h = vec_hash_value(key_arr, pi);
            int was_new = 0;
            int64_t gid = vec_ht_find_or_insert(&ht, h,
                                                 key_arr, 1, pi,
                                                 &arena.arena, arena.length,
                                                 &was_new);
            if (was_new) {
                /* Key in B but not A: added */
                switch (key_type) {
                case VEC_INT64:
                    i64vec_push(&added_i64,
                                vec_array_is_valid(key_arr, pi)
                                    ? key_arr->buf.i64[pi]
                                    : INT64_MIN);
                    break;
                case VEC_DOUBLE:
                    /* store as bit pattern via int64 reinterpret */
                    {
                        double dv = vec_array_is_valid(key_arr, pi)
                                    ? key_arr->buf.dbl[pi]
                                    : 0.0 / 0.0; /* NaN for NA */
                        int64_t iv;
                        memcpy(&iv, &dv, sizeof(int64_t));
                        i64vec_push(&added_i64, iv);
                    }
                    break;
                case VEC_BOOL:
                    i64vec_push(&added_i64,
                                vec_array_is_valid(key_arr, pi)
                                    ? (int64_t)key_arr->buf.bln[pi]
                                    : -1);
                    break;
                case VEC_STRING: {
                    if (!vec_array_is_valid(key_arr, pi)) {
                        strvec_push(&added_str, NULL, 0);
                    } else {
                        int64_t s    = key_arr->buf.str.offsets[pi];
                        int64_t e    = key_arr->buf.str.offsets[pi + 1];
                        strvec_push(&added_str,
                                    key_arr->buf.str.data + s, e - s);
                    }
                    break;
                }
                }
                /* extend arena and seen_in_b for completeness (new group_id
                   was assigned by find_or_insert; we don't need to mark it) */
                dka_append(&arena, key_arr, pi);
                /* ensure seen_in_b covers the new group */
                int64_t new_total = arena.length;
                seen_in_b = (uint8_t *)realloc(seen_in_b, (size_t)new_total);
                if (!seen_in_b) vectra_error("C_diff_vtr: realloc failed");
                seen_in_b[new_total - 1] = 0; /* new B-only key, not needed */
            } else {
                /* Key found in A: mark as seen */
                seen_in_b[gid] = 1;
            }
        }
        vec_batch_free(batch);
    }
    node_b->free_node(node_b);

    vec_ht_free(&ht);

    /* ---- Build deleted_keys from A keys not seen in B ---- */
    /* Collect deleted indices first, then gather from arena */
    int64_t n_deleted = 0;
    for (int64_t i = 0; i < n_a_keys; i++)
        if (!seen_in_b[i]) n_deleted++;

    /* Build a selection vector for arena gather */
    int32_t *del_sel = NULL;
    if (n_deleted > 0) {
        del_sel = (int32_t *)malloc((size_t)n_deleted * sizeof(int32_t));
        if (!del_sel) vectra_error("C_diff_vtr: alloc failed for del_sel");
        int64_t j = 0;
        for (int64_t i = 0; i < n_a_keys; i++)
            if (!seen_in_b[i]) del_sel[j++] = (int32_t)i;
    }
    free(seen_in_b);

    /* Gather deleted keys from arena */
    VecArray del_arr = vec_array_gather(&arena.arena, del_sel, (int32_t)n_deleted);
    free(del_sel);
    dka_free(&arena);

    SEXP deleted_sexp = PROTECT(array_col_to_sexp(&del_arr));
    vec_array_free(&del_arr);

    /* ---- Build added_keys SEXP ---- */
    SEXP added_sexp;
    switch (key_type) {
    case VEC_INT64: {
        added_sexp = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t)added_i64.len));
        double *p = REAL(added_sexp);
        for (int64_t i = 0; i < added_i64.len; i++)
            p[i] = (added_i64.data[i] == INT64_MIN)
                   ? NA_REAL
                   : (double)added_i64.data[i];
        i64vec_free(&added_i64);
        break;
    }
    case VEC_DOUBLE: {
        added_sexp = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t)added_i64.len));
        double *p = REAL(added_sexp);
        for (int64_t i = 0; i < added_i64.len; i++) {
            double dv;
            memcpy(&dv, &added_i64.data[i], sizeof(double));
            p[i] = dv;
        }
        i64vec_free(&added_i64);
        break;
    }
    case VEC_BOOL: {
        added_sexp = PROTECT(Rf_allocVector(LGLSXP, (R_xlen_t)added_i64.len));
        int *p = LOGICAL(added_sexp);
        for (int64_t i = 0; i < added_i64.len; i++)
            p[i] = (added_i64.data[i] < 0) ? NA_LOGICAL : (int)added_i64.data[i];
        i64vec_free(&added_i64);
        break;
    }
    case VEC_STRING: {
        int64_t n_added = added_str.len;
        added_sexp = PROTECT(Rf_allocVector(STRSXP, (R_xlen_t)n_added));
        for (int64_t i = 0; i < n_added; i++) {
            int64_t s    = added_str.offsets[i];
            int64_t e    = added_str.offsets[i + 1];
            int64_t slen = e - s;
            SET_STRING_ELT(added_sexp, (R_xlen_t)i,
                Rf_mkCharLenCE(added_str.data + s, (int)slen, CE_UTF8));
        }
        strvec_free(&added_str);
        break;
    }
    default:
        added_sexp = PROTECT(Rf_allocVector(REALSXP, 0));
        i64vec_free(&added_i64);
        break;
    }

    /* ---- Assemble result list ---- */
    SEXP result    = PROTECT(Rf_allocVector(VECSXP, 2));
    SEXP res_names = PROTECT(Rf_allocVector(STRSXP, 2));
    SET_VECTOR_ELT(result, 0, added_sexp);
    SET_VECTOR_ELT(result, 1, deleted_sexp);
    SET_STRING_ELT(res_names, 0, Rf_mkChar("added_keys"));
    SET_STRING_ELT(res_names, 1, Rf_mkChar("deleted_keys"));
    Rf_setAttrib(result, R_NamesSymbol, res_names);

    UNPROTECT(4);  /* result, res_names, added_sexp, deleted_sexp */
    return result;
}
