#include "vtr_diff.h"
#include "vtr1_tdc.h"
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
    case VEC_INT32:
        memcpy(new_arr.buf.i32, old.buf.i32,
               (size_t)old.length * sizeof(int32_t));
        break;
    case VEC_INT16:
        memcpy(new_arr.buf.i16, old.buf.i16,
               (size_t)old.length * sizeof(int16_t));
        break;
    case VEC_INT8:
        memcpy(new_arr.buf.i8, old.buf.i8,
               (size_t)old.length * sizeof(int8_t));
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
        case VEC_INT32:
            a->buf.i32[pos] = col->buf.i32[row];
            break;
        case VEC_INT16:
            a->buf.i16[pos] = col->buf.i16[row];
            break;
        case VEC_INT8:
            a->buf.i8[pos] = col->buf.i8[row];
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
    case VEC_INT32: {
        out = PROTECT(Rf_allocVector(INTSXP, (R_xlen_t)n));
        int *p = INTEGER(out);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i))
                p[i] = NA_INTEGER;
            else
                p[i] = (int)arr->buf.i32[i];
        }
        UNPROTECT(1);
        return out;
    }
    case VEC_INT16: {
        out = PROTECT(Rf_allocVector(INTSXP, (R_xlen_t)n));
        int *p = INTEGER(out);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i))
                p[i] = NA_INTEGER;
            else
                p[i] = (int)arr->buf.i16[i];
        }
        UNPROTECT(1);
        return out;
    }
    case VEC_INT8: {
        out = PROTECT(Rf_allocVector(INTSXP, (R_xlen_t)n));
        int *p = INTEGER(out);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(arr, i))
                p[i] = NA_INTEGER;
            else
                p[i] = (int)arr->buf.i8[i];
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
                if (slen == 0) {
                    SET_STRING_ELT(out, (R_xlen_t)i, R_BlankString);
                } else {
                    SET_STRING_ELT(out, (R_xlen_t)i,
                        Rf_mkCharLenCE(arr->buf.str.data + s,
                                       (int)slen, CE_UTF8));
                }
            }
        }
        UNPROTECT(1);
        return out;
    }
    }
    return R_NilValue;
}

/* ------------------------------------------------------------------ */
/*  Main diff implementation                                           */
/* ------------------------------------------------------------------ */

SEXP C_diff_vtr(SEXP path_a_sexp, SEXP path_b_sexp, SEXP key_col_sexp) {
    const char *path_a  = CHAR(STRING_ELT(path_a_sexp, 0));
    const char *path_b  = CHAR(STRING_ELT(path_b_sexp, 0));
    const char *key_col = CHAR(STRING_ELT(key_col_sexp, 0));

    /* ---- Validate key column exists in both files ---- */
    Vtr1TdcFile *fa = vtr1_open_tdc(path_a);
    if (!fa) vectra_error("vtr1_open_tdc failed for %s", path_a);
    const VecSchema *fa_schema = vtr1_tdc_schema(fa);
    int key_idx_a = vec_schema_find_col(fa_schema, key_col);
    if (key_idx_a < 0) {
        vtr1_close_tdc(fa);
        vectra_error("key_col '%s' not found in old_path", key_col);
    }
    VecType key_type = fa_schema->col_types[key_idx_a];
    vtr1_close_tdc(fa);

    Vtr1TdcFile *fb = vtr1_open_tdc(path_b);
    if (!fb) vectra_error("vtr1_open_tdc failed for %s", path_b);
    const VecSchema *fb_schema = vtr1_tdc_schema(fb);
    int key_idx_b = vec_schema_find_col(fb_schema, key_col);
    if (key_idx_b < 0) {
        vtr1_close_tdc(fb);
        vectra_error("key_col '%s' not found in new_path", key_col);
    }
    VecType key_type_b = fb_schema->col_types[key_idx_b];

    /* Capture B's full schema for the temp file header */
    VecSchema b_schema = vec_schema_copy(fb_schema);
    vtr1_close_tdc(fb);

    if (key_type != key_type_b) {
        vec_schema_free(&b_schema);
        vectra_error("key_col '%s' has different types in old_path and new_path",
                     key_col);
    }

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
    if (!seen_in_b) {
        vec_schema_free(&b_schema);
        vectra_error("C_diff_vtr: alloc failed for seen_in_b");
    }

    /* ---- Open temp .vtr file to stream added rows ---- */
    /* Build a temp path: <R tempdir>/vectra_diff_XXXXXX.vtr */
    {
        /* Compute a temp path using R's tempdir */
        SEXP td_call   = PROTECT(Rf_lang1(Rf_install("tempdir")));
        SEXP td_result = PROTECT(Rf_eval(td_call, R_BaseEnv));
        const char *tmpdir = CHAR(STRING_ELT(td_result, 0));

        static const char suffix[] = "/vectra_diff_added.vtr";
        /* Use a counter to make unique names within a session */
        static unsigned int diff_counter = 0;
        diff_counter++;

        /* Build path: <tmpdir>/vectra_diff_added_<counter>.vtr */
        size_t tmpdir_len = strlen(tmpdir);
        static const char prefix[] = "/vectra_diff_added_";
        char counter_str[32];
        int counter_len = snprintf(counter_str, sizeof(counter_str),
                                   "%u", diff_counter);
        static const char ext[] = ".vtr";
        size_t tmp_path_len = tmpdir_len
                            + strlen(prefix)
                            + (size_t)counter_len
                            + strlen(ext);
        char *tmp_path = (char *)malloc(tmp_path_len + 1);
        if (!tmp_path) {
            UNPROTECT(2);
            vec_schema_free(&b_schema);
            free(seen_in_b);
            vectra_error("C_diff_vtr: alloc failed for tmp_path");
        }
        memcpy(tmp_path, tmpdir, tmpdir_len);
        memcpy(tmp_path + tmpdir_len, prefix, strlen(prefix));
        memcpy(tmp_path + tmpdir_len + strlen(prefix),
               counter_str, (size_t)counter_len);
        memcpy(tmp_path + tmpdir_len + strlen(prefix) + (size_t)counter_len,
               ext, strlen(ext) + 1); /* +1 for '\0' */

        UNPROTECT(2);

        /* Suppress unused-variable warning for 'suffix' */
        (void)suffix;

        Vtr1TdcWriter *tmp_w = vtr1_open_tdc_writer(tmp_path, &b_schema);
        /* tdc writer aborts via vectra_error on open failure; on success
         * it owns the file handle for the duration of the diff pass. */

        /* ---- Pass 2: stream ALL columns of B, write added rows ---- */
        ScanNode *scan_b = scan_node_create(path_b, NULL, 0);
        VecNode  *node_b = (VecNode *)scan_b;

        while ((batch = node_b->next_batch(node_b)) != NULL) {
            int64_t n_logical = vec_batch_logical_rows(batch);
            /* Key column is at key_idx_b in the full-scan batch */
            const VecArray *key_arr = &batch->columns[key_idx_b];

            /* Allocate a per-batch selection buffer for added rows.
               vec_batch_compact -> vec_batch_free will free batch->sel,
               so we hand ownership to the batch after setting it. */
            int32_t *added_sel = (int32_t *)malloc(
                (size_t)(n_logical > 0 ? n_logical : 1) * sizeof(int32_t));
            if (!added_sel) {
                vec_batch_free(batch);
                node_b->free_node(node_b);
                vtr1_close_tdc_writer(tmp_w);
                free(tmp_path);
                vec_schema_free(&b_schema);
                free(seen_in_b);
                vectra_error("C_diff_vtr: alloc failed for added_sel");
            }
            int32_t n_added_this_batch = 0;

            for (int64_t li = 0; li < n_logical; li++) {
                int64_t pi = vec_batch_physical_row(batch, li);
                uint64_t h = vec_hash_value(key_arr, pi);
                int was_new = 0;
                int64_t gid = vec_ht_find_or_insert(&ht, h,
                                                     key_arr, 1, pi,
                                                     &arena.arena, arena.length,
                                                     &was_new);
                if (was_new) {
                    /* Key in B but not A: record physical row for this batch */
                    added_sel[n_added_this_batch++] = (int32_t)pi;
                    /* Extend arena and seen_in_b for completeness */
                    dka_append(&arena, key_arr, pi);
                    int64_t new_total = arena.length;
                    seen_in_b = (uint8_t *)realloc(seen_in_b, (size_t)new_total);
                    if (!seen_in_b) {
                        vec_batch_free(batch);
                        node_b->free_node(node_b);
                        free(added_sel);
                        vtr1_close_tdc_writer(tmp_w);
                        free(tmp_path);
                        vec_schema_free(&b_schema);
                        vectra_error("C_diff_vtr: realloc failed for seen_in_b");
                    }
                    seen_in_b[new_total - 1] = 0;
                } else {
                    /* Key found in A: mark as seen */
                    seen_in_b[gid] = 1;
                }
            }

            /* If this batch has any added rows, write them as a row group.
               Install the selection vector into the batch; vec_batch_compact
               will compact it into a new flat batch and free the original
               (including batch->sel = added_sel). */
            if (n_added_this_batch > 0) {
                batch->sel   = added_sel;
                batch->sel_n = n_added_this_batch;
                VecBatch *compact = vec_batch_compact(batch);
                /* batch is now freed by compact — do not use */
                vtr1_write_rowgroup_tdc(tmp_w, compact, VTR_COMPRESS_FAST,
                                        NULL, NULL);
                vec_batch_free(compact);
            } else {
                free(added_sel);
                vec_batch_free(batch);
            }
        }
        node_b->free_node(node_b);

        /* tdc writer self-finalizes the trailing rowgroup index in close;
         * no equivalent of v4's manual n_rowgroups patch is needed. */
        vtr1_close_tdc_writer(tmp_w);

        vec_ht_free(&ht);
        vec_schema_free(&b_schema);

        /* ---- Build deleted_keys from A keys not seen in B ---- */
        int64_t n_deleted = 0;
        for (int64_t i = 0; i < n_a_keys; i++)
            if (!seen_in_b[i]) n_deleted++;

        int32_t *del_sel = NULL;
        if (n_deleted > 0) {
            del_sel = (int32_t *)malloc((size_t)n_deleted * sizeof(int32_t));
            if (!del_sel) {
                free(seen_in_b);
                free(tmp_path);
                dka_free(&arena);
                vectra_error("C_diff_vtr: alloc failed for del_sel");
            }
            int64_t j = 0;
            for (int64_t i = 0; i < n_a_keys; i++)
                if (!seen_in_b[i]) del_sel[j++] = (int32_t)i;
        }
        free(seen_in_b);

        /* Gather deleted keys from arena */
        VecArray del_arr = vec_array_gather(&arena.arena, del_sel, (int32_t)n_deleted);
        free(del_sel);
        dka_free(&arena);

        SEXP deleted_sexp  = PROTECT(array_col_to_sexp(&del_arr));
        vec_array_free(&del_arr);

        /* Return the temp path as a string */
        SEXP added_path_sexp = PROTECT(Rf_allocVector(STRSXP, 1));
        SET_STRING_ELT(added_path_sexp, 0, Rf_mkCharCE(tmp_path, CE_UTF8));
        free(tmp_path);

        /* ---- Assemble result list ---- */
        SEXP result    = PROTECT(Rf_allocVector(VECSXP, 2));
        SEXP res_names = PROTECT(Rf_allocVector(STRSXP, 2));
        SET_VECTOR_ELT(result, 0, added_path_sexp);
        SET_VECTOR_ELT(result, 1, deleted_sexp);
        SET_STRING_ELT(res_names, 0, Rf_mkChar("added_path"));
        SET_STRING_ELT(res_names, 1, Rf_mkChar("deleted_keys"));
        Rf_setAttrib(result, R_NamesSymbol, res_names);

        UNPROTECT(4);  /* result, res_names, added_path_sexp, deleted_sexp */
        return result;
    }
}
