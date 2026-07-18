#include "vtr_diff.h"
#include "vtr1_tdc.h"
#include "scan.h"
#include "schema.h"
#include "sort.h"
#include "builder.h"
#include "array.h"
#include "batch.h"
#include "error.h"
#include "r_bridge_internal.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

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
/*  Bounded sweep-merge diff                                           */
/* ------------------------------------------------------------------ */
/*
 * Both files are streamed through the external sort (keyed ascending by the key
 * column, NA last) and swept in one merged pass. No hash set of A's keys is held
 * resident -- peak state is the two sorts' own bounded spill, the current output
 * chunk of added rows, and the deleted-key builder (which is the returned diff).
 * The key is a primary key (unique per file), but the sweep still collapses any
 * equal-key run defensively so repeated keys collapse to one, matching the
 * old set semantics. sort_compare_value is shared with the sort's own merge, so
 * the sweep and the sort agree on ordering (NaN clustering, NA placement).
 */

#define DIFF_EMIT_CHUNK 8192

typedef struct {
    VecNode  *node;
    VecBatch *batch;
    int64_t   li, nlog;
    int       done;
    int       key_col;
    int64_t   phys;      /* physical row of the current position */
} DiffCursor;

static void dcur_load(DiffCursor *c) {
    if (c->batch) { vec_batch_free(c->batch); c->batch = NULL; }
    c->batch = c->node->next_batch(c->node);
    if (!c->batch) { c->done = 1; return; }
    c->li   = 0;
    c->nlog = vec_batch_logical_rows(c->batch);
}

/* Advance to the next physical row, loading batches as needed. */
static void dcur_next(DiffCursor *c) {
    while (!c->done) {
        if (!c->batch || c->li >= c->nlog) {
            dcur_load(c);
            if (c->done) return;
            continue;
        }
        c->phys = vec_batch_physical_row(c->batch, c->li);
        c->li++;
        return;
    }
}

static inline const VecArray *dcur_key(const DiffCursor *c) {
    return &c->batch->columns[c->key_col];
}

/* One-row copy of the cursor's current key (survives batch reload). */
static VecArray dcur_key_snap(const DiffCursor *c) {
    int32_t r = (int32_t)c->phys;
    return vec_array_gather(dcur_key(c), &r, 1);
}

/* Advance the cursor past every row whose key equals `ref` (a 1-row array). */
static void dcur_skip_run(DiffCursor *c, const VecArray *ref) {
    dcur_next(c);
    while (!c->done) {
        if (sort_compare_value(dcur_key(c), c->phys, ref, 0, 0, 1) != 0) break;
        dcur_next(c);
    }
}

/* Wrap a child scan in a SortNode keyed ascending (NA last) by one column. */
static VecNode *diff_sort_by_key(VecNode *child, int key_col,
                                 const char *temp_dir, int64_t mem_budget) {
    SortKey *sk = (SortKey *)malloc(sizeof(SortKey));
    if (!sk) vectra_error("C_diff_vtr: alloc failed for sort key");
    sk->col_index = key_col;
    sk->descending = 0;
    sk->na_last = 1;
    int64_t m = mem_budget > 0 ? mem_budget : VECTRA_SORT_MEM_DEFAULT;
    /* sort_node_create takes ownership of sk. */
    return (VecNode *)sort_node_create(child, 1, sk, temp_dir, m);
}

/* Flush the accumulated added-row builders as one row group. Re-inits the
   builders to empty for the next chunk. */
static void diff_flush_added(Vtr1TdcWriter *w, const VecSchema *bs,
                             VecArrayBuilder *bb, int64_t *added_n) {
    if (*added_n == 0) return;
    int nb = bs->n_cols;
    VecBatch *batch = vec_batch_alloc(nb, *added_n);
    for (int c = 0; c < nb; c++)
        batch->columns[c] = vec_builder_finish(&bb[c]);
    for (int c = 0; c < nb; c++) {
        free(batch->col_names[c]);
        batch->col_names[c] = strdup(bs->col_names[c]);
    }
    batch->n_rows = *added_n;
    vtr1_write_rowgroup_tdc(w, batch, VTR_COMPRESS_FAST, NULL, NULL);
    vec_batch_free(batch);
    for (int c = 0; c < nb; c++)
        bb[c] = vec_builder_init(bs->col_types[c]);
    *added_n = 0;
}

/* ------------------------------------------------------------------ */
/*  Main diff implementation                                           */
/* ------------------------------------------------------------------ */

SEXP C_diff_vtr(SEXP path_a_sexp, SEXP path_b_sexp, SEXP key_col_sexp,
                SEXP mem_sexp) {
    const char *path_a  = CHAR(STRING_ELT(path_a_sexp, 0));
    const char *path_b  = CHAR(STRING_ELT(path_b_sexp, 0));
    const char *key_col = CHAR(STRING_ELT(key_col_sexp, 0));
    int64_t mem_budget  = (int64_t)Rf_asReal(mem_sexp);

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

    const char *temp_dir = get_r_tempdir();

    /* ---- Build a temp .vtr path for the added rows ---- */
    SEXP td_call   = PROTECT(Rf_lang1(Rf_install("tempdir")));
    SEXP td_result = PROTECT(Rf_eval(td_call, R_BaseEnv));
    const char *tmpdir = CHAR(STRING_ELT(td_result, 0));

    static unsigned int diff_counter = 0;
    diff_counter++;

    size_t tmpdir_len = strlen(tmpdir);
    static const char prefix[] = "/vectra_diff_added_";
    char counter_str[32];
    int counter_len = snprintf(counter_str, sizeof(counter_str), "%u", diff_counter);
    static const char ext[] = ".vtr";
    size_t tmp_path_len = tmpdir_len + strlen(prefix) + (size_t)counter_len + strlen(ext);
    char *tmp_path = (char *)malloc(tmp_path_len + 1);
    if (!tmp_path) {
        UNPROTECT(2);
        vec_schema_free(&b_schema);
        vectra_error("C_diff_vtr: alloc failed for tmp_path");
    }
    memcpy(tmp_path, tmpdir, tmpdir_len);
    memcpy(tmp_path + tmpdir_len, prefix, strlen(prefix));
    memcpy(tmp_path + tmpdir_len + strlen(prefix), counter_str, (size_t)counter_len);
    memcpy(tmp_path + tmpdir_len + strlen(prefix) + (size_t)counter_len,
           ext, strlen(ext) + 1);
    UNPROTECT(2);

    /* ---- Wire both sides through the external sort, keyed by the key col ---- */
    int col_idx_a[1] = { key_idx_a };
    VecNode *node_a = diff_sort_by_key(
        (VecNode *)scan_node_create(path_a, col_idx_a, 1),
        0, temp_dir, mem_budget);
    VecNode *node_b = diff_sort_by_key(
        (VecNode *)scan_node_create(path_b, NULL, 0),
        key_idx_b, temp_dir, mem_budget);

    Vtr1TdcWriter *tmp_w = vtr1_open_tdc_writer(tmp_path, &b_schema);

    /* Added-row builders (all B columns); deleted-key builder (key col only). */
    int nbcols = b_schema.n_cols;
    VecArrayBuilder *bb = (VecArrayBuilder *)calloc((size_t)nbcols,
                                                    sizeof(VecArrayBuilder));
    if (!bb) vectra_error("C_diff_vtr: alloc failed for added builders");
    for (int c = 0; c < nbcols; c++)
        bb[c] = vec_builder_init(b_schema.col_types[c]);
    int64_t added_n = 0;
    VecArrayBuilder del_b = vec_builder_init(key_type);

    /* ---- Sweep-merge the two sorted key streams ---- */
    DiffCursor A = {0}, B = {0};
    A.node = node_a; A.key_col = 0;
    B.node = node_b; B.key_col = key_idx_b;
    dcur_next(&A);
    dcur_next(&B);

    while (!A.done || !B.done) {
        int order;
        if (A.done)      order = 1;
        else if (B.done) order = -1;
        else order = sort_compare_value(dcur_key(&A), A.phys,
                                        dcur_key(&B), B.phys, 0, 1);

        if (order < 0) {
            /* key in A, absent from B -> deleted */
            vec_builder_append_one(&del_b, dcur_key(&A), A.phys);
            VecArray k = dcur_key_snap(&A);
            dcur_skip_run(&A, &k);
            vec_array_free(&k);
        } else if (order > 0) {
            /* key in B, absent from A -> added: emit the whole B row */
            for (int c = 0; c < nbcols; c++)
                vec_builder_append_one(&bb[c], &B.batch->columns[c], B.phys);
            added_n++;
            if (added_n >= DIFF_EMIT_CHUNK)
                diff_flush_added(tmp_w, &b_schema, bb, &added_n);
            VecArray k = dcur_key_snap(&B);
            dcur_skip_run(&B, &k);
            vec_array_free(&k);
        } else {
            /* key in both -> unchanged: skip the run on both sides */
            VecArray ka = dcur_key_snap(&A);
            dcur_skip_run(&A, &ka);
            vec_array_free(&ka);
            VecArray kb = dcur_key_snap(&B);
            dcur_skip_run(&B, &kb);
            vec_array_free(&kb);
        }
    }

    diff_flush_added(tmp_w, &b_schema, bb, &added_n);

    if (A.batch) vec_batch_free(A.batch);
    if (B.batch) vec_batch_free(B.batch);
    node_a->free_node(node_a);
    node_b->free_node(node_b);
    vtr1_close_tdc_writer(tmp_w);
    for (int c = 0; c < nbcols; c++)
        vec_builder_free(&bb[c]);
    free(bb);
    vec_schema_free(&b_schema);

    /* ---- Assemble result ---- */
    VecArray del_arr = vec_builder_finish(&del_b);
    SEXP deleted_sexp = PROTECT(array_col_to_sexp(&del_arr));
    vec_array_free(&del_arr);

    SEXP added_path_sexp = PROTECT(Rf_allocVector(STRSXP, 1));
    SET_STRING_ELT(added_path_sexp, 0, Rf_mkCharCE(tmp_path, CE_UTF8));
    free(tmp_path);

    SEXP result    = PROTECT(Rf_allocVector(VECSXP, 2));
    SEXP res_names = PROTECT(Rf_allocVector(STRSXP, 2));
    SET_VECTOR_ELT(result, 0, added_path_sexp);
    SET_VECTOR_ELT(result, 1, deleted_sexp);
    SET_STRING_ELT(res_names, 0, Rf_mkChar("added_path"));
    SET_STRING_ELT(res_names, 1, Rf_mkChar("deleted_keys"));
    Rf_setAttrib(result, R_NamesSymbol, res_names);

    UNPROTECT(4);
    return result;
}
