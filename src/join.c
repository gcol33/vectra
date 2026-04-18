#include "join.h"
#include "vec_omp.h"
#include "hash.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "builder.h"
#include "coerce.h"
#include "project.h"
#include "expr.h"
#include "sort.h"
#include "scan.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* Forward declarations */
static int child_sorted_on_keys(VecNode *child, const int *key_idx, int n_keys);

/* FNV-1a constants (must match hash.c) */
#define FNV_OFFSET 14695981039346656037ULL
#define FNV_PRIME  1099511628211ULL

/* ------------------------------------------------------------------ */
/*  JoinHT: hash table for build side                                  */
/* ------------------------------------------------------------------ */

static JoinHT jht_create(int64_t n_build_rows) {
    JoinHT jht;
    int64_t n_slots = 64;
    while (n_slots < n_build_rows * 2) n_slots *= 2;
    jht.n_slots = n_slots;
    jht.head = (int64_t *)malloc((size_t)n_slots * sizeof(int64_t));
    jht.slot_hash = (uint64_t *)malloc((size_t)n_slots * sizeof(uint64_t));
    jht.build_next = (int64_t *)malloc((size_t)n_build_rows * sizeof(int64_t));
    jht.n_build = n_build_rows;
    if (!jht.head || !jht.slot_hash || !jht.build_next)
        vectra_error("alloc failed for join hash table");
    memset(jht.head, -1, (size_t)n_slots * sizeof(int64_t));
    memset(jht.build_next, -1, (size_t)n_build_rows * sizeof(int64_t));
    return jht;
}

static void jht_free(JoinHT *jht) {
    free(jht->head);    jht->head = NULL;
    free(jht->slot_hash); jht->slot_hash = NULL;
    free(jht->build_next); jht->build_next = NULL;
}

static void jht_insert(JoinHT *jht, uint64_t hash, int64_t build_row) {
    int64_t mask = jht->n_slots - 1;
    int64_t slot = (int64_t)(hash & (uint64_t)mask);
    for (;;) {
        if (jht->head[slot] == -1) {
            jht->head[slot] = build_row;
            jht->slot_hash[slot] = hash;
            return;
        }
        if (jht->slot_hash[slot] == hash) {
            jht->build_next[build_row] = jht->head[slot];
            jht->head[slot] = build_row;
            return;
        }
        slot = (slot + 1) & mask;
    }
}

/* ------------------------------------------------------------------ */
/*  Key hashing and comparison                                         */
/* ------------------------------------------------------------------ */

static uint64_t hash_join_key(const VecArray *cols, const int *key_indices,
                              int n_keys, int64_t row) {
    uint64_t h = 0;
    for (int k = 0; k < n_keys; k++) {
        uint64_t kh = vec_hash_value(&cols[key_indices[k]], row);
        h = (k == 0) ? kh : vec_hash_combine(h, kh);
    }
    return h;
}

static int join_keys_equal(const VecArray *probe_cols, const int *probe_key_idx,
                           const VecArray *build_cols, const int *build_key_idx,
                           int n_keys, int64_t probe_row, int64_t build_row) {
    for (int k = 0; k < n_keys; k++) {
        const VecArray *pa = &probe_cols[probe_key_idx[k]];
        const VecArray *ba = &build_cols[build_key_idx[k]];
        int pv = vec_array_is_valid(pa, probe_row);
        int bv = vec_array_is_valid(ba, build_row);
        if (!pv || !bv) return 0;  /* NA never matches */
        switch (pa->type) {
        case VEC_INT64:
            if (pa->buf.i64[probe_row] != ba->buf.i64[build_row]) return 0;
            break;
        case VEC_INT32:
            if (pa->buf.i32[probe_row] != ba->buf.i32[build_row]) return 0;
            break;
        case VEC_INT16:
            if (pa->buf.i16[probe_row] != ba->buf.i16[build_row]) return 0;
            break;
        case VEC_INT8:
            if (pa->buf.i8[probe_row] != ba->buf.i8[build_row]) return 0;
            break;
        case VEC_DOUBLE:
            if (pa->buf.dbl[probe_row] != ba->buf.dbl[build_row]) return 0;
            break;
        case VEC_BOOL:
            if (pa->buf.bln[probe_row] != ba->buf.bln[build_row]) return 0;
            break;
        case VEC_STRING: {
            int64_t ps = pa->buf.str.offsets[probe_row];
            int64_t pe = pa->buf.str.offsets[probe_row + 1];
            int64_t bs = ba->buf.str.offsets[build_row];
            int64_t be = ba->buf.str.offsets[build_row + 1];
            int64_t plen = pe - ps, blen = be - bs;
            if (plen != blen) return 0;
            if (plen > 0 && memcmp(pa->buf.str.data + ps, ba->buf.str.data + bs,
                       (size_t)plen) != 0)
                return 0;
            break;
        }
        }
    }
    return 1;
}

static int64_t jht_probe(const JoinHT *jht, uint64_t hash,
                          const VecArray *probe_cols, const int *probe_key_idx,
                          const VecArray *build_cols, const int *build_key_idx,
                          int n_keys, int64_t probe_row) {
    int64_t mask = jht->n_slots - 1;
    int64_t slot = (int64_t)(hash & (uint64_t)mask);
    for (;;) {
        if (jht->head[slot] == -1) return -1;
        if (jht->slot_hash[slot] == hash) {
            int64_t br = jht->head[slot];
            while (br >= 0) {
                if (join_keys_equal(probe_cols, probe_key_idx,
                                    build_cols, build_key_idx,
                                    n_keys, probe_row, br))
                    return br;
                br = jht->build_next[br];
            }
        }
        slot = (slot + 1) & mask;
    }
}

static int64_t jht_chain_next(const JoinHT *jht, int64_t build_row,
                               const VecArray *probe_cols, const int *probe_key_idx,
                               const VecArray *build_cols, const int *build_key_idx,
                               int n_keys, int64_t probe_row) {
    int64_t br = jht->build_next[build_row];
    while (br >= 0) {
        if (join_keys_equal(probe_cols, probe_key_idx,
                            build_cols, build_key_idx,
                            n_keys, probe_row, br))
            return br;
        br = jht->build_next[br];
    }
    return -1;
}

/* ------------------------------------------------------------------ */
/*  Build phase: materialize right side into hash table                */
/* ------------------------------------------------------------------ */

static void join_build(JoinNode *jn) {
    const VecSchema *rschema = &jn->right->output_schema;
    jn->r_ncols = rschema->n_cols;

    VecArrayBuilder *r_builders = (VecArrayBuilder *)calloc(
        (size_t)jn->r_ncols, sizeof(VecArrayBuilder));
    for (int c = 0; c < jn->r_ncols; c++)
        r_builders[c] = vec_builder_init(rschema->col_types[c]);

    VecBatch *batch;
    while ((batch = jn->right->next_batch(jn->right)) != NULL) {
        if (!batch->sel) {
            for (int c = 0; c < jn->r_ncols; c++)
                vec_builder_append_array(&r_builders[c], &batch->columns[c]);
        } else {
            int64_t n_logical = vec_batch_logical_rows(batch);
            for (int c = 0; c < jn->r_ncols; c++)
                vec_builder_reserve(&r_builders[c], n_logical);
            for (int64_t li = 0; li < n_logical; li++) {
                int64_t pi = vec_batch_physical_row(batch, li);
                for (int c = 0; c < jn->r_ncols; c++)
                    vec_builder_append_one(&r_builders[c],
                                           &batch->columns[c], pi);
            }
        }
        vec_batch_free(batch);
    }

    int64_t r_nrows = r_builders[0].length;
    jn->r_cols = (VecArray *)malloc((size_t)jn->r_ncols * sizeof(VecArray));
    for (int c = 0; c < jn->r_ncols; c++)
        jn->r_cols[c] = vec_builder_finish(&r_builders[c]);
    free(r_builders);

    /* Coerce build-side key columns to match probe-side types */
    const VecSchema *lschema = &jn->left->output_schema;
    for (int k = 0; k < jn->n_keys; k++) {
        VecType lt = lschema->col_types[jn->lkey_idx[k]];
        VecType rt = jn->r_cols[jn->rkey_idx[k]].type;
        if (lt != rt) {
            VecType common = vec_common_type(lt, rt);
            if (jn->r_cols[jn->rkey_idx[k]].type != common) {
                VecArray *coerced = vec_coerce(
                    &jn->r_cols[jn->rkey_idx[k]], common);
                vec_array_free(&jn->r_cols[jn->rkey_idx[k]]);
                jn->r_cols[jn->rkey_idx[k]] = *coerced;
                free(coerced);
            }
        }
    }

    /* Check if both sides are sorted on join keys — use merge join if so */
    if (child_sorted_on_keys(jn->left, jn->lkey_idx, jn->n_keys) &&
        child_sorted_on_keys(jn->right, jn->rkey_idx, jn->n_keys)) {
        jn->use_merge = 1;
    }

    if (jn->use_merge) {
        /* Merge join: skip hash table, just store row count for cursor bounds */
        memset(&jn->jht, 0, sizeof(JoinHT));
        jn->jht.n_build = r_nrows;  /* reuse for row count */
    } else {
        /* Build hash table: pre-compute hashes in parallel, insert sequentially.
           Hashing is the expensive part (60-80% of build cost); insertion into
           the open-addressing table with chaining is cheap but has write conflicts. */
        jn->jht = jht_create(r_nrows > 0 ? r_nrows : 1);
        {
            uint64_t *build_hashes = (uint64_t *)malloc(
                (size_t)(r_nrows > 0 ? r_nrows : 1) * sizeof(uint64_t));
            if (!build_hashes) vectra_error("alloc failed for build hash array");

            #pragma omp parallel for if(r_nrows > VEC_OMP_THRESHOLD) schedule(static)
            for (int64_t r = 0; r < r_nrows; r++) {
                build_hashes[r] = hash_join_key(jn->r_cols, jn->rkey_idx,
                                                 jn->n_keys, r);
            }

            for (int64_t r = 0; r < r_nrows; r++)
                jht_insert(&jn->jht, build_hashes[r], r);

            free(build_hashes);
        }
    }

    /* full_join: allocate build_matched bitset */
    if (jn->kind == JOIN_FULL) {
        int64_t nbytes = (r_nrows + 7) / 8;
        jn->build_matched = (uint8_t *)calloc(
            nbytes > 0 ? (size_t)nbytes : 1, 1);
    }
}

/* ------------------------------------------------------------------ */
/*  Probe phase: process one left batch, return output batch           */
/* ------------------------------------------------------------------ */

/*
 * Streaming probe: process a single pre-fetched probe batch against the
 * hash table. Returns the output batch (may be NULL if output is empty,
 * e.g. anti_join where all rows matched). Caller owns pbatch lifetime.
 *
 * Lifetime contract:
 *   - Build side (r_cols) is owned by JoinNode and persists across calls.
 *   - Probe batch (pbatch) is read but NOT freed; caller frees it.
 *   - Output is built via vec_builder_append_one/na which deep-copy values.
 *   - For left_join: unmatched probe rows are emitted after all matches
 *     for the batch, using a per-batch `matched` bitset.
 */
/* Specialized 1-key hash functions: avoid generic dispatch per row */
static inline uint64_t hash_i64(int64_t val) {
    uint64_t h = FNV_OFFSET;
    const uint8_t *p = (const uint8_t *)&val;
    for (int k = 0; k < 8; k++) { h ^= p[k]; h *= FNV_PRIME; }
    return h;
}

static inline uint64_t hash_dbl(double val) {
    if (val == 0.0) val = 0.0; /* normalize -0 */
    uint64_t h = FNV_OFFSET;
    const uint8_t *p = (const uint8_t *)&val;
    for (int k = 0; k < 8; k++) { h ^= p[k]; h *= FNV_PRIME; }
    return h;
}

static inline uint64_t hash_string(const char *data, int64_t off, int64_t end) {
    uint64_t h = FNV_OFFSET;
    const uint8_t *p = (const uint8_t *)(data + off);
    int64_t len = end - off;
    for (int64_t k = 0; k < len; k++) { h ^= p[k]; h *= FNV_PRIME; }
    return h;
}

static VecBatch *join_probe_one(JoinNode *jn, VecBatch *pbatch) {
    const VecSchema *lschema = &jn->left->output_schema;
    int l_ncols = lschema->n_cols;
    int out_ncols = jn->base.output_schema.n_cols;
    int64_t p_logical = vec_batch_logical_rows(pbatch);

    /* Build coerced probe key columns for hashing/comparison.
       The batch itself stays untouched (originals used for output). */
    VecArray *coerced_probe_keys[16] = {0};
    /* Temporary columns array for hashing: same as pbatch->columns but
       with coerced key columns swapped in */
    int need_coerce = 0;
    for (int k = 0; k < jn->n_keys && k < 16; k++) {
        VecType pt = pbatch->columns[jn->lkey_idx[k]].type;
        VecType bt = jn->r_cols[jn->rkey_idx[k]].type;
        if (pt != bt) {
            coerced_probe_keys[k] = vec_coerce(
                &pbatch->columns[jn->lkey_idx[k]], bt);
            need_coerce = 1;
        }
    }
    /* Build a separate columns array for hash/compare only */
    VecArray *hash_cols = NULL;
    if (need_coerce) {
        hash_cols = (VecArray *)malloc((size_t)l_ncols * sizeof(VecArray));
        memcpy(hash_cols, pbatch->columns, (size_t)l_ncols * sizeof(VecArray));
        for (int k = 0; k < jn->n_keys && k < 16; k++) {
            if (coerced_probe_keys[k])
                hash_cols[jn->lkey_idx[k]] = *coerced_probe_keys[k];
        }
    }
    VecArray *probe_cols = need_coerce ? hash_cols : pbatch->columns;

    /* Initialize output builders with reserve for expected output */
    VecArrayBuilder *out = (VecArrayBuilder *)calloc(
        (size_t)out_ncols, sizeof(VecArrayBuilder));
    for (int c = 0; c < out_ncols; c++) {
        out[c] = vec_builder_init(jn->base.output_schema.col_types[c]);
        vec_builder_reserve(&out[c], p_logical);
    }

    /* For left_join/full_join: track which logical probe rows got a match */
    uint8_t *probe_matched = NULL;
    if (jn->kind == JOIN_LEFT || jn->kind == JOIN_FULL) {
        int64_t nbytes = (p_logical + 7) / 8;
        probe_matched = (uint8_t *)calloc(nbytes > 0 ? (size_t)nbytes : 1, 1);
    }

    /* Vectorized pre-hash: compute hashes for logical rows only */
    uint64_t *phash = (uint64_t *)malloc(
        (size_t)(p_logical > 0 ? p_logical : 1) * sizeof(uint64_t));
    if (!phash) vectra_error("alloc failed for probe hash array");

    /* Fast path: 1-key with specialized hash to avoid per-row dispatch.
       Each loop is embarrassingly parallel — phash[li] depends only on
       read-only input arrays, so we parallelize with OpenMP. */
    if (jn->n_keys == 1) {
        const VecArray *pkey = &probe_cols[jn->lkey_idx[0]];
        switch (pkey->type) {
        case VEC_INT64:
            #pragma omp parallel for if(p_logical > VEC_OMP_THRESHOLD) schedule(static)
            for (int64_t li = 0; li < p_logical; li++) {
                int64_t pi = vec_batch_physical_row(pbatch, li);
                phash[li] = vec_array_is_valid(pkey, pi)
                    ? hash_i64(pkey->buf.i64[pi])
                    : (FNV_OFFSET ^ 0xFF);
            }
            break;
        case VEC_DOUBLE:
            #pragma omp parallel for if(p_logical > VEC_OMP_THRESHOLD) schedule(static)
            for (int64_t li = 0; li < p_logical; li++) {
                int64_t pi = vec_batch_physical_row(pbatch, li);
                phash[li] = vec_array_is_valid(pkey, pi)
                    ? hash_dbl(pkey->buf.dbl[pi])
                    : (FNV_OFFSET ^ 0xFF);
            }
            break;
        case VEC_STRING:
            #pragma omp parallel for if(p_logical > VEC_OMP_THRESHOLD) schedule(static)
            for (int64_t li = 0; li < p_logical; li++) {
                int64_t pi = vec_batch_physical_row(pbatch, li);
                phash[li] = vec_array_is_valid(pkey, pi)
                    ? hash_string(pkey->buf.str.data,
                               pkey->buf.str.offsets[pi],
                               pkey->buf.str.offsets[pi + 1])
                    : (FNV_OFFSET ^ 0xFF);
            }
            break;
        default:
            #pragma omp parallel for if(p_logical > VEC_OMP_THRESHOLD) schedule(static)
            for (int64_t li = 0; li < p_logical; li++) {
                int64_t pi = vec_batch_physical_row(pbatch, li);
                phash[li] = hash_join_key(probe_cols, jn->lkey_idx,
                                           jn->n_keys, pi);
            }
            break;
        }
    } else {
        /* Generic composite key hash */
        #pragma omp parallel for if(p_logical > VEC_OMP_THRESHOLD) schedule(static)
        for (int64_t li = 0; li < p_logical; li++) {
            int64_t pi = vec_batch_physical_row(pbatch, li);
            phash[li] = hash_join_key(probe_cols, jn->lkey_idx,
                                       jn->n_keys, pi);
        }
    }

    /* Probe each logical row using pre-computed hashes */
    for (int64_t li = 0; li < p_logical; li++) {
        int64_t pr = vec_batch_physical_row(pbatch, li);
        int64_t br = jht_probe(&jn->jht, phash[li],
                                probe_cols, jn->lkey_idx,
                                jn->r_cols, jn->rkey_idx,
                                jn->n_keys, pr);

        switch (jn->kind) {
        case JOIN_SEMI:
            if (br >= 0) {
                for (int c = 0; c < l_ncols; c++)
                    vec_builder_append_one(&out[c], &pbatch->columns[c], pr);
            }
            break;

        case JOIN_ANTI:
            if (br < 0) {
                for (int c = 0; c < l_ncols; c++)
                    vec_builder_append_one(&out[c], &pbatch->columns[c], pr);
            }
            break;

        case JOIN_INNER:
            while (br >= 0) {
                for (int c = 0; c < l_ncols; c++)
                    vec_builder_append_one(&out[c], &pbatch->columns[c], pr);
                for (int j = 0; j < jn->r_non_key_count; j++)
                    vec_builder_append_one(&out[l_ncols + j],
                        &jn->r_cols[jn->r_non_key_idx[j]], br);
                br = jht_chain_next(&jn->jht, br,
                    probe_cols, jn->lkey_idx,
                    jn->r_cols, jn->rkey_idx, jn->n_keys, pr);
            }
            break;

        case JOIN_LEFT:
            if (br >= 0) {
                probe_matched[li / 8] |= (uint8_t)(1 << (li % 8));
                while (br >= 0) {
                    for (int c = 0; c < l_ncols; c++)
                        vec_builder_append_one(&out[c],
                            &pbatch->columns[c], pr);
                    for (int j = 0; j < jn->r_non_key_count; j++)
                        vec_builder_append_one(&out[l_ncols + j],
                            &jn->r_cols[jn->r_non_key_idx[j]], br);
                    br = jht_chain_next(&jn->jht, br,
                        pbatch->columns, jn->lkey_idx,
                        jn->r_cols, jn->rkey_idx, jn->n_keys, pr);
                }
            }
            break;

        case JOIN_FULL:
            if (br >= 0) {
                probe_matched[li / 8] |= (uint8_t)(1 << (li % 8));
                while (br >= 0) {
                    jn->build_matched[br / 8] |=
                        (uint8_t)(1 << (br % 8));
                    for (int c = 0; c < l_ncols; c++)
                        vec_builder_append_one(&out[c],
                            &pbatch->columns[c], pr);
                    for (int j = 0; j < jn->r_non_key_count; j++)
                        vec_builder_append_one(&out[l_ncols + j],
                            &jn->r_cols[jn->r_non_key_idx[j]], br);
                    br = jht_chain_next(&jn->jht, br,
                        pbatch->columns, jn->lkey_idx,
                        jn->r_cols, jn->rkey_idx, jn->n_keys, pr);
                }
            }
            break;
        }
    }

    free(phash);

    /* left_join / full_join: emit unmatched probe rows with NA right columns */
    if (jn->kind == JOIN_LEFT || jn->kind == JOIN_FULL) {
        for (int64_t li = 0; li < p_logical; li++) {
            if (probe_matched[li / 8] & (1 << (li % 8))) continue;
            int64_t pr = vec_batch_physical_row(pbatch, li);
            for (int c = 0; c < l_ncols; c++)
                vec_builder_append_one(&out[c], &pbatch->columns[c], pr);
            /* Bulk NA fill for all right non-key columns */
            for (int j = 0; j < jn->r_non_key_count; j++)
                vec_builder_append_na(&out[l_ncols + j]);
        }
        free(probe_matched);
    }

    /* Free coerced probe key arrays */
    for (int k = 0; k < jn->n_keys && k < 16; k++) {
        if (coerced_probe_keys[k]) {
            vec_array_free(coerced_probe_keys[k]);
            free(coerced_probe_keys[k]);
        }
    }
    free(hash_cols); /* NULL-safe */

    /* Build result batch */
    int64_t out_nrows = out[0].length;
    if (out_nrows == 0) {
        /* Empty batch (e.g. anti_join with all matches): free and try next */
        for (int c = 0; c < out_ncols; c++)
            vec_builder_free(&out[c]);
        free(out);
        return NULL; /* signal caller to try next batch */
    }

    VecBatch *result = vec_batch_alloc(out_ncols, out_nrows);
    for (int c = 0; c < out_ncols; c++) {
        result->columns[c] = vec_builder_finish(&out[c]);
        const char *nm = jn->base.output_schema.col_names[c];
        size_t nm_len = strlen(nm);
        result->col_names[c] = (char *)malloc(nm_len + 1);
        memcpy(result->col_names[c], nm, nm_len + 1);
    }
    free(out);
    return result;
}

/* ------------------------------------------------------------------ */
/*  Merge join: sorted merge for pre-sorted inputs                     */
/* ------------------------------------------------------------------ */

#define MERGE_JOIN_BATCH_SIZE 65536

/* Compare a single value from two arrays (ASC order, NAs sort last) */
static int merge_compare_value(const VecArray *a, int64_t ra,
                                const VecArray *b, int64_t rb) {
    int av = vec_array_is_valid(a, ra);
    int bv = vec_array_is_valid(b, rb);
    if (!av && !bv) return 0;
    if (!av) return 1;   /* NA sorts last */
    if (!bv) return -1;

    switch (a->type) {
    case VEC_DOUBLE: {
        double va = a->buf.dbl[ra], vb = b->buf.dbl[rb];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    case VEC_INT64: {
        int64_t va = a->buf.i64[ra], vb = b->buf.i64[rb];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    case VEC_INT32: {
        int32_t va = a->buf.i32[ra], vb = b->buf.i32[rb];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    case VEC_INT16: {
        int16_t va = a->buf.i16[ra], vb = b->buf.i16[rb];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    case VEC_INT8: {
        int8_t va = a->buf.i8[ra], vb = b->buf.i8[rb];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    case VEC_BOOL:
        return (int)a->buf.bln[ra] - (int)b->buf.bln[rb];
    case VEC_STRING: {
        int64_t sa = a->buf.str.offsets[ra], ea = a->buf.str.offsets[ra + 1];
        int64_t sb = b->buf.str.offsets[rb], eb = b->buf.str.offsets[rb + 1];
        int64_t la = ea - sa, lb = eb - sb;
        int64_t minlen = la < lb ? la : lb;
        int cmp = (minlen > 0) ? memcmp(a->buf.str.data + sa,
                                          b->buf.str.data + sb,
                                          (size_t)minlen) : 0;
        if (cmp == 0) cmp = (la < lb) ? -1 : (la > lb) ? 1 : 0;
        return cmp;
    }
    }
    return 0;
}

/* Compare join keys between left row and right row */
static int merge_compare_keys(const VecArray *l_cols, const int *l_key_idx,
                               const VecArray *r_cols, const int *r_key_idx,
                               int n_keys, int64_t l_row, int64_t r_row) {
    for (int k = 0; k < n_keys; k++) {
        int cmp = merge_compare_value(&l_cols[l_key_idx[k]], l_row,
                                       &r_cols[r_key_idx[k]], r_row);
        if (cmp != 0) return cmp;
    }
    return 0;
}

/* Check if a child node produces output sorted on the given key columns (ASC).
   Returns 1 if the child is a SortNode whose leading keys match, or a
   ScanNode reading a VTR file with col_sorted set for single-key cases. */
static int child_sorted_on_keys(VecNode *child, const int *key_idx,
                                 int n_keys) {
    if (strcmp(child->kind, "SortNode") == 0) {
        SortNode *sn = (SortNode *)child;
        if (sn->n_keys < n_keys) return 0;
        for (int k = 0; k < n_keys; k++) {
            if (sn->keys[k].col_index != key_idx[k]) return 0;
            if (sn->keys[k].descending) return 0;
        }
        return 1;
    }
    if (n_keys == 1 && strcmp(child->kind, "ScanNode") == 0) {
        ScanNode *sn = (ScanNode *)child;
        const uint8_t *cs = vtr1_tdc_col_sorted(sn->file);
        if (cs && cs[key_idx[0]])
            return 1;
    }
    return 0;
}

/* Advance to the next left row; pull new batch if needed.
   Returns 0 if advanced, 1 if left side is exhausted. */
static int merge_advance_left(JoinNode *jn) {
    jn->merge_l_pos++;
    int64_t logical = jn->merge_l_batch ?
        vec_batch_logical_rows(jn->merge_l_batch) : 0;
    if (jn->merge_l_batch && jn->merge_l_pos < logical)
        return 0;
    /* Need next batch */
    if (jn->merge_l_batch) {
        vec_batch_free(jn->merge_l_batch);
        jn->merge_l_batch = NULL;
    }
    jn->merge_l_batch = jn->left->next_batch(jn->left);
    if (!jn->merge_l_batch) {
        jn->merge_l_done = 1;
        return 1;
    }
    jn->merge_l_pos = 0;
    return 0;
}

/* Get the physical row index for the current left position */
static int64_t merge_left_phys(JoinNode *jn) {
    return vec_batch_physical_row(jn->merge_l_batch, jn->merge_l_pos);
}

/* Find the end of an equal-key run in the build side starting at r_start */
static int64_t merge_find_group_end(JoinNode *jn, int64_t r_start) {
    int64_t r_nrows = jn->jht.n_build;
    int64_t r_end = r_start + 1;
    while (r_end < r_nrows) {
        int cmp = merge_compare_keys(jn->r_cols, jn->rkey_idx,
                                      jn->r_cols, jn->rkey_idx,
                                      jn->n_keys, r_start, r_end);
        if (cmp != 0) break;
        r_end++;
    }
    return r_end;
}

static VecBatch *merge_join_batch(JoinNode *jn) {
    const VecSchema *lschema = &jn->left->output_schema;
    int l_ncols = lschema->n_cols;
    int out_ncols = jn->base.output_schema.n_cols;
    int64_t r_nrows = jn->jht.n_build;  /* reuse n_build for row count */

    VecArrayBuilder *out = (VecArrayBuilder *)calloc(
        (size_t)out_ncols, sizeof(VecArrayBuilder));
    for (int c = 0; c < out_ncols; c++) {
        out[c] = vec_builder_init(jn->base.output_schema.col_types[c]);
        vec_builder_reserve(&out[c], MERGE_JOIN_BATCH_SIZE);
    }

    int64_t emitted = 0;

    while (emitted < MERGE_JOIN_BATCH_SIZE) {
        /* Pull first left batch if not yet loaded */
        if (!jn->merge_l_batch && !jn->merge_l_done) {
            jn->merge_l_batch = jn->left->next_batch(jn->left);
            if (!jn->merge_l_batch) {
                jn->merge_l_done = 1;
            } else {
                jn->merge_l_pos = 0;
            }
        }

        /* If we're in the middle of a M:N group cross product, continue it */
        if (jn->merge_r_sub > 0 && jn->merge_r_sub < jn->merge_r_group_end
            && !jn->merge_l_done) {
            int64_t pr = merge_left_phys(jn);
            while (jn->merge_r_sub < jn->merge_r_group_end &&
                   emitted < MERGE_JOIN_BATCH_SIZE) {
                for (int c = 0; c < l_ncols; c++)
                    vec_builder_append_one(&out[c],
                        &jn->merge_l_batch->columns[c], pr);
                for (int j = 0; j < jn->r_non_key_count; j++)
                    vec_builder_append_one(&out[l_ncols + j],
                        &jn->r_cols[jn->r_non_key_idx[j]],
                        jn->merge_r_sub);
                if (jn->kind == JOIN_FULL)
                    jn->build_matched[jn->merge_r_sub / 8] |=
                        (uint8_t)(1 << (jn->merge_r_sub % 8));
                jn->merge_r_sub++;
                emitted++;
            }
            if (jn->merge_r_sub >= jn->merge_r_group_end) {
                /* Done with this left row's group; advance left */
                jn->merge_r_sub = 0;
                if (merge_advance_left(jn)) break;
                /* Check if next left row also matches this group */
                if (!jn->merge_l_done) {
                    int64_t npr = merge_left_phys(jn);
                    int cmp = merge_compare_keys(
                        jn->merge_l_batch->columns, jn->lkey_idx,
                        jn->r_cols, jn->rkey_idx,
                        jn->n_keys, npr, jn->merge_r_group);
                    if (cmp == 0) {
                        jn->merge_r_sub = jn->merge_r_group;
                        continue;  /* restart group for next left row */
                    }
                    /* Left row doesn't match group; reset cursor to group end */
                    jn->merge_r_cursor = jn->merge_r_group_end;
                }
            }
            continue;
        }

        /* Left exhausted */
        if (jn->merge_l_done) {
            /* FULL: remaining right rows handled by finalize */
            break;
        }

        /* Right exhausted */
        if (jn->merge_r_cursor >= r_nrows) {
            /* Emit remaining left rows for LEFT/FULL/ANTI */
            if (jn->kind == JOIN_LEFT || jn->kind == JOIN_FULL) {
                while (!jn->merge_l_done &&
                       emitted < MERGE_JOIN_BATCH_SIZE) {
                    int64_t pr = merge_left_phys(jn);
                    for (int c = 0; c < l_ncols; c++)
                        vec_builder_append_one(&out[c],
                            &jn->merge_l_batch->columns[c], pr);
                    for (int j = 0; j < jn->r_non_key_count; j++)
                        vec_builder_append_na(&out[l_ncols + j]);
                    emitted++;
                    merge_advance_left(jn);
                }
            } else if (jn->kind == JOIN_ANTI) {
                while (!jn->merge_l_done &&
                       emitted < MERGE_JOIN_BATCH_SIZE) {
                    int64_t pr = merge_left_phys(jn);
                    for (int c = 0; c < l_ncols; c++)
                        vec_builder_append_one(&out[c],
                            &jn->merge_l_batch->columns[c], pr);
                    emitted++;
                    merge_advance_left(jn);
                }
            } else {
                /* INNER/SEMI: done */
                jn->merge_l_done = 1;
            }
            break;
        }

        /* Compare current left row to current right row */
        int64_t pr = merge_left_phys(jn);
        int cmp = merge_compare_keys(jn->merge_l_batch->columns, jn->lkey_idx,
                                      jn->r_cols, jn->rkey_idx,
                                      jn->n_keys, pr, jn->merge_r_cursor);

        if (cmp < 0) {
            /* Left < right: no match for this left row */
            if (jn->kind == JOIN_LEFT || jn->kind == JOIN_FULL) {
                for (int c = 0; c < l_ncols; c++)
                    vec_builder_append_one(&out[c],
                        &jn->merge_l_batch->columns[c], pr);
                for (int j = 0; j < jn->r_non_key_count; j++)
                    vec_builder_append_na(&out[l_ncols + j]);
                emitted++;
            } else if (jn->kind == JOIN_ANTI) {
                for (int c = 0; c < l_ncols; c++)
                    vec_builder_append_one(&out[c],
                        &jn->merge_l_batch->columns[c], pr);
                emitted++;
            }
            merge_advance_left(jn);
        } else if (cmp > 0) {
            /* Left > right: advance right */
            if (jn->kind == JOIN_FULL) {
                /* Emit unmatched right row with NA left columns */
                int *l_col_rkey = (int *)malloc((size_t)l_ncols * sizeof(int));
                for (int c = 0; c < l_ncols; c++) {
                    l_col_rkey[c] = -1;
                    for (int k = 0; k < jn->n_keys; k++) {
                        if (jn->lkey_idx[k] == c) {
                            l_col_rkey[c] = jn->rkey_idx[k];
                            break;
                        }
                    }
                }
                for (int c = 0; c < l_ncols; c++) {
                    if (l_col_rkey[c] >= 0)
                        vec_builder_append_one(&out[c],
                            &jn->r_cols[l_col_rkey[c]],
                            jn->merge_r_cursor);
                    else
                        vec_builder_append_na(&out[c]);
                }
                for (int j = 0; j < jn->r_non_key_count; j++)
                    vec_builder_append_one(&out[l_ncols + j],
                        &jn->r_cols[jn->r_non_key_idx[j]],
                        jn->merge_r_cursor);
                free(l_col_rkey);
                emitted++;
            }
            jn->merge_r_cursor++;
        } else {
            /* Keys equal: handle match */
            int64_t grp_start = jn->merge_r_cursor;
            int64_t grp_end = merge_find_group_end(jn, grp_start);
            jn->merge_r_group = grp_start;
            jn->merge_r_group_end = grp_end;

            switch (jn->kind) {
            case JOIN_SEMI:
                for (int c = 0; c < l_ncols; c++)
                    vec_builder_append_one(&out[c],
                        &jn->merge_l_batch->columns[c], pr);
                emitted++;
                merge_advance_left(jn);
                break;

            case JOIN_ANTI:
                /* Skip this left row */
                merge_advance_left(jn);
                break;

            case JOIN_INNER:
            case JOIN_LEFT:
            case JOIN_FULL:
                /* Start cross product: emit left row x each right row in group */
                jn->merge_r_sub = grp_start;
                /* The cross product loop at the top of the while will handle it */
                break;
            }
        }
    }

    /* Build result batch */
    int64_t out_nrows = out[0].length;
    if (out_nrows == 0) {
        for (int c = 0; c < out_ncols; c++)
            vec_builder_free(&out[c]);
        free(out);
        return NULL;
    }

    VecBatch *result = vec_batch_alloc(out_ncols, out_nrows);
    for (int c = 0; c < out_ncols; c++) {
        result->columns[c] = vec_builder_finish(&out[c]);
        const char *nm = jn->base.output_schema.col_names[c];
        size_t nm_len = strlen(nm);
        result->col_names[c] = (char *)malloc(nm_len + 1);
        memcpy(result->col_names[c], nm, nm_len + 1);
    }
    free(out);
    return result;
}

/* ------------------------------------------------------------------ */
/*  Finalize phase: emit unmatched build rows (full_join only)         */
/* ------------------------------------------------------------------ */

/* Maximum rows per finalize batch (keeps memory bounded) */
#define FINALIZE_BATCH_SIZE 65536

static VecBatch *join_finalize(JoinNode *jn) {
    const VecSchema *lschema = &jn->left->output_schema;
    int l_ncols = lschema->n_cols;
    int out_ncols = jn->base.output_schema.n_cols;
    int64_t r_nrows = jn->jht.n_build;

    VecArrayBuilder *out = (VecArrayBuilder *)calloc(
        (size_t)out_ncols, sizeof(VecArrayBuilder));
    for (int c = 0; c < out_ncols; c++)
        out[c] = vec_builder_init(jn->base.output_schema.col_types[c]);

    /* Precompute: for each left output column, which right key column
       provides its value (or -1 if NA). Avoids inner-loop key search. */
    int *l_col_rkey = (int *)malloc((size_t)l_ncols * sizeof(int));
    for (int c = 0; c < l_ncols; c++) {
        l_col_rkey[c] = -1;
        for (int k = 0; k < jn->n_keys; k++) {
            if (jn->lkey_idx[k] == c) {
                l_col_rkey[c] = jn->rkey_idx[k];
                break;
            }
        }
    }

    int64_t emitted = 0;
    int64_t br = jn->finalize_cursor;
    for (; br < r_nrows && emitted < FINALIZE_BATCH_SIZE; br++) {
        if (jn->build_matched[br / 8] & (1 << (br % 8))) continue;
        /* Key cols from build side, non-key left cols as NA */
        for (int c = 0; c < l_ncols; c++) {
            if (l_col_rkey[c] >= 0)
                vec_builder_append_one(&out[c],
                    &jn->r_cols[l_col_rkey[c]], br);
            else
                vec_builder_append_na(&out[c]);
        }
        /* Right non-key cols from build */
        for (int j = 0; j < jn->r_non_key_count; j++)
            vec_builder_append_one(&out[l_ncols + j],
                &jn->r_cols[jn->r_non_key_idx[j]], br);
        emitted++;
    }
    jn->finalize_cursor = br;
    free(l_col_rkey);

    int64_t out_nrows = out[0].length;
    if (out_nrows == 0) {
        for (int c = 0; c < out_ncols; c++)
            vec_builder_free(&out[c]);
        free(out);
        return NULL;
    }

    VecBatch *result = vec_batch_alloc(out_ncols, out_nrows);
    for (int c = 0; c < out_ncols; c++) {
        result->columns[c] = vec_builder_finish(&out[c]);
        const char *nm = jn->base.output_schema.col_names[c];
        size_t nm_len = strlen(nm);
        result->col_names[c] = (char *)malloc(nm_len + 1);
        memcpy(result->col_names[c], nm, nm_len + 1);
    }
    free(out);
    return result;
}

/* ------------------------------------------------------------------ */
/*  State machine: next_batch                                          */
/* ------------------------------------------------------------------ */

/*
 * Streaming hash join with right (build) side materialized once.
 *
 * Lifetime contract:
 *   - Build side (r_cols, jht) is owned by JoinNode, allocated during BUILD
 *     phase, freed in join_free.
 *   - Probe batches are pulled one at a time from left child and consumed
 *     within a single next_batch call.
 *   - Output batches are built via deep-copying (vec_builder_append_one/na).
 *   - For full_join, build_matched bitset persists across probe calls and
 *     is consumed during FINALIZE.
 */
static VecBatch *join_next_batch(VecNode *self) {
    JoinNode *jn = (JoinNode *)self;

    if (jn->state == JSTATE_BUILD) {
        join_build(jn);
        jn->state = jn->use_merge ? JSTATE_MERGE : JSTATE_PROBE;
    }

    /* Merge join path: sorted merge */
    while (jn->state == JSTATE_MERGE) {
        VecBatch *result = merge_join_batch(jn);
        if (result) return result;
        /* merge_join_batch returned NULL: left exhausted or both done */
        jn->state = (jn->kind == JOIN_FULL) ? JSTATE_FINALIZE
                                             : JSTATE_DONE;
    }

    /* Hash join probe phase: pull left batches, skip empty-output batches */
    while (jn->state == JSTATE_PROBE) {
        VecBatch *pbatch = jn->left->next_batch(jn->left);
        if (!pbatch) {
            /* Left child exhausted */
            jn->state = (jn->kind == JOIN_FULL) ? JSTATE_FINALIZE
                                                 : JSTATE_DONE;
            break;
        }
        VecBatch *result = join_probe_one(jn, pbatch);
        vec_batch_free(pbatch);
        if (result) return result;
        /* Empty output for this batch (e.g. anti_join all matched): loop */
    }

    while (jn->state == JSTATE_FINALIZE) {
        VecBatch *result = join_finalize(jn);
        if (result) return result;
        jn->state = JSTATE_DONE;
    }

    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Cleanup                                                            */
/* ------------------------------------------------------------------ */

static void join_free(VecNode *self) {
    JoinNode *jn = (JoinNode *)self;
    jn->left->free_node(jn->left);
    jn->right->free_node(jn->right);
    free(jn->keys);
    free(jn->suffix_x);
    free(jn->suffix_y);
    free(jn->lkey_idx);
    free(jn->rkey_idx);
    free(jn->r_non_key_idx);
    free(jn->l_non_key_idx);
    free(jn->build_matched);
    if (jn->r_cols) {
        for (int c = 0; c < jn->r_ncols; c++)
            vec_array_free(&jn->r_cols[c]);
        free(jn->r_cols);
    }
    if (jn->jht.head) jht_free(&jn->jht);
    if (jn->merge_l_batch) vec_batch_free(jn->merge_l_batch);
    vec_schema_free(&jn->base.output_schema);
    free(jn);
}

/* ------------------------------------------------------------------ */
/*  Constructor                                                        */
/* ------------------------------------------------------------------ */

JoinNode *join_node_create(VecNode *left, VecNode *right,
                           JoinKind kind, int n_keys, JoinKey *keys,
                           const char *suffix_x, const char *suffix_y) {
    JoinNode *jn = (JoinNode *)calloc(1, sizeof(JoinNode));
    if (!jn) vectra_error("alloc failed for JoinNode");
    jn->left = left;
    jn->right = right;
    jn->kind = kind;
    jn->n_keys = n_keys;
    jn->keys = keys;
    size_t sx_len = strlen(suffix_x);
    jn->suffix_x = (char *)malloc(sx_len + 1);
    memcpy(jn->suffix_x, suffix_x, sx_len + 1);
    size_t sy_len = strlen(suffix_y);
    jn->suffix_y = (char *)malloc(sy_len + 1);
    memcpy(jn->suffix_y, suffix_y, sy_len + 1);
    jn->state = JSTATE_BUILD;

    const VecSchema *ls = &left->output_schema;
    const VecSchema *rs = &right->output_schema;

    /* Verify key types are compatible (allow numeric coercion) */
    static const char *kind_names[] = {
        "inner_join", "left_join", "full_join", "semi_join", "anti_join"
    };
    for (int k = 0; k < n_keys; k++) {
        VecType lt = ls->col_types[keys[k].left_col];
        VecType rt = rs->col_types[keys[k].right_col];
        if (lt != rt) {
            /* String vs non-string is an error */
            if (lt == VEC_STRING || rt == VEC_STRING)
                vectra_error("%s key type mismatch: x.%s (%s) vs y.%s (%s)",
                             kind_names[kind],
                             ls->col_names[keys[k].left_col],
                             vec_type_name(lt),
                             rs->col_names[keys[k].right_col],
                             vec_type_name(rt));
            /* Numeric types (bool/int64/double) are compatible —
               coercion happens in join_build */
        }
    }

    /* Precompute key index arrays */
    jn->lkey_idx = (int *)malloc((size_t)n_keys * sizeof(int));
    jn->rkey_idx = (int *)malloc((size_t)n_keys * sizeof(int));
    for (int k = 0; k < n_keys; k++) {
        jn->lkey_idx[k] = keys[k].left_col;
        jn->rkey_idx[k] = keys[k].right_col;
    }

    /* Precompute non-key column indices */
    if (kind == JOIN_INNER || kind == JOIN_LEFT || kind == JOIN_FULL) {
        int *r_is_key = (int *)calloc((size_t)rs->n_cols, sizeof(int));
        for (int k = 0; k < n_keys; k++)
            r_is_key[keys[k].right_col] = 1;
        jn->r_non_key_idx = (int *)malloc((size_t)rs->n_cols * sizeof(int));
        jn->r_non_key_count = 0;
        for (int c = 0; c < rs->n_cols; c++)
            if (!r_is_key[c])
                jn->r_non_key_idx[jn->r_non_key_count++] = c;
        free(r_is_key);
    }
    if (kind == JOIN_FULL) {
        int *l_is_key = (int *)calloc((size_t)ls->n_cols, sizeof(int));
        for (int k = 0; k < n_keys; k++)
            l_is_key[keys[k].left_col] = 1;
        jn->l_non_key_idx = (int *)malloc((size_t)ls->n_cols * sizeof(int));
        jn->l_non_key_count = 0;
        for (int c = 0; c < ls->n_cols; c++)
            if (!l_is_key[c])
                jn->l_non_key_idx[jn->l_non_key_count++] = c;
        free(l_is_key);
    }

    /* Build output schema (unchanged from before) */
    int out_n;
    if (kind == JOIN_SEMI || kind == JOIN_ANTI) {
        out_n = ls->n_cols;
        char **names = (char **)malloc((size_t)out_n * sizeof(char *));
        VecType *types = (VecType *)malloc((size_t)out_n * sizeof(VecType));
        for (int i = 0; i < out_n; i++) {
            names[i] = ls->col_names[i];
            types[i] = ls->col_types[i];
        }
        jn->base.output_schema = vec_schema_create(out_n, names, types);
        free(names);
        free(types);
    } else {
        int *r_is_key = (int *)calloc((size_t)rs->n_cols, sizeof(int));
        for (int k = 0; k < n_keys; k++)
            r_is_key[keys[k].right_col] = 1;

        int r_extra = 0;
        for (int c = 0; c < rs->n_cols; c++)
            if (!r_is_key[c]) r_extra++;

        out_n = ls->n_cols + r_extra;
        char **names = (char **)malloc((size_t)out_n * sizeof(char *));
        VecType *types = (VecType *)malloc((size_t)out_n * sizeof(VecType));

        for (int i = 0; i < ls->n_cols; i++) {
            names[i] = ls->col_names[i];
            types[i] = ls->col_types[i];
        }
        int idx = ls->n_cols;
        for (int c = 0; c < rs->n_cols; c++) {
            if (r_is_key[c]) continue;
            int collision = 0;
            for (int li = 0; li < ls->n_cols; li++) {
                if (strcmp(ls->col_names[li], rs->col_names[c]) == 0) {
                    collision = 1;
                    break;
                }
            }
            if (collision) {
                size_t len = strlen(rs->col_names[c]) + strlen(suffix_y) + 1;
                char *suffixed = (char *)malloc(len);
                snprintf(suffixed, len, "%s%s", rs->col_names[c], suffix_y);
                names[idx] = suffixed;
                for (int li = 0; li < ls->n_cols; li++) {
                    if (strcmp(ls->col_names[li], rs->col_names[c]) == 0) {
                        size_t llen = strlen(ls->col_names[li]) +
                                      strlen(suffix_x) + 1;
                        char *lsuf = (char *)malloc(llen);
                        snprintf(lsuf, llen, "%s%s", ls->col_names[li],
                                 suffix_x);
                        names[li] = lsuf;
                        break;
                    }
                }
            } else {
                names[idx] = rs->col_names[c];
            }
            types[idx] = rs->col_types[c];
            idx++;
        }

        jn->base.output_schema = vec_schema_create(out_n, names, types);

        for (int i = 0; i < ls->n_cols; i++) {
            if (names[i] != ls->col_names[i]) free(names[i]);
        }
        for (int i = ls->n_cols; i < out_n; i++) {
            int c_idx = 0, j = 0;
            for (int c = 0; c < rs->n_cols; c++) {
                if (r_is_key[c]) continue;
                if (j == i - ls->n_cols) { c_idx = c; break; }
                j++;
            }
            if (names[i] != rs->col_names[c_idx]) free(names[i]);
        }

        free(names);
        free(types);
        free(r_is_key);
    }

    jn->base.next_batch = join_next_batch;
    jn->base.kind = "JoinNode";
    jn->base.free_node = join_free;

    return jn;
}
