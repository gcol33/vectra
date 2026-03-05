#include "join.h"
#include "hash.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "builder.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

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
            assert(pa->buf.str.data != NULL && "join: probe string data is NULL");
            assert(ba->buf.str.data != NULL && "join: build string data is NULL");
            if (memcmp(pa->buf.str.data + ps, ba->buf.str.data + bs,
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

    /* Build hash table */
    jn->jht = jht_create(r_nrows > 0 ? r_nrows : 1);
    for (int64_t r = 0; r < r_nrows; r++) {
        uint64_t h = hash_join_key(jn->r_cols, jn->rkey_idx, jn->n_keys, r);
        jht_insert(&jn->jht, h, r);
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

static inline uint64_t hash_str(const char *data, int64_t off, int64_t end) {
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

    /* Fast path: 1-key with specialized hash to avoid per-row dispatch */
    if (jn->n_keys == 1) {
        const VecArray *pkey = &pbatch->columns[jn->lkey_idx[0]];
        switch (pkey->type) {
        case VEC_INT64:
            for (int64_t li = 0; li < p_logical; li++) {
                int64_t pi = vec_batch_physical_row(pbatch, li);
                phash[li] = vec_array_is_valid(pkey, pi)
                    ? hash_i64(pkey->buf.i64[pi])
                    : (FNV_OFFSET ^ 0xFF);
            }
            break;
        case VEC_DOUBLE:
            for (int64_t li = 0; li < p_logical; li++) {
                int64_t pi = vec_batch_physical_row(pbatch, li);
                phash[li] = vec_array_is_valid(pkey, pi)
                    ? hash_dbl(pkey->buf.dbl[pi])
                    : (FNV_OFFSET ^ 0xFF);
            }
            break;
        case VEC_STRING:
            for (int64_t li = 0; li < p_logical; li++) {
                int64_t pi = vec_batch_physical_row(pbatch, li);
                phash[li] = vec_array_is_valid(pkey, pi)
                    ? hash_str(pkey->buf.str.data,
                               pkey->buf.str.offsets[pi],
                               pkey->buf.str.offsets[pi + 1])
                    : (FNV_OFFSET ^ 0xFF);
            }
            break;
        default:
            for (int64_t li = 0; li < p_logical; li++) {
                int64_t pi = vec_batch_physical_row(pbatch, li);
                phash[li] = hash_join_key(pbatch->columns, jn->lkey_idx,
                                           jn->n_keys, pi);
            }
            break;
        }
    } else {
        /* Generic composite key hash */
        for (int64_t li = 0; li < p_logical; li++) {
            int64_t pi = vec_batch_physical_row(pbatch, li);
            phash[li] = hash_join_key(pbatch->columns, jn->lkey_idx,
                                       jn->n_keys, pi);
        }
    }

    /* Probe each logical row using pre-computed hashes */
    for (int64_t li = 0; li < p_logical; li++) {
        int64_t pr = vec_batch_physical_row(pbatch, li);
        int64_t br = jht_probe(&jn->jht, phash[li],
                                pbatch->columns, jn->lkey_idx,
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
                    pbatch->columns, jn->lkey_idx,
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
        result->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(result->col_names[c], nm);
    }
    free(out);
    return result;
}

/* ------------------------------------------------------------------ */
/*  Finalize phase: emit unmatched build rows (full_join only)         */
/* ------------------------------------------------------------------ */

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

    for (int64_t br = jn->finalize_cursor; br < r_nrows; br++) {
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
    }
    jn->finalize_cursor = r_nrows;
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
        result->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(result->col_names[c], nm);
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
        jn->state = JSTATE_PROBE;
    }

    /* Probe phase: pull left batches, skip empty-output batches */
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

    if (jn->state == JSTATE_FINALIZE) {
        VecBatch *result = join_finalize(jn);
        jn->state = JSTATE_DONE;
        return result;
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
    jn->suffix_x = (char *)malloc(strlen(suffix_x) + 1);
    strcpy(jn->suffix_x, suffix_x);
    jn->suffix_y = (char *)malloc(strlen(suffix_y) + 1);
    strcpy(jn->suffix_y, suffix_y);
    jn->state = JSTATE_BUILD;

    const VecSchema *ls = &left->output_schema;
    const VecSchema *rs = &right->output_schema;

    /* Verify key types match (no implicit coercion) */
    static const char *kind_names[] = {
        "inner_join", "left_join", "full_join", "semi_join", "anti_join"
    };
    for (int k = 0; k < n_keys; k++) {
        VecType lt = ls->col_types[keys[k].left_col];
        VecType rt = rs->col_types[keys[k].right_col];
        if (lt != rt)
            vectra_error("%s key type mismatch: x.%s (%s) vs y.%s (%s)",
                         kind_names[kind],
                         ls->col_names[keys[k].left_col],
                         vec_type_name(lt),
                         rs->col_names[keys[k].right_col],
                         vec_type_name(rt));
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
