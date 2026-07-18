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
#include "vtr1_tdc.h"
#include "vtr_codec.h"
#include "error.h"
#include <math.h>
#include <stdio.h>
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
    /* n_build must be the true build-row count (0 for an empty build side, e.g.
       a full-join partition whose keys appear only on the probe side). The chain
       array needs at least one slot to avoid malloc(0); n_build stays accurate
       so join_finalize/merge_join do not walk a phantom row into an empty
       column. */
    int64_t next_len = n_build_rows > 0 ? n_build_rows : 1;
    jht.n_slots = n_slots;
    jht.head = (int64_t *)malloc((size_t)n_slots * sizeof(int64_t));
    jht.slot_hash = (uint64_t *)malloc((size_t)n_slots * sizeof(uint64_t));
    jht.build_next = (int64_t *)malloc((size_t)next_len * sizeof(int64_t));
    jht.n_build = n_build_rows;
    if (!jht.head || !jht.slot_hash || !jht.build_next)
        vectra_error("alloc failed for join hash table");
    memset(jht.head, -1, (size_t)n_slots * sizeof(int64_t));
    memset(jht.build_next, -1, (size_t)next_len * sizeof(int64_t));
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
                           int n_keys, int64_t probe_row, int64_t build_row,
                           int na_matches) {
    for (int k = 0; k < n_keys; k++) {
        const VecArray *pa = &probe_cols[probe_key_idx[k]];
        const VecArray *ba = &build_cols[build_key_idx[k]];
        int pv = vec_array_is_valid(pa, probe_row);
        int bv = vec_array_is_valid(ba, build_row);
        if (!pv || !bv) {
            /* na_matches: NA matches NA (dplyr default); otherwise NA never
               matches (SQL). One side NA and the other not is never a match. */
            if (na_matches && !pv && !bv) continue;
            return 0;
        }
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
        case VEC_DOUBLE: {
            double pd = pa->buf.dbl[probe_row], bd = ba->buf.dbl[build_row];
            /* NaN keys match each other (as in group_by/distinct and the merge
               path); plain != would reject NaN==NaN and miss the join. */
            if (pd != bd && !(pd != pd && bd != bd)) return 0;
            break;
        }
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
                          int n_keys, int64_t probe_row, int na_matches) {
    int64_t mask = jht->n_slots - 1;
    int64_t slot = (int64_t)(hash & (uint64_t)mask);
    for (;;) {
        if (jht->head[slot] == -1) return -1;
        if (jht->slot_hash[slot] == hash) {
            int64_t br = jht->head[slot];
            while (br >= 0) {
                if (join_keys_equal(probe_cols, probe_key_idx,
                                    build_cols, build_key_idx,
                                    n_keys, probe_row, br, na_matches))
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
                               int n_keys, int64_t probe_row, int na_matches) {
    int64_t br = jht->build_next[build_row];
    while (br >= 0) {
        if (join_keys_equal(probe_cols, probe_key_idx,
                            build_cols, build_key_idx,
                            n_keys, probe_row, br, na_matches))
            return br;
        br = jht->build_next[br];
    }
    return -1;
}

/* ------------------------------------------------------------------ */
/*  Shared row emitters (one source of truth for probe + BNL paths)    */
/* ------------------------------------------------------------------ */

/* Emit left row `lr` (from lcols) x build row `br` (from r_cols). */
static inline void join_emit_matched(JoinNode *jn, VecArrayBuilder *out,
                                     const VecArray *lcols, int l_ncols,
                                     int64_t lr, int64_t br) {
    for (int c = 0; c < l_ncols; c++)
        vec_builder_append_one(&out[c], &lcols[c], lr);
    for (int j = 0; j < jn->r_non_key_count; j++)
        vec_builder_append_one(&out[l_ncols + j],
            &jn->r_cols[jn->r_non_key_idx[j]], br);
}

/* Emit left row `lr` with NA in every right non-key column (unmatched left). */
static inline void join_emit_left_only(JoinNode *jn, VecArrayBuilder *out,
                                       const VecArray *lcols, int l_ncols,
                                       int64_t lr) {
    for (int c = 0; c < l_ncols; c++)
        vec_builder_append_one(&out[c], &lcols[c], lr);
    for (int j = 0; j < jn->r_non_key_count; j++)
        vec_builder_append_na(&out[l_ncols + j]);
}

/* Emit unmatched build row `br` (full join): key columns come from the build
   side where a left column is a join key, NA elsewhere; right non-key columns
   from the build side. `l_col_rkey` maps each left column to its build-key
   column index, or -1. `rcols` is the build-column array to read from. */
static inline void join_emit_right_only(JoinNode *jn, VecArrayBuilder *out,
                                        const VecArray *rcols, int l_ncols,
                                        const int *l_col_rkey, int64_t br) {
    for (int c = 0; c < l_ncols; c++) {
        if (l_col_rkey[c] >= 0)
            vec_builder_append_one(&out[c], &rcols[l_col_rkey[c]], br);
        else
            vec_builder_append_na(&out[c]);
    }
    for (int j = 0; j < jn->r_non_key_count; j++)
        vec_builder_append_one(&out[l_ncols + j],
            &rcols[jn->r_non_key_idx[j]], br);
}

/* Map each left output column to the build-key column supplying its value in a
   full-join unmatched-build row (-1 = fill NA). Caller frees. */
static int *join_build_l_col_rkey(JoinNode *jn, int l_ncols) {
    int *m = (int *)malloc((size_t)l_ncols * sizeof(int));
    for (int c = 0; c < l_ncols; c++) {
        m[c] = -1;
        for (int k = 0; k < jn->n_keys; k++)
            if (jn->lkey_idx[k] == c) { m[c] = jn->rkey_idx[k]; break; }
    }
    return m;
}

/* Coerce probe key columns to the build key types (jht_probe compares same
   type). Fills coerced[k] (owned, NULL when no coercion) and returns the column
   array to hash/compare against: pbatch->columns when nothing needed coercion,
   else a fresh hash_cols (returned via *hash_cols_out) that the caller frees
   with join_free_probe_keys. Shared by the resident probe and the BNL probe. */
static VecArray *join_coerce_probe_keys(JoinNode *jn, VecBatch *pbatch,
                                        VecArray **coerced,
                                        VecArray **hash_cols_out) {
    int l_ncols = jn->left->output_schema.n_cols;
    int need = 0;
    for (int k = 0; k < jn->n_keys && k < 16; k++) {
        VecType pt = pbatch->columns[jn->lkey_idx[k]].type;
        VecType bt = jn->r_cols[jn->rkey_idx[k]].type;
        if (pt != bt) {
            coerced[k] = vec_coerce(&pbatch->columns[jn->lkey_idx[k]], bt);
            need = 1;
        } else {
            coerced[k] = NULL;
        }
    }
    VecArray *hash_cols = NULL;
    if (need) {
        hash_cols = (VecArray *)malloc((size_t)l_ncols * sizeof(VecArray));
        memcpy(hash_cols, pbatch->columns, (size_t)l_ncols * sizeof(VecArray));
        for (int k = 0; k < jn->n_keys && k < 16; k++)
            if (coerced[k]) hash_cols[jn->lkey_idx[k]] = *coerced[k];
    }
    *hash_cols_out = hash_cols;
    return need ? hash_cols : pbatch->columns;
}

static void join_free_probe_keys(JoinNode *jn, VecArray **coerced,
                                  VecArray *hash_cols) {
    for (int k = 0; k < jn->n_keys && k < 16; k++)
        if (coerced[k]) { vec_array_free(coerced[k]); free(coerced[k]); }
    free(hash_cols); /* NULL-safe */
}

/* Finish output builders into a VecBatch (NULL when empty), naming columns from
   the join's output schema. Shared by the probe, merge, finalize, BNL paths. */
static VecBatch *join_finish_out(JoinNode *jn, VecArrayBuilder *out,
                                 int out_ncols) {
    int64_t n = out[0].length;
    if (n == 0) {
        for (int c = 0; c < out_ncols; c++) vec_builder_free(&out[c]);
        free(out);
        return NULL;
    }
    VecBatch *result = vec_batch_alloc(out_ncols, n);
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

/* Allocate out_ncols output builders, each reserved for `reserve` rows. */
static VecArrayBuilder *join_alloc_out(JoinNode *jn, int out_ncols,
                                       int64_t reserve) {
    VecArrayBuilder *out = (VecArrayBuilder *)calloc(
        (size_t)out_ncols, sizeof(VecArrayBuilder));
    for (int c = 0; c < out_ncols; c++) {
        out[c] = vec_builder_init(jn->base.output_schema.col_types[c]);
        if (reserve > 0) vec_builder_reserve(&out[c], reserve);
    }
    return out;
}

/* Coerce build-side key columns in jn->r_cols to the common (probe) key type,
   so probe and build compare at the same type. join_coerce_probe_keys then
   coerces the probe keys to the same type. Shared by the resident build and the
   per-block BNL build; keeping them identical avoids a type-mismatch where one
   path narrows the probe value and the other narrows the build value. */
static void join_coerce_build_keys(JoinNode *jn) {
    const VecSchema *lschema = &jn->left->output_schema;
    for (int k = 0; k < jn->n_keys; k++) {
        VecType lt = lschema->col_types[jn->lkey_idx[k]];
        VecType rt = jn->r_cols[jn->rkey_idx[k]].type;
        if (lt == rt) continue;
        VecType common = vec_common_type(lt, rt);
        if (jn->r_cols[jn->rkey_idx[k]].type != common) {
            VecArray *coerced = vec_coerce(&jn->r_cols[jn->rkey_idx[k]], common);
            vec_array_free(&jn->r_cols[jn->rkey_idx[k]]);
            jn->r_cols[jn->rkey_idx[k]] = *coerced;
            free(coerced);
        }
    }
}

/* Build jn->jht over the current jn->r_cols (r_nrows rows). Parallel hashing,
   serial insert. Shared by the resident build and the per-block BNL build. */
static void jht_build_from_rcols(JoinNode *jn, int64_t r_nrows) {
    jn->jht = jht_create(r_nrows);   /* n_build == r_nrows, 0 for an empty build */
    uint64_t *build_hashes = (uint64_t *)malloc(
        (size_t)(r_nrows > 0 ? r_nrows : 1) * sizeof(uint64_t));
    if (!build_hashes) vectra_error("alloc failed for build hash array");
    #pragma omp parallel for if(r_nrows > VEC_OMP_THRESHOLD) schedule(static)
    for (int64_t r = 0; r < r_nrows; r++)
        build_hashes[r] = hash_join_key(jn->r_cols, jn->rkey_idx,
                                         jn->n_keys, r);
    for (int64_t r = 0; r < r_nrows; r++)
        jht_insert(&jn->jht, build_hashes[r], r);
    free(build_hashes);
}

/* ------------------------------------------------------------------ */
/*  Build phase: materialize right side into hash table                */
/* ------------------------------------------------------------------ */

/* ------------------------------------------------------------------ */
/*  Grace-hash spill: partition both sides, join one partition at a time */
/* ------------------------------------------------------------------ */

#define JOIN_SPILL_PARTS 64

/* Deepest re-partitioning level before a still-oversized partition (a single
   hot key) drops to the block-nested-loop fallback. 64^3 effective ways is far
   more than any multi-key skew needs, so BNL fires only for a true hot key. */
#define JOIN_MAX_SPILL_DEPTH 3

/* Depth salt for the partition hash. A constant XOR before `% K` is only a
   fixed bucket permutation (keys that collide stay collided), so mix the salt
   in multiplicatively (murmur3 fmix): a different depth reshuffles which keys
   share a partition, so a multi-key oversized partition actually splits when
   its sub-join re-partitions. salt 0 (the top level) is the identity. */
static inline uint64_t join_salt_mix(uint64_t h, uint64_t salt) {
    if (salt == 0) return h;
    h ^= salt * 0x9E3779B97F4A7C15ULL;
    h ^= h >> 33; h *= 0xFF51AFD7ED558CCDULL;
    h ^= h >> 33; h *= 0xC4CEB9FE1A85EC53ULL;
    h ^= h >> 33;
    return h;
}

/* Rough in-memory byte size of a batch's logical rows: string payload plus a
   flat 8 bytes/value. Only a trigger heuristic, so exactness is not needed. */
static int64_t join_batch_bytes(const VecBatch *b, int n_cols) {
    int64_t n = vec_batch_logical_rows((VecBatch *)b);
    int64_t bytes = 0;
    for (int c = 0; c < n_cols; c++) {
        const VecArray *a = &b->columns[c];
        if (a->type == VEC_STRING && a->length > 0)
            bytes += a->buf.str.offsets[a->length];
        bytes += n * 8;
    }
    return bytes;
}

static char *make_spill_path(const char *temp_dir, char side, int p) {
    static int join_counter = 0;
    int id = join_counter++;
    int len = snprintf(NULL, 0, "%s/vectra_join_%d_%c%d.vtr",
                       temp_dir, id, side, p);
    char *path = (char *)malloc((size_t)(len + 1));
    snprintf(path, (size_t)(len + 1), "%s/vectra_join_%d_%c%d.vtr",
             temp_dir, id, side, p);
    return path;
}

/* Route a batch into K partition writers by the hash of the key columns coerced
   to `common` types, so equal keys on both sides land in the same partition (the
   sub-join re-coerces from the original stored rows). `writers` may hold NULL
   slots: a slot is opened from paths[p] on first use, so a partition that never
   receives a row creates no file (a hot key leaves 63 of 64 empty each level).
   paths may be NULL only when every writer is pre-opened (K=1 BNL). */
static void spill_write_batch(VecBatch *batch, const VecSchema *schema,
                              const int *key_col, const VecType *common,
                              int n_keys, Vtr1TdcWriter **writers,
                              char **paths, int K, uint64_t salt) {
    int n_cols = schema->n_cols;
    int64_t n = vec_batch_logical_rows(batch);
    if (n == 0) return;

    VecArray ckeys[16];
    int       need_free[16];
    int       key_id[16];
    for (int k = 0; k < n_keys; k++) {
        key_id[k] = k;
        VecArray *src = &batch->columns[key_col[k]];
        if (src->type != common[k]) {
            VecArray *co = vec_coerce(src, common[k]);
            ckeys[k] = *co; free(co); need_free[k] = 1;
        } else { ckeys[k] = *src; need_free[k] = 0; }
    }

    int *pid = (int *)malloc((size_t)n * sizeof(int));
    for (int64_t li = 0; li < n; li++) {
        int64_t pr = vec_batch_physical_row(batch, li);
        uint64_t h = join_salt_mix(hash_join_key(ckeys, key_id, n_keys, pr),
                                   salt);
        pid[li] = (int)(h % (uint64_t)K);
    }

    for (int p = 0; p < K; p++) {
        int64_t m = 0;
        for (int64_t li = 0; li < n; li++) if (pid[li] == p) m++;
        if (m == 0) continue;
        VecArrayBuilder *bld = (VecArrayBuilder *)calloc(
            (size_t)n_cols, sizeof(VecArrayBuilder));
        for (int c = 0; c < n_cols; c++) {
            bld[c] = vec_builder_init(schema->col_types[c]);
            vec_builder_reserve(&bld[c], m);
        }
        for (int64_t li = 0; li < n; li++) {
            if (pid[li] != p) continue;
            int64_t pr = vec_batch_physical_row(batch, li);
            for (int c = 0; c < n_cols; c++)
                vec_builder_append_one(&bld[c], &batch->columns[c], pr);
        }
        VecBatch *ob = vec_batch_alloc(n_cols, m);
        for (int c = 0; c < n_cols; c++) {
            ob->columns[c] = vec_builder_finish(&bld[c]);
            ob->col_names[c] = (char *)malloc(strlen(schema->col_names[c]) + 1);
            strcpy(ob->col_names[c], schema->col_names[c]);
        }
        free(bld);
        if (writers[p] == NULL)
            writers[p] = vtr1_open_tdc_writer(paths[p], schema);
        vtr1_write_rowgroup_tdc(writers[p], ob, VTR_COMPRESS_FAST, NULL, NULL);
        vec_batch_free(ob);
    }
    free(pid);
    for (int k = 0; k < n_keys; k++)
        if (need_free[k]) vec_array_free(&ckeys[k]);
}

/* Switch to spill mode: partition the already-materialized build rows plus the
   rest of the build stream, and the whole probe stream, into K run-files per
   side. Consumes r_builders (finishes them). */
static void join_spill(JoinNode *jn, VecArrayBuilder *r_builders) {
    int K = JOIN_SPILL_PARTS;
    /* Reshuffle the partition assignment per recursion level so a partition a
       sub-join re-spills does not route every key back to one child. */
    uint64_t salt = (uint64_t)jn->spill_depth;
    const VecSchema *rs = &jn->right->output_schema;
    const VecSchema *ls = &jn->left->output_schema;
    int r_ncols = rs->n_cols;

    VecType common[16];
    int rkey[16], lkey[16];
    for (int k = 0; k < jn->n_keys; k++) {
        VecType lt = ls->col_types[jn->keys[k].left_col];
        VecType rt = rs->col_types[jn->keys[k].right_col];
        common[k] = (lt == rt) ? lt : vec_common_type(lt, rt);
        rkey[k] = jn->keys[k].right_col;
        lkey[k] = jn->keys[k].left_col;
    }

    jn->n_parts = K;
    jn->right_parts = (char **)calloc((size_t)K, sizeof(char *));
    jn->left_parts  = (char **)calloc((size_t)K, sizeof(char *));
    Vtr1TdcWriter **rw = (Vtr1TdcWriter **)calloc((size_t)K, sizeof(Vtr1TdcWriter *));
    Vtr1TdcWriter **lw = (Vtr1TdcWriter **)calloc((size_t)K, sizeof(Vtr1TdcWriter *));
    for (int p = 0; p < K; p++) {
        jn->right_parts[p] = make_spill_path(jn->temp_dir, 'r', p);
        jn->left_parts[p]  = make_spill_path(jn->temp_dir, 'l', p);
    }

    /* Route the already-materialized build partial. */
    int64_t nrp = r_builders[0].length;
    if (nrp > 0) {
        VecBatch *pb = vec_batch_alloc(r_ncols, nrp);
        for (int c = 0; c < r_ncols; c++) {
            pb->columns[c] = vec_builder_finish(&r_builders[c]);
            pb->col_names[c] = (char *)malloc(strlen(rs->col_names[c]) + 1);
            strcpy(pb->col_names[c], rs->col_names[c]);
        }
        spill_write_batch(pb, rs, rkey, common, jn->n_keys, rw,
                          jn->right_parts, K, salt);
        vec_batch_free(pb);
    } else {
        for (int c = 0; c < r_ncols; c++) {
            VecArray a = vec_builder_finish(&r_builders[c]);
            vec_array_free(&a);
        }
    }

    /* Route the rest of the build stream, then the whole probe stream. */
    VecBatch *b;
    while ((b = jn->right->next_batch(jn->right)) != NULL) {
        spill_write_batch(b, rs, rkey, common, jn->n_keys, rw,
                          jn->right_parts, K, salt);
        vec_batch_free(b);
    }
    while ((b = jn->left->next_batch(jn->left)) != NULL) {
        spill_write_batch(b, ls, lkey, common, jn->n_keys, lw,
                          jn->left_parts, K, salt);
        vec_batch_free(b);
    }

    /* Close opened writers. A partition empty on BOTH sides is dropped (no files
       created); one empty on a single side still needs a valid empty run-file so
       the sub-join's scan can open it (unmatched rows there still matter for
       left/right/full). */
    for (int p = 0; p < K; p++) {
        int ro = (rw[p] != NULL), lo = (lw[p] != NULL);
        if (!ro && !lo) {
            free(jn->right_parts[p]); jn->right_parts[p] = NULL;
            free(jn->left_parts[p]);  jn->left_parts[p]  = NULL;
            continue;
        }
        if (!ro) rw[p] = vtr1_open_tdc_writer(jn->right_parts[p], rs);
        if (!lo) lw[p] = vtr1_open_tdc_writer(jn->left_parts[p], ls);
        vtr1_close_tdc_writer(rw[p]);
        vtr1_close_tdc_writer(lw[p]);
    }
    free(rw); free(lw);

    jn->spill = 1;
    jn->cur_part = 0;
    jn->sub_join = NULL;
}

/* Sub-join over one partition's spilled left/right files. It carries the same
   memory budget and a deeper spill level, so a partition still over budget
   re-partitions itself (salted by depth) or, at JOIN_MAX_SPILL_DEPTH, drops to
   the block-nested-loop fallback -- keeping every partition bounded. */
static VecNode *make_partition_join(JoinNode *jn, int p) {
    ScanNode *lsc = scan_node_create(jn->left_parts[p], NULL, 0);
    ScanNode *rsc = scan_node_create(jn->right_parts[p], NULL, 0);
    JoinKey *keys = (JoinKey *)malloc((size_t)jn->n_keys * sizeof(JoinKey));
    memcpy(keys, jn->keys, (size_t)jn->n_keys * sizeof(JoinKey));
    JoinNode *sj = join_node_create((VecNode *)lsc, (VecNode *)rsc, jn->kind,
                                    jn->n_keys, keys, jn->suffix_x, jn->suffix_y,
                                    jn->mem_budget, jn->temp_dir);
    sj->na_matches = jn->na_matches;   /* inherit NA-matching in re-partition */
    sj->spill_depth = jn->spill_depth + 1;
    return (VecNode *)sj;
}

/* ------------------------------------------------------------------ */
/*  Block-nested-loop terminal fallback (single hot key)               */
/* ------------------------------------------------------------------ */

/* A partition that survives JOIN_MAX_SPILL_DEPTH re-partitions is dominated by
   one key value that hashing cannot split. Rather than materialize it resident,
   consolidate each side to a single run-file and block-nested-loop: read the
   build file in <= mem_budget blocks; for each block, re-scan the whole probe
   file and emit matches. Peak = one build block + one probe batch + 1-bit/row
   matched bitsets, independent of key skew. Non-inner kinds defer unmatched /
   matched emission to a final scan driven by the bitsets. */

static char *bnl_make_path(const char *temp_dir, char side) {
    static int bnl_counter = 0;
    int id = bnl_counter++;
    int len = snprintf(NULL, 0, "%s/vectra_bnl_%d_%c.vtr", temp_dir, id, side);
    char *path = (char *)malloc((size_t)(len + 1));
    snprintf(path, (size_t)(len + 1), "%s/vectra_bnl_%d_%c.vtr",
             temp_dir, id, side);
    return path;
}

/* Stream `partial` (may be NULL) then all of `child` into one run-file,
   compacting selection vectors via the tested spill_write_batch (K=1). Returns
   the logical row count written. */
static int64_t bnl_consolidate(JoinNode *jn, VecNode *child,
                               const VecSchema *schema, const int *key_col,
                               VecBatch *partial, const char *path) {
    Vtr1TdcWriter *w = vtr1_open_tdc_writer(path, schema);
    Vtr1TdcWriter *ws[1] = { w };
    VecType common[16];
    for (int k = 0; k < jn->n_keys; k++)
        common[k] = schema->col_types[key_col[k]];  /* K=1: identity, no coerce */
    int64_t rows = 0;
    if (partial) {
        rows += vec_batch_logical_rows(partial);
        spill_write_batch(partial, schema, key_col, common, jn->n_keys, ws,
                          NULL, 1, 0);
    }
    VecBatch *b;
    while ((b = child->next_batch(child)) != NULL) {
        rows += vec_batch_logical_rows(b);
        spill_write_batch(b, schema, key_col, common, jn->n_keys, ws, NULL, 1, 0);
        vec_batch_free(b);
    }
    vtr1_close_tdc_writer(w);
    return rows;
}

/* Consumes r_builders. Consolidates both sides and arms BNL mode. */
static void join_spill_bnl(JoinNode *jn, VecArrayBuilder *r_builders) {
    const VecSchema *rs = &jn->right->output_schema;
    const VecSchema *ls = &jn->left->output_schema;
    int r_ncols = rs->n_cols;

    /* Materialized build partial -> a batch (no selection vector). */
    VecBatch *pb = vec_batch_alloc(r_ncols, r_builders[0].length);
    for (int c = 0; c < r_ncols; c++) {
        pb->columns[c] = vec_builder_finish(&r_builders[c]);
        pb->col_names[c] = (char *)malloc(strlen(rs->col_names[c]) + 1);
        strcpy(pb->col_names[c], rs->col_names[c]);
    }

    jn->bnl_rpath = bnl_make_path(jn->temp_dir, 'r');
    jn->bnl_lpath = bnl_make_path(jn->temp_dir, 'l');
    jn->bnl_rrows = bnl_consolidate(jn, jn->right, rs, jn->rkey_idx,
                                    pb, jn->bnl_rpath);
    vec_batch_free(pb);
    jn->bnl_lrows = bnl_consolidate(jn, jn->left, ls, jn->lkey_idx,
                                    NULL, jn->bnl_lpath);

    if (jn->kind != JOIN_INNER) {
        int64_t nb = (jn->bnl_lrows + 7) / 8;
        jn->bnl_pmatched = (uint8_t *)calloc(nb > 0 ? (size_t)nb : 1, 1);
    }
    if (jn->kind == JOIN_FULL) {
        int64_t nb = (jn->bnl_rrows + 7) / 8;
        jn->bnl_bmatched = (uint8_t *)calloc(nb > 0 ? (size_t)nb : 1, 1);
    }
    jn->bnl = 1;
    jn->bnl_stage = 0;
    jn->bnl_block_base = 0;
    jn->bnl_pbase = 0;
    jn->bnl_rscan = NULL;
    jn->bnl_pscan = NULL;
}

/* Free the current build block (r_cols + its hash table). */
static void bnl_free_block(JoinNode *jn) {
    if (jn->r_cols) {
        for (int c = 0; c < jn->r_ncols; c++)
            vec_array_free(&jn->r_cols[c]);
        free(jn->r_cols);
        jn->r_cols = NULL;
    }
    if (jn->jht.head) jht_free(&jn->jht);
}

/* Load up to mem_budget bytes of the next build block into jn->r_cols and build
   its hash table. Returns rows loaded, 0 when the build file is exhausted. */
static int64_t bnl_load_block(JoinNode *jn) {
    const VecSchema *rs = &jn->right->output_schema;
    int r_ncols = rs->n_cols;
    VecArrayBuilder *rb = (VecArrayBuilder *)calloc(
        (size_t)r_ncols, sizeof(VecArrayBuilder));
    for (int c = 0; c < r_ncols; c++) rb[c] = vec_builder_init(rs->col_types[c]);

    int64_t acc = 0;
    VecBatch *b;
    while (acc <= jn->mem_budget &&
           (b = jn->bnl_rscan->next_batch(jn->bnl_rscan)) != NULL) {
        if (!b->sel) {
            for (int c = 0; c < r_ncols; c++)
                vec_builder_append_array(&rb[c], &b->columns[c]);
        } else {
            int64_t nl = vec_batch_logical_rows(b);
            for (int c = 0; c < r_ncols; c++) vec_builder_reserve(&rb[c], nl);
            for (int64_t li = 0; li < nl; li++) {
                int64_t pi = vec_batch_physical_row(b, li);
                for (int c = 0; c < r_ncols; c++)
                    vec_builder_append_one(&rb[c], &b->columns[c], pi);
            }
        }
        acc += join_batch_bytes(b, r_ncols);
        vec_batch_free(b);
    }

    int64_t nrows = rb[0].length;
    jn->r_cols = (VecArray *)malloc((size_t)r_ncols * sizeof(VecArray));
    for (int c = 0; c < r_ncols; c++) jn->r_cols[c] = vec_builder_finish(&rb[c]);
    free(rb);
    if (nrows == 0) { free(jn->r_cols); jn->r_cols = NULL; return 0; }

    join_coerce_build_keys(jn);
    jht_build_from_rcols(jn, nrows);  /* jht.n_build = block row count */
    return nrows;
}

/* Probe one probe batch against the current build block. Emits matched pairs
   for inner/left/full; records probe-matched (non-inner) and build-matched
   (full) into the global bitsets. Advances jn->bnl_pbase. NULL when empty. */
static VecBatch *bnl_probe_batch(JoinNode *jn, VecBatch *pbatch) {
    int l_ncols = jn->left->output_schema.n_cols;
    int out_ncols = jn->base.output_schema.n_cols;
    int64_t p_logical = vec_batch_logical_rows(pbatch);

    VecArray *coerced[16] = {0};
    VecArray *hash_cols = NULL;
    VecArray *probe_cols = join_coerce_probe_keys(jn, pbatch, coerced, &hash_cols);

    VecArrayBuilder *out = join_alloc_out(jn, out_ncols, p_logical);

    for (int64_t li = 0; li < p_logical; li++) {
        int64_t pr = vec_batch_physical_row(pbatch, li);
        int64_t gord = jn->bnl_pbase + li;
        uint64_t h = hash_join_key(probe_cols, jn->lkey_idx, jn->n_keys, pr);
        int64_t br = jht_probe(&jn->jht, h, probe_cols, jn->lkey_idx,
                               jn->r_cols, jn->rkey_idx, jn->n_keys, pr, jn->na_matches);
        if (br < 0) continue;  /* no match in this block */

        if (jn->kind != JOIN_INNER)
            jn->bnl_pmatched[gord >> 3] |= (uint8_t)(1 << (gord & 7));

        if (jn->kind == JOIN_INNER || jn->kind == JOIN_LEFT ||
            jn->kind == JOIN_FULL) {
            while (br >= 0) {
                if (jn->kind == JOIN_FULL) {
                    int64_t gbr = jn->bnl_block_base + br;
                    jn->bnl_bmatched[gbr >> 3] |= (uint8_t)(1 << (gbr & 7));
                }
                join_emit_matched(jn, out, pbatch->columns, l_ncols, pr, br);
                br = jht_chain_next(&jn->jht, br, probe_cols, jn->lkey_idx,
                                    jn->r_cols, jn->rkey_idx, jn->n_keys, pr, jn->na_matches);
            }
        }
        /* semi/anti: only the pmatched bit; output happens in finalize */
    }

    jn->bnl_pbase += p_logical;
    join_free_probe_keys(jn, coerced, hash_cols);
    return join_finish_out(jn, out, out_ncols);
}

/* Finalize probe-side output from a full probe re-scan: left/full emit unmatched
   rows, semi emits matched rows, anti emits unmatched rows -- all driven by the
   global probe-matched bitset. Loops internally so NULL means the scan is done
   (not merely an empty batch). Advances jn->bnl_pbase as the ordinal cursor. */
static VecBatch *bnl_finalize_probe(JoinNode *jn) {
    int l_ncols = jn->left->output_schema.n_cols;
    int out_ncols = jn->base.output_schema.n_cols;
    for (;;) {
        VecBatch *pb = jn->bnl_pscan->next_batch(jn->bnl_pscan);
        if (!pb) return NULL;
        int64_t nl = vec_batch_logical_rows(pb);
        VecArrayBuilder *out = join_alloc_out(jn, out_ncols, 0);
        for (int64_t li = 0; li < nl; li++) {
            int64_t gord = jn->bnl_pbase + li;
            int matched = (jn->bnl_pmatched[gord >> 3] >> (gord & 7)) & 1;
            int emit = (jn->kind == JOIN_SEMI) ? matched : !matched;
            if (emit)
                join_emit_left_only(jn, out, pb->columns, l_ncols,
                                    vec_batch_physical_row(pb, li));
        }
        jn->bnl_pbase += nl;
        vec_batch_free(pb);
        VecBatch *r = join_finish_out(jn, out, out_ncols);
        if (r) return r;
    }
}

/* Finalize build-side output (full join): emit each unmatched build row as a
   right-only row from a full build re-scan. Loops internally; NULL = done. */
static VecBatch *bnl_finalize_build(JoinNode *jn) {
    int l_ncols = jn->left->output_schema.n_cols;
    int out_ncols = jn->base.output_schema.n_cols;
    int *l_col_rkey = join_build_l_col_rkey(jn, l_ncols);
    for (;;) {
        VecBatch *rb = jn->bnl_rscan->next_batch(jn->bnl_rscan);
        if (!rb) { free(l_col_rkey); return NULL; }
        int64_t nl = vec_batch_logical_rows(rb);
        VecArrayBuilder *out = join_alloc_out(jn, out_ncols, 0);
        for (int64_t li = 0; li < nl; li++) {
            int64_t gord = jn->bnl_pbase + li;  /* reused as build ordinal */
            int matched = (jn->bnl_bmatched[gord >> 3] >> (gord & 7)) & 1;
            if (!matched)
                join_emit_right_only(jn, out, rb->columns, l_ncols, l_col_rkey,
                                     vec_batch_physical_row(rb, li));
        }
        jn->bnl_pbase += nl;
        vec_batch_free(rb);
        VecBatch *r = join_finish_out(jn, out, out_ncols);
        if (r) { free(l_col_rkey); return r; }
    }
}

/* BNL driver: block the build, re-scan the probe per block, then finalize the
   deferred probe-side and (full) build-side rows. */
static VecBatch *join_bnl_next_batch(JoinNode *jn) {
    for (;;) {
        if (jn->bnl_stage == 0) {                 /* load next build block */
            if (!jn->bnl_rscan)
                jn->bnl_rscan = (VecNode *)scan_node_create(jn->bnl_rpath, NULL, 0);
            int64_t nrows = bnl_load_block(jn);
            if (nrows == 0) {                     /* build exhausted */
                jn->bnl_rscan->free_node(jn->bnl_rscan); jn->bnl_rscan = NULL;
                jn->bnl_stage = 2; jn->bnl_fin_side = 0; jn->bnl_pbase = 0;
                continue;
            }
            jn->bnl_pscan = (VecNode *)scan_node_create(jn->bnl_lpath, NULL, 0);
            jn->bnl_pbase = 0;
            jn->bnl_stage = 1;
            continue;
        }
        if (jn->bnl_stage == 1) {                 /* probe current block */
            VecBatch *pb = jn->bnl_pscan->next_batch(jn->bnl_pscan);
            if (!pb) {                            /* block's probe pass done */
                jn->bnl_pscan->free_node(jn->bnl_pscan); jn->bnl_pscan = NULL;
                jn->bnl_block_base += jn->jht.n_build;
                bnl_free_block(jn);
                jn->bnl_stage = 0;
                continue;
            }
            VecBatch *out = bnl_probe_batch(jn, pb);
            vec_batch_free(pb);
            if (out) return out;
            continue;
        }
        /* stage 2: finalize */
        if (jn->bnl_fin_side == 0) {              /* deferred probe-side rows */
            if (jn->kind == JOIN_INNER) {
                jn->bnl_fin_side = 1; jn->bnl_pbase = 0; continue;
            }
            if (!jn->bnl_pscan) {
                jn->bnl_pscan = (VecNode *)scan_node_create(jn->bnl_lpath, NULL, 0);
                jn->bnl_pbase = 0;
            }
            VecBatch *out = bnl_finalize_probe(jn);
            if (out) return out;
            jn->bnl_pscan->free_node(jn->bnl_pscan); jn->bnl_pscan = NULL;
            jn->bnl_fin_side = 1; jn->bnl_pbase = 0;
            continue;
        }
        /* fin_side 1: unmatched build rows (full only) */
        if (jn->kind != JOIN_FULL) return NULL;
        if (!jn->bnl_rscan) {
            jn->bnl_rscan = (VecNode *)scan_node_create(jn->bnl_rpath, NULL, 0);
            jn->bnl_pbase = 0;
        }
        VecBatch *out = bnl_finalize_build(jn);
        if (out) return out;
        jn->bnl_rscan->free_node(jn->bnl_rscan); jn->bnl_rscan = NULL;
        return NULL;
    }
}

static void join_build(JoinNode *jn) {
    const VecSchema *rschema = &jn->right->output_schema;
    jn->r_ncols = rschema->n_cols;

    VecArrayBuilder *r_builders = (VecArrayBuilder *)calloc(
        (size_t)jn->r_ncols, sizeof(VecArrayBuilder));
    for (int c = 0; c < jn->r_ncols; c++)
        r_builders[c] = vec_builder_init(rschema->col_types[c]);

    int64_t acc_bytes = 0;
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
        acc_bytes += join_batch_bytes(batch, jn->r_ncols);
        vec_batch_free(batch);
        /* Build side outgrew the budget. Below the depth cap, hash-partition
           both sides and join one partition at a time (sub-joins re-spill as
           needed). At the cap the partition is un-splittable by hashing (a
           single hot key), so fall back to a block-nested-loop that blocks the
           build under budget and re-scans the probe per block. Either way peak
           stays bounded. */
        if (jn->mem_budget > 0 && acc_bytes > jn->mem_budget) {
            if (jn->spill_depth < JOIN_MAX_SPILL_DEPTH)
                join_spill(jn, r_builders);
            else
                join_spill_bnl(jn, r_builders);
            free(r_builders);
            return;
        }
    }

    int64_t r_nrows = r_builders[0].length;
    jn->r_cols = (VecArray *)malloc((size_t)jn->r_ncols * sizeof(VecArray));
    for (int c = 0; c < jn->r_ncols; c++)
        jn->r_cols[c] = vec_builder_finish(&r_builders[c]);
    free(r_builders);

    /* Coerce build-side key columns to match probe-side types */
    join_coerce_build_keys(jn);

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
        jht_build_from_rcols(jn, r_nrows);
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
    /* Match hash.c vec_hash_value: normalize -0 to +0 and every NaN payload to
       one canonical NaN, so a NaN probe key hashes to the same bucket as the
       canonicalized build key and can actually meet it in join_keys_equal. */
    if (val == 0.0) val = 0.0;
    else if (val != val) val = (double)NAN;
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
    VecArray *hash_cols = NULL;
    VecArray *probe_cols = join_coerce_probe_keys(jn, pbatch,
                                                  coerced_probe_keys, &hash_cols);

    /* Initialize output builders with reserve for expected output */
    VecArrayBuilder *out = join_alloc_out(jn, out_ncols, p_logical);

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
                                jn->n_keys, pr, jn->na_matches);

        switch (jn->kind) {
        case JOIN_SEMI:
            if (br >= 0)
                join_emit_left_only(jn, out, pbatch->columns, l_ncols, pr);
            break;

        case JOIN_ANTI:
            if (br < 0)
                join_emit_left_only(jn, out, pbatch->columns, l_ncols, pr);
            break;

        case JOIN_INNER:
            while (br >= 0) {
                join_emit_matched(jn, out, pbatch->columns, l_ncols, pr, br);
                br = jht_chain_next(&jn->jht, br,
                    probe_cols, jn->lkey_idx,
                    jn->r_cols, jn->rkey_idx, jn->n_keys, pr, jn->na_matches);
            }
            break;

        case JOIN_LEFT:
            if (br >= 0) {
                probe_matched[li / 8] |= (uint8_t)(1 << (li % 8));
                while (br >= 0) {
                    join_emit_matched(jn, out, pbatch->columns, l_ncols, pr, br);
                    br = jht_chain_next(&jn->jht, br,
                        probe_cols, jn->lkey_idx,
                        jn->r_cols, jn->rkey_idx, jn->n_keys, pr, jn->na_matches);
                }
            }
            break;

        case JOIN_FULL:
            if (br >= 0) {
                probe_matched[li / 8] |= (uint8_t)(1 << (li % 8));
                while (br >= 0) {
                    jn->build_matched[br / 8] |=
                        (uint8_t)(1 << (br % 8));
                    join_emit_matched(jn, out, pbatch->columns, l_ncols, pr, br);
                    br = jht_chain_next(&jn->jht, br,
                        probe_cols, jn->lkey_idx,
                        jn->r_cols, jn->rkey_idx, jn->n_keys, pr, jn->na_matches);
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
            join_emit_left_only(jn, out, pbatch->columns, l_ncols, pr);
        }
        free(probe_matched);
    }

    join_free_probe_keys(jn, coerced_probe_keys, hash_cols);

    /* Build result batch (NULL when empty, e.g. anti_join with all matches). */
    return join_finish_out(jn, out, out_ncols);
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

/* An NA key never matches (SQL semantics, matching the hash-join path). Because
   NA sorts last, the only way two keys compare equal with an NA present is when
   both are NA -- so testing the left key is enough at a cmp==0 point. */
static int merge_left_key_na(JoinNode *jn, int64_t pr) {
    for (int k = 0; k < jn->n_keys; k++)
        if (!vec_array_is_valid(&jn->merge_l_batch->columns[jn->lkey_idx[k]], pr))
            return 1;
    return 0;
}

static VecBatch *merge_join_batch(JoinNode *jn) {
    const VecSchema *lschema = &jn->left->output_schema;
    int l_ncols = lschema->n_cols;
    int out_ncols = jn->base.output_schema.n_cols;
    int64_t r_nrows = jn->jht.n_build;  /* reuse n_build for row count */

    VecArrayBuilder *out = join_alloc_out(jn, out_ncols, MERGE_JOIN_BATCH_SIZE);

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

        /* If we're in the middle of a M:N group cross product, continue it.
           merge_r_sub == -1 means inactive; a real cursor is in
           [merge_r_group, merge_r_group_end), and merge_r_group can be 0 (the
           first build group), so the inactive test is `< 0`, not `== 0`. */
        if (jn->merge_r_sub >= 0 && jn->merge_r_sub < jn->merge_r_group_end
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
                /* Done with this left row's group; advance left. -1 = inactive
                   (0 is a valid group cursor, so it cannot mean "done"). */
                jn->merge_r_sub = -1;
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
            /* Left > right: this build row has no matching left row. Emit it now
               (full) and mark it matched so the finalize pass -- which also
               scans build_matched for unmatched build rows -- does not emit it a
               second time. */
            if (jn->kind == JOIN_FULL) {
                int *l_col_rkey = join_build_l_col_rkey(jn, l_ncols);
                join_emit_right_only(jn, out, jn->r_cols, l_ncols,
                                     l_col_rkey, jn->merge_r_cursor);
                free(l_col_rkey);
                jn->build_matched[jn->merge_r_cursor / 8] |=
                    (uint8_t)(1 << (jn->merge_r_cursor % 8));
                emitted++;
            }
            jn->merge_r_cursor++;
        } else if (!jn->na_matches && merge_left_key_na(jn, pr)) {
            /* SQL semantics: cmp == 0 with an NA key means both keys are NA,
               which must not match. Treat this left row as unmatched and advance
               it; the NA build rows are left for the finalize pass (full) or
               dropped (inner/left/semi/anti). Under na_matches this branch is
               skipped so NA falls into the equal-key match below (dplyr). */
            if (jn->kind == JOIN_LEFT || jn->kind == JOIN_FULL)
                join_emit_left_only(jn, out, jn->merge_l_batch->columns,
                                    l_ncols, pr);
            else if (jn->kind == JOIN_ANTI)
                for (int c = 0; c < l_ncols; c++)
                    vec_builder_append_one(&out[c],
                        &jn->merge_l_batch->columns[c], pr);
            if (jn->kind != JOIN_INNER && jn->kind != JOIN_SEMI) emitted++;
            merge_advance_left(jn);
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

    return join_finish_out(jn, out, out_ncols);
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

    VecArrayBuilder *out = join_alloc_out(jn, out_ncols, 0);

    /* Precompute: for each left output column, which right key column
       provides its value (or -1 if NA). Avoids inner-loop key search. */
    int *l_col_rkey = join_build_l_col_rkey(jn, l_ncols);

    int64_t emitted = 0;
    int64_t br = jn->finalize_cursor;
    for (; br < r_nrows && emitted < FINALIZE_BATCH_SIZE; br++) {
        if (jn->build_matched[br / 8] & (1 << (br % 8))) continue;
        join_emit_right_only(jn, out, jn->r_cols, l_ncols, l_col_rkey, br);
        emitted++;
    }
    jn->finalize_cursor = br;
    free(l_col_rkey);

    return join_finish_out(jn, out, out_ncols);
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
        jn->state = (jn->spill || jn->bnl) ? JSTATE_DONE
                  : jn->use_merge ? JSTATE_MERGE : JSTATE_PROBE;
    }

    /* Block-nested-loop terminal fallback (single hot key). */
    if (jn->bnl) return join_bnl_next_batch(jn);

    /* Grace-hash spill driver: pull one partition's sub-join to exhaustion,
       free it, delete its run-files, advance to the next partition. */
    if (jn->spill) {
        for (;;) {
            if (jn->sub_join == NULL) {
                /* Skip partitions that were empty on both sides (no files). */
                while (jn->cur_part < jn->n_parts &&
                       jn->right_parts[jn->cur_part] == NULL)
                    jn->cur_part++;
                if (jn->cur_part >= jn->n_parts) return NULL;
                jn->sub_join = make_partition_join(jn, jn->cur_part);
            }
            VecBatch *out = jn->sub_join->next_batch(jn->sub_join);
            if (out) return out;
            jn->sub_join->free_node(jn->sub_join);
            jn->sub_join = NULL;
            remove(jn->right_parts[jn->cur_part]);
            remove(jn->left_parts[jn->cur_part]);
            jn->cur_part++;
        }
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
    free(jn->build_matched);
    if (jn->r_cols) {
        for (int c = 0; c < jn->r_ncols; c++)
            vec_array_free(&jn->r_cols[c]);
        free(jn->r_cols);
    }
    if (jn->jht.head) jht_free(&jn->jht);
    if (jn->merge_l_batch) vec_batch_free(jn->merge_l_batch);
    /* Grace-hash spill teardown: any active sub-join, then remaining run-files. */
    if (jn->sub_join) jn->sub_join->free_node(jn->sub_join);
    if (jn->right_parts) {
        for (int p = 0; p < jn->n_parts; p++)
            if (jn->right_parts[p]) { remove(jn->right_parts[p]); free(jn->right_parts[p]); }
        free(jn->right_parts);
    }
    if (jn->left_parts) {
        for (int p = 0; p < jn->n_parts; p++)
            if (jn->left_parts[p]) { remove(jn->left_parts[p]); free(jn->left_parts[p]); }
        free(jn->left_parts);
    }
    /* Block-nested-loop teardown: active scans, then the consolidated files. */
    if (jn->bnl_rscan) jn->bnl_rscan->free_node(jn->bnl_rscan);
    if (jn->bnl_pscan) jn->bnl_pscan->free_node(jn->bnl_pscan);
    if (jn->bnl_rpath) { remove(jn->bnl_rpath); free(jn->bnl_rpath); }
    if (jn->bnl_lpath) { remove(jn->bnl_lpath); free(jn->bnl_lpath); }
    free(jn->bnl_pmatched);
    free(jn->bnl_bmatched);
    free(jn->temp_dir);
    vec_schema_free(&jn->base.output_schema);
    free(jn);
}

/* ------------------------------------------------------------------ */
/*  Constructor                                                        */
/* ------------------------------------------------------------------ */

JoinNode *join_node_create(VecNode *left, VecNode *right,
                           JoinKind kind, int n_keys, JoinKey *keys,
                           const char *suffix_x, const char *suffix_y,
                           int64_t mem_budget, const char *temp_dir) {
    /* Several spill-path locals (common[16], rkey[16], lkey[16], ckeys[16]) and
       the probe-key coercion loop are fixed at 16; a join on more keys would
       overrun them. Reject up front rather than corrupt the stack. */
    if (n_keys > 16)
        vectra_error("join supports at most 16 key columns (got %d)", n_keys);
    JoinNode *jn = (JoinNode *)calloc(1, sizeof(JoinNode));
    if (!jn) vectra_error("alloc failed for JoinNode");
    jn->left = left;
    jn->right = right;
    jn->kind = kind;
    jn->n_keys = n_keys;
    jn->na_matches = 1;   /* dplyr default; the bridge overrides from R */
    jn->keys = keys;
    jn->mem_budget = mem_budget;
    if (temp_dir) {
        jn->temp_dir = (char *)malloc(strlen(temp_dir) + 1);
        strcpy(jn->temp_dir, temp_dir);
    }
    size_t sx_len = strlen(suffix_x);
    jn->suffix_x = (char *)malloc(sx_len + 1);
    memcpy(jn->suffix_x, suffix_x, sx_len + 1);
    size_t sy_len = strlen(suffix_y);
    jn->suffix_y = (char *)malloc(sy_len + 1);
    memcpy(jn->suffix_y, suffix_y, sy_len + 1);
    jn->state = JSTATE_BUILD;
    jn->merge_r_sub = -1;   /* -1 = not in an M:N cross product (0 is valid) */

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
