#include "kmer.h"
#include "hash.h"
#include "key_arena.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "seq_util.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/* Phases of the node. */
enum { KP_CONSUME = 0, KP_EMIT = 1, KP_DONE = 2 };

/* Rows emitted per next_batch() call in the emit phase. */
#define KMER_EMIT 131072

/* ------------------------------------------------------------------ */
/*  Record sort order: (group id, packed k-mer) ascending             */
/* ------------------------------------------------------------------ */

static int cmp_kmer_rec(const void *a, const void *b) {
    const KmerRec *x = (const KmerRec *)a;
    const KmerRec *y = (const KmerRec *)b;
    if (x->gid != y->gid) return x->gid < y->gid ? -1 : 1;
    if (x->kmer != y->kmer) return x->kmer < y->kmer ? -1 : 1;
    return 0;
}

/* Slide a k-window over one sequence, packing each all-ACGT window and pushing
   a (group, k-mer) record. A non-ACGT base resets the rolling window, so any
   window spanning it is skipped. rc rolls the reverse complement in parallel. */
static void count_kmers(RecSpill *spill, int64_t g, const char *p, int64_t L,
                        int k, int canonical) {
    if (L < k) return;
    uint64_t mask = (k >= 32) ? ~0ULL : ((1ULL << (2 * k)) - 1);
    int hi = 2 * (k - 1);
    uint64_t fwd = 0, rc = 0;
    int run = 0;
    for (int64_t i = 0; i < L; i++) {
        int code = seq_base2bit(p[i]);
        if (code < 0) { run = 0; fwd = 0; rc = 0; continue; }
        fwd = ((fwd << 2) | (uint64_t)code) & mask;
        rc = (rc >> 2) | ((uint64_t)(3 - code) << hi);
        if (++run >= k) {
            uint64_t key = fwd;
            if (canonical && rc < key) key = rc;
            KmerRec rec = { g, key };
            rec_spill_push(spill, &rec);
        }
    }
}

/* ------------------------------------------------------------------ */
/*  Consume phase: stream the child, spilling one record per k-mer     */
/* ------------------------------------------------------------------ */

static void kmer_consume(KmerNode *kn) {
    const VecSchema *cs = &kn->child->output_schema;

    int seq_idx = vec_schema_find_col(cs, kn->seq_col);
    if (seq_idx < 0)
        vectra_error("kmer: sequence column not found: %s", kn->seq_col);
    if (cs->col_types[seq_idx] != VEC_STRING)
        vectra_error("kmer: sequence column '%s' must be a string column",
                     kn->seq_col);

    int *key_idx = (int *)malloc((size_t)(kn->n_keys > 0 ? kn->n_keys : 1)
                                 * sizeof(int));
    VecType *key_types = (VecType *)malloc((size_t)(kn->n_keys > 0 ? kn->n_keys : 1)
                                           * sizeof(VecType));
    for (int k = 0; k < kn->n_keys; k++) {
        key_idx[k] = vec_schema_find_col(cs, kn->key_names[k]);
        if (key_idx[k] < 0)
            vectra_error("kmer: group column not found: %s", kn->key_names[k]);
        key_types[k] = cs->col_types[key_idx[k]];
    }

    rec_spill_init(&kn->spill, sizeof(KmerRec), cmp_kmer_rec,
                   kn->mem_budget, kn->temp_dir);

    /* Group table (only when there are key columns). */
    VecHashTable ht;
    kn->have_keys = kn->n_keys > 0;
    if (kn->have_keys) {
        ht = vec_ht_create(64);
        key_arena_init(&kn->arena, kn->n_keys, key_types);
    }

    VecBatch *batch;
    while ((batch = kn->child->next_batch(kn->child)) != NULL) {
        int64_t n_logical = vec_batch_logical_rows(batch);
        const VecArray *seq = &batch->columns[seq_idx];

        VecArray *batch_keys = NULL;
        if (kn->have_keys) {
            batch_keys = (VecArray *)malloc((size_t)kn->n_keys * sizeof(VecArray));
            for (int k = 0; k < kn->n_keys; k++)
                batch_keys[k] = batch->columns[key_idx[k]];
        }

        for (int64_t li = 0; li < n_logical; li++) {
            int64_t r = vec_batch_physical_row(batch, li);

            int64_t gid = 0;
            if (kn->have_keys) {
                uint64_t h = 0;
                for (int k = 0; k < kn->n_keys; k++) {
                    uint64_t kh = vec_hash_value(&batch_keys[k], r);
                    h = (k == 0) ? kh : vec_hash_combine(h, kh);
                }
                int was_new = 0;
                gid = vec_ht_find_or_insert(&ht, h, batch_keys, kn->n_keys, r,
                                            kn->arena.arenas, kn->arena.length,
                                            &was_new);
                if (was_new)
                    key_arena_append_row(&kn->arena, batch_keys, r);
            }

            if (!vec_array_is_valid(seq, r)) continue;  /* NA sequence */
            const char *p = seq->buf.str.data + seq->buf.str.offsets[r];
            int64_t L = seq->buf.str.offsets[r + 1] - seq->buf.str.offsets[r];
            count_kmers(&kn->spill, gid, p, L, kn->k, kn->canonical);
        }

        free(batch_keys);
        vec_batch_free(batch);
    }

    if (kn->have_keys) vec_ht_free(&ht);
    free(key_idx);
    free(key_types);

    kn->merge = rec_spill_merge_begin(&kn->spill);
    kn->have_cur = 0;
    kn->cur_count = 0;
}

/* ------------------------------------------------------------------ */
/*  Emit phase: stream distinct (group, k-mer) counts one batch at a   */
/*  time. cur / cur_count carry the open run across batch boundaries.  */
/* ------------------------------------------------------------------ */

static VecBatch *kmer_emit(KmerNode *kn) {
    int64_t cap = KMER_EMIT;
    int32_t *sel = kn->have_keys
                   ? (int32_t *)malloc((size_t)cap * sizeof(int32_t)) : NULL;
    uint64_t *packed = (uint64_t *)malloc((size_t)cap * sizeof(uint64_t));
    int64_t  *counts = (int64_t *)malloc((size_t)cap * sizeof(int64_t));
    if (!packed || !counts || (kn->have_keys && !sel))
        vectra_error("alloc failed for k-mer output");

    int64_t produced = 0;
    for (;;) {
        if (produced >= cap) break;
        KmerRec r;
        if (!rec_spill_merge_next(kn->merge, &r)) {
            if (kn->have_cur) {           /* flush the final open run */
                if (kn->have_keys) sel[produced] = (int32_t)kn->cur.gid;
                packed[produced] = kn->cur.kmer;
                counts[produced] = kn->cur_count;
                produced++;
                kn->have_cur = 0;
            }
            /* Stream drained: close the merge and unlink run files now rather
               than holding their handles open until the node is freed (GC). */
            rec_spill_merge_end(kn->merge);
            kn->merge = NULL;
            rec_spill_free(&kn->spill);
            break;
        }
        if (kn->have_cur && r.gid == kn->cur.gid && r.kmer == kn->cur.kmer) {
            kn->cur_count++;
        } else {
            if (kn->have_cur) {
                if (kn->have_keys) sel[produced] = (int32_t)kn->cur.gid;
                packed[produced] = kn->cur.kmer;
                counts[produced] = kn->cur_count;
                produced++;
            }
            kn->cur = r;
            kn->cur_count = 1;
            kn->have_cur = 1;
        }
    }

    if (produced == 0) {
        free(sel); free(packed); free(counts);
        return NULL;
    }

    int64_t n_out = produced;
    int n_cols = kn->n_keys + 2;
    VecBatch *result = vec_batch_alloc(n_cols, n_out);

    /* Group key columns, gathered from the arena by group id. */
    for (int k = 0; k < kn->n_keys; k++) {
        result->columns[k] = vec_array_gather(&kn->arena.arenas[k], sel,
                                              (int32_t)n_out);
        size_t nm = strlen(kn->key_names[k]);
        result->col_names[k] = (char *)malloc(nm + 1);
        memcpy(result->col_names[k], kn->key_names[k], nm + 1);
    }

    /* kmer string column: each value is exactly k characters. */
    {
        int kk = kn->k;
        VecArray arr = vec_array_alloc(VEC_STRING, n_out);
        int64_t total = n_out * kk;
        free(arr.buf.str.data);
        arr.buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
        if (!arr.buf.str.data) vectra_error("alloc failed for kmer strings");
        arr.buf.str.data_len = total;
        char *d = arr.buf.str.data;
        for (int64_t r = 0; r < n_out; r++) {
            arr.buf.str.offsets[r] = r * kk;
            vec_array_set_valid(&arr, r);
            uint64_t key = packed[r];
            char *dst = d + r * kk;
            for (int c = 0; c < kk; c++) {
                int base = (int)((key >> (2 * (kk - 1 - c))) & 3);
                dst[c] = "ACGT"[base];
            }
        }
        arr.buf.str.offsets[n_out] = total;
        result->columns[kn->n_keys] = arr;
        result->col_names[kn->n_keys] = (char *)malloc(5);
        memcpy(result->col_names[kn->n_keys], "kmer", 5);
    }

    /* count column (int64). */
    {
        VecArray arr = vec_array_alloc(VEC_INT64, n_out);
        for (int64_t r = 0; r < n_out; r++) {
            arr.buf.i64[r] = counts[r];
            vec_array_set_valid(&arr, r);
        }
        result->columns[kn->n_keys + 1] = arr;
        result->col_names[kn->n_keys + 1] = (char *)malloc(6);
        memcpy(result->col_names[kn->n_keys + 1], "count", 6);
    }

    free(sel);
    free(packed);
    free(counts);
    return result;
}

/* ------------------------------------------------------------------ */
/*  Node body                                                          */
/* ------------------------------------------------------------------ */

static VecBatch *kmer_next_batch(VecNode *self) {
    KmerNode *kn = (KmerNode *)self;
    if (kn->phase == KP_DONE) return NULL;
    if (kn->phase == KP_CONSUME) {
        kmer_consume(kn);
        kn->phase = KP_EMIT;
    }
    if (kn->merge == NULL) {           /* already drained on a prior call */
        kn->phase = KP_DONE;
        return NULL;
    }
    VecBatch *b = kmer_emit(kn);
    if (!b) kn->phase = KP_DONE;
    return b;
}

static void kmer_free(VecNode *self) {
    KmerNode *kn = (KmerNode *)self;
    kn->child->free_node(kn->child);
    if (kn->merge) rec_spill_merge_end(kn->merge);
    if (kn->phase != KP_CONSUME) {
        rec_spill_free(&kn->spill);
        if (kn->have_keys) key_arena_free(&kn->arena);
    }
    free(kn->seq_col);
    free(kn->temp_dir);
    for (int k = 0; k < kn->n_keys; k++)
        free(kn->key_names[k]);
    free(kn->key_names);
    vec_schema_free(&kn->base.output_schema);
    free(kn);
}

KmerNode *kmer_node_create(VecNode *child, const char *seq_col,
                           int k, int canonical,
                           int n_keys, char **key_names,
                           int64_t mem_budget, const char *temp_dir) {
    if (k < 1 || k > 32)
        vectra_error("kmer: k must be between 1 and 32 (got %d)", k);

    KmerNode *kn = (KmerNode *)calloc(1, sizeof(KmerNode));
    if (!kn) vectra_error("alloc failed for KmerNode");

    kn->child = child;
    kn->seq_col = (char *)malloc(strlen(seq_col) + 1);
    memcpy(kn->seq_col, seq_col, strlen(seq_col) + 1);
    kn->k = k;
    kn->canonical = canonical ? 1 : 0;
    kn->n_keys = n_keys;
    kn->key_names = key_names;
    kn->mem_budget = mem_budget;
    kn->temp_dir = temp_dir ? strdup(temp_dir) : NULL;
    kn->phase = KP_CONSUME;

    /* Output schema: key columns (types from child) + kmer (string) + count. */
    const VecSchema *cs = &child->output_schema;
    int n_out = n_keys + 2;
    char   **out_names = (char **)malloc((size_t)n_out * sizeof(char *));
    VecType *out_types = (VecType *)malloc((size_t)n_out * sizeof(VecType));
    for (int i = 0; i < n_keys; i++) {
        out_names[i] = key_names[i];
        int idx = vec_schema_find_col(cs, key_names[i]);
        out_types[i] = (idx >= 0) ? cs->col_types[idx] : VEC_STRING;
    }
    out_names[n_keys]     = (char *)"kmer";
    out_types[n_keys]     = VEC_STRING;
    out_names[n_keys + 1] = (char *)"count";
    out_types[n_keys + 1] = VEC_INT64;

    kn->base.output_schema = vec_schema_create(n_out, out_names, out_types);
    free(out_names);
    free(out_types);

    kn->base.next_batch = kmer_next_batch;
    kn->base.free_node = kmer_free;
    kn->base.kind = "KmerNode";
    kn->base.row_count_hint = -1;

    return kn;
}
