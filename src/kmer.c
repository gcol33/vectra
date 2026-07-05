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

/* ------------------------------------------------------------------ */
/*  (group_id, packed k-mer) -> count open-addressing hash table       */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t  group_id;
    uint64_t kmer;    /* 2-bit packed, canonical if requested */
    int64_t  count;   /* 0 = empty slot */
} KmerCell;

typedef struct {
    KmerCell *cells;
    int64_t   n_slots;  /* power of 2 */
    int64_t   n_used;
} KmerTable;

/* SplitMix64 finalizer over a mix of the group id and the packed k-mer. */
static inline uint64_t kmer_hash(int64_t g, uint64_t kmer) {
    uint64_t h = kmer + 0x9E3779B97F4A7C15ULL * (uint64_t)g;
    h ^= h >> 30; h *= 0xBF58476D1CE4E5B9ULL;
    h ^= h >> 27; h *= 0x94D049BB133111EBULL;
    h ^= h >> 31;
    return h;
}

static void kmer_table_init(KmerTable *t) {
    t->n_slots = 1024;
    t->n_used = 0;
    t->cells = (KmerCell *)calloc((size_t)t->n_slots, sizeof(KmerCell));
    if (!t->cells) vectra_error("alloc failed for k-mer table");
}

static void kmer_table_grow(KmerTable *t) {
    int64_t new_slots = t->n_slots * 2;
    KmerCell *nc = (KmerCell *)calloc((size_t)new_slots, sizeof(KmerCell));
    if (!nc) vectra_error("alloc failed for k-mer table");
    uint64_t mask = (uint64_t)new_slots - 1;
    for (int64_t s = 0; s < t->n_slots; s++) {
        if (t->cells[s].count == 0) continue;
        uint64_t i = kmer_hash(t->cells[s].group_id, t->cells[s].kmer) & mask;
        while (nc[i].count != 0) i = (i + 1) & mask;
        nc[i] = t->cells[s];
    }
    free(t->cells);
    t->cells = nc;
    t->n_slots = new_slots;
}

static void kmer_table_add(KmerTable *t, int64_t g, uint64_t kmer) {
    /* Grow past 70% load. */
    if ((t->n_used + 1) * 10 >= t->n_slots * 7) kmer_table_grow(t);
    uint64_t mask = (uint64_t)t->n_slots - 1;
    uint64_t i = kmer_hash(g, kmer) & mask;
    while (t->cells[i].count != 0) {
        if (t->cells[i].group_id == g && t->cells[i].kmer == kmer) {
            t->cells[i].count++;
            return;
        }
        i = (i + 1) & mask;
    }
    t->cells[i].group_id = g;
    t->cells[i].kmer = kmer;
    t->cells[i].count = 1;
    t->n_used++;
}

/* Slide a k-window over one sequence, packing each all-ACGT window and feeding
   it to the table. A non-ACGT base resets the rolling window, so any window
   spanning it is skipped. rc rolls the reverse complement in parallel. */
static void count_kmers(KmerTable *t, int64_t g, const char *p, int64_t L,
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
            kmer_table_add(t, g, key);
        }
    }
}

/* ------------------------------------------------------------------ */
/*  Node body                                                          */
/* ------------------------------------------------------------------ */

static VecBatch *kmer_run(KmerNode *kn) {
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

    KmerTable kt;
    kmer_table_init(&kt);

    /* Group table (only when there are key columns). */
    VecHashTable ht;
    KeyArena arena;
    int have_keys = kn->n_keys > 0;
    if (have_keys) {
        ht = vec_ht_create(64);
        key_arena_init(&arena, kn->n_keys, key_types);
    }

    VecBatch *batch;
    while ((batch = kn->child->next_batch(kn->child)) != NULL) {
        int64_t n_logical = vec_batch_logical_rows(batch);
        const VecArray *seq = &batch->columns[seq_idx];

        VecArray *batch_keys = NULL;
        if (have_keys) {
            batch_keys = (VecArray *)malloc((size_t)kn->n_keys * sizeof(VecArray));
            for (int k = 0; k < kn->n_keys; k++)
                batch_keys[k] = batch->columns[key_idx[k]];
        }

        for (int64_t li = 0; li < n_logical; li++) {
            int64_t r = vec_batch_physical_row(batch, li);

            int64_t gid = 0;
            if (have_keys) {
                uint64_t h = 0;
                for (int k = 0; k < kn->n_keys; k++) {
                    uint64_t kh = vec_hash_value(&batch_keys[k], r);
                    h = (k == 0) ? kh : vec_hash_combine(h, kh);
                }
                int was_new = 0;
                gid = vec_ht_find_or_insert(&ht, h, batch_keys, kn->n_keys, r,
                                            arena.arenas, arena.length, &was_new);
                if (was_new)
                    key_arena_append_row(&arena, batch_keys, r);
            }

            if (!vec_array_is_valid(seq, r)) continue;  /* NA sequence */
            const char *p = seq->buf.str.data + seq->buf.str.offsets[r];
            int64_t L = seq->buf.str.offsets[r + 1] - seq->buf.str.offsets[r];
            count_kmers(&kt, gid, p, L, kn->k, kn->canonical);
        }

        free(batch_keys);
        vec_batch_free(batch);
    }

    /* Materialize distinct (group, k-mer) cells into flat output columns. */
    int64_t n_out = kt.n_used;
    int32_t *sel = have_keys
                   ? (int32_t *)malloc((size_t)(n_out > 0 ? n_out : 1) * sizeof(int32_t))
                   : NULL;
    uint64_t *packed = (uint64_t *)malloc((size_t)(n_out > 0 ? n_out : 1) * sizeof(uint64_t));
    int64_t  *counts = (int64_t *)malloc((size_t)(n_out > 0 ? n_out : 1) * sizeof(int64_t));
    if (!packed || !counts || (have_keys && !sel))
        vectra_error("alloc failed for k-mer output");

    int64_t j = 0;
    for (int64_t s = 0; s < kt.n_slots; s++) {
        if (kt.cells[s].count == 0) continue;
        if (have_keys) sel[j] = (int32_t)kt.cells[s].group_id;
        packed[j] = kt.cells[s].kmer;
        counts[j] = kt.cells[s].count;
        j++;
    }

    int n_cols = kn->n_keys + 2;
    VecBatch *result = vec_batch_alloc(n_cols, n_out);

    /* Group key columns, gathered from the arena by group id. */
    for (int k = 0; k < kn->n_keys; k++) {
        result->columns[k] = vec_array_gather(&arena.arenas[k], sel, (int32_t)n_out);
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
    free(kt.cells);
    if (have_keys) { vec_ht_free(&ht); key_arena_free(&arena); }
    free(key_idx);
    free(key_types);

    return result;
}

static VecBatch *kmer_next_batch(VecNode *self) {
    KmerNode *kn = (KmerNode *)self;
    if (kn->done) return NULL;
    kn->done = 1;
    return kmer_run(kn);
}

static void kmer_free(VecNode *self) {
    KmerNode *kn = (KmerNode *)self;
    kn->child->free_node(kn->child);
    free(kn->seq_col);
    for (int k = 0; k < kn->n_keys; k++)
        free(kn->key_names[k]);
    free(kn->key_names);
    vec_schema_free(&kn->base.output_schema);
    free(kn);
}

KmerNode *kmer_node_create(VecNode *child, const char *seq_col,
                           int k, int canonical,
                           int n_keys, char **key_names) {
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
    kn->done = 0;

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
