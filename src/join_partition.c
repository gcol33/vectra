#include "join_partition.h"
#include "array.h"
#include "batch.h"
#include "builder.h"
#include "error.h"
#include "grow.h"
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/*  String access helpers for materialized VecArrays                   */
/* ------------------------------------------------------------------ */

static inline const char *str_ptr(const VecArray *arr, int64_t row) {
    return arr->buf.str.data + arr->buf.str.offsets[row];
}

static inline int64_t str_len(const VecArray *arr, int64_t row) {
    return arr->buf.str.offsets[row + 1] - arr->buf.str.offsets[row];
}

static void partition_push(JoinPartition *p, int64_t row) {
    vec_grow_to((void **)&p->rows, &p->capacity, p->n_rows + 1,
                sizeof(int64_t), "JoinPartition");
    p->rows[p->n_rows++] = row;
}

/* ------------------------------------------------------------------ */
/*  Hash table mapping a block-key string to a partition id            */
/* ------------------------------------------------------------------ */

static uint64_t hash_string(const char *s, int64_t len) {
    uint64_t h = 14695981039346656037ULL;
    for (int64_t i = 0; i < len; i++)
        h = (h ^ (uint8_t)s[i]) * 1099511628211ULL;
    return h;
}

typedef struct {
    int64_t       n_slots;
    int64_t      *part_id;       /* partition id at slot, or -1 */
    uint64_t     *slot_hash;
    const char  **slot_key;      /* pointer + length into the source array */
    int64_t      *slot_key_len;
    int64_t       n_parts;
} BlockHT;

static BlockHT block_ht_create(int64_t estimated_groups) {
    BlockHT ht;
    int64_t n_slots = 64;
    while (n_slots < estimated_groups * 2) n_slots *= 2;
    ht.n_slots = n_slots;
    ht.part_id = (int64_t *)malloc((size_t)n_slots * sizeof(int64_t));
    ht.slot_hash = (uint64_t *)malloc((size_t)n_slots * sizeof(uint64_t));
    ht.slot_key = (const char **)calloc((size_t)n_slots, sizeof(const char *));
    ht.slot_key_len = (int64_t *)calloc((size_t)n_slots, sizeof(int64_t));
    ht.n_parts = 0;
    if (!ht.part_id || !ht.slot_hash || !ht.slot_key || !ht.slot_key_len)
        vectra_error("alloc failed for BlockHT");
    memset(ht.part_id, -1, (size_t)n_slots * sizeof(int64_t));
    return ht;
}

static void block_ht_free(BlockHT *ht) {
    free(ht->part_id);
    free(ht->slot_hash);
    free((void *)ht->slot_key);
    free(ht->slot_key_len);
}

static int64_t block_ht_find_or_insert(BlockHT *ht,
                                       const char *key, int64_t key_len,
                                       uint64_t hash) {
    int64_t mask = ht->n_slots - 1;
    int64_t slot = (int64_t)(hash & (uint64_t)mask);
    for (;;) {
        if (ht->part_id[slot] == -1) {
            int64_t pid = ht->n_parts++;
            ht->part_id[slot] = pid;
            ht->slot_hash[slot] = hash;
            ht->slot_key[slot] = key;
            ht->slot_key_len[slot] = key_len;
            return pid;
        }
        if (ht->slot_hash[slot] == hash &&
            ht->slot_key_len[slot] == key_len &&
            memcmp(ht->slot_key[slot], key, (size_t)key_len) == 0) {
            return ht->part_id[slot];
        }
        slot = (slot + 1) & mask;
    }
}

/* Look up only; return -1 when the key is absent (probe key not in build). */
static int64_t block_ht_lookup(const BlockHT *ht,
                               const char *key, int64_t key_len,
                               uint64_t hash) {
    int64_t mask = ht->n_slots - 1;
    int64_t slot = (int64_t)(hash & (uint64_t)mask);
    for (;;) {
        if (ht->part_id[slot] == -1) return -1;
        if (ht->slot_hash[slot] == hash &&
            ht->slot_key_len[slot] == key_len &&
            memcmp(ht->slot_key[slot], key, (size_t)key_len) == 0) {
            return ht->part_id[slot];
        }
        slot = (slot + 1) & mask;
    }
}

/* ------------------------------------------------------------------ */
/*  Build                                                              */
/* ------------------------------------------------------------------ */

static JoinPartition *single_partition_all_rows(int64_t nrows) {
    JoinPartition *parts = (JoinPartition *)calloc(1, sizeof(JoinPartition));
    if (!parts) vectra_error("alloc failed for partition");
    parts[0].rows = (int64_t *)malloc((size_t)(nrows > 0 ? nrows : 1) *
                                      sizeof(int64_t));
    if (!parts[0].rows) vectra_error("alloc failed for partition rows");
    parts[0].n_rows = nrows;
    parts[0].capacity = nrows;
    for (int64_t i = 0; i < nrows; i++) parts[0].rows[i] = i;
    return parts;
}

JoinPartitionSet join_partition_build(
    const VecArray *p_cols, int64_t p_nrows, int probe_block_col,
    const VecArray *b_cols, int64_t b_nrows, int build_block_col)
{
    JoinPartitionSet set = {0};

    int has_block = (probe_block_col >= 0 && build_block_col >= 0);
    if (!has_block) {
        set.n_parts = 1;
        set.probe_parts = single_partition_all_rows(p_nrows);
        set.build_parts = single_partition_all_rows(b_nrows);
        return set;
    }

    int64_t est_parts = b_nrows / 50;
    if (est_parts < 64) est_parts = 64;
    BlockHT ht = block_ht_create(est_parts);

    int64_t parts_cap = est_parts;
    JoinPartition *b_parts = (JoinPartition *)calloc(
        (size_t)parts_cap, sizeof(JoinPartition));
    if (!b_parts) vectra_error("alloc failed for build partitions");

    const VecArray *b_block = &b_cols[build_block_col];
    for (int64_t r = 0; r < b_nrows; r++) {
        if (!vec_array_is_valid(b_block, r)) continue;
        const char *key = str_ptr(b_block, r);
        int64_t klen = str_len(b_block, r);
        uint64_t h = hash_string(key, klen);
        int64_t pid = block_ht_find_or_insert(&ht, key, klen, h);

        if (pid >= parts_cap) {
            int64_t new_cap = parts_cap * 2;
            while (pid >= new_cap) new_cap *= 2;
            b_parts = (JoinPartition *)realloc(b_parts,
                (size_t)new_cap * sizeof(JoinPartition));
            if (!b_parts) vectra_error("realloc failed for build partitions");
            memset(b_parts + parts_cap, 0,
                   (size_t)(new_cap - parts_cap) * sizeof(JoinPartition));
            parts_cap = new_cap;
        }
        partition_push(&b_parts[pid], r);
    }

    int64_t n_parts = ht.n_parts;

    JoinPartition *p_parts = (JoinPartition *)calloc(
        (size_t)(n_parts > 0 ? n_parts : 1), sizeof(JoinPartition));
    if (!p_parts) vectra_error("alloc failed for probe partitions");

    const VecArray *p_block = &p_cols[probe_block_col];
    for (int64_t r = 0; r < p_nrows; r++) {
        if (!vec_array_is_valid(p_block, r)) continue;
        const char *key = str_ptr(p_block, r);
        int64_t klen = str_len(p_block, r);
        uint64_t h = hash_string(key, klen);
        int64_t pid = block_ht_lookup(&ht, key, klen, h);
        if (pid >= 0) partition_push(&p_parts[pid], r);
    }

    block_ht_free(&ht);

    set.n_parts = n_parts;
    set.probe_parts = p_parts;
    set.build_parts = b_parts;
    return set;
}

void join_partition_free(JoinPartition *parts, int64_t n_parts) {
    if (!parts) return;
    for (int64_t i = 0; i < n_parts; i++)
        free(parts[i].rows);
    free(parts);
}

void join_partition_set_free(JoinPartitionSet *set) {
    join_partition_free(set->probe_parts, set->n_parts);
    join_partition_free(set->build_parts, set->n_parts);
    set->probe_parts = NULL;
    set->build_parts = NULL;
    set->n_parts = 0;
}

/* ------------------------------------------------------------------ */
/*  Materialize a child node into flat VecArrays                       */
/* ------------------------------------------------------------------ */

void join_materialize_side(VecNode *child, int ncols,
                           VecArray **out_cols, int64_t *out_nrows) {
    VecArrayBuilder *builders = (VecArrayBuilder *)calloc(
        (size_t)ncols, sizeof(VecArrayBuilder));
    if (!builders) vectra_error("alloc failed in join materialize");

    const VecSchema *schema = &child->output_schema;
    for (int c = 0; c < ncols; c++)
        builders[c] = vec_builder_init(schema->col_types[c]);

    VecBatch *batch;
    while ((batch = child->next_batch(child)) != NULL) {
        if (!batch->sel) {
            for (int c = 0; c < ncols; c++)
                vec_builder_append_array(&builders[c], &batch->columns[c]);
        } else {
            int64_t n_logical = vec_batch_logical_rows(batch);
            for (int c = 0; c < ncols; c++)
                vec_builder_reserve(&builders[c], n_logical);
            for (int64_t li = 0; li < n_logical; li++) {
                int64_t pi = vec_batch_physical_row(batch, li);
                for (int c = 0; c < ncols; c++)
                    vec_builder_append_one(&builders[c],
                                           &batch->columns[c], pi);
            }
        }
        vec_batch_free(batch);
    }

    int64_t nrows = builders[0].length;
    VecArray *cols = (VecArray *)malloc((size_t)ncols * sizeof(VecArray));
    if (!cols) vectra_error("alloc failed in join materialize");
    for (int c = 0; c < ncols; c++)
        cols[c] = vec_builder_finish(&builders[c]);
    free(builders);

    *out_cols = cols;
    *out_nrows = nrows;
}
