#include "group_agg.h"
#include "hash.h"
#include "key_arena.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "coerce.h"
#include "builder.h"
#include "sort.h"
#include "key_snap.h"
#include "error.h"
#include "vec_omp.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* Per-group spill budget for one holistic accumulator (median / n_distinct).
   The node's mem_budget is split across the holistic aggregations so their
   concurrent in-RAM buffers for a single group sum to <= mem_budget before any
   spills. Scalar aggregations ignore the value (they hold O(1) state). */
static int64_t agg_holistic_budget(const GroupAggNode *ga) {
    int n_holistic = 0;
    for (int a = 0; a < ga->n_aggs; a++)
        if (ga->agg_specs[a].kind == AGG_MEDIAN ||
            ga->agg_specs[a].kind == AGG_N_DISTINCT)
            n_holistic++;
    if (n_holistic < 1) n_holistic = 1;
    return ga->mem_budget / n_holistic;
}

/* ================================================================== */
/*  Hash-based aggregation (original path)                            */
/* ================================================================== */

/* Output column type of an aggregate. Every aggregate emits a double except
   first()/last() on a string column, which preserve the string type. */
static VecType agg_output_type(AggKind kind, VecType input_type) {
    if ((kind == AGG_FIRST || kind == AGG_LAST) && input_type == VEC_STRING)
        return VEC_STRING;
    return VEC_DOUBLE;
}

static VecBatch *hash_agg_next_batch(GroupAggNode *ga) {
    const VecSchema *child_schema = &ga->child->output_schema;

    int *key_indices = (int *)malloc((size_t)ga->n_keys * sizeof(int));
    VecType *key_types = (VecType *)malloc((size_t)ga->n_keys * sizeof(VecType));
    for (int k = 0; k < ga->n_keys; k++) {
        key_indices[k] = vec_schema_find_col(child_schema, ga->key_names[k]);
        if (key_indices[k] < 0)
            vectra_error("group_by: column not found: %s", ga->key_names[k]);
        key_types[k] = child_schema->col_types[key_indices[k]];
    }

    int *agg_col_indices = (int *)malloc((size_t)ga->n_aggs * sizeof(int));
    VecType *agg_types = (VecType *)malloc((size_t)ga->n_aggs * sizeof(VecType));
    for (int a = 0; a < ga->n_aggs; a++) {
        if (ga->agg_specs[a].kind == AGG_COUNT_STAR) {
            agg_col_indices[a] = -1;
            agg_types[a] = VEC_INT64;
        } else {
            agg_col_indices[a] = vec_schema_find_col(child_schema,
                ga->agg_specs[a].input_col);
            if (agg_col_indices[a] < 0)
                vectra_error("summarise: column not found: %s",
                             ga->agg_specs[a].input_col);
            agg_types[a] = child_schema->col_types[agg_col_indices[a]];
        }
    }

    int64_t store_mem = agg_holistic_budget(ga);
    AggAccum *accums = (AggAccum *)malloc((size_t)ga->n_aggs * sizeof(AggAccum));
    for (int a = 0; a < ga->n_aggs; a++) {
        accums[a] = agg_accum_init(ga->agg_specs[a].kind,
                                    agg_types[a],
                                    ga->agg_specs[a].na_rm,
                                    store_mem, ga->temp_dir);
    }

    VecHashTable ht = vec_ht_create(64);
    KeyArena arena;
    key_arena_init(&arena, ga->n_keys, key_types);

    VecBatch *batch;
    while ((batch = ga->child->next_batch(ga->child)) != NULL) {
        VecArray *batch_keys = (VecArray *)malloc((size_t)ga->n_keys * sizeof(VecArray));
        for (int k = 0; k < ga->n_keys; k++)
            batch_keys[k] = batch->columns[key_indices[k]];

        int64_t n_logical = vec_batch_logical_rows(batch);

        /* Pre-compute row hashes in parallel.  The hash function reads
           only from immutable batch columns, so there are no conflicts.
           Insert + accumulate remain sequential (hash table writes and
           accumulator feeds have data dependencies). */
        uint64_t *row_hashes = (uint64_t *)malloc(
            (size_t)(n_logical > 0 ? n_logical : 1) * sizeof(uint64_t));
        if (!row_hashes) vectra_error("alloc failed for row hash array");

        #pragma omp parallel for if(n_logical > VEC_OMP_THRESHOLD) schedule(static)
        for (int64_t li = 0; li < n_logical; li++) {
            int64_t r = vec_batch_physical_row(batch, li);
            uint64_t h = 0;
            for (int k = 0; k < ga->n_keys; k++) {
                uint64_t kh = vec_hash_value(&batch_keys[k], r);
                h = (k == 0) ? kh : vec_hash_combine(h, kh);
            }
            row_hashes[li] = h;
        }

        /* Sequential insert + accumulate using pre-computed hashes.
           Prefetch upcoming hash table slots to hide memory latency. */
        int64_t ht_mask = ht.n_slots - 1;
        for (int64_t li = 0; li < n_logical; li++) {
            if (li + 8 < n_logical) {
                int64_t pf_idx = (int64_t)(row_hashes[li + 8] & (uint64_t)ht_mask);
                __builtin_prefetch(&ht.entries[pf_idx], 1, 1);
            }
            int64_t r = vec_batch_physical_row(batch, li);

            int was_new = 0;
            int64_t gid = vec_ht_find_or_insert(
                &ht, row_hashes[li], batch_keys, ga->n_keys, r,
                arena.arenas, arena.length, &was_new);

            if (was_new) {
                key_arena_append_row(&arena, batch_keys, r);
                for (int a = 0; a < ga->n_aggs; a++)
                    agg_accum_ensure(&accums[a], ht.n_groups);
            }

            for (int a = 0; a < ga->n_aggs; a++) {
                if (agg_col_indices[a] >= 0) {
                    agg_accum_feed(&accums[a], gid,
                                   &batch->columns[agg_col_indices[a]], r);
                } else {
                    agg_accum_feed(&accums[a], gid, NULL, 0);
                }
            }
        }

        free(row_hashes);

        free(batch_keys);
        vec_batch_free(batch);
    }

    int64_t n_groups = ht.n_groups;
    int n_out = ga->n_keys + ga->n_aggs;
    VecBatch *result = vec_batch_alloc(n_out, n_groups);

    for (int k = 0; k < ga->n_keys; k++) {
        VecArray *src = &arena.arenas[k];
        src->length = n_groups;
        if (key_types[k] == VEC_STRING) {
            VecArray arr = vec_array_alloc(VEC_STRING, n_groups);
            memcpy(arr.validity, src->validity, (size_t)vec_validity_bytes(n_groups));
            memcpy(arr.buf.str.offsets, src->buf.str.offsets,
                   (size_t)(n_groups + 1) * sizeof(int64_t));
            int64_t dlen = arena.str_data_len[k];
            free(arr.buf.str.data);
            arr.buf.str.data = (char *)malloc((size_t)(dlen > 0 ? dlen : 1));
            if (dlen > 0)
                memcpy(arr.buf.str.data, arena.str_data[k], (size_t)dlen);
            arr.buf.str.data_len = dlen;
            result->columns[k] = arr;
        } else {
            VecArray *copy = vec_coerce(src, src->type);
            copy->length = n_groups;
            result->columns[k] = *copy;
            free(copy);
        }
        size_t kn_len = strlen(ga->key_names[k]);
        result->col_names[k] = (char *)malloc(kn_len + 1);
        memcpy(result->col_names[k], ga->key_names[k], kn_len + 1);
    }

    for (int a = 0; a < ga->n_aggs; a++) {
        result->columns[ga->n_keys + a] = agg_accum_finish(&accums[a]);
        size_t on_len = strlen(ga->agg_specs[a].output_name);
        result->col_names[ga->n_keys + a] = (char *)malloc(on_len + 1);
        memcpy(result->col_names[ga->n_keys + a], ga->agg_specs[a].output_name, on_len + 1);
    }

    for (int a = 0; a < ga->n_aggs; a++)
        agg_accum_free(&accums[a]);
    free(accums);
    free(key_indices);
    free(key_types);
    free(agg_col_indices);
    free(agg_types);
    vec_ht_free(&ht);
    key_arena_free(&arena);

    return result;
}

/* ================================================================== */
/*  Sort-based aggregation (spill-safe path)                          */
/*                                                                    */
/*  Pre-condition: child is a SortNode sorted by the key columns.     */
/*  Linear scan: consecutive rows with identical keys belong to the   */
/*  same group.  Accumulators hold state for ONE group at a time.     */
/* ================================================================== */

/* KeySnap (group-boundary detection over a key-sorted stream) is shared with
   group_topn; see key_snap.h. */

/* Flush completed group: append key snapshot + agg results to builders */
static void flush_group(const KeySnap *snap,
                        VecArrayBuilder *key_builders, int n_keys,
                        VecArrayBuilder *agg_builders, int n_aggs,
                        AggAccum *accums, const VecType *agg_types,
                        const AggSpec *agg_specs,
                        int64_t mem_budget, const char *temp_dir) {
    /* Append key values */
    for (int k = 0; k < n_keys; k++) {
        VecArrayBuilder *b = &key_builders[k];
        if (!snap->valid[k]) {
            vec_builder_append_na(b);
        } else {
            /* Ensure capacity for 1 row */
            vec_builder_reserve(b, 1);
            b->validity[b->length / 8] |= (uint8_t)(1 << (b->length % 8));
            switch (snap->types[k]) {
            case VEC_INT64:  b->buf.i64[b->length] = snap->i64[k]; break;
            case VEC_INT32:  b->buf.i32[b->length] = (int32_t)snap->i64[k]; break;
            case VEC_INT16:  b->buf.i16[b->length] = (int16_t)snap->i64[k]; break;
            case VEC_INT8:   b->buf.i8[b->length]  = (int8_t)snap->i64[k]; break;
            case VEC_DOUBLE: b->buf.dbl[b->length] = snap->dbl[k]; break;
            case VEC_BOOL:   b->buf.bln[b->length] = snap->bln[k]; break;
            case VEC_STRING: {
                int64_t soff = snap->str_offs[k];
                int64_t slen = snap->str_offs[k + 1] - soff;
                /* Manually append string */
                if (slen > 0) {
                    int64_t needed = b->str_data_len + slen;
                    if (needed > b->str_data_cap) {
                        int64_t nc = b->str_data_cap == 0 ? 256 : b->str_data_cap;
                        while (nc < needed) nc *= 2;
                        b->str_data = (char *)realloc(b->str_data, (size_t)nc);
                        b->str_data_cap = nc;
                    }
                    b->str_offsets[b->length] = b->str_data_len;
                    memcpy(b->str_data + b->str_data_len,
                           snap->str_data + soff, (size_t)slen);
                    b->str_data_len += slen;
                    b->str_offsets[b->length + 1] = b->str_data_len;
                } else {
                    b->str_offsets[b->length] = b->str_data_len;
                    b->str_offsets[b->length + 1] = b->str_data_len;
                }
                break;
            }
            }
            b->length++;
        }
    }

    /* Append agg results (each accumulator has n_groups=1) */
    for (int a = 0; a < n_aggs; a++) {
        VecArray arr = agg_accum_finish(&accums[a]);
        vec_builder_append_one(&agg_builders[a], &arr, 0);
        vec_array_free(&arr);
        /* Free this group's accumulator (buffers, spill run files) before
           reusing the slot for the next group -- otherwise every group but the
           last leaks its state, which for median/n_distinct is the whole
           group. Then reinitialize for the next group. */
        agg_accum_free(&accums[a]);
        accums[a] = agg_accum_init(agg_specs[a].kind, agg_types[a],
                                    agg_specs[a].na_rm, mem_budget, temp_dir);
        agg_accum_ensure(&accums[a], 1);
    }
}

/* Emit the sorted grouped result in bounded batches rather than one giant batch
   whose size is O(#groups). State persists on the node across next_batch calls;
   completed groups accumulate in the builders and are flushed out once the
   emit threshold is reached, while the open group's accumulator + key snapshot
   carry over. Peak resident output is the emit threshold plus one child batch
   of groups -- bounded by the child rowgroup size, not the total group count. */
#define GROUP_AGG_EMIT 131072

typedef struct {
    int              inited;
    int              scan_done;   /* child exhausted; last group flushed */
    int             *key_indices;
    VecType         *key_types;
    int             *agg_col_indices;
    VecType         *agg_types;
    VecArrayBuilder *key_builders;
    VecArrayBuilder *agg_builders;
    AggAccum        *accums;
    int64_t          store_mem;
    KeySnap          snap;
    VecBatch        *cur_batch;   /* child batch being scanned (mid-batch resume) */
    int64_t          cur_row;     /* next row to process in cur_batch */
} SortedAggState;

/* (Re)initialize the keys+aggs output builders after a flush-out. */
static void sagg_reset_builders(SortedAggState *st, GroupAggNode *ga) {
    for (int k = 0; k < ga->n_keys; k++)
        st->key_builders[k] = vec_builder_init(st->key_types[k]);
    for (int a = 0; a < ga->n_aggs; a++)
        st->agg_builders[a] = vec_builder_init(
            agg_output_type(ga->agg_specs[a].kind, st->agg_types[a]));
}

static SortedAggState *sagg_init(GroupAggNode *ga) {
    const VecSchema *child_schema = &ga->child->output_schema;
    SortedAggState *st = (SortedAggState *)calloc(1, sizeof(SortedAggState));
    if (!st) vectra_error("alloc failed for SortedAggState");

    st->key_indices = (int *)malloc((size_t)ga->n_keys * sizeof(int));
    st->key_types = (VecType *)malloc((size_t)ga->n_keys * sizeof(VecType));
    for (int k = 0; k < ga->n_keys; k++) {
        st->key_indices[k] = vec_schema_find_col(child_schema, ga->key_names[k]);
        if (st->key_indices[k] < 0)
            vectra_error("group_by: column not found: %s", ga->key_names[k]);
        st->key_types[k] = child_schema->col_types[st->key_indices[k]];
    }

    st->agg_col_indices = (int *)malloc((size_t)ga->n_aggs * sizeof(int));
    st->agg_types = (VecType *)malloc((size_t)ga->n_aggs * sizeof(VecType));
    for (int a = 0; a < ga->n_aggs; a++) {
        if (ga->agg_specs[a].kind == AGG_COUNT_STAR) {
            st->agg_col_indices[a] = -1;
            st->agg_types[a] = VEC_INT64;
        } else {
            st->agg_col_indices[a] = vec_schema_find_col(child_schema,
                ga->agg_specs[a].input_col);
            if (st->agg_col_indices[a] < 0)
                vectra_error("summarise: column not found: %s",
                             ga->agg_specs[a].input_col);
            st->agg_types[a] = child_schema->col_types[st->agg_col_indices[a]];
        }
    }

    st->key_builders = (VecArrayBuilder *)calloc(
        (size_t)ga->n_keys, sizeof(VecArrayBuilder));
    st->agg_builders = (VecArrayBuilder *)calloc(
        (size_t)ga->n_aggs, sizeof(VecArrayBuilder));
    sagg_reset_builders(st, ga);

    st->store_mem = agg_holistic_budget(ga);
    st->accums = (AggAccum *)malloc((size_t)ga->n_aggs * sizeof(AggAccum));
    for (int a = 0; a < ga->n_aggs; a++) {
        st->accums[a] = agg_accum_init(ga->agg_specs[a].kind, st->agg_types[a],
                                       ga->agg_specs[a].na_rm,
                                       st->store_mem, ga->temp_dir);
        agg_accum_ensure(&st->accums[a], 1);
    }

    st->snap = snap_create(ga->n_keys, st->key_types);
    st->inited = 1;
    return st;
}

static void sagg_free(SortedAggState *st, int n_keys, int n_aggs) {
    if (!st) return;
    if (st->cur_batch) { vec_batch_free(st->cur_batch); st->cur_batch = NULL; }
    for (int a = 0; a < n_aggs; a++)
        agg_accum_free(&st->accums[a]);
    free(st->accums);
    /* Builders that were never finished (e.g. abandoned mid-stream) still own
       their buffers; finishing frees them. A finished builder is already empty. */
    for (int k = 0; k < n_keys; k++) {
        VecArray a = vec_builder_finish(&st->key_builders[k]);
        vec_array_free(&a);
    }
    for (int a = 0; a < n_aggs; a++) {
        VecArray arr = vec_builder_finish(&st->agg_builders[a]);
        vec_array_free(&arr);
    }
    free(st->key_builders);
    free(st->agg_builders);
    free(st->key_indices);
    free(st->key_types);
    free(st->agg_col_indices);
    free(st->agg_types);
    snap_free(&st->snap);
    free(st);
}

/* Finish the current builders into a result batch and reset them for reuse. */
static VecBatch *sagg_emit(SortedAggState *st, GroupAggNode *ga) {
    int64_t n_groups = st->key_builders[0].length;
    int n_out = ga->n_keys + ga->n_aggs;
    VecBatch *result = vec_batch_alloc(n_out, n_groups);
    for (int k = 0; k < ga->n_keys; k++) {
        result->columns[k] = vec_builder_finish(&st->key_builders[k]);
        size_t kn_len = strlen(ga->key_names[k]);
        result->col_names[k] = (char *)malloc(kn_len + 1);
        memcpy(result->col_names[k], ga->key_names[k], kn_len + 1);
    }
    for (int a = 0; a < ga->n_aggs; a++) {
        result->columns[ga->n_keys + a] = vec_builder_finish(&st->agg_builders[a]);
        size_t on_len = strlen(ga->agg_specs[a].output_name);
        result->col_names[ga->n_keys + a] = (char *)malloc(on_len + 1);
        memcpy(result->col_names[ga->n_keys + a],
               ga->agg_specs[a].output_name, on_len + 1);
    }
    sagg_reset_builders(st, ga);   /* fresh builders; open group carries over */
    return result;
}

static VecBatch *sorted_agg_next_batch(GroupAggNode *ga) {
    SortedAggState *st = (SortedAggState *)ga->sagg;
    if (st == NULL) {
        st = sagg_init(ga);
        ga->sagg = st;
    }
    if (st->scan_done)
        return NULL;

    /* Linear scan of sorted input with mid-batch resume. The emit threshold is
       checked after every row (not just at child-batch boundaries) because the
       child sort can hand back one arbitrarily large batch; buffering a whole
       such batch of groups would defeat the bound. Completed groups sit in the
       builders; the open group's accumulator + snap carry over between calls. */
    while (1) {
        if (st->cur_batch == NULL) {
            st->cur_batch = ga->child->next_batch(ga->child);
            st->cur_row = 0;
            if (st->cur_batch == NULL) break;   /* child exhausted */
        }
        VecBatch *batch = st->cur_batch;
        int64_t n_rows = batch->n_rows;
        while (st->cur_row < n_rows) {
            int64_t row = st->cur_row;
            if (!snap_matches(&st->snap, batch, row, st->key_indices)) {
                if (st->snap.initialized)
                    flush_group(&st->snap, st->key_builders, ga->n_keys,
                                st->agg_builders, ga->n_aggs,
                                st->accums, st->agg_types, ga->agg_specs,
                                st->store_mem, ga->temp_dir);
                snap_update(&st->snap, batch, row, st->key_indices);
            }
            for (int a = 0; a < ga->n_aggs; a++) {
                if (st->agg_col_indices[a] >= 0)
                    agg_accum_feed(&st->accums[a], 0,
                                   &batch->columns[st->agg_col_indices[a]], row);
                else
                    agg_accum_feed(&st->accums[a], 0, NULL, 0);
            }
            st->cur_row++;
            /* The open group (the one just fed) is not yet flushed, so the
               builders hold only completed groups. Emit and resume here. */
            if (st->key_builders[0].length >= GROUP_AGG_EMIT)
                return sagg_emit(st, ga);
        }
        vec_batch_free(batch);
        st->cur_batch = NULL;
    }

    /* Child exhausted: flush the last open group and emit the tail. */
    if (st->snap.initialized)
        flush_group(&st->snap, st->key_builders, ga->n_keys,
                    st->agg_builders, ga->n_aggs,
                    st->accums, st->agg_types, ga->agg_specs,
                    st->store_mem, ga->temp_dir);
    st->scan_done = 1;
    return sagg_emit(st, ga);   /* may be an empty batch (0 groups) */
}

/* ================================================================== */
/*  GroupAggNode interface                                            */
/* ================================================================== */

static VecBatch *group_agg_next_batch(VecNode *self) {
    GroupAggNode *ga = (GroupAggNode *)self;
    /* Sorted path streams its output in bounded batches and signals completion
       by returning NULL itself (via SortedAggState.scan_done). */
    if (ga->use_sorted)
        return sorted_agg_next_batch(ga);
    /* Hash path (single group, n_keys == 0) is one-shot. */
    if (ga->done) return NULL;
    ga->done = 1;
    return hash_agg_next_batch(ga);
}

static void group_agg_free(VecNode *self) {
    GroupAggNode *ga = (GroupAggNode *)self;
    if (ga->sagg) {
        sagg_free((SortedAggState *)ga->sagg, ga->n_keys, ga->n_aggs);
        ga->sagg = NULL;
    }
    ga->child->free_node(ga->child);
    for (int k = 0; k < ga->n_keys; k++)
        free(ga->key_names[k]);
    free(ga->key_names);
    for (int a = 0; a < ga->n_aggs; a++) {
        free(ga->agg_specs[a].output_name);
        free(ga->agg_specs[a].input_col);
    }
    free(ga->agg_specs);
    free(ga->temp_dir);
    vec_schema_free(&ga->base.output_schema);
    free(ga);
}

GroupAggNode *group_agg_node_create(VecNode *child,
                                    int n_keys, char **key_names,
                                    int n_aggs, AggSpec *agg_specs,
                                    const char *temp_dir, int64_t mem_budget) {
    GroupAggNode *ga = (GroupAggNode *)calloc(1, sizeof(GroupAggNode));
    if (!ga) vectra_error("alloc failed for GroupAggNode");

    ga->mem_budget = mem_budget;
    if (temp_dir) {
        ga->temp_dir = (char *)malloc(strlen(temp_dir) + 1);
        strcpy(ga->temp_dir, temp_dir);
    }

    /* One budget for the whole node: the external sort's spill threshold and
       the per-group holistic (median / n_distinct) spill both derive from it. */
    int64_t sort_mem = mem_budget > 0 ? mem_budget : VECTRA_SORT_MEM_DEFAULT;

    /* If temp_dir provided, wrap child in a SortNode for spill-safe agg */
    if (temp_dir && n_keys > 0) {
        const VecSchema *cs = &child->output_schema;
        SortKey *sort_keys = (SortKey *)malloc((size_t)n_keys * sizeof(SortKey));
        for (int k = 0; k < n_keys; k++) {
            int idx = vec_schema_find_col(cs, key_names[k]);
            if (idx < 0)
                vectra_error("group_by: column not found: %s", key_names[k]);
            sort_keys[k].col_index = idx;
            sort_keys[k].descending = 0;
            sort_keys[k].na_last = 0;   /* cluster NA keys as one group */
        }
        SortNode *sn = sort_node_create(child, n_keys, sort_keys, temp_dir,
                                        sort_mem);
        child = (VecNode *)sn;
        ga->use_sorted = 1;
    }

    ga->child = child;
    ga->n_keys = n_keys;
    ga->key_names = key_names;
    ga->n_aggs = n_aggs;
    ga->agg_specs = agg_specs;
    ga->done = 0;

    /* Build output schema: key columns + agg columns */
    int n_out = n_keys + n_aggs;
    char **out_names = (char **)malloc((size_t)n_out * sizeof(char *));
    VecType *out_types = (VecType *)malloc((size_t)n_out * sizeof(VecType));

    const VecSchema *cs = &child->output_schema;
    for (int k = 0; k < n_keys; k++) {
        out_names[k] = key_names[k];
        int idx = vec_schema_find_col(cs, key_names[k]);
        out_types[k] = (idx >= 0) ? cs->col_types[idx] : VEC_DOUBLE;
    }
    for (int a = 0; a < n_aggs; a++) {
        out_names[n_keys + a] = agg_specs[a].output_name;
        VecType it = VEC_DOUBLE;
        if (agg_specs[a].kind != AGG_COUNT_STAR && agg_specs[a].input_col) {
            int ci = vec_schema_find_col(cs, agg_specs[a].input_col);
            if (ci >= 0) it = cs->col_types[ci];
        }
        out_types[n_keys + a] = agg_output_type(agg_specs[a].kind, it);
    }

    ga->base.output_schema = vec_schema_create(n_out, out_names, out_types);
    free(out_names);
    free(out_types);

    ga->base.next_batch = group_agg_next_batch;
    ga->base.kind = "GroupAggNode";
    ga->base.free_node = group_agg_free;

    return ga;
}
