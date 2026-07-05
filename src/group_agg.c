#include "group_agg.h"
#include "hash.h"
#include "key_arena.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "coerce.h"
#include "builder.h"
#include "sort.h"
#include "error.h"
#include "vec_omp.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* ================================================================== */
/*  Hash-based aggregation (original path)                            */
/* ================================================================== */

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

    AggAccum *accums = (AggAccum *)malloc((size_t)ga->n_aggs * sizeof(AggAccum));
    for (int a = 0; a < ga->n_aggs; a++) {
        accums[a] = agg_accum_init(ga->agg_specs[a].kind,
                                    agg_types[a],
                                    ga->agg_specs[a].na_rm);
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

/* Snapshot of the current group's key values for boundary detection */
typedef struct {
    int       n_keys;
    VecType  *types;
    int64_t  *i64;
    double   *dbl;
    uint8_t  *bln;
    char     *str_data;
    int64_t  *str_offs;    /* n_keys + 1 entries */
    int64_t   str_cap;
    uint8_t  *valid;
    int       initialized;
} KeySnap;

static KeySnap snap_create(int n_keys, const VecType *types) {
    KeySnap s;
    memset(&s, 0, sizeof(s));
    s.n_keys = n_keys;
    s.types = (VecType *)malloc((size_t)n_keys * sizeof(VecType));
    memcpy(s.types, types, (size_t)n_keys * sizeof(VecType));
    s.i64  = (int64_t *)calloc((size_t)n_keys, sizeof(int64_t));
    s.dbl  = (double  *)calloc((size_t)n_keys, sizeof(double));
    s.bln  = (uint8_t *)calloc((size_t)n_keys, sizeof(uint8_t));
    s.str_offs = (int64_t *)calloc((size_t)(n_keys + 1), sizeof(int64_t));
    s.valid = (uint8_t *)calloc((size_t)n_keys, sizeof(uint8_t));
    return s;
}

static void snap_free(KeySnap *s) {
    free(s->types); free(s->i64); free(s->dbl); free(s->bln);
    free(s->str_data); free(s->str_offs); free(s->valid);
    memset(s, 0, sizeof(*s));
}

/* Check if row in batch matches the snapshot */
static int snap_matches(const KeySnap *s, const VecBatch *batch,
                        int64_t row, const int *key_indices) {
    if (!s->initialized) return 0;
    for (int k = 0; k < s->n_keys; k++) {
        const VecArray *col = &batch->columns[key_indices[k]];
        int cur_valid = vec_array_is_valid(col, row);
        if (cur_valid != s->valid[k]) return 0;
        if (!cur_valid) continue; /* both NA = equal */
        switch (s->types[k]) {
        case VEC_INT64:
            if (col->buf.i64[row] != s->i64[k]) return 0;
            break;
        case VEC_INT32:
            if ((int64_t)col->buf.i32[row] != s->i64[k]) return 0;
            break;
        case VEC_INT16:
            if ((int64_t)col->buf.i16[row] != s->i64[k]) return 0;
            break;
        case VEC_INT8:
            if ((int64_t)col->buf.i8[row] != s->i64[k]) return 0;
            break;
        case VEC_DOUBLE:
            if (col->buf.dbl[row] != s->dbl[k]) return 0;
            break;
        case VEC_BOOL:
            if (col->buf.bln[row] != s->bln[k]) return 0;
            break;
        case VEC_STRING: {
            int64_t cs = col->buf.str.offsets[row];
            int64_t ce = col->buf.str.offsets[row + 1];
            int64_t clen = ce - cs;
            int64_t slen = s->str_offs[k + 1] - s->str_offs[k];
            if (clen != slen) return 0;
            if (clen > 0 && s->str_data &&
                memcmp(col->buf.str.data + cs,
                       s->str_data + s->str_offs[k], (size_t)clen) != 0)
                return 0;
            break;
        }
        }
    }
    return 1;
}

/* Capture the current row's keys into the snapshot */
static void snap_update(KeySnap *s, const VecBatch *batch,
                        int64_t row, const int *key_indices) {
    s->initialized = 1;

    /* First pass: compute total string length */
    int64_t str_total = 0;
    for (int k = 0; k < s->n_keys; k++) {
        const VecArray *col = &batch->columns[key_indices[k]];
        s->valid[k] = (uint8_t)vec_array_is_valid(col, row);
        if (!s->valid[k]) continue;
        switch (s->types[k]) {
        case VEC_INT64:  s->i64[k] = col->buf.i64[row]; break;
        case VEC_INT32:  s->i64[k] = (int64_t)col->buf.i32[row]; break;
        case VEC_INT16:  s->i64[k] = (int64_t)col->buf.i16[row]; break;
        case VEC_INT8:   s->i64[k] = (int64_t)col->buf.i8[row]; break;
        case VEC_DOUBLE: s->dbl[k] = col->buf.dbl[row]; break;
        case VEC_BOOL:   s->bln[k] = col->buf.bln[row]; break;
        case VEC_STRING: {
            int64_t cs = col->buf.str.offsets[row];
            int64_t ce = col->buf.str.offsets[row + 1];
            str_total += ce - cs;
            break;
        }
        }
    }

    /* Ensure string buffer capacity */
    if (str_total > s->str_cap) {
        s->str_cap = str_total > 256 ? str_total * 2 : 256;
        s->str_data = (char *)realloc(s->str_data, (size_t)s->str_cap);
    }

    /* Second pass: copy string data */
    int64_t off = 0;
    for (int k = 0; k < s->n_keys; k++) {
        s->str_offs[k] = off;
        if (s->types[k] == VEC_STRING && s->valid[k]) {
            const VecArray *col = &batch->columns[key_indices[k]];
            int64_t cs = col->buf.str.offsets[row];
            int64_t ce = col->buf.str.offsets[row + 1];
            int64_t len = ce - cs;
            if (len > 0)
                memcpy(s->str_data + off, col->buf.str.data + cs, (size_t)len);
            off += len;
        }
    }
    s->str_offs[s->n_keys] = off;
}

/* Flush completed group: append key snapshot + agg results to builders */
static void flush_group(const KeySnap *snap,
                        VecArrayBuilder *key_builders, int n_keys,
                        VecArrayBuilder *agg_builders, int n_aggs,
                        AggAccum *accums, const VecType *agg_types,
                        const AggSpec *agg_specs) {
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
        /* Reinitialize for next group */
        accums[a] = agg_accum_init(agg_specs[a].kind, agg_types[a],
                                    agg_specs[a].na_rm);
        agg_accum_ensure(&accums[a], 1);
    }
}

static VecBatch *sorted_agg_next_batch(GroupAggNode *ga) {
    const VecSchema *child_schema = &ga->child->output_schema;

    /* Resolve key column indices */
    int *key_indices = (int *)malloc((size_t)ga->n_keys * sizeof(int));
    VecType *key_types = (VecType *)malloc((size_t)ga->n_keys * sizeof(VecType));
    for (int k = 0; k < ga->n_keys; k++) {
        key_indices[k] = vec_schema_find_col(child_schema, ga->key_names[k]);
        if (key_indices[k] < 0)
            vectra_error("group_by: column not found: %s", ga->key_names[k]);
        key_types[k] = child_schema->col_types[key_indices[k]];
    }

    /* Resolve agg input column indices */
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

    /* Output builders: keys + aggs */
    VecArrayBuilder *key_builders = (VecArrayBuilder *)calloc(
        (size_t)ga->n_keys, sizeof(VecArrayBuilder));
    for (int k = 0; k < ga->n_keys; k++)
        key_builders[k] = vec_builder_init(key_types[k]);

    VecArrayBuilder *agg_builders = (VecArrayBuilder *)calloc(
        (size_t)ga->n_aggs, sizeof(VecArrayBuilder));
    for (int a = 0; a < ga->n_aggs; a++)
        agg_builders[a] = vec_builder_init(VEC_DOUBLE); /* all aggs -> double */

    /* Accumulators for current group (always group_id = 0) */
    AggAccum *accums = (AggAccum *)malloc((size_t)ga->n_aggs * sizeof(AggAccum));
    for (int a = 0; a < ga->n_aggs; a++) {
        accums[a] = agg_accum_init(ga->agg_specs[a].kind,
                                    agg_types[a],
                                    ga->agg_specs[a].na_rm);
        agg_accum_ensure(&accums[a], 1);
    }

    KeySnap snap = snap_create(ga->n_keys, key_types);

    /* Linear scan of sorted input */
    VecBatch *batch;
    while ((batch = ga->child->next_batch(ga->child)) != NULL) {
        int64_t n_rows = batch->n_rows;

        for (int64_t row = 0; row < n_rows; row++) {
            if (!snap_matches(&snap, batch, row, key_indices)) {
                /* Group boundary */
                if (snap.initialized) {
                    flush_group(&snap, key_builders, ga->n_keys,
                                agg_builders, ga->n_aggs,
                                accums, agg_types, ga->agg_specs);
                }
                snap_update(&snap, batch, row, key_indices);
            }

            /* Feed accumulators (always group 0) */
            for (int a = 0; a < ga->n_aggs; a++) {
                if (agg_col_indices[a] >= 0) {
                    agg_accum_feed(&accums[a], 0,
                                   &batch->columns[agg_col_indices[a]], row);
                } else {
                    agg_accum_feed(&accums[a], 0, NULL, 0);
                }
            }
        }

        vec_batch_free(batch);
    }

    /* Flush the last group */
    if (snap.initialized) {
        flush_group(&snap, key_builders, ga->n_keys,
                    agg_builders, ga->n_aggs,
                    accums, agg_types, ga->agg_specs);
    }

    /* Build result batch */
    int64_t n_groups = key_builders[0].length;
    int n_out = ga->n_keys + ga->n_aggs;
    VecBatch *result = vec_batch_alloc(n_out, n_groups);

    for (int k = 0; k < ga->n_keys; k++) {
        result->columns[k] = vec_builder_finish(&key_builders[k]);
        size_t kn_len = strlen(ga->key_names[k]);
        result->col_names[k] = (char *)malloc(kn_len + 1);
        memcpy(result->col_names[k], ga->key_names[k], kn_len + 1);
    }
    for (int a = 0; a < ga->n_aggs; a++) {
        result->columns[ga->n_keys + a] = vec_builder_finish(&agg_builders[a]);
        size_t on_len = strlen(ga->agg_specs[a].output_name);
        result->col_names[ga->n_keys + a] = (char *)malloc(on_len + 1);
        memcpy(result->col_names[ga->n_keys + a], ga->agg_specs[a].output_name, on_len + 1);
    }

    /* Cleanup */
    for (int a = 0; a < ga->n_aggs; a++)
        agg_accum_free(&accums[a]);
    free(accums);
    free(key_builders);
    free(agg_builders);
    free(key_indices);
    free(key_types);
    free(agg_col_indices);
    free(agg_types);
    snap_free(&snap);

    return result;
}

/* ================================================================== */
/*  GroupAggNode interface                                            */
/* ================================================================== */

static VecBatch *group_agg_next_batch(VecNode *self) {
    GroupAggNode *ga = (GroupAggNode *)self;
    if (ga->done) return NULL;
    ga->done = 1;

    if (ga->use_sorted)
        return sorted_agg_next_batch(ga);
    else
        return hash_agg_next_batch(ga);
}

static void group_agg_free(VecNode *self) {
    GroupAggNode *ga = (GroupAggNode *)self;
    ga->child->free_node(ga->child);
    for (int k = 0; k < ga->n_keys; k++)
        free(ga->key_names[k]);
    free(ga->key_names);
    for (int a = 0; a < ga->n_aggs; a++) {
        free(ga->agg_specs[a].output_name);
        free(ga->agg_specs[a].input_col);
    }
    free(ga->agg_specs);
    vec_schema_free(&ga->base.output_schema);
    free(ga);
}

GroupAggNode *group_agg_node_create(VecNode *child,
                                    int n_keys, char **key_names,
                                    int n_aggs, AggSpec *agg_specs,
                                    const char *temp_dir) {
    GroupAggNode *ga = (GroupAggNode *)calloc(1, sizeof(GroupAggNode));
    if (!ga) vectra_error("alloc failed for GroupAggNode");

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
        }
        SortNode *sn = sort_node_create(child, n_keys, sort_keys, temp_dir,
                                        VECTRA_SORT_MEM_DEFAULT);
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
        out_types[n_keys + a] = VEC_DOUBLE;
    }

    ga->base.output_schema = vec_schema_create(n_out, out_names, out_types);
    free(out_names);
    free(out_types);

    ga->base.next_batch = group_agg_next_batch;
    ga->base.kind = "GroupAggNode";
    ga->base.free_node = group_agg_free;

    return ga;
}
