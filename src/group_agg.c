#include "group_agg.h"
#include "hash.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* Key arena builder: stores one copy of each unique key combination */
typedef struct {
    int        n_keys;
    VecType   *key_types;
    /* Per-key growable arrays */
    int64_t    capacity;
    int64_t    length;
    VecArray  *arenas;     /* array per key column */
    /* String data builders */
    char     **str_data;
    int64_t   *str_data_len;
    int64_t   *str_data_cap;
} KeyArena;

static void arena_init(KeyArena *ka, int n_keys, VecType *key_types) {
    ka->n_keys = n_keys;
    ka->key_types = (VecType *)malloc((size_t)n_keys * sizeof(VecType));
    memcpy(ka->key_types, key_types, (size_t)n_keys * sizeof(VecType));
    ka->capacity = 64;
    ka->length = 0;
    ka->arenas = (VecArray *)calloc((size_t)n_keys, sizeof(VecArray));
    ka->str_data = (char **)calloc((size_t)n_keys, sizeof(char *));
    ka->str_data_len = (int64_t *)calloc((size_t)n_keys, sizeof(int64_t));
    ka->str_data_cap = (int64_t *)calloc((size_t)n_keys, sizeof(int64_t));

    for (int k = 0; k < n_keys; k++) {
        ka->arenas[k] = vec_array_alloc(key_types[k], ka->capacity);
        if (key_types[k] == VEC_STRING)
            ka->arenas[k].owns_data = 0; /* string data owned by ka->str_data */
    }
}

static void arena_ensure(KeyArena *ka, int64_t n) {
    if (n <= ka->capacity) return;
    int64_t new_cap = ka->capacity;
    while (new_cap < n) new_cap *= 2;

    for (int k = 0; k < ka->n_keys; k++) {
        VecArray old = ka->arenas[k];
        VecArray new_arr = vec_array_alloc(ka->key_types[k], new_cap);
        /* Copy validity */
        memcpy(new_arr.validity, old.validity, (size_t)vec_validity_bytes(old.length));
        /* Copy data */
        switch (ka->key_types[k]) {
        case VEC_INT64:
            memcpy(new_arr.buf.i64, old.buf.i64, (size_t)old.length * sizeof(int64_t));
            break;
        case VEC_DOUBLE:
            memcpy(new_arr.buf.dbl, old.buf.dbl, (size_t)old.length * sizeof(double));
            break;
        case VEC_BOOL:
            memcpy(new_arr.buf.bln, old.buf.bln, (size_t)old.length);
            break;
        case VEC_STRING:
            memcpy(new_arr.buf.str.offsets, old.buf.str.offsets,
                   (size_t)(old.length + 1) * sizeof(int64_t));
            /* String data is owned by ka->str_data[k], not the VecArray.
               old.owns_data == 0, so vec_array_free won't free the shared
               buffer. Point the new array at it for hash probing. */
            free(new_arr.buf.str.data); /* free the empty alloc from vec_array_alloc */
            new_arr.buf.str.data = ka->str_data[k];
            new_arr.buf.str.data_len = ka->str_data_len[k];
            new_arr.owns_data = 0; /* borrowed from ka->str_data[k] */
            assert(!old.owns_data && "arena string array must be borrowed");
            assert(ka->str_data_cap[k] >= ka->str_data_len[k]);
            /* After resize, borrowed pointer must be valid if groups exist */
            assert(ka->length == 0 || new_arr.buf.str.data != NULL);
            break;
        }
        new_arr.length = old.length;
        vec_array_free(&old);
        ka->arenas[k] = new_arr;
    }
    ka->capacity = new_cap;
}

static void arena_append_row(KeyArena *ka, const VecArray *keys, int64_t row) {
    int64_t pos = ka->length;
    arena_ensure(ka, pos + 1);

    for (int k = 0; k < ka->n_keys; k++) {
        VecArray *a = &ka->arenas[k];
        a->length = pos + 1;
        if (vec_array_is_valid(&keys[k], row)) {
            vec_array_set_valid(a, pos);
            switch (ka->key_types[k]) {
            case VEC_INT64:  a->buf.i64[pos] = keys[k].buf.i64[row]; break;
            case VEC_DOUBLE: a->buf.dbl[pos] = keys[k].buf.dbl[row]; break;
            case VEC_BOOL:   a->buf.bln[pos] = keys[k].buf.bln[row]; break;
            case VEC_STRING: {
                int64_t s = keys[k].buf.str.offsets[row];
                int64_t e = keys[k].buf.str.offsets[row + 1];
                int64_t slen = e - s;
                /* Ensure str_data capacity */
                int64_t needed = ka->str_data_len[k] + slen;
                if (needed > ka->str_data_cap[k]) {
                    int64_t nc = ka->str_data_cap[k] == 0 ? 256 : ka->str_data_cap[k];
                    while (nc < needed) nc *= 2;
                    ka->str_data[k] = (char *)realloc(ka->str_data[k], (size_t)nc);
                    assert(ka->str_data[k] != NULL && "arena string realloc failed");
                    ka->str_data_cap[k] = nc;
                }
                a->buf.str.offsets[pos] = ka->str_data_len[k];
                memcpy(ka->str_data[k] + ka->str_data_len[k],
                       keys[k].buf.str.data + s, (size_t)slen);
                ka->str_data_len[k] += slen;
                a->buf.str.offsets[pos + 1] = ka->str_data_len[k];
                /* Point arena string data to our buffer */
                a->buf.str.data = ka->str_data[k];
                a->buf.str.data_len = ka->str_data_len[k];
                break;
            }
            }
        } else {
            vec_array_set_null(a, pos);
            if (ka->key_types[k] == VEC_STRING) {
                /* Set offsets for NA string: zero-length */
                int64_t cur = ka->str_data_len[k];
                a->buf.str.offsets[pos] = cur;
                a->buf.str.offsets[pos + 1] = cur;
                a->buf.str.data = ka->str_data[k];
                a->buf.str.data_len = cur;
            }
        }
    }
    ka->length = pos + 1;
}

static void arena_free(KeyArena *ka) {
    for (int k = 0; k < ka->n_keys; k++) {
        /* vec_array_free skips str.data when owns_data == 0 */
        vec_array_free(&ka->arenas[k]);
        if (ka->key_types[k] == VEC_STRING) {
            free(ka->str_data[k]);
#ifndef NDEBUG
            /* Poison freed pointer so post-free dereference crashes fast */
            ka->str_data[k] = (char *)(uintptr_t)0xDEADBEEFDEADBEEFULL;
#endif
        }
    }
    free(ka->arenas);
    free(ka->key_types);
    free(ka->str_data);
    free(ka->str_data_len);
    free(ka->str_data_cap);
}

/* --- GroupAggNode --- */

static VecBatch *group_agg_next_batch(VecNode *self) {
    GroupAggNode *ga = (GroupAggNode *)self;
    if (ga->done) return NULL;
    ga->done = 1;

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

    /* Initialize accumulators */
    AggAccum *accums = (AggAccum *)malloc((size_t)ga->n_aggs * sizeof(AggAccum));
    for (int a = 0; a < ga->n_aggs; a++) {
        accums[a] = agg_accum_init(ga->agg_specs[a].kind,
                                    agg_types[a],
                                    ga->agg_specs[a].na_rm);
    }

    /* Initialize hash table and key arena */
    VecHashTable ht = vec_ht_create(64);
    KeyArena arena;
    arena_init(&arena, ga->n_keys, key_types);

    /* Consume all child batches (sel-aware: no compaction needed) */
    VecBatch *batch;
    while ((batch = ga->child->next_batch(ga->child)) != NULL) {
        /* Extract key columns as pointers (physical arrays) */
        VecArray *batch_keys = (VecArray *)malloc((size_t)ga->n_keys * sizeof(VecArray));
        for (int k = 0; k < ga->n_keys; k++)
            batch_keys[k] = batch->columns[key_indices[k]];

        int64_t n_logical = vec_batch_logical_rows(batch);
        for (int64_t li = 0; li < n_logical; li++) {
            int64_t r = vec_batch_physical_row(batch, li);

            /* Hash the key */
            uint64_t h = 0;
            for (int k = 0; k < ga->n_keys; k++) {
                uint64_t kh = vec_hash_value(&batch_keys[k], r);
                h = (k == 0) ? kh : vec_hash_combine(h, kh);
            }

            int was_new = 0;
            int64_t gid = vec_ht_find_or_insert(
                &ht, h, batch_keys, ga->n_keys, r,
                arena.arenas, arena.length, &was_new);

            if (was_new) {
                arena_append_row(&arena, batch_keys, r);
                /* Grow accumulators */
                for (int a = 0; a < ga->n_aggs; a++)
                    agg_accum_ensure(&accums[a], ht.n_groups);
            }

            /* Feed accumulators */
            for (int a = 0; a < ga->n_aggs; a++) {
                if (agg_col_indices[a] >= 0) {
                    agg_accum_feed(&accums[a], gid,
                                   &batch->columns[agg_col_indices[a]], r);
                } else {
                    /* count_star: just count */
                    agg_accum_feed(&accums[a], gid, NULL, 0);
                }
            }
        }

        free(batch_keys);
        vec_batch_free(batch);
    }

    /* Build result batch */
    int64_t n_groups = ht.n_groups;
    int n_out = ga->n_keys + ga->n_aggs;
    VecBatch *result = vec_batch_alloc(n_out, n_groups);

    /* Key columns from arena */
    for (int k = 0; k < ga->n_keys; k++) {
        VecArray *src = &arena.arenas[k];
        src->length = n_groups;
        if (key_types[k] == VEC_STRING) {
            /* Deep copy string data */
            VecArray arr = vec_array_alloc(VEC_STRING, n_groups);
            memcpy(arr.validity, src->validity, (size_t)vec_validity_bytes(n_groups));
            memcpy(arr.buf.str.offsets, src->buf.str.offsets,
                   (size_t)(n_groups + 1) * sizeof(int64_t));
            int64_t dlen = arena.str_data_len[k];
            free(arr.buf.str.data);
            arr.buf.str.data = (char *)malloc((size_t)(dlen > 0 ? dlen : 1));
            memcpy(arr.buf.str.data, arena.str_data[k], (size_t)dlen);
            arr.buf.str.data_len = dlen;
            result->columns[k] = arr;
        } else {
            VecArray *copy = vec_coerce(src, src->type);
            copy->length = n_groups;
            result->columns[k] = *copy;
            free(copy);
        }
        result->col_names[k] = (char *)malloc(strlen(ga->key_names[k]) + 1);
        strcpy(result->col_names[k], ga->key_names[k]);
    }

    /* Agg result columns */
    for (int a = 0; a < ga->n_aggs; a++) {
        result->columns[ga->n_keys + a] = agg_accum_finish(&accums[a]);
        result->col_names[ga->n_keys + a] = (char *)malloc(
            strlen(ga->agg_specs[a].output_name) + 1);
        strcpy(result->col_names[ga->n_keys + a], ga->agg_specs[a].output_name);
    }

    /* Cleanup */
    for (int a = 0; a < ga->n_aggs; a++)
        agg_accum_free(&accums[a]);
    free(accums);
    free(key_indices);
    free(key_types);
    free(agg_col_indices);
    free(agg_types);
    vec_ht_free(&ht);
    arena_free(&arena);

    return result;
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
                                    int n_aggs, AggSpec *agg_specs) {
    GroupAggNode *ga = (GroupAggNode *)calloc(1, sizeof(GroupAggNode));
    if (!ga) vectra_error("alloc failed for GroupAggNode");
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
        out_types[n_keys + a] = VEC_DOUBLE; /* all aggs return double */
    }

    ga->base.output_schema = vec_schema_create(n_out, out_names, out_types);
    free(out_names);
    free(out_types);

    ga->base.next_batch = group_agg_next_batch;
    ga->base.kind = "GroupAggNode";
    ga->base.free_node = group_agg_free;

    return ga;
}
