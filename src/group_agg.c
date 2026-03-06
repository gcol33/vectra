#include "group_agg.h"
#include "hash.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "coerce.h"
#include "builder.h"
#include "sort.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* ================================================================== */
/*  Key arena builder: stores one copy of each unique key combination */
/* ================================================================== */

typedef struct {
    int        n_keys;
    VecType   *key_types;
    int64_t    capacity;
    int64_t    length;
    VecArray  *arenas;
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
            ka->arenas[k].owns_data = 0;
    }
}

static void arena_ensure(KeyArena *ka, int64_t n) {
    if (n <= ka->capacity) return;
    int64_t new_cap = ka->capacity;
    while (new_cap < n) new_cap *= 2;

    for (int k = 0; k < ka->n_keys; k++) {
        VecArray old = ka->arenas[k];
        VecArray new_arr = vec_array_alloc(ka->key_types[k], new_cap);
        memcpy(new_arr.validity, old.validity, (size_t)vec_validity_bytes(old.length));
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
            free(new_arr.buf.str.data);
            new_arr.buf.str.data = ka->str_data[k];
            new_arr.buf.str.data_len = ka->str_data_len[k];
            new_arr.owns_data = 0;
            assert(!old.owns_data && "arena string array must be borrowed");
            assert(ka->str_data_cap[k] >= ka->str_data_len[k]);
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
                a->buf.str.data = ka->str_data[k];
                a->buf.str.data_len = ka->str_data_len[k];
                break;
            }
            }
        } else {
            vec_array_set_null(a, pos);
            if (ka->key_types[k] == VEC_STRING) {
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
        vec_array_free(&ka->arenas[k]);
        if (ka->key_types[k] == VEC_STRING) {
            free(ka->str_data[k]);
#ifndef NDEBUG
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
    arena_init(&arena, ga->n_keys, key_types);

    VecBatch *batch;
    while ((batch = ga->child->next_batch(ga->child)) != NULL) {
        VecArray *batch_keys = (VecArray *)malloc((size_t)ga->n_keys * sizeof(VecArray));
        for (int k = 0; k < ga->n_keys; k++)
            batch_keys[k] = batch->columns[key_indices[k]];

        int64_t n_logical = vec_batch_logical_rows(batch);
        for (int64_t li = 0; li < n_logical; li++) {
            int64_t r = vec_batch_physical_row(batch, li);

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

    for (int a = 0; a < ga->n_aggs; a++) {
        result->columns[ga->n_keys + a] = agg_accum_finish(&accums[a]);
        result->col_names[ga->n_keys + a] = (char *)malloc(
            strlen(ga->agg_specs[a].output_name) + 1);
        strcpy(result->col_names[ga->n_keys + a], ga->agg_specs[a].output_name);
    }

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
            if (clen > 0 &&
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
        result->col_names[k] = (char *)malloc(strlen(ga->key_names[k]) + 1);
        strcpy(result->col_names[k], ga->key_names[k]);
    }
    for (int a = 0; a < ga->n_aggs; a++) {
        result->columns[ga->n_keys + a] = vec_builder_finish(&agg_builders[a]);
        result->col_names[ga->n_keys + a] = (char *)malloc(
            strlen(ga->agg_specs[a].output_name) + 1);
        strcpy(result->col_names[ga->n_keys + a], ga->agg_specs[a].output_name);
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
        SortNode *sn = sort_node_create(child, n_keys, sort_keys, temp_dir);
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
