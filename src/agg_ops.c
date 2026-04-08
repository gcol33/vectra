#include "agg_ops.h"
#include "array.h"
#include "error.h"
#include <R.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>
#include <math.h>

/* Read any integer column value as int64 (handles narrow types) */
static inline int64_t agg_get_i64(const VecArray *col, int64_t row) {
    switch (col->type) {
    case VEC_INT64:  return col->buf.i64[row];
    case VEC_INT32:  return (int64_t)col->buf.i32[row];
    case VEC_INT16:  return (int64_t)col->buf.i16[row];
    case VEC_INT8:   return (int64_t)col->buf.i8[row];
    default:         return 0;
    }
}

/*
 * NA semantics (matches R/dplyr):
 *
 * Without na.rm (default):
 *   sum/mean/min/max: any NA in group -> result is NA
 *   n():     counts all rows (NAs included)
 *   count(): counts non-NA values only (not yet exposed)
 *
 * With na.rm = TRUE:
 *   sum:  all-NA group -> 0       (R: sum(c(NA,NA), na.rm=TRUE) == 0)
 *   mean: all-NA group -> NaN     (R: mean(c(NA,NA), na.rm=TRUE) is NaN)
 *   min:  all-NA group -> Inf     (R: min(c(NA,NA), na.rm=TRUE) == Inf)
 *   max:  all-NA group -> -Inf    (R: max(c(NA,NA), na.rm=TRUE) == -Inf)
 */

AggAccum agg_accum_init(AggKind kind, VecType input_type, int na_rm) {
    AggAccum acc;
    memset(&acc, 0, sizeof(acc));
    acc.kind = kind;
    /* Normalize narrow int types to VEC_INT64 for accumulation.
       The feed function uses agg_get_i64() to read any int width. */
    acc.input_type = (input_type == VEC_INT8 || input_type == VEC_INT16 ||
                      input_type == VEC_INT32) ? VEC_INT64 : input_type;
    acc.na_rm = na_rm;
    return acc;
}

static void grow_has_na(AggAccum *acc, int64_t old_cap, int64_t new_cap) {
    acc->has_na = (int *)realloc(acc->has_na, (size_t)new_cap * sizeof(int));
    if (!acc->has_na) vectra_error("agg alloc failed");
    memset(acc->has_na + old_cap, 0, (size_t)(new_cap - old_cap) * sizeof(int));
}

void agg_accum_ensure(AggAccum *acc, int64_t n_groups) {
    if (n_groups <= acc->capacity) {
        acc->n_groups = n_groups;
        return;
    }
    int64_t old_cap = acc->capacity;
    int64_t new_cap = old_cap == 0 ? 64 : old_cap;
    while (new_cap < n_groups) new_cap *= 2;

    switch (acc->kind) {
    case AGG_COUNT:
    case AGG_COUNT_STAR:
        acc->count = (int64_t *)realloc(acc->count, (size_t)new_cap * sizeof(int64_t));
        if (!acc->count) vectra_error("agg alloc failed");
        memset(acc->count + old_cap, 0, (size_t)(new_cap - old_cap) * sizeof(int64_t));
        break;
    case AGG_SUM:
    case AGG_MEAN:
        acc->count = (int64_t *)realloc(acc->count, (size_t)new_cap * sizeof(int64_t));
        if (!acc->count) vectra_error("agg alloc failed");
        memset(acc->count + old_cap, 0, (size_t)(new_cap - old_cap) * sizeof(int64_t));
        grow_has_na(acc, old_cap, new_cap);
        if (acc->input_type == VEC_DOUBLE || acc->input_type == VEC_BOOL) {
            acc->sum_dbl = (double *)realloc(acc->sum_dbl, (size_t)new_cap * sizeof(double));
            if (!acc->sum_dbl) vectra_error("agg alloc failed");
            for (int64_t i = old_cap; i < new_cap; i++) acc->sum_dbl[i] = 0.0;
        } else {
            acc->sum_i64 = (int64_t *)realloc(acc->sum_i64, (size_t)new_cap * sizeof(int64_t));
            if (!acc->sum_i64) vectra_error("agg alloc failed");
            memset(acc->sum_i64 + old_cap, 0, (size_t)(new_cap - old_cap) * sizeof(int64_t));
        }
        break;
    case AGG_MIN:
    case AGG_MAX:
        acc->has_value = (int *)realloc(acc->has_value, (size_t)new_cap * sizeof(int));
        if (!acc->has_value) vectra_error("agg alloc failed");
        memset(acc->has_value + old_cap, 0, (size_t)(new_cap - old_cap) * sizeof(int));
        grow_has_na(acc, old_cap, new_cap);
        if (acc->input_type == VEC_DOUBLE) {
            acc->min_dbl = (double *)realloc(acc->min_dbl, (size_t)new_cap * sizeof(double));
            acc->max_dbl = (double *)realloc(acc->max_dbl, (size_t)new_cap * sizeof(double));
            if (!acc->min_dbl || !acc->max_dbl) vectra_error("agg alloc failed");
        } else {
            acc->min_i64 = (int64_t *)realloc(acc->min_i64, (size_t)new_cap * sizeof(int64_t));
            acc->max_i64 = (int64_t *)realloc(acc->max_i64, (size_t)new_cap * sizeof(int64_t));
            if (!acc->min_i64 || !acc->max_i64) vectra_error("agg alloc failed");
        }
        break;
    case AGG_VAR:
    case AGG_SD:
        acc->count = (int64_t *)realloc(acc->count, (size_t)new_cap * sizeof(int64_t));
        if (!acc->count) vectra_error("agg alloc failed");
        memset(acc->count + old_cap, 0, (size_t)(new_cap - old_cap) * sizeof(int64_t));
        acc->sum_dbl = (double *)realloc(acc->sum_dbl, (size_t)new_cap * sizeof(double));
        if (!acc->sum_dbl) vectra_error("agg alloc failed");
        for (int64_t i = old_cap; i < new_cap; i++) acc->sum_dbl[i] = 0.0;
        acc->m2 = (double *)realloc(acc->m2, (size_t)new_cap * sizeof(double));
        if (!acc->m2) vectra_error("agg alloc failed");
        for (int64_t i = old_cap; i < new_cap; i++) acc->m2[i] = 0.0;
        grow_has_na(acc, old_cap, new_cap);
        break;
    case AGG_FIRST:
        acc->has_first = (int *)realloc(acc->has_first, (size_t)new_cap * sizeof(int));
        if (!acc->has_first) vectra_error("agg alloc failed");
        memset(acc->has_first + old_cap, 0, (size_t)(new_cap - old_cap) * sizeof(int));
        grow_has_na(acc, old_cap, new_cap);
        if (acc->input_type == VEC_INT64) {
            acc->first_i64 = (int64_t *)realloc(acc->first_i64, (size_t)new_cap * sizeof(int64_t));
            if (!acc->first_i64) vectra_error("agg alloc failed");
        } else {
            acc->first_dbl = (double *)realloc(acc->first_dbl, (size_t)new_cap * sizeof(double));
            if (!acc->first_dbl) vectra_error("agg alloc failed");
        }
        break;
    case AGG_LAST:
        acc->has_value = (int *)realloc(acc->has_value, (size_t)new_cap * sizeof(int));
        if (!acc->has_value) vectra_error("agg alloc failed");
        memset(acc->has_value + old_cap, 0, (size_t)(new_cap - old_cap) * sizeof(int));
        grow_has_na(acc, old_cap, new_cap);
        if (acc->input_type == VEC_INT64) {
            acc->last_i64 = (int64_t *)realloc(acc->last_i64, (size_t)new_cap * sizeof(int64_t));
            if (!acc->last_i64) vectra_error("agg alloc failed");
        } else {
            acc->last_dbl = (double *)realloc(acc->last_dbl, (size_t)new_cap * sizeof(double));
            if (!acc->last_dbl) vectra_error("agg alloc failed");
        }
        break;
    case AGG_ANY:
        acc->has_value = (int *)realloc(acc->has_value, (size_t)new_cap * sizeof(int));
        if (!acc->has_value) vectra_error("agg alloc failed");
        memset(acc->has_value + old_cap, 0, (size_t)(new_cap - old_cap) * sizeof(int));
        grow_has_na(acc, old_cap, new_cap);
        break;
    case AGG_ALL:
        acc->has_value = (int *)realloc(acc->has_value, (size_t)new_cap * sizeof(int));
        if (!acc->has_value) vectra_error("agg alloc failed");
        for (int64_t i = old_cap; i < new_cap; i++) acc->has_value[i] = 1;
        grow_has_na(acc, old_cap, new_cap);
        break;
    case AGG_N_DISTINCT:
        acc->nd_slots = (uint64_t **)realloc(acc->nd_slots, (size_t)new_cap * sizeof(uint64_t *));
        acc->nd_size = (int64_t *)realloc(acc->nd_size, (size_t)new_cap * sizeof(int64_t));
        acc->nd_count = (int64_t *)realloc(acc->nd_count, (size_t)new_cap * sizeof(int64_t));
        if (!acc->nd_slots || !acc->nd_size || !acc->nd_count) vectra_error("agg alloc failed");
        for (int64_t i = old_cap; i < new_cap; i++) {
            acc->nd_slots[i] = NULL;
            acc->nd_size[i] = 0;
            acc->nd_count[i] = 0;
        }
        grow_has_na(acc, old_cap, new_cap);
        break;
    case AGG_MEDIAN:
        acc->med_vals = (double **)realloc(acc->med_vals, (size_t)new_cap * sizeof(double *));
        acc->med_count = (int64_t *)realloc(acc->med_count, (size_t)new_cap * sizeof(int64_t));
        acc->med_cap = (int64_t *)realloc(acc->med_cap, (size_t)new_cap * sizeof(int64_t));
        if (!acc->med_vals || !acc->med_count || !acc->med_cap) vectra_error("agg alloc failed");
        for (int64_t i = old_cap; i < new_cap; i++) {
            acc->med_vals[i] = NULL;
            acc->med_count[i] = 0;
            acc->med_cap[i] = 0;
        }
        grow_has_na(acc, old_cap, new_cap);
        break;
    }
    acc->capacity = new_cap;
    acc->n_groups = n_groups;
}

/* --- n_distinct hash set helpers --- */

/* Sentinel: 0 means empty slot. We map hash 0 -> 1 to avoid collision. */
static uint64_t nd_hash_val_i64(int64_t v) {
    uint64_t h = (uint64_t)v * 0x9E3779B97F4A7C15ULL;
    h ^= h >> 33; h *= 0xFF51AFD7ED558CCDULL; h ^= h >> 33;
    return h == 0 ? 1 : h;
}

static uint64_t nd_hash_val_dbl(double v) {
    uint64_t bits;
    memcpy(&bits, &v, sizeof(bits));
    return nd_hash_val_i64((int64_t)bits);
}

static uint64_t nd_hash_val_str(const char *s, int64_t len) {
    /* FNV-1a */
    uint64_t h = 0xCBF29CE484222325ULL;
    for (int64_t i = 0; i < len; i++) {
        h ^= (uint64_t)(unsigned char)s[i];
        h *= 0x100000001B3ULL;
    }
    return h == 0 ? 1 : h;
}

/* Insert hash into group's set. Returns 1 if new, 0 if already present. */
static int nd_insert(AggAccum *acc, int64_t g, uint64_t h) {
    /* Lazy init: start with 16 slots */
    if (acc->nd_slots[g] == NULL) {
        acc->nd_size[g] = 16;
        acc->nd_slots[g] = (uint64_t *)calloc(16, sizeof(uint64_t));
        if (!acc->nd_slots[g]) vectra_error("n_distinct alloc failed");
    }
    /* Grow at 70% load */
    if (acc->nd_count[g] * 10 >= acc->nd_size[g] * 7) {
        int64_t new_sz = acc->nd_size[g] * 2;
        uint64_t *new_slots = (uint64_t *)calloc((size_t)new_sz, sizeof(uint64_t));
        if (!new_slots) vectra_error("n_distinct alloc failed");
        /* Rehash */
        for (int64_t i = 0; i < acc->nd_size[g]; i++) {
            if (acc->nd_slots[g][i] != 0) {
                uint64_t slot = acc->nd_slots[g][i] & ((uint64_t)new_sz - 1);
                while (new_slots[slot] != 0) slot = (slot + 1) & ((uint64_t)new_sz - 1);
                new_slots[slot] = acc->nd_slots[g][i];
            }
        }
        free(acc->nd_slots[g]);
        acc->nd_slots[g] = new_slots;
        acc->nd_size[g] = new_sz;
    }
    uint64_t mask = (uint64_t)(acc->nd_size[g] - 1);
    uint64_t slot = h & mask;
    while (acc->nd_slots[g][slot] != 0) {
        if (acc->nd_slots[g][slot] == h) return 0; /* already present */
        slot = (slot + 1) & mask;
    }
    acc->nd_slots[g][slot] = h;
    acc->nd_count[g]++;
    return 1;
}

/* --- median value array helper --- */

static void med_append(AggAccum *acc, int64_t g, double val) {
    if (acc->med_count[g] >= acc->med_cap[g]) {
        int64_t new_cap = acc->med_cap[g] == 0 ? 16 : acc->med_cap[g] * 2;
        acc->med_vals[g] = (double *)realloc(acc->med_vals[g], (size_t)new_cap * sizeof(double));
        if (!acc->med_vals[g]) vectra_error("median alloc failed");
        acc->med_cap[g] = new_cap;
    }
    acc->med_vals[g][acc->med_count[g]++] = val;
}

static int dbl_cmp(const void *a, const void *b) {
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

/* Insertion sort for small arrays — faster than qsort overhead for n < 64 */
static void dbl_insertion_sort(double *arr, int64_t n) {
    for (int64_t i = 1; i < n; i++) {
        double key = arr[i];
        int64_t j = i - 1;
        while (j >= 0 && arr[j] > key) {
            arr[j + 1] = arr[j];
            j--;
        }
        arr[j + 1] = key;
    }
}

void agg_accum_feed(AggAccum *acc, int64_t group_id,
                    const VecArray *col, int64_t row) {
    int is_valid = col ? vec_array_is_valid(col, row) : 0;

    switch (acc->kind) {
    case AGG_COUNT_STAR:
        acc->count[group_id]++;
        break;
    case AGG_COUNT:
        if (is_valid) acc->count[group_id]++;
        break;
    case AGG_SUM:
    case AGG_MEAN:
        if (!is_valid) {
            if (!acc->na_rm) acc->has_na[group_id] = 1;
            break;
        }
        acc->count[group_id]++;
        if (acc->input_type == VEC_DOUBLE) {
            acc->sum_dbl[group_id] += col->buf.dbl[row];
        } else if (acc->input_type == VEC_INT64) {
            acc->sum_i64[group_id] += agg_get_i64(col, row);
        } else if (acc->input_type == VEC_BOOL) {
            acc->sum_dbl[group_id] += (double)col->buf.bln[row];
        }
        break;
    case AGG_MIN:
        if (!is_valid) {
            if (!acc->na_rm) acc->has_na[group_id] = 1;
            break;
        }
        if (acc->input_type == VEC_DOUBLE) {
            double v = col->buf.dbl[row];
            if (!acc->has_value[group_id] || v < acc->min_dbl[group_id]) {
                acc->min_dbl[group_id] = v;
                acc->has_value[group_id] = 1;
            }
        } else if (acc->input_type == VEC_INT64) {
            int64_t v = agg_get_i64(col, row);
            if (!acc->has_value[group_id] || v < acc->min_i64[group_id]) {
                acc->min_i64[group_id] = v;
                acc->has_value[group_id] = 1;
            }
        }
        break;
    case AGG_MAX:
        if (!is_valid) {
            if (!acc->na_rm) acc->has_na[group_id] = 1;
            break;
        }
        if (acc->input_type == VEC_DOUBLE) {
            double v = col->buf.dbl[row];
            if (!acc->has_value[group_id] || v > acc->max_dbl[group_id]) {
                acc->max_dbl[group_id] = v;
                acc->has_value[group_id] = 1;
            }
        } else if (acc->input_type == VEC_INT64) {
            int64_t v = agg_get_i64(col, row);
            if (!acc->has_value[group_id] || v > acc->max_i64[group_id]) {
                acc->max_i64[group_id] = v;
                acc->has_value[group_id] = 1;
            }
        }
        break;
    case AGG_VAR:
    case AGG_SD:
        if (!is_valid) {
            if (!acc->na_rm) acc->has_na[group_id] = 1;
            break;
        }
        acc->count[group_id]++;
        {
            double val;
            if (acc->input_type == VEC_DOUBLE)
                val = col->buf.dbl[row];
            else if (acc->input_type == VEC_INT64)
                val = (double)agg_get_i64(col, row);
            else
                val = (double)col->buf.bln[row];
            /* Welford's online algorithm */
            double delta = val - acc->sum_dbl[group_id];  /* sum_dbl holds mean */
            acc->sum_dbl[group_id] += delta / (double)acc->count[group_id];
            double delta2 = val - acc->sum_dbl[group_id];
            acc->m2[group_id] += delta * delta2;
        }
        break;
    case AGG_FIRST:
        if (!is_valid) {
            if (!acc->na_rm) acc->has_na[group_id] = 1;
            break;
        }
        if (!acc->has_first[group_id]) {
            acc->has_first[group_id] = 1;
            if (acc->input_type == VEC_DOUBLE)
                acc->first_dbl[group_id] = col->buf.dbl[row];
            else if (acc->input_type == VEC_INT64)
                acc->first_i64[group_id] = agg_get_i64(col, row);
            else if (acc->input_type == VEC_BOOL)
                acc->first_dbl[group_id] = (double)col->buf.bln[row];
        }
        break;
    case AGG_LAST:
        if (!is_valid) {
            if (!acc->na_rm) acc->has_na[group_id] = 1;
            break;
        }
        acc->has_value[group_id] = 1;
        if (acc->input_type == VEC_DOUBLE)
            acc->last_dbl[group_id] = col->buf.dbl[row];
        else if (acc->input_type == VEC_INT64)
            acc->last_i64[group_id] = agg_get_i64(col, row);
        else if (acc->input_type == VEC_BOOL)
            acc->last_dbl[group_id] = (double)col->buf.bln[row];
        break;
    case AGG_ANY:
        if (!is_valid) {
            if (!acc->na_rm) acc->has_na[group_id] = 1;
            break;
        }
        if (acc->input_type == VEC_BOOL) {
            if (col->buf.bln[row]) acc->has_value[group_id] = 1;
        } else if (acc->input_type == VEC_DOUBLE) {
            if (col->buf.dbl[row] != 0.0) acc->has_value[group_id] = 1;
        } else if (acc->input_type == VEC_INT64) {
            if (agg_get_i64(col, row) != 0) acc->has_value[group_id] = 1;
        }
        break;
    case AGG_ALL:
        if (!is_valid) {
            if (!acc->na_rm) acc->has_na[group_id] = 1;
            break;
        }
        if (acc->input_type == VEC_BOOL) {
            if (!col->buf.bln[row]) acc->has_value[group_id] = 0;
        } else if (acc->input_type == VEC_DOUBLE) {
            if (col->buf.dbl[row] == 0.0) acc->has_value[group_id] = 0;
        } else if (acc->input_type == VEC_INT64) {
            if (agg_get_i64(col, row) == 0) acc->has_value[group_id] = 0;
        }
        break;
    case AGG_N_DISTINCT:
        if (!is_valid) break; /* NAs are not counted as distinct values */
        {
            uint64_t h;
            if (acc->input_type == VEC_INT64)
                h = nd_hash_val_i64(agg_get_i64(col, row));
            else if (acc->input_type == VEC_DOUBLE)
                h = nd_hash_val_dbl(col->buf.dbl[row]);
            else if (acc->input_type == VEC_STRING) {
                int64_t so = col->buf.str.offsets[row];
                int64_t slen = col->buf.str.offsets[row+1] - so;
                h = nd_hash_val_str(col->buf.str.data + so, slen);
            } else if (acc->input_type == VEC_BOOL)
                h = nd_hash_val_i64((int64_t)col->buf.bln[row]);
            else break;
            nd_insert(acc, group_id, h);
        }
        break;
    case AGG_MEDIAN:
        if (!is_valid) {
            if (!acc->na_rm) acc->has_na[group_id] = 1;
            break;
        }
        {
            double val;
            if (acc->input_type == VEC_DOUBLE) val = col->buf.dbl[row];
            else if (acc->input_type == VEC_INT64) val = (double)agg_get_i64(col, row);
            else if (acc->input_type == VEC_BOOL) val = (double)col->buf.bln[row];
            else break;
            med_append(acc, group_id, val);
        }
        break;
    }
}

VecArray agg_accum_finish(AggAccum *acc) {
    int64_t n = acc->n_groups;

    switch (acc->kind) {
    case AGG_COUNT:
    case AGG_COUNT_STAR: {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n);
        vec_array_set_all_valid(&arr);
        for (int64_t i = 0; i < n; i++)
            arr.buf.dbl[i] = (double)acc->count[i];
        return arr;
    }
    case AGG_SUM: {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n);
        for (int64_t i = 0; i < n; i++) {
            if (acc->has_na && acc->has_na[i]) {
                /* NA poisons the group */
                vec_array_set_null(&arr, i);
            } else {
                vec_array_set_valid(&arr, i);
                /* na_rm=TRUE + all-NA -> count==0, sum==0 (R semantics) */
                if (acc->input_type == VEC_DOUBLE || acc->input_type == VEC_BOOL)
                    arr.buf.dbl[i] = acc->sum_dbl[i];
                else
                    arr.buf.dbl[i] = (double)acc->sum_i64[i];
            }
        }
        return arr;
    }
    case AGG_MEAN: {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n);
        for (int64_t i = 0; i < n; i++) {
            if (acc->has_na && acc->has_na[i]) {
                vec_array_set_null(&arr, i);
            } else if (acc->count[i] == 0) {
                /* na_rm=TRUE + all-NA -> NaN (R semantics: 0/0) */
                vec_array_set_valid(&arr, i);
                arr.buf.dbl[i] = R_NaN;
            } else {
                vec_array_set_valid(&arr, i);
                double sum;
                if (acc->input_type == VEC_INT64)
                    sum = (double)acc->sum_i64[i];
                else
                    sum = acc->sum_dbl[i];
                arr.buf.dbl[i] = sum / (double)acc->count[i];
            }
        }
        return arr;
    }
    case AGG_MIN: {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n);
        for (int64_t i = 0; i < n; i++) {
            if (acc->has_na && acc->has_na[i]) {
                vec_array_set_null(&arr, i);
            } else if (!acc->has_value[i]) {
                /* na_rm=TRUE + all-NA -> Inf (R semantics) */
                vec_array_set_valid(&arr, i);
                arr.buf.dbl[i] = R_PosInf;
                Rf_warning("no non-missing arguments to min; returning Inf");
            } else {
                vec_array_set_valid(&arr, i);
                if (acc->input_type == VEC_DOUBLE)
                    arr.buf.dbl[i] = acc->min_dbl[i];
                else
                    arr.buf.dbl[i] = (double)acc->min_i64[i];
            }
        }
        return arr;
    }
    case AGG_MAX: {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n);
        for (int64_t i = 0; i < n; i++) {
            if (acc->has_na && acc->has_na[i]) {
                vec_array_set_null(&arr, i);
            } else if (!acc->has_value[i]) {
                /* na_rm=TRUE + all-NA -> -Inf (R semantics) */
                vec_array_set_valid(&arr, i);
                arr.buf.dbl[i] = R_NegInf;
                Rf_warning("no non-missing arguments to max; returning -Inf");
            } else {
                vec_array_set_valid(&arr, i);
                if (acc->input_type == VEC_DOUBLE)
                    arr.buf.dbl[i] = acc->max_dbl[i];
                else
                    arr.buf.dbl[i] = (double)acc->max_i64[i];
            }
        }
        return arr;
    }
    case AGG_VAR: {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n);
        for (int64_t i = 0; i < n; i++) {
            if (acc->has_na && acc->has_na[i]) {
                vec_array_set_null(&arr, i);
            } else if (acc->count[i] < 2) {
                vec_array_set_null(&arr, i);
            } else {
                vec_array_set_valid(&arr, i);
                arr.buf.dbl[i] = acc->m2[i] / (double)(acc->count[i] - 1);
            }
        }
        return arr;
    }
    case AGG_SD: {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n);
        for (int64_t i = 0; i < n; i++) {
            if (acc->has_na && acc->has_na[i]) {
                vec_array_set_null(&arr, i);
            } else if (acc->count[i] < 2) {
                vec_array_set_null(&arr, i);
            } else {
                vec_array_set_valid(&arr, i);
                arr.buf.dbl[i] = sqrt(acc->m2[i] / (double)(acc->count[i] - 1));
            }
        }
        return arr;
    }
    case AGG_FIRST: {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n);
        for (int64_t i = 0; i < n; i++) {
            if ((acc->has_na && acc->has_na[i]) || !acc->has_first[i]) {
                vec_array_set_null(&arr, i);
            } else {
                vec_array_set_valid(&arr, i);
                if (acc->input_type == VEC_INT64)
                    arr.buf.dbl[i] = (double)acc->first_i64[i];
                else
                    arr.buf.dbl[i] = acc->first_dbl[i];
            }
        }
        return arr;
    }
    case AGG_LAST: {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n);
        for (int64_t i = 0; i < n; i++) {
            if ((acc->has_na && acc->has_na[i]) || !acc->has_value[i]) {
                vec_array_set_null(&arr, i);
            } else {
                vec_array_set_valid(&arr, i);
                if (acc->input_type == VEC_INT64)
                    arr.buf.dbl[i] = (double)acc->last_i64[i];
                else
                    arr.buf.dbl[i] = acc->last_dbl[i];
            }
        }
        return arr;
    }
    case AGG_ANY:
    case AGG_ALL: {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n);
        for (int64_t i = 0; i < n; i++) {
            if (acc->has_na && acc->has_na[i]) {
                vec_array_set_null(&arr, i);
            } else {
                vec_array_set_valid(&arr, i);
                arr.buf.dbl[i] = (double)acc->has_value[i];
            }
        }
        return arr;
    }
    case AGG_N_DISTINCT: {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n);
        vec_array_set_all_valid(&arr);
        for (int64_t i = 0; i < n; i++)
            arr.buf.dbl[i] = (double)(acc->nd_count ? acc->nd_count[i] : 0);
        return arr;
    }
    case AGG_MEDIAN: {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n);
        for (int64_t i = 0; i < n; i++) {
            if (acc->has_na && acc->has_na[i]) {
                vec_array_set_null(&arr, i);
            } else if (acc->med_count[i] == 0) {
                vec_array_set_null(&arr, i);
            } else {
                vec_array_set_valid(&arr, i);
                if (acc->med_count[i] < 64)
                    dbl_insertion_sort(acc->med_vals[i], acc->med_count[i]);
                else
                    qsort(acc->med_vals[i], (size_t)acc->med_count[i], sizeof(double), dbl_cmp);
                int64_t m = acc->med_count[i];
                if (m % 2 == 1)
                    arr.buf.dbl[i] = acc->med_vals[i][m / 2];
                else
                    arr.buf.dbl[i] = (acc->med_vals[i][m / 2 - 1] + acc->med_vals[i][m / 2]) / 2.0;
            }
        }
        return arr;
    }
    }

    VecArray empty;
    memset(&empty, 0, sizeof(empty));
    return empty;
}

void agg_accum_free(AggAccum *acc) {
    free(acc->count);
    free(acc->count_all);
    free(acc->sum_dbl);
    free(acc->sum_i64);
    free(acc->min_dbl);
    free(acc->max_dbl);
    free(acc->min_i64);
    free(acc->max_i64);
    free(acc->has_value);
    free(acc->has_na);
    free(acc->m2);
    free(acc->first_dbl);
    free(acc->first_i64);
    free(acc->last_dbl);
    free(acc->last_i64);
    free(acc->has_first);
    /* n_distinct hash sets */
    if (acc->nd_slots) {
        for (int64_t i = 0; i < acc->capacity; i++) free(acc->nd_slots[i]);
        free(acc->nd_slots);
    }
    free(acc->nd_size);
    free(acc->nd_count);
    /* median value arrays */
    if (acc->med_vals) {
        for (int64_t i = 0; i < acc->capacity; i++) free(acc->med_vals[i]);
        free(acc->med_vals);
    }
    free(acc->med_count);
    free(acc->med_cap);
    memset(acc, 0, sizeof(*acc));
}
