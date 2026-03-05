#include "agg_ops.h"
#include "array.h"
#include "error.h"
#include <R.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>
#include <math.h>

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
    acc.input_type = input_type;
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
    }
    acc->capacity = new_cap;
    acc->n_groups = n_groups;
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
            acc->sum_i64[group_id] += col->buf.i64[row];
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
            int64_t v = col->buf.i64[row];
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
            int64_t v = col->buf.i64[row];
            if (!acc->has_value[group_id] || v > acc->max_i64[group_id]) {
                acc->max_i64[group_id] = v;
                acc->has_value[group_id] = 1;
            }
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
    memset(acc, 0, sizeof(*acc));
}
