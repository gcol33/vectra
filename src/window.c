#include "window.h"
#include "vec_omp.h"
#include "hash.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "builder.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* Forward declaration */
static int vec_compare_values(const VecArray *arr, int64_t a, int64_t b);

/* Read any numeric column value as double */
static inline double win_get_double(const VecArray *arr, int64_t i) {
    switch (arr->type) {
    case VEC_DOUBLE: return arr->buf.dbl[i];
    case VEC_INT64:  return (double)arr->buf.i64[i];
    case VEC_INT32:  return (double)arr->buf.i32[i];
    case VEC_INT16:  return (double)arr->buf.i16[i];
    case VEC_INT8:   return (double)arr->buf.i8[i];
    default:         return 0.0;
    }
}

/* Thread-safe merge sort for window index arrays.
   Sorts indices[0..n-1] using arr for comparison.
   tmp must be at least n elements.
   Sequential — safe to call from within OMP parallel for (grouped path). */
static void win_merge_sort(int64_t *indices, int64_t *tmp, int64_t n,
                           const VecArray *arr) {
    if (n <= 1) return;
    /* Insertion sort for small arrays */
    if (n <= 32) {
        for (int64_t i = 1; i < n; i++) {
            int64_t key = indices[i];
            int64_t j = i - 1;
            while (j >= 0 && vec_compare_values(arr, indices[j], key) > 0) {
                indices[j + 1] = indices[j];
                j--;
            }
            indices[j + 1] = key;
        }
        return;
    }
    int64_t mid = n / 2;
    win_merge_sort(indices, tmp, mid, arr);
    win_merge_sort(indices + mid, tmp + mid, n - mid, arr);
    int64_t i = 0, j = mid, k = 0;
    while (i < mid && j < n) {
        if (vec_compare_values(arr, indices[i], indices[j]) <= 0)
            tmp[k++] = indices[i++];
        else
            tmp[k++] = indices[j++];
    }
    while (i < mid) tmp[k++] = indices[i++];
    while (j < n)   tmp[k++] = indices[j++];
    memcpy(indices, tmp, (size_t)n * sizeof(int64_t));
}

#ifdef _OPENMP
/* OMP task-parallel merge sort — only for top-level calls (ungrouped path).
   Uses OMP tasks to parallelize the recursive sort across cores. */
static void win_merge_sort_par(int64_t *indices, int64_t *tmp, int64_t n,
                               const VecArray *arr) {
    if (n <= 1) return;
    if (n <= 32) {
        for (int64_t i = 1; i < n; i++) {
            int64_t key = indices[i];
            int64_t j = i - 1;
            while (j >= 0 && vec_compare_values(arr, indices[j], key) > 0) {
                indices[j + 1] = indices[j];
                j--;
            }
            indices[j + 1] = key;
        }
        return;
    }
    int64_t mid = n / 2;
    if (n > VEC_OMP_THRESHOLD) {
        #pragma omp task shared(arr) if(n > VEC_OMP_THRESHOLD)
        win_merge_sort_par(indices, tmp, mid, arr);
        #pragma omp task shared(arr) if(n > VEC_OMP_THRESHOLD)
        win_merge_sort_par(indices + mid, tmp + mid, n - mid, arr);
        #pragma omp taskwait
    } else {
        win_merge_sort_par(indices, tmp, mid, arr);
        win_merge_sort_par(indices + mid, tmp + mid, n - mid, arr);
    }
    int64_t i = 0, j = mid, k = 0;
    while (i < mid && j < n) {
        if (vec_compare_values(arr, indices[i], indices[j]) <= 0)
            tmp[k++] = indices[i++];
        else
            tmp[k++] = indices[j++];
    }
    while (i < mid) tmp[k++] = indices[i++];
    while (j < n)   tmp[k++] = indices[j++];
    memcpy(indices, tmp, (size_t)n * sizeof(int64_t));
}
#endif  /* _OPENMP */

/* Top-level sort entry: uses parallel merge sort for large arrays,
   sequential for small.  Thread-safe, no global state. */
static void win_sort_indices(int64_t *indices, int64_t n,
                             const VecArray *arr) {
    int64_t *tmp = (int64_t *)malloc((size_t)n * sizeof(int64_t));
    if (!tmp) vectra_error("alloc failed in win_sort_indices");
#ifdef _OPENMP
    if (n > VEC_OMP_THRESHOLD && !omp_in_parallel()) {
        #pragma omp parallel
        {
            #pragma omp single
            win_merge_sort_par(indices, tmp, n, arr);
        }
    } else {
#endif
        win_merge_sort(indices, tmp, n, arr);
#ifdef _OPENMP
    }
#endif
    free(tmp);
}

/* Compare two values in a VecArray. Returns <0, 0, or >0.
   NAs sort last (greater than any non-NA value). */
static int vec_compare_values(const VecArray *arr, int64_t a, int64_t b) {
    int a_valid = vec_array_is_valid(arr, a);
    int b_valid = vec_array_is_valid(arr, b);
    if (!a_valid && !b_valid) return 0;
    if (!a_valid) return 1;   /* NA > non-NA */
    if (!b_valid) return -1;
    switch (arr->type) {
    case VEC_INT64: {
        int64_t va = arr->buf.i64[a], vb = arr->buf.i64[b];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    case VEC_INT32: {
        int32_t va = arr->buf.i32[a], vb = arr->buf.i32[b];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    case VEC_INT16: {
        int16_t va = arr->buf.i16[a], vb = arr->buf.i16[b];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    case VEC_INT8: {
        int8_t va = arr->buf.i8[a], vb = arr->buf.i8[b];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    case VEC_DOUBLE: {
        double va = arr->buf.dbl[a], vb = arr->buf.dbl[b];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    case VEC_STRING: {
        int64_t sa = arr->buf.str.offsets[a], ea = arr->buf.str.offsets[a + 1];
        int64_t sb = arr->buf.str.offsets[b], eb = arr->buf.str.offsets[b + 1];
        int64_t la = ea - sa, lb = eb - sb;
        int64_t mn = la < lb ? la : lb;
        int cmp = (mn > 0) ? memcmp(arr->buf.str.data + sa,
                                     arr->buf.str.data + sb, (size_t)mn) : 0;
        if (cmp != 0) return cmp;
        return (la < lb) ? -1 : (la > lb) ? 1 : 0;
    }
    default:
        return 0;
    }
}

/* Evaluate grouped lag or lead over rows[0..glen-1].
   direction: -1 for lag, +1 for lead. */
static void win_grp_shift(const VecArray *in_arr, const int64_t *rows,
                           int64_t glen, int direction, int offset,
                           double default_val, int has_default,
                           double *out_buf, uint8_t *null_flags) {
    for (int64_t j = 0; j < glen; j++) {
        int64_t src_j = j + direction * offset;
        if (src_j < 0 || src_j >= glen) {
            if (has_default)
                out_buf[rows[j]] = default_val;
            else
                null_flags[rows[j]] = 1;
        } else {
            int64_t src_row = rows[src_j];
            if (!vec_array_is_valid(in_arr, src_row)) {
                null_flags[rows[j]] = 1;
            } else {
                switch (in_arr->type) {
                case VEC_DOUBLE: out_buf[rows[j]] = in_arr->buf.dbl[src_row]; break;
                case VEC_INT64:  out_buf[rows[j]] = (double)in_arr->buf.i64[src_row]; break;
                case VEC_INT32:  out_buf[rows[j]] = (double)in_arr->buf.i32[src_row]; break;
                case VEC_INT16:  out_buf[rows[j]] = (double)in_arr->buf.i16[src_row]; break;
                case VEC_INT8:   out_buf[rows[j]] = (double)in_arr->buf.i8[src_row]; break;
                default: out_buf[rows[j]] = 0.0; break;
                }
            }
        }
    }
}

/* Evaluate grouped cume_dist over rows[0..glen-1].
   Uses thread-safe merge sort (no global state). */
static void win_grp_cume_dist(const VecArray *in_arr, const int64_t *rows,
                               int64_t glen, double *out_buf) {
    int64_t *sorted = (int64_t *)malloc((size_t)glen * sizeof(int64_t));
    int64_t *stmp   = (int64_t *)malloc((size_t)glen * sizeof(int64_t));
    for (int64_t j = 0; j < glen; j++) sorted[j] = rows[j];
    win_merge_sort(sorted, stmp, glen, in_arr);
    int64_t si = 0;
    while (si < glen) {
        int64_t sj = si + 1;
        while (sj < glen && vec_compare_values(in_arr,
                sorted[sj], sorted[si]) == 0)
            sj++;
        double cd = (double)sj / (double)glen;
        for (int64_t sk = si; sk < sj; sk++)
            out_buf[sorted[sk]] = cd;
        si = sj;
    }
    free(stmp);
    free(sorted);
}

/* Evaluate lag/lead for a contiguous segment.
   direction: -1 for lag (look back), +1 for lead (look forward). */
static void win_eval_shift(const VecArray *input, int64_t start, int64_t end,
                           int direction, int offset, double default_val,
                           int has_default, VecArray *result) {
    /* Build validity bitmap (sequential — bitmap bytes are shared) */
    for (int64_t i = start; i < end; i++) {
        int64_t src_row = i + direction * offset;
        if (src_row < start || src_row >= end) {
            if (has_default)
                vec_array_set_valid(result, i);
            else
                vec_array_set_null(result, i);
        } else if (!vec_array_is_valid(input, src_row)) {
            vec_array_set_null(result, i);
        } else {
            vec_array_set_valid(result, i);
        }
    }
    /* Parallel data copy */
    #pragma omp parallel for if((end - start) > VEC_OMP_THRESHOLD) schedule(static)
    for (int64_t i = start; i < end; i++) {
        int64_t src_row = i + direction * offset;
        if (src_row < start || src_row >= end) {
            if (has_default)
                result->buf.dbl[i] = default_val;
        } else if (vec_array_is_valid(result, i)) {
            switch (input->type) {
            case VEC_DOUBLE: result->buf.dbl[i] = input->buf.dbl[src_row]; break;
            case VEC_INT64:  result->buf.dbl[i] = (double)input->buf.i64[src_row]; break;
            case VEC_INT32:  result->buf.dbl[i] = (double)input->buf.i32[src_row]; break;
            case VEC_INT16:  result->buf.dbl[i] = (double)input->buf.i16[src_row]; break;
            case VEC_INT8:   result->buf.dbl[i] = (double)input->buf.i8[src_row]; break;
            default: result->buf.dbl[i] = 0.0; break;
            }
        }
    }
}

/* Evaluate cume_dist for a contiguous segment using thread-safe merge sort. */
static void win_eval_cume_dist(const VecArray *input, int64_t start,
                               int64_t seg_len, VecArray *result) {
    int64_t *idx = (int64_t *)malloc((size_t)seg_len * sizeof(int64_t));
    for (int64_t i = 0; i < seg_len; i++) idx[i] = start + i;
    win_sort_indices(idx, seg_len, input);
    /* Groups of ties get cume_dist = (last position in group + 1) / n */
    int64_t i = 0;
    while (i < seg_len) {
        int64_t j = i + 1;
        while (j < seg_len && vec_compare_values(input, idx[j], idx[i]) == 0)
            j++;
        double cd = (double)j / (double)seg_len;
        for (int64_t k = i; k < j; k++) {
            vec_array_set_valid(result, idx[k]);
            result->buf.dbl[idx[k]] = cd;
        }
        i = j;
    }
    free(idx);
}

/* Apply a window kernel over a contiguous segment [start, end) */
static VecArray win_eval_segment(WinKind kind, const VecArray *input,
                                 int64_t start, int64_t end, int64_t n_total,
                                 int offset, double default_val, int has_default,
                                 int desc, VecArray *result) {
    (void)n_total;
    int64_t seg_len = end - start;

    switch (kind) {
    case WIN_LAG:
        win_eval_shift(input, start, end, -1, offset, default_val,
                       has_default, result);
        break;

    case WIN_LEAD:
        win_eval_shift(input, start, end, +1, offset, default_val,
                       has_default, result);
        break;

    case WIN_ROW_NUMBER:
        if (input) {
            /* Ordered row_number: 1..n by input column (deterministic, no ties) */
            int64_t *idx = (int64_t *)malloc((size_t)seg_len * sizeof(int64_t));
            for (int64_t i = 0; i < seg_len; i++) idx[i] = start + i;
            win_sort_indices(idx, seg_len, input);
            for (int64_t i = 0; i < seg_len; i++) {
                vec_array_set_valid(result, idx[i]);
                result->buf.dbl[idx[i]] =
                    desc ? (double)(seg_len - i) : (double)(i + 1);
            }
            free(idx);
        } else {
            for (int64_t i = start; i < end; i++) {
                vec_array_set_valid(result, i);
                result->buf.dbl[i] = (double)(i - start + 1);
            }
        }
        break;

    case WIN_RANK: {
        /* O(n log n) min_rank via sort-then-scan (thread-safe) */
        int64_t *idx = (int64_t *)malloc((size_t)seg_len * sizeof(int64_t));
        for (int64_t i = 0; i < seg_len; i++) idx[i] = start + i;
        win_sort_indices(idx, seg_len, input);
        int64_t rank = 1;
        if (!desc) {
            for (int64_t i = 0; i < seg_len; i++) {
                if (i > 0 && vec_compare_values(input, idx[i], idx[i - 1]) != 0)
                    rank = i + 1;
                vec_array_set_valid(result, idx[i]);
                result->buf.dbl[idx[i]] = (double)rank;
            }
        } else {
            /* Descending min_rank: largest value gets rank 1 */
            for (int64_t p = 0; p < seg_len; p++) {
                int64_t i = seg_len - 1 - p;
                if (p > 0 && vec_compare_values(input, idx[i], idx[i + 1]) != 0)
                    rank = p + 1;
                vec_array_set_valid(result, idx[i]);
                result->buf.dbl[idx[i]] = (double)rank;
            }
        }
        free(idx);
        break;
    }
    case WIN_DENSE_RANK: {
        /* O(n log n) dense_rank via sort-then-scan (thread-safe) */
        int64_t *idx = (int64_t *)malloc((size_t)seg_len * sizeof(int64_t));
        for (int64_t i = 0; i < seg_len; i++) idx[i] = start + i;
        win_sort_indices(idx, seg_len, input);
        int64_t rank = 1;
        for (int64_t i = 0; i < seg_len; i++) {
            if (i > 0 && vec_compare_values(input, idx[i], idx[i - 1]) != 0)
                rank++;
            vec_array_set_valid(result, idx[i]);
            result->buf.dbl[idx[i]] = (double)rank;
        }
        free(idx);
        break;
    }

    case WIN_CUMSUM: {
        double acc = 0.0;
        for (int64_t i = start; i < end; i++) {
            if (!vec_array_is_valid(input, i)) {
                vec_array_set_null(result, i);
                /* Once NA is seen, rest is NA (R semantics) */
                for (int64_t j = i + 1; j < end; j++)
                    vec_array_set_null(result, j);
                break;
            }
            double v = win_get_double(input, i);
            acc += v;
            vec_array_set_valid(result, i);
            result->buf.dbl[i] = acc;
        }
        break;
    }

    case WIN_CUMMEAN: {
        double acc = 0.0;
        int64_t cnt = 0;
        for (int64_t i = start; i < end; i++) {
            if (!vec_array_is_valid(input, i)) {
                vec_array_set_null(result, i);
                for (int64_t j = i + 1; j < end; j++)
                    vec_array_set_null(result, j);
                break;
            }
            double v = win_get_double(input, i);
            acc += v;
            cnt++;
            vec_array_set_valid(result, i);
            result->buf.dbl[i] = acc / (double)cnt;
        }
        break;
    }

    case WIN_CUMMIN: {
        double cur_min = INFINITY;
        for (int64_t i = start; i < end; i++) {
            if (!vec_array_is_valid(input, i)) {
                vec_array_set_null(result, i);
                for (int64_t j = i + 1; j < end; j++)
                    vec_array_set_null(result, j);
                break;
            }
            double v = win_get_double(input, i);
            if (v < cur_min) cur_min = v;
            vec_array_set_valid(result, i);
            result->buf.dbl[i] = cur_min;
        }
        break;
    }

    case WIN_CUMMAX: {
        double cur_max = -INFINITY;
        for (int64_t i = start; i < end; i++) {
            if (!vec_array_is_valid(input, i)) {
                vec_array_set_null(result, i);
                for (int64_t j = i + 1; j < end; j++)
                    vec_array_set_null(result, j);
                break;
            }
            double v = win_get_double(input, i);
            if (v > cur_max) cur_max = v;
            vec_array_set_valid(result, i);
            result->buf.dbl[i] = cur_max;
        }
        break;
    }

    case WIN_NTILE: {
        /* ntile(k): divide partition into k roughly equal buckets.
           offset holds the number of tiles (k). */
        int k = offset;
        for (int64_t i = start; i < end; i++) {
            int64_t row_idx = i - start;  /* 0-based within partition */
            int64_t bucket = (row_idx * k / seg_len) + 1;
            vec_array_set_valid(result, i);
            result->buf.dbl[i] = (double)bucket;
        }
        break;
    }

    case WIN_PERCENT_RANK: {
        /* percent_rank = (rank - 1) / (n - 1), where rank is min_rank.
           If n == 1, result is 0. */
        int64_t *idx = (int64_t *)malloc((size_t)seg_len * sizeof(int64_t));
        for (int64_t i = 0; i < seg_len; i++) idx[i] = start + i;
        win_sort_indices(idx, seg_len, input);
        int64_t rank = 1;
        for (int64_t i = 0; i < seg_len; i++) {
            if (i > 0 && vec_compare_values(input, idx[i], idx[i - 1]) != 0)
                rank = i + 1;
            vec_array_set_valid(result, idx[i]);
            if (seg_len <= 1)
                result->buf.dbl[idx[i]] = 0.0;
            else
                result->buf.dbl[idx[i]] = (double)(rank - 1) / (double)(seg_len - 1);
        }
        free(idx);
        break;
    }

    case WIN_CUME_DIST:
        win_eval_cume_dist(input, start, seg_len, result);
        break;
    }

    (void)seg_len;
    return *result;
}

static VecBatch *window_next_batch(VecNode *self) {
    WindowNode *wn = (WindowNode *)self;
    if (wn->done) return NULL;
    wn->done = 1;

    const VecSchema *cschema = &wn->child->output_schema;

    /* 1) Materialize all child batches */
    int n_cols = cschema->n_cols;
    VecArrayBuilder *builders = (VecArrayBuilder *)calloc(
        (size_t)n_cols, sizeof(VecArrayBuilder));
    for (int c = 0; c < n_cols; c++)
        builders[c] = vec_builder_init(cschema->col_types[c]);

    VecBatch *batch;
    while ((batch = wn->child->next_batch(wn->child)) != NULL) {
        if (!batch->sel) {
            for (int c = 0; c < n_cols; c++)
                vec_builder_append_array(&builders[c], &batch->columns[c]);
        } else {
            int64_t n_logical = vec_batch_logical_rows(batch);
            for (int c = 0; c < n_cols; c++)
                vec_builder_reserve(&builders[c], n_logical);
            for (int64_t li = 0; li < n_logical; li++) {
                int64_t pi = vec_batch_physical_row(batch, li);
                for (int c = 0; c < n_cols; c++)
                    vec_builder_append_one(&builders[c],
                                           &batch->columns[c], pi);
            }
        }
        vec_batch_free(batch);
    }

    int64_t n_rows = builders[0].length;
    VecArray *cols = (VecArray *)malloc((size_t)n_cols * sizeof(VecArray));
    for (int c = 0; c < n_cols; c++)
        cols[c] = vec_builder_finish(&builders[c]);
    free(builders);

    /* 2) Evaluate window functions */
    if (wn->n_keys > 0) {
        /* Grouped: find key column indices */
        int *key_idx = (int *)malloc((size_t)wn->n_keys * sizeof(int));
        for (int k = 0; k < wn->n_keys; k++) {
            key_idx[k] = vec_schema_find_col(cschema, wn->key_names[k]);
            if (key_idx[k] < 0)
                vectra_error("window: group column not found: %s",
                             wn->key_names[k]);
        }

        /* Assign group IDs via hash table */
        int64_t *group_ids = (int64_t *)malloc((size_t)n_rows * sizeof(int64_t));
        VecHashTable ht = vec_ht_create(64);

        /* Key arena via builders (supports all types including strings) */
        VecArrayBuilder *arena_builders = (VecArrayBuilder *)calloc(
            (size_t)wn->n_keys, sizeof(VecArrayBuilder));
        for (int k = 0; k < wn->n_keys; k++)
            arena_builders[k] = vec_builder_init(cols[key_idx[k]].type);

        /* Temporary arena arrays for hash lookups (rebuilt after each insert) */
        VecArray *arena = (VecArray *)calloc((size_t)wn->n_keys, sizeof(VecArray));
        int64_t arena_len = 0;

        /* Build key_cols array for hash lookups */
        VecArray *key_cols = (VecArray *)malloc(
            (size_t)wn->n_keys * sizeof(VecArray));
        for (int k = 0; k < wn->n_keys; k++)
            key_cols[k] = cols[key_idx[k]];

        for (int64_t r = 0; r < n_rows; r++) {
            uint64_t h = 0;
            for (int k = 0; k < wn->n_keys; k++) {
                uint64_t kh = vec_hash_value(&key_cols[k], r);
                h = (k == 0) ? kh : vec_hash_combine(h, kh);
            }

            int was_new = 0;
            int64_t gid = vec_ht_find_or_insert(
                &ht, h, key_cols, wn->n_keys, r,
                arena, arena_len, &was_new);

            if (was_new) {
                for (int k = 0; k < wn->n_keys; k++)
                    vec_builder_append_one(&arena_builders[k],
                                           &cols[key_idx[k]], r);
                arena_len++;
                /* Rebuild arena arrays from builders for next lookup */
                for (int k = 0; k < wn->n_keys; k++) {
                    if (arena[k].validity) vec_array_free(&arena[k]);
                    arena[k] = vec_builder_finish(&arena_builders[k]);
                    /* Re-init builder and re-append everything */
                    arena_builders[k] = vec_builder_init(cols[key_idx[k]].type);
                    vec_builder_append_array(&arena_builders[k], &arena[k]);
                }
            }
            group_ids[r] = gid;
        }

        int64_t n_groups = ht.n_groups;

        /* Build segments: collect row ranges per group
           Since rows may not be contiguous by group, collect indices per group */
        /* For simplicity: process window functions per-row with group awareness.
           Build per-group row lists. */
        int64_t **grp_rows = (int64_t **)calloc((size_t)n_groups, sizeof(int64_t *));
        int64_t *grp_lens = (int64_t *)calloc((size_t)n_groups, sizeof(int64_t));
        int64_t *grp_caps = (int64_t *)calloc((size_t)n_groups, sizeof(int64_t));

        for (int64_t r = 0; r < n_rows; r++) {
            int64_t g = group_ids[r];
            if (grp_lens[g] >= grp_caps[g]) {
                int64_t nc = grp_caps[g] == 0 ? 16 : grp_caps[g] * 2;
                grp_rows[g] = (int64_t *)realloc(grp_rows[g],
                    (size_t)nc * sizeof(int64_t));
                grp_caps[g] = nc;
            }
            grp_rows[g][grp_lens[g]++] = r;
        }

        /* Evaluate window functions per group */
        int out_ncols = wn->base.output_schema.n_cols;
        VecBatch *result = vec_batch_alloc(out_ncols, n_rows);

        /* Copy pass-through columns */
        for (int c = 0; c < n_cols; c++) {
            VecArray *copy = vec_coerce(&cols[c], cols[c].type);
            result->columns[c] = *copy;
            free(copy);
            const char *nm = cschema->col_names[c];
            result->col_names[c] = (char *)malloc(strlen(nm) + 1);
            strcpy(result->col_names[c], nm);
        }

        /* Evaluate window expressions */
        for (int w = 0; w < wn->n_wins; w++) {
            WinSpec *ws = &wn->win_specs[w];
            int in_col = -1;
            if (ws->input_col) {
                in_col = vec_schema_find_col(cschema, ws->input_col);
                if (in_col < 0)
                    vectra_error("window: column not found: %s", ws->input_col);
            }

            VecArray out = vec_array_alloc(VEC_DOUBLE, n_rows);
            const VecArray *in_arr = (in_col >= 0) ? &cols[in_col] : NULL;

            /* Pre-set all validity bits so the parallel loop only needs to
               clear bits for NAs.  We use a per-row byte flag (null_flags)
               instead of bitmap clear to avoid byte-level races, then apply
               nulls sequentially afterwards. */
            vec_array_set_all_valid(&out);
            uint8_t *null_flags = (uint8_t *)calloc((size_t)n_rows, 1);

            /* Each group is independent — parallelize the outer loop.
               All rank-like sorts use win_merge_sort (thread-safe, no globals).
               Writes to out.buf.dbl[rows[j]] are safe because each row belongs
               to exactly one group.  null_flags[row] is one byte per row, so
               no sharing between threads. */
#ifdef _OPENMP
            #pragma omp parallel for schedule(dynamic) if(n_groups > 64)
#endif
            for (int64_t g = 0; g < n_groups; g++) {
                int64_t glen = grp_lens[g];
                int64_t *rows = grp_rows[g];

                switch (ws->kind) {
                case WIN_ROW_NUMBER:
                    if (in_arr) {
                        /* Ordered row_number: 1..glen by input column, no ties */
                        int64_t *sorted = (int64_t *)malloc((size_t)glen * sizeof(int64_t));
                        int64_t *stmp   = (int64_t *)malloc((size_t)glen * sizeof(int64_t));
                        for (int64_t j = 0; j < glen; j++) sorted[j] = rows[j];
                        win_merge_sort(sorted, stmp, glen, in_arr);
                        for (int64_t j = 0; j < glen; j++)
                            out.buf.dbl[sorted[j]] =
                                ws->desc ? (double)(glen - j) : (double)(j + 1);
                        free(stmp);
                        free(sorted);
                    } else {
                        for (int64_t j = 0; j < glen; j++)
                            out.buf.dbl[rows[j]] = (double)(j + 1);
                    }
                    break;

                case WIN_RANK: {
                    int64_t *sorted = (int64_t *)malloc((size_t)glen * sizeof(int64_t));
                    int64_t *stmp   = (int64_t *)malloc((size_t)glen * sizeof(int64_t));
                    for (int64_t j = 0; j < glen; j++) sorted[j] = rows[j];
                    win_merge_sort(sorted, stmp, glen, in_arr);
                    int64_t rank = 1;
                    if (!ws->desc) {
                        for (int64_t j = 0; j < glen; j++) {
                            if (j > 0 && vec_compare_values(in_arr,
                                    sorted[j], sorted[j - 1]) != 0)
                                rank = j + 1;
                            out.buf.dbl[sorted[j]] = (double)rank;
                        }
                    } else {
                        /* Descending min_rank: largest value gets rank 1 */
                        for (int64_t p = 0; p < glen; p++) {
                            int64_t j = glen - 1 - p;
                            if (p > 0 && vec_compare_values(in_arr,
                                    sorted[j], sorted[j + 1]) != 0)
                                rank = p + 1;
                            out.buf.dbl[sorted[j]] = (double)rank;
                        }
                    }
                    free(stmp);
                    free(sorted);
                    break;
                }
                case WIN_DENSE_RANK: {
                    int64_t *sorted = (int64_t *)malloc((size_t)glen * sizeof(int64_t));
                    int64_t *stmp   = (int64_t *)malloc((size_t)glen * sizeof(int64_t));
                    for (int64_t j = 0; j < glen; j++) sorted[j] = rows[j];
                    win_merge_sort(sorted, stmp, glen, in_arr);
                    int64_t rank = 1;
                    for (int64_t j = 0; j < glen; j++) {
                        if (j > 0 && vec_compare_values(in_arr,
                                sorted[j], sorted[j - 1]) != 0)
                            rank++;
                        out.buf.dbl[sorted[j]] = (double)rank;
                    }
                    free(stmp);
                    free(sorted);
                    break;
                }

                case WIN_LAG:
                    win_grp_shift(in_arr, rows, glen, -1, ws->offset,
                                  ws->default_val, ws->has_default,
                                  out.buf.dbl, null_flags);
                    break;

                case WIN_LEAD:
                    win_grp_shift(in_arr, rows, glen, +1, ws->offset,
                                  ws->default_val, ws->has_default,
                                  out.buf.dbl, null_flags);
                    break;

                case WIN_CUMSUM: {
                    double acc = 0.0;
                    int poisoned = 0;
                    for (int64_t j = 0; j < glen; j++) {
                        int64_t ri = rows[j];
                        if (poisoned || !vec_array_is_valid(in_arr, ri)) {
                            null_flags[ri] = 1;
                            poisoned = 1;
                        } else {
                            double v = win_get_double(in_arr, ri);
                            acc += v;
                            out.buf.dbl[ri] = acc;
                        }
                    }
                    break;
                }

                case WIN_CUMMEAN: {
                    double acc = 0.0;
                    int64_t cnt = 0;
                    int poisoned = 0;
                    for (int64_t j = 0; j < glen; j++) {
                        int64_t ri = rows[j];
                        if (poisoned || !vec_array_is_valid(in_arr, ri)) {
                            null_flags[ri] = 1;
                            poisoned = 1;
                        } else {
                            double v = win_get_double(in_arr, ri);
                            acc += v;
                            cnt++;
                            out.buf.dbl[ri] = acc / (double)cnt;
                        }
                    }
                    break;
                }

                case WIN_CUMMIN: {
                    double cur = INFINITY;
                    int poisoned = 0;
                    for (int64_t j = 0; j < glen; j++) {
                        int64_t ri = rows[j];
                        if (poisoned || !vec_array_is_valid(in_arr, ri)) {
                            null_flags[ri] = 1;
                            poisoned = 1;
                        } else {
                            double v = win_get_double(in_arr, ri);
                            if (v < cur) cur = v;
                            out.buf.dbl[ri] = cur;
                        }
                    }
                    break;
                }

                case WIN_CUMMAX: {
                    double cur = -INFINITY;
                    int poisoned = 0;
                    for (int64_t j = 0; j < glen; j++) {
                        int64_t ri = rows[j];
                        if (poisoned || !vec_array_is_valid(in_arr, ri)) {
                            null_flags[ri] = 1;
                            poisoned = 1;
                        } else {
                            double v = win_get_double(in_arr, ri);
                            if (v > cur) cur = v;
                            out.buf.dbl[ri] = cur;
                        }
                    }
                    break;
                }

                case WIN_NTILE: {
                    int nt = ws->offset;  /* number of tiles */
                    for (int64_t j = 0; j < glen; j++) {
                        int64_t bucket = (j * nt / glen) + 1;
                        out.buf.dbl[rows[j]] = (double)bucket;
                    }
                    break;
                }

                case WIN_PERCENT_RANK: {
                    int64_t *sorted = (int64_t *)malloc((size_t)glen * sizeof(int64_t));
                    int64_t *stmp   = (int64_t *)malloc((size_t)glen * sizeof(int64_t));
                    for (int64_t j = 0; j < glen; j++) sorted[j] = rows[j];
                    win_merge_sort(sorted, stmp, glen, in_arr);
                    int64_t rank = 1;
                    for (int64_t j = 0; j < glen; j++) {
                        if (j > 0 && vec_compare_values(in_arr,
                                sorted[j], sorted[j - 1]) != 0)
                            rank = j + 1;
                        if (glen <= 1)
                            out.buf.dbl[sorted[j]] = 0.0;
                        else
                            out.buf.dbl[sorted[j]] = (double)(rank - 1) / (double)(glen - 1);
                    }
                    free(stmp);
                    free(sorted);
                    break;
                }

                case WIN_CUME_DIST:
                    win_grp_cume_dist(in_arr, rows, glen, out.buf.dbl);
                    break;
                }
            }

            /* Apply null flags to the validity bitmap (sequential, no races) */
            for (int64_t r = 0; r < n_rows; r++) {
                if (null_flags[r])
                    vec_array_set_null(&out, r);
            }
            free(null_flags);

            result->columns[n_cols + w] = out;
            result->col_names[n_cols + w] = (char *)malloc(
                strlen(ws->output_name) + 1);
            strcpy(result->col_names[n_cols + w], ws->output_name);
        }

        /* Cleanup */
        for (int64_t g = 0; g < n_groups; g++) free(grp_rows[g]);
        free(grp_rows);
        free(grp_lens);
        free(grp_caps);
        free(group_ids);
        for (int k = 0; k < wn->n_keys; k++) {
            vec_array_free(&arena[k]);
            vec_builder_free(&arena_builders[k]);
        }
        free(arena);
        free(arena_builders);
        free(key_cols);
        free(key_idx);
        vec_ht_free(&ht);
        for (int c = 0; c < n_cols; c++) vec_array_free(&cols[c]);
        free(cols);

        return result;
    }

    /* Ungrouped path: single segment over entire data */
    int out_ncols = wn->base.output_schema.n_cols;
    VecBatch *result = vec_batch_alloc(out_ncols, n_rows);

    for (int c = 0; c < n_cols; c++) {
        VecArray *copy = vec_coerce(&cols[c], cols[c].type);
        result->columns[c] = *copy;
        free(copy);
        const char *nm = cschema->col_names[c];
        result->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(result->col_names[c], nm);
    }

    for (int w = 0; w < wn->n_wins; w++) {
        WinSpec *ws = &wn->win_specs[w];
        int in_col = -1;
        if (ws->input_col) {
            in_col = vec_schema_find_col(cschema, ws->input_col);
            if (in_col < 0)
                vectra_error("window: column not found: %s", ws->input_col);
        }

        VecArray out = vec_array_alloc(VEC_DOUBLE, n_rows);
        win_eval_segment(ws->kind, in_col >= 0 ? &cols[in_col] : NULL,
                         0, n_rows, n_rows,
                         ws->offset, ws->default_val, ws->has_default,
                         ws->desc, &out);
        result->columns[n_cols + w] = out;
        result->col_names[n_cols + w] = (char *)malloc(
            strlen(ws->output_name) + 1);
        strcpy(result->col_names[n_cols + w], ws->output_name);
    }

    for (int c = 0; c < n_cols; c++) vec_array_free(&cols[c]);
    free(cols);

    return result;
}

static void window_free(VecNode *self) {
    WindowNode *wn = (WindowNode *)self;
    wn->child->free_node(wn->child);
    for (int k = 0; k < wn->n_keys; k++) free(wn->key_names[k]);
    free(wn->key_names);
    for (int w = 0; w < wn->n_wins; w++) {
        free(wn->win_specs[w].output_name);
        free(wn->win_specs[w].input_col);
    }
    free(wn->win_specs);
    vec_schema_free(&wn->base.output_schema);
    free(wn);
}

WindowNode *window_node_create(VecNode *child,
                               int n_keys, char **key_names,
                               int n_wins, WinSpec *win_specs) {
    WindowNode *wn = (WindowNode *)calloc(1, sizeof(WindowNode));
    if (!wn) vectra_error("alloc failed for WindowNode");
    wn->child = child;
    wn->n_keys = n_keys;
    wn->key_names = key_names;
    wn->n_wins = n_wins;
    wn->win_specs = win_specs;
    wn->done = 0;

    /* Output schema: child schema + window columns (all double) */
    const VecSchema *cs = &child->output_schema;
    int out_n = cs->n_cols + n_wins;
    char **names = (char **)malloc((size_t)out_n * sizeof(char *));
    VecType *types = (VecType *)malloc((size_t)out_n * sizeof(VecType));
    for (int i = 0; i < cs->n_cols; i++) {
        names[i] = cs->col_names[i];
        types[i] = cs->col_types[i];
    }
    for (int w = 0; w < n_wins; w++) {
        names[cs->n_cols + w] = win_specs[w].output_name;
        types[cs->n_cols + w] = VEC_DOUBLE;
    }

    wn->base.output_schema = vec_schema_create(out_n, names, types);
    free(names);
    free(types);

    wn->base.next_batch = window_next_batch;
    wn->base.kind = "WindowNode";
    wn->base.free_node = window_free;
    wn->base.row_count_hint = child->row_count_hint;

    return wn;
}
