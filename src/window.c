#include "window.h"
#include "vec_omp.h"
#include "hash.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "builder.h"
#include "coerce.h"
#include "error.h"
#include "sort.h"
#include "rowid.h"
#include "dropcol.h"
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

    default:  /* roll_* kinds are handled before this switch */
        break;
    }

    (void)seg_len;
    return *result;
}

/* Normalize an order value to seconds: a Date (days since epoch) scales by
   86400, a POSIXct (seconds) passes through. Same magnitude heuristic the
   datetime expr ops use, so rolling windows given in seconds line up. */
static inline double win_order_seconds(const VecArray *ord, int64_t row) {
    double v = win_get_double(ord, row);
    return (fabs(v) < 200000.0) ? v * 86400.0 : v;
}

static inline int win_is_roll(WinKind k) {
    return k == WIN_ROLL_SUM || k == WIN_ROLL_MEAN || k == WIN_ROLL_MIN ||
           k == WIN_ROLL_MAX || k == WIN_ROLL_N;
}

/* Time-based trailing rolling aggregate over one group's rows.
   For each row, aggregates the value column over rows whose order value falls
   in (order - window, order], inclusive of the row itself. NA values are
   skipped (na.rm semantics). Writes results and validity into `out` directly.
   `val` may be NULL for roll_n (count of rows). */
static void win_roll_segment(WinKind kind, const VecArray *val,
                             const VecArray *ord, const int64_t *rows,
                             int64_t glen, double window, VecArray *out) {
    if (glen == 0) return;

    int64_t *idx = (int64_t *)malloc((size_t)glen * sizeof(int64_t));
    int64_t *tmp = (int64_t *)malloc((size_t)glen * sizeof(int64_t));
    for (int64_t j = 0; j < glen; j++) idx[j] = rows[j];
    win_merge_sort(idx, tmp, glen, ord);   /* ascending by order value */

    int is_minmax = (kind == WIN_ROLL_MIN || kind == WIN_ROLL_MAX);
    int counts_only = (kind == WIN_ROLL_N);

    double sum = 0.0;
    int64_t cnt = 0;
    int64_t *dq = is_minmax ? (int64_t *)malloc((size_t)glen * sizeof(int64_t)) : NULL;
    int64_t dq_head = 0, dq_tail = 0;

    int64_t left = 0;
    for (int64_t j = 0; j < glen; j++) {
        int64_t rj = idx[j];
        int val_ok = (counts_only || (val && vec_array_is_valid(val, rj)));

        /* include position j */
        if (is_minmax) {
            if (val_ok) {
                double vj = win_get_double(val, rj);
                while (dq_tail > dq_head) {
                    double vb = win_get_double(val, idx[dq[dq_tail - 1]]);
                    int worse = (kind == WIN_ROLL_MIN) ? (vb >= vj) : (vb <= vj);
                    if (worse) dq_tail--; else break;
                }
                dq[dq_tail++] = j;
            }
        } else {
            if (val_ok) {
                if (!counts_only) sum += win_get_double(val, rj);
                cnt++;
            }
        }

        /* advance left edge: trailing window is (order(rj) - window, order(rj)],
           so drop rows at or before the lower bound (open on the left). */
        double thr = win_order_seconds(ord, rj) - window;
        while (left <= j && win_order_seconds(ord, idx[left]) <= thr) {
            int64_t rl = idx[left];
            if (!is_minmax) {
                int lok = (counts_only || (val && vec_array_is_valid(val, rl)));
                if (lok) {
                    if (!counts_only) sum -= win_get_double(val, rl);
                    cnt--;
                }
            }
            left++;
        }
        if (is_minmax)
            while (dq_tail > dq_head && dq[dq_head] < left) dq_head++;

        /* emit */
        switch (kind) {
        case WIN_ROLL_SUM:
            out->buf.dbl[rj] = sum; vec_array_set_valid(out, rj); break;
        case WIN_ROLL_N:
            out->buf.dbl[rj] = (double)cnt; vec_array_set_valid(out, rj); break;
        case WIN_ROLL_MEAN:
            if (cnt > 0) { out->buf.dbl[rj] = sum / (double)cnt; vec_array_set_valid(out, rj); }
            else vec_array_set_null(out, rj);
            break;
        case WIN_ROLL_MIN:
        case WIN_ROLL_MAX:
            if (dq_tail > dq_head) {
                out->buf.dbl[rj] = win_get_double(val, idx[dq[dq_head]]);
                vec_array_set_valid(out, rj);
            } else vec_array_set_null(out, rj);
            break;
        default: break;
        }
    }

    free(dq);
    free(tmp);
    free(idx);
}

/* ------------------------------------------------------------------ */
/*  Spill-safe streaming path: one group per pull from a sorted child  */
/* ------------------------------------------------------------------ */

/* Value equality for two same-typed cells, both assumed valid. */
static int win_value_equal(const VecArray *a, int64_t ia,
                           const VecArray *b, int64_t ib) {
    switch (a->type) {
    case VEC_INT64:
    case VEC_INT32:
    case VEC_INT16:
    case VEC_INT8:
        return vec_array_get_int(a, ia) == vec_array_get_int(b, ib);
    case VEC_DOUBLE:
        return a->buf.dbl[ia] == b->buf.dbl[ib];
    case VEC_BOOL:
        return a->buf.bln[ia] == b->buf.bln[ib];
    case VEC_STRING: {
        int64_t sa = a->buf.str.offsets[ia], ea = a->buf.str.offsets[ia + 1];
        int64_t sb = b->buf.str.offsets[ib], eb = b->buf.str.offsets[ib + 1];
        int64_t la = ea - sa, lb = eb - sb;
        if (la != lb) return 0;
        return la == 0 || memcmp(a->buf.str.data + sa,
                                 b->buf.str.data + sb, (size_t)la) == 0;
    }
    default:
        return 0;
    }
}

/* Does row `ar` of `acols` carry the same group key as the snapshot `gkey`
   (a compact n_keys array holding the current group's key at row 0)? Two NA
   key values compare equal, so NAs form one group (dplyr semantics). */
static int win_group_key_equal(const VecArray *acols, int64_t ar,
                               const VecArray *gkey, const int *key_idx,
                               int n_keys) {
    for (int k = 0; k < n_keys; k++) {
        const VecArray *a = &acols[key_idx[k]];
        const VecArray *g = &gkey[k];
        int av = vec_array_is_valid(a, ar);
        int gv = vec_array_is_valid(g, 0);
        if (av != gv) return 0;
        if (!av) continue;                 /* both NA -> same group */
        if (!win_value_equal(a, ar, g, 0)) return 0;
    }
    return 1;
}

/* Snapshot row `ar`'s key columns into a standalone compact array (one row
   each), so the group key survives across batch boundaries and frees. */
static VecArray *win_snapshot_keys(const VecArray *acols, int64_t ar,
                                   const int *key_idx, int n_keys) {
    VecArray *g = (VecArray *)malloc((size_t)n_keys * sizeof(VecArray));
    for (int k = 0; k < n_keys; k++) {
        VecArrayBuilder b = vec_builder_init(acols[key_idx[k]].type);
        vec_builder_append_one(&b, &acols[key_idx[k]], ar);
        g[k] = vec_builder_finish(&b);
    }
    return g;
}

static void win_free_keys(VecArray *g, int n_keys) {
    if (!g) return;
    for (int k = 0; k < n_keys; k++) vec_array_free(&g[k]);
    free(g);
}

/* Pull the next contiguous group from the sorted child into freshly finished
   VecArrays (one per child column). Groups are contiguous because the child is
   sorted on the key columns, and ordered within the group by the trailing
   row-id, so cumulative windows see original arrival order. Returns the group
   columns and sets *out_glen; returns NULL when the child is exhausted. */
static VecArray *win_pull_group(WindowNode *wn, int n_cols, int64_t *out_glen) {
    const VecSchema *cschema = &wn->child->output_schema;

    if (!wn->hold_batch) {
        wn->hold_batch = wn->child->next_batch(wn->child);
        wn->hold_pos = 0;
        wn->hold_n = wn->hold_batch ? vec_batch_logical_rows(wn->hold_batch) : 0;
        if (!wn->hold_batch) return NULL;
    }

    VecArrayBuilder *gb = (VecArrayBuilder *)calloc((size_t)n_cols,
                                                    sizeof(VecArrayBuilder));
    for (int c = 0; c < n_cols; c++)
        gb[c] = vec_builder_init(cschema->col_types[c]);

    VecArray *gkey = NULL;
    int have_key = 0;
    int64_t glen = 0;

    for (;;) {
        while (wn->hold_pos < wn->hold_n) {
            int64_t pr = vec_batch_physical_row(wn->hold_batch, wn->hold_pos);
            if (have_key) {
                if (!win_group_key_equal(wn->hold_batch->columns, pr,
                                         gkey, wn->key_idx, wn->n_keys))
                    goto group_done;      /* boundary; leave row for next call */
            } else {
                gkey = win_snapshot_keys(wn->hold_batch->columns, pr,
                                         wn->key_idx, wn->n_keys);
                have_key = 1;
            }
            for (int c = 0; c < n_cols; c++)
                vec_builder_append_one(&gb[c], &wn->hold_batch->columns[c], pr);
            glen++;
            wn->hold_pos++;
        }
        vec_batch_free(wn->hold_batch);
        wn->hold_batch = wn->child->next_batch(wn->child);
        wn->hold_pos = 0;
        wn->hold_n = wn->hold_batch ? vec_batch_logical_rows(wn->hold_batch) : 0;
        if (!wn->hold_batch) break;       /* last group ends at stream end */
    }

group_done:
    win_free_keys(gkey, wn->n_keys);
    VecArray *cols = (VecArray *)malloc((size_t)n_cols * sizeof(VecArray));
    for (int c = 0; c < n_cols; c++)
        cols[c] = vec_builder_finish(&gb[c]);
    free(gb);
    *out_glen = glen;
    return cols;
}

/* Build one output batch for a single materialized group held in
   cols[0..n_cols-1] (each of length glen). The pass-through columns are moved
   into the result; each window column is evaluated over the whole [0, glen)
   segment, reusing the same kernels as the ungrouped path. */
static VecBatch *win_segment_batch(WindowNode *wn, VecArray *cols, int n_cols,
                                   int64_t glen, const VecSchema *cschema) {
    int out_ncols = wn->base.output_schema.n_cols;
    VecBatch *result = vec_batch_alloc(out_ncols, glen);

    for (int c = 0; c < n_cols; c++) {
        result->columns[c] = cols[c];     /* move ownership */
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

        VecArray out = vec_array_alloc(VEC_DOUBLE, glen);
        const VecArray *in_arr = (in_col >= 0) ? &result->columns[in_col] : NULL;

        if (win_is_roll(ws->kind)) {
            if (!ws->order_col)
                vectra_error("rolling window: order column required");
            int oc = vec_schema_find_col(cschema, ws->order_col);
            if (oc < 0)
                vectra_error("window: order column not found: %s", ws->order_col);
            int64_t *all_rows = (int64_t *)malloc((size_t)glen * sizeof(int64_t));
            for (int64_t r = 0; r < glen; r++) all_rows[r] = r;
            win_roll_segment(ws->kind, in_arr, &result->columns[oc],
                             all_rows, glen, ws->window, &out);
            free(all_rows);
        } else {
            win_eval_segment(ws->kind, in_arr, 0, glen, glen,
                             ws->offset, ws->default_val, ws->has_default,
                             ws->desc, &out);
        }

        result->columns[n_cols + w] = out;
        result->col_names[n_cols + w] = (char *)malloc(
            strlen(ws->output_name) + 1);
        strcpy(result->col_names[n_cols + w], ws->output_name);
    }

    return result;
}

/* Streaming next_batch: emit one group per call, in group-sorted order. The
   restore sort above this node returns the rows to original order. */
static VecBatch *window_stream_next(WindowNode *wn) {
    if (wn->done) return NULL;
    const VecSchema *cschema = &wn->child->output_schema;
    int n_cols = cschema->n_cols;

    int64_t glen = 0;
    VecArray *cols = win_pull_group(wn, n_cols, &glen);
    if (!cols || glen == 0) {
        if (cols) {
            for (int c = 0; c < n_cols; c++) vec_array_free(&cols[c]);
            free(cols);
        }
        wn->done = 1;
        return NULL;
    }

    VecBatch *result = win_segment_batch(wn, cols, n_cols, glen, cschema);
    free(cols);   /* arrays were moved into result */
    return result;
}

static VecBatch *window_next_batch(VecNode *self) {
    WindowNode *wn = (WindowNode *)self;
    if (wn->streaming) return window_stream_next(wn);
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

            /* Time-based rolling: a separate per-group sweep over a sorted
               order column, bypassing the row-order switch below. */
            int roll = win_is_roll(ws->kind);
            if (roll) {
                if (!ws->order_col)
                    vectra_error("rolling window: order column required");
                int oc = vec_schema_find_col(cschema, ws->order_col);
                if (oc < 0)
                    vectra_error("window: order column not found: %s", ws->order_col);
                const VecArray *ord_arr = &cols[oc];
#ifdef _OPENMP
                #pragma omp parallel for schedule(dynamic) if(n_groups > 64)
#endif
                for (int64_t g = 0; g < n_groups; g++)
                    win_roll_segment(ws->kind, in_arr, ord_arr,
                                     grp_rows[g], grp_lens[g], ws->window, &out);
            }
            int64_t g_lim = roll ? 0 : n_groups;

            /* Each group is independent — parallelize the outer loop.
               All rank-like sorts use win_merge_sort (thread-safe, no globals).
               Writes to out.buf.dbl[rows[j]] are safe because each row belongs
               to exactly one group.  null_flags[row] is one byte per row, so
               no sharing between threads. */
#ifdef _OPENMP
            #pragma omp parallel for schedule(dynamic) if(n_groups > 64)
#endif
            for (int64_t g = 0; g < g_lim; g++) {
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

                default:  /* roll_* kinds are handled before this switch */
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
        if (win_is_roll(ws->kind)) {
            if (!ws->order_col)
                vectra_error("rolling window: order column required");
            int oc = vec_schema_find_col(cschema, ws->order_col);
            if (oc < 0)
                vectra_error("window: order column not found: %s", ws->order_col);
            int64_t *all_rows = (int64_t *)malloc((size_t)n_rows * sizeof(int64_t));
            for (int64_t r = 0; r < n_rows; r++) all_rows[r] = r;
            win_roll_segment(ws->kind, in_col >= 0 ? &cols[in_col] : NULL,
                             &cols[oc], all_rows, n_rows, ws->window, &out);
            free(all_rows);
        } else {
            win_eval_segment(ws->kind, in_col >= 0 ? &cols[in_col] : NULL,
                             0, n_rows, n_rows,
                             ws->offset, ws->default_val, ws->has_default,
                             ws->desc, &out);
        }
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
    if (wn->hold_batch) vec_batch_free(wn->hold_batch);
    wn->child->free_node(wn->child);
    for (int k = 0; k < wn->n_keys; k++) free(wn->key_names[k]);
    free(wn->key_names);
    free(wn->key_idx);
    for (int w = 0; w < wn->n_wins; w++) {
        free(wn->win_specs[w].output_name);
        free(wn->win_specs[w].input_col);
        free(wn->win_specs[w].order_col);
    }
    free(wn->win_specs);
    vec_schema_free(&wn->base.output_schema);
    free(wn);
}

/* Row-id column name for the spill-safe streaming pipeline. Unlikely to clash
   with a user column; the projection at the top drops it before results reach
   R, so it is never visible. */
#define WIN_ROWID_COL "__vtr_window_rowid"

VecNode *window_node_create(VecNode *child,
                            int n_keys, char **key_names,
                            int n_wins, WinSpec *win_specs,
                            const char *temp_dir) {
    /* Grouped windows with a spill directory take the streaming path: sort by
       the group keys (plus a row-id tiebreak) so each group is contiguous and
       in arrival order, process one group at a time, then restore row order.
       Peak memory is one group, not the whole table. Ungrouped windows (a
       single global partition) or a NULL temp_dir fall back to the in-memory
       node below. */
    int streaming = (temp_dir != NULL && n_keys > 0);

    VecNode *src = child;          /* node the window node reads from */
    int rowid_idx = -1;

    if (streaming) {
        RowIdNode *rid = rowid_node_create(child, WIN_ROWID_COL);
        const VecSchema *rs = &rid->base.output_schema;
        rowid_idx = rs->n_cols - 1;

        SortKey *sk = (SortKey *)malloc((size_t)(n_keys + 1) * sizeof(SortKey));
        for (int k = 0; k < n_keys; k++) {
            int idx = vec_schema_find_col(rs, key_names[k]);
            if (idx < 0)
                vectra_error("window: group column not found: %s", key_names[k]);
            sk[k].col_index = idx;
            sk[k].descending = 0;
        }
        sk[n_keys].col_index = rowid_idx;   /* stable arrival order per group */
        sk[n_keys].descending = 0;
        SortNode *sn = sort_node_create((VecNode *)rid, n_keys + 1, sk,
                                        temp_dir, VECTRA_SORT_MEM_DEFAULT);
        src = (VecNode *)sn;
    }

    WindowNode *wn = (WindowNode *)calloc(1, sizeof(WindowNode));
    if (!wn) vectra_error("alloc failed for WindowNode");
    wn->child = src;
    wn->n_keys = n_keys;
    wn->key_names = key_names;
    wn->n_wins = n_wins;
    wn->win_specs = win_specs;
    wn->done = 0;
    wn->streaming = streaming;
    wn->key_idx = NULL;
    wn->rowid_idx = rowid_idx;
    wn->hold_batch = NULL;
    wn->hold_pos = 0;
    wn->hold_n = 0;

    const VecSchema *cs = &src->output_schema;

    if (streaming) {
        wn->key_idx = (int *)malloc((size_t)n_keys * sizeof(int));
        for (int k = 0; k < n_keys; k++)
            wn->key_idx[k] = vec_schema_find_col(cs, key_names[k]);
    }

    /* Output schema: src columns (child cols, plus row-id when streaming) +
       window columns (all double). */
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
    wn->base.row_count_hint = src->row_count_hint;

    if (!streaming)
        return (VecNode *)wn;

    /* Restore original row order: sort by the row-id (unique, arrival order). */
    SortKey *rk = (SortKey *)malloc(sizeof(SortKey));
    rk[0].col_index = rowid_idx;   /* row-id keeps its position in wn's schema */
    rk[0].descending = 0;
    SortNode *restore = sort_node_create((VecNode *)wn, 1, rk,
                                         temp_dir, VECTRA_SORT_MEM_DEFAULT);

    /* Drop the row-id by position, leaving child columns + window columns in
       the same layout the in-memory path produces. */
    DropColNode *drop = dropcol_node_create((VecNode *)restore, rowid_idx);
    return (VecNode *)drop;
}
