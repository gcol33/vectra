#include "filter.h"
#include "vec_omp.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/*  AND-chain flattening                                              */
/* ------------------------------------------------------------------ */

/* Recursively collect AND conjuncts from the expression tree.
   Returns number of conjuncts found.  Stops at FILTER_MAX_CONJUNCTS. */
static int flatten_and_chain(VecExpr *expr, VecExpr **out, int max) {
    if (!expr || max <= 0) return 0;
    if (expr->kind == EXPR_BOOL && expr->op == '&') {
        int n = flatten_and_chain(expr->left, out, max);
        n += flatten_and_chain(expr->right, out + n, max - n);
        return n;
    }
    out[0] = expr;
    return 1;
}

/* ------------------------------------------------------------------ */
/*  Mask application                                                  */
/* ------------------------------------------------------------------ */

/* Build a selection vector from a boolean mask applied to a batch.
   The mask is evaluated on full physical rows (length == batch->n_rows).
   If the batch already has a sel, compose: only keep physical rows that
   are in the existing sel AND pass the mask. */
static void apply_mask_sel(VecBatch *batch, const VecArray *mask) {
    int64_t n_logical = vec_batch_logical_rows(batch);

    /* Fast path: when mask has no NAs, skip validity checks */
    int mask_all_valid = vec_array_all_valid(mask);

    /* Count qualifying rows */
    int32_t n_sel = 0;
    if (mask_all_valid) {
        for (int64_t li = 0; li < n_logical; li++) {
            int64_t pi = vec_batch_physical_row(batch, li);
            if (mask->buf.bln[pi]) n_sel++;
        }
    } else {
        for (int64_t li = 0; li < n_logical; li++) {
            int64_t pi = vec_batch_physical_row(batch, li);
            if (vec_array_is_valid(mask, pi) && mask->buf.bln[pi])
                n_sel++;
        }
    }

    int32_t *sel = (int32_t *)malloc(
        (size_t)(n_sel > 0 ? n_sel : 1) * sizeof(int32_t));
    if (!sel) vectra_error("alloc failed for filter sel");

#ifdef _OPENMP
    if (n_logical > VEC_OMP_THRESHOLD) {
        int nthreads = omp_get_max_threads();
        int64_t *offsets = (int64_t *)calloc((size_t)(nthreads + 1), sizeof(int64_t));

        /* Phase 1: each thread counts its matches */
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int64_t local_count = 0;
            #pragma omp for schedule(static)
            for (int64_t li = 0; li < n_logical; li++) {
                int64_t pi = vec_batch_physical_row(batch, li);
                if (mask_all_valid ? mask->buf.bln[pi]
                    : (vec_array_is_valid(mask, pi) && mask->buf.bln[pi]))
                    local_count++;
            }
            offsets[tid + 1] = local_count;
        }

        /* Phase 2: prefix sum on offsets */
        for (int t = 1; t <= nthreads; t++)
            offsets[t] += offsets[t - 1];

        /* Phase 3: each thread writes at its offset */
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int64_t pos = offsets[tid];
            #pragma omp for schedule(static)
            for (int64_t li = 0; li < n_logical; li++) {
                int64_t pi = vec_batch_physical_row(batch, li);
                if (mask_all_valid ? mask->buf.bln[pi]
                    : (vec_array_is_valid(mask, pi) && mask->buf.bln[pi]))
                    sel[pos++] = (int32_t)pi;
            }
        }

        free(offsets);
    } else {
#endif
        int32_t j = 0;
        if (mask_all_valid) {
            for (int64_t li = 0; li < n_logical; li++) {
                int64_t pi = vec_batch_physical_row(batch, li);
                if (mask->buf.bln[pi]) sel[j++] = (int32_t)pi;
            }
        } else {
            for (int64_t li = 0; li < n_logical; li++) {
                int64_t pi = vec_batch_physical_row(batch, li);
                if (vec_array_is_valid(mask, pi) && mask->buf.bln[pi])
                    sel[j++] = (int32_t)pi;
            }
        }
#ifdef _OPENMP
    }
#endif

    /* Replace any existing sel */
    free(batch->sel);
    batch->sel = sel;
    batch->sel_n = n_sel;
}

/* ------------------------------------------------------------------ */
/*  Selectivity-based reordering                                      */
/* ------------------------------------------------------------------ */

/* Reorder conjuncts: most selective (lowest pass rate) first.
   Only reorder after we have enough data (> 1000 rows sampled). */
static void reorder_conjuncts(FilterNode *fn) {
    int nc = fn->n_conjuncts;
    if (nc < 2) return;

    /* Check if we have enough data */
    int64_t min_rows = fn->conj_total_rows[fn->conj_order[0]];
    if (min_rows < 1000) return;

    /* Simple insertion sort by selectivity (ascending = most selective first) */
    for (int i = 1; i < nc; i++) {
        int key = fn->conj_order[i];
        double key_sel = fn->conj_selectivity[key];
        int j = i - 1;
        while (j >= 0 && fn->conj_selectivity[fn->conj_order[j]] > key_sel) {
            fn->conj_order[j + 1] = fn->conj_order[j];
            j--;
        }
        fn->conj_order[j + 1] = key;
    }
}

/* ------------------------------------------------------------------ */
/*  Next batch                                                        */
/* ------------------------------------------------------------------ */

static VecBatch *filter_next_batch(VecNode *self) {
    FilterNode *fn = (FilterNode *)self;
    VecBatch *batch;

    while ((batch = fn->child->next_batch(fn->child)) != NULL) {
        if (fn->n_conjuncts > 1) {
            /* Multi-conjunct AND: evaluate sequentially with short-circuit */
            int64_t input_rows = vec_batch_logical_rows(batch);
            int all_eliminated = 0;

            for (int ci = 0; ci < fn->n_conjuncts; ci++) {
                int idx = fn->conj_order[ci];
                VecArray *mask = vec_expr_eval(fn->conjuncts[idx], batch);
                if (mask->type != VEC_BOOL) {
                    vec_array_free(mask); free(mask);
                    vec_batch_free(batch);
                    vectra_error("filter predicate must evaluate to boolean");
                }

                apply_mask_sel(batch, mask);
                vec_array_free(mask); free(mask);

                /* Update selectivity tracking */
                int64_t passed = (int64_t)batch->sel_n;
                fn->conj_total_rows[idx] += input_rows;
                /* Exponential moving average: weight recent batches more */
                double batch_rate = (input_rows > 0)
                    ? (double)passed / (double)input_rows : 1.0;
                double alpha = (fn->conj_total_rows[idx] < 10000) ? 0.3 : 0.1;
                fn->conj_selectivity[idx] =
                    fn->conj_selectivity[idx] * (1.0 - alpha) + batch_rate * alpha;
                input_rows = passed;

                /* Short-circuit: if all rows eliminated, skip remaining */
                if (batch->sel_n == 0) {
                    all_eliminated = 1;
                    break;
                }
            }

            /* Periodically reorder for next batch */
            reorder_conjuncts(fn);

            if (all_eliminated || batch->sel_n == 0) {
                vec_batch_free(batch);
                continue;
            }
            return batch;
        }

        /* Single predicate: original path */
        VecArray *mask = vec_expr_eval(fn->predicate, batch);
        if (mask->type != VEC_BOOL) {
            vec_array_free(mask); free(mask);
            vec_batch_free(batch);
            vectra_error("filter predicate must evaluate to boolean");
        }

        apply_mask_sel(batch, mask);
        vec_array_free(mask); free(mask);

        if (batch->sel_n > 0)
            return batch;

        /* Empty batch after filtering: skip and pull next */
        vec_batch_free(batch);
    }

    return NULL;
}

static void filter_free(VecNode *self) {
    FilterNode *fn = (FilterNode *)self;
    fn->child->free_node(fn->child);
    vec_expr_free(fn->predicate);
    vec_schema_free(&fn->base.output_schema);
    free(fn);
}

FilterNode *filter_node_create(VecNode *child, VecExpr *predicate) {
    FilterNode *fn = (FilterNode *)calloc(1, sizeof(FilterNode));
    if (!fn) vectra_error("alloc failed for FilterNode");
    fn->child = child;
    fn->predicate = predicate;
    fn->base.output_schema = vec_schema_copy(&child->output_schema);
    fn->base.next_batch = filter_next_batch;
    fn->base.free_node = filter_free;
    fn->base.kind = "FilterNode";

    /* Flatten AND chain for selectivity-based reordering */
    fn->n_conjuncts = flatten_and_chain(predicate, fn->conjuncts,
                                         FILTER_MAX_CONJUNCTS);
    if (fn->n_conjuncts <= 1) {
        /* Single predicate or couldn't flatten — use original path */
        fn->n_conjuncts = 0;
    } else {
        /* Initialize order and selectivity (assume uniform 50% initially) */
        for (int i = 0; i < fn->n_conjuncts; i++) {
            fn->conj_order[i] = i;
            fn->conj_selectivity[i] = 0.5;
            fn->conj_total_rows[i] = 0;
        }
    }

    return fn;
}
