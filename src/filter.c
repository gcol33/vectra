#include "filter.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/* Build a selection vector from a boolean mask applied to a batch.
   The mask is evaluated on full physical rows (length == batch->n_rows).
   If the batch already has a sel, compose: only keep physical rows that
   are in the existing sel AND pass the mask. */
static void apply_mask_sel(VecBatch *batch, const VecArray *mask) {
    int64_t n_logical = vec_batch_logical_rows(batch);

    /* Count qualifying rows */
    int32_t n_sel = 0;
    for (int64_t li = 0; li < n_logical; li++) {
        int64_t pi = vec_batch_physical_row(batch, li);
        if (vec_array_is_valid(mask, pi) && mask->buf.bln[pi])
            n_sel++;
    }

    int32_t *sel = (int32_t *)malloc(
        (size_t)(n_sel > 0 ? n_sel : 1) * sizeof(int32_t));
    if (!sel) vectra_error("alloc failed for filter sel");

    int32_t j = 0;
    for (int64_t li = 0; li < n_logical; li++) {
        int64_t pi = vec_batch_physical_row(batch, li);
        if (vec_array_is_valid(mask, pi) && mask->buf.bln[pi])
            sel[j++] = (int32_t)pi;
    }

    /* Replace any existing sel */
    free(batch->sel);
    batch->sel = sel;
    batch->sel_n = n_sel;
}

static VecBatch *filter_next_batch(VecNode *self) {
    FilterNode *fn = (FilterNode *)self;
    VecBatch *batch;

    while ((batch = fn->child->next_batch(fn->child)) != NULL) {
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
    return fn;
}
