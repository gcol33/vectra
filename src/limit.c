#include "limit.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

static VecBatch *limit_next_batch(VecNode *self) {
    LimitNode *ln = (LimitNode *)self;
    if (ln->rows_emitted >= ln->max_rows) return NULL;

    VecBatch *batch = ln->child->next_batch(ln->child);
    if (!batch) return NULL;

    int64_t logical_n = vec_batch_logical_rows(batch);
    int64_t remaining = ln->max_rows - ln->rows_emitted;

    if (logical_n <= remaining) {
        /* Whole batch fits within limit */
        ln->rows_emitted += logical_n;
        return batch;
    }

    /* Need to truncate: keep only first `keep` logical rows.
       Build a sel of the first `keep` physical row indices, then gather. */
    int32_t keep = (int32_t)remaining;
    int32_t *trunc_sel = (int32_t *)malloc((size_t)keep * sizeof(int32_t));
    for (int32_t li = 0; li < keep; li++)
        trunc_sel[li] = (int32_t)vec_batch_physical_row(batch, (int64_t)li);

    VecBatch *out = vec_batch_alloc(batch->n_cols, (int64_t)keep);
    for (int c = 0; c < batch->n_cols; c++) {
        out->columns[c] = vec_array_gather(&batch->columns[c],
                                            trunc_sel, keep);
        const char *nm = batch->col_names[c];
        out->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(out->col_names[c], nm);
    }

    free(trunc_sel);
    vec_batch_free(batch);
    ln->rows_emitted += (int64_t)keep;
    return out;
}

static void limit_free(VecNode *self) {
    LimitNode *ln = (LimitNode *)self;
    ln->child->free_node(ln->child);
    vec_schema_free(&ln->base.output_schema);
    free(ln);
}

LimitNode *limit_node_create(VecNode *child, int64_t max_rows) {
    LimitNode *ln = (LimitNode *)calloc(1, sizeof(LimitNode));
    if (!ln) vectra_error("alloc failed for LimitNode");
    ln->child = child;
    ln->max_rows = max_rows;
    ln->rows_emitted = 0;

    ln->base.output_schema = vec_schema_copy(&child->output_schema);
    ln->base.next_batch = limit_next_batch;
    ln->base.free_node = limit_free;
    ln->base.kind = "LimitNode";

    return ln;
}
