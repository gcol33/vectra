#include "concat.h"
#include "coerce.h"
#include "schema.h"
#include "array.h"
#include "batch.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

static VecBatch *concat_next_batch(VecNode *self) {
    ConcatNode *cn = (ConcatNode *)self;

    while (cn->current < cn->n_children) {
        VecBatch *batch = cn->children[cn->current]->next_batch(
            cn->children[cn->current]);
        if (batch) {
            /* Coerce columns to output schema types if needed */
            const VecSchema *out = &cn->base.output_schema;
            for (int c = 0; c < batch->n_cols && c < out->n_cols; c++) {
                if (batch->columns[c].type != out->col_types[c]) {
                    VecArray *coerced = vec_coerce(&batch->columns[c],
                                                    out->col_types[c]);
                    vec_array_free(&batch->columns[c]);
                    batch->columns[c] = *coerced;
                    free(coerced);
                }
            }
            return batch;
        }
        cn->current++;
    }
    return NULL;
}

static void concat_free(VecNode *self) {
    ConcatNode *cn = (ConcatNode *)self;
    for (int i = 0; i < cn->n_children; i++)
        cn->children[i]->free_node(cn->children[i]);
    free(cn->children);
    vec_schema_free(&cn->base.output_schema);
    free(cn);
}

ConcatNode *concat_node_create(int n_children, VecNode **children) {
    if (n_children < 1) vectra_error("concat requires at least one child");

    ConcatNode *cn = (ConcatNode *)calloc(1, sizeof(ConcatNode));
    if (!cn) vectra_error("alloc failed for ConcatNode");
    cn->n_children = n_children;
    cn->children = children;
    cn->current = 0;

    /* Output schema: common types across all children (int+double -> double) */
    cn->base.output_schema = vec_schema_copy(&children[0]->output_schema);
    VecSchema *out = &cn->base.output_schema;
    for (int i = 1; i < n_children; i++) {
        const VecSchema *cs = &children[i]->output_schema;
        for (int c = 0; c < out->n_cols && c < cs->n_cols; c++) {
            if (out->col_types[c] != cs->col_types[c]) {
                out->col_types[c] = vec_common_type(out->col_types[c],
                                                     cs->col_types[c]);
            }
        }
    }
    cn->base.next_batch = concat_next_batch;
    cn->base.free_node = concat_free;
    cn->base.kind = "ConcatNode";

    return cn;
}
