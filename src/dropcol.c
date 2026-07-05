#include "dropcol.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "error.h"
#include <stdlib.h>

static VecBatch *dropcol_next_batch(VecNode *self) {
    DropColNode *d = (DropColNode *)self;
    VecBatch *b = d->child->next_batch(d->child);
    if (!b) return NULL;

    int di = d->drop_idx;
    vec_array_free(&b->columns[di]);
    free(b->col_names[di]);
    for (int c = di; c < b->n_cols - 1; c++) {
        b->columns[c] = b->columns[c + 1];
        b->col_names[c] = b->col_names[c + 1];
    }
    b->n_cols--;
    return b;
}

static void dropcol_free(VecNode *self) {
    DropColNode *d = (DropColNode *)self;
    d->child->free_node(d->child);
    vec_schema_free(&d->base.output_schema);
    free(d);
}

DropColNode *dropcol_node_create(VecNode *child, int drop_idx) {
    const VecSchema *cs = &child->output_schema;
    if (drop_idx < 0 || drop_idx >= cs->n_cols)
        vectra_error("dropcol: index %d out of range", drop_idx);

    DropColNode *d = (DropColNode *)calloc(1, sizeof(DropColNode));
    if (!d) vectra_error("alloc failed for DropColNode");
    d->child = child;
    d->drop_idx = drop_idx;

    int out_n = cs->n_cols - 1;
    char **names = (char **)malloc((size_t)out_n * sizeof(char *));
    VecType *types = (VecType *)malloc((size_t)out_n * sizeof(VecType));
    int j = 0;
    for (int i = 0; i < cs->n_cols; i++) {
        if (i == drop_idx) continue;
        names[j] = cs->col_names[i];
        types[j] = cs->col_types[i];
        j++;
    }
    d->base.output_schema = vec_schema_create(out_n, names, types);
    free(names);
    free(types);

    d->base.next_batch = dropcol_next_batch;
    d->base.free_node = dropcol_free;
    d->base.kind = "DropColNode";
    d->base.row_count_hint = child->row_count_hint;
    return d;
}
