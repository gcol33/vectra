#include "rowid.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

static VecBatch *rowid_next_batch(VecNode *self) {
    RowIdNode *r = (RowIdNode *)self;
    VecBatch *b = r->child->next_batch(r->child);
    if (!b) return NULL;

    int64_t nphys = b->n_rows;
    VecArray idcol = vec_array_alloc(VEC_INT64, nphys);
    vec_array_set_all_valid(&idcol);
    int64_t base = r->counter;
    for (int64_t i = 0; i < nphys; i++) idcol.buf.i64[i] = base + i;
    r->counter = base + nphys;

    int nc = b->n_cols;
    VecArray *ncols = (VecArray *)realloc(b->columns,
                                          (size_t)(nc + 1) * sizeof(VecArray));
    char **nnames = (char **)realloc(b->col_names,
                                     (size_t)(nc + 1) * sizeof(char *));
    if (!ncols || !nnames) vectra_error("rowid: realloc failed");
    b->columns = ncols;
    b->col_names = nnames;
    b->columns[nc] = idcol;
    size_t ln = strlen(r->name);
    b->col_names[nc] = (char *)malloc(ln + 1);
    memcpy(b->col_names[nc], r->name, ln + 1);
    b->n_cols = nc + 1;
    return b;
}

static void rowid_free(VecNode *self) {
    RowIdNode *r = (RowIdNode *)self;
    r->child->free_node(r->child);
    free(r->name);
    vec_schema_free(&r->base.output_schema);
    free(r);
}

RowIdNode *rowid_node_create(VecNode *child, const char *name) {
    RowIdNode *r = (RowIdNode *)calloc(1, sizeof(RowIdNode));
    if (!r) vectra_error("alloc failed for RowIdNode");
    r->child = child;
    r->counter = 0;
    size_t ln = strlen(name);
    r->name = (char *)malloc(ln + 1);
    memcpy(r->name, name, ln + 1);

    /* Output schema = child schema + one int64 column `name` appended. */
    const VecSchema *cs = &child->output_schema;
    int out_n = cs->n_cols + 1;
    char **names = (char **)malloc((size_t)out_n * sizeof(char *));
    VecType *types = (VecType *)malloc((size_t)out_n * sizeof(VecType));
    for (int i = 0; i < cs->n_cols; i++) {
        names[i] = cs->col_names[i];
        types[i] = cs->col_types[i];
    }
    names[cs->n_cols] = r->name;
    types[cs->n_cols] = VEC_INT64;
    r->base.output_schema = vec_schema_create(out_n, names, types);
    free(names);
    free(types);

    r->base.next_batch = rowid_next_batch;
    r->base.free_node = rowid_free;
    r->base.kind = "RowIdNode";
    r->base.row_count_hint = child->row_count_hint;
    return r;
}
