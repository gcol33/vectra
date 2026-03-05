#include "scan.h"
#include "batch.h"
#include "schema.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

static VecBatch *scan_next_batch(VecNode *self) {
    ScanNode *sn = (ScanNode *)self;
    if (sn->next_rg >= sn->file->header.n_rowgroups)
        return NULL;

    VecBatch *batch = vtr1_read_rowgroup(sn->file, sn->next_rg, sn->col_mask);
    sn->next_rg++;
    return batch;
}

static void scan_free(VecNode *self) {
    ScanNode *sn = (ScanNode *)self;
    vtr1_close(sn->file);
    free(sn->col_mask);
    vec_schema_free(&sn->base.output_schema);
    free(sn);
}

ScanNode *scan_node_create(const char *path, int *col_indices, int n_selected) {
    ScanNode *sn = (ScanNode *)calloc(1, sizeof(ScanNode));
    if (!sn) vectra_error("alloc failed for ScanNode");

    sn->file = vtr1_open(path);
    sn->next_rg = 0;

    const VecSchema *file_schema = &sn->file->header.schema;
    int n_cols = file_schema->n_cols;

    /* Build column mask */
    sn->col_mask = (int *)calloc((size_t)n_cols, sizeof(int));
    if (!sn->col_mask) vectra_error("alloc failed for col_mask");

    if (!col_indices) {
        /* Select all */
        for (int i = 0; i < n_cols; i++) sn->col_mask[i] = 1;
        sn->base.output_schema = vec_schema_copy(file_schema);
    } else {
        for (int i = 0; i < n_selected; i++) {
            if (col_indices[i] < 0 || col_indices[i] >= n_cols)
                vectra_error("column index out of range: %d", col_indices[i]);
            sn->col_mask[col_indices[i]] = 1;
        }
        /* Build output schema with only selected columns */
        char **sel_names = (char **)malloc((size_t)n_selected * sizeof(char *));
        VecType *sel_types = (VecType *)malloc((size_t)n_selected * sizeof(VecType));
        int j = 0;
        for (int i = 0; i < n_cols; i++) {
            if (sn->col_mask[i]) {
                sel_names[j] = file_schema->col_names[i];
                sel_types[j] = file_schema->col_types[i];
                j++;
            }
        }
        sn->base.output_schema = vec_schema_create(n_selected, sel_names, sel_types);
        free(sel_names);
        free(sel_types);
    }

    sn->base.next_batch = scan_next_batch;
    sn->base.free_node = scan_free;
    sn->base.kind = "ScanNode";

    return sn;
}
