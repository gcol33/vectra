#include "vtr_append.h"
#include "vtr1_tdc.h"
#include "batch.h"
#include "schema.h"
#include "optimize.h"
#include "error.h"
#include "r_bridge.h"
#include "vtr_atomic_rename.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The tdc container keeps its row-group index in the trailer and
   patches header pointers on close, so v4-style in-place append (seek
   to EOF, write rgs, patch header n_rowgroups) is structurally not an
   option. Instead: stream all existing rgs through a fresh writer
   targeting a temp file, append the new rgs from the node, then
   atomically swap the temp over the original. */

void vtr_append_node(VecNode *node, const char *path) {
    vec_optimize(node);

    Vtr1TdcFile *existing = vtr1_open_tdc(path);
    if (!existing)
        vectra_error("append_vtr: cannot open existing file: %s", path);

    const VecSchema *file_schema = vtr1_tdc_schema(existing);
    const VecSchema *node_schema = &node->output_schema;

    /* Validate up-front against snapshots so any error path can format
       its message *after* freeing the open file handle without
       dereferencing freed schema strings. */
    if (node_schema->n_cols != file_schema->n_cols) {
        int file_n = file_schema->n_cols;
        int node_n = node_schema->n_cols;
        vtr1_close_tdc(existing);
        vectra_error("append_vtr: column count mismatch (file has %d, node has %d)",
                     file_n, node_n);
    }
    for (int i = 0; i < file_schema->n_cols; i++) {
        if (strcmp(node_schema->col_names[i], file_schema->col_names[i]) != 0) {
            char file_nm[256], node_nm[256];
            snprintf(file_nm, sizeof(file_nm), "%s", file_schema->col_names[i]);
            snprintf(node_nm, sizeof(node_nm), "%s", node_schema->col_names[i]);
            vtr1_close_tdc(existing);
            vectra_error("append_vtr: column name mismatch at position %d "
                         "(file: '%s', node: '%s')",
                         i, file_nm, node_nm);
        }
        if (node_schema->col_types[i] != file_schema->col_types[i]) {
            char file_nm[256];
            snprintf(file_nm, sizeof(file_nm), "%s", file_schema->col_names[i]);
            vtr1_close_tdc(existing);
            vectra_error("append_vtr: column type mismatch at column '%s'", file_nm);
        }
    }

    /* Snapshot the file schema so we can keep the writer alive after
       closing the read handle. */
    VecSchema schema_copy = vec_schema_copy(file_schema);

    int n_cols = file_schema->n_cols;
    int *all_cols = (int *)malloc((size_t)n_cols * sizeof(int));
    if (!all_cols) {
        vec_schema_free(&schema_copy);
        vtr1_close_tdc(existing);
        vectra_error("append_vtr: alloc failed");
    }
    for (int c = 0; c < n_cols; c++) all_cols[c] = 1;

    size_t path_len = strlen(path);
    char *tmp_path = (char *)malloc(path_len + 10);
    if (!tmp_path) {
        free(all_cols);
        vec_schema_free(&schema_copy);
        vtr1_close_tdc(existing);
        vectra_error("append_vtr: alloc failed for tmp_path");
    }
    memcpy(tmp_path, path, path_len);
    memcpy(tmp_path + path_len, ".~append", 9);

    Vtr1TdcWriter *w = vtr1_open_tdc_writer(tmp_path, &schema_copy);

    uint32_t n_rg = vtr1_tdc_n_rowgroups(existing);
    for (uint32_t rg = 0; rg < n_rg; rg++) {
        VecBatch *batch = vtr1_read_rowgroup_tdc(existing, rg, all_cols);
        vtr1_write_rowgroup_tdc(w, batch, VTR_COMPRESS_FAST, NULL, NULL);
        vec_batch_free(batch);
    }
    vtr1_close_tdc(existing);
    free(all_cols);

    VecBatch *batch;
    while ((batch = node->next_batch(node)) != NULL) {
        batch = vec_batch_compact(batch);
        vtr1_write_rowgroup_tdc(w, batch, VTR_COMPRESS_FAST, NULL, NULL);
        vec_batch_free(batch);
    }

    vtr1_close_tdc_writer(w);
    vec_schema_free(&schema_copy);

    if (vtr_atomic_replace(tmp_path, path) != 0) {
        remove(tmp_path);
        free(tmp_path);
        vectra_error("append_vtr: failed to rename temp file to: %s", path);
    }
    free(tmp_path);
}

/* --- .Call bridge --- */

static VecNode *unwrap_node_for_append(SEXP xptr) {
    VecNode *node = (VecNode *)R_ExternalPtrAddr(xptr);
    if (!node) vectra_error("vectra node has been freed or collected");
    return node;
}

SEXP C_append_vtr(SEXP node_xptr, SEXP path_sexp) {
    VecNode *node = unwrap_node_for_append(node_xptr);
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    vtr_append_node(node, path);
    return R_NilValue;
}
