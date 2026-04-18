#include "vtr_write.h"
#include "vtr1_tdc.h"
#include "optimize.h"
#include "array.h"
#include "batch.h"
#include "builder.h"
#include "error.h"
#include "vtr_atomic_rename.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The tdc writer writes directly to the path it's opened on. We preserve
   vectra's atomic-rename guarantee (other readers of `path` never see a
   half-written file) by writing to "<path>.~writing" first, then
   removing the target and renaming the temp file over it on close. */

static char *make_tmp_path(const char *path) {
    size_t path_len = strlen(path);
    char *tmp_path = (char *)malloc(path_len + 10);
    if (!tmp_path) vectra_error("alloc failed for tmp_path");
    memcpy(tmp_path, path, path_len);
    memcpy(tmp_path + path_len, ".~writing", 10); /* includes '\0' */
    return tmp_path;
}

static void atomic_swap(char *tmp_path, const char *path) {
    if (vtr_atomic_replace(tmp_path, path) != 0) {
        remove(tmp_path);
        free(tmp_path);
        vectra_error("failed to rename temp file to: %s", path);
    }
    free(tmp_path);
}

void vtr_write_node_qs(VecNode *node, const char *path, int comp_level,
                       const VtrQuantizeSpec *qspecs,
                       const VtrSpatialSpec *sspecs) {
    vec_optimize(node);

    const VecSchema *schema = &node->output_schema;

    char *tmp_path = make_tmp_path(path);
    Vtr1TdcWriter *w = vtr1_open_tdc_writer(tmp_path, schema);

    VecBatch *batch;
    while ((batch = node->next_batch(node)) != NULL) {
        batch = vec_batch_compact(batch);
        vtr1_write_rowgroup_tdc(w, batch, comp_level, qspecs, sspecs);
        vec_batch_free(batch);
    }

    vtr1_close_tdc_writer(w);
    atomic_swap(tmp_path, path);
}

void vtr_write_node_q(VecNode *node, const char *path, int comp_level,
                      const VtrQuantizeSpec *qspecs) {
    vtr_write_node_qs(node, path, comp_level, qspecs, NULL);
}

void vtr_write_node(VecNode *node, const char *path, int comp_level) {
    vtr_write_node_qs(node, path, comp_level, NULL, NULL);
}

/* Flush builders as a VecBatch row group */
static void flush_builders(Vtr1TdcWriter *w, VecArrayBuilder *builders,
                           int n_cols, int64_t n_rows, const VecSchema *schema,
                           int comp_level, const VtrQuantizeSpec *qspecs,
                           const VtrSpatialSpec *sspecs) {
    if (n_rows == 0) return;
    VecBatch *batch = vec_batch_alloc(n_cols, n_rows);
    for (int c = 0; c < n_cols; c++) {
        vec_array_free(&batch->columns[c]);
        batch->columns[c] = vec_builder_finish(&builders[c]);
        free(batch->col_names[c]);
        batch->col_names[c] = (char *)malloc(strlen(schema->col_names[c]) + 1);
        strcpy(batch->col_names[c], schema->col_names[c]);
    }
    vtr1_write_rowgroup_tdc(w, batch, comp_level, qspecs, sspecs);
    vec_batch_free(batch);
}

void vtr_write_node_batched_qs(VecNode *node, const char *path, int64_t batch_size,
                               int comp_level, const VtrQuantizeSpec *qspecs,
                               const VtrSpatialSpec *sspecs) {
    if (batch_size <= 0) {
        vtr_write_node_qs(node, path, comp_level, qspecs, sspecs);
        return;
    }

    vec_optimize(node);
    const VecSchema *schema = &node->output_schema;
    int n_cols = schema->n_cols;

    char *tmp_path = make_tmp_path(path);
    Vtr1TdcWriter *w = vtr1_open_tdc_writer(tmp_path, schema);

    /* Initialize per-column builders */
    VecArrayBuilder *builders = (VecArrayBuilder *)malloc((size_t)n_cols * sizeof(VecArrayBuilder));
    if (!builders) vectra_error("builders alloc failed");
    for (int c = 0; c < n_cols; c++)
        builders[c] = vec_builder_init(schema->col_types[c]);

    int64_t buffered = 0;
    VecBatch *batch;

    while ((batch = node->next_batch(node)) != NULL) {
        batch = vec_batch_compact(batch);
        for (int c = 0; c < n_cols; c++)
            vec_builder_append_array(&builders[c], &batch->columns[c]);
        buffered += batch->n_rows;
        vec_batch_free(batch);

        while (buffered >= batch_size) {
            flush_builders(w, builders, n_cols, buffered, schema,
                           comp_level, qspecs, sspecs);
            buffered = 0;
            for (int c = 0; c < n_cols; c++)
                builders[c] = vec_builder_init(schema->col_types[c]);
        }
    }

    flush_builders(w, builders, n_cols, buffered, schema,
                   comp_level, qspecs, sspecs);
    free(builders);

    vtr1_close_tdc_writer(w);
    atomic_swap(tmp_path, path);
}

void vtr_write_node_batched_q(VecNode *node, const char *path, int64_t batch_size,
                              int comp_level, const VtrQuantizeSpec *qspecs) {
    vtr_write_node_batched_qs(node, path, batch_size, comp_level, qspecs, NULL);
}

void vtr_write_node_batched(VecNode *node, const char *path, int64_t batch_size,
                            int comp_level) {
    vtr_write_node_batched_qs(node, path, batch_size, comp_level, NULL, NULL);
}
