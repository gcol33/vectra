#include "vtr_write.h"
#include "vtr1.h"
#include "optimize.h"
#include "array.h"
#include "batch.h"
#include "builder.h"
#include "error.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void vtr_write_node_qs(VecNode *node, const char *path, int comp_level,
                       const VtrQuantizeSpec *qspecs,
                       const VtrSpatialSpec *sspecs) {
    vec_optimize(node);

    const VecSchema *schema = &node->output_schema;

    /* Build temp path: "{path}.~writing" */
    size_t path_len = strlen(path);
    char *tmp_path = (char *)malloc(path_len + 10);
    if (!tmp_path) vectra_error("alloc failed for tmp_path");
    memcpy(tmp_path, path, path_len);
    memcpy(tmp_path + path_len, ".~writing", 10); /* includes '\0' */

    FILE *fp = fopen(tmp_path, "wb");
    if (!fp) {
        free(tmp_path);
        vectra_error("cannot open file for writing: %s", path);
    }
    setvbuf(fp, NULL, _IOFBF, 256 * 1024); /* 256KB write buffer */

    /* Write header with n_rowgroups = 0 (placeholder) */
    vtr1_write_header(fp, schema, 0);

    /* The n_rowgroups field is the last 4 bytes of the header.
       Record its offset so we can patch it later. */
    long rg_count_pos = ftell(fp) - 4;

    /* Pull batches and write as row groups */
    uint32_t n_rg = 0;
    VecBatch *batch;
    while ((batch = node->next_batch(node)) != NULL) {
        /* Materialize selection vector if present */
        batch = vec_batch_compact(batch);
        vtr1_write_rowgroup_qs(fp, batch, comp_level, qspecs, sspecs);
        vec_batch_free(batch);
        n_rg++;
    }

    /* Patch the n_rowgroups count in the header */
    if (fseek(fp, rg_count_pos, SEEK_SET) != 0) {
        fclose(fp);
        remove(tmp_path);
        free(tmp_path);
        vectra_error("failed to seek in vtr file");
    }
    fwrite(&n_rg, sizeof(uint32_t), 1, fp);
    fclose(fp);

    /* Atomic rename: remove target first (required on Windows) */
    remove(path);
    if (rename(tmp_path, path) != 0) {
        /* Rename failed — try to clean up temp file */
        remove(tmp_path);
        free(tmp_path);
        vectra_error("failed to rename temp file to: %s", path);
    }

    free(tmp_path);
}

void vtr_write_node_q(VecNode *node, const char *path, int comp_level,
                      const VtrQuantizeSpec *qspecs) {
    vtr_write_node_qs(node, path, comp_level, qspecs, NULL);
}

void vtr_write_node(VecNode *node, const char *path, int comp_level) {
    vtr_write_node_qs(node, path, comp_level, NULL, NULL);
}

/* Flush builders as a VecBatch row group */
static void flush_builders(FILE *fp, VecArrayBuilder *builders, int n_cols,
                           int64_t n_rows, const VecSchema *schema,
                           uint32_t *n_rg, int comp_level,
                           const VtrQuantizeSpec *qspecs,
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
    vtr1_write_rowgroup_qs(fp, batch, comp_level, qspecs, sspecs);
    vec_batch_free(batch);
    (*n_rg)++;
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

    /* Build temp path */
    size_t path_len = strlen(path);
    char *tmp_path = (char *)malloc(path_len + 10);
    if (!tmp_path) vectra_error("alloc failed for tmp_path");
    memcpy(tmp_path, path, path_len);
    memcpy(tmp_path + path_len, ".~writing", 10);

    FILE *fp = fopen(tmp_path, "wb");
    if (!fp) { free(tmp_path); vectra_error("cannot open file for writing: %s", path); }
    setvbuf(fp, NULL, _IOFBF, 256 * 1024); /* 256KB write buffer */

    vtr1_write_header(fp, schema, 0);
    long rg_count_pos = ftell(fp) - 4;

    /* Initialize per-column builders */
    VecArrayBuilder *builders = (VecArrayBuilder *)malloc((size_t)n_cols * sizeof(VecArrayBuilder));
    if (!builders) vectra_error("builders alloc failed");
    for (int c = 0; c < n_cols; c++)
        builders[c] = vec_builder_init(schema->col_types[c]);

    uint32_t n_rg = 0;
    int64_t buffered = 0;
    VecBatch *batch;

    while ((batch = node->next_batch(node)) != NULL) {
        batch = vec_batch_compact(batch);
        for (int c = 0; c < n_cols; c++)
            vec_builder_append_array(&builders[c], &batch->columns[c]);
        buffered += batch->n_rows;
        vec_batch_free(batch);

        while (buffered >= batch_size) {
            flush_builders(fp, builders, n_cols, buffered, schema, &n_rg,
                           comp_level, qspecs, sspecs);
            buffered = 0;
            for (int c = 0; c < n_cols; c++)
                builders[c] = vec_builder_init(schema->col_types[c]);
        }
    }

    flush_builders(fp, builders, n_cols, buffered, schema, &n_rg,
                   comp_level, qspecs, sspecs);
    free(builders);

    if (fseek(fp, rg_count_pos, SEEK_SET) != 0) {
        fclose(fp); remove(tmp_path); free(tmp_path);
        vectra_error("failed to seek in vtr file");
    }
    fwrite(&n_rg, sizeof(uint32_t), 1, fp);
    fclose(fp);

    remove(path);
    if (rename(tmp_path, path) != 0) {
        remove(tmp_path); free(tmp_path);
        vectra_error("failed to rename temp file to: %s", path);
    }
    free(tmp_path);
}

void vtr_write_node_batched_q(VecNode *node, const char *path, int64_t batch_size,
                              int comp_level, const VtrQuantizeSpec *qspecs) {
    vtr_write_node_batched_qs(node, path, batch_size, comp_level, qspecs, NULL);
}

void vtr_write_node_batched(VecNode *node, const char *path, int64_t batch_size,
                            int comp_level) {
    vtr_write_node_batched_qs(node, path, batch_size, comp_level, NULL, NULL);
}
