#include "vtr_write.h"
#include "vtr1.h"
#include "optimize.h"
#include "batch.h"
#include "error.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void vtr_write_node(VecNode *node, const char *path) {
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
        vtr1_write_rowgroup(fp, batch);
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
