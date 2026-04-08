#include "vtr_append.h"
#include "vtr1.h"
#include "vtr_write.h"
#include "batch.h"
#include "schema.h"
#include "optimize.h"
#include "error.h"
#include "r_bridge.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Helper: compute the byte offset of the n_rowgroups uint32 in the header.
   Header layout (v4):
     magic        4 bytes
     version      2 bytes
     n_cols       2 bytes
     per column:  2 (name_len) + name_len + 1 (type) + 2 (ann_len) + ann_len
     n_rowgroups  4 bytes   <-- this is what we want to patch
*/
static long compute_rg_count_offset(const VecSchema *schema) {
    long off = 4 + 2 + 2; /* magic + version + n_cols */
    for (int i = 0; i < schema->n_cols; i++) {
        uint16_t name_len = (uint16_t)strlen(schema->col_names[i]);
        off += 2 + name_len + 1; /* name_len(2) + name + type(1) */
        const char *ann = schema->col_annotations ? schema->col_annotations[i] : NULL;
        uint16_t ann_len = ann ? (uint16_t)strlen(ann) : 0;
        off += 2 + ann_len; /* ann_len(2) + ann */
    }
    /* n_rowgroups is at this offset */
    return off;
}

void vtr_append_node(VecNode *node, const char *path) {
    vec_optimize(node);

    /* 1. Open the existing file to read the current n_rowgroups and validate schema. */
    Vtr1File *existing = vtr1_open(path);
    uint32_t existing_n_rg = existing->header.n_rowgroups;
    const VecSchema *file_schema = &existing->header.schema;

    /* Validate schema matches the node's output schema */
    const VecSchema *node_schema = &node->output_schema;
    if (node_schema->n_cols != file_schema->n_cols)
        vectra_error("append_vtr: column count mismatch (file has %d, node has %d)",
                     file_schema->n_cols, node_schema->n_cols);
    for (int i = 0; i < file_schema->n_cols; i++) {
        if (strcmp(node_schema->col_names[i], file_schema->col_names[i]) != 0)
            vectra_error("append_vtr: column name mismatch at position %d "
                         "(file: '%s', node: '%s')",
                         i, file_schema->col_names[i], node_schema->col_names[i]);
        if (node_schema->col_types[i] != file_schema->col_types[i])
            vectra_error("append_vtr: column type mismatch at column '%s'",
                         file_schema->col_names[i]);
    }

    /* Record header offset for later patching and close the read handle */
    long rg_count_pos = compute_rg_count_offset(file_schema);
    vtr1_close(existing);

    /* 2. Open the file in append+update mode ("r+b") to write at the end
       and patch the header.  We cannot use "ab" because that forces all
       writes to EOF but we also need to seek back to patch the header. */
    FILE *fp = fopen(path, "r+b");
    if (!fp)
        vectra_error("append_vtr: cannot open file for update: %s", path);

    /* Seek to end to append new row groups */
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        vectra_error("append_vtr: cannot seek to end of file: %s", path);
    }

    /* 3. Pull batches from node and write as new row groups */
    uint32_t new_rg = 0;
    VecBatch *batch;
    while ((batch = node->next_batch(node)) != NULL) {
        batch = vec_batch_compact(batch);
        vtr1_write_rowgroup(fp, batch, VTR_COMPRESS_FAST);
        vec_batch_free(batch);
        new_rg++;
    }

    /* 4. Patch n_rowgroups in the header */
    uint32_t total_rg = existing_n_rg + new_rg;
    if (fseek(fp, rg_count_pos, SEEK_SET) != 0) {
        fclose(fp);
        vectra_error("append_vtr: cannot seek to rowgroup count in header: %s", path);
    }
    if (fwrite(&total_rg, sizeof(uint32_t), 1, fp) != 1) {
        fclose(fp);
        vectra_error("append_vtr: failed to update rowgroup count: %s", path);
    }

    fclose(fp);
}

/* --- .Call bridge --- */

/* Forward declarations from r_bridge.c (already static there, so we duplicate
   the minimal unwrap logic here to avoid exposing internal statics). */
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
