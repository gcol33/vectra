#include "vtr_append.h"
#include "vtr1_tdc.h"
#include "batch.h"
#include "schema.h"
#include "optimize.h"
#include "error.h"
#include "r_bridge.h"
#include "r_bridge_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Row append.
 *
 * The store's existing row groups are neither read nor rewritten. tdc appends
 * the new blocks and a rebuilt row-group index past the container's trailing
 * index and patches the 64-byte header last (see vtr1_tdc.h), so the cost
 * tracks the rows being appended rather than the size of the store, and
 * building a store by appending batch after batch is linear in the rows
 * written rather than quadratic.
 *
 * The container keeps its row-group index in the trailer, so there is no count
 * to bump at a fixed offset: an append cannot simply seek to EOF and write. It
 * works anyway because that index is rebuilt wholesale on every close -- the
 * entries describing the existing row groups are carried over verbatim, and
 * they stay correct because nothing before the old trailer is touched.
 *
 * Peak memory is one row group, and an interrupted append leaves the store
 * exactly as it was -- the appended bytes sit past the trailer referenced by
 * nothing, and the next append writes over them.
 */

void vtr_append_node(VecNode *node, const char *path, int comp_level) {
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

    /* Snapshot the file schema so it outlives the read handle: the extender
       opens the same path for update, so the reader is closed first. */
    VecSchema schema_copy = vec_schema_copy(file_schema);
    int n_cols = file_schema->n_cols;
    vtr1_close_tdc(existing);

    Vtr1TdcExtender *x = vtr1_open_tdc_extender(path, &schema_copy);

    int fail = 0;
    VecBatch *batch;
    while ((batch = node->next_batch(node)) != NULL) {
        batch = vec_batch_compact(batch);
        if (batch->n_cols != n_cols) {
            vec_batch_free(batch);
            fail = 1;
            break;
        }
        vtr1_extend_rowgroup_tdc(x, batch, comp_level, NULL, NULL);
        vec_batch_free(batch);
    }

    vec_schema_free(&schema_copy);

    if (fail) {
        /* Nothing is committed until close, so walking away here leaves the
           store exactly as it was found. */
        vtr1_abort_tdc_extender(x);
        vectra_error("append_vtr: the appended rows changed shape mid-stream");
    }

    vtr1_close_tdc_extender(x);
}

/* --- .Call bridge --- */

static VecNode *unwrap_node_for_append(SEXP xptr) {
    VecNode *node = (VecNode *)R_ExternalPtrAddr(xptr);
    if (!node) vectra_error("this vectra query has already been consumed; a query runs once (collect, write_*, or another verb consumes it) -- rebuild the pipeline to run it again");
    return node;
}

SEXP C_append_vtr(SEXP node_xptr, SEXP path_sexp, SEXP compress_sexp) {
    VecNode *node = unwrap_node_for_append(node_xptr);
    /* Consume-once: invalidate the handle before draining, so a later terminal
       op on the same node errors clearly rather than re-running an exhausted
       plan (mirrors write_node_dispatch and C_collect). */
    R_ClearExternalPtr(node_xptr);
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    int comp_level = parse_compress_level(compress_sexp);
    vtr_append_node(node, path, comp_level);
    node->free_node(node);
    return R_NilValue;
}
