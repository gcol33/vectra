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
 * The store's existing row groups are neither read nor rewritten. tdc writes
 * the new blocks from where the container's trailing index starts, then a
 * rebuilt row-group index, and patches the 64-byte header last (see
 * vtr1_tdc.h). The superseded index is written over rather than left behind,
 * so a store built by appending batch after batch holds one index and is as
 * long as the same row groups written by write_vtr().
 *
 * The container keeps its row-group index at its end, so there is no count to
 * bump at a fixed offset: an append cannot simply seek to EOF and write. It
 * works because that index is rebuilt wholesale on every close -- the entries
 * describing the existing row groups are carried over verbatim, and they stay
 * correct because nothing before the old index is touched.
 *
 * Peak memory is one row group. Until the header is patched it names the
 * original index, at its own offset or as a copy tdc keeps ahead of the bytes
 * being written, closed by the store's original trailer; so an interrupted
 * append leaves the store readable exactly as it was, with the same
 * fingerprint, and the next append starts over from that index.
 */

/* Open the store at path for a row append of columns named names[0..n_cols-1]
   with types types[0..n_cols-1], after checking them against its schema.
   compatible(i, file_type) says whether input column i can be written as the
   store's type; NULL requires the types to be equal. Returns the extender and
   the number of columns. */
typedef int (*append_type_ok)(const void *ctx, int i, VecType file_type);

static Vtr1TdcExtender *append_open(const char *path, int n_cols, const char *const *names,
                                    const VecType *types, append_type_ok ok, const void *ctx,
                                    VecType **file_types) {
    Vtr1TdcFile *existing = vtr1_open_tdc(path);
    if (!existing)
        vectra_error("append_vtr: cannot open existing file: %s", path);

    const VecSchema *file_schema = vtr1_tdc_schema(existing);

    /* Validate up-front against snapshots so any error path can format
       its message *after* freeing the open file handle without
       dereferencing freed schema strings. */
    if (n_cols != file_schema->n_cols) {
        int file_n = file_schema->n_cols;
        vtr1_close_tdc(existing);
        vectra_error("append_vtr: column count mismatch (file has %d, node has %d)",
                     file_n, n_cols);
    }
    for (int i = 0; i < file_schema->n_cols; i++) {
        if (strcmp(names[i], file_schema->col_names[i]) != 0) {
            char file_nm[256], node_nm[256];
            snprintf(file_nm, sizeof(file_nm), "%s", file_schema->col_names[i]);
            snprintf(node_nm, sizeof(node_nm), "%s", names[i]);
            vtr1_close_tdc(existing);
            vectra_error("append_vtr: column name mismatch at position %d "
                         "(file: '%s', node: '%s')",
                         i, file_nm, node_nm);
        }
        VecType ft = file_schema->col_types[i];
        if (ok ? !ok(ctx, i, ft) : types[i] != ft) {
            char file_nm[256];
            snprintf(file_nm, sizeof(file_nm), "%s", file_schema->col_names[i]);
            vtr1_close_tdc(existing);
            vectra_error("append_vtr: column type mismatch at column '%s'", file_nm);
        }
    }

    /* Snapshot the file schema so it outlives the read handle: the extender
       opens the same path for update, so the reader is closed first. */
    VecSchema schema_copy = vec_schema_copy(file_schema);
    if (file_types) {
        *file_types = (VecType *)malloc((size_t)n_cols * sizeof(VecType));
        for (int i = 0; i < n_cols; i++) (*file_types)[i] = schema_copy.col_types[i];
    }
    vtr1_close_tdc(existing);

    Vtr1TdcExtender *x = vtr1_open_tdc_extender(path, &schema_copy);
    vec_schema_free(&schema_copy);
    return x;
}

void vtr_append_node(VecNode *node, const char *path, int comp_level) {
    vec_optimize(node);
    const VecSchema *node_schema = &node->output_schema;
    int n_cols = node_schema->n_cols;
    Vtr1TdcExtender *x = append_open(path, n_cols, (const char *const *)node_schema->col_names,
                                     node_schema->col_types, NULL, NULL, NULL);

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

    if (fail) {
        /* Nothing is committed until close, so walking away here leaves the
           store exactly as it was found. */
        vtr1_abort_tdc_extender(x);
        vectra_error("append_vtr: the appended rows changed shape mid-stream");
    }

    vtr1_close_tdc_extender(x);
}

/* Whether R column i of a data.frame can be stored as the store's type: an
   integer column as any integer width, a factor or character column as
   string, the other columns as the type write_vtr() gives them. */
static int df_type_ok(const void *ctx, int i, VecType file_type) {
    SEXP col = VECTOR_ELT((SEXP)ctx, i);
    if (Rf_isInteger(col) && !Rf_isFactor(col))
        return file_type == VEC_INT8 || file_type == VEC_INT16 ||
               file_type == VEC_INT32 || file_type == VEC_INT64;
    return r_col_type(col) == file_type;
}

/* Rows of a data.frame appended to the store, converted straight from the R
   columns to the store's column types, in row groups of at most rg_rows. */
static void vtr_append_df(SEXP df, const char *path, int comp_level, int64_t rg_rows) {
    int n_cols = Rf_length(df);
    if (n_cols == 0) vectra_error("append_vtr: data.frame has no columns");
    int64_t n_rows = (int64_t)XLENGTH(VECTOR_ELT(df, 0));
    SEXP names = PROTECT(Rf_getAttrib(df, R_NamesSymbol));
    const char **nm = (const char **)R_alloc((size_t)n_cols, sizeof(char *));
    for (int i = 0; i < n_cols; i++) nm[i] = CHAR(STRING_ELT(names, i));

    VecType *types = NULL;
    Vtr1TdcExtender *x = append_open(path, n_cols, nm, NULL, df_type_ok, df, &types);
    for (int64_t start = 0; start < n_rows; start += rg_rows) {
        int64_t n = n_rows - start < rg_rows ? n_rows - start : rg_rows;
        VecBatch *batch = df_to_batch_range(df, start, n, types);
        vtr1_extend_rowgroup_tdc(x, batch, comp_level, NULL, NULL);
        vec_batch_free(batch);
    }
    free(types);
    vtr1_close_tdc_extender(x);
    UNPROTECT(1);
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

SEXP C_append_vtr_df(SEXP df, SEXP path_sexp, SEXP compress_sexp, SEXP rg_rows_sexp) {
    if (!Rf_isNewList(df)) vectra_error("append_vtr: first argument must be a data.frame");
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    int comp_level = parse_compress_level(compress_sexp);
    int64_t rg = (int64_t)Rf_asInteger(rg_rows_sexp);
    if (rg < 1) vectra_error("append_vtr: row group size must be positive");
    vtr_append_df(df, path, comp_level, rg);
    return R_NilValue;
}
