#include "vtr_append_cols.h"
#include "vtr1_tdc.h"
#include "schema.h"
#include "batch.h"
#include "builder.h"
#include "array.h"
#include "optimize.h"
#include "error.h"
#include "r_bridge.h"
#include "r_bridge_internal.h"
#include <stdlib.h>
#include <string.h>

/*
 * Column append.
 *
 * The store's existing columns are neither read nor rewritten -- that is
 * the whole point, and what separates this from vtr_append_node (row
 * append), which restreams the file through a fresh writer. tdc appends the
 * new blocks past the container's trailing index and patches the header
 * last (see vtr1_tdc.h), so peak memory is one row group of the incoming
 * columns and an interrupted append leaves the store as it was.
 *
 * The one thing that has to be arranged here is chunking. Row-group
 * boundaries are fixed by the existing file: a new column's block for row
 * group i must hold exactly that group's row count. The incoming node
 * arrives in whatever batches its own plan produces, so rows are
 * accumulated into builders and cut at the store's boundaries.
 */

/* Builders for the row group currently being filled. Rows are copied
   straight from the incoming batches into these, so a row is copied exactly
   once between arriving and being encoded. */
typedef struct {
    VecArrayBuilder *b;
    int              n_cols;
    int64_t          n_rows;    /* rows accumulated for the current group */
} ColAccum;

static void accum_init(ColAccum *a, const VecSchema *s) {
    a->n_cols = s->n_cols;
    a->n_rows = 0;
    a->b = (VecArrayBuilder *)calloc((size_t)a->n_cols, sizeof(VecArrayBuilder));
    if (!a->b) vectra_error("append_cols: alloc failed for builders");
    for (int c = 0; c < a->n_cols; c++)
        a->b[c] = vec_builder_init(s->col_types[c]);
}

static void accum_free(ColAccum *a) {
    if (!a->b) return;
    for (int c = 0; c < a->n_cols; c++) vec_builder_free(&a->b[c]);
    free(a->b);
    a->b = NULL;
}

/* Close out the accumulated rows as a batch and reset for the next group. */
static VecBatch *accum_finish(ColAccum *a, const VecSchema *s) {
    VecBatch *out = vec_batch_alloc(a->n_cols, a->n_rows);
    out->n_rows = a->n_rows;
    for (int c = 0; c < a->n_cols; c++) {
        out->columns[c] = vec_builder_finish(&a->b[c]);
        a->b[c] = vec_builder_init(s->col_types[c]);
        free(out->col_names[c]);
        out->col_names[c] = strdup(s->col_names[c]);
    }
    a->n_rows = 0;
    return out;
}

void vtr_append_cols_node(VecNode *node, const char *path, int comp_level) {
    vec_optimize(node);

    /* ---- Read the store's schema and row-group shape. ---- */
    Vtr1TdcFile *f = vtr1_open_tdc(path);
    if (!f) vectra_error("append_vtr: cannot open existing file: %s", path);

    VecSchema file_schema = vec_schema_copy(vtr1_tdc_schema(f));
    uint32_t n_rg = vtr1_tdc_n_rowgroups(f);
    int64_t *rg_rows = NULL;
    int64_t  total_rows = 0;
    if (n_rg > 0) {
        rg_rows = (int64_t *)malloc((size_t)n_rg * sizeof(int64_t));
        if (!rg_rows) {
            vec_schema_free(&file_schema);
            vtr1_close_tdc(f);
            vectra_error("append_cols: alloc failed for row-group sizes");
        }
        for (uint32_t i = 0; i < n_rg; i++) {
            rg_rows[i] = vtr1_tdc_rowgroup_n_rows(f, i);
            total_rows += rg_rows[i];
        }
    }
    vtr1_close_tdc(f);

    const VecSchema *node_schema = &node->output_schema;
    int n_new = node_schema->n_cols;

    if (n_new <= 0) {
        free(rg_rows);
        vec_schema_free(&file_schema);
        vectra_error("append_vtr(along = \"cols\"): no columns to append");
    }
    if (n_rg == 0) {
        free(rg_rows);
        vec_schema_free(&file_schema);
        vectra_error("append_vtr(along = \"cols\"): '%s' has no row groups; "
                     "columns can only be appended to a store that holds rows",
                     path);
    }

    /* Name collisions are rejected before anything is written. */
    VecSchema widened;
    {
        /* vec_schema_concat raises on a collision; snapshot what we need for
           the message first so nothing leaks through the longjmp. */
        for (int j = 0; j < n_new; j++) {
            if (vec_schema_find_col(&file_schema, node_schema->col_names[j]) >= 0) {
                char nm[256];
                snprintf(nm, sizeof(nm), "%s", node_schema->col_names[j]);
                free(rg_rows);
                vec_schema_free(&file_schema);
                vectra_error("append_vtr(along = \"cols\"): column '%s' already "
                             "exists in '%s'", nm, path);
            }
        }
        widened = vec_schema_concat(&file_schema, node_schema);
    }
    vec_schema_free(&file_schema);

    /* ---- Stream the new columns, cut at the store's boundaries. ---- */
    Vtr1TdcWidener *w = vtr1_open_tdc_widener(path, &widened, n_new);

    ColAccum acc;
    accum_init(&acc, node_schema);

    uint32_t rg = 0;
    int64_t  seen = 0;
    const char *fail = NULL;     /* set to abort after cleanup */
    int64_t     fail_seen = 0;

    /* Emit every row group whose quota is now met. Runs before the first
       batch too, so a leading zero-row group is closed out rather than
       waiting for rows that will never come. */
    #define FLUSH_COMPLETE_GROUPS()                                        \
        while (rg < n_rg && acc.n_rows == rg_rows[rg]) {                   \
            VecBatch *slice = accum_finish(&acc, node_schema);             \
            vtr1_widen_rowgroup_tdc(w, rg, slice, comp_level);             \
            vec_batch_free(slice);                                         \
            rg++;                                                          \
        }

    FLUSH_COMPLETE_GROUPS();

    VecBatch *batch;
    while ((batch = node->next_batch(node)) != NULL) {
        batch = vec_batch_compact(batch);

        if (batch->n_cols != n_new) {
            vec_batch_free(batch);
            fail = "column count changed mid-stream";
            break;
        }

        int64_t nb  = batch->n_rows;
        int64_t off = 0;
        while (off < nb) {
            if (rg >= n_rg) {
                /* More rows than the store holds. Stop now rather than
                   encode blocks that can never be committed. */
                fail = "too many rows";
                fail_seen = seen + (nb - off);
                break;
            }
            int64_t take = rg_rows[rg] - acc.n_rows;
            if (take > nb - off) take = nb - off;

            for (int c = 0; c < n_new; c++)
                vec_builder_append_range(&acc.b[c], &batch->columns[c],
                                         off, take);
            acc.n_rows += take;
            off        += take;
            seen       += take;

            FLUSH_COMPLETE_GROUPS();
        }
        vec_batch_free(batch);
        if (fail) break;
    }

    #undef FLUSH_COMPLETE_GROUPS

    if (!fail && rg != n_rg) {
        fail = "too few rows";
        fail_seen = seen;
    }

    accum_free(&acc);
    vec_schema_free(&widened);

    if (fail) {
        vtr1_abort_tdc_widener(w);
        free(rg_rows);
        if (strcmp(fail, "column count changed mid-stream") == 0)
            vectra_error("append_vtr(along = \"cols\"): the appended columns "
                         "changed shape mid-stream");
        vectra_error("append_vtr(along = \"cols\"): row count mismatch -- '%s' "
                     "holds %lld rows, the appended columns have %s%lld; "
                     "the store is unchanged",
                     path, (long long)total_rows,
                     (strcmp(fail, "too many rows") == 0) ? "at least " : "",
                     (long long)fail_seen);
    }

    vtr1_close_tdc_widener(w);
    free(rg_rows);
}

/* --- .Call bridge --- */

SEXP C_append_cols_vtr(SEXP node_xptr, SEXP path_sexp, SEXP compress_sexp) {
    VecNode *node = (VecNode *)R_ExternalPtrAddr(node_xptr);
    if (!node)
        vectra_error("this vectra query has already been consumed; a query runs once (collect, write_*, or another verb consumes it) -- rebuild the pipeline to run it again");
    /* Consume-once: invalidate before draining, as C_append_vtr does. */
    R_ClearExternalPtr(node_xptr);

    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    int comp_level = parse_compress_level(compress_sexp);

    vtr_append_cols_node(node, path, comp_level);
    node->free_node(node);
    return R_NilValue;
}
