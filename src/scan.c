#include "scan.h"
#include "batch.h"
#include "schema.h"
#include "expr.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/* Check if a simple comparison predicate can be pruned using row group stats.
 * Returns 0 if the predicate is definitely false for all rows in this row group,
 * 1 if it might be true (or we can't determine). */
static int predicate_might_match(const VecExpr *pred, const Vtr1ColStat *stats,
                                  const VecSchema *schema) {
    if (!pred || !stats) return 1;

    /* Handle AND: both sides must possibly match */
    if (pred->kind == EXPR_BOOL && pred->op == '&') {
        return predicate_might_match(pred->left, stats, schema) &&
               predicate_might_match(pred->right, stats, schema);
    }

    /* Handle OR: at least one side must possibly match */
    if (pred->kind == EXPR_BOOL && pred->op == '|') {
        return predicate_might_match(pred->left, stats, schema) ||
               predicate_might_match(pred->right, stats, schema);
    }

    /* Handle simple comparison: col <op> literal */
    if (pred->kind != EXPR_CMP) return 1;

    /* Check if one side is a col_ref and the other is a literal */
    const VecExpr *col_expr = NULL;
    const VecExpr *lit_expr = NULL;
    int col_is_left = 0;

    if (pred->left->kind == EXPR_COL_REF &&
        (pred->right->kind == EXPR_LIT_INT64 ||
         pred->right->kind == EXPR_LIT_DOUBLE)) {
        col_expr = pred->left;
        lit_expr = pred->right;
        col_is_left = 1;
    } else if (pred->right->kind == EXPR_COL_REF &&
               (pred->left->kind == EXPR_LIT_INT64 ||
                pred->left->kind == EXPR_LIT_DOUBLE)) {
        col_expr = pred->right;
        lit_expr = pred->left;
        col_is_left = 0;
    } else {
        return 1; /* can't prune complex expressions */
    }

    /* Find column index */
    int col_idx = vec_schema_find_col(schema, col_expr->col_name);
    if (col_idx < 0) return 1;

    const Vtr1ColStat *st = &stats[col_idx];
    if (!st->has_stats) return 1;

    /* Get literal value as double */
    double lit_val;
    if (lit_expr->kind == EXPR_LIT_DOUBLE)
        lit_val = lit_expr->lit_dbl;
    else
        lit_val = (double)lit_expr->lit_i64;

    /* Get column min/max as double */
    double col_min, col_max;
    VecType ct = schema->col_types[col_idx];
    if (ct == VEC_INT64) {
        col_min = (double)st->i64.min;
        col_max = (double)st->i64.max;
    } else if (ct == VEC_DOUBLE) {
        col_min = st->dbl.min;
        col_max = st->dbl.max;
    } else {
        return 1; /* bool/string: can't prune */
    }

    char op = pred->op;
    char op2 = pred->op2;

    /* Flip operator if column is on the right: literal <op> column */
    if (!col_is_left) {
        if (op == '<') op = '>';
        else if (op == '>') op = '<';
        /* <= and >= flip similarly */
        if (op2 == '=') {
            /* already handled by the flip of op */
        }
        /* == and != are symmetric, no flip needed */
    }

    /* Now: col <op><op2> lit_val */
    if (op == '>' && op2 == ' ') return col_max > lit_val;
    if (op == '>' && op2 == '=') return col_max >= lit_val;
    if (op == '<' && op2 == ' ') return col_min < lit_val;
    if (op == '<' && op2 == '=') return col_min <= lit_val;
    if (op == '=' && op2 == '=') return col_min <= lit_val && col_max >= lit_val;
    if (op == '!' && op2 == '=') {
        /* Only prune if all values are the same and equal to lit_val */
        return !(col_min == lit_val && col_max == lit_val);
    }

    return 1; /* unknown op, don't prune */
}

static VecBatch *scan_next_batch(VecNode *self) {
    ScanNode *sn = (ScanNode *)self;

    while (sn->next_rg < sn->file->header.n_rowgroups) {
        /* Predicate pushdown: skip row groups that can't match */
        if (sn->predicate && sn->file->header.version >= 3) {
            Vtr1ColStat *stats = sn->file->rowgroups[sn->next_rg].col_stats;
            if (stats && !predicate_might_match(sn->predicate, stats,
                                                 &sn->file->header.schema)) {
                sn->next_rg++;
                continue;
            }
        }

        VecBatch *batch = vtr1_read_rowgroup(sn->file, sn->next_rg,
                                              sn->col_mask);
        sn->next_rg++;
        return batch;
    }
    return NULL;
}

static void scan_free(VecNode *self) {
    ScanNode *sn = (ScanNode *)self;
    if (sn->predicate && !sn->pred_borrowed)
        vec_expr_free(sn->predicate);
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
