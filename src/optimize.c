#include "optimize.h"
#include "scan.h"
#include "filter.h"
#include "project.h"
#include "sort.h"
#include "group_agg.h"
#include "limit.h"
#include "join.h"
#include "window.h"
#include "topn.h"
#include "concat.h"
#include "expr.h"
#include "schema.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/* ---- Column pruning ----
 *
 * Walk the plan tree top-down, computing which columns each node needs
 * from its child. At scan nodes, update col_mask to skip unneeded columns
 * from disk.
 */

static void mark_needed(const char *name, const VecSchema *schema,
                         uint8_t *needed) {
    for (int i = 0; i < schema->n_cols; i++) {
        if (strcmp(schema->col_names[i], name) == 0) {
            needed[i] = 1;
            return;
        }
    }
}

static void propagate_cols(VecNode *node, const uint8_t *parent_needed,
                            int parent_ncols);

static void prune_scan(ScanNode *sn, const uint8_t *parent_needed,
                        int parent_ncols) {
    const VecSchema *file_schema = &sn->file->header.schema;
    int n_file_cols = file_schema->n_cols;

    /* Map parent_needed (in output-schema order) to file col_mask */
    int *new_mask = (int *)calloc((size_t)n_file_cols, sizeof(int));
    int out_i = 0;
    for (int f = 0; f < n_file_cols; f++) {
        if (sn->col_mask[f]) {
            if (out_i < parent_ncols && parent_needed[out_i])
                new_mask[f] = 1;
            out_i++;
        }
    }

    int new_selected = 0;
    for (int f = 0; f < n_file_cols; f++)
        if (new_mask[f]) new_selected++;

    /* Only update if we actually pruned something */
    if (new_selected < out_i && new_selected > 0) {
        free(sn->col_mask);
        sn->col_mask = new_mask;

        /* Rebuild output schema */
        vec_schema_free(&sn->base.output_schema);
        char **sel_names = (char **)malloc((size_t)new_selected * sizeof(char *));
        VecType *sel_types = (VecType *)malloc(
            (size_t)new_selected * sizeof(VecType));
        int j = 0;
        for (int f = 0; f < n_file_cols; f++) {
            if (new_mask[f]) {
                sel_names[j] = file_schema->col_names[f];
                sel_types[j] = file_schema->col_types[f];
                j++;
            }
        }
        sn->base.output_schema = vec_schema_create(new_selected,
                                                     sel_names, sel_types);
        free(sel_names);
        free(sel_types);
    } else {
        free(new_mask);
    }
}

static void propagate_cols(VecNode *node, const uint8_t *parent_needed,
                            int parent_ncols) {
    const char *kind = node->kind ? node->kind : "";

    /* ---- Scan: terminal node ---- */
    if (strcmp(kind, "ScanNode") == 0) {
        prune_scan((ScanNode *)node, parent_needed, parent_ncols);
        return;
    }

    /* Non-.vtr scans: can't prune, stop */
    if (strcmp(kind, "CsvScanNode") == 0 ||
        strcmp(kind, "SqlScanNode") == 0 ||
        strcmp(kind, "TiffScanNode") == 0)
        return;

    /* ---- Filter: needs parent cols + predicate cols ---- */
    if (strcmp(kind, "FilterNode") == 0) {
        FilterNode *fn = (FilterNode *)node;
        const VecSchema *cs = &fn->child->output_schema;
        int cn = cs->n_cols;
        uint8_t *child_needed = (uint8_t *)calloc((size_t)cn, 1);

        /* Filter output schema == child output schema */
        for (int i = 0; i < parent_ncols && i < cn; i++)
            child_needed[i] = parent_needed[i];

        /* Predicate columns */
        vec_expr_collect_colrefs(fn->predicate, cs->col_names,
                                 cn, child_needed);

        propagate_cols(fn->child, child_needed, cn);
        free(child_needed);

        /* Sync output schema with child (child may have been pruned) */
        if (fn->child->output_schema.n_cols != fn->base.output_schema.n_cols) {
            vec_schema_free(&fn->base.output_schema);
            fn->base.output_schema = vec_schema_copy(&fn->child->output_schema);
        }
        return;
    }

    /* ---- Project: map output needs to child column refs ---- */
    if (strcmp(kind, "ProjectNode") == 0) {
        ProjectNode *pn = (ProjectNode *)node;
        const VecSchema *cs = &pn->child->output_schema;
        int cn = cs->n_cols;
        uint8_t *child_needed = (uint8_t *)calloc((size_t)cn, 1);

        /* Mark child columns needed by ALL project entries.
         * We can't skip entries because the project node will try to
         * look up pass-through columns even if the parent doesn't need them. */
        for (int i = 0; i < pn->n_entries; i++) {
            ProjEntry *pe = &pn->entries[i];
            if (!pe->expr) {
                mark_needed(pe->output_name, cs, child_needed);
            } else {
                /* Expression entry: also mark the column if it's a col_ref
                 * (rename case), plus any nested column references */
                vec_expr_collect_colrefs(pe->expr, cs->col_names,
                                         cn, child_needed);
            }
        }

        propagate_cols(pn->child, child_needed, cn);
        free(child_needed);
        return;
    }

    /* ---- Sort: needs parent cols + sort key cols ---- */
    if (strcmp(kind, "SortNode") == 0) {
        SortNode *sn = (SortNode *)node;
        const VecSchema *cs = &sn->child->output_schema;
        int cn = cs->n_cols;
        uint8_t *child_needed = (uint8_t *)calloc((size_t)cn, 1);

        /* Save sort key column names before pruning child (copy since
         * prune_scan may free the child schema that owns these strings) */
        char **sort_col_names = (char **)malloc((size_t)sn->n_keys * sizeof(char *));
        for (int k = 0; k < sn->n_keys; k++) {
            const char *src = cs->col_names[sn->keys[k].col_index];
            sort_col_names[k] = (char *)malloc(strlen(src) + 1);
            strcpy(sort_col_names[k], src);
        }

        for (int i = 0; i < parent_ncols && i < cn; i++)
            child_needed[i] = parent_needed[i];
        for (int k = 0; k < sn->n_keys; k++)
            if (sn->keys[k].col_index < cn)
                child_needed[sn->keys[k].col_index] = 1;

        propagate_cols(sn->child, child_needed, cn);
        free(child_needed);

        /* Sync output schema and recompute sort key indices */
        const VecSchema *new_cs = &sn->child->output_schema;
        if (new_cs->n_cols != cn) {
            for (int k = 0; k < sn->n_keys; k++)
                sn->keys[k].col_index = vec_schema_find_col(new_cs,
                                                              sort_col_names[k]);
            vec_schema_free(&sn->base.output_schema);
            sn->base.output_schema = vec_schema_copy(new_cs);
        }
        for (int k = 0; k < sn->n_keys; k++)
            free(sort_col_names[k]);
        free(sort_col_names);
        return;
    }

    /* ---- Limit: pass through ---- */
    if (strcmp(kind, "LimitNode") == 0) {
        LimitNode *ln = (LimitNode *)node;
        propagate_cols(ln->child, parent_needed, parent_ncols);
        if (ln->child->output_schema.n_cols != ln->base.output_schema.n_cols) {
            vec_schema_free(&ln->base.output_schema);
            ln->base.output_schema = vec_schema_copy(&ln->child->output_schema);
        }
        return;
    }

    /* ---- TopN: needs parent cols + sort key cols ---- */
    if (strcmp(kind, "TopNNode") == 0) {
        TopNNode *tn = (TopNNode *)node;
        const VecSchema *cs = &tn->child->output_schema;
        int cn = cs->n_cols;
        uint8_t *child_needed = (uint8_t *)calloc((size_t)cn, 1);

        char **sort_col_names = (char **)malloc((size_t)tn->n_keys * sizeof(char *));
        for (int k = 0; k < tn->n_keys; k++) {
            const char *src = cs->col_names[tn->keys[k].col_index];
            sort_col_names[k] = (char *)malloc(strlen(src) + 1);
            strcpy(sort_col_names[k], src);
        }

        for (int i = 0; i < parent_ncols && i < cn; i++)
            child_needed[i] = parent_needed[i];
        for (int k = 0; k < tn->n_keys; k++)
            if (tn->keys[k].col_index < cn)
                child_needed[tn->keys[k].col_index] = 1;

        propagate_cols(tn->child, child_needed, cn);
        free(child_needed);

        const VecSchema *new_cs = &tn->child->output_schema;
        if (new_cs->n_cols != cn) {
            for (int k = 0; k < tn->n_keys; k++)
                tn->keys[k].col_index = vec_schema_find_col(new_cs,
                                                              sort_col_names[k]);
            vec_schema_free(&tn->base.output_schema);
            tn->base.output_schema = vec_schema_copy(new_cs);
        }
        for (int k = 0; k < tn->n_keys; k++)
            free(sort_col_names[k]);
        free(sort_col_names);
        return;
    }

    /* ---- GroupAgg: needs key cols + agg input cols ---- */
    if (strcmp(kind, "GroupAggNode") == 0) {
        GroupAggNode *ga = (GroupAggNode *)node;
        const VecSchema *cs = &ga->child->output_schema;
        int cn = cs->n_cols;
        uint8_t *child_needed = (uint8_t *)calloc((size_t)cn, 1);

        for (int k = 0; k < ga->n_keys; k++)
            mark_needed(ga->key_names[k], cs, child_needed);
        for (int a = 0; a < ga->n_aggs; a++) {
            if (ga->agg_specs[a].input_col)
                mark_needed(ga->agg_specs[a].input_col, cs, child_needed);
        }

        propagate_cols(ga->child, child_needed, cn);
        free(child_needed);
        return;
    }

    /* ---- Window: needs parent cols + key cols + window input cols ---- */
    if (strcmp(kind, "WindowNode") == 0) {
        WindowNode *wn = (WindowNode *)node;
        const VecSchema *cs = &wn->child->output_schema;
        int cn = cs->n_cols;
        uint8_t *child_needed = (uint8_t *)calloc((size_t)cn, 1);

        /* Parent needs (excluding window output columns which are appended) */
        for (int i = 0; i < cn && i < parent_ncols; i++)
            child_needed[i] = parent_needed[i];

        /* Key columns */
        for (int k = 0; k < wn->n_keys; k++)
            mark_needed(wn->key_names[k], cs, child_needed);

        /* Window input columns */
        for (int w = 0; w < wn->n_wins; w++) {
            if (wn->win_specs[w].input_col)
                mark_needed(wn->win_specs[w].input_col, cs, child_needed);
        }

        propagate_cols(wn->child, child_needed, cn);
        free(child_needed);

        /* Window output = child cols + window result cols.
         * If child was pruned, rebuild output schema. */
        const VecSchema *new_cs = &wn->child->output_schema;
        if (new_cs->n_cols != cn) {
            int new_out = new_cs->n_cols + wn->n_wins;
            char **out_names = (char **)malloc((size_t)new_out * sizeof(char *));
            VecType *out_types = (VecType *)malloc((size_t)new_out * sizeof(VecType));
            for (int i = 0; i < new_cs->n_cols; i++) {
                out_names[i] = new_cs->col_names[i];
                out_types[i] = new_cs->col_types[i];
            }
            /* Append window output columns from old schema */
            const VecSchema *old_out = &wn->base.output_schema;
            for (int w = 0; w < wn->n_wins; w++) {
                int idx = cn + w;  /* window cols were after the old child cols */
                out_names[new_cs->n_cols + w] = old_out->col_names[idx];
                out_types[new_cs->n_cols + w] = old_out->col_types[idx];
            }
            VecSchema new_schema = vec_schema_create(new_out, out_names, out_types);
            vec_schema_free(&wn->base.output_schema);
            wn->base.output_schema = new_schema;
            free(out_names);
            free(out_types);
        }
        return;
    }

    /* ---- Join: propagate to both sides ---- */
    if (strcmp(kind, "JoinNode") == 0) {
        JoinNode *jn = (JoinNode *)node;

        /* Left side: need all left columns (we don't track which output
         * columns map to which side, so conservatively keep all) */
        const VecSchema *ls = &jn->left->output_schema;
        int ln = ls->n_cols;
        uint8_t *left_needed = (uint8_t *)malloc((size_t)ln);
        memset(left_needed, 1, (size_t)ln);

        const VecSchema *rs = &jn->right->output_schema;
        int rn = rs->n_cols;
        uint8_t *right_needed = (uint8_t *)malloc((size_t)rn);
        memset(right_needed, 1, (size_t)rn);

        propagate_cols(jn->left, left_needed, ln);
        propagate_cols(jn->right, right_needed, rn);
        free(left_needed);
        free(right_needed);
        return;
    }

    /* ---- Concat: propagate to all children ---- */
    if (strcmp(kind, "ConcatNode") == 0) {
        ConcatNode *cn = (ConcatNode *)node;
        for (int i = 0; i < cn->n_children; i++)
            propagate_cols(cn->children[i], parent_needed, parent_ncols);
        return;
    }

    /* Unknown node: stop propagation (safe default) */
}

/* ---- Predicate pushdown ----
 *
 * If a FilterNode sits directly above a ScanNode that has per-rowgroup
 * column statistics (vtr v3), attach the predicate to the scan node so
 * it can skip entire row groups.
 */

static void pushdown_predicates(VecNode *node) {
    const char *kind = node->kind ? node->kind : "";

    if (strcmp(kind, "FilterNode") == 0) {
        FilterNode *fn = (FilterNode *)node;

        /* Check if child is a ScanNode with v3 stats */
        if (fn->child->kind && strcmp(fn->child->kind, "ScanNode") == 0) {
            ScanNode *sn = (ScanNode *)fn->child;
            if (sn->file->header.version >= 3 && !sn->predicate) {
                /* Clone the predicate for the scan (filter keeps its copy) */
                sn->predicate = fn->predicate;
                sn->pred_borrowed = 1;  /* don't free on scan cleanup */
            }
        }

        /* Recurse into child */
        pushdown_predicates(fn->child);
        return;
    }

    /* Recurse into children */
    VecNode *children[16];
    int n_children = 0;

    if (strcmp(kind, "ProjectNode") == 0) {
        children[0] = ((ProjectNode *)node)->child;
        n_children = 1;
    } else if (strcmp(kind, "SortNode") == 0) {
        children[0] = ((SortNode *)node)->child;
        n_children = 1;
    } else if (strcmp(kind, "LimitNode") == 0) {
        children[0] = ((LimitNode *)node)->child;
        n_children = 1;
    } else if (strcmp(kind, "TopNNode") == 0) {
        children[0] = ((TopNNode *)node)->child;
        n_children = 1;
    } else if (strcmp(kind, "GroupAggNode") == 0) {
        children[0] = ((GroupAggNode *)node)->child;
        n_children = 1;
    } else if (strcmp(kind, "WindowNode") == 0) {
        children[0] = ((WindowNode *)node)->child;
        n_children = 1;
    } else if (strcmp(kind, "JoinNode") == 0) {
        JoinNode *jn = (JoinNode *)node;
        children[0] = jn->left;
        children[1] = jn->right;
        n_children = 2;
    } else if (strcmp(kind, "ConcatNode") == 0) {
        ConcatNode *cn = (ConcatNode *)node;
        int show = cn->n_children < 16 ? cn->n_children : 16;
        for (int i = 0; i < show; i++)
            children[i] = cn->children[i];
        n_children = show;
    }

    for (int i = 0; i < n_children; i++)
        pushdown_predicates(children[i]);
}

void vec_optimize(VecNode *root) {
    /* Pass 1: Predicate pushdown */
    pushdown_predicates(root);

    /* Pass 2: Column pruning */
    int n = root->output_schema.n_cols;
    if (n > 0) {
        uint8_t *needed = (uint8_t *)malloc((size_t)n);
        memset(needed, 1, (size_t)n);  /* root needs all its outputs */
        propagate_cols(root, needed, n);
        free(needed);
    }
}
