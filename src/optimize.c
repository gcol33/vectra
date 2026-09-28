#include "optimize.h"
#include "plan_budget.h"
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
#include "parquet_scan.h"
#include "expr.h"
#include "schema.h"
#include "coerce.h"
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
    const VecSchema *file_schema = vtr1_tdc_schema(sn->file);
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

/* Remove the project entries the parent does not read, so the columns only they
   referenced are no longer demanded from the child. Entries are evaluated in
   order and an expression may name an earlier entry's output, so an entry is
   kept when the parent needs it or a kept later expression names it (a name
   that resolves to a child column instead only makes the entry survive
   needlessly). At least one entry is kept so batches still carry their row
   count: a pass-through if there is one, else the first entry, which cannot
   depend on an earlier one. */
static void project_drop_unneeded(ProjectNode *pn, const uint8_t *parent_needed,
                                  int parent_ncols) {
    int n = pn->n_entries;
    if (n == 0 || parent_ncols != n) return;

    uint8_t *keep = (uint8_t *)calloc((size_t)n, 1);
    char **names = (char **)malloc((size_t)n * sizeof(char *));
    for (int i = 0; i < n; i++) names[i] = pn->entries[i].output_name;

    for (int i = n - 1; i >= 0; i--) {
        if (parent_needed[i]) keep[i] = 1;
        if (keep[i] && pn->entries[i].expr && i > 0)
            vec_expr_collect_colrefs(pn->entries[i].expr, names, i, keep);
    }
    free(names);

    int n_keep = 0;
    for (int i = 0; i < n; i++) n_keep += keep[i];
    if (n_keep == n) { free(keep); return; }
    if (n_keep == 0) {
        int pick = 0;
        for (int i = 0; i < n; i++)
            if (!pn->entries[i].expr) { pick = i; break; }
        keep[pick] = 1;
        n_keep = 1;
    }

    ProjEntry *kept = (ProjEntry *)malloc((size_t)n_keep * sizeof(ProjEntry));
    char **out_names = (char **)malloc((size_t)n_keep * sizeof(char *));
    VecType *out_types = (VecType *)malloc((size_t)n_keep * sizeof(VecType));
    const VecSchema *old = &pn->base.output_schema;
    int j = 0;
    for (int i = 0; i < n; i++) {
        if (keep[i]) {
            kept[j] = pn->entries[i];
            out_names[j] = pn->entries[i].output_name;
            out_types[j] = old->col_types[i];
            j++;
        } else {
            free(pn->entries[i].output_name);
            vec_expr_free(pn->entries[i].expr);
        }
    }
    VecSchema schema = vec_schema_create(n_keep, out_names, out_types);
    vec_schema_free(&pn->base.output_schema);
    pn->base.output_schema = schema;
    free(pn->entries);
    pn->entries = kept;
    pn->n_entries = n_keep;
    free(out_names);
    free(out_types);
    free(keep);
}

/* Output schema of a concat: the first child's names, types widened to the
   common type across children, as concat_node_create builds it. */
static void concat_sync_schema(ConcatNode *cn) {
    VecSchema s = vec_schema_copy(&cn->children[0]->output_schema);
    for (int i = 1; i < cn->n_children; i++) {
        const VecSchema *cs = &cn->children[i]->output_schema;
        for (int c = 0; c < s.n_cols && c < cs->n_cols; c++)
            if (s.col_types[c] != cs->col_types[c])
                s.col_types[c] = vec_common_type(s.col_types[c],
                                                 cs->col_types[c]);
    }
    vec_schema_free(&cn->base.output_schema);
    cn->base.output_schema = s;
}

static void propagate_cols(VecNode *node, const uint8_t *parent_needed,
                            int parent_ncols) {
    const char *kind = node->kind ? node->kind : "";

    /* ---- Scan: terminal node ---- */
    if (strcmp(kind, "ScanNode") == 0) {
        prune_scan((ScanNode *)node, parent_needed, parent_ncols);
        return;
    }

    if (strcmp(kind, "ParquetScanNode") == 0) {
        parquet_scan_prune(node, parent_needed, parent_ncols);
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

    /* ---- Project: drop unneeded entries, map the rest to child refs ---- */
    if (strcmp(kind, "ProjectNode") == 0) {
        ProjectNode *pn = (ProjectNode *)node;
        project_drop_unneeded(pn, parent_needed, parent_ncols);

        const VecSchema *cs = &pn->child->output_schema;
        int cn = cs->n_cols;
        uint8_t *child_needed = (uint8_t *)calloc((size_t)cn, 1);

        for (int i = 0; i < pn->n_entries; i++) {
            ProjEntry *pe = &pn->entries[i];
            if (!pe->expr)
                mark_needed(pe->output_name, cs, child_needed);
            else
                vec_expr_collect_colrefs(pe->expr, cs->col_names,
                                         cn, child_needed);
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
            if (wn->win_specs[w].order_col)
                mark_needed(wn->win_specs[w].order_col, cs, child_needed);
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
        if (cn->children[0]->output_schema.n_cols != cn->base.output_schema.n_cols)
            concat_sync_schema(cn);
        return;
    }

    /* Unknown node: stop propagation (safe default) */
}

/* ---- Predicate pushdown ----
 *
 * A filter keeps running where the user wrote it; what moves down is a
 * row-group pruning copy of its predicate, handed to every .vtr scan the
 * filter's rows come from, so zone maps, binary search and .vtri indexes apply
 * whatever sits in between. The scan only skips row groups with it, so the copy
 * need only be implied by the predicate, never equal to it: it keeps the parts
 * a scan can evaluate against statistics (AND / OR of `col <cmp> literal` and
 * `col %in% set`), drops an AND conjunct it cannot keep (dropping one weakens
 * the predicate, which is sound) and gives up an OR or a negation it cannot
 * keep whole.
 *
 * The copy descends through nodes that pass rows unchanged in value and only
 * drop or reorder them in ways the filter above would not undo: projections
 * (column references are renamed to the child's names; a reference to a
 * computed column makes its conjunct unpushable), filters, sorts, and concats
 * (one copy per input whose referenced columns carry the concat's own types).
 * It stops at limits, top-n, windows, aggregates and joins, where removing
 * input rows changes the rows or values that come out.
 */

static int is_prunable_lit(const VecExpr *e) {
    return e && (e->kind == EXPR_LIT_INT64 || e->kind == EXPR_LIT_DOUBLE ||
                 e->kind == EXPR_LIT_STRING);
}

static char *dup_str(const char *s) {
    if (!s) return NULL;
    size_t n = strlen(s) + 1;
    char *d = (char *)malloc(n);
    if (!d) vectra_error("alloc failed in optimizer");
    memcpy(d, s, n);
    return d;
}

/* Copy of one node: every scalar field, and of the owned pointers only the
   ones a pruning node uses (column name, string literal, %in% set). Children
   are attached by the caller. */
static VecExpr *copy_node(const VecExpr *e) {
    VecExpr *c = vec_expr_alloc(e->kind);
    *c = *e;
    c->col_name = NULL; c->lit_str = NULL;
    c->left = c->right = c->operand = NULL;
    c->cond = c->then_expr = c->else_expr = NULL;
    c->set_dbl = NULL; c->set_i64 = NULL; c->set_str = NULL;
    c->gsub_pattern = c->gsub_replacement = NULL;
    c->children = NULL; c->n_children = 0;
    c->paste_sep = NULL;

    c->col_name = dup_str(e->col_name);
    c->lit_str = dup_str(e->lit_str);
    if (e->kind == EXPR_IN) {
        size_t n = (size_t)(e->n_set > 0 ? e->n_set : 0);
        if (e->set_dbl) {
            c->set_dbl = (double *)malloc(n * sizeof(double) + 1);
            if (!c->set_dbl) vectra_error("alloc failed in optimizer");
            memcpy(c->set_dbl, e->set_dbl, n * sizeof(double));
        }
        if (e->set_i64) {
            c->set_i64 = (int64_t *)malloc(n * sizeof(int64_t) + 1);
            if (!c->set_i64) vectra_error("alloc failed in optimizer");
            memcpy(c->set_i64, e->set_i64, n * sizeof(int64_t));
        }
        if (e->set_str) {
            c->set_str = (char **)calloc(n + 1, sizeof(char *));
            if (!c->set_str) vectra_error("alloc failed in optimizer");
            for (size_t i = 0; i < n; i++) c->set_str[i] = dup_str(e->set_str[i]);
        }
    }
    return c;
}

static VecExpr *make_bool(char op, VecExpr *l, VecExpr *r) {
    VecExpr *a = vec_expr_alloc(EXPR_BOOL);
    a->op = op;
    a->result_type = VEC_BOOL;
    a->left = l;
    a->right = r;
    return a;
}

/* AND of two optional predicates (NULL = no constraint). */
static VecExpr *and_opt(VecExpr *l, VecExpr *r) {
    if (!l) return r;
    if (!r) return l;
    return make_bool('&', l, r);
}

/* OR of two optional predicates: a side with no constraint makes the whole
   disjunction unconstrained. */
static VecExpr *or_opt(VecExpr *l, VecExpr *r) {
    if (!l || !r) { vec_expr_free(l); vec_expr_free(r); return NULL; }
    return make_bool('|', l, r);
}

/* The pruning copy of `e` (see above), or NULL when nothing of it survives. */
static VecExpr *prunable_copy(const VecExpr *e) {
    if (!e) return NULL;
    if (e->kind == EXPR_BOOL && (e->op == '&' || e->op == '|') &&
        e->left && e->right) {
        VecExpr *l = prunable_copy(e->left);
        VecExpr *r = prunable_copy(e->right);
        return e->op == '&' ? and_opt(l, r) : or_opt(l, r);
    }
    if (e->kind == EXPR_CMP && e->left && e->right &&
        ((e->left->kind == EXPR_COL_REF && is_prunable_lit(e->right)) ||
         (e->right->kind == EXPR_COL_REF && is_prunable_lit(e->left)))) {
        VecExpr *c = copy_node(e);
        c->left = copy_node(e->left);
        c->right = copy_node(e->right);
        return c;
    }
    if (e->kind == EXPR_IN && e->operand && e->operand->kind == EXPR_COL_REF) {
        VecExpr *c = copy_node(e);
        c->operand = copy_node(e->operand);
        return c;
    }
    return NULL;
}

/* The column reference inside a pruning leaf. */
static VecExpr *leaf_colref(const VecExpr *e) {
    if (e->kind == EXPR_IN) return e->operand;
    return e->left->kind == EXPR_COL_REF ? e->left : e->right;
}

/* Rewrite the pruning predicate `e` (owned) into the child column names of
   `pn`, dropping what refers to a computed column. */
static VecExpr *through_project(VecExpr *e, const ProjectNode *pn) {
    if (e->kind == EXPR_BOOL) {
        VecExpr *l = through_project(e->left, pn);
        VecExpr *r = through_project(e->right, pn);
        char op = e->op;
        e->left = e->right = NULL;
        vec_expr_free(e);
        return op == '&' ? and_opt(l, r) : or_opt(l, r);
    }

    VecExpr *ref = leaf_colref(e);
    const ProjEntry *hit = NULL;
    int n_hits = 0;
    for (int i = 0; i < pn->n_entries; i++)
        if (strcmp(pn->entries[i].output_name, ref->col_name) == 0) {
            hit = &pn->entries[i];
            n_hits++;
        }
    const char *src = NULL;
    if (n_hits == 1) {
        if (!hit->expr)
            src = hit->output_name;
        else if (hit->expr->kind == EXPR_COL_REF)
            src = hit->expr->col_name;
    }
    if (!src || vec_schema_find_col(&pn->child->output_schema, src) < 0) {
        vec_expr_free(e);
        return NULL;
    }
    if (strcmp(src, ref->col_name) != 0) {
        char *renamed = dup_str(src);
        free(ref->col_name);
        ref->col_name = renamed;
    }
    return e;
}

/* 1 when every column `e` references has the same type in `have` as in
   `want`. */
static int refs_same_type(const VecExpr *e, const VecSchema *want,
                          const VecSchema *have) {
    if (e->kind == EXPR_BOOL)
        return refs_same_type(e->left, want, have) &&
               refs_same_type(e->right, want, have);
    const VecExpr *ref = leaf_colref(e);
    int wi = vec_schema_find_col(want, ref->col_name);
    int hi = vec_schema_find_col(have, ref->col_name);
    return wi >= 0 && hi >= 0 && want->col_types[wi] == have->col_types[hi];
}

/* Hand the pruning predicate `pred` (owned) to the scans below `node`. */
static void push_into(VecNode *node, VecExpr *pred) {
    if (!pred) return;
    const char *kind = node->kind ? node->kind : "";

    if (strcmp(kind, "ScanNode") == 0) {
        ScanNode *sn = (ScanNode *)node;
        sn->predicate = and_opt(sn->predicate, pred);
        return;
    }
    if (strcmp(kind, "ParquetScanNode") == 0) {
        VecExpr **slot = parquet_scan_predicate_slot(node);
        if (slot) *slot = and_opt(*slot, pred);
        else vec_expr_free(pred);
        return;
    }
    if (strcmp(kind, "ProjectNode") == 0) {
        ProjectNode *pn = (ProjectNode *)node;
        push_into(pn->child, through_project(pred, pn));
        return;
    }
    if (strcmp(kind, "FilterNode") == 0) {
        push_into(((FilterNode *)node)->child, pred);
        return;
    }
    if (strcmp(kind, "SortNode") == 0) {
        push_into(((SortNode *)node)->child, pred);
        return;
    }
    if (strcmp(kind, "ConcatNode") == 0) {
        ConcatNode *cn = (ConcatNode *)node;
        for (int i = 0; i < cn->n_children; i++)
            if (refs_same_type(pred, &cn->base.output_schema,
                               &cn->children[i]->output_schema))
                push_into(cn->children[i], prunable_copy(pred));
        vec_expr_free(pred);
        return;
    }
    vec_expr_free(pred);
}

static void pushdown_predicates(VecNode *node) {
    const char *kind = node->kind ? node->kind : "";

    if (strcmp(kind, "FilterNode") == 0) {
        FilterNode *fn = (FilterNode *)node;
        if (!fn->pushed_down) {
            fn->pushed_down = 1;
            push_into(fn->child, prunable_copy(fn->predicate));
        }
        pushdown_predicates(fn->child);
        return;
    }

    if (strcmp(kind, "ProjectNode") == 0) {
        pushdown_predicates(((ProjectNode *)node)->child);
    } else if (strcmp(kind, "SortNode") == 0) {
        pushdown_predicates(((SortNode *)node)->child);
    } else if (strcmp(kind, "LimitNode") == 0) {
        pushdown_predicates(((LimitNode *)node)->child);
    } else if (strcmp(kind, "TopNNode") == 0) {
        pushdown_predicates(((TopNNode *)node)->child);
    } else if (strcmp(kind, "GroupAggNode") == 0) {
        pushdown_predicates(((GroupAggNode *)node)->child);
    } else if (strcmp(kind, "WindowNode") == 0) {
        pushdown_predicates(((WindowNode *)node)->child);
    } else if (strcmp(kind, "JoinNode") == 0) {
        pushdown_predicates(((JoinNode *)node)->left);
        pushdown_predicates(((JoinNode *)node)->right);
    } else if (strcmp(kind, "ConcatNode") == 0) {
        ConcatNode *cn = (ConcatNode *)node;
        for (int i = 0; i < cn->n_children; i++)
            pushdown_predicates(cn->children[i]);
    }
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

    /* Pass 3: divide the memory budget among the plan's budgeted nodes */
    vec_plan_assign_budgets(root);
}
