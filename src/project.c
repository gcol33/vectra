#include "project.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/* Find column index by name in a batch, or -1 if not found */
static int find_col_idx(const VecBatch *batch, const char *name) {
    for (int c = 0; c < batch->n_cols; c++)
        if (strcmp(batch->col_names[c], name) == 0) return c;
    return -1;
}

/* Pure select/rename path: gather only the needed columns from input.
   No expressions to evaluate, so we skip gathering unreferenced columns. */
static VecBatch *project_select_gather(ProjectNode *pn, VecBatch *input) {
    int64_t logical_n = vec_batch_logical_rows(input);
    VecBatch *out = vec_batch_alloc(pn->n_entries, logical_n);

    for (int i = 0; i < pn->n_entries; i++) {
        ProjEntry *pe = &pn->entries[i];
        int found = find_col_idx(input, pe->output_name);
        if (found < 0) {
            vec_batch_free(out);
            vec_batch_free(input);
            vectra_error("select: column not found: %s", pe->output_name);
        }
        out->col_names[i] = (char *)malloc(strlen(pe->output_name) + 1);
        strcpy(out->col_names[i], pe->output_name);

        if (input->sel) {
            out->columns[i] = vec_array_gather(&input->columns[found],
                                                input->sel, input->sel_n);
        } else {
            out->columns[i] = *vec_coerce(&input->columns[found],
                                           input->columns[found].type);
        }
    }

    vec_batch_free(input);
    return out;
}

/* General path: handles mutate (expressions) with or without sel.
   Gathers only the input columns needed by expressions and pass-through
   entries into a dense working batch. Evaluates expressions against it
   and transfers ownership to the output. */
static VecBatch *project_mutate_path(ProjectNode *pn, VecBatch *input) {
    int has_sel = (input->sel != NULL);
    int64_t logical_n = vec_batch_logical_rows(input);
    int n_input_cols = input->n_cols;

    /* Determine which input columns are needed */
    uint8_t *needed = (uint8_t *)calloc((size_t)n_input_cols, 1);
    for (int i = 0; i < pn->n_entries; i++) {
        ProjEntry *pe = &pn->entries[i];
        if (!pe->expr) {
            /* Pass-through: mark the named column */
            for (int c = 0; c < n_input_cols; c++) {
                if (strcmp(input->col_names[c], pe->output_name) == 0) {
                    needed[c] = 1;
                    break;
                }
            }
        } else {
            /* Walk expression tree for col_ref dependencies */
            vec_expr_collect_colrefs(pe->expr, input->col_names,
                                     n_input_cols, needed);
        }
    }

    /* Count needed columns and build mapping */
    int n_needed = 0;
    for (int c = 0; c < n_input_cols; c++)
        if (needed[c]) n_needed++;

    /* Build dense working batch with only needed columns */
    VecBatch *work = vec_batch_alloc(n_needed, logical_n);

    /* orig_to_work[i] = index in work batch, or -1 */
    int *orig_to_work = (int *)malloc((size_t)n_input_cols * sizeof(int));
    int wi = 0;
    for (int c = 0; c < n_input_cols; c++) {
        if (!needed[c]) {
            orig_to_work[c] = -1;
            continue;
        }
        orig_to_work[c] = wi;
        work->col_names[wi] = (char *)malloc(
            strlen(input->col_names[c]) + 1);
        strcpy(work->col_names[wi], input->col_names[c]);

        if (has_sel) {
            work->columns[wi] = vec_array_gather(&input->columns[c],
                                                  input->sel, input->sel_n);
        } else {
            /* No sel: deep copy via vec_coerce */
            VecArray *copy = vec_coerce(&input->columns[c],
                                         input->columns[c].type);
            work->columns[wi] = *copy;
            free(copy);
        }
        wi++;
    }
    free(needed);
    free(orig_to_work);

    /* Done with input */
    vec_batch_free(input);

    /* Now process entries against the dense working batch.
       working starts as work, grows with new mutate columns. */
    VecBatch *working = work;

    VecBatch *out = vec_batch_alloc(pn->n_entries, logical_n);

    /* Track which work columns have been moved to output (to avoid double-free) */
    uint8_t *moved = (uint8_t *)calloc((size_t)work->n_cols, 1);

    for (int i = 0; i < pn->n_entries; i++) {
        ProjEntry *pe = &pn->entries[i];

        if (!pe->expr) {
            /* Pass-through: find in working batch, transfer ownership */
            int found = find_col_idx(working, pe->output_name);
            if (found < 0) {
                free(moved);
                vec_batch_free(out);
                if (working != work) {
                    for (int c = work->n_cols; c < working->n_cols; c++) {
                        vec_array_free(&working->columns[c]);
                        free(working->col_names[c]);
                    }
                    free(working->columns);
                    free(working->col_names);
                    free(working);
                }
                vec_batch_free(work);
                vectra_error("select: column not found: %s", pe->output_name);
            }
            out->col_names[i] = (char *)malloc(strlen(pe->output_name) + 1);
            strcpy(out->col_names[i], pe->output_name);

            /* Check if this column might be used by a later expression.
               If so, deep copy; otherwise transfer ownership. */
            int used_later = 0;
            for (int j = i + 1; j < pn->n_entries; j++) {
                if (pn->entries[j].expr) {
                    /* Quick check: does this expr reference the column? */
                    uint8_t ref = 0;
                    vec_expr_collect_colrefs(pn->entries[j].expr,
                        &pe->output_name, 1, &ref);
                    if (ref) { used_later = 1; break; }
                }
            }

            if (used_later) {
                /* Deep copy: column still needed in working batch */
                VecArray *copy = vec_coerce(&working->columns[found],
                                             working->columns[found].type);
                out->columns[i] = *copy;
                free(copy);
            } else {
                /* Transfer ownership */
                out->columns[i] = working->columns[found];
                /* Zero out in working so it won't be freed */
                memset(&working->columns[found], 0, sizeof(VecArray));
                if (found < work->n_cols) moved[found] = 1;
            }
        } else {
            /* Evaluate expression against working batch */
            VecArray *result = vec_expr_eval(pe->expr, working);
            out->col_names[i] = (char *)malloc(strlen(pe->output_name) + 1);
            strcpy(out->col_names[i], pe->output_name);
            out->columns[i] = *result;
            free(result);

            /* For mutate: add this new column to working batch so subsequent
               entries can reference it */
            if (i < pn->n_entries - 1) {
                int new_n = working->n_cols + 1;

                if (working == work) {
                    /* First extension: copy work column pointers */
                    VecArray *new_cols = (VecArray *)malloc(
                        (size_t)new_n * sizeof(VecArray));
                    char **new_names = (char **)malloc(
                        (size_t)new_n * sizeof(char *));
                    for (int c = 0; c < work->n_cols; c++) {
                        new_cols[c] = work->columns[c];
                        new_names[c] = work->col_names[c];
                    }
                    VecBatch *wb = (VecBatch *)calloc(1, sizeof(VecBatch));
                    wb->n_rows = logical_n;
                    wb->n_cols = new_n;
                    wb->columns = new_cols;
                    wb->col_names = new_names;
                    working = wb;
                } else {
                    working->columns = (VecArray *)realloc(
                        working->columns,
                        (size_t)new_n * sizeof(VecArray));
                    working->col_names = (char **)realloc(
                        working->col_names,
                        (size_t)new_n * sizeof(char *));
                    working->n_cols = new_n;
                }

                /* Add new column (deep copy for working reference) */
                VecArray *ref = vec_coerce(&out->columns[i],
                                           out->columns[i].type);
                working->columns[new_n - 1] = *ref;
                free(ref);
                working->col_names[new_n - 1] = (char *)malloc(
                    strlen(pe->output_name) + 1);
                strcpy(working->col_names[new_n - 1], pe->output_name);
            }
        }
    }

    /* Clean up working batch */
    if (working != work) {
        /* Free extended columns (mutate working copies) */
        for (int c = work->n_cols; c < working->n_cols; c++) {
            vec_array_free(&working->columns[c]);
            free(working->col_names[c]);
        }
        free(working->columns);
        free(working->col_names);
        free(working);
    }

    /* Free remaining work columns that weren't moved to output */
    for (int c = 0; c < work->n_cols; c++) {
        if (!moved[c])
            vec_array_free(&work->columns[c]);
    }
    free(moved);

    /* Free work batch shell (columns already freed individually) */
    for (int c = 0; c < work->n_cols; c++)
        free(work->col_names[c]);
    free(work->columns);
    free(work->col_names);
    free(work->sel);
    free(work);

    return out;
}

static VecBatch *project_next_batch(VecNode *self) {
    ProjectNode *pn = (ProjectNode *)self;
    VecBatch *input = pn->child->next_batch(pn->child);
    if (!input) return NULL;

    /* Check if any entry has an expression (mutate/transmute) */
    int has_exprs = 0;
    for (int i = 0; i < pn->n_entries; i++) {
        if (pn->entries[i].expr) { has_exprs = 1; break; }
    }

    if (!has_exprs) {
        /* Pure select/rename/relocate: gather only needed columns */
        return project_select_gather(pn, input);
    } else {
        /* Mutate: needs dense batch for expr evaluation */
        return project_mutate_path(pn, input);
    }
}

static void project_free(VecNode *self) {
    ProjectNode *pn = (ProjectNode *)self;
    pn->child->free_node(pn->child);
    for (int i = 0; i < pn->n_entries; i++) {
        free(pn->entries[i].output_name);
        vec_expr_free(pn->entries[i].expr);
    }
    free(pn->entries);
    vec_schema_free(&pn->base.output_schema);
    free(pn);
}

ProjectNode *project_node_create(VecNode *child, int n_entries,
                                  ProjEntry *entries) {
    ProjectNode *pn = (ProjectNode *)calloc(1, sizeof(ProjectNode));
    if (!pn) vectra_error("alloc failed for ProjectNode");
    pn->child = child;
    pn->n_entries = n_entries;
    pn->entries = entries;

    /* Build output schema */
    char **names = (char **)malloc((size_t)n_entries * sizeof(char *));
    VecType *types = (VecType *)malloc((size_t)n_entries * sizeof(VecType));

    for (int i = 0; i < n_entries; i++) {
        names[i] = entries[i].output_name;
        if (entries[i].expr) {
            types[i] = entries[i].expr->result_type;
        } else {
            /* Find type from child schema */
            int idx = vec_schema_find_col(&child->output_schema,
                                           entries[i].output_name);
            if (idx < 0)
                vectra_error("project: column not found in child: %s",
                             entries[i].output_name);
            types[i] = child->output_schema.col_types[idx];
        }
    }

    pn->base.output_schema = vec_schema_create(n_entries, names, types);
    free(names);
    free(types);

    pn->base.next_batch = project_next_batch;
    pn->base.free_node = project_free;
    pn->base.kind = "ProjectNode";

    return pn;
}
