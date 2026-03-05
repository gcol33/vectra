#include "project.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

static VecBatch *project_next_batch(VecNode *self) {
    ProjectNode *pn = (ProjectNode *)self;
    VecBatch *input = pn->child->next_batch(pn->child);
    if (!input) return NULL;

    /* We build the output batch incrementally so mutate can reference
       columns created earlier in the same mutate() call. We work on
       a "working" batch that starts as the input and grows. */
    VecBatch *working = input; /* we'll extend this in-place */

    VecBatch *out = vec_batch_alloc(pn->n_entries, 0);

    for (int i = 0; i < pn->n_entries; i++) {
        ProjEntry *pe = &pn->entries[i];

        if (!pe->expr) {
            /* Pass-through: find column in working batch */
            int found = -1;
            for (int c = 0; c < working->n_cols; c++) {
                if (strcmp(working->col_names[c], pe->output_name) == 0) {
                    found = c;
                    break;
                }
            }
            if (found < 0) {
                vec_batch_free(out);
                if (working != input) vec_batch_free(working);
                else vec_batch_free(input);
                vectra_error("select: column not found: %s", pe->output_name);
            }
            out->col_names[i] = (char *)malloc(strlen(pe->output_name) + 1);
            strcpy(out->col_names[i], pe->output_name);
            out->columns[i] = *vec_coerce(&working->columns[found],
                                           working->columns[found].type);
            /* vec_coerce returns heap ptr, copy the struct and free the wrapper */
            /* Actually vec_coerce returns a pointer, we need to handle this */
        } else {
            /* Evaluate expression against working batch */
            VecArray *result = vec_expr_eval(pe->expr, working);
            out->col_names[i] = (char *)malloc(strlen(pe->output_name) + 1);
            strcpy(out->col_names[i], pe->output_name);
            out->columns[i] = *result;
            free(result); /* free the wrapper, not the data */

            /* For mutate: add this new column to working batch so subsequent
               entries can reference it */
            if (i < pn->n_entries - 1) {
                /* Extend working batch */
                int new_n = working->n_cols + 1;
                VecArray *new_cols = (VecArray *)realloc(
                    working == input ? NULL : working->columns,
                    (size_t)new_n * sizeof(VecArray));
                char **new_names = (char **)realloc(
                    working == input ? NULL : working->col_names,
                    (size_t)new_n * sizeof(char *));

                if (working == input) {
                    /* First extension: copy input columns */
                    new_cols = (VecArray *)malloc((size_t)new_n * sizeof(VecArray));
                    new_names = (char **)malloc((size_t)new_n * sizeof(char *));
                    for (int c = 0; c < input->n_cols; c++) {
                        new_cols[c] = input->columns[c];
                        new_names[c] = input->col_names[c];
                    }
                    VecBatch *wb = (VecBatch *)calloc(1, sizeof(VecBatch));
                    wb->n_rows = input->n_rows;
                    wb->n_cols = new_n;
                    wb->columns = new_cols;
                    wb->col_names = new_names;
                    working = wb;
                } else {
                    working->columns = new_cols;
                    working->col_names = new_names;
                    working->n_cols = new_n;
                }

                /* Add the new column (shallow copy for reference) */
                VecArray *ref = vec_coerce(&out->columns[i], out->columns[i].type);
                working->columns[new_n - 1] = *ref;
                free(ref);
                working->col_names[new_n - 1] = (char *)malloc(
                    strlen(pe->output_name) + 1);
                strcpy(working->col_names[new_n - 1], pe->output_name);
            }
        }
    }

    out->n_rows = input->n_rows;

    /* Clean up */
    if (working != input) {
        /* Free the extended columns we added (they were deep-copied) */
        for (int c = input->n_cols; c < working->n_cols; c++) {
            vec_array_free(&working->columns[c]);
            free(working->col_names[c]);
        }
        free(working->columns);
        free(working->col_names);
        free(working);
    }

    /* Free input: but we moved data out, so we need to be careful.
       For pass-through columns, we deep-copied via vec_coerce.
       So we can free input normally. */
    vec_batch_free(input);

    return out;
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
