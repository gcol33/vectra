#include "schema.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

VecSchema vec_schema_create(int n_cols, char **col_names, VecType *col_types) {
    VecSchema s;
    s.n_cols = n_cols;
    s.col_names = (char **)calloc((size_t)n_cols, sizeof(char *));
    s.col_types = (VecType *)calloc((size_t)n_cols, sizeof(VecType));
    s.col_annotations = (char **)calloc((size_t)n_cols, sizeof(char *));
    if ((!s.col_names || !s.col_types || !s.col_annotations) && n_cols > 0)
        vectra_error("failed to allocate schema");
    for (int i = 0; i < n_cols; i++) {
        s.col_names[i] = (char *)malloc(strlen(col_names[i]) + 1);
        if (!s.col_names[i]) vectra_error("failed to allocate schema column name");
        strcpy(s.col_names[i], col_names[i]);
        s.col_types[i] = col_types[i];
        /* col_annotations[i] stays NULL */
    }
    return s;
}

void vec_schema_free(VecSchema *schema) {
    if (!schema) return;
    for (int i = 0; i < schema->n_cols; i++) {
        free(schema->col_names[i]);
        free(schema->col_annotations[i]);
    }
    free(schema->col_names);
    free(schema->col_types);
    free(schema->col_annotations);
    schema->n_cols = 0;
    schema->col_names = NULL;
    schema->col_types = NULL;
    schema->col_annotations = NULL;
}

/* Duplicate one column's annotation into dst[i] (NULL stays NULL). */
static void schema_copy_annotation(char **dst, int i,
                                   char *const *src_annotations, int src_i) {
    if (!src_annotations || !src_annotations[src_i]) return;
    size_t len = strlen(src_annotations[src_i]) + 1;
    dst[i] = (char *)malloc(len);
    if (!dst[i]) vectra_error("failed to allocate schema column annotation");
    memcpy(dst[i], src_annotations[src_i], len);
}

VecSchema vec_schema_copy(const VecSchema *src) {
    VecSchema s = vec_schema_create(src->n_cols, src->col_names, src->col_types);
    for (int i = 0; i < src->n_cols; i++)
        schema_copy_annotation(s.col_annotations, i, src->col_annotations, i);
    return s;
}

VecSchema vec_schema_concat(const VecSchema *a, const VecSchema *b) {
    if (!a || !b) vectra_error("vec_schema_concat: NULL schema");

    /* Column names are the identity of a column to every verb, so a
       collision is an error rather than something to disambiguate. */
    for (int j = 0; j < b->n_cols; j++) {
        if (vec_schema_find_col(a, b->col_names[j]) >= 0)
            vectra_error("column '%s' already exists", b->col_names[j]);
    }
    for (int j = 1; j < b->n_cols; j++) {
        for (int k = 0; k < j; k++) {
            if (strcmp(b->col_names[j], b->col_names[k]) == 0)
                vectra_error("duplicate column name '%s'", b->col_names[j]);
        }
    }

    int n = a->n_cols + b->n_cols;
    VecSchema s;
    s.n_cols          = n;
    s.col_names       = (char **)calloc((size_t)n, sizeof(char *));
    s.col_types       = (VecType *)calloc((size_t)n, sizeof(VecType));
    s.col_annotations = (char **)calloc((size_t)n, sizeof(char *));
    if ((!s.col_names || !s.col_types || !s.col_annotations) && n > 0) {
        free(s.col_names); free(s.col_types); free(s.col_annotations);
        vectra_error("failed to allocate schema");
    }

    for (int i = 0; i < n; i++) {
        const VecSchema *src = (i < a->n_cols) ? a : b;
        int si = (i < a->n_cols) ? i : i - a->n_cols;

        size_t len = strlen(src->col_names[si]) + 1;
        s.col_names[i] = (char *)malloc(len);
        if (!s.col_names[i]) {
            vec_schema_free(&s);
            vectra_error("failed to allocate schema column name");
        }
        memcpy(s.col_names[i], src->col_names[si], len);
        s.col_types[i] = src->col_types[si];
        schema_copy_annotation(s.col_annotations, i, src->col_annotations, si);
    }
    return s;
}

int vec_schema_find_col(const VecSchema *schema, const char *name) {
    for (int i = 0; i < schema->n_cols; i++) {
        if (strcmp(schema->col_names[i], name) == 0)
            return i;
    }
    return -1;
}
