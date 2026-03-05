#include "schema.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

VecSchema vec_schema_create(int n_cols, char **col_names, VecType *col_types) {
    VecSchema s;
    s.n_cols = n_cols;
    s.col_names = (char **)calloc((size_t)n_cols, sizeof(char *));
    s.col_types = (VecType *)calloc((size_t)n_cols, sizeof(VecType));
    if ((!s.col_names || !s.col_types) && n_cols > 0)
        vectra_error("failed to allocate schema");
    for (int i = 0; i < n_cols; i++) {
        s.col_names[i] = (char *)malloc(strlen(col_names[i]) + 1);
        if (!s.col_names[i]) vectra_error("failed to allocate schema column name");
        strcpy(s.col_names[i], col_names[i]);
        s.col_types[i] = col_types[i];
    }
    return s;
}

void vec_schema_free(VecSchema *schema) {
    if (!schema) return;
    for (int i = 0; i < schema->n_cols; i++)
        free(schema->col_names[i]);
    free(schema->col_names);
    free(schema->col_types);
    schema->n_cols = 0;
    schema->col_names = NULL;
    schema->col_types = NULL;
}

VecSchema vec_schema_copy(const VecSchema *src) {
    return vec_schema_create(src->n_cols, src->col_names, src->col_types);
}

int vec_schema_find_col(const VecSchema *schema, const char *name) {
    for (int i = 0; i < schema->n_cols; i++) {
        if (strcmp(schema->col_names[i], name) == 0)
            return i;
    }
    return -1;
}
