#include "r_bridge.h"
#include "vtri.h"
#include "error.h"
#include <stdio.h>
#include <string.h>

/* --- C_create_index(path, col_name, ci) --- */

SEXP C_create_index(SEXP path, SEXP col_name, SEXP ci) {
    const char *vtr_path = CHAR(STRING_ELT(path, 0));
    int ci_flag = Rf_asLogical(ci);
    int n_cols = Rf_length(col_name);

    if (n_cols == 1) {
        vtri_build(vtr_path, CHAR(STRING_ELT(col_name, 0)), ci_flag);
    } else {
        const char **col_names = (const char **)malloc((size_t)n_cols * sizeof(char *));
        if (!col_names) vectra_error("alloc failed");
        for (int i = 0; i < n_cols; i++)
            col_names[i] = CHAR(STRING_ELT(col_name, i));
        vtri_build_composite(vtr_path, col_names, n_cols, ci_flag);
        free(col_names);
    }

    return R_NilValue;
}

/* --- C_has_index(path, col_name) --- */

SEXP C_has_index(SEXP path, SEXP col_name) {
    const char *vtr_path = CHAR(STRING_ELT(path, 0));
    int n_cols = Rf_length(col_name);
    char *vtri_path;

    if (n_cols == 1) {
        vtri_path = vtri_make_path(vtr_path, CHAR(STRING_ELT(col_name, 0)));
    } else {
        const char **col_names = (const char **)malloc((size_t)n_cols * sizeof(char *));
        if (!col_names) return Rf_ScalarLogical(0);
        for (int i = 0; i < n_cols; i++)
            col_names[i] = CHAR(STRING_ELT(col_name, i));
        vtri_path = vtri_make_path_composite(vtr_path, col_names, n_cols);
        free(col_names);
    }
    if (!vtri_path) return Rf_ScalarLogical(0);

    FILE *fp = fopen(vtri_path, "rb");
    free(vtri_path);

    if (fp) {
        fclose(fp);
        return Rf_ScalarLogical(1);
    }
    return Rf_ScalarLogical(0);
}
