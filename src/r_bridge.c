/*
 * r_bridge.c — thin coordinator
 *
 * Shared infrastructure lives in r_bridge_core.c (xptr helpers, type
 * detection, df_to_batch, expression parser).
 * Node constructors live in r_bridge_nodes.c.
 * Format I/O (CSV/SQL/TIFF/streaming-VTR) lives in r_bridge_io.c.
 *
 * This file contains:
 *   - C_write_vtr  (df-based write with multi-row-group support)
 *   - C_scan_node
 *   - C_collect
 *   - C_node_schema
 *   - C_node_plan  (+ static plan-walking helpers)
 */

#include "r_bridge.h"
#include "r_bridge_internal.h"
#include "types.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "vtr1.h"
#include "scan.h"
#include "collect.h"
#include "filter.h"
#include "project.h"
#include "group_agg.h"
#include "sort.h"
#include "topn.h"
#include "limit.h"
#include "join.h"
#include "window.h"
#include "concat.h"
#include "csv_scan.h"
#include "sql_scan.h"
#include "tiff_scan.h"
#include "optimize.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* --- C_write_vtr --- */

SEXP C_write_vtr(SEXP df, SEXP path, SEXP batch_size) {
    if (!Rf_isNewList(df)) vectra_error("first argument must be a data.frame");
    const char *fpath = CHAR(STRING_ELT(path, 0));
    int bs = Rf_asInteger(batch_size);

    int n_cols = Rf_length(df);
    SEXP first_col = VECTOR_ELT(df, 0);
    int64_t n_rows = (int64_t)XLENGTH(first_col);

    /* Build annotations for all columns */
    char **annotations = (char **)calloc((size_t)n_cols, sizeof(char *));
    for (int i = 0; i < n_cols; i++)
        annotations[i] = r_col_annotation(VECTOR_ELT(df, i));

    if (bs <= 0 || (int64_t)bs >= n_rows) {
        /* Single row group */
        VecBatch *batch = df_to_batch(df);
        /* Build schema with annotations */
        VecSchema schema;
        memset(&schema, 0, sizeof(schema));
        schema.n_cols = batch->n_cols;
        schema.col_names = batch->col_names;
        schema.col_types = (VecType *)malloc((size_t)batch->n_cols * sizeof(VecType));
        schema.col_annotations = annotations;
        for (int i = 0; i < batch->n_cols; i++)
            schema.col_types[i] = batch->columns[i].type;

        FILE *fp = fopen(fpath, "wb");
        if (!fp) vectra_error("cannot open file for writing: %s", fpath);
        vtr1_write_header(fp, &schema, 1);
        vtr1_write_rowgroup(fp, batch);
        fclose(fp);

        free(schema.col_types);
        vec_batch_free(batch);
    } else {
        /* Multiple row groups */
        FILE *fp = fopen(fpath, "wb");
        if (!fp) vectra_error("cannot open file for writing: %s", fpath);

        uint32_t n_rg = (uint32_t)((n_rows + bs - 1) / bs);

        /* Build schema from first few elements */
        SEXP names = Rf_getAttrib(df, R_NamesSymbol);
        char **col_names = (char **)malloc((size_t)n_cols * sizeof(char *));
        VecType *col_types = (VecType *)malloc((size_t)n_cols * sizeof(VecType));
        for (int i = 0; i < n_cols; i++) {
            col_names[i] = (char *)CHAR(STRING_ELT(names, i));
            col_types[i] = r_col_type(VECTOR_ELT(df, i));
        }
        VecSchema schema = vec_schema_create(n_cols, col_names, col_types);
        free(col_names);
        free(col_types);
        /* Set annotations */
        for (int i = 0; i < n_cols; i++) {
            free(schema.col_annotations[i]);
            schema.col_annotations[i] = annotations[i];
            annotations[i] = NULL; /* ownership transferred */
        }

        vtr1_write_header(fp, &schema, n_rg);

        /* Write row groups as slices of the data.frame */
        for (uint32_t rg = 0; rg < n_rg; rg++) {
            int64_t start = (int64_t)rg * bs;
            int64_t end = start + bs;
            if (end > n_rows) end = n_rows;
            int64_t rg_rows = end - start;

            VecBatch *batch = vec_batch_alloc(n_cols, rg_rows);
            for (int c = 0; c < n_cols; c++) {
                SEXP col = VECTOR_ELT(df, c);
                VecType type = schema.col_types[c];
                VecArray arr = vec_array_alloc(type, rg_rows);
                vec_array_set_all_valid(&arr);

                batch->col_names[c] = (char *)malloc(
                    strlen(schema.col_names[c]) + 1);
                strcpy(batch->col_names[c], schema.col_names[c]);

                if (Rf_isFactor(col)) {
                    /* Factor: convert codes to level strings */
                    SEXP levels = Rf_getAttrib(col, R_LevelsSymbol);
                    int *ip = INTEGER(col);
                    int64_t total_len = 0;
                    for (int64_t i = 0; i < rg_rows; i++) {
                        if (ip[start + i] != NA_INTEGER)
                            total_len += (int64_t)strlen(
                                CHAR(STRING_ELT(levels, ip[start + i] - 1)));
                    }
                    free(arr.buf.str.data);  /* free 1-byte from vec_array_alloc */
                    arr.buf.str.data = (char *)malloc(
                        (size_t)(total_len > 0 ? total_len : 1));
                    arr.buf.str.data_len = total_len;
                    int64_t offset = 0;
                    for (int64_t i = 0; i < rg_rows; i++) {
                        arr.buf.str.offsets[i] = offset;
                        if (ip[start + i] == NA_INTEGER) {
                            vec_array_set_null(&arr, i);
                        } else {
                            const char *lev = CHAR(STRING_ELT(levels,
                                ip[start + i] - 1));
                            int64_t slen = (int64_t)strlen(lev);
                            memcpy(arr.buf.str.data + offset, lev, (size_t)slen);
                            offset += slen;
                        }
                    }
                    arr.buf.str.offsets[rg_rows] = offset;
                } else
                switch (type) {
                case VEC_INT64:
                    if (Rf_isInteger(col)) {
                        int *ip = INTEGER(col);
                        for (int64_t i = 0; i < rg_rows; i++) {
                            if (ip[start + i] == NA_INTEGER) {
                                vec_array_set_null(&arr, i);
                            } else {
                                arr.buf.i64[i] = (int64_t)ip[start + i];
                            }
                        }
                    } else {
                        double *dp = REAL(col);
                        for (int64_t i = 0; i < rg_rows; i++) {
                            int64_t v;
                            memcpy(&v, &dp[start + i], sizeof(int64_t));
                            if (v == INT64_MIN) {
                                vec_array_set_null(&arr, i);
                            } else {
                                arr.buf.i64[i] = v;
                            }
                        }
                    }
                    break;
                case VEC_DOUBLE: {
                    double *dp = REAL(col);
                    for (int64_t i = 0; i < rg_rows; i++) {
                        if (ISNA(dp[start + i]) || ISNAN(dp[start + i])) {
                            vec_array_set_null(&arr, i);
                        } else {
                            arr.buf.dbl[i] = dp[start + i];
                        }
                    }
                    break;
                }
                case VEC_BOOL: {
                    int *lp = LOGICAL(col);
                    for (int64_t i = 0; i < rg_rows; i++) {
                        if (lp[start + i] == NA_LOGICAL) {
                            vec_array_set_null(&arr, i);
                        } else {
                            arr.buf.bln[i] = (uint8_t)(lp[start + i] != 0);
                        }
                    }
                    break;
                }
                case VEC_STRING: {
                    int64_t total_len = 0;
                    for (int64_t i = 0; i < rg_rows; i++) {
                        SEXP s = STRING_ELT(col, (R_xlen_t)(start + i));
                        if (s != NA_STRING) total_len += (int64_t)strlen(CHAR(s));
                    }
                    free(arr.buf.str.data);  /* free 1-byte from vec_array_alloc */
                    arr.buf.str.data = (char *)malloc((size_t)(total_len > 0 ? total_len : 1));
                    arr.buf.str.data_len = total_len;
                    int64_t offset = 0;
                    for (int64_t i = 0; i < rg_rows; i++) {
                        arr.buf.str.offsets[i] = offset;
                        SEXP s = STRING_ELT(col, (R_xlen_t)(start + i));
                        if (s == NA_STRING) {
                            vec_array_set_null(&arr, i);
                        } else {
                            const char *cs = CHAR(s);
                            int64_t slen = (int64_t)strlen(cs);
                            memcpy(arr.buf.str.data + offset, cs, (size_t)slen);
                            offset += slen;
                        }
                    }
                    arr.buf.str.offsets[rg_rows] = offset;
                    break;
                }
                }
                batch->columns[c] = arr;
            }

            vtr1_write_rowgroup(fp, batch);
            vec_batch_free(batch);
        }

        vec_schema_free(&schema);
        fclose(fp);
    }

    /* Free any annotations not transferred to schema */
    for (int i = 0; i < n_cols; i++)
        free(annotations[i]);
    free(annotations);

    return R_NilValue;
}

/* --- C_scan_node --- */

SEXP C_scan_node(SEXP path) {
    const char *fpath = CHAR(STRING_ELT(path, 0));
    ScanNode *sn = scan_node_create(fpath, NULL, 0);
    return wrap_node((VecNode *)sn);
}

/* --- C_collect --- */

SEXP C_collect(SEXP node_xptr) {
    VecNode *node = unwrap_node(node_xptr);
    return vec_collect(node);
}

/* --- C_node_schema --- */

SEXP C_node_schema(SEXP node_xptr) {
    VecNode *node = unwrap_node(node_xptr);
    const VecSchema *schema = &node->output_schema;

    SEXP result = PROTECT(Rf_allocVector(VECSXP, 3));
    SEXP col_names = PROTECT(Rf_allocVector(STRSXP, schema->n_cols));
    SEXP col_types = PROTECT(Rf_allocVector(STRSXP, schema->n_cols));
    SEXP col_annotations = PROTECT(Rf_allocVector(STRSXP, schema->n_cols));

    const char *type_names[] = {"int64", "double", "bool", "string"};
    for (int i = 0; i < schema->n_cols; i++) {
        SET_STRING_ELT(col_names, i,
            Rf_mkCharCE(schema->col_names[i], CE_UTF8));
        SET_STRING_ELT(col_types, i, Rf_mkChar(type_names[schema->col_types[i]]));
        if (schema->col_annotations && schema->col_annotations[i])
            SET_STRING_ELT(col_annotations, i,
                Rf_mkCharCE(schema->col_annotations[i], CE_UTF8));
        else
            SET_STRING_ELT(col_annotations, i, NA_STRING);
    }

    SET_VECTOR_ELT(result, 0, col_names);
    SET_VECTOR_ELT(result, 1, col_types);
    SET_VECTOR_ELT(result, 2, col_annotations);

    SEXP rnames = PROTECT(Rf_allocVector(STRSXP, 3));
    SET_STRING_ELT(rnames, 0, Rf_mkChar("name"));
    SET_STRING_ELT(rnames, 1, Rf_mkChar("type"));
    SET_STRING_ELT(rnames, 2, Rf_mkChar("annotation"));
    Rf_setAttrib(result, R_NamesSymbol, rnames);

    UNPROTECT(5);
    return result;
}

/* --- C_node_plan: walk the node tree for explain() --- */

/* Helper: get child node(s) from a node */
static void node_get_children(VecNode *node, VecNode **children, int *n_children) {
    *n_children = 0;
    const char *kind = node->kind ? node->kind : "Unknown";

    if (strcmp(kind, "ScanNode") == 0 || strcmp(kind, "CsvScanNode") == 0 ||
        strcmp(kind, "SqlScanNode") == 0 || strcmp(kind, "TiffScanNode") == 0) {
        *n_children = 0;
    } else if (strcmp(kind, "FilterNode") == 0) {
        FilterNode *fn = (FilterNode *)node;
        children[0] = fn->child;
        *n_children = 1;
    } else if (strcmp(kind, "ProjectNode") == 0) {
        ProjectNode *pn = (ProjectNode *)node;
        children[0] = pn->child;
        *n_children = 1;
    } else if (strcmp(kind, "GroupAggNode") == 0) {
        GroupAggNode *ga = (GroupAggNode *)node;
        children[0] = ga->child;
        *n_children = 1;
    } else if (strcmp(kind, "SortNode") == 0) {
        SortNode *sn = (SortNode *)node;
        children[0] = sn->child;
        *n_children = 1;
    } else if (strcmp(kind, "LimitNode") == 0) {
        LimitNode *ln = (LimitNode *)node;
        children[0] = ln->child;
        *n_children = 1;
    } else if (strcmp(kind, "JoinNode") == 0) {
        JoinNode *jn = (JoinNode *)node;
        children[0] = jn->left;
        children[1] = jn->right;
        *n_children = 2;
    } else if (strcmp(kind, "WindowNode") == 0) {
        WindowNode *wn = (WindowNode *)node;
        children[0] = wn->child;
        *n_children = 1;
    } else if (strcmp(kind, "TopNNode") == 0) {
        TopNNode *tn = (TopNNode *)node;
        children[0] = tn->child;
        *n_children = 1;
    } else if (strcmp(kind, "ConcatNode") == 0) {
        ConcatNode *cn = (ConcatNode *)node;
        int show = cn->n_children < 16 ? cn->n_children : 16;
        for (int i = 0; i < show; i++)
            children[i] = cn->children[i];
        *n_children = show;
    }
}

/* Build annotation string for a node (writes to buf, returns length written) */
static int node_annotation(VecNode *node, char *buf, int bufsize) {
    const char *kind = node->kind ? node->kind : "Unknown";

    if (strcmp(kind, "ScanNode") == 0) {
        ScanNode *sn = (ScanNode *)node;
        int file_cols = sn->file->header.schema.n_cols;
        int read_cols = sn->base.output_schema.n_cols;
        int pos = 0;
        if (read_cols < file_cols)
            pos += snprintf(buf + pos, (size_t)(bufsize - pos),
                            "streaming, %d/%d cols (pruned)", read_cols, file_cols);
        else
            pos += snprintf(buf + pos, (size_t)(bufsize - pos),
                            "streaming, %d cols", read_cols);
        if (sn->predicate)
            pos += snprintf(buf + pos, (size_t)(bufsize - pos),
                            ", predicate pushdown");
        if (sn->file->header.version >= 3)
            pos += snprintf(buf + pos, (size_t)(bufsize - pos), ", v3 stats");
        return pos;
    }
    if (strcmp(kind, "CsvScanNode") == 0) {
        CsvScanNode *cn = (CsvScanNode *)node;
        return snprintf(buf, (size_t)bufsize, "streaming csv, %d cols",
                        cn->n_file_cols);
    }
    if (strcmp(kind, "SqlScanNode") == 0) {
        SqlScanNode *sn = (SqlScanNode *)node;
        return snprintf(buf, (size_t)bufsize, "streaming sql, %d cols",
                        sn->n_cols);
    }
    if (strcmp(kind, "TiffScanNode") == 0) {
        TiffScanNode *tn = (TiffScanNode *)node;
        return snprintf(buf, (size_t)bufsize, "streaming tiff, %d bands",
                        tn->n_bands);
    }
    if (strcmp(kind, "FilterNode") == 0)
        return snprintf(buf, (size_t)bufsize, "streaming");
    if (strcmp(kind, "ProjectNode") == 0) {
        ProjectNode *pn = (ProjectNode *)node;
        int has_tmp = 0;
        for (int i = 0; i < pn->n_entries; i++) {
            if (strncmp(pn->entries[i].output_name, ".vectra_tmp_", 12) == 0) {
                has_tmp = 1;
                break;
            }
        }
        if (has_tmp)
            return snprintf(buf, (size_t)bufsize, "streaming, hidden mutate");
        return snprintf(buf, (size_t)bufsize, "streaming");
    }
    if (strcmp(kind, "GroupAggNode") == 0) {
        GroupAggNode *ga = (GroupAggNode *)node;
        return snprintf(buf, (size_t)bufsize, "materializes, %d keys",
                        ga->n_keys);
    }
    if (strcmp(kind, "SortNode") == 0)
        return snprintf(buf, (size_t)bufsize, "materializes");
    if (strcmp(kind, "LimitNode") == 0) {
        LimitNode *ln = (LimitNode *)node;
        return snprintf(buf, (size_t)bufsize, "streaming, n=%lld",
                        (long long)ln->max_rows);
    }
    if (strcmp(kind, "JoinNode") == 0) {
        JoinNode *jn = (JoinNode *)node;
        const char *jkind = "unknown";
        switch (jn->kind) {
        case JOIN_INNER: jkind = "inner"; break;
        case JOIN_LEFT:  jkind = "left"; break;
        case JOIN_FULL:  jkind = "full"; break;
        case JOIN_SEMI:  jkind = "semi"; break;
        case JOIN_ANTI:  jkind = "anti"; break;
        }
        return snprintf(buf, (size_t)bufsize,
                        "build right + stream left, %s, %d keys",
                        jkind, jn->n_keys);
    }
    if (strcmp(kind, "WindowNode") == 0) {
        WindowNode *wn = (WindowNode *)node;
        return snprintf(buf, (size_t)bufsize,
                        "materializes, %d fns", wn->n_wins);
    }
    if (strcmp(kind, "TopNNode") == 0) {
        TopNNode *tn = (TopNNode *)node;
        return snprintf(buf, (size_t)bufsize,
                        "heap, k=%lld, %d keys",
                        (long long)tn->limit, tn->n_keys);
    }
    if (strcmp(kind, "ConcatNode") == 0) {
        ConcatNode *cn = (ConcatNode *)node;
        return snprintf(buf, (size_t)bufsize,
                        "streaming, %d inputs", cn->n_children);
    }
    buf[0] = '\0';
    return 0;
}

/* Collect plan lines recursively. lines/count managed by caller. */
static void collect_plan_lines(VecNode *node, int depth,
                                char **lines, int *count, int max_lines) {
    if (*count >= max_lines) return;
    const char *kind = node->kind ? node->kind : "Unknown";

    char ann[128];
    node_annotation(node, ann, 128);

    char buf[512];
    int pos = 0;
    for (int i = 0; i < depth * 2 && pos < 500; i++) buf[pos++] = ' ';
    int written = snprintf(buf + pos, (size_t)(512 - pos), "%s [%s]", kind, ann);
    pos += written;
    buf[pos] = '\0';

    lines[*count] = (char *)malloc(strlen(buf) + 1);
    strcpy(lines[*count], buf);
    (*count)++;

    VecNode *children[16];
    int n_children;
    node_get_children(node, children, &n_children);
    for (int i = 0; i < n_children; i++)
        collect_plan_lines(children[i], depth + 1, lines, count, max_lines);
}

SEXP C_node_plan(SEXP node_xptr) {
    VecNode *node = unwrap_node(node_xptr);

    /* Run optimizer so explain() shows the optimized plan */
    vec_optimize(node);

    char *lines[64];
    int count = 0;
    collect_plan_lines(node, 0, lines, &count, 64);

    SEXP result = PROTECT(Rf_allocVector(STRSXP, count));
    for (int i = 0; i < count; i++) {
        SET_STRING_ELT(result, i, Rf_mkChar(lines[i]));
        free(lines[i]);
    }
    UNPROTECT(1);
    return result;
}
