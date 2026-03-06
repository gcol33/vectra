#include "r_bridge.h"
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
#include "csv_write.h"
#include "csv_scan.h"
#include "sql_scan.h"
#include "sql_write.h"
#include "sqlite_format.h"
#include "tiff_scan.h"
#include "tiff_write.h"
#include "expr.h"
#include "optimize.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Get R's temp directory via the public API (avoids R_TempDir non-API call) */
static const char *get_r_tempdir(void) {
    SEXP call = PROTECT(Rf_lang1(Rf_install("tempdir")));
    SEXP result = PROTECT(Rf_eval(call, R_BaseEnv));
    const char *path = CHAR(STRING_ELT(result, 0));
    UNPROTECT(2);
    return path;
}

/* --- External pointer helpers --- */

static void node_finalizer(SEXP xptr) {
    VecNode *node = (VecNode *)R_ExternalPtrAddr(xptr);
    if (node) {
        node->free_node(node);
        R_ClearExternalPtr(xptr);
    }
}

static SEXP wrap_node(VecNode *node) {
    SEXP xptr = PROTECT(R_MakeExternalPtr(node, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(xptr, node_finalizer, TRUE);
    UNPROTECT(1);
    return xptr;
}

static VecNode *unwrap_node(SEXP xptr) {
    VecNode *node = (VecNode *)R_ExternalPtrAddr(xptr);
    if (!node) vectra_error("vectra node has been freed or collected");
    return node;
}

/* --- Detect R column type -> VecType + annotation --- */

static int r_has_class(SEXP col, const char *cls_name) {
    SEXP cls = Rf_getAttrib(col, R_ClassSymbol);
    if (cls == R_NilValue || TYPEOF(cls) != STRSXP) return 0;
    for (R_xlen_t i = 0; i < XLENGTH(cls); i++) {
        if (strcmp(CHAR(STRING_ELT(cls, i)), cls_name) == 0) return 1;
    }
    return 0;
}

static VecType r_col_type(SEXP col) {
    if (Rf_isLogical(col)) return VEC_BOOL;
    if (Rf_isFactor(col))  return VEC_STRING;
    if (Rf_isInteger(col)) return VEC_INT64;
    if (Rf_isReal(col)) {
        if (r_has_class(col, "integer64")) return VEC_INT64;
        /* Date and POSIXct are stored as double */
        return VEC_DOUBLE;
    }
    if (Rf_isString(col)) return VEC_STRING;
    vectra_error("unsupported column type: %s", Rf_type2char(TYPEOF(col)));
    return VEC_DOUBLE; /* unreachable */
}

/* Build an annotation string for a column (caller frees), or NULL if none. */
static char *r_col_annotation(SEXP col) {
    /* Factor: "factor|level1|level2|..." */
    if (Rf_isFactor(col)) {
        SEXP levels = Rf_getAttrib(col, R_LevelsSymbol);
        if (levels == R_NilValue) return NULL;
        /* Compute total length */
        size_t len = 6; /* "factor" */
        R_xlen_t nlev = XLENGTH(levels);
        for (R_xlen_t i = 0; i < nlev; i++)
            len += 1 + strlen(CHAR(STRING_ELT(levels, i))); /* |level */
        char *ann = (char *)malloc(len + 1);
        strcpy(ann, "factor");
        size_t pos = 6;
        for (R_xlen_t i = 0; i < nlev; i++) {
            const char *lev = CHAR(STRING_ELT(levels, i));
            ann[pos++] = '|';
            size_t ll = strlen(lev);
            memcpy(ann + pos, lev, ll);
            pos += ll;
        }
        ann[pos] = '\0';
        return ann;
    }
    /* Date: "Date" */
    if (r_has_class(col, "Date")) {
        char *ann = (char *)malloc(5);
        strcpy(ann, "Date");
        return ann;
    }
    /* POSIXct: "POSIXct|timezone" */
    if (r_has_class(col, "POSIXct")) {
        SEXP tz = Rf_getAttrib(col, Rf_install("tzone"));
        const char *tzstr = "";
        if (tz != R_NilValue && TYPEOF(tz) == STRSXP && XLENGTH(tz) > 0)
            tzstr = CHAR(STRING_ELT(tz, 0));
        size_t len = 7 + 1 + strlen(tzstr); /* POSIXct|tz */
        char *ann = (char *)malloc(len + 1);
        snprintf(ann, len + 1, "POSIXct|%s", tzstr);
        return ann;
    }
    return NULL;
}

/* --- Convert R data.frame to VecBatch --- */

static VecBatch *df_to_batch(SEXP df) {
    int n_cols = Rf_length(df);
    if (n_cols == 0) vectra_error("data.frame has no columns");

    SEXP first_col = VECTOR_ELT(df, 0);
    int64_t n_rows = (int64_t)XLENGTH(first_col);

    VecBatch *batch = vec_batch_alloc(n_cols, n_rows);

    SEXP names = Rf_getAttrib(df, R_NamesSymbol);
    for (int c = 0; c < n_cols; c++) {
        const char *nm = CHAR(STRING_ELT(names, c));
        batch->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(batch->col_names[c], nm);
    }

    for (int c = 0; c < n_cols; c++) {
        SEXP col = VECTOR_ELT(df, c);
        int is_factor = Rf_isFactor(col);
        VecType type = r_col_type(col);
        VecArray arr = vec_array_alloc(type, n_rows);
        vec_array_set_all_valid(&arr);

        if (is_factor) {
            /* Factor: convert integer codes → level strings */
            SEXP levels = Rf_getAttrib(col, R_LevelsSymbol);
            int *ip = INTEGER(col);
            /* First pass: compute total string length */
            int64_t total_len = 0;
            for (int64_t i = 0; i < n_rows; i++) {
                if (ip[i] == NA_INTEGER) continue;
                total_len += (int64_t)strlen(CHAR(STRING_ELT(levels, ip[i] - 1)));
            }
            arr.buf.str.data = (char *)malloc((size_t)(total_len > 0 ? total_len : 1));
            if (!arr.buf.str.data && total_len > 0)
                vectra_error("alloc failed for factor string data");
            arr.buf.str.data_len = total_len;
            int64_t offset = 0;
            for (int64_t i = 0; i < n_rows; i++) {
                arr.buf.str.offsets[i] = offset;
                if (ip[i] == NA_INTEGER) {
                    vec_array_set_null(&arr, i);
                } else {
                    const char *lev = CHAR(STRING_ELT(levels, ip[i] - 1));
                    int64_t slen = (int64_t)strlen(lev);
                    memcpy(arr.buf.str.data + offset, lev, (size_t)slen);
                    offset += slen;
                }
            }
            arr.buf.str.offsets[n_rows] = offset;
        } else {
        switch (type) {
        case VEC_INT64:
            if (Rf_isInteger(col)) {
                int *ip = INTEGER(col);
                for (int64_t i = 0; i < n_rows; i++) {
                    if (ip[i] == NA_INTEGER) {
                        vec_array_set_null(&arr, i);
                        arr.buf.i64[i] = 0;
                    } else {
                        arr.buf.i64[i] = (int64_t)ip[i];
                    }
                }
            } else {
                /* bit64::integer64 - stored as double but is really int64 */
                double *dp = REAL(col);
                for (int64_t i = 0; i < n_rows; i++) {
                    int64_t v;
                    memcpy(&v, &dp[i], sizeof(int64_t));
                    if (v == INT64_MIN) {
                        vec_array_set_null(&arr, i);
                        arr.buf.i64[i] = 0;
                    } else {
                        arr.buf.i64[i] = v;
                    }
                }
            }
            break;
        case VEC_DOUBLE: {
            double *dp = REAL(col);
            for (int64_t i = 0; i < n_rows; i++) {
                if (ISNA(dp[i]) || ISNAN(dp[i])) {
                    vec_array_set_null(&arr, i);
                    arr.buf.dbl[i] = 0.0;
                } else {
                    arr.buf.dbl[i] = dp[i];
                }
            }
            break;
        }
        case VEC_BOOL: {
            int *lp = LOGICAL(col);
            for (int64_t i = 0; i < n_rows; i++) {
                if (lp[i] == NA_LOGICAL) {
                    vec_array_set_null(&arr, i);
                    arr.buf.bln[i] = 0;
                } else {
                    arr.buf.bln[i] = (uint8_t)(lp[i] != 0);
                }
            }
            break;
        }
        case VEC_STRING: {
            /* First pass: compute total string length */
            int64_t total_len = 0;
            for (int64_t i = 0; i < n_rows; i++) {
                SEXP s = STRING_ELT(col, (R_xlen_t)i);
                if (s == NA_STRING) continue;
                total_len += (int64_t)strlen(CHAR(s));
            }
            arr.buf.str.data = (char *)malloc((size_t)total_len);
            if (!arr.buf.str.data && total_len > 0)
                vectra_error("alloc failed for string data");
            arr.buf.str.data_len = total_len;

            int64_t offset = 0;
            for (int64_t i = 0; i < n_rows; i++) {
                arr.buf.str.offsets[i] = offset;
                SEXP s = STRING_ELT(col, (R_xlen_t)i);
                if (s == NA_STRING) {
                    vec_array_set_null(&arr, i);
                } else {
                    const char *cs = CHAR(s);
                    int64_t slen = (int64_t)strlen(cs);
                    memcpy(arr.buf.str.data + offset, cs, (size_t)slen);
                    offset += slen;
                }
            }
            arr.buf.str.offsets[n_rows] = offset;
            break;
        }
        }
        } /* end !is_factor */

        batch->columns[c] = arr;
    }

    return batch;
}

/* --- .Call implementations --- */

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

SEXP C_scan_node(SEXP path) {
    const char *fpath = CHAR(STRING_ELT(path, 0));
    ScanNode *sn = scan_node_create(fpath, NULL, 0);
    return wrap_node((VecNode *)sn);
}

SEXP C_collect(SEXP node_xptr) {
    VecNode *node = unwrap_node(node_xptr);
    return vec_collect(node);
}

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

/* --- Expression parser: R list -> VecExpr --- */

static const char *list_get_string(SEXP lst, const char *field) {
    SEXP names = Rf_getAttrib(lst, R_NamesSymbol);
    for (R_xlen_t i = 0; i < XLENGTH(lst); i++) {
        if (strcmp(CHAR(STRING_ELT(names, i)), field) == 0) {
            SEXP val = VECTOR_ELT(lst, i);
            if (TYPEOF(val) == STRSXP && XLENGTH(val) > 0)
                return CHAR(STRING_ELT(val, 0));
            return NULL;
        }
    }
    return NULL;
}

static SEXP list_get(SEXP lst, const char *field) {
    SEXP names = Rf_getAttrib(lst, R_NamesSymbol);
    for (R_xlen_t i = 0; i < XLENGTH(lst); i++) {
        if (strcmp(CHAR(STRING_ELT(names, i)), field) == 0)
            return VECTOR_ELT(lst, i);
    }
    return R_NilValue;
}

static VecExpr *parse_expr(SEXP lst, const VecSchema *schema);

static VecExpr *parse_expr(SEXP lst, const VecSchema *schema) {
    if (TYPEOF(lst) != VECSXP)
        vectra_error("expression must be a list");

    const char *kind = list_get_string(lst, "kind");
    if (!kind) vectra_error("expression list missing 'kind'");

    if (strcmp(kind, "col_ref") == 0) {
        const char *name = list_get_string(lst, "name");
        if (!name) vectra_error("col_ref missing 'name'");
        VecExpr *e = vec_expr_alloc(EXPR_COL_REF);
        e->col_name = (char *)malloc(strlen(name) + 1);
        strcpy(e->col_name, name);
        /* Resolve type from schema */
        int idx = vec_schema_find_col(schema, name);
        if (idx >= 0)
            e->result_type = schema->col_types[idx];
        else
            e->result_type = VEC_DOUBLE; /* fallback */
        return e;
    }
    if (strcmp(kind, "lit_double") == 0) {
        SEXP val = list_get(lst, "value");
        VecExpr *e = vec_expr_alloc(EXPR_LIT_DOUBLE);
        e->lit_dbl = Rf_asReal(val);
        e->result_type = VEC_DOUBLE;
        return e;
    }
    if (strcmp(kind, "lit_integer") == 0) {
        SEXP val = list_get(lst, "value");
        VecExpr *e = vec_expr_alloc(EXPR_LIT_INT64);
        e->lit_i64 = (int64_t)Rf_asInteger(val);
        e->result_type = VEC_INT64;
        return e;
    }
    if (strcmp(kind, "lit_logical") == 0) {
        SEXP val = list_get(lst, "value");
        VecExpr *e = vec_expr_alloc(EXPR_LIT_BOOL);
        e->lit_bln = (uint8_t)(Rf_asLogical(val) != 0);
        e->result_type = VEC_BOOL;
        return e;
    }
    if (strcmp(kind, "lit_string") == 0) {
        const char *val = list_get_string(lst, "value");
        VecExpr *e = vec_expr_alloc(EXPR_LIT_STRING);
        e->lit_str = (char *)malloc(strlen(val) + 1);
        strcpy(e->lit_str, val);
        e->result_type = VEC_STRING;
        return e;
    }
    if (strcmp(kind, "lit_na") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_LIT_NA);
        e->result_type = VEC_DOUBLE; /* default NA type */
        return e;
    }
    if (strcmp(kind, "arith") == 0) {
        const char *op_str = list_get_string(lst, "op");
        VecExpr *e = vec_expr_alloc(EXPR_ARITH);
        e->op = op_str[0];
        e->left = parse_expr(list_get(lst, "left"), schema);
        e->right = parse_expr(list_get(lst, "right"), schema);
        /* Infer result type */
        VecType lt = e->left->result_type;
        VecType rt = e->right->result_type;
        if (lt == VEC_DOUBLE || rt == VEC_DOUBLE)
            e->result_type = VEC_DOUBLE;
        else
            e->result_type = VEC_INT64;
        return e;
    }
    if (strcmp(kind, "cmp") == 0) {
        const char *op_str = list_get_string(lst, "op");
        VecExpr *e = vec_expr_alloc(EXPR_CMP);
        e->op = op_str[0];
        e->op2 = (strlen(op_str) > 1) ? op_str[1] : ' ';
        e->left = parse_expr(list_get(lst, "left"), schema);
        e->right = parse_expr(list_get(lst, "right"), schema);
        e->result_type = VEC_BOOL;
        return e;
    }
    if (strcmp(kind, "bool") == 0) {
        const char *op_str = list_get_string(lst, "op");
        VecExpr *e = vec_expr_alloc(EXPR_BOOL);
        e->op = op_str[0];
        if (op_str[0] == '!') {
            e->operand = parse_expr(list_get(lst, "operand"), schema);
        } else {
            e->left = parse_expr(list_get(lst, "left"), schema);
            e->right = parse_expr(list_get(lst, "right"), schema);
        }
        e->result_type = VEC_BOOL;
        return e;
    }
    if (strcmp(kind, "is_na") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_IS_NA);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->result_type = VEC_BOOL;
        return e;
    }
    if (strcmp(kind, "negate") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_NEGATE);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->result_type = e->operand->result_type;
        return e;
    }
    if (strcmp(kind, "nchar") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_NCHAR);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->result_type = VEC_INT64;
        return e;
    }
    if (strcmp(kind, "substr") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_SUBSTR);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->left = parse_expr(list_get(lst, "start"), schema);
        e->right = parse_expr(list_get(lst, "stop"), schema);
        e->result_type = VEC_STRING;
        return e;
    }
    if (strcmp(kind, "grepl") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_GREPL);
        const char *pat = list_get_string(lst, "pattern");
        if (!pat) vectra_error("grepl missing 'pattern'");
        e->lit_str = (char *)malloc(strlen(pat) + 1);
        strcpy(e->lit_str, pat);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->result_type = VEC_BOOL;
        return e;
    }

    vectra_error("unknown expression kind: %s", kind);
    return NULL;
}

/* --- C_filter_node --- */

SEXP C_filter_node(SEXP node_xptr, SEXP expr_list) {
    VecNode *child = unwrap_node(node_xptr);
    /* Clear the external pointer so the old R object can't double-free */
    R_ClearExternalPtr(node_xptr);

    VecExpr *pred = parse_expr(expr_list, &child->output_schema);
    FilterNode *fn = filter_node_create(child, pred);
    return wrap_node((VecNode *)fn);
}

/* --- C_project_node --- */

SEXP C_project_node(SEXP node_xptr, SEXP names, SEXP expr_lists) {
    VecNode *child = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);

    const VecSchema *schema = &child->output_schema;

    /* Build a mutable schema that we extend as mutate entries are parsed,
       so later entries can reference earlier ones */
    int n = Rf_length(names);
    int schema_n = schema->n_cols;
    int schema_cap = schema_n + n;
    char **ext_names = (char **)malloc((size_t)schema_cap * sizeof(char *));
    VecType *ext_types = (VecType *)malloc((size_t)schema_cap * sizeof(VecType));
    for (int i = 0; i < schema_n; i++) {
        ext_names[i] = schema->col_names[i];
        ext_types[i] = schema->col_types[i];
    }
    VecSchema ext_schema;
    memset(&ext_schema, 0, sizeof(ext_schema));
    ext_schema.n_cols = schema_n;
    ext_schema.col_names = ext_names;
    ext_schema.col_types = ext_types;

    ProjEntry *entries = (ProjEntry *)calloc((size_t)n, sizeof(ProjEntry));
    if (!entries) vectra_error("alloc failed for ProjEntry");

    for (int i = 0; i < n; i++) {
        const char *nm = CHAR(STRING_ELT(names, i));
        entries[i].output_name = (char *)malloc(strlen(nm) + 1);
        strcpy(entries[i].output_name, nm);

        SEXP expr = VECTOR_ELT(expr_lists, i);
        if (expr == R_NilValue) {
            entries[i].expr = NULL; /* pass-through */
        } else {
            entries[i].expr = parse_expr(expr, &ext_schema);
            /* Add this entry to the extended schema so later entries
               can reference it */
            ext_names[ext_schema.n_cols] = entries[i].output_name;
            ext_types[ext_schema.n_cols] = entries[i].expr->result_type;
            ext_schema.n_cols++;
        }
    }

    free(ext_names);
    free(ext_types);

    ProjectNode *pn = project_node_create(child, n, entries);
    return wrap_node((VecNode *)pn);
}

/* --- C_group_agg_node --- */

static AggKind parse_agg_kind(const char *s) {
    if (strcmp(s, "n") == 0 || strcmp(s, "count_star") == 0) return AGG_COUNT_STAR;
    if (strcmp(s, "count") == 0) return AGG_COUNT;
    if (strcmp(s, "sum") == 0) return AGG_SUM;
    if (strcmp(s, "mean") == 0) return AGG_MEAN;
    if (strcmp(s, "min") == 0) return AGG_MIN;
    if (strcmp(s, "max") == 0) return AGG_MAX;
    vectra_error("unknown aggregation function: %s", s);
    return AGG_COUNT; /* unreachable */
}

SEXP C_group_agg_node(SEXP node_xptr, SEXP key_names_sexp, SEXP agg_specs_sexp) {
    VecNode *child = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);

    /* Parse key names */
    int n_keys = Rf_length(key_names_sexp);
    char **key_names = (char **)malloc((size_t)n_keys * sizeof(char *));
    for (int k = 0; k < n_keys; k++) {
        const char *nm = CHAR(STRING_ELT(key_names_sexp, k));
        key_names[k] = (char *)malloc(strlen(nm) + 1);
        strcpy(key_names[k], nm);
    }

    /* Parse agg specs: list of lists with $name, $kind, $col, $na_rm */
    int n_aggs = Rf_length(agg_specs_sexp);
    AggSpec *specs = (AggSpec *)calloc((size_t)n_aggs, sizeof(AggSpec));
    for (int a = 0; a < n_aggs; a++) {
        SEXP spec = VECTOR_ELT(agg_specs_sexp, a);
        const char *name = list_get_string(spec, "name");
        const char *kind = list_get_string(spec, "kind");
        const char *col = list_get_string(spec, "col");
        SEXP na_rm_sexp = list_get(spec, "na_rm");

        specs[a].output_name = (char *)malloc(strlen(name) + 1);
        strcpy(specs[a].output_name, name);
        specs[a].kind = parse_agg_kind(kind);
        if (col) {
            specs[a].input_col = (char *)malloc(strlen(col) + 1);
            strcpy(specs[a].input_col, col);
        }
        specs[a].na_rm = (na_rm_sexp != R_NilValue) ? Rf_asLogical(na_rm_sexp) : 0;
    }

    GroupAggNode *ga = group_agg_node_create(child, n_keys, key_names,
                                              n_aggs, specs, get_r_tempdir());
    return wrap_node((VecNode *)ga);
}

/* --- C_sort_node --- */

SEXP C_sort_node(SEXP node_xptr, SEXP col_names_sexp, SEXP desc_sexp) {
    VecNode *child = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);

    const VecSchema *schema = &child->output_schema;
    int n_keys = Rf_length(col_names_sexp);

    SortKey *keys = (SortKey *)malloc((size_t)n_keys * sizeof(SortKey));
    for (int k = 0; k < n_keys; k++) {
        const char *nm = CHAR(STRING_ELT(col_names_sexp, k));
        int idx = vec_schema_find_col(schema, nm);
        if (idx < 0)
            vectra_error("arrange: column not found: %s", nm);
        keys[k].col_index = idx;
        keys[k].descending = LOGICAL(desc_sexp)[k];
    }

    SortNode *sn = sort_node_create(child, n_keys, keys, get_r_tempdir());
    return wrap_node((VecNode *)sn);
}

/* --- C_limit_node --- */

SEXP C_limit_node(SEXP node_xptr, SEXP n_sexp) {
    VecNode *child = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);

    int64_t max_rows = (int64_t)Rf_asReal(n_sexp);
    LimitNode *ln = limit_node_create(child, max_rows);
    return wrap_node((VecNode *)ln);
}

/* --- C_topn_node --- */

SEXP C_topn_node(SEXP node_xptr, SEXP col_names_sexp,
                  SEXP desc_sexp, SEXP n_sexp) {
    VecNode *child = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);

    const VecSchema *schema = &child->output_schema;
    int n_keys = Rf_length(col_names_sexp);

    SortKey *keys = (SortKey *)malloc((size_t)n_keys * sizeof(SortKey));
    for (int k = 0; k < n_keys; k++) {
        const char *nm = CHAR(STRING_ELT(col_names_sexp, k));
        int idx = vec_schema_find_col(schema, nm);
        if (idx < 0)
            vectra_error("topn: column not found: %s", nm);
        keys[k].col_index = idx;
        keys[k].descending = LOGICAL(desc_sexp)[k];
    }

    int64_t limit = (int64_t)Rf_asReal(n_sexp);
    TopNNode *tn = topn_node_create(child, n_keys, keys, limit);
    return wrap_node((VecNode *)tn);
}

/* --- C_join_node --- */

SEXP C_join_node(SEXP left_xptr, SEXP right_xptr,
                 SEXP kind_sexp, SEXP left_keys_sexp, SEXP right_keys_sexp,
                 SEXP suffix_x_sexp, SEXP suffix_y_sexp) {
    VecNode *left = unwrap_node(left_xptr);
    R_ClearExternalPtr(left_xptr);
    VecNode *right = unwrap_node(right_xptr);
    R_ClearExternalPtr(right_xptr);

    const char *kind_str = CHAR(STRING_ELT(kind_sexp, 0));
    JoinKind kind = JOIN_INNER;
    if (strcmp(kind_str, "inner") == 0) kind = JOIN_INNER;
    else if (strcmp(kind_str, "left") == 0) kind = JOIN_LEFT;
    else if (strcmp(kind_str, "full") == 0) kind = JOIN_FULL;
    else if (strcmp(kind_str, "semi") == 0) kind = JOIN_SEMI;
    else if (strcmp(kind_str, "anti") == 0) kind = JOIN_ANTI;
    else vectra_error("unknown join kind: %s", kind_str);

    const VecSchema *lschema = &left->output_schema;
    const VecSchema *rschema = &right->output_schema;

    int n_keys = Rf_length(left_keys_sexp);
    JoinKey *keys = (JoinKey *)malloc((size_t)n_keys * sizeof(JoinKey));
    for (int k = 0; k < n_keys; k++) {
        const char *lk = CHAR(STRING_ELT(left_keys_sexp, k));
        const char *rk = CHAR(STRING_ELT(right_keys_sexp, k));
        keys[k].left_col = vec_schema_find_col(lschema, lk);
        keys[k].right_col = vec_schema_find_col(rschema, rk);
        if (keys[k].left_col < 0)
            vectra_error("join: left key column not found: %s", lk);
        if (keys[k].right_col < 0)
            vectra_error("join: right key column not found: %s", rk);
    }

    const char *sx = CHAR(STRING_ELT(suffix_x_sexp, 0));
    const char *sy = CHAR(STRING_ELT(suffix_y_sexp, 0));

    JoinNode *jn = join_node_create(left, right, kind, n_keys, keys, sx, sy);
    return wrap_node((VecNode *)jn);
}

/* --- C_window_node --- */

static WinKind parse_win_kind(const char *s) {
    if (strcmp(s, "lag") == 0) return WIN_LAG;
    if (strcmp(s, "lead") == 0) return WIN_LEAD;
    if (strcmp(s, "row_number") == 0) return WIN_ROW_NUMBER;
    if (strcmp(s, "rank") == 0) return WIN_RANK;
    if (strcmp(s, "dense_rank") == 0) return WIN_DENSE_RANK;
    if (strcmp(s, "cumsum") == 0) return WIN_CUMSUM;
    if (strcmp(s, "cummean") == 0) return WIN_CUMMEAN;
    if (strcmp(s, "cummin") == 0) return WIN_CUMMIN;
    if (strcmp(s, "cummax") == 0) return WIN_CUMMAX;
    vectra_error("unknown window function: %s", s);
    return WIN_LAG; /* unreachable */
}

SEXP C_window_node(SEXP node_xptr, SEXP key_names_sexp, SEXP win_specs_sexp) {
    VecNode *child = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);

    int n_keys = Rf_length(key_names_sexp);
    char **key_names = (char **)malloc((size_t)n_keys * sizeof(char *));
    for (int k = 0; k < n_keys; k++) {
        const char *nm = CHAR(STRING_ELT(key_names_sexp, k));
        key_names[k] = (char *)malloc(strlen(nm) + 1);
        strcpy(key_names[k], nm);
    }

    int n_wins = Rf_length(win_specs_sexp);
    WinSpec *specs = (WinSpec *)calloc((size_t)n_wins, sizeof(WinSpec));
    for (int w = 0; w < n_wins; w++) {
        SEXP spec = VECTOR_ELT(win_specs_sexp, w);
        const char *name = list_get_string(spec, "name");
        const char *kind = list_get_string(spec, "kind");
        const char *col = list_get_string(spec, "col");
        SEXP offset_sexp = list_get(spec, "offset");
        SEXP default_sexp = list_get(spec, "default");

        specs[w].output_name = (char *)malloc(strlen(name) + 1);
        strcpy(specs[w].output_name, name);
        specs[w].kind = parse_win_kind(kind);
        if (col) {
            specs[w].input_col = (char *)malloc(strlen(col) + 1);
            strcpy(specs[w].input_col, col);
        }
        specs[w].offset = (offset_sexp != R_NilValue) ? Rf_asInteger(offset_sexp) : 1;
        if (default_sexp != R_NilValue && !Rf_isNull(default_sexp)) {
            specs[w].default_val = Rf_asReal(default_sexp);
            specs[w].has_default = 1;
        }
    }

    WindowNode *wn = window_node_create(child, n_keys, key_names, n_wins, specs);
    return wrap_node((VecNode *)wn);
}

/* --- C_concat_node --- */

SEXP C_concat_node(SEXP node_xptrs) {
    int n = Rf_length(node_xptrs);
    VecNode **children = (VecNode **)malloc((size_t)n * sizeof(VecNode *));
    for (int i = 0; i < n; i++) {
        SEXP xptr = VECTOR_ELT(node_xptrs, i);
        children[i] = unwrap_node(xptr);
        R_ClearExternalPtr(xptr);
    }
    ConcatNode *cn = concat_node_create(n, children);
    return wrap_node((VecNode *)cn);
}

/* --- C_write_csv --- */

SEXP C_csv_scan_node(SEXP path_sexp, SEXP batch_size_sexp) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));
    int64_t batch_size = (int64_t)Rf_asReal(batch_size_sexp);
    CsvScanNode *sn = csv_scan_node_create(fpath, batch_size);
    return wrap_node((VecNode *)sn);
}

SEXP C_write_csv(SEXP node_xptr, SEXP path_sexp) {
    VecNode *node = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    csv_write_node(node, path);
    node->free_node(node);
    return R_NilValue;
}

SEXP C_sql_scan_node(SEXP path_sexp, SEXP table_sexp, SEXP batch_size_sexp) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));
    const char *table = CHAR(STRING_ELT(table_sexp, 0));
    int64_t batch_size = (int64_t)Rf_asReal(batch_size_sexp);
    SqlScanNode *sn = sql_scan_node_create(fpath, table, batch_size);
    return wrap_node((VecNode *)sn);
}

SEXP C_write_sqlite(SEXP node_xptr, SEXP path_sexp, SEXP table_sexp) {
    VecNode *node = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    const char *table = CHAR(STRING_ELT(table_sexp, 0));
    sql_write_node(node, path, table);
    node->free_node(node);
    return R_NilValue;
}

SEXP C_tiff_scan_node(SEXP path_sexp, SEXP batch_size_sexp) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));
    int64_t batch_size = (int64_t)Rf_asReal(batch_size_sexp);
    TiffScanNode *sn = tiff_scan_node_create(fpath, batch_size);
    return wrap_node((VecNode *)sn);
}

SEXP C_tiff_scan_meta(SEXP node_xptr) {
    VecNode *node = unwrap_node(node_xptr);
    if (!node->kind || strcmp(node->kind, "TiffScanNode") != 0)
        vectra_error("not a TiffScanNode");
    TiffScanNode *sn = (TiffScanNode *)node;
    TiffReader *r = sn->reader;

    SEXP result = PROTECT(Rf_allocVector(VECSXP, 5));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, 5));

    SET_STRING_ELT(names, 0, Rf_mkChar("width"));
    SET_STRING_ELT(names, 1, Rf_mkChar("height"));
    SET_STRING_ELT(names, 2, Rf_mkChar("nbands"));
    SET_STRING_ELT(names, 3, Rf_mkChar("gt"));
    SET_STRING_ELT(names, 4, Rf_mkChar("nodata"));

    SET_VECTOR_ELT(result, 0, Rf_ScalarReal((double)tiff_reader_width(r)));
    SET_VECTOR_ELT(result, 1, Rf_ScalarReal((double)tiff_reader_height(r)));
    SET_VECTOR_ELT(result, 2, Rf_ScalarInteger(tiff_reader_nbands(r)));

    SEXP gt_sexp = PROTECT(Rf_allocVector(REALSXP, 6));
    const double *gt = tiff_reader_geotransform(r);
    memcpy(REAL(gt_sexp), gt, 6 * sizeof(double));
    SET_VECTOR_ELT(result, 3, gt_sexp);

    if (tiff_reader_has_nodata(r))
        SET_VECTOR_ELT(result, 4, Rf_ScalarReal(tiff_reader_nodata(r)));
    else
        SET_VECTOR_ELT(result, 4, Rf_ScalarReal(NA_REAL));

    Rf_setAttrib(result, R_NamesSymbol, names);
    UNPROTECT(3);
    return result;
}

SEXP C_write_tiff(SEXP node_xptr, SEXP path_sexp, SEXP compress_sexp) {
    VecNode *node = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    int use_deflate = Rf_asLogical(compress_sexp);
    tiff_write_node(node, path, use_deflate);
    node->free_node(node);
    return R_NilValue;
}
