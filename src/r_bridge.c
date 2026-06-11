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
#include "vtr1_tdc.h"
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

/* Parse col_types: named character vector, e.g. c(x = "int8", y = "int16")
   Returns allocated array of VecType overrides indexed by column position.
   Non-overridden columns get VEC_INT64 as sentinel (meaning "keep default").
   Caller must free the result. Returns NULL if col_types is R_NilValue. */
static VecType *parse_col_types(SEXP col_types, SEXP df_names, int n_cols) {
    if (col_types == R_NilValue) return NULL;
    if (TYPEOF(col_types) != STRSXP)
        vectra_error("col_types must be a named character vector");
    SEXP ct_names = Rf_getAttrib(col_types, R_NamesSymbol);
    if (ct_names == R_NilValue)
        vectra_error("col_types must be named");

    VecType *overrides = (VecType *)malloc((size_t)n_cols * sizeof(VecType));
    for (int i = 0; i < n_cols; i++) overrides[i] = VEC_INT64; /* sentinel: no override */

    int n_ct = Rf_length(col_types);
    for (int j = 0; j < n_ct; j++) {
        const char *cname = CHAR(STRING_ELT(ct_names, j));
        const char *tname = CHAR(STRING_ELT(col_types, j));
        VecType target;
        if (strcmp(tname, "int8") == 0) target = VEC_INT8;
        else if (strcmp(tname, "int16") == 0) target = VEC_INT16;
        else if (strcmp(tname, "int32") == 0) target = VEC_INT32;
        else vectra_error("col_types value '%s' must be 'int8', 'int16', or 'int32'", tname);

        /* Find matching column */
        int found = 0;
        for (int i = 0; i < n_cols; i++) {
            if (strcmp(CHAR(STRING_ELT(df_names, i)), cname) == 0) {
                overrides[i] = target;
                found = 1;
                break;
            }
        }
        if (!found)
            vectra_error("col_types name '%s' not found in data.frame columns", cname);
    }
    return overrides;
}

/* Narrow an int64 VecArray in-place to a target narrow type.
   Allocates new buffer, copies with truncation, frees old. */
static void narrow_int_array(VecArray *arr, VecType target) {
    int64_t n = arr->length;
    switch (target) {
    case VEC_INT32: {
        int32_t *buf = (int32_t *)calloc((size_t)n, sizeof(int32_t));
        if (!buf) vectra_error("alloc failed for int32 narrowing");
        for (int64_t i = 0; i < n; i++)
            if (vec_array_is_valid(arr, i))
                buf[i] = (int32_t)arr->buf.i64[i];
        free(arr->buf.i64);
        arr->buf.i32 = buf;
        arr->type = VEC_INT32;
        break;
    }
    case VEC_INT16: {
        int16_t *buf = (int16_t *)calloc((size_t)n, sizeof(int16_t));
        if (!buf) vectra_error("alloc failed for int16 narrowing");
        for (int64_t i = 0; i < n; i++)
            if (vec_array_is_valid(arr, i))
                buf[i] = (int16_t)arr->buf.i64[i];
        free(arr->buf.i64);
        arr->buf.i16 = buf;
        arr->type = VEC_INT16;
        break;
    }
    case VEC_INT8: {
        int8_t *buf = (int8_t *)calloc((size_t)n, sizeof(int8_t));
        if (!buf) vectra_error("alloc failed for int8 narrowing");
        for (int64_t i = 0; i < n; i++)
            if (vec_array_is_valid(arr, i))
                buf[i] = (int8_t)arr->buf.i64[i];
        free(arr->buf.i64);
        arr->buf.i8 = buf;
        arr->type = VEC_INT8;
        break;
    }
    default: break;
    }
}

/* Build a narrow-typed array directly from R integer data */
static VecArray r_int_to_narrow(SEXP col, VecType target, int64_t start, int64_t n) {
    VecArray arr = vec_array_alloc(target, n);
    vec_array_set_all_valid(&arr);
    int *ip = INTEGER(col);
    for (int64_t i = 0; i < n; i++) {
        if (ip[start + i] == NA_INTEGER) {
            vec_array_set_null(&arr, i);
        } else {
            switch (target) {
            case VEC_INT32: arr.buf.i32[i] = (int32_t)ip[start + i]; break;
            case VEC_INT16: arr.buf.i16[i] = (int16_t)ip[start + i]; break;
            case VEC_INT8:  arr.buf.i8[i]  = (int8_t)ip[start + i]; break;
            default: break;
            }
        }
    }
    return arr;
}

/* --- C_write_vtr --- */

SEXP C_write_vtr(SEXP df, SEXP path, SEXP batch_size, SEXP compress_sexp,
                 SEXP col_types_sexp, SEXP quantize_sexp, SEXP spatial_sexp) {
    if (!Rf_isNewList(df)) vectra_error("first argument must be a data.frame");
    const char *fpath = CHAR(STRING_ELT(path, 0));
    int bs = Rf_asInteger(batch_size);

    int comp_level = 1; /* default: fast */
    if (compress_sexp != R_NilValue && TYPEOF(compress_sexp) == STRSXP &&
        Rf_length(compress_sexp) > 0) {
        const char *cstr = CHAR(STRING_ELT(compress_sexp, 0));
        if (strcmp(cstr, "fast") == 0) comp_level = 1;
        else if (strcmp(cstr, "small") == 0) comp_level = 2;
        else if (strcmp(cstr, "none") == 0) comp_level = 0;
        else vectra_error("unknown compress level '%s' (expected \"fast\", \"small\", or \"none\")", cstr);
    }

    int n_cols = Rf_length(df);
    SEXP first_col = VECTOR_ELT(df, 0);
    int64_t n_rows = (int64_t)XLENGTH(first_col);

    /* Parse col_types overrides. df_names is PROTECTed across the
     * parse_col_types / parse_quantize / parse_spatial calls below
     * because rchk treats getAttrib results as fresh-allocated SEXPs
     * even though they're rooted via `df`'s attribute pairlist. */
    SEXP df_names = PROTECT(Rf_getAttrib(df, R_NamesSymbol));
    VecType *col_type_overrides = parse_col_types(col_types_sexp, df_names, n_cols);

    /* Parse quantize + spatial specs */
    VtrQuantizeSpec *qspecs = parse_quantize(quantize_sexp, df_names, n_cols);
    VtrSpatialSpec *sspecs = parse_spatial(spatial_sexp, df_names, n_cols);

    /* Build annotations for all columns */
    char **annotations = (char **)calloc((size_t)n_cols, sizeof(char *));
    for (int i = 0; i < n_cols; i++)
        annotations[i] = r_col_annotation(VECTOR_ELT(df, i));

    if (bs <= 0 || (int64_t)bs >= n_rows) {
        /* Single row group */
        VecBatch *batch = df_to_batch(df);

        /* Apply col_types narrowing */
        if (col_type_overrides) {
            for (int i = 0; i < n_cols; i++) {
                VecType ov = col_type_overrides[i];
                if (ov == VEC_INT8 || ov == VEC_INT16 || ov == VEC_INT32) {
                    if (batch->columns[i].type == VEC_INT64)
                        narrow_int_array(&batch->columns[i], ov);
                }
            }
        }

        /* Build schema with annotations */
        VecSchema schema;
        memset(&schema, 0, sizeof(schema));
        schema.n_cols = batch->n_cols;
        schema.col_names = batch->col_names;
        schema.col_types = (VecType *)malloc((size_t)batch->n_cols * sizeof(VecType));
        schema.col_annotations = annotations;
        for (int i = 0; i < batch->n_cols; i++)
            schema.col_types[i] = batch->columns[i].type;

        Vtr1TdcWriter *w = vtr1_open_tdc_writer(fpath, &schema);
        vtr1_write_rowgroup_tdc(w, batch, comp_level, qspecs, sspecs);
        vtr1_close_tdc_writer(w);

        free(schema.col_types);
        vec_batch_free(batch);
    } else {
        /* Multiple row groups */
        uint32_t n_rg = (uint32_t)((n_rows + bs - 1) / bs);

        /* Build schema from first few elements. names is PROTECTed
         * across the r_col_type allocating call inside the loop. */
        SEXP names = PROTECT(Rf_getAttrib(df, R_NamesSymbol));
        char **col_names = (char **)malloc((size_t)n_cols * sizeof(char *));
        VecType *col_types = (VecType *)malloc((size_t)n_cols * sizeof(VecType));
        for (int i = 0; i < n_cols; i++) {
            col_names[i] = (char *)CHAR(STRING_ELT(names, i));
            col_types[i] = r_col_type(VECTOR_ELT(df, i));
            /* Apply col_types overrides */
            if (col_type_overrides) {
                VecType ov = col_type_overrides[i];
                if (ov == VEC_INT8 || ov == VEC_INT16 || ov == VEC_INT32)
                    col_types[i] = ov;
            }
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

        Vtr1TdcWriter *w = vtr1_open_tdc_writer(fpath, &schema);

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

                size_t cn_len = strlen(schema.col_names[c]);
                batch->col_names[c] = (char *)malloc(cn_len + 1);
                memcpy(batch->col_names[c], schema.col_names[c], cn_len + 1);

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
                case VEC_INT8:
                case VEC_INT16:
                case VEC_INT32: {
                    /* Narrow int from R integer */
                    vec_array_free(&arr);
                    arr = r_int_to_narrow(col, type, start, rg_rows);
                    break;
                }
                case VEC_INT64:
                    if (Rf_isInteger(col)) {
                        int *ip = INTEGER(col) + start;
                        int has_na = 0;
                        for (int64_t i = 0; i < rg_rows; i++) {
                            if (ip[i] == NA_INTEGER) { has_na = 1; break; }
                        }
                        if (!has_na) {
                            for (int64_t i = 0; i < rg_rows; i++)
                                arr.buf.i64[i] = (int64_t)ip[i];
                        } else {
                            for (int64_t i = 0; i < rg_rows; i++) {
                                if (ip[i] == NA_INTEGER) {
                                    vec_array_set_null(&arr, i);
                                } else {
                                    arr.buf.i64[i] = (int64_t)ip[i];
                                }
                            }
                        }
                    } else {
                        double *dp = REAL(col) + start;
                        for (int64_t i = 0; i < rg_rows; i++) {
                            int64_t v;
                            memcpy(&v, &dp[i], sizeof(int64_t));
                            if (v == INT64_MIN) {
                                vec_array_set_null(&arr, i);
                            } else {
                                arr.buf.i64[i] = v;
                            }
                        }
                    }
                    break;
                case VEC_DOUBLE: {
                    double *dp = REAL(col) + start;
                    int has_na = 0;
                    for (int64_t i = 0; i < rg_rows; i++) {
                        if (ISNAN(dp[i])) { has_na = 1; break; }
                    }
                    if (!has_na) {
                        memcpy(arr.buf.dbl, dp, (size_t)rg_rows * sizeof(double));
                    } else {
                        for (int64_t i = 0; i < rg_rows; i++) {
                            if (ISNA(dp[i]) || ISNAN(dp[i])) {
                                vec_array_set_null(&arr, i);
                            } else {
                                arr.buf.dbl[i] = dp[i];
                            }
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

            vtr1_write_rowgroup_tdc(w, batch, comp_level, qspecs, sspecs);
            vec_batch_free(batch);
        }

        vtr1_close_tdc_writer(w);
        vec_schema_free(&schema);
    }

    /* Free any annotations not transferred to schema */
    for (int i = 0; i < n_cols; i++)
        free(annotations[i]);
    free(annotations);
    free(col_type_overrides);
    free(qspecs);
    free(sspecs);

    /* df_names + (multi-rg branch's) names. Both branches PROTECT df_names
     * once; the multi-rg branch additionally PROTECTs `names`. We track
     * the count so this UNPROTECT is correct in both paths. */
    UNPROTECT(bs <= 0 || (int64_t)bs >= n_rows ? 1 : 2);
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

/* --- C_node_optimize / C_node_next_batch ---
   Streaming pull interface backing collect_chunked() and chunk_feeder().
   The R cursor calls C_node_optimize once when opened, then C_node_next_batch
   repeatedly: each call returns the next non-empty batch as a data.frame, or
   NULL at end of stream. The node carries the pull cursor between calls, so the
   node is consumed and cannot be reused once the stream is drained. */

SEXP C_node_optimize(SEXP node_xptr) {
    VecNode *node = unwrap_node(node_xptr);
    vec_optimize(node);
    return R_NilValue;
}

SEXP C_node_next_batch(SEXP node_xptr) {
    VecNode *node = unwrap_node(node_xptr);
    return vec_collect_next(node);
}

/* --- C_node_schema --- */

SEXP C_node_schema(SEXP node_xptr) {
    VecNode *node = unwrap_node(node_xptr);
    const VecSchema *schema = &node->output_schema;

    SEXP result = PROTECT(Rf_allocVector(VECSXP, 3));
    SEXP col_names = PROTECT(Rf_allocVector(STRSXP, schema->n_cols));
    SEXP col_types = PROTECT(Rf_allocVector(STRSXP, schema->n_cols));
    SEXP col_annotations = PROTECT(Rf_allocVector(STRSXP, schema->n_cols));

    const char *type_names[] = {"int64", "double", "bool", "string",
                                "int8", "int16", "int32"};
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
        int file_cols = vtr1_tdc_schema(sn->file)->n_cols;
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
        pos += snprintf(buf + pos, (size_t)(bufsize - pos), ", tdc stats");
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

    size_t buf_len = strlen(buf);
    lines[*count] = (char *)malloc(buf_len + 1);
    memcpy(lines[*count], buf, buf_len + 1);
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
