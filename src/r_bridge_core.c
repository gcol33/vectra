#include "r_bridge.h"
#include "r_bridge_internal.h"
#include "types.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
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
#include "expr.h"
#include "optimize.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Get R's temp directory via the public API (avoids R_TempDir non-API call) */
const char *get_r_tempdir(void) {
    SEXP call = PROTECT(Rf_lang1(Rf_install("tempdir")));
    SEXP result = PROTECT(Rf_eval(call, R_BaseEnv));
    const char *path = CHAR(STRING_ELT(result, 0));
    UNPROTECT(2);
    return path;
}

/* --- External pointer helpers --- */

void node_finalizer(SEXP xptr) {
    VecNode *node = (VecNode *)R_ExternalPtrAddr(xptr);
    if (node) {
        node->free_node(node);
        R_ClearExternalPtr(xptr);
    }
}

SEXP wrap_node(VecNode *node) {
    SEXP xptr = PROTECT(R_MakeExternalPtr(node, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(xptr, node_finalizer, TRUE);
    UNPROTECT(1);
    return xptr;
}

VecNode *unwrap_node(SEXP xptr) {
    VecNode *node = (VecNode *)R_ExternalPtrAddr(xptr);
    if (!node) vectra_error("vectra node has been freed or collected");
    return node;
}

/* --- Detect R column type -> VecType + annotation --- */

int r_has_class(SEXP col, const char *cls_name) {
    SEXP cls = Rf_getAttrib(col, R_ClassSymbol);
    if (cls == R_NilValue || TYPEOF(cls) != STRSXP) return 0;
    for (R_xlen_t i = 0; i < XLENGTH(cls); i++) {
        if (strcmp(CHAR(STRING_ELT(cls, i)), cls_name) == 0) return 1;
    }
    return 0;
}

VecType r_col_type(SEXP col) {
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
char *r_col_annotation(SEXP col) {
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

VecBatch *df_to_batch(SEXP df) {
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
            free(arr.buf.str.data);  /* free 1-byte from vec_array_alloc */
            arr.buf.str.data = (char *)malloc((size_t)(total_len > 0 ? total_len : 1));
            if (!arr.buf.str.data)
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
                /* Fast scan for NA */
                int has_na = 0;
                for (int64_t i = 0; i < n_rows; i++) {
                    if (ip[i] == NA_INTEGER) { has_na = 1; break; }
                }
                if (!has_na) {
                    /* Widen int32 → int64 in bulk (no NA checks) */
                    for (int64_t i = 0; i < n_rows; i++)
                        arr.buf.i64[i] = (int64_t)ip[i];
                } else {
                    for (int64_t i = 0; i < n_rows; i++) {
                        if (ip[i] == NA_INTEGER) {
                            vec_array_set_null(&arr, i);
                            arr.buf.i64[i] = 0;
                        } else {
                            arr.buf.i64[i] = (int64_t)ip[i];
                        }
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
        case VEC_INT8: {
            int *ip = INTEGER(col);
            for (int64_t i = 0; i < n_rows; i++) {
                if (ip[i] == NA_INTEGER) {
                    vec_array_set_null(&arr, i);
                    arr.buf.i8[i] = 0;
                } else {
                    arr.buf.i8[i] = (int8_t)ip[i];
                }
            }
            break;
        }
        case VEC_INT16: {
            int *ip = INTEGER(col);
            for (int64_t i = 0; i < n_rows; i++) {
                if (ip[i] == NA_INTEGER) {
                    vec_array_set_null(&arr, i);
                    arr.buf.i16[i] = 0;
                } else {
                    arr.buf.i16[i] = (int16_t)ip[i];
                }
            }
            break;
        }
        case VEC_INT32: {
            int *ip = INTEGER(col);
            for (int64_t i = 0; i < n_rows; i++) {
                if (ip[i] == NA_INTEGER) {
                    vec_array_set_null(&arr, i);
                    arr.buf.i32[i] = 0;
                } else {
                    arr.buf.i32[i] = (int32_t)ip[i];
                }
            }
            break;
        }
        case VEC_DOUBLE: {
            double *dp = REAL(col);
            /* Fast scan: check if any NAs exist */
            int has_na = 0;
            for (int64_t i = 0; i < n_rows; i++) {
                if (ISNAN(dp[i])) { has_na = 1; break; }
            }
            if (!has_na) {
                /* Bulk copy — no NAs */
                memcpy(arr.buf.dbl, dp, (size_t)n_rows * sizeof(double));
            } else {
                for (int64_t i = 0; i < n_rows; i++) {
                    if (ISNA(dp[i]) || ISNAN(dp[i])) {
                        vec_array_set_null(&arr, i);
                        arr.buf.dbl[i] = 0.0;
                    } else {
                        arr.buf.dbl[i] = dp[i];
                    }
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
            free(arr.buf.str.data);  /* free 1-byte from vec_array_alloc */
            arr.buf.str.data = (char *)malloc((size_t)(total_len > 0 ? total_len : 1));
            if (!arr.buf.str.data)
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

/* --- Expression parser: R list -> VecExpr --- */

const char *list_get_string(SEXP lst, const char *field) {
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

SEXP list_get(SEXP lst, const char *field) {
    SEXP names = Rf_getAttrib(lst, R_NamesSymbol);
    for (R_xlen_t i = 0; i < XLENGTH(lst); i++) {
        if (strcmp(CHAR(STRING_ELT(names, i)), field) == 0)
            return VECTOR_ELT(lst, i);
    }
    return R_NilValue;
}

VecExpr *parse_expr(SEXP lst, const VecSchema *schema) {
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
        SEXP fixed_sexp = list_get(lst, "fixed");
        e->fixed = (fixed_sexp != R_NilValue) ? Rf_asLogical(fixed_sexp) : 1;
        e->result_type = VEC_BOOL;
        return e;
    }

    if (strcmp(kind, "math_unary") == 0) {
        const char *fn = list_get_string(lst, "fn");
        VecExpr *e = vec_expr_alloc(EXPR_MATH_UNARY);
        e->math_fn = fn[0];
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->result_type = VEC_DOUBLE;
        return e;
    }
    if (strcmp(kind, "if_else") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_IF_ELSE);
        e->cond = parse_expr(list_get(lst, "cond"), schema);
        e->then_expr = parse_expr(list_get(lst, "then_expr"), schema);
        e->else_expr = parse_expr(list_get(lst, "else_expr"), schema);
        /* Must match the common type the evaluator coerces both branches to
           (string > double > int64 > bool); otherwise the column schema and the
           produced array disagree and int64/double bits get reinterpreted -- e.g.
           ifelse(int64_col, NA) where the NA literal is typed double. */
        VecType tt = e->then_expr->result_type, et = e->else_expr->result_type;
        if (tt == VEC_STRING || et == VEC_STRING)      e->result_type = VEC_STRING;
        else if (tt == VEC_DOUBLE || et == VEC_DOUBLE) e->result_type = VEC_DOUBLE;
        else                                           e->result_type = tt;
        return e;
    }
    if (strcmp(kind, "cast") == 0) {
        const char *to = list_get_string(lst, "to");
        VecExpr *e = vec_expr_alloc(EXPR_CAST);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        if (strcmp(to, "double") == 0) e->cast_to = VEC_DOUBLE;
        else if (strcmp(to, "int64") == 0) e->cast_to = VEC_INT64;
        else if (strcmp(to, "bool") == 0) e->cast_to = VEC_BOOL;
        else if (strcmp(to, "string") == 0) e->cast_to = VEC_STRING;
        else vectra_error("unknown cast target: %s", to);
        e->result_type = e->cast_to;
        return e;
    }
    if (strcmp(kind, "tolower") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_TOLOWER);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->result_type = VEC_STRING;
        return e;
    }
    if (strcmp(kind, "toupper") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_TOUPPER);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->result_type = VEC_STRING;
        return e;
    }
    if (strcmp(kind, "trimws") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_TRIMWS);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->result_type = VEC_STRING;
        return e;
    }
    if (strcmp(kind, "in") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_IN);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        SEXP set_sexp = list_get(lst, "set");
        e->n_set = Rf_length(set_sexp);
        if (TYPEOF(set_sexp) == REALSXP) {
            e->set_dbl = (double *)malloc((size_t)e->n_set * sizeof(double));
            for (int64_t i = 0; i < e->n_set; i++) e->set_dbl[i] = REAL(set_sexp)[i];
        } else if (TYPEOF(set_sexp) == INTSXP) {
            e->set_i64 = (int64_t *)malloc((size_t)e->n_set * sizeof(int64_t));
            for (int64_t i = 0; i < e->n_set; i++) e->set_i64[i] = (int64_t)INTEGER(set_sexp)[i];
        } else if (TYPEOF(set_sexp) == STRSXP) {
            e->set_str = (char **)malloc((size_t)e->n_set * sizeof(char *));
            for (int64_t i = 0; i < e->n_set; i++) {
                const char *s = CHAR(STRING_ELT(set_sexp, i));
                e->set_str[i] = (char *)malloc(strlen(s) + 1);
                strcpy(e->set_str[i], s);
            }
        }
        e->result_type = VEC_BOOL;
        return e;
    }

    if (strcmp(kind, "paste0") == 0) {
        /* Backward compat: old 2-arg paste0 with left/right fields */
        VecExpr *e = vec_expr_alloc(EXPR_PASTE0);
        e->left = parse_expr(list_get(lst, "left"), schema);
        e->right = parse_expr(list_get(lst, "right"), schema);
        e->result_type = VEC_STRING;
        return e;
    }
    if (strcmp(kind, "paste") == 0) {
        /* N-arg paste/paste0 with children array */
        SEXP args_sexp = list_get(lst, "args");
        int64_t nc = Rf_length(args_sexp);
        VecExpr *e = vec_expr_alloc(EXPR_PASTE);
        e->n_children = nc;
        e->children = (VecExpr **)malloc((size_t)nc * sizeof(VecExpr *));
        for (int64_t i = 0; i < nc; i++)
            e->children[i] = parse_expr(VECTOR_ELT(args_sexp, (R_xlen_t)i), schema);
        SEXP sep_sexp = list_get(lst, "sep");
        if (sep_sexp != R_NilValue && TYPEOF(sep_sexp) == STRSXP) {
            const char *sep = CHAR(STRING_ELT(sep_sexp, 0));
            e->paste_sep = (char *)malloc(strlen(sep) + 1);
            strcpy(e->paste_sep, sep);
        }
        e->result_type = VEC_STRING;
        return e;
    }
    if (strcmp(kind, "case_when") == 0) {
        SEXP cases_sexp = list_get(lst, "cases");
        SEXP def_sexp = list_get(lst, "default");
        int n_cases = Rf_length(cases_sexp);
        int has_default = (def_sexp != R_NilValue && TYPEOF(def_sexp) == VECSXP);
        VecExpr *e = vec_expr_alloc(EXPR_CASE_WHEN);
        e->n_children = n_cases * 2 + (has_default ? 1 : 0);
        e->children = (VecExpr **)malloc((size_t)e->n_children * sizeof(VecExpr *));
        VecType val_type = VEC_STRING; /* will be overridden by first value */
        for (int i = 0; i < n_cases; i++) {
            SEXP cas = VECTOR_ELT(cases_sexp, i);
            e->children[i * 2] = parse_expr(list_get(cas, "cond"), schema);
            e->children[i * 2 + 1] = parse_expr(list_get(cas, "val"), schema);
            if (i == 0) val_type = e->children[1]->result_type;
        }
        if (has_default) {
            e->children[n_cases * 2] = parse_expr(def_sexp, schema);
            if (n_cases == 0) val_type = e->children[0]->result_type;
        }
        e->result_type = val_type;
        return e;
    }
    if (strcmp(kind, "coalesce") == 0) {
        SEXP args_sexp = list_get(lst, "args");
        int64_t nc = Rf_length(args_sexp);
        VecExpr *e = vec_expr_alloc(EXPR_COALESCE);
        e->n_children = nc;
        e->children = (VecExpr **)malloc((size_t)nc * sizeof(VecExpr *));
        for (int64_t i = 0; i < nc; i++)
            e->children[i] = parse_expr(VECTOR_ELT(args_sexp, (R_xlen_t)i), schema);
        e->result_type = (nc > 0) ? e->children[0]->result_type : VEC_DOUBLE;
        return e;
    }
    if (strcmp(kind, "startsWith") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_STARTSWITH);
        const char *prefix = list_get_string(lst, "prefix");
        e->lit_str = (char *)malloc(strlen(prefix) + 1);
        strcpy(e->lit_str, prefix);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->result_type = VEC_BOOL;
        return e;
    }
    if (strcmp(kind, "endsWith") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_ENDSWITH);
        const char *suffix = list_get_string(lst, "suffix");
        e->lit_str = (char *)malloc(strlen(suffix) + 1);
        strcpy(e->lit_str, suffix);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->result_type = VEC_BOOL;
        return e;
    }
    if (strcmp(kind, "gsub") == 0 || strcmp(kind, "sub") == 0) {
        VecExpr *e = vec_expr_alloc(strcmp(kind, "gsub") == 0 ? EXPR_GSUB : EXPR_SUB);
        const char *pat = list_get_string(lst, "pattern");
        const char *rep = list_get_string(lst, "replacement");
        e->gsub_pattern = (char *)malloc(strlen(pat) + 1);
        strcpy(e->gsub_pattern, pat);
        e->gsub_replacement = (char *)malloc(strlen(rep) + 1);
        strcpy(e->gsub_replacement, rep);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        SEXP fixed_sexp = list_get(lst, "fixed");
        e->fixed = (fixed_sexp != R_NilValue) ? Rf_asLogical(fixed_sexp) : 1;
        e->result_type = VEC_STRING;
        return e;
    }
    if (strcmp(kind, "str_extract") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_STR_EXTRACT);
        const char *pat = list_get_string(lst, "pattern");
        if (!pat) vectra_error("str_extract missing 'pattern'");
        e->lit_str = (char *)malloc(strlen(pat) + 1);
        strcpy(e->lit_str, pat);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->result_type = VEC_STRING;
        return e;
    }
    if (strcmp(kind, "pmin") == 0 || strcmp(kind, "pmax") == 0) {
        VecExpr *e = vec_expr_alloc(strcmp(kind, "pmin") == 0 ? EXPR_PMIN : EXPR_PMAX);
        e->left = parse_expr(list_get(lst, "left"), schema);
        e->right = parse_expr(list_get(lst, "right"), schema);
        e->result_type = VEC_DOUBLE;
        return e;
    }

    if (strcmp(kind, "date_part") == 0) {
        const char *part = list_get_string(lst, "part");
        if (!part) vectra_error("date_part missing 'part'");
        VecExpr *e = vec_expr_alloc(EXPR_DATE_PART);
        e->date_part = part[0];
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->result_type = VEC_DOUBLE;
        return e;
    }
    if (strcmp(kind, "as_date") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_AS_DATE);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->result_type = VEC_DOUBLE;
        return e;
    }
    if (strcmp(kind, "levenshtein") == 0 || strcmp(kind, "levenshtein_norm") == 0) {
        int is_norm = (strcmp(kind, "levenshtein_norm") == 0);
        VecExpr *e = vec_expr_alloc(is_norm ? EXPR_LEVENSHTEIN_NORM : EXPR_LEVENSHTEIN);
        e->operand = parse_expr(list_get(lst, "operand"), schema);
        e->max_dist = -1;  /* default: no bound */

        /* pattern: can be a literal string or a column expression */
        SEXP pattern = list_get(lst, "pattern");
        if (pattern != R_NilValue) {
            SEXP pat_kind = list_get(pattern, "kind");
            if (pat_kind != R_NilValue && TYPEOF(pat_kind) == STRSXP &&
                strcmp(CHAR(STRING_ELT(pat_kind, 0)), "lit_string") == 0) {
                /* Literal string pattern -> store in lit_str */
                const char *s = CHAR(STRING_ELT(list_get(pattern, "value"), 0));
                e->lit_str = (char *)malloc(strlen(s) + 1);
                strcpy(e->lit_str, s);
            } else {
                /* Column or expression -> store in left */
                e->left = parse_expr(pattern, schema);
            }
        }

        /* Optional max_dist parameter */
        SEXP md = list_get(lst, "max_dist");
        if (md != R_NilValue) {
            if (TYPEOF(md) == REALSXP) e->max_dist = (int64_t)REAL(md)[0];
            else if (TYPEOF(md) == INTSXP) e->max_dist = (int64_t)INTEGER(md)[0];
        }

        e->result_type = is_norm ? VEC_DOUBLE : VEC_INT64;
        return e;
    }
    if (strcmp(kind, "dl_dist") == 0 || strcmp(kind, "dl_dist_norm") == 0) {
        int is_norm = (strcmp(kind, "dl_dist_norm") == 0);
        VecExpr *e = vec_expr_alloc(is_norm ? EXPR_DL_DIST_NORM : EXPR_DL_DIST);
        e->operand = parse_expr(list_get(lst, "operand"), schema);

        SEXP pattern = list_get(lst, "pattern");
        if (pattern != R_NilValue) {
            SEXP pat_kind = list_get(pattern, "kind");
            if (pat_kind != R_NilValue && TYPEOF(pat_kind) == STRSXP &&
                strcmp(CHAR(STRING_ELT(pat_kind, 0)), "lit_string") == 0) {
                const char *s = CHAR(STRING_ELT(list_get(pattern, "value"), 0));
                e->lit_str = (char *)malloc(strlen(s) + 1);
                strcpy(e->lit_str, s);
            } else {
                e->left = parse_expr(pattern, schema);
            }
        }

        SEXP md = list_get(lst, "max_dist");
        if (md != R_NilValue) {
            if (TYPEOF(md) == REALSXP) e->max_dist = (int64_t)REAL(md)[0];
            else if (TYPEOF(md) == INTSXP) e->max_dist = (int64_t)INTEGER(md)[0];
        }

        e->result_type = is_norm ? VEC_DOUBLE : VEC_INT64;
        return e;
    }
    if (strcmp(kind, "jaro_winkler") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_JARO_WINKLER);
        e->operand = parse_expr(list_get(lst, "operand"), schema);

        SEXP pattern = list_get(lst, "pattern");
        if (pattern != R_NilValue) {
            SEXP pat_kind = list_get(pattern, "kind");
            if (pat_kind != R_NilValue && TYPEOF(pat_kind) == STRSXP &&
                strcmp(CHAR(STRING_ELT(pat_kind, 0)), "lit_string") == 0) {
                const char *s = CHAR(STRING_ELT(list_get(pattern, "value"), 0));
                e->lit_str = (char *)malloc(strlen(s) + 1);
                strcpy(e->lit_str, s);
            } else {
                e->left = parse_expr(pattern, schema);
            }
        }

        e->result_type = VEC_DOUBLE;
        return e;
    }

    if (strcmp(kind, "resolve") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_RESOLVE);
        e->operand = parse_expr(list_get(lst, "fk"), schema);
        e->left = parse_expr(list_get(lst, "pk"), schema);
        e->right = parse_expr(list_get(lst, "val"), schema);
        e->result_type = e->right->result_type;
        return e;
    }
    if (strcmp(kind, "propagate") == 0) {
        VecExpr *e = vec_expr_alloc(EXPR_PROPAGATE);
        e->operand = parse_expr(list_get(lst, "parent_fk"), schema);
        e->left = parse_expr(list_get(lst, "pk"), schema);
        e->right = parse_expr(list_get(lst, "seed"), schema);
        e->result_type = e->right->result_type;
        return e;
    }

    vectra_error("unknown expression kind: %s", kind);
    return NULL;
}
