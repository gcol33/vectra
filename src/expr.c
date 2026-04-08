#include "expr.h"
#include "vec_omp.h"
#include "array.h"
#include "scalar_ops.h"
#include "coerce.h"
#include "hash.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <ctype.h>
#include <time.h>

VecExpr *vec_expr_alloc(VecExprKind kind) {
    VecExpr *e = (VecExpr *)calloc(1, sizeof(VecExpr));
    if (!e) vectra_error("alloc failed for VecExpr");
    e->kind = kind;
    e->max_dist = -1;  /* no bound by default */
    return e;
}

void vec_expr_free(VecExpr *expr) {
    if (!expr) return;
    free(expr->col_name);
    free(expr->lit_str);
    vec_expr_free(expr->left);
    vec_expr_free(expr->right);
    vec_expr_free(expr->operand);
    vec_expr_free(expr->cond);
    vec_expr_free(expr->then_expr);
    vec_expr_free(expr->else_expr);
    if (expr->set_dbl) free(expr->set_dbl);
    if (expr->set_i64) free(expr->set_i64);
    if (expr->set_str) {
        for (int64_t i = 0; i < expr->n_set; i++) free(expr->set_str[i]);
        free(expr->set_str);
    }
    free(expr->gsub_pattern);
    free(expr->gsub_replacement);
    if (expr->children) {
        for (int64_t i = 0; i < expr->n_children; i++)
            vec_expr_free(expr->children[i]);
        free(expr->children);
    }
    free(expr->paste_sep);
    free(expr);
}

/* Find column in batch by name */
static const VecArray *find_col(const VecBatch *batch, const char *name) {
    for (int i = 0; i < batch->n_cols; i++) {
        if (strcmp(batch->col_names[i], name) == 0)
            return &batch->columns[i];
    }
    vectra_error("column not found: %s", name);
    return NULL;
}

/* Create a scalar broadcast array */
static VecArray *make_scalar_i64(int64_t val, int64_t n) {
    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    *out = vec_array_alloc(VEC_INT64, n);
    vec_array_set_all_valid(out);
    #pragma omp parallel for if(n > VEC_OMP_THRESHOLD) schedule(static)
    for (int64_t i = 0; i < n; i++) out->buf.i64[i] = val;
    return out;
}

static VecArray *make_scalar_dbl(double val, int64_t n) {
    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    *out = vec_array_alloc(VEC_DOUBLE, n);
    vec_array_set_all_valid(out);
    #pragma omp parallel for if(n > VEC_OMP_THRESHOLD) schedule(static)
    for (int64_t i = 0; i < n; i++) out->buf.dbl[i] = val;
    return out;
}

static VecArray *make_scalar_bln(uint8_t val, int64_t n) {
    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    *out = vec_array_alloc(VEC_BOOL, n);
    vec_array_set_all_valid(out);
    #pragma omp parallel for if(n > VEC_OMP_THRESHOLD) schedule(static)
    for (int64_t i = 0; i < n; i++) out->buf.bln[i] = val;
    return out;
}

static VecArray *make_scalar_str(const char *val, int64_t n) {
    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    int64_t slen = (int64_t)strlen(val);
    int64_t total = slen * n;
    *out = vec_array_alloc(VEC_STRING, n);
    vec_array_set_all_valid(out);
    free(out->buf.str.data);  /* free the 1-byte from vec_array_alloc */
    out->buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
    out->buf.str.data_len = total;
    for (int64_t i = 0; i < n; i++) {
        out->buf.str.offsets[i] = i * slen;
        memcpy(out->buf.str.data + i * slen, val, (size_t)slen);
    }
    out->buf.str.offsets[n] = slen * n;
    return out;
}

static VecArray *make_na_array(VecType type, int64_t n) {
    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    *out = vec_array_alloc(type, n);
    /* validity already zeroed = all NA */
    return out;
}

/* Copy a column (deep copy) */
static VecArray *copy_col(const VecArray *src) {
    return vec_coerce(src, src->type);
}

/* Copy a single non-string scalar value from src to dst at row i.
   Assumes the caller has already validated that src is valid at i.
   For VEC_STRING, the caller must handle separately (two-pass buffer). */
static void copy_scalar_value(VecArray *dst, const VecArray *src,
                              int64_t i, VecType type) {
    switch (type) {
    case VEC_INT64:  dst->buf.i64[i] = src->buf.i64[i]; break;
    case VEC_INT32:  dst->buf.i32[i] = src->buf.i32[i]; break;
    case VEC_INT16:  dst->buf.i16[i] = src->buf.i16[i]; break;
    case VEC_INT8:   dst->buf.i8[i]  = src->buf.i8[i];  break;
    case VEC_DOUBLE: dst->buf.dbl[i] = src->buf.dbl[i]; break;
    case VEC_BOOL:   dst->buf.bln[i] = src->buf.bln[i]; break;
    case VEC_STRING: break; /* handled by string two-pass path */
    }
}

VecArray *vec_expr_eval(const VecExpr *expr, const VecBatch *batch) {
    switch (expr->kind) {
    case EXPR_COL_REF: {
        const VecArray *col = find_col(batch, expr->col_name);
        /* Widen narrow ints to int64 for expression evaluation */
        if (col->type == VEC_INT8 || col->type == VEC_INT16 || col->type == VEC_INT32)
            return vec_coerce(col, VEC_INT64);
        return copy_col(col);
    }
    case EXPR_LIT_INT64:
        return make_scalar_i64(expr->lit_i64, batch->n_rows);
    case EXPR_LIT_DOUBLE:
        return make_scalar_dbl(expr->lit_dbl, batch->n_rows);
    case EXPR_LIT_BOOL:
        return make_scalar_bln(expr->lit_bln, batch->n_rows);
    case EXPR_LIT_STRING:
        return make_scalar_str(expr->lit_str, batch->n_rows);
    case EXPR_LIT_NA:
        return make_na_array(expr->result_type, batch->n_rows);
    case EXPR_ARITH: {
        VecArray *l = vec_expr_eval(expr->left, batch);
        VecArray *r = vec_expr_eval(expr->right, batch);
        VecArray *res = vec_arith(l, r, expr->op);
        vec_array_free(l); free(l);
        vec_array_free(r); free(r);
        return res;
    }
    case EXPR_CMP: {
        VecArray *l = vec_expr_eval(expr->left, batch);
        VecArray *r = vec_expr_eval(expr->right, batch);
        VecArray *res = vec_cmp(l, r, expr->op, expr->op2);
        vec_array_free(l); free(l);
        vec_array_free(r); free(r);
        return res;
    }
    case EXPR_BOOL: {
        if (expr->op == '!') {
            VecArray *o = vec_expr_eval(expr->operand, batch);
            VecArray *res = vec_bool_not(o);
            vec_array_free(o); free(o);
            return res;
        }
        VecArray *l = vec_expr_eval(expr->left, batch);
        VecArray *r = vec_expr_eval(expr->right, batch);
        VecArray *res = vec_bool_binary(l, r, expr->op);
        vec_array_free(l); free(l);
        vec_array_free(r); free(r);
        return res;
    }
    case EXPR_IS_NA: {
        VecArray *o = vec_expr_eval(expr->operand, batch);
        VecArray *res = (VecArray *)malloc(sizeof(VecArray));
        *res = vec_array_alloc(VEC_BOOL, o->length);
        vec_array_set_all_valid(res);
        for (int64_t i = 0; i < o->length; i++)
            res->buf.bln[i] = (uint8_t)(!vec_array_is_valid(o, i));
        vec_array_free(o); free(o);
        return res;
    }
    case EXPR_NEGATE: {
        VecArray *o = vec_expr_eval(expr->operand, batch);
        VecArray *res = vec_negate(o);
        vec_array_free(o); free(o);
        return res;
    }
    case EXPR_MATH_UNARY: {
        VecArray *o = vec_expr_eval(expr->operand, batch);
        /* Coerce to double if int64 */
        VecArray *d = vec_type_is_int(o->type) ? vec_coerce(o, VEC_DOUBLE) : copy_col(o);
        if (vec_type_is_int(o->type)) { vec_array_free(o); free(o); o = d; } else { free(d); d = o; }
        /* d is now VEC_DOUBLE */
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_DOUBLE, d->length);
        for (int64_t i = 0; i < d->length; i++) {
            if (!vec_array_is_valid(d, i)) { vec_array_set_null(out, i); continue; }
            vec_array_set_valid(out, i);
            double v = d->buf.dbl[i];
            switch (expr->math_fn) {
            case 'a': out->buf.dbl[i] = fabs(v); break;
            case 's': out->buf.dbl[i] = sqrt(v); break;
            case 'l': out->buf.dbl[i] = log(v); break;
            case 'e': out->buf.dbl[i] = exp(v); break;
            case 'f': out->buf.dbl[i] = floor(v); break;
            case 'c': out->buf.dbl[i] = ceil(v); break;
            case 'r': out->buf.dbl[i] = round(v); break;
            case '2': out->buf.dbl[i] = log2(v); break;
            case 't': out->buf.dbl[i] = log10(v); break;
            case 'g': out->buf.dbl[i] = (v > 0) ? 1.0 : (v < 0) ? -1.0 : 0.0; break;
            case 'u': out->buf.dbl[i] = trunc(v); break;
            default: vectra_error("unknown math function: %c", expr->math_fn);
            }
        }
        vec_array_free(d); free(d);
        return out;
    }
    case EXPR_CAST: {
        VecArray *o = vec_expr_eval(expr->operand, batch);
        VecArray *res = vec_coerce(o, expr->cast_to);
        vec_array_free(o); free(o);
        return res;
    }
    /* String operations — dispatched to expr_string.c */
    case EXPR_NCHAR:
    case EXPR_SUBSTR:
    case EXPR_GREPL:
    case EXPR_TOLOWER:
    case EXPR_TOUPPER:
    case EXPR_TRIMWS:
    case EXPR_IN:
    case EXPR_PASTE0:
    case EXPR_STARTSWITH:
    case EXPR_ENDSWITH:
    case EXPR_GSUB:
    case EXPR_SUB:
    case EXPR_LEVENSHTEIN:
    case EXPR_LEVENSHTEIN_NORM:
    case EXPR_DL_DIST:
    case EXPR_DL_DIST_NORM:
    case EXPR_JARO_WINKLER:
    case EXPR_PASTE:
    case EXPR_STR_EXTRACT:
        return vec_expr_eval_string(expr->kind, expr, batch);
    /* case_when and coalesce — evaluated here directly */
    case EXPR_CASE_WHEN: {
        int64_t n = batch->n_rows;
        int n_pairs = (int)(expr->n_children / 2);
        int has_default = (expr->n_children % 2 == 1);
        /* Evaluate all conditions and values eagerly */
        VecArray **conds = (VecArray **)malloc((size_t)n_pairs * sizeof(VecArray *));
        VecArray **vals  = (VecArray **)malloc((size_t)n_pairs * sizeof(VecArray *));
        for (int p = 0; p < n_pairs; p++) {
            conds[p] = vec_expr_eval(expr->children[p * 2], batch);
            vals[p]  = vec_expr_eval(expr->children[p * 2 + 1], batch);
        }
        VecArray *def_val = has_default
            ? vec_expr_eval(expr->children[expr->n_children - 1], batch) : NULL;
        VecType out_type = expr->result_type;
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(out_type, n);
        for (int64_t i = 0; i < n; i++) {
            /* Find the first matching condition for this row */
            const VecArray *src = NULL;
            for (int p = 0; p < n_pairs; p++) {
                if (vec_array_is_valid(conds[p], i) && conds[p]->buf.bln[i]) {
                    src = vals[p];
                    break;
                }
            }
            if (!src) src = def_val;

            if (src && vec_array_is_valid(src, i)) {
                vec_array_set_valid(out, i);
                copy_scalar_value(out, src, i, out_type);
            } else {
                vec_array_set_null(out, i);
            }
        }
        /* String output: requires two-pass to build unified buffer */
        if (out_type == VEC_STRING) {
            /* Pass 1: compute total string length */
            int64_t total = 0;
            for (int64_t i = 0; i < n; i++) {
                const VecArray *src = NULL;
                for (int p = 0; p < n_pairs; p++) {
                    if (vec_array_is_valid(conds[p], i) && conds[p]->buf.bln[i]) {
                        src = vals[p]; break;
                    }
                }
                if (!src) src = def_val;
                if (src && vec_array_is_valid(src, i))
                    total += src->buf.str.offsets[i+1] - src->buf.str.offsets[i];
            }
            free(out->buf.str.data);
            out->buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
            out->buf.str.data_len = total;
            /* Pass 2: copy string data */
            int64_t off = 0;
            for (int64_t i = 0; i < n; i++) {
                out->buf.str.offsets[i] = off;
                const VecArray *src = NULL;
                for (int p = 0; p < n_pairs; p++) {
                    if (vec_array_is_valid(conds[p], i) && conds[p]->buf.bln[i]) {
                        src = vals[p]; break;
                    }
                }
                if (!src) src = def_val;
                if (src && vec_array_is_valid(src, i)) {
                    vec_array_set_valid(out, i);
                    int64_t s = src->buf.str.offsets[i];
                    int64_t l = src->buf.str.offsets[i+1] - s;
                    if (l > 0) memcpy(out->buf.str.data + off, src->buf.str.data + s, (size_t)l);
                    off += l;
                } else {
                    vec_array_set_null(out, i);
                }
            }
            out->buf.str.offsets[n] = off;
        }
        for (int p = 0; p < n_pairs; p++) {
            vec_array_free(conds[p]); free(conds[p]);
            vec_array_free(vals[p]);  free(vals[p]);
        }
        free(conds); free(vals);
        if (def_val) { vec_array_free(def_val); free(def_val); }
        return out;
    }
    case EXPR_COALESCE: {
        int64_t n = batch->n_rows;
        int64_t nc = expr->n_children;
        VecType out_type = expr->result_type;
        /* Evaluate all children */
        VecArray **args = (VecArray **)malloc((size_t)nc * sizeof(VecArray *));
        for (int64_t c = 0; c < nc; c++)
            args[c] = vec_expr_eval(expr->children[c], batch);
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(out_type, n);
        if (out_type == VEC_STRING) {
            /* Two-pass for strings */
            int64_t total = 0;
            for (int64_t i = 0; i < n; i++) {
                int found = 0;
                for (int64_t c = 0; c < nc; c++) {
                    if (vec_array_is_valid(args[c], i)) {
                        total += args[c]->buf.str.offsets[i+1] - args[c]->buf.str.offsets[i];
                        found = 1; break;
                    }
                }
                (void)found;
            }
            free(out->buf.str.data);
            out->buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
            out->buf.str.data_len = total;
            int64_t off = 0;
            for (int64_t i = 0; i < n; i++) {
                out->buf.str.offsets[i] = off;
                int found = 0;
                for (int64_t c = 0; c < nc; c++) {
                    if (vec_array_is_valid(args[c], i)) {
                        vec_array_set_valid(out, i);
                        int64_t s = args[c]->buf.str.offsets[i];
                        int64_t l = args[c]->buf.str.offsets[i+1] - s;
                        if (l > 0) memcpy(out->buf.str.data + off, args[c]->buf.str.data + s, (size_t)l);
                        off += l;
                        found = 1; break;
                    }
                }
                if (!found) vec_array_set_null(out, i);
            }
            out->buf.str.offsets[n] = off;
        } else {
            for (int64_t i = 0; i < n; i++) {
                int found = 0;
                for (int64_t c = 0; c < nc; c++) {
                    if (vec_array_is_valid(args[c], i)) {
                        vec_array_set_valid(out, i);
                        switch (out_type) {
                        case VEC_INT64:  out->buf.i64[i] = args[c]->buf.i64[i]; break;
                        case VEC_INT8:   out->buf.i64[i] = (int64_t)args[c]->buf.i8[i]; break;
                        case VEC_INT16:  out->buf.i64[i] = (int64_t)args[c]->buf.i16[i]; break;
                        case VEC_INT32:  out->buf.i64[i] = (int64_t)args[c]->buf.i32[i]; break;
                        case VEC_DOUBLE: out->buf.dbl[i] = args[c]->buf.dbl[i]; break;
                        case VEC_BOOL:   out->buf.bln[i] = args[c]->buf.bln[i]; break;
                        case VEC_STRING: break;
                        }
                        found = 1; break;
                    }
                }
                if (!found) vec_array_set_null(out, i);
            }
        }
        for (int64_t c = 0; c < nc; c++) { vec_array_free(args[c]); free(args[c]); }
        free(args);
        return out;
    }
    /* Datetime / extended operations — dispatched to expr_datetime.c */
    case EXPR_PMIN:
    case EXPR_PMAX:
    case EXPR_DATE_PART:
    case EXPR_AS_DATE:
    case EXPR_IF_ELSE:
    case EXPR_RESOLVE:
    case EXPR_PROPAGATE:
        return vec_expr_eval_extended(expr->kind, expr, batch);
    }
    vectra_error("unknown expr kind: %d", expr->kind);
    return NULL;
}

void vec_expr_collect_colrefs(const VecExpr *expr, char **col_names,
                              int n_cols, uint8_t *needed) {
    if (!expr) return;
    if (expr->kind == EXPR_COL_REF) {
        for (int i = 0; i < n_cols; i++) {
            if (strcmp(col_names[i], expr->col_name) == 0) {
                needed[i] = 1;
                break;
            }
        }
        return;
    }
    vec_expr_collect_colrefs(expr->left, col_names, n_cols, needed);
    vec_expr_collect_colrefs(expr->right, col_names, n_cols, needed);
    vec_expr_collect_colrefs(expr->operand, col_names, n_cols, needed);
    vec_expr_collect_colrefs(expr->cond, col_names, n_cols, needed);
    vec_expr_collect_colrefs(expr->then_expr, col_names, n_cols, needed);
    vec_expr_collect_colrefs(expr->else_expr, col_names, n_cols, needed);
    for (int64_t i = 0; i < expr->n_children; i++)
        vec_expr_collect_colrefs(expr->children[i], col_names, n_cols, needed);
}
