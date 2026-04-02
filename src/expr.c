#include "expr.h"
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
    for (int64_t i = 0; i < n; i++) out->buf.i64[i] = val;
    return out;
}

static VecArray *make_scalar_dbl(double val, int64_t n) {
    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    *out = vec_array_alloc(VEC_DOUBLE, n);
    vec_array_set_all_valid(out);
    for (int64_t i = 0; i < n; i++) out->buf.dbl[i] = val;
    return out;
}

static VecArray *make_scalar_bln(uint8_t val, int64_t n) {
    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    *out = vec_array_alloc(VEC_BOOL, n);
    vec_array_set_all_valid(out);
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

VecArray *vec_expr_eval(const VecExpr *expr, const VecBatch *batch) {
    switch (expr->kind) {
    case EXPR_COL_REF: {
        const VecArray *col = find_col(batch, expr->col_name);
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
        VecArray *d = (o->type == VEC_INT64) ? vec_coerce(o, VEC_DOUBLE) : copy_col(o);
        if (o->type == VEC_INT64) { vec_array_free(o); free(o); o = d; } else { free(d); d = o; }
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
        return vec_expr_eval_string(expr->kind, expr, batch);
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
}
