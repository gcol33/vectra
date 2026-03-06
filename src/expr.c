#include "expr.h"
#include "array.h"
#include "scalar_ops.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

VecExpr *vec_expr_alloc(VecExprKind kind) {
    VecExpr *e = (VecExpr *)calloc(1, sizeof(VecExpr));
    if (!e) vectra_error("alloc failed for VecExpr");
    e->kind = kind;
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
    *out = vec_array_alloc(VEC_STRING, n);
    vec_array_set_all_valid(out);
    out->buf.str.data = (char *)malloc((size_t)(slen * n));
    out->buf.str.data_len = slen * n;
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
    case EXPR_NCHAR: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING)
            vectra_error("nchar: argument must be string");
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_INT64, s->length);
        for (int64_t i = 0; i < s->length; i++) {
            if (!vec_array_is_valid(s, i)) {
                vec_array_set_null(out, i);
            } else {
                vec_array_set_valid(out, i);
                out->buf.i64[i] = s->buf.str.offsets[i + 1] -
                                   s->buf.str.offsets[i];
            }
        }
        vec_array_free(s); free(s);
        return out;
    }
    case EXPR_SUBSTR: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        VecArray *start_a = vec_expr_eval(expr->left, batch);
        VecArray *stop_a = vec_expr_eval(expr->right, batch);
        if (s->type != VEC_STRING)
            vectra_error("substr: first argument must be string");
        int64_t n = s->length;

        /* First pass: compute total output length */
        int64_t total_len = 0;
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(s, i) ||
                !vec_array_is_valid(start_a, i) ||
                !vec_array_is_valid(stop_a, i))
                continue;
            int64_t slen = s->buf.str.offsets[i + 1] - s->buf.str.offsets[i];
            int64_t st = (start_a->type == VEC_DOUBLE)
                         ? (int64_t)start_a->buf.dbl[i]
                         : start_a->buf.i64[i];
            int64_t sp = (stop_a->type == VEC_DOUBLE)
                         ? (int64_t)stop_a->buf.dbl[i]
                         : stop_a->buf.i64[i];
            st = st - 1; /* R is 1-based */
            if (st < 0) st = 0;
            if (sp > slen) sp = slen;
            if (sp > st) total_len += sp - st;
        }

        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_STRING, n);
        out->buf.str.data = (char *)malloc((size_t)(total_len > 0 ? total_len : 1));
        out->buf.str.data_len = total_len;

        int64_t offset = 0;
        for (int64_t i = 0; i < n; i++) {
            out->buf.str.offsets[i] = offset;
            if (!vec_array_is_valid(s, i) ||
                !vec_array_is_valid(start_a, i) ||
                !vec_array_is_valid(stop_a, i)) {
                vec_array_set_null(out, i);
                continue;
            }
            vec_array_set_valid(out, i);
            int64_t so = s->buf.str.offsets[i];
            int64_t slen = s->buf.str.offsets[i + 1] - so;
            int64_t st = (start_a->type == VEC_DOUBLE)
                         ? (int64_t)start_a->buf.dbl[i]
                         : start_a->buf.i64[i];
            int64_t sp = (stop_a->type == VEC_DOUBLE)
                         ? (int64_t)stop_a->buf.dbl[i]
                         : stop_a->buf.i64[i];
            st = st - 1;
            if (st < 0) st = 0;
            if (sp > slen) sp = slen;
            int64_t sub_len = (sp > st) ? sp - st : 0;
            if (sub_len > 0) {
                memcpy(out->buf.str.data + offset,
                       s->buf.str.data + so + st, (size_t)sub_len);
                offset += sub_len;
            }
        }
        out->buf.str.offsets[n] = offset;

        vec_array_free(s); free(s);
        vec_array_free(start_a); free(start_a);
        vec_array_free(stop_a); free(stop_a);
        return out;
    }
    case EXPR_GREPL: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING)
            vectra_error("grepl: argument must be string");
        const char *pattern = expr->lit_str;
        int64_t pat_len = (int64_t)strlen(pattern);
        int64_t n = s->length;

        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_BOOL, n);

        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(s, i)) {
                vec_array_set_null(out, i);
                continue;
            }
            vec_array_set_valid(out, i);
            int64_t so = s->buf.str.offsets[i];
            int64_t slen = s->buf.str.offsets[i + 1] - so;
            int found = 0;
            if (pat_len <= slen) {
                for (int64_t j = 0; j <= slen - pat_len; j++) {
                    if (memcmp(s->buf.str.data + so + j, pattern,
                               (size_t)pat_len) == 0) {
                        found = 1;
                        break;
                    }
                }
            }
            out->buf.bln[i] = (uint8_t)found;
        }

        vec_array_free(s); free(s);
        return out;
    }
    case EXPR_IF_ELSE:
    case EXPR_CAST:
        vectra_error("if_else/cast not yet implemented");
        return NULL;
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
