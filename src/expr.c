#include "expr.h"
#include "array.h"
#include "scalar_ops.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <ctype.h>

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
    if (expr->set_dbl) free(expr->set_dbl);
    if (expr->set_i64) free(expr->set_i64);
    if (expr->set_str) {
        for (int64_t i = 0; i < expr->n_set; i++) free(expr->set_str[i]);
        free(expr->set_str);
    }
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
            default: vectra_error("unknown math function: %c", expr->math_fn);
            }
        }
        vec_array_free(d); free(d);
        return out;
    }
    case EXPR_IF_ELSE: {
        VecArray *cond = vec_expr_eval(expr->cond, batch);
        VecArray *then_v = vec_expr_eval(expr->then_expr, batch);
        VecArray *else_v = vec_expr_eval(expr->else_expr, batch);
        /* Coerce then/else to common type */
        VecType common = (then_v->type == VEC_DOUBLE || else_v->type == VEC_DOUBLE) ? VEC_DOUBLE :
                         (then_v->type == VEC_STRING || else_v->type == VEC_STRING) ? VEC_STRING : then_v->type;
        if (then_v->type != common) { VecArray *t2 = vec_coerce(then_v, common); vec_array_free(then_v); free(then_v); then_v = t2; }
        if (else_v->type != common) { VecArray *e2 = vec_coerce(else_v, common); vec_array_free(else_v); free(else_v); else_v = e2; }
        int64_t n = cond->length;
        VecArray *out;
        if (common == VEC_STRING) {
            /* String if_else: compute total length first */
            int64_t total = 0;
            for (int64_t i = 0; i < n; i++) {
                if (!vec_array_is_valid(cond, i)) continue;
                const VecArray *src = cond->buf.bln[i] ? then_v : else_v;
                if (vec_array_is_valid(src, i))
                    total += src->buf.str.offsets[i + 1] - src->buf.str.offsets[i];
            }
            out = (VecArray *)malloc(sizeof(VecArray));
            *out = vec_array_alloc(VEC_STRING, n);
            free(out->buf.str.data);
            out->buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
            out->buf.str.data_len = total;
            int64_t off = 0;
            for (int64_t i = 0; i < n; i++) {
                out->buf.str.offsets[i] = off;
                if (!vec_array_is_valid(cond, i)) { vec_array_set_null(out, i); continue; }
                const VecArray *src = cond->buf.bln[i] ? then_v : else_v;
                if (!vec_array_is_valid(src, i)) { vec_array_set_null(out, i); continue; }
                vec_array_set_valid(out, i);
                int64_t s = src->buf.str.offsets[i], e = src->buf.str.offsets[i + 1];
                int64_t slen = e - s;
                if (slen > 0) memcpy(out->buf.str.data + off, src->buf.str.data + s, (size_t)slen);
                off += slen;
            }
            out->buf.str.offsets[n] = off;
        } else {
            out = (VecArray *)malloc(sizeof(VecArray));
            *out = vec_array_alloc(common, n);
            for (int64_t i = 0; i < n; i++) {
                if (!vec_array_is_valid(cond, i)) { vec_array_set_null(out, i); continue; }
                const VecArray *src = cond->buf.bln[i] ? then_v : else_v;
                if (!vec_array_is_valid(src, i)) { vec_array_set_null(out, i); continue; }
                vec_array_set_valid(out, i);
                switch (common) {
                case VEC_DOUBLE: out->buf.dbl[i] = src->buf.dbl[i]; break;
                case VEC_INT64:  out->buf.i64[i] = src->buf.i64[i]; break;
                case VEC_BOOL:   out->buf.bln[i] = src->buf.bln[i]; break;
                default: break;
                }
            }
        }
        vec_array_free(cond); free(cond);
        vec_array_free(then_v); free(then_v);
        vec_array_free(else_v); free(else_v);
        return out;
    }
    case EXPR_CAST: {
        VecArray *o = vec_expr_eval(expr->operand, batch);
        VecArray *res = vec_coerce(o, expr->cast_to);
        vec_array_free(o); free(o);
        return res;
    }
    case EXPR_TOLOWER: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING) vectra_error("tolower: argument must be string");
        VecArray *out = copy_col(s);
        for (int64_t i = 0; i < out->buf.str.data_len; i++)
            out->buf.str.data[i] = (char)tolower((unsigned char)out->buf.str.data[i]);
        vec_array_free(s); free(s);
        return out;
    }
    case EXPR_TOUPPER: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING) vectra_error("toupper: argument must be string");
        VecArray *out = copy_col(s);
        for (int64_t i = 0; i < out->buf.str.data_len; i++)
            out->buf.str.data[i] = (char)toupper((unsigned char)out->buf.str.data[i]);
        vec_array_free(s); free(s);
        return out;
    }
    case EXPR_TRIMWS: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING) vectra_error("trimws: argument must be string");
        int64_t n = s->length;
        /* First pass: compute trimmed lengths */
        int64_t total = 0;
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(s, i)) continue;
            int64_t so = s->buf.str.offsets[i], eo = s->buf.str.offsets[i + 1];
            const char *p = s->buf.str.data + so;
            int64_t len = eo - so;
            int64_t start = 0, end = len;
            while (start < end && (p[start] == ' ' || p[start] == '\t' || p[start] == '\n' || p[start] == '\r')) start++;
            while (end > start && (p[end - 1] == ' ' || p[end - 1] == '\t' || p[end - 1] == '\n' || p[end - 1] == '\r')) end--;
            total += end - start;
        }
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_STRING, n);
        free(out->buf.str.data);
        out->buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
        out->buf.str.data_len = total;
        int64_t off = 0;
        for (int64_t i = 0; i < n; i++) {
            out->buf.str.offsets[i] = off;
            if (!vec_array_is_valid(s, i)) { vec_array_set_null(out, i); continue; }
            vec_array_set_valid(out, i);
            int64_t so = s->buf.str.offsets[i], eo = s->buf.str.offsets[i + 1];
            const char *p = s->buf.str.data + so;
            int64_t len = eo - so;
            int64_t start = 0, end = len;
            while (start < end && (p[start] == ' ' || p[start] == '\t' || p[start] == '\n' || p[start] == '\r')) start++;
            while (end > start && (p[end - 1] == ' ' || p[end - 1] == '\t' || p[end - 1] == '\n' || p[end - 1] == '\r')) end--;
            int64_t tlen = end - start;
            if (tlen > 0) memcpy(out->buf.str.data + off, p + start, (size_t)tlen);
            off += tlen;
        }
        out->buf.str.offsets[n] = off;
        vec_array_free(s); free(s);
        return out;
    }
    case EXPR_IN: {
        VecArray *o = vec_expr_eval(expr->operand, batch);
        int64_t n = o->length;
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_BOOL, n);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(o, i)) { vec_array_set_null(out, i); continue; }
            vec_array_set_valid(out, i);
            int found = 0;
            if (o->type == VEC_DOUBLE) {
                double v = o->buf.dbl[i];
                for (int64_t j = 0; j < expr->n_set; j++) {
                    if (v == expr->set_dbl[j]) { found = 1; break; }
                }
            } else if (o->type == VEC_INT64) {
                int64_t v = o->buf.i64[i];
                for (int64_t j = 0; j < expr->n_set; j++) {
                    if (v == expr->set_i64[j]) { found = 1; break; }
                }
            } else if (o->type == VEC_STRING) {
                int64_t so = o->buf.str.offsets[i], eo = o->buf.str.offsets[i + 1];
                int64_t slen = eo - so;
                for (int64_t j = 0; j < expr->n_set; j++) {
                    int64_t clen = (int64_t)strlen(expr->set_str[j]);
                    if (slen == clen && memcmp(o->buf.str.data + so, expr->set_str[j], (size_t)slen) == 0) {
                        found = 1; break;
                    }
                }
            }
            out->buf.bln[i] = (uint8_t)found;
        }
        vec_array_free(o); free(o);
        return out;
    }
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
