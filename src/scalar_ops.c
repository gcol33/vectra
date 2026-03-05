#include "scalar_ops.h"
#include "coerce.h"
#include "array.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* --- Arithmetic --- */

VecArray *vec_arith(const VecArray *left, const VecArray *right, char op) {
    if (left->length != right->length)
        vectra_error("arith: mismatched lengths");

    VecType common = vec_common_type(left->type, right->type);
    if (common == VEC_BOOL) common = VEC_INT64; /* promote bool for arith */

    VecArray *cl = vec_coerce(left, common);
    VecArray *cr = vec_coerce(right, common);

    int64_t n = left->length;
    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    if (!out) vectra_error("alloc failed");
    *out = vec_array_alloc(common, n);

    for (int64_t i = 0; i < n; i++) {
        int valid = vec_array_is_valid(cl, i) && vec_array_is_valid(cr, i);
        if (!valid) {
            vec_array_set_null(out, i);
            continue;
        }
        vec_array_set_valid(out, i);

        if (common == VEC_INT64) {
            int64_t a = cl->buf.i64[i], b = cr->buf.i64[i];
            switch (op) {
            case '+': out->buf.i64[i] = a + b; break;
            case '-': out->buf.i64[i] = a - b; break;
            case '*': out->buf.i64[i] = a * b; break;
            case '/':
                if (b == 0) { vec_array_set_null(out, i); }
                else out->buf.i64[i] = a / b;
                break;
            case '%':
                if (b == 0) { vec_array_set_null(out, i); }
                else out->buf.i64[i] = a % b;
                break;
            default: vectra_error("unknown arith op: %c", op);
            }
        } else {
            double a = cl->buf.dbl[i], b = cr->buf.dbl[i];
            switch (op) {
            case '+': out->buf.dbl[i] = a + b; break;
            case '-': out->buf.dbl[i] = a - b; break;
            case '*': out->buf.dbl[i] = a * b; break;
            case '/': out->buf.dbl[i] = a / b; break;
            case '%': out->buf.dbl[i] = fmod(a, b); break;
            default: vectra_error("unknown arith op: %c", op);
            }
        }
    }

    vec_array_free(cl); free(cl);
    vec_array_free(cr); free(cr);
    return out;
}

/* --- Comparison --- */

VecArray *vec_cmp(const VecArray *left, const VecArray *right, char op, char op2) {
    if (left->length != right->length)
        vectra_error("cmp: mismatched lengths");

    int64_t n = left->length;
    VecType common;

    /* String comparison */
    if (left->type == VEC_STRING && right->type == VEC_STRING) {
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        if (!out) vectra_error("alloc failed");
        *out = vec_array_alloc(VEC_BOOL, n);

        for (int64_t i = 0; i < n; i++) {
            int valid = vec_array_is_valid(left, i) && vec_array_is_valid(right, i);
            if (!valid) { vec_array_set_null(out, i); continue; }
            vec_array_set_valid(out, i);

            int64_t ls = left->buf.str.offsets[i];
            int64_t le = left->buf.str.offsets[i + 1];
            int64_t rs = right->buf.str.offsets[i];
            int64_t re = right->buf.str.offsets[i + 1];
            int64_t llen = le - ls, rlen = re - rs;
            int64_t minlen = llen < rlen ? llen : rlen;
            int cmp = memcmp(left->buf.str.data + ls, right->buf.str.data + rs,
                             (size_t)minlen);
            if (cmp == 0) cmp = (llen > rlen) - (llen < rlen);

            int result = 0;
            if (op == '=' && op2 == '=') result = (cmp == 0);
            else if (op == '!' && op2 == '=') result = (cmp != 0);
            else if (op == '<' && op2 == ' ') result = (cmp < 0);
            else if (op == '<' && op2 == '=') result = (cmp <= 0);
            else if (op == '>' && op2 == ' ') result = (cmp > 0);
            else if (op == '>' && op2 == '=') result = (cmp >= 0);
            out->buf.bln[i] = (uint8_t)result;
        }
        return out;
    }

    common = vec_common_type(left->type, right->type);
    if (common == VEC_BOOL) common = VEC_INT64;

    VecArray *cl = vec_coerce(left, common);
    VecArray *cr = vec_coerce(right, common);

    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    if (!out) vectra_error("alloc failed");
    *out = vec_array_alloc(VEC_BOOL, n);

    for (int64_t i = 0; i < n; i++) {
        int valid = vec_array_is_valid(cl, i) && vec_array_is_valid(cr, i);
        if (!valid) { vec_array_set_null(out, i); continue; }
        vec_array_set_valid(out, i);

        int cmp;
        if (common == VEC_INT64) {
            int64_t a = cl->buf.i64[i], b = cr->buf.i64[i];
            cmp = (a > b) - (a < b);
        } else {
            double a = cl->buf.dbl[i], b = cr->buf.dbl[i];
            cmp = (a > b) - (a < b);
        }

        int result = 0;
        if (op == '=' && op2 == '=') result = (cmp == 0);
        else if (op == '!' && op2 == '=') result = (cmp != 0);
        else if (op == '<' && op2 == ' ') result = (cmp < 0);
        else if (op == '<' && op2 == '=') result = (cmp <= 0);
        else if (op == '>' && op2 == ' ') result = (cmp > 0);
        else if (op == '>' && op2 == '=') result = (cmp >= 0);
        out->buf.bln[i] = (uint8_t)result;
    }

    vec_array_free(cl); free(cl);
    vec_array_free(cr); free(cr);
    return out;
}

/* --- Boolean --- */

VecArray *vec_bool_binary(const VecArray *left, const VecArray *right, char op) {
    if (left->length != right->length)
        vectra_error("bool: mismatched lengths");
    if (left->type != VEC_BOOL || right->type != VEC_BOOL)
        vectra_error("bool: operands must be boolean");

    int64_t n = left->length;
    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    if (!out) vectra_error("alloc failed");
    *out = vec_array_alloc(VEC_BOOL, n);

    for (int64_t i = 0; i < n; i++) {
        int lv = vec_array_is_valid(left, i);
        int rv = vec_array_is_valid(right, i);
        int lb = lv ? left->buf.bln[i] : -1;
        int rb = rv ? right->buf.bln[i] : -1;

        /* R's 3-valued logic:
           TRUE & NA = NA, FALSE & NA = FALSE
           TRUE | NA = TRUE, FALSE | NA = NA */
        if (op == '&') {
            if (lv && !lb) { vec_array_set_valid(out, i); out->buf.bln[i] = 0; }
            else if (rv && !rb) { vec_array_set_valid(out, i); out->buf.bln[i] = 0; }
            else if (lv && rv) { vec_array_set_valid(out, i); out->buf.bln[i] = (uint8_t)(lb && rb); }
            else { vec_array_set_null(out, i); }
        } else { /* '|' */
            if (lv && lb) { vec_array_set_valid(out, i); out->buf.bln[i] = 1; }
            else if (rv && rb) { vec_array_set_valid(out, i); out->buf.bln[i] = 1; }
            else if (lv && rv) { vec_array_set_valid(out, i); out->buf.bln[i] = (uint8_t)(lb || rb); }
            else { vec_array_set_null(out, i); }
        }
    }

    return out;
}

VecArray *vec_bool_not(const VecArray *arr) {
    if (arr->type != VEC_BOOL)
        vectra_error("bool not: operand must be boolean");

    int64_t n = arr->length;
    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    if (!out) vectra_error("alloc failed");
    *out = vec_array_alloc(VEC_BOOL, n);
    memcpy(out->validity, arr->validity, (size_t)vec_validity_bytes(n));

    for (int64_t i = 0; i < n; i++) {
        if (vec_array_is_valid(arr, i))
            out->buf.bln[i] = (uint8_t)(!arr->buf.bln[i]);
    }

    return out;
}

VecArray *vec_negate(const VecArray *arr) {
    int64_t n = arr->length;
    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    if (!out) vectra_error("alloc failed");
    *out = vec_array_alloc(arr->type, n);
    memcpy(out->validity, arr->validity, (size_t)vec_validity_bytes(n));

    switch (arr->type) {
    case VEC_INT64:
        for (int64_t i = 0; i < n; i++)
            out->buf.i64[i] = -arr->buf.i64[i];
        break;
    case VEC_DOUBLE:
        for (int64_t i = 0; i < n; i++)
            out->buf.dbl[i] = -arr->buf.dbl[i];
        break;
    default:
        vec_array_free(out); free(out);
        vectra_error("negate: unsupported type %d", arr->type);
    }
    return out;
}
