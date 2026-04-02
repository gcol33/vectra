#include "coerce.h"
#include "array.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

VecType vec_common_type(VecType a, VecType b) {
    if (a == b) return a;
    /* bool < int64 < double */
    if (a == VEC_STRING || b == VEC_STRING)
        vectra_error("cannot coerce string in arithmetic/comparison");
    /* Return the "wider" type */
    if (a == VEC_DOUBLE || b == VEC_DOUBLE) return VEC_DOUBLE;
    if (a == VEC_INT64 || b == VEC_INT64) return VEC_INT64;
    return VEC_BOOL;
}

VecArray *vec_coerce(const VecArray *arr, VecType target) {
    if (arr->type == target) {
        /* Copy */
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        if (!out) vectra_error("alloc failed");
        *out = vec_array_alloc(target, arr->length);
        /* Copy validity */
        memcpy(out->validity, arr->validity, (size_t)vec_validity_bytes(arr->length));
        switch (target) {
        case VEC_INT64:
            memcpy(out->buf.i64, arr->buf.i64, (size_t)arr->length * sizeof(int64_t));
            break;
        case VEC_DOUBLE:
            memcpy(out->buf.dbl, arr->buf.dbl, (size_t)arr->length * sizeof(double));
            break;
        case VEC_BOOL:
            memcpy(out->buf.bln, arr->buf.bln, (size_t)arr->length);
            break;
        case VEC_STRING:
            memcpy(out->buf.str.offsets, arr->buf.str.offsets,
                   (size_t)(arr->length + 1) * sizeof(int64_t));
            out->buf.str.data_len = arr->buf.str.data_len;
            free(out->buf.str.data); /* free the 1-byte from alloc */
            out->buf.str.data = (char *)malloc((size_t)(arr->buf.str.data_len > 0 ? arr->buf.str.data_len : 1));
            if (!out->buf.str.data)
                vectra_error("alloc failed for string copy");
            if (arr->buf.str.data_len > 0)
                memcpy(out->buf.str.data, arr->buf.str.data, (size_t)arr->buf.str.data_len);
            break;
        }
        return out;
    }

    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    if (!out) vectra_error("alloc failed");
    *out = vec_array_alloc(target, arr->length);
    memcpy(out->validity, arr->validity, (size_t)vec_validity_bytes(arr->length));

    if (arr->type == VEC_BOOL && target == VEC_INT64) {
        for (int64_t i = 0; i < arr->length; i++)
            out->buf.i64[i] = (int64_t)arr->buf.bln[i];
    } else if (arr->type == VEC_BOOL && target == VEC_DOUBLE) {
        for (int64_t i = 0; i < arr->length; i++)
            out->buf.dbl[i] = (double)arr->buf.bln[i];
    } else if (arr->type == VEC_INT64 && target == VEC_DOUBLE) {
        for (int64_t i = 0; i < arr->length; i++)
            out->buf.dbl[i] = (double)arr->buf.i64[i];
    } else if (target == VEC_STRING) {
        /* Coerce numeric/bool to string: only valid values get converted,
           NAs stay as NAs. For ifelse(cond, string_col, NA) where NA branch
           needs to become VEC_STRING. */
        char numbuf[64];
        /* Re-allocate as string type */
        vec_array_free(out);
        free(out);
        out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_STRING, arr->length);
        memcpy(out->validity, arr->validity, (size_t)vec_validity_bytes(arr->length));
        /* For each valid value, convert to string representation */
        int64_t total_len = 0;
        for (int64_t i = 0; i < arr->length; i++) {
            if (!vec_array_is_valid(arr, i)) continue;
            int len = 0;
            switch (arr->type) {
            case VEC_BOOL:   len = snprintf(numbuf, sizeof(numbuf), "%s", arr->buf.bln[i] ? "TRUE" : "FALSE"); break;
            case VEC_INT64:  len = snprintf(numbuf, sizeof(numbuf), "%lld", (long long)arr->buf.i64[i]); break;
            case VEC_DOUBLE: len = snprintf(numbuf, sizeof(numbuf), "%g", arr->buf.dbl[i]); break;
            default: break;
            }
            total_len += len;
        }
        free(out->buf.str.data);
        out->buf.str.data = (char *)malloc((size_t)(total_len > 0 ? total_len : 1));
        out->buf.str.data_len = total_len;
        int64_t off = 0;
        for (int64_t i = 0; i < arr->length; i++) {
            out->buf.str.offsets[i] = off;
            if (!vec_array_is_valid(arr, i)) continue;
            int len = 0;
            switch (arr->type) {
            case VEC_BOOL:   len = snprintf(numbuf, sizeof(numbuf), "%s", arr->buf.bln[i] ? "TRUE" : "FALSE"); break;
            case VEC_INT64:  len = snprintf(numbuf, sizeof(numbuf), "%lld", (long long)arr->buf.i64[i]); break;
            case VEC_DOUBLE: len = snprintf(numbuf, sizeof(numbuf), "%g", arr->buf.dbl[i]); break;
            default: break;
            }
            memcpy(out->buf.str.data + off, numbuf, (size_t)len);
            off += len;
        }
        out->buf.str.offsets[arr->length] = off;
    } else {
        vec_array_free(out);
        free(out);
        vectra_error("unsupported coercion: %d -> %d", arr->type, target);
    }

    return out;
}
