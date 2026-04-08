#include "coerce.h"
#include "array.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

VecType vec_common_type(VecType a, VecType b) {
    if (a == b) return a;
    /* bool < int8 < int16 < int32 < int64 < double */
    if (a == VEC_STRING || b == VEC_STRING)
        vectra_error("cannot coerce string in arithmetic/comparison");
    /* Return the "wider" type */
    if (a == VEC_DOUBLE || b == VEC_DOUBLE) return VEC_DOUBLE;
    if (a == VEC_INT64 || b == VEC_INT64) return VEC_INT64;
    if (a == VEC_INT32 || b == VEC_INT32) return VEC_INT32;
    if (a == VEC_INT16 || b == VEC_INT16) return VEC_INT16;
    if (a == VEC_INT8  || b == VEC_INT8)  return VEC_INT8;
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
        case VEC_INT32:
            memcpy(out->buf.i32, arr->buf.i32, (size_t)arr->length * sizeof(int32_t));
            break;
        case VEC_INT16:
            memcpy(out->buf.i16, arr->buf.i16, (size_t)arr->length * sizeof(int16_t));
            break;
        case VEC_INT8:
            memcpy(out->buf.i8, arr->buf.i8, (size_t)arr->length);
            break;
        case VEC_DOUBLE:
            memcpy(out->buf.dbl, arr->buf.dbl, (size_t)arr->length * sizeof(double));
            break;
        case VEC_BOOL:
            memcpy(out->buf.bln, arr->buf.bln, (size_t)arr->length);
            break;
        case VEC_STRING:
            break; /* handled below */
        }
        if (target == VEC_STRING) {
            memcpy(out->buf.str.offsets, arr->buf.str.offsets,
                   (size_t)(arr->length + 1) * sizeof(int64_t));
            out->buf.str.data_len = arr->buf.str.data_len;
            free(out->buf.str.data); /* free the 1-byte from alloc */
            out->buf.str.data = (char *)malloc((size_t)(arr->buf.str.data_len > 0 ? arr->buf.str.data_len : 1));
            if (!out->buf.str.data)
                vectra_error("alloc failed for string copy");
            if (arr->buf.str.data_len > 0)
                memcpy(out->buf.str.data, arr->buf.str.data, (size_t)arr->buf.str.data_len);
        }
        return out;
    }

    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    if (!out) vectra_error("alloc failed");
    *out = vec_array_alloc(target, arr->length);
    memcpy(out->validity, arr->validity, (size_t)vec_validity_bytes(arr->length));

    /* Any integer type → int64 (widen) */
    if (vec_type_is_int(arr->type) && target == VEC_INT64) {
        for (int64_t i = 0; i < arr->length; i++)
            out->buf.i64[i] = vec_array_get_int(arr, i);
    }
    /* Any integer type → double (widen) */
    else if (vec_type_is_int(arr->type) && target == VEC_DOUBLE) {
        for (int64_t i = 0; i < arr->length; i++)
            out->buf.dbl[i] = (double)vec_array_get_int(arr, i);
    }
    /* bool → int64 */
    else if (arr->type == VEC_BOOL && target == VEC_INT64) {
        for (int64_t i = 0; i < arr->length; i++)
            out->buf.i64[i] = (int64_t)arr->buf.bln[i];
    }
    /* bool → double */
    else if (arr->type == VEC_BOOL && target == VEC_DOUBLE) {
        for (int64_t i = 0; i < arr->length; i++)
            out->buf.dbl[i] = (double)arr->buf.bln[i];
    }
    /* int64 → double */
    else if (arr->type == VEC_INT64 && target == VEC_DOUBLE) {
        for (int64_t i = 0; i < arr->length; i++)
            out->buf.dbl[i] = (double)arr->buf.i64[i];
    } else if (target == VEC_STRING) {
        /* Coerce numeric/bool to string: only valid values get converted,
           NAs stay as NAs. Single-pass: format into a growth buffer, record
           offsets as we go, then hand ownership to the output array. */
        char numbuf[64];
        /* Re-allocate as string type */
        vec_array_free(out);
        free(out);
        out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_STRING, arr->length);
        memcpy(out->validity, arr->validity, (size_t)vec_validity_bytes(arr->length));
        /* Growth buffer — start at 16 bytes per valid value estimate */
        int64_t buf_cap = arr->length * 16;
        if (buf_cap < 64) buf_cap = 64;
        free(out->buf.str.data);
        char *buf = (char *)malloc((size_t)buf_cap);
        int64_t off = 0;
        for (int64_t i = 0; i < arr->length; i++) {
            out->buf.str.offsets[i] = off;
            if (!vec_array_is_valid(arr, i)) continue;
            int len = 0;
            switch (arr->type) {
            case VEC_BOOL:   len = snprintf(numbuf, sizeof(numbuf), "%s", arr->buf.bln[i] ? "TRUE" : "FALSE"); break;
            case VEC_INT8:   len = snprintf(numbuf, sizeof(numbuf), "%d", (int)arr->buf.i8[i]); break;
            case VEC_INT16:  len = snprintf(numbuf, sizeof(numbuf), "%d", (int)arr->buf.i16[i]); break;
            case VEC_INT32:  len = snprintf(numbuf, sizeof(numbuf), "%d", (int)arr->buf.i32[i]); break;
            case VEC_INT64:  len = snprintf(numbuf, sizeof(numbuf), "%lld", (long long)arr->buf.i64[i]); break;
            case VEC_DOUBLE: len = snprintf(numbuf, sizeof(numbuf), "%g", arr->buf.dbl[i]); break;
            default: break;
            }
            if (off + len > buf_cap) {
                buf_cap = (off + len) * 2;
                buf = (char *)realloc(buf, (size_t)buf_cap);
            }
            memcpy(buf + off, numbuf, (size_t)len);
            off += len;
        }
        out->buf.str.offsets[arr->length] = off;
        out->buf.str.data = buf;
        out->buf.str.data_len = off;
    } else {
        vec_array_free(out);
        free(out);
        vectra_error("unsupported coercion: %d -> %d", arr->type, target);
    }

    return out;
}
