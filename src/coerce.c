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
            free(out->buf.str.data); /* free the NULL from alloc */
            out->buf.str.data = (char *)malloc((size_t)(arr->buf.str.data_len > 0 ? arr->buf.str.data_len : 1));
            if (!out->buf.str.data && arr->buf.str.data_len > 0)
                vectra_error("alloc failed for string copy");
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
    } else {
        vec_array_free(out);
        free(out);
        vectra_error("unsupported coercion: %d -> %d", arr->type, target);
    }

    return out;
}
