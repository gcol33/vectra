#include "array.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

VecArray vec_array_alloc(VecType type, int64_t length) {
    VecArray arr;
    memset(&arr, 0, sizeof(arr));
    arr.type = type;
    arr.length = length;
    arr.owns_data = 1;

    int64_t vbytes = vec_validity_bytes(length);
    arr.validity = (uint8_t *)calloc((size_t)vbytes, 1);
    if (!arr.validity && vbytes > 0)
        vectra_error("failed to allocate validity bitmap (%lld bytes)", (long long)vbytes);

    switch (type) {
    case VEC_INT64:
        arr.buf.i64 = (int64_t *)calloc((size_t)length, sizeof(int64_t));
        if (!arr.buf.i64 && length > 0) vectra_error("alloc failed for int64 array");
        break;
    case VEC_DOUBLE:
        arr.buf.dbl = (double *)calloc((size_t)length, sizeof(double));
        if (!arr.buf.dbl && length > 0) vectra_error("alloc failed for double array");
        break;
    case VEC_BOOL:
        arr.buf.bln = (uint8_t *)calloc((size_t)length, 1);
        if (!arr.buf.bln && length > 0) vectra_error("alloc failed for bool array");
        break;
    case VEC_STRING:
        arr.buf.str.offsets = (int64_t *)calloc((size_t)(length + 1), sizeof(int64_t));
        if (!arr.buf.str.offsets && length > 0) vectra_error("alloc failed for string offsets");
        arr.buf.str.data = NULL;
        arr.buf.str.data_len = 0;
        break;
    }
    return arr;
}

void vec_array_free(VecArray *arr) {
    if (!arr) return;
    free(arr->validity);
    arr->validity = NULL;
    switch (arr->type) {
    case VEC_INT64:  free(arr->buf.i64); arr->buf.i64 = NULL; break;
    case VEC_DOUBLE: free(arr->buf.dbl); arr->buf.dbl = NULL; break;
    case VEC_BOOL:   free(arr->buf.bln); arr->buf.bln = NULL; break;
    case VEC_STRING:
        free(arr->buf.str.offsets); arr->buf.str.offsets = NULL;
        if (arr->owns_data) {
            free(arr->buf.str.data);
        }
        arr->buf.str.data = NULL;
        break;
    }
    arr->length = 0;
}

void vec_array_set_all_valid(VecArray *arr) {
    if (!arr->validity) return;
    int64_t vbytes = vec_validity_bytes(arr->length);
    memset(arr->validity, 0xFF, (size_t)vbytes);
}

VecArray vec_array_gather(const VecArray *src, const int32_t *sel, int32_t sel_n) {
    int64_t n = (int64_t)sel_n;
    VecArray dst = vec_array_alloc(src->type, n);

    switch (src->type) {
    case VEC_INT64:
        for (int32_t j = 0; j < sel_n; j++) {
            int64_t pi = (int64_t)sel[j];
            if (vec_array_is_valid(src, pi)) {
                vec_array_set_valid(&dst, j);
                dst.buf.i64[j] = src->buf.i64[pi];
            }
        }
        break;
    case VEC_DOUBLE:
        for (int32_t j = 0; j < sel_n; j++) {
            int64_t pi = (int64_t)sel[j];
            if (vec_array_is_valid(src, pi)) {
                vec_array_set_valid(&dst, j);
                dst.buf.dbl[j] = src->buf.dbl[pi];
            }
        }
        break;
    case VEC_BOOL:
        for (int32_t j = 0; j < sel_n; j++) {
            int64_t pi = (int64_t)sel[j];
            if (vec_array_is_valid(src, pi)) {
                vec_array_set_valid(&dst, j);
                dst.buf.bln[j] = src->buf.bln[pi];
            }
        }
        break;
    case VEC_STRING: {
        /* Compute total string bytes */
        int64_t total = 0;
        for (int32_t j = 0; j < sel_n; j++) {
            int64_t pi = (int64_t)sel[j];
            if (vec_array_is_valid(src, pi))
                total += src->buf.str.offsets[pi + 1] -
                         src->buf.str.offsets[pi];
        }
        free(dst.buf.str.data);
        dst.buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
        dst.buf.str.data_len = total;

        int64_t off = 0;
        for (int32_t j = 0; j < sel_n; j++) {
            int64_t pi = (int64_t)sel[j];
            dst.buf.str.offsets[j] = off;
            if (vec_array_is_valid(src, pi)) {
                vec_array_set_valid(&dst, j);
                int64_t s = src->buf.str.offsets[pi];
                int64_t slen = src->buf.str.offsets[pi + 1] - s;
                if (slen > 0)
                    memcpy(dst.buf.str.data + off,
                           src->buf.str.data + s, (size_t)slen);
                off += slen;
            }
        }
        dst.buf.str.offsets[sel_n] = off;
        break;
    }
    }
    return dst;
}
