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
