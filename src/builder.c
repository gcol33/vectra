#include "builder.h"
#include "array.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

#define INITIAL_CAP 1024

static void ensure_capacity(VecArrayBuilder *b, int64_t extra) {
    int64_t needed = b->length + extra;
    if (needed <= b->capacity) return;

    int64_t new_cap = b->capacity == 0 ? INITIAL_CAP : b->capacity;
    while (new_cap < needed) new_cap *= 2;

    int64_t old_vbytes = vec_validity_bytes(b->capacity);
    int64_t new_vbytes = vec_validity_bytes(new_cap);
    b->validity = (uint8_t *)realloc(b->validity, (size_t)new_vbytes);
    if (!b->validity) vectra_error("builder realloc failed (validity)");
    if (new_vbytes > old_vbytes)
        memset(b->validity + old_vbytes, 0, (size_t)(new_vbytes - old_vbytes));

    switch (b->type) {
    case VEC_INT64:
        b->buf.i64 = (int64_t *)realloc(b->buf.i64, (size_t)new_cap * sizeof(int64_t));
        if (!b->buf.i64) vectra_error("builder realloc failed (i64)");
        break;
    case VEC_DOUBLE:
        b->buf.dbl = (double *)realloc(b->buf.dbl, (size_t)new_cap * sizeof(double));
        if (!b->buf.dbl) vectra_error("builder realloc failed (dbl)");
        break;
    case VEC_BOOL:
        b->buf.bln = (uint8_t *)realloc(b->buf.bln, (size_t)new_cap);
        if (!b->buf.bln) vectra_error("builder realloc failed (bln)");
        break;
    case VEC_STRING:
        b->str_offsets = (int64_t *)realloc(b->str_offsets,
            (size_t)(new_cap + 1) * sizeof(int64_t));
        if (!b->str_offsets) vectra_error("builder realloc failed (str offsets)");
        break;
    }
    b->capacity = new_cap;
}

static void ensure_str_data(VecArrayBuilder *b, int64_t extra) {
    int64_t needed = b->str_data_len + extra;
    if (needed <= b->str_data_cap) return;
    int64_t new_cap = b->str_data_cap == 0 ? INITIAL_CAP : b->str_data_cap;
    while (new_cap < needed) new_cap *= 2;
    b->str_data = (char *)realloc(b->str_data, (size_t)new_cap);
    if (!b->str_data) vectra_error("builder realloc failed (str data)");
    b->str_data_cap = new_cap;
}

VecArrayBuilder vec_builder_init(VecType type) {
    VecArrayBuilder b;
    memset(&b, 0, sizeof(b));
    b.type = type;
    return b;
}

void vec_builder_reserve(VecArrayBuilder *b, int64_t extra) {
    ensure_capacity(b, extra);
}

void vec_builder_append_array(VecArrayBuilder *b, const VecArray *arr) {
    if (arr->length == 0) return;
    ensure_capacity(b, arr->length);

    /* Copy validity bits */
    for (int64_t i = 0; i < arr->length; i++) {
        if (vec_array_is_valid(arr, i))
            b->validity[(b->length + i) / 8] |= (uint8_t)(1 << ((b->length + i) % 8));
    }

    switch (b->type) {
    case VEC_INT64:
        memcpy(b->buf.i64 + b->length, arr->buf.i64,
               (size_t)arr->length * sizeof(int64_t));
        break;
    case VEC_DOUBLE:
        memcpy(b->buf.dbl + b->length, arr->buf.dbl,
               (size_t)arr->length * sizeof(double));
        break;
    case VEC_BOOL:
        memcpy(b->buf.bln + b->length, arr->buf.bln,
               (size_t)arr->length);
        break;
    case VEC_STRING: {
        int64_t base_offset = b->str_data_len;
        /* Copy string data */
        int64_t sdata_len = arr->buf.str.offsets[arr->length] - arr->buf.str.offsets[0];
        ensure_str_data(b, sdata_len);
        if (sdata_len > 0)
            memcpy(b->str_data + b->str_data_len,
                   arr->buf.str.data + arr->buf.str.offsets[0], (size_t)sdata_len);
        /* Copy offsets (rebased) */
        for (int64_t i = 0; i < arr->length; i++) {
            b->str_offsets[b->length + i] =
                base_offset + (arr->buf.str.offsets[i] - arr->buf.str.offsets[0]);
        }
        b->str_offsets[b->length + arr->length] = base_offset + sdata_len;
        b->str_data_len += sdata_len;
        break;
    }
    }
    b->length += arr->length;
}

void vec_builder_append_one(VecArrayBuilder *b, const VecArray *arr, int64_t row) {
    ensure_capacity(b, 1);
    if (vec_array_is_valid(arr, row)) {
        b->validity[b->length / 8] |= (uint8_t)(1 << (b->length % 8));
        switch (b->type) {
        case VEC_INT64:  b->buf.i64[b->length] = arr->buf.i64[row]; break;
        case VEC_DOUBLE: b->buf.dbl[b->length] = arr->buf.dbl[row]; break;
        case VEC_BOOL:   b->buf.bln[b->length] = arr->buf.bln[row]; break;
        case VEC_STRING: {
            int64_t s = arr->buf.str.offsets[row];
            int64_t e = arr->buf.str.offsets[row + 1];
            int64_t slen = e - s;
            ensure_str_data(b, slen);
            b->str_offsets[b->length] = b->str_data_len;
            memcpy(b->str_data + b->str_data_len, arr->buf.str.data + s, (size_t)slen);
            b->str_data_len += slen;
            b->str_offsets[b->length + 1] = b->str_data_len;
            break;
        }
        }
    } else {
        /* NULL value: don't set validity bit */
        if (b->type == VEC_STRING) {
            b->str_offsets[b->length] = b->str_data_len;
            b->str_offsets[b->length + 1] = b->str_data_len;
        }
    }
    b->length++;
}

void vec_builder_append_na(VecArrayBuilder *b) {
    ensure_capacity(b, 1);
    /* validity bit is already 0 from ensure_capacity's zeroed extension */
    b->validity[b->length / 8] &= ~(uint8_t)(1 << (b->length % 8));
    if (b->type == VEC_STRING) {
        b->str_offsets[b->length] = b->str_data_len;
        b->str_offsets[b->length + 1] = b->str_data_len;
    }
    b->length++;
}

void vec_builder_append_na_n(VecArrayBuilder *b, int64_t n) {
    if (n <= 0) return;
    ensure_capacity(b, n);
    /* Validity bits stay 0 (already zeroed by ensure_capacity) */
    /* Clear any bits that might have been set previously in this byte range */
    for (int64_t i = 0; i < n; i++)
        b->validity[(b->length + i) / 8] &=
            ~(uint8_t)(1 << ((b->length + i) % 8));
    if (b->type == VEC_STRING) {
        for (int64_t i = 0; i <= n; i++)
            b->str_offsets[b->length + i] = b->str_data_len;
    }
    b->length += n;
}

void vec_builder_append_repeat(VecArrayBuilder *b, const VecArray *arr,
                               int64_t row, int64_t count) {
    if (count <= 0) return;
    ensure_capacity(b, count);
    int valid = vec_array_is_valid(arr, row);

    if (valid) {
        /* Set validity bits for entire run */
        for (int64_t i = 0; i < count; i++)
            b->validity[(b->length + i) / 8] |=
                (uint8_t)(1 << ((b->length + i) % 8));

        switch (b->type) {
        case VEC_INT64: {
            int64_t v = arr->buf.i64[row];
            for (int64_t i = 0; i < count; i++)
                b->buf.i64[b->length + i] = v;
            break;
        }
        case VEC_DOUBLE: {
            double v = arr->buf.dbl[row];
            for (int64_t i = 0; i < count; i++)
                b->buf.dbl[b->length + i] = v;
            break;
        }
        case VEC_BOOL: {
            uint8_t v = arr->buf.bln[row];
            memset(b->buf.bln + b->length, v, (size_t)count);
            break;
        }
        case VEC_STRING: {
            int64_t s = arr->buf.str.offsets[row];
            int64_t e = arr->buf.str.offsets[row + 1];
            int64_t slen = e - s;
            ensure_str_data(b, slen * count);
            for (int64_t i = 0; i < count; i++) {
                b->str_offsets[b->length + i] = b->str_data_len;
                memcpy(b->str_data + b->str_data_len,
                       arr->buf.str.data + s, (size_t)slen);
                b->str_data_len += slen;
            }
            b->str_offsets[b->length + count] = b->str_data_len;
            break;
        }
        }
    } else {
        /* NA repeated: validity bits stay 0 */
        if (b->type == VEC_STRING) {
            for (int64_t i = 0; i <= count; i++)
                b->str_offsets[b->length + i] = b->str_data_len;
        }
    }
    b->length += count;
}

VecArray vec_builder_finish(VecArrayBuilder *b) {
    VecArray arr;
    memset(&arr, 0, sizeof(arr));
    arr.type = b->type;
    arr.length = b->length;
    arr.owns_data = 1;
    arr.validity = b->validity;

    switch (b->type) {
    case VEC_INT64:  arr.buf.i64 = b->buf.i64; break;
    case VEC_DOUBLE: arr.buf.dbl = b->buf.dbl; break;
    case VEC_BOOL:   arr.buf.bln = b->buf.bln; break;
    case VEC_STRING:
        arr.buf.str.offsets = b->str_offsets;
        arr.buf.str.data = b->str_data;
        arr.buf.str.data_len = b->str_data_len;
        break;
    }

    /* Zero out builder so it can't be reused */
    memset(b, 0, sizeof(*b));
    return arr;
}

void vec_builder_free(VecArrayBuilder *b) {
    free(b->validity);
    switch (b->type) {
    case VEC_INT64:  free(b->buf.i64); break;
    case VEC_DOUBLE: free(b->buf.dbl); break;
    case VEC_BOOL:   free(b->buf.bln); break;
    case VEC_STRING:
        free(b->str_offsets);
        free(b->str_data);
        break;
    }
    memset(b, 0, sizeof(*b));
}
