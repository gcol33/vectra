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
    case VEC_INT32:
        arr.buf.i32 = (int32_t *)calloc((size_t)length, sizeof(int32_t));
        if (!arr.buf.i32 && length > 0) vectra_error("alloc failed for int32 array");
        break;
    case VEC_INT16:
        arr.buf.i16 = (int16_t *)calloc((size_t)length, sizeof(int16_t));
        if (!arr.buf.i16 && length > 0) vectra_error("alloc failed for int16 array");
        break;
    case VEC_INT8:
        arr.buf.i8 = (int8_t *)calloc((size_t)length, sizeof(int8_t));
        if (!arr.buf.i8 && length > 0) vectra_error("alloc failed for int8 array");
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
        arr.buf.str.data = (char *)malloc(1);  /* never NULL — avoids UB on ptr arithmetic */
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
    case VEC_INT64:  if (arr->owns_data) free(arr->buf.i64); arr->buf.i64 = NULL; break;
    case VEC_INT32:  if (arr->owns_data) free(arr->buf.i32); arr->buf.i32 = NULL; break;
    case VEC_INT16:  if (arr->owns_data) free(arr->buf.i16); arr->buf.i16 = NULL; break;
    case VEC_INT8:   if (arr->owns_data) free(arr->buf.i8);  arr->buf.i8  = NULL; break;
    case VEC_DOUBLE: if (arr->owns_data) free(arr->buf.dbl); arr->buf.dbl = NULL; break;
    case VEC_BOOL:   if (arr->owns_data) free(arr->buf.bln); arr->buf.bln = NULL; break;
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

/* ------------------------------------------------------------------ */
/*  Bulk validity bitmap operations                                   */
/* ------------------------------------------------------------------ */

void vec_validity_set_bits(uint8_t *bitmap, int64_t off, int64_t n) {
    if (n <= 0 || !bitmap) return;

    int64_t end = off + n;
    int64_t first_byte = off >> 3;
    int64_t last_byte  = (end - 1) >> 3;
    int first_bit = (int)(off & 7);
    int last_bit  = (int)(end & 7);  /* 0 means full last byte */

    if (first_byte == last_byte) {
        /* All bits in a single byte */
        uint8_t mask = (uint8_t)(((1 << n) - 1) << first_bit);
        bitmap[first_byte] |= mask;
        return;
    }

    /* First partial byte */
    if (first_bit != 0) {
        bitmap[first_byte] |= (uint8_t)(0xFF << first_bit);
        first_byte++;
    }

    /* Full bytes in the middle */
    int64_t full_bytes = last_byte - first_byte;
    if (last_bit == 0) full_bytes++;  /* last byte is also full */
    if (full_bytes > 0)
        memset(bitmap + first_byte, 0xFF, (size_t)full_bytes);

    /* Last partial byte */
    if (last_bit != 0)
        bitmap[last_byte] |= (uint8_t)((1 << last_bit) - 1);
}

void vec_validity_clear_bits(uint8_t *bitmap, int64_t off, int64_t n) {
    if (n <= 0 || !bitmap) return;

    int64_t end = off + n;
    int64_t first_byte = off >> 3;
    int64_t last_byte  = (end - 1) >> 3;
    int first_bit = (int)(off & 7);
    int last_bit  = (int)(end & 7);

    if (first_byte == last_byte) {
        uint8_t mask = (uint8_t)(((1 << n) - 1) << first_bit);
        bitmap[first_byte] &= ~mask;
        return;
    }

    if (first_bit != 0) {
        bitmap[first_byte] &= (uint8_t)((1 << first_bit) - 1);
        first_byte++;
    }

    int64_t full_bytes = last_byte - first_byte;
    if (last_bit == 0) full_bytes++;
    if (full_bytes > 0)
        memset(bitmap + first_byte, 0, (size_t)full_bytes);

    if (last_bit != 0)
        bitmap[last_byte] &= (uint8_t)(0xFF << last_bit);
}

void vec_validity_copy_bits(uint8_t *dst, int64_t dst_off,
                            const uint8_t *src, int64_t src_off,
                            int64_t n) {
    if (n <= 0 || !dst || !src) return;

    /* Fast path: both byte-aligned */
    if ((dst_off & 7) == 0 && (src_off & 7) == 0) {
        int64_t db = dst_off >> 3, sb = src_off >> 3;
        int64_t full = n >> 3;
        if (full > 0)
            memcpy(dst + db, src + sb, (size_t)full);
        int rem = (int)(n & 7);
        if (rem > 0) {
            uint8_t mask = (uint8_t)((1 << rem) - 1);
            dst[db + full] = (uint8_t)((dst[db + full] & ~mask) |
                                        (src[sb + full] & mask));
        }
        return;
    }

    /* General case: byte-level shift-and-combine.
       Read source bits in 8-bit chunks, shift to destination alignment. */
    int64_t src_bytes = (src_off + n + 7) >> 3;
    for (int64_t i = 0; i < n; ) {
        /* How many bits until next dst byte boundary? */
        int64_t di = dst_off + i;
        int64_t si = src_off + i;
        int d_bit = (int)(di & 7);
        int s_bit = (int)(si & 7);

        /* Try to process a full byte at a time when dst is byte-aligned */
        if (d_bit == 0 && s_bit == 0 && n - i >= 8) {
            dst[di >> 3] = src[si >> 3];
            i += 8;
            continue;
        }

        /* Process up to 8 bits: extract from src, place into dst */
        int chunk = 8 - (d_bit > s_bit ? d_bit : s_bit);
        if (chunk > (int)(n - i)) chunk = (int)(n - i);
        if (chunk <= 0) chunk = 1;

        /* Extract chunk bits from src */
        uint8_t sbyte = src[si >> 3];
        uint8_t bits = (uint8_t)((sbyte >> s_bit) & ((1 << chunk) - 1));

        /* Place into dst */
        uint8_t dmask = (uint8_t)(((1 << chunk) - 1) << d_bit);
        dst[di >> 3] = (uint8_t)((dst[di >> 3] & ~dmask) |
                                   ((bits << d_bit) & dmask));
        i += chunk;
    }
}

VecArray vec_array_gather(const VecArray *src, const int32_t *sel, int32_t sel_n) {
    int64_t n = (int64_t)sel_n;
    VecArray dst = vec_array_alloc(src->type, n);

    switch (src->type) {
    case VEC_INT64:
        for (int32_t j = 0; j < sel_n; j++) {
            if (j + VEC_PREFETCH_AHEAD < sel_n)
                VEC_PREFETCH_READ(&src->buf.i64[sel[j + VEC_PREFETCH_AHEAD]]);
            int64_t pi = (int64_t)sel[j];
            if (vec_array_is_valid(src, pi)) {
                vec_array_set_valid(&dst, j);
                dst.buf.i64[j] = src->buf.i64[pi];
            }
        }
        break;
    case VEC_INT32:
        for (int32_t j = 0; j < sel_n; j++) {
            int64_t pi = (int64_t)sel[j];
            if (vec_array_is_valid(src, pi)) {
                vec_array_set_valid(&dst, j);
                dst.buf.i32[j] = src->buf.i32[pi];
            }
        }
        break;
    case VEC_INT16:
        for (int32_t j = 0; j < sel_n; j++) {
            int64_t pi = (int64_t)sel[j];
            if (vec_array_is_valid(src, pi)) {
                vec_array_set_valid(&dst, j);
                dst.buf.i16[j] = src->buf.i16[pi];
            }
        }
        break;
    case VEC_INT8:
        for (int32_t j = 0; j < sel_n; j++) {
            int64_t pi = (int64_t)sel[j];
            if (vec_array_is_valid(src, pi)) {
                vec_array_set_valid(&dst, j);
                dst.buf.i8[j] = src->buf.i8[pi];
            }
        }
        break;
    case VEC_DOUBLE:
        for (int32_t j = 0; j < sel_n; j++) {
            if (j + VEC_PREFETCH_AHEAD < sel_n)
                VEC_PREFETCH_READ(&src->buf.dbl[sel[j + VEC_PREFETCH_AHEAD]]);
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
