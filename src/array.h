#ifndef VECTRA_ARRAY_H
#define VECTRA_ARRAY_H

#include "types.h"

/* Allocate a VecArray of given type and length (zeroed validity = all NA) */
VecArray vec_array_alloc(VecType type, int64_t length);

/* Free array buffers */
void vec_array_free(VecArray *arr);

/* Validity bitmap helpers */
static inline int vec_array_is_valid(const VecArray *arr, int64_t i) {
    if (!arr->validity) return 1;
    return (arr->validity[i / 8] >> (i % 8)) & 1;
}

static inline void vec_array_set_valid(VecArray *arr, int64_t i) {
    if (arr->validity)
        arr->validity[i / 8] |= (uint8_t)(1 << (i % 8));
}

static inline void vec_array_set_null(VecArray *arr, int64_t i) {
    if (arr->validity)
        arr->validity[i / 8] &= (uint8_t)~(1 << (i % 8));
}

/* Set all bits valid */
void vec_array_set_all_valid(VecArray *arr);

/* Validity bitmap byte count */
static inline int64_t vec_validity_bytes(int64_t n) {
    return (n + 7) / 8;
}

/* Gather selected rows into a new dense VecArray.
   If sel is NULL, copies all rows 0..n-1 (where n = src->length). */
VecArray vec_array_gather(const VecArray *src, const int32_t *sel, int32_t sel_n);

#endif /* VECTRA_ARRAY_H */
