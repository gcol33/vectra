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

/* Check if all elements are valid (no NAs). Uses word-level checks. */
static inline int vec_array_all_valid(const VecArray *arr) {
    if (!arr->validity) return 1;
    int64_t n = arr->length;
    if (n == 0) return 1;
    int64_t full_bytes = n / 8;
    /* Check full bytes (fast path: cast to uint64_t for 8x throughput) */
    int64_t full_words = full_bytes / 8;
    const uint64_t *words = (const uint64_t *)arr->validity;
    for (int64_t i = 0; i < full_words; i++) {
        if (words[i] != 0xFFFFFFFFFFFFFFFFULL) return 0;
    }
    for (int64_t i = full_words * 8; i < full_bytes; i++) {
        if (arr->validity[i] != 0xFF) return 0;
    }
    int rem = (int)(n % 8);
    if (rem > 0) {
        uint8_t mask = (uint8_t)((1 << rem) - 1);
        if ((arr->validity[full_bytes] & mask) != mask) return 0;
    }
    return 1;
}

/* Validity bitmap byte count */
static inline int64_t vec_validity_bytes(int64_t n) {
    return (n + 7) / 8;
}

/* Software prefetch for gather loops with random access patterns.
   Prefetch for read, low temporal locality (NTA). */
#if defined(__GNUC__) || defined(__clang__)
#define VEC_PREFETCH_READ(addr) __builtin_prefetch((addr), 0, 0)
#else
#define VEC_PREFETCH_READ(addr) ((void)0)
#endif
#define VEC_PREFETCH_AHEAD 8

/* Bulk validity bitmap operations — process 64 bits at a time instead of
   bit-by-bit.  ~5-10x faster for large arrays. */

/* Set n bits to 1 starting at bit offset `off`. */
void vec_validity_set_bits(uint8_t *bitmap, int64_t off, int64_t n);

/* Clear n bits to 0 starting at bit offset `off`. */
void vec_validity_clear_bits(uint8_t *bitmap, int64_t off, int64_t n);

/* Copy n bits from src[src_off..] to dst[dst_off..].
   Handles arbitrary alignment; fast memcpy path when both are byte-aligned. */
void vec_validity_copy_bits(uint8_t *dst, int64_t dst_off,
                            const uint8_t *src, int64_t src_off,
                            int64_t n);

/* Gather selected rows into a new dense VecArray.
   If sel is NULL, copies all rows 0..n-1 (where n = src->length). */
VecArray vec_array_gather(const VecArray *src, const int32_t *sel, int32_t sel_n);

#endif /* VECTRA_ARRAY_H */
