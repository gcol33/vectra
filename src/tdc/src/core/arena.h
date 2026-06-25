/*
 * src/core/arena.h
 *
 * Bump-pointer scratch allocator. Avoids malloc/free churn for per-block
 * temporary buffers (residual arrays, hash tables, tile coefficient arrays).
 *
 * The arena is caller-owned: the caller provides a pre-allocated region
 * and resets between blocks. tdc never frees arena memory. This keeps
 * allocation on the hot path to a pointer bump + alignment fixup.
 *
 * Defined as static inline (same convention as buffer.h) so each
 * translation unit gets its own copy with no link dependency.
 */

#ifndef TDC_CORE_ARENA_H
#define TDC_CORE_ARENA_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint8_t *base;
    size_t   used;
    size_t   capacity;
} tdc_arena;

/* Initialize an arena over a caller-provided buffer. */
static inline void tdc_arena_init(tdc_arena *a, uint8_t *base, size_t capacity) {
    a->base     = base;
    a->used     = 0;
    a->capacity = capacity;
}

/* Allocate n bytes with the given alignment (must be a power of 2).
 * Returns NULL if the arena is exhausted. */
static inline void *tdc_arena_alloc(tdc_arena *a, size_t n, size_t align) {
    size_t mask = align - 1u;
    size_t pad  = (align - (a->used & mask)) & mask;
    if (a->used + pad + n > a->capacity) return NULL;
    void *ptr = a->base + a->used + pad;
    a->used += pad + n;
    return ptr;
}

/* Convenience: allocate with default alignment (8 bytes). */
static inline void *tdc_arena_alloc8(tdc_arena *a, size_t n) {
    return tdc_arena_alloc(a, n, 8u);
}

/* Reset the arena to empty. Does not touch the backing memory. */
static inline void tdc_arena_reset(tdc_arena *a) {
    a->used = 0;
}

/* Bytes currently in use. */
static inline size_t tdc_arena_used(const tdc_arena *a) {
    return a->used;
}

/* Bytes remaining. */
static inline size_t tdc_arena_remaining(const tdc_arena *a) {
    return a->capacity - a->used;
}

#ifdef __cplusplus
}
#endif

#endif /* TDC_CORE_ARENA_H */
