/*
 * src/core/buffer.h
 *
 * Shared output-buffer growth helper. Single source of truth for the
 * "grow tdc_buffer to at least N bytes via realloc_fn" pattern that
 * every encode path needs.
 *
 * Lifted from the three independent copies that previously lived in
 * src/entropy/lz2.c, src/transform/shuffle.c, and src/transform/quantize.c
 * (lz2_buf_reserve / shuffle_buf_reserve / quantize_buf_reserve). All three
 * had identical bodies. Per the project's "always clean, always scaling"
 * rule, the next stage that needed it (zigzag) is the threshold to
 * extract instead of copy-pasting a fourth time.
 *
 * Defined as `static inline` so each translation unit gets its own copy
 * with no link dependency on a new .c file. The header lives under src/,
 * not include/, because it is not part of the public ABI — it's a tdc
 * internal building block.
 *
 * Semantics:
 *   - Grows buf->capacity to at least `need` bytes.
 *   - Does NOT touch buf->size. The caller updates buf->size after
 *     filling the bytes.
 *   - Returns TDC_OK if buf->capacity is already >= need (no realloc).
 *   - Doubling growth policy starting at 64 bytes, matching the
 *     pre-extraction copies.
 *   - Returns TDC_E_NOMEM if realloc_fn returns NULL; buf is left
 *     unchanged in that case.
 */

#ifndef TDC_CORE_BUFFER_H
#define TDC_CORE_BUFFER_H

#include "tdc/types.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

static inline tdc_status tdc_buf_reserve(tdc_buffer *buf, size_t need) {
    if (buf->capacity >= need) return TDC_OK;
    size_t new_cap = buf->capacity ? buf->capacity : 64u;
    while (new_cap < need) new_cap *= 2u;
    void *p = buf->realloc_fn(buf->user, buf->data, new_cap);
    if (!p) return TDC_E_NOMEM;
    buf->data = (uint8_t *)p;
    buf->capacity = new_cap;
    return TDC_OK;
}

#ifdef __cplusplus
}
#endif

#endif /* TDC_CORE_BUFFER_H */
