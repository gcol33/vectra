/*
 * src/entropy/none.c
 *
 * TDC_ENTROPY_NONE — memcpy passthrough. Useful as a baseline and for
 * tiny blocks where any compressor would lose to its own header overhead.
 *
 * The vtable behavior is the minimum that satisfies the entropy contract:
 *   - encode_bound(n) == n        (no header, no expansion)
 *   - encode  copies src into dst, sets dst->size = src_size
 *   - decode  copies src into dst, requires src_size == dst_size
 *
 * No params are consulted. The level field on tdc_entropy_level is
 * ignored — there is nothing to tune.
 */

#include "tdc/entropy.h"
#include "entropy_internal.h"
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

static size_t none_encode_bound(size_t src_size) {
    return src_size;
}

static tdc_status none_encode(const uint8_t *src, size_t src_size,
                              const void    *params,
                              tdc_buffer    *dst) {
    (void)params;
    if (!dst || !dst->realloc_fn) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;

    tdc_status st = tdc_buf_reserve(dst, src_size);
    if (st != TDC_OK) return st;

    if (src_size > 0) memcpy(dst->data, src, src_size);
    dst->size = src_size;
    return TDC_OK;
}

static tdc_status none_decode(const uint8_t *src, size_t src_size,
                              uint8_t       *dst, size_t dst_size) {
    if (src_size != dst_size) return TDC_E_CORRUPT;
    if (src_size > 0 && (!src || !dst)) return TDC_E_INVAL;
    if (src_size > 0) memcpy(dst, src, src_size);
    return TDC_OK;
}

const tdc_entropy_vt tdc_entropy_none_vt = {
    .id           = TDC_ENTROPY_NONE,
    .name         = "none",
    .encode_bound = none_encode_bound,
    .encode       = none_encode,
    .decode       = none_decode,
};
