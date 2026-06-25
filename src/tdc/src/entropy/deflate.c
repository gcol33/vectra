/*
 * src/entropy/deflate.c
 *
 * TDC_ENTROPY_DEFLATE — zlib deflate wrapper. Optional dependency,
 * gated by a TDC_HAVE_ZLIB compile flag. When zlib is not linked, the
 * vtable is not compiled and tdc_entropy_get(TDC_ENTROPY_DEFLATE) returns
 * NULL from the registry.
 *
 * Provides the "ratio" mode: slower but smaller than native LZ. Useful
 * when the caller genuinely wants max ratio over max speed.
 *
 * Params: tdc_entropy_level::level maps to zlib compression level (1-9).
 * Level 0 uses Z_DEFAULT_COMPRESSION (6).
 */

#include "tdc/entropy.h"
#include "entropy_internal.h"
#include "../core/buffer.h"

#ifdef TDC_HAVE_ZLIB

#include <zlib.h>
#include <stddef.h>
#include <stdint.h>

/* ----- Encode bound ------------------------------------------------------ */

static size_t deflate_encode_bound(size_t src_size) {
    /* zlib's compressBound returns a conservative upper bound. */
    return (size_t)compressBound((uLong)src_size);
}

/* ----- Encode ------------------------------------------------------------- */

static tdc_status deflate_encode(const uint8_t *src, size_t src_size,
                                 const void *params, tdc_buffer *dst) {
    if (!dst || !dst->realloc_fn) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;

    int level = Z_DEFAULT_COMPRESSION;
    if (params) {
        const tdc_entropy_level *p = (const tdc_entropy_level *)params;
        if (p->level != 0) level = p->level;
    }

    if (src_size == 0) {
        /* zlib handles empty input, but we short-circuit for clarity. */
        size_t bound = deflate_encode_bound(0);
        tdc_status st = tdc_buf_reserve(dst, bound);
        if (st != TDC_OK) return st;
        uLongf dest_len = (uLongf)dst->capacity;
        int ret = compress2(dst->data, &dest_len, src, 0, level);
        if (ret != Z_OK) return TDC_E_NOMEM;
        dst->size = (size_t)dest_len;
        return TDC_OK;
    }

    size_t bound = deflate_encode_bound(src_size);
    tdc_status st = tdc_buf_reserve(dst, bound);
    if (st != TDC_OK) return st;

    uLongf dest_len = (uLongf)dst->capacity;
    int ret = compress2(dst->data, &dest_len, src, (uLong)src_size, level);
    if (ret != Z_OK) {
        if (ret == Z_MEM_ERROR) return TDC_E_NOMEM;
        return TDC_E_INVAL;
    }

    dst->size = (size_t)dest_len;
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status deflate_decode(const uint8_t *src, size_t src_size,
                                 uint8_t *dst, size_t dst_size) {
    if (src_size > 0 && !src) return TDC_E_INVAL;
    if (dst_size > 0 && !dst) return TDC_E_INVAL;

    uLongf dest_len = (uLongf)dst_size;
    int ret = uncompress(dst, &dest_len, src, (uLong)src_size);
    if (ret != Z_OK) return TDC_E_CORRUPT;
    if ((size_t)dest_len != dst_size) return TDC_E_CORRUPT;
    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_entropy_vt tdc_entropy_deflate_vt = {
    .id           = TDC_ENTROPY_DEFLATE,
    .name         = "deflate",
    .encode_bound = deflate_encode_bound,
    .encode       = deflate_encode,
    .decode       = deflate_decode,
};

#endif /* TDC_HAVE_ZLIB */
