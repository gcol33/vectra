/*
 * src/api/entropy_chain.c
 *
 * Implements: driver_entropy_chain_encode, driver_entropy_chain_decode
 * (declared in driver_internal.h).
 *
 * The entropy chain is walked LTR on encode and RTL on decode, ping-ponging
 * between two scratch buffers. Intermediate sizes between stages are not
 * stored in the 80-byte block header; a small size table is prepended to the
 * encoded bytes instead:
 *
 *     [ u32 * len ]  stage-input sizes (stage 0 first, LE)
 *     [ final entropy-encoded bytes ]
 *
 * The innermost stage's input size is redundant with the caller's logical
 * size, and is kept anyway as a cross-check and to avoid a special-case
 * branch in the decoder.
 */

#ifdef _MSC_VER
#  define _CRT_SECURE_NO_WARNINGS 1
#endif

#include "tdc/codec.h"
#include "tdc/entropy.h"
#include "tdc/types.h"

#include "../core/buffer.h"
#include "driver_internal.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

tdc_status driver_entropy_chain_encode(const tdc_entropy_id *ids,
                                       const void *const    *params,
                                       int                   len,
                                       const uint8_t        *src,
                                       size_t                src_size,
                                       tdc_buffer           *out,
                                       const tdc_buffer     *scratch_parent) {
    if (!ids || !params || !out || !scratch_parent) return TDC_E_INVAL;
    if (len < 0 || len > TDC_MAX_ENTROPY) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;

    tdc_status st;
    if (len == 0) {
        st = tdc_buf_reserve(out, src_size > 0 ? src_size : 1u);
        if (st != TDC_OK) return st;
        if (src_size > 0) memcpy(out->data, src, src_size);
        out->size = src_size;
        return TDC_OK;
    }

    tdc_buffer bufs[2];
    driver_scratch_init(&bufs[0], scratch_parent);
    driver_scratch_init(&bufs[1], scratch_parent);
    int cur = 0;

    uint32_t       stage_in_sizes[TDC_MAX_ENTROPY];
    const uint8_t *stage_src      = src;
    size_t         stage_src_size = src_size;

    for (int i = 0; i < len; ++i) {
        if (stage_src_size > UINT32_MAX) { st = TDC_E_INVAL; goto cleanup; }
        stage_in_sizes[i] = (uint32_t)stage_src_size;

        const tdc_entropy_vt *evt = tdc_entropy_get(ids[i]);
        if (!evt) { st = TDC_E_UNSUPPORTED; goto cleanup; }

        bufs[cur].size = 0;
        st = evt->encode(stage_src, stage_src_size, params[i], &bufs[cur]);
        if (st != TDC_OK) goto cleanup;

        stage_src      = bufs[cur].data;
        stage_src_size = bufs[cur].size;
        cur = 1 - cur;
    }

    size_t sizes_bytes = (size_t)len * sizeof(uint32_t);
    size_t total       = sizes_bytes + stage_src_size;
    if (total < sizes_bytes) { st = TDC_E_INVAL; goto cleanup; }
    st = tdc_buf_reserve(out, total);
    if (st != TDC_OK) goto cleanup;
    for (int i = 0; i < len; ++i) {
        uint32_t le = stage_in_sizes[i];
        memcpy(out->data + (size_t)i * sizeof(uint32_t), &le, sizeof(le));
    }
    if (stage_src_size > 0) {
        memcpy(out->data + sizes_bytes, stage_src, stage_src_size);
    }
    out->size = total;
    st = TDC_OK;

cleanup:
    driver_scratch_free(&bufs[0]);
    driver_scratch_free(&bufs[1]);
    return st;
}

tdc_status driver_entropy_chain_decode(const uint16_t *ids,
                                       int             len,
                                       const uint8_t  *payload,
                                       size_t          payload_size,
                                       size_t          out_size,
                                       tdc_buffer      bufs[2],
                                       int            *cur) {
    if (!ids || !bufs || !cur) return TDC_E_INVAL;
    if (len < 0 || len > TDC_MAX_ENTROPY) return TDC_E_INVAL;
    if (payload_size > 0 && !payload) return TDC_E_INVAL;

    /* At least 1 byte so data is non-NULL for empty inputs, plus 16 bytes
     * of wildcopy slack for LZ-family decoders. */
    tdc_status st = tdc_buf_reserve(&bufs[*cur],
                                    (out_size > 0 ? out_size : 1u) + 16u);
    if (st != TDC_OK) return st;

    if (len == 0) {
        if (payload_size != out_size) return TDC_E_CORRUPT;
        if (out_size > 0) memcpy(bufs[*cur].data, payload, out_size);
        bufs[*cur].size = out_size;
        return TDC_OK;
    }

    size_t sizes_bytes = (size_t)len * sizeof(uint32_t);
    if (payload_size < sizes_bytes) return TDC_E_CORRUPT;
    uint32_t stage_in_sizes[TDC_MAX_ENTROPY];
    for (int i = 0; i < len; ++i) {
        uint32_t le;
        memcpy(&le, payload + (size_t)i * sizeof(uint32_t), sizeof(le));
        stage_in_sizes[i] = le;
    }
    if ((uint64_t)stage_in_sizes[0] != (uint64_t)out_size) return TDC_E_CORRUPT;

    const uint8_t *stage_src      = payload + sizes_bytes;
    size_t         stage_src_size = payload_size - sizes_bytes;

    for (int i = len - 1; i >= 0; --i) {
        const tdc_entropy_vt *evt = tdc_entropy_get((tdc_entropy_id)ids[i]);
        if (!evt) return TDC_E_UNSUPPORTED;

        size_t dst_size = (size_t)stage_in_sizes[i];
        int    nxt      = 1 - *cur;
        st = tdc_buf_reserve(&bufs[nxt], (dst_size > 0 ? dst_size : 1u) + 16u);
        if (st != TDC_OK) return st;

        st = evt->decode(stage_src, stage_src_size, bufs[nxt].data, dst_size);
        if (st != TDC_OK) return st;
        bufs[nxt].size = dst_size;

        stage_src      = bufs[nxt].data;
        stage_src_size = dst_size;
        *cur = nxt;
    }
    if (bufs[*cur].size != out_size) return TDC_E_CORRUPT;
    return TDC_OK;
}
