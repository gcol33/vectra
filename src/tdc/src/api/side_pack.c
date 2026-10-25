/*
 * src/api/side_pack.c
 *
 * Implements: driver_side_packs, driver_side_pack, driver_side_unpack
 * (declared in driver_internal.h).
 *
 * A model hands the driver its side metadata in the form its decoder reads
 * (for DICT_1D: a u32 offsets table, so dictionary entries are reachable by
 * index). That form is not always the one that compresses. The current
 * format stores some of it packed, and the driver converts at the boundary,
 * so the models and every consumer of decoded side metadata keep one layout.
 *
 * DICT_1D
 *
 *   in memory:  u32 count, u32 total, u32 offsets[count + 1], u8 heap[total]
 *   on disk:    u32 count, LEB128 length[count], u8 heap[total]
 *
 * The absolute offsets of a dictionary of distinct names form a ramp that
 * no LZ coder matches; the lengths are small and repetitive. total is
 * implied by the heap that remains after the lengths.
 */

#include "tdc/codec.h"
#include "tdc/types.h"

#include "../core/buffer.h"
#include "driver_internal.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

int driver_side_packs(tdc_model_id model) {
    return model == TDC_MODEL_DICT_1D;
}

static size_t leb128_put(uint8_t *p, uint32_t v) {
    size_t n = 0;
    while (v >= 0x80u) { p[n++] = (uint8_t)(v | 0x80u); v >>= 7; }
    p[n++] = (uint8_t)v;
    return n;
}

/* Reads one LEB128 u32 from [p, end). Returns bytes consumed, 0 on a
 * truncated or over-long encoding. */
static size_t leb128_get(const uint8_t *p, const uint8_t *end, uint32_t *v) {
    uint32_t r = 0;
    for (size_t n = 0; n < 5u && p + n < end; ++n) {
        uint8_t b = p[n];
        if (n == 4u && b > 0x0Fu) return 0;
        r |= (uint32_t)(b & 0x7Fu) << (7u * n);
        if (!(b & 0x80u)) { *v = r; return n + 1u; }
    }
    return 0;
}

tdc_status driver_side_pack(tdc_model_id model,
                            const uint8_t *side, size_t side_size,
                            tdc_buffer *out) {
    if (!out || (side_size > 0 && !side)) return TDC_E_INVAL;
    if (model != TDC_MODEL_DICT_1D) return TDC_E_UNSUPPORTED;
    if (side_size < 12u) return TDC_E_CORRUPT;

    uint32_t count, total;
    memcpy(&count, side + 0, 4u);
    memcpy(&total, side + 4, 4u);
    uint64_t want = 8u + ((uint64_t)count + 1u) * 4u + (uint64_t)total;
    if ((uint64_t)side_size != want) return TDC_E_CORRUPT;

    const uint8_t *off_p = side + 8u;
    const uint8_t *heap  = off_p + ((size_t)count + 1u) * 4u;

    tdc_status st = tdc_buf_reserve(out, 4u + (size_t)count * 5u + (size_t)total);
    if (st != TDC_OK) return st;
    uint8_t *w = out->data;
    memcpy(w, &count, 4u);
    w += 4;
    uint32_t prev;
    memcpy(&prev, off_p, 4u);
    if (prev != 0u) return TDC_E_CORRUPT;
    for (uint32_t d = 0; d < count; ++d) {
        uint32_t next;
        memcpy(&next, off_p + ((size_t)d + 1u) * 4u, 4u);
        if (next < prev || next > total) return TDC_E_CORRUPT;
        w += leb128_put(w, next - prev);
        prev = next;
    }
    if (prev != total) return TDC_E_CORRUPT;
    if (total > 0u) memcpy(w, heap, total);
    w += total;
    out->size = (size_t)(w - out->data);
    return TDC_OK;
}

tdc_status driver_side_unpack(tdc_model_id model,
                              const uint8_t *packed, size_t packed_size,
                              tdc_buffer *out) {
    if (!out || (packed_size > 0 && !packed)) return TDC_E_INVAL;
    if (model != TDC_MODEL_DICT_1D) return TDC_E_UNSUPPORTED;
    if (packed_size < 4u) return TDC_E_CORRUPT;

    uint32_t count;
    memcpy(&count, packed, 4u);
    const uint8_t *p   = packed + 4u;
    const uint8_t *end = packed + packed_size;
    /* Every length takes at least one byte, so a count beyond the bytes
     * left is corrupt; checking it first bounds the offsets allocation. */
    if ((uint64_t)count > (uint64_t)(end - p)) return TDC_E_CORRUPT;

    size_t off_bytes = ((size_t)count + 1u) * 4u;
    tdc_status st = tdc_buf_reserve(out, 8u + off_bytes);
    if (st != TDC_OK) return st;
    uint8_t *off_w = out->data + 8u;

    uint64_t total = 0u;
    uint32_t zero = 0u;
    memcpy(off_w, &zero, 4u);
    for (uint32_t d = 0; d < count; ++d) {
        uint32_t len;
        size_t used = leb128_get(p, end, &len);
        if (used == 0) return TDC_E_CORRUPT;
        p += used;
        total += len;
        if (total > UINT32_MAX) return TDC_E_CORRUPT;
        uint32_t o = (uint32_t)total;
        memcpy(off_w + ((size_t)d + 1u) * 4u, &o, 4u);
    }
    if ((uint64_t)(end - p) != total) return TDC_E_CORRUPT;

    st = tdc_buf_reserve(out, 8u + off_bytes + (size_t)total);
    if (st != TDC_OK) return st;
    uint32_t total32 = (uint32_t)total;
    memcpy(out->data + 0, &count, 4u);
    memcpy(out->data + 4, &total32, 4u);
    if (total > 0u) memcpy(out->data + 8u + off_bytes, p, (size_t)total);
    out->size = 8u + off_bytes + (size_t)total;
    return TDC_OK;
}
