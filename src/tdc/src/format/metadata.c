/*
 * src/format/metadata.c
 *
 * Side-metadata blob writer helpers.
 *
 * Side metadata is a FIRST-CLASS section of the block record (lives
 * between the record header and the entropy payload). The byte-level
 * primitives — load/store u16/u32/u64, bounds-checked reader cursor —
 * are static inlines in metadata_internal.h so each translation unit
 * gets its own copy with no link cost.
 *
 * The growable writer functions live here (not inlined) for two reasons:
 *
 *   1. They each call tdc_buf_reserve, which is itself the doubling-
 *      growth policy. Inlining the writer in eight different .c files
 *      would mean eight independent inlines of "reserve, store, advance",
 *      which is exactly the duplication this file is meant to retire.
 *
 *   2. The non-inline boundary is the natural place to keep the
 *      "tdc_buffer growth policy" out of the per-stage source files.
 *      Models that emit side metadata get a one-call API
 *      (tdc_meta_write_u32(out, n_tiles)) instead of an open-coded
 *      reserve/store/advance triple.
 *
 * Consumers: model side-meta serializers (plane2d coefficients, dict
 * dictionary header, future stack2d frame table). The transform-params
 * TLV writer in src/api/driver_internal.h predates this header and uses
 * its own minimal append because it lives on the encode hot path; it
 * could move here in a later cleanup.
 */

#include "tdc/format.h"
#include "metadata_internal.h"
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* All fixed-width writers share the same shape: reserve → store → advance.
 * The macro generates each width without duplicating the control flow. */

#define DEFINE_META_WRITE(SUFFIX, TYPE, WIDTH, STORE_EXPR)           \
tdc_status tdc_meta_write_##SUFFIX(tdc_buffer *out, TYPE v) {        \
    if (!out || !out->realloc_fn) return TDC_E_INVAL;                \
    size_t need = out->size + (WIDTH);                               \
    tdc_status st = tdc_buf_reserve(out, need);                      \
    if (st != TDC_OK) return st;                                     \
    STORE_EXPR;                                                      \
    out->size = need;                                                \
    return TDC_OK;                                                   \
}

DEFINE_META_WRITE(u8,  uint8_t,  1u, out->data[out->size] = v)
DEFINE_META_WRITE(u16, uint16_t, 2u, tdc_le_store_u16(out->data + out->size, v))
DEFINE_META_WRITE(u32, uint32_t, 4u, tdc_le_store_u32(out->data + out->size, v))
DEFINE_META_WRITE(u64, uint64_t, 8u, tdc_le_store_u64(out->data + out->size, v))

#undef DEFINE_META_WRITE

tdc_status tdc_meta_write_bytes(tdc_buffer *out, const void *src, size_t n) {
    if (!out || !out->realloc_fn) return TDC_E_INVAL;
    if (n == 0u) return TDC_OK;
    if (!src) return TDC_E_INVAL;
    size_t need = out->size + n;
    if (need < out->size) return TDC_E_NOMEM; /* size_t overflow */
    tdc_status st = tdc_buf_reserve(out, need);
    if (st != TDC_OK) return st;
    memcpy(out->data + out->size, src, n);
    out->size = need;
    return TDC_OK;
}
