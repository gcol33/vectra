/*
 * src/api/decode_into.c
 *
 * Implements: tdc_decode_block_into and tdc_decode_peek
 *             (declared in tdc/codec.h)
 *
 * Zero-allocation decode entry point: the caller pre-sizes dst->data and
 * pre-populates dst->dtype / dst->layout / dst->shape to match the record
 * header. No tdc_buffer argument — internal ping-pong scratch is sourced
 * from the libc allocator via driver_make_libc_scratch_parent(), identical
 * to tdc_decode_block.
 *
 * Why a separate entry point and not a flag on tdc_decode_block? Because
 * tdc_decode_block's contract already says the caller pre-sizes dst->data.
 * The _into variant adds two differences:
 *
 *   1. The caller needs the record's sizing information BEFORE calling
 *      decode — otherwise they cannot pre-allocate. tdc_decode_peek
 *      walks only the 80-byte block header and returns meta fields; it
 *      is the intended companion to _into.
 *
 *   2. _into can be strict about dst->data being non-NULL: a NULL
 *      dst->data for a non-empty block returns TDC_E_INVAL immediately
 *      (driver_decode_block_impl enforces the same rule, so this wrapper
 *      defers to the shared pipeline — no re-validation needed).
 *
 * The "zero-copy" label is about the destination: dst->data is written
 * in place. The pipeline still needs two small ping-pong scratch buffers
 * for entropy + transform stages; those come from libc and are freed
 * before return.
 */

#ifdef _MSC_VER
#  define _CRT_SECURE_NO_WARNINGS 1
#endif

#include "tdc/codec.h"
#include "tdc/format.h"
#include "tdc/types.h"

#include "driver_internal.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Peek --------------------------------------------------------------- */

tdc_status tdc_decode_peek(const uint8_t *src, size_t src_size,
                           tdc_block *out_meta,
                           size_t    *out_bytes_required) {
    if (!src || !out_meta) return TDC_E_INVAL;
    if (src_size < TDC_BLOCK_HEADER_SIZE) return TDC_E_CORRUPT;

    tdc_block_record hdr;
    memcpy(&hdr, src, TDC_BLOCK_HEADER_SIZE);

    tdc_status st = tdc_block_record_validate(&hdr);
    if (st != TDC_OK) return st;

    /* Populate the meta fields the caller needs to pre-size dst->data. */
    memset(out_meta, 0, sizeof(*out_meta));
    out_meta->dtype      = (tdc_dtype)hdr.dtype;
    out_meta->layout     = (tdc_layout)hdr.layout;
    out_meta->shape.rank = hdr.rank;
    for (uint8_t i = 0; i < hdr.rank; ++i) {
        out_meta->shape.dim[i] = hdr.dim[i];
    }
    tdc_shape_set_contiguous(&out_meta->shape);

    if (out_bytes_required) {
        /* Fixed-width dtypes only in v0; TDC_DT_STRING would need a
         * separate byte-heap count and is rejected below via dtype_size==0. */
        size_t elem = tdc_dtype_size((tdc_dtype)hdr.dtype);
        if (elem == 0) {
            /* Variable-length dtype or unknown id — we cannot compute a
             * required byte count from the header alone. Let the caller
             * fall back to tdc_decode_block for these. */
            return TDC_E_UNSUPPORTED;
        }
        int64_t n = 1;
        for (uint8_t i = 0; i < hdr.rank; ++i) n *= hdr.dim[i];
        *out_bytes_required = (n > 0) ? (size_t)n * elem : 0u;
    }
    return TDC_OK;
}

/* ----- Into --------------------------------------------------------------- */

tdc_status tdc_decode_block_into(const uint8_t *src, size_t src_size,
                                 tdc_block *dst) {
    /* driver_decode_block_impl already enforces:
     *   - dst->dtype / layout / shape match the record (TDC_E_DTYPE /
     *     TDC_E_LAYOUT / TDC_E_SHAPE);
     *   - dst->data non-NULL for n_elems > 0 (TDC_E_INVAL);
     *   - src bounds / header magic / version (TDC_E_CORRUPT / _VERSION).
     *
     * No extra pre-flight is needed; this wrapper exists to pin the
     * libc-backed scratch parent and to document the zero-alloc contract
     * to the caller. */
    tdc_buffer scratch = driver_make_libc_scratch_parent();
    return driver_decode_block_impl(src, src_size, dst, &scratch, "decin",
                                    NULL, NULL, 1);
}
