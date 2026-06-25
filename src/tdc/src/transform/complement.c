/*
 * src/transform/complement.c
 *
 * TDC_XFORM_COMPLEMENT — bitwise-NOT of every input byte. Self-inverting
 * (encode and decode share the same kernel). Dtype-agnostic: the transform
 * never looks at element width.
 *
 * This backend ships as a reference example for the "Extending" vignette.
 * It reserves one id in the experimental range (0x0100-0x01FF) so that
 * changing it later never collides with a core backend. Documentation and
 * tests use it to show the minimum surface area a new transform needs.
 *
 * Properties:
 *   accepted_dtypes = every numeric dtype (fixed-width)
 *   is_lossy        = 0
 *   can_inplace     = 1 (per-byte kernel)
 *
 * Encode output size always equals input size. Empty input returns
 * TDC_OK with dst->size = 0 and no allocation.
 */

#include "tdc/transform.h"
#include "transform_internal.h"
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>

/* Every fixed-width numeric dtype known to tdc v0. STRING is variable
 * width and never reaches a transform stage — omit it here so the
 * dispatcher rejects it with TDC_E_DTYPE before we touch any bytes. */
#define COMPLEMENT_BIT(dt) (1u << (uint32_t)(dt))
#define COMPLEMENT_ACCEPTED_DTYPES ( \
    COMPLEMENT_BIT(TDC_DT_I8)  | COMPLEMENT_BIT(TDC_DT_I16) | \
    COMPLEMENT_BIT(TDC_DT_I32) | COMPLEMENT_BIT(TDC_DT_I64) | \
    COMPLEMENT_BIT(TDC_DT_U8)  | COMPLEMENT_BIT(TDC_DT_U16) | \
    COMPLEMENT_BIT(TDC_DT_U32) | COMPLEMENT_BIT(TDC_DT_U64) | \
    COMPLEMENT_BIT(TDC_DT_F16) | COMPLEMENT_BIT(TDC_DT_F32) | \
    COMPLEMENT_BIT(TDC_DT_F64))

static int complement_dtype_ok(tdc_dtype dt) {
    return (COMPLEMENT_ACCEPTED_DTYPES & (1u << (uint32_t)dt)) != 0u;
}

static tdc_status complement_encode(const uint8_t *src, size_t src_size,
                                    tdc_dtype      in_dtype,
                                    const void    *params,
                                    tdc_buffer    *dst,
                                    tdc_dtype     *out_dtype) {
    (void)params;
    if (!dst || !dst->realloc_fn) return TDC_E_INVAL;
    if (src_size > 0 && !src)     return TDC_E_INVAL;
    if (!complement_dtype_ok(in_dtype)) return TDC_E_DTYPE;

    tdc_status st = tdc_buf_reserve(dst, src_size);
    if (st != TDC_OK) return st;

    if (out_dtype) *out_dtype = in_dtype;

    for (size_t i = 0; i < src_size; ++i) dst->data[i] = (uint8_t)~src[i];
    dst->size = src_size;
    return TDC_OK;
}

static tdc_status complement_decode(const uint8_t *src, size_t src_size,
                                    tdc_dtype      in_dtype,
                                    const void    *params,
                                    uint8_t       *dst, size_t dst_size,
                                    tdc_dtype     *out_dtype) {
    (void)params;
    if (src_size > 0 && !src) return TDC_E_INVAL;
    if (dst_size > 0 && !dst) return TDC_E_INVAL;
    if (!complement_dtype_ok(in_dtype)) return TDC_E_DTYPE;
    if (dst_size != src_size)           return TDC_E_CORRUPT;

    if (out_dtype) *out_dtype = in_dtype;
    for (size_t i = 0; i < src_size; ++i) dst[i] = (uint8_t)~src[i];
    return TDC_OK;
}

const tdc_xform_vt tdc_xform_complement_vt = {
    .id              = TDC_XFORM_COMPLEMENT,
    .name            = "complement",
    .accepted_dtypes = COMPLEMENT_ACCEPTED_DTYPES,
    .can_inplace     = 1,
    .is_lossy        = 0,
    .encode          = complement_encode,
    .decode          = complement_decode,
};
