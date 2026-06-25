/*
 * tdc/transform.h — frozen v0
 *
 * Transform (representation) vtable.
 *
 * Transforms are dimension-agnostic. They take a flat typed buffer and
 * produce another flat typed buffer. They never look at layout, neighborhood,
 * or shape — only at element count and dtype.
 *
 * The transform stage owns SYMBOLIZATION. Zigzag, byte/bit shuffle, packing,
 * stream splitting, run-length encoding of small alphabets — all of those
 * live here. The pipeline therefore has only three logical phases: model,
 * representation (= transform chain), entropy. There is no separate
 * "symbolize" phase.
 *
 * Transform chains run left to right on encode and right to left on decode.
 * Each transform may change the dtype of its output (e.g. quantize converts
 * f64 to i16). The dtype is threaded through the chain in tdc_codec_spec.
 */

#ifndef TDC_TRANSFORM_H
#define TDC_TRANSFORM_H

#include "types.h"
#include "codec.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tdc_xform_vt {
    tdc_xform_id id;
    const char  *name;

    uint32_t accepted_dtypes;   /* bitmask over tdc_dtype (input dtype) */
    int      can_inplace;       /* 1 if encode/decode may share src and dst */
    int      is_lossy;          /* 1 if information is discarded (quantize) */

    /*
     * Forward.
     *
     *   src, src_size   input bytes
     *   in_dtype        dtype of the input element stream
     *   params          transform-specific params struct, may be NULL
     *   dst             grown via realloc_fn; receives output bytes
     *   out_dtype       dtype of the output element stream (may differ)
     */
    tdc_status (*encode)(const uint8_t *src, size_t src_size,
                         tdc_dtype      in_dtype,
                         const void    *params,
                         tdc_buffer    *dst,
                         tdc_dtype     *out_dtype);

    /*
     * Inverse. dst must be pre-sized; on success it is filled exactly.
     * dst_size is required so the inverse can avoid trial decompression.
     */
    tdc_status (*decode)(const uint8_t *src, size_t src_size,
                         tdc_dtype      in_dtype,
                         const void    *params,
                         uint8_t       *dst, size_t dst_size,
                         tdc_dtype     *out_dtype);
} tdc_xform_vt;

const tdc_xform_vt *tdc_xform_get(tdc_xform_id id);

#ifdef __cplusplus
}
#endif
#endif /* TDC_TRANSFORM_H */
