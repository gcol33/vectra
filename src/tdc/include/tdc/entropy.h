/*
 * tdc/entropy.h — frozen v0
 *
 * Entropy coder vtable.
 *
 * Entropy coders are dimension-agnostic AND dtype-agnostic. They see only
 * a flat byte stream. By the time data reaches an entropy coder, the model
 * and the transform chain have already done all the work that depends on
 * structure: the input here is just bytes that should compress well.
 *
 * v0 backends:
 *   TDC_ENTROPY_NONE     memcpy passthrough
 *   TDC_ENTROPY_LZ2      native LZ77, separated-stream, 64K window
 *   TDC_ENTROPY_DEFLATE  zlib deflate (link-time optional, "ratio" mode)
 *
 * Decode is called with a known dst_size. Entropy coders MUST NOT need to
 * grow their output buffer at decode time — the block record carries the
 * exact uncompressed size. This keeps the decode hot path branch-free.
 */

#ifndef TDC_ENTROPY_H
#define TDC_ENTROPY_H

#include "types.h"
#include "codec.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tdc_entropy_vt {
    tdc_entropy_id id;
    const char    *name;

    /* Worst-case encoded size for src_size input bytes. */
    size_t (*encode_bound)(size_t src_size);

    /*
     * Forward. dst is grown via realloc_fn up to encode_bound(src_size).
     * On success, dst->size is set to the actual encoded length.
     */
    tdc_status (*encode)(const uint8_t *src, size_t src_size,
                         const void    *params,  /* tdc_entropy_level or NULL */
                         tdc_buffer    *dst);

    /*
     * Inverse. dst is caller-provided, exactly dst_size bytes.
     * On success, dst is filled and the function returns TDC_OK.
     */
    tdc_status (*decode)(const uint8_t *src, size_t src_size,
                         uint8_t       *dst, size_t dst_size);
} tdc_entropy_vt;

const tdc_entropy_vt *tdc_entropy_get(tdc_entropy_id id);

#ifdef __cplusplus
}
#endif
#endif /* TDC_ENTROPY_H */
