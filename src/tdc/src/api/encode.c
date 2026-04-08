/*
 * src/api/encode.c
 *
 * Implements: tdc_encode_block (declared in tdc/codec.h)
 *
 * Single-block encode pipeline driver:
 *
 *   1. Validate src block (tdc_block_validate).
 *   2. Look up model vtable from spec->model. Encode in -> residual
 *      stream + side_meta. Threads residual_dtype out.
 *   3. Walk spec->xform[] left to right. Each transform consumes the
 *      previous stage's output bytes and produces the next stage's
 *      input. The element dtype is updated as it changes (e.g.
 *      quantize f64 -> i16). Two ping-pong scratch buffers carry the
 *      data through the chain. As each transform runs, its caller-
 *      supplied params blob (if any) is appended to a TLV section so
 *      decode can recover it without consulting the in-memory spec.
 *   4. Look up entropy vtable from spec->entropy. Encode the final
 *      transform output into a payload buffer.
 *   5. Serialize a tdc_block_record into out, followed by side_meta,
 *      then xform_params (TLV), then the entropy payload, then the
 *      validity bitmap if HAS_VALIDITY is set.
 *
 * The driver uses scratch buffers from the caller-supplied tdc_buffer
 * realloc_fn — no internal mallocs.
 *
 * out semantics: out is the destination for ONE block record. On
 * success, out->size is set to the total record length (header +
 * side_meta + xform_params + payload + optional validity). The caller
 * stitches multiple blocks together at a higher level (containers).
 */

#include "tdc/codec.h"
#include "tdc/format.h"
#include "tdc/model.h"
#include "tdc/transform.h"
#include "tdc/entropy.h"
#include "tdc/types.h"

#include "../core/buffer.h"
#include "driver_internal.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Helpers ------------------------------------------------------------ */

/* Round n_elems up to whole bytes for the validity bitmap (1 bit per
 * element). The bitmap is laid out LSB-first within each byte; the
 * driver does not care about the bit order, it only forwards the
 * caller-supplied bytes verbatim. */
static size_t validity_bytes_for(int64_t n_elems) {
    if (n_elems <= 0) return 0;
    return (size_t)((n_elems + 7) / 8);
}

/* Compute n_elems = product of dim[0..rank-1]. Caller has already
 * passed the block through tdc_block_validate so dims are sane. */
static int64_t shape_n_elems(const tdc_shape *s) {
    int64_t n = 1;
    for (uint8_t i = 0; i < s->rank; ++i) n *= s->dim[i];
    return n;
}

/* ----- Entry point -------------------------------------------------------- */

tdc_status tdc_encode_block(const tdc_block      *src,
                            const tdc_codec_spec *spec,
                            tdc_buffer           *out) {
    if (!src || !spec || !out || !out->realloc_fn) return TDC_E_INVAL;

    tdc_status st = tdc_block_validate(src);
    if (st != TDC_OK) return st;

    /* Resolve model + entropy vtables up front. Transforms are looked
     * up lazily inside the chain loop because the chain may be empty. */
    const tdc_model_vt *model_vt = tdc_model_get(spec->model);
    if (!model_vt) return TDC_E_UNSUPPORTED;

    const tdc_entropy_vt *entropy_vt = tdc_entropy_get(spec->entropy);
    if (!entropy_vt) return TDC_E_UNSUPPORTED;

    /* ----- Stage 1: model encode --------------------------------------- */

    tdc_buffer bufs[2];
    tdc_buffer side_meta;
    tdc_buffer xform_params;
    tdc_buffer payload;
    driver_scratch_init(&bufs[0], out);
    driver_scratch_init(&bufs[1], out);
    driver_scratch_init(&side_meta, out);
    driver_scratch_init(&xform_params, out);
    driver_scratch_init(&payload, out);

    int        cur       = 0;
    tdc_dtype  cur_dtype = (tdc_dtype)0;

    st = model_vt->encode(src, spec->model_params,
                          &bufs[cur], &cur_dtype, &side_meta);
    if (st != TDC_OK) goto cleanup;

    /* ----- Stage 2: transform chain ------------------------------------ */

    for (int i = 0; i < TDC_MAX_TRANSFORMS; ++i) {
        tdc_xform_id xid = spec->xform[i];
        if (xid == TDC_XFORM_NONE) break;

        const tdc_xform_vt *xv = tdc_xform_get(xid);
        if (!xv) { st = TDC_E_UNSUPPORTED; goto cleanup; }

        /* Reset destination buffer's logical size before reuse. The
         * underlying allocation is preserved across iterations so the
         * second and later transforms reuse capacity from the first. */
        bufs[1 - cur].size = 0;

        tdc_dtype next_dtype = (tdc_dtype)0;
        st = xv->encode(bufs[cur].data, bufs[cur].size,
                        cur_dtype, spec->xform_params[i],
                        &bufs[1 - cur], &next_dtype);
        if (st != TDC_OK) goto cleanup;

        /* Append a TLV entry for this slot if the transform has params
         * the driver knows how to serialize. The serializer is owned by
         * driver_internal.h so encode/decode see exactly the same byte
         * layout. Slots without params (ZIGZAG, BYTE_SHUFFLE, ...) emit
         * nothing. */
        st = driver_xform_params_append(&xform_params, (uint16_t)i, xid,
                                        spec->xform_params[i]);
        if (st != TDC_OK) goto cleanup;

        cur       = 1 - cur;
        cur_dtype = next_dtype;
    }

    /* bufs[cur] now holds the residual stream that goes to entropy. */
    const size_t uncompressed_size = bufs[cur].size;

    /* ----- Stage 3: entropy encode ------------------------------------- */

    st = entropy_vt->encode(bufs[cur].data, uncompressed_size,
                            spec->entropy_params, &payload);
    if (st != TDC_OK) goto cleanup;

    /* ----- Stage 4: assemble block record ------------------------------ */

    int64_t n_elems = shape_n_elems(&src->shape);
    size_t  vbytes  = (src->validity != NULL) ? validity_bytes_for(n_elems) : 0;

    if (side_meta.size    > UINT32_MAX ||
        xform_params.size > UINT32_MAX ||
        payload.size      > UINT32_MAX ||
        vbytes            > UINT32_MAX) {
        st = TDC_E_INVAL; goto cleanup;
    }
    if (uncompressed_size > UINT64_MAX) { /* always false; documents intent */
        st = TDC_E_INVAL; goto cleanup;
    }

    size_t total = (size_t)TDC_BLOCK_HEADER_SIZE
                 + side_meta.size + xform_params.size + payload.size + vbytes;
    st = tdc_buf_reserve(out, total);
    if (st != TDC_OK) goto cleanup;

    tdc_block_record hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic      = TDC_BLOCK_MAGIC;
    hdr.version    = TDC_BLOCK_VERSION;
    hdr.flags      = 0;
    if (src->validity != NULL) hdr.flags |= TDC_BLOCK_FLAG_HAS_VALIDITY;
    hdr.model_id   = (uint16_t)spec->model;
    for (int i = 0; i < TDC_MAX_TRANSFORMS; ++i) {
        hdr.xform_ids[i] = (uint16_t)spec->xform[i];
    }
    hdr.entropy_id        = (uint16_t)spec->entropy;
    hdr.dtype             = (uint8_t)src->dtype;
    hdr.layout            = (uint8_t)src->layout;
    hdr.rank              = src->shape.rank;
    for (uint8_t i = 0; i < TDC_MAX_RANK; ++i) {
        hdr.dim[i] = (i < src->shape.rank) ? src->shape.dim[i] : 0;
    }
    hdr.uncompressed_size = (uint64_t)uncompressed_size;
    hdr.side_meta_size    = (uint32_t)side_meta.size;
    hdr.payload_size      = (uint32_t)payload.size;
    hdr.xform_params_size = (uint32_t)xform_params.size;
    hdr.validity_size     = (uint32_t)vbytes;

    /* Mark the block lossy if any transform in the spec is lossy. The
     * read side does not consume this flag in v0, but it makes the
     * record honest about whether bit-exact recovery is possible. */
    for (int i = 0; i < TDC_MAX_TRANSFORMS; ++i) {
        tdc_xform_id xid = spec->xform[i];
        if (xid == TDC_XFORM_NONE) break;
        const tdc_xform_vt *xv = tdc_xform_get(xid);
        if (xv && xv->is_lossy) { hdr.flags |= TDC_BLOCK_FLAG_LOSSY; break; }
    }

    /* Memcpy the struct out byte-for-byte. The frozen header guarantees
     * the C struct packs to exactly 80 bytes on every supported target,
     * matching the documented little-endian on-disk layout one-to-one,
     * so a single memcpy is correct without per-field byte twiddling. */
    memcpy(out->data, &hdr, TDC_BLOCK_HEADER_SIZE);

    size_t off = TDC_BLOCK_HEADER_SIZE;
    if (side_meta.size > 0) {
        memcpy(out->data + off, side_meta.data, side_meta.size);
    }
    off += side_meta.size;
    if (xform_params.size > 0) {
        memcpy(out->data + off, xform_params.data, xform_params.size);
    }
    off += xform_params.size;
    if (payload.size > 0) {
        memcpy(out->data + off, payload.data, payload.size);
    }
    off += payload.size;
    if (vbytes > 0) {
        memcpy(out->data + off, src->validity, vbytes);
    }
    out->size = total;

    st = TDC_OK;

cleanup:
    driver_scratch_free(&bufs[0]);
    driver_scratch_free(&bufs[1]);
    driver_scratch_free(&side_meta);
    driver_scratch_free(&xform_params);
    driver_scratch_free(&payload);
    return st;
}
