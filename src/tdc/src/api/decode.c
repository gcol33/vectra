/*
 * src/api/decode.c
 *
 * Implements: tdc_decode_block (declared in tdc/codec.h)
 *
 * Single-block decode pipeline driver:
 *
 *   1. Memcpy the 80-byte block record header out of src and validate
 *      it (tdc_block_record_validate).
 *   2. Cross-check dst dtype/layout/shape against the header. The
 *      caller is responsible for sizing dst->data; we never reallocate
 *      caller-owned data.
 *   3. Parse the TLV xform_params section into a per-slot params table.
 *   4. Look up entropy + model vtables.
 *   5. Forward-walk the transform chain once to compute each
 *      transform's encoder-side input dtype, threading the per-slot
 *      params pointer for transforms whose output width depends on
 *      params (QUANTIZE).
 *   6. Entropy.decode the payload into a scratch buffer sized to the
 *      worst-case stage byte count.
 *   7. Walk the transform chain in reverse, ping-ponging between two
 *      scratch buffers. Each step's dst byte count is the encoder-side
 *      input byte count of that stage.
 *   8. Call model.decode with the post-chain bytes, the side metadata
 *      pointer, and dst->dtype as residual_dtype (every v0 model emits
 *      residual_dtype == in->dtype).
 *
 * Validity bitmap: in v0, dst->validity is `const uint8_t *` so the
 * driver cannot return reconstructed validity bytes through the block.
 * The encoder writes the bitmap to disk (validity_size in the header is
 * authoritative); the decoder validates that the byte count is consistent
 * but does not surface the bytes. A future API extension can hand them
 * back without another header bump.
 *
 * Hot-path constraint: every stage knows its dst_size up front, so the
 * decode loop never grows a buffer past its first reservation. This is
 * what the entropy.decode contract guarantees.
 *
 * Allocator note: tdc_decode_block has no tdc_buffer argument and
 * therefore no caller-supplied realloc_fn. The driver wraps the C
 * runtime in a small shim so the rest of the pipeline still allocates
 * via the documented mechanism. This is the only place in tdc that
 * calls realloc/free directly, and it exists because the public
 * decode API in v0 does not pass an allocator. Lifting this requires
 * a (frozen) signature change to tdc_decode_block — tracked in
 * PORTING.md.
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
#include <stdlib.h>
#include <string.h>

/* ----- Decode-side allocator shim ----------------------------------------- */

static void *driver_libc_realloc(void *user, void *ptr, size_t new_size) {
    (void)user;
    if (new_size == 0) { free(ptr); return NULL; }
    return realloc(ptr, new_size);
}

static tdc_buffer driver_make_scratch_parent(void) {
    tdc_buffer b = {0};
    b.realloc_fn = driver_libc_realloc;
    return b;
}

/* ----- Entry point -------------------------------------------------------- */

tdc_status tdc_decode_block(const uint8_t *src, size_t src_size,
                            tdc_block     *dst) {
    if (!src || !dst) return TDC_E_INVAL;
    if (src_size < TDC_BLOCK_HEADER_SIZE) return TDC_E_CORRUPT;

    /* Pull the header out by memcpy (no aliasing assumption on src
     * alignment). The struct layout matches the on-disk byte order
     * one-to-one on every supported little-endian target. */
    tdc_block_record hdr;
    memcpy(&hdr, src, TDC_BLOCK_HEADER_SIZE);

    tdc_status st = tdc_block_record_validate(&hdr);
    if (st != TDC_OK) return st;

    /* Bounds: header + side_meta + xform_params + payload + validity
     * must fit inside src_size. */
    int64_t n_elems = 1;
    for (uint8_t i = 0; i < hdr.rank; ++i) n_elems *= hdr.dim[i];

    /* Validity byte count must match the header field exactly. The
     * validator already enforced flag<->size agreement, but the size
     * itself must match the bitmap shape derived from n_elems. */
    size_t expected_vbytes = (n_elems > 0) ? (size_t)((n_elems + 7) / 8) : 0u;
    if (hdr.flags & TDC_BLOCK_FLAG_HAS_VALIDITY) {
        if ((size_t)hdr.validity_size != expected_vbytes) return TDC_E_CORRUPT;
    } else {
        if (hdr.validity_size != 0u) return TDC_E_CORRUPT;
    }
    size_t vbytes = (size_t)hdr.validity_size;

    size_t total = (size_t)TDC_BLOCK_HEADER_SIZE
                 + hdr.side_meta_size + hdr.xform_params_size
                 + hdr.payload_size + vbytes;
    if (total > src_size) return TDC_E_CORRUPT;

    /* Cross-check dst against header. dst->shape must already be set
     * by the caller; we refuse to silently rewrite it. */
    if (dst->dtype  != (tdc_dtype)hdr.dtype)   return TDC_E_DTYPE;
    if (dst->layout != (tdc_layout)hdr.layout) return TDC_E_LAYOUT;
    if (dst->shape.rank != hdr.rank)           return TDC_E_SHAPE;
    for (uint8_t i = 0; i < hdr.rank; ++i) {
        if (dst->shape.dim[i] != hdr.dim[i]) return TDC_E_SHAPE;
    }

    /* dst->data may be NULL only if the block is empty. */
    if (n_elems > 0 && dst->data == NULL) return TDC_E_INVAL;

    /* Resolve vtables. */
    const tdc_model_vt *model_vt = tdc_model_get((tdc_model_id)hdr.model_id);
    if (!model_vt) return TDC_E_UNSUPPORTED;

    const tdc_entropy_vt *entropy_vt = tdc_entropy_get((tdc_entropy_id)hdr.entropy_id);
    if (!entropy_vt) return TDC_E_UNSUPPORTED;

    /* Source pointers into the record. */
    const uint8_t *side_meta_p    = src + TDC_BLOCK_HEADER_SIZE;
    const uint8_t *xform_params_p = side_meta_p + hdr.side_meta_size;
    const uint8_t *payload_p      = xform_params_p + hdr.xform_params_size;

    /* ----- Stage 1a: parse TLV xform params ---------------------------- */

    driver_xform_params_table xparams;
    st = driver_xform_params_parse(&xparams, hdr.xform_ids,
                                   xform_params_p,
                                   (size_t)hdr.xform_params_size);
    if (st != TDC_OK) return st;

    /* ----- Stage 1b: forward dtype walk for the transform chain -------- */
    /*
     * For each non-NONE transform we record the encoder-side input dtype
     * AND the encoder-side input byte count of the buffer entering that
     * stage. Every v0 model emits residual_dtype == in->dtype, so the
     * chain walk starts at dst->dtype with byte count = n_elems *
     * sizeof(dst->dtype).
     */
    /* Seed the chain walk with the model's residual dtype, not the
     * block's user-facing dtype. For most v0 models the two are equal,
     * but DICT_1D emits u32 indices from a STRING block, and the
     * downstream transforms (and the entropy stage's uncompressed byte
     * count) all see the residual dtype, not the block dtype. */
    tdc_dtype residual_dtype = driver_model_residual_dtype(
        (tdc_model_id)hdr.model_id, dst->dtype);
    size_t    residual_elem_size = tdc_dtype_size(residual_dtype);
    if (residual_elem_size == 0) return TDC_E_UNSUPPORTED;

    int       chain_len = 0;
    tdc_dtype xform_in[TDC_MAX_TRANSFORMS];
    size_t    xform_in_bytes[TDC_MAX_TRANSFORMS];
    tdc_dtype walk = residual_dtype;
    size_t    walk_bytes = (size_t)n_elems * residual_elem_size;
    for (int i = 0; i < TDC_MAX_TRANSFORMS; ++i) {
        tdc_xform_id xid = (tdc_xform_id)hdr.xform_ids[i];
        if (xid == TDC_XFORM_NONE) break;
        if (!tdc_xform_get(xid)) return TDC_E_UNSUPPORTED;
        xform_in[i]       = walk;
        xform_in_bytes[i] = walk_bytes;
        tdc_dtype next = driver_xform_out_dtype(xid, walk, xparams.xform_params[i]);
        if (next == (tdc_dtype)0) return TDC_E_UNSUPPORTED;
        size_t next_elem_size = tdc_dtype_size(next);
        if (next_elem_size == 0) return TDC_E_UNSUPPORTED;
        walk       = next;
        walk_bytes = (size_t)n_elems * next_elem_size;
        chain_len  = i + 1;
    }

    /* The post-chain (entropy-side) byte count must match the header's
     * uncompressed_size, which is the byte count just before entropy
     * encoding. */
    if ((uint64_t)walk_bytes != hdr.uncompressed_size) return TDC_E_CORRUPT;

    /* ----- Stage 2: scratch buffers + entropy decode ------------------ */

    tdc_buffer parent = driver_make_scratch_parent();
    tdc_buffer bufs[2];
    driver_scratch_init(&bufs[0], &parent);
    driver_scratch_init(&bufs[1], &parent);

    int cur = 0;

    if (hdr.uncompressed_size > SIZE_MAX) { st = TDC_E_INVAL; goto cleanup; }
    size_t entropy_out_size = (size_t)hdr.uncompressed_size;

    /* Reserve at least 1 byte even for empty inputs so bufs[cur].data
     * is non-NULL — the entropy decoders short-circuit on size 0 but
     * the buffer still needs to exist for the chain plumbing below. */
    st = tdc_buf_reserve(&bufs[cur], entropy_out_size > 0 ? entropy_out_size : 1u);
    if (st != TDC_OK) goto cleanup;

    st = entropy_vt->decode(payload_p, hdr.payload_size,
                            bufs[cur].data, entropy_out_size);
    if (st != TDC_OK) goto cleanup;
    bufs[cur].size = entropy_out_size;

    /* ----- Stage 3: reverse the transform chain ----------------------- */
    /*
     * Stage `i` decodes from its encoder-OUTPUT byte count (which is the
     * encoder-INPUT byte count of stage i+1, or entropy_out_size for the
     * last stage) into its encoder-INPUT byte count xform_in_bytes[i].
     * QUANTIZE is the only v0 transform that changes byte count;
     * everything else has equal in/out sizes.
     */
    for (int i = chain_len - 1; i >= 0; --i) {
        tdc_xform_id xid = (tdc_xform_id)hdr.xform_ids[i];
        const tdc_xform_vt *xv = tdc_xform_get(xid);
        if (!xv) { st = TDC_E_UNSUPPORTED; goto cleanup; }

        size_t dst_bytes = xform_in_bytes[i];

        st = tdc_buf_reserve(&bufs[1 - cur], dst_bytes > 0 ? dst_bytes : 1u);
        if (st != TDC_OK) goto cleanup;

        tdc_dtype out_dtype = (tdc_dtype)0;
        st = xv->decode(bufs[cur].data, bufs[cur].size,
                        xform_in[i], xparams.xform_params[i],
                        bufs[1 - cur].data, dst_bytes,
                        &out_dtype);
        if (st != TDC_OK) goto cleanup;

        bufs[1 - cur].size = dst_bytes;
        cur = 1 - cur;
    }

    /* ----- Stage 4: model decode -------------------------------------- */

    st = model_vt->decode(dst, NULL, residual_dtype,
                          bufs[cur].data, bufs[cur].size,
                          (hdr.side_meta_size > 0) ? side_meta_p : NULL,
                          hdr.side_meta_size);
    if (st != TDC_OK) goto cleanup;

    st = TDC_OK;

cleanup:
    driver_scratch_free(&bufs[0]);
    driver_scratch_free(&bufs[1]);
    return st;
}
