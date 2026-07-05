/*
 * src/api/decode_impl.c
 *
 * Shared single-block decode pipeline. Single source of truth for every
 * public decode entry point:
 *
 *   tdc_decode_block      (src/api/decode.c)       — libc-backed scratch.
 *   tdc_decode_block_ex   (src/api/decode_ex.c)    — caller-supplied allocator.
 *   tdc_decode_block_into (src/api/decode_into.c)  — zero-alloc into caller's
 *                                                    pre-sized dst->data.
 *
 * The three wrappers differ only in how they source the ping-pong scratch
 * allocator and (for _into) in their dst->data validation strictness. The
 * pipeline body lives here exactly once.
 *
 * History: decode.c and decode_ex.c were nearly-identical copies until
 * tdc_decode_block_into required a third variant. Per project rule
 * "No copy-paste across specialized functions", the pipeline was extracted
 * before growing a third sibling. As a side effect this fixes two latent
 * drifts that had developed between the old decode.c and decode_ex.c:
 *   - decode_ex.c was missing TDC_BLOCK_FLAG_ZERO_RESIDUAL handling;
 *   - decode_ex.c was missing the +16 wildcopy slack on scratch buffers.
 * Both bugs are gone now that there is a single implementation.
 */

#ifdef _MSC_VER
#  define _CRT_SECURE_NO_WARNINGS 1
#endif

#include "tdc/codec.h"
#include "tdc/format.h"
#include "tdc/model.h"
#include "tdc/transform.h"
#include "tdc/entropy.h"
#include "tdc/types.h"

#include "../core/buffer.h"
#include "../core/log.h"
#include "../core/timer.h"
#include "driver_internal.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Shared pipeline ---------------------------------------------------- */

tdc_status driver_decode_block_impl(const uint8_t        *src,
                                    size_t                src_size,
                                    tdc_block            *dst,
                                    const tdc_buffer     *scratch_parent,
                                    const char           *timer_tag,
                                    driver_pre_model_hook hook,
                                    void                 *hook_user,
                                    int                   run_model) {
    if (!src || !dst || !scratch_parent) return TDC_E_INVAL;
    if (!scratch_parent->realloc_fn)     return TDC_E_INVAL;
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

    /* Validity byte count must match the header field exactly. */
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

    /* dst->data may be NULL only if the block is empty. For variable-
     * width entry points the hook below allocates dst->data after the
     * residual is in hand, so we defer this check until after the hook
     * has had a chance to run. */
    if (!hook && n_elems > 0 && dst->data == NULL) return TDC_E_INVAL;

    /* Resolve vtables. */
    const tdc_model_vt *model_vt = tdc_model_get((tdc_model_id)hdr.model_id);
    if (!model_vt) return TDC_E_UNSUPPORTED;

    /* Count the entropy chain length from the header. NONE terminates,
     * and the validator already enforced the sticky-terminator rule so
     * we can trust the first NONE ends the chain. */
    int entropy_len = 0;
    for (int i = 0; i < TDC_MAX_ENTROPY; ++i) {
        tdc_entropy_id eid = (tdc_entropy_id)hdr.entropy_ids[i];
        if (eid == TDC_ENTROPY_NONE) break;
        if (!tdc_entropy_get(eid)) return TDC_E_UNSUPPORTED;
        entropy_len = i + 1;
    }

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
     * Seed the chain walk with the model's residual dtype, not the
     * block's user-facing dtype. For most v0 models the two are equal,
     * but DICT_1D / DICT_NUMERIC_1D emit u32 indices from blocks of
     * different dtypes, and the downstream transforms (and the entropy
     * stage's uncompressed byte count) all see the residual dtype, not
     * the block dtype.
     */
    tdc_dtype residual_dtype = driver_model_residual_dtype(
        (tdc_model_id)hdr.model_id, dst->dtype,
        (hdr.side_meta_size > 0) ? side_meta_p : NULL,
        (size_t)hdr.side_meta_size);
    size_t    residual_elem_size = tdc_dtype_size(residual_dtype);
    if (residual_elem_size == 0) return TDC_E_UNSUPPORTED;

    /* Residual length in ELEMENTS. Equals n_elems for every model except
     * SPARSE_ZERO_1D, whose residual carries one u32 position per non-zero
     * input (n_nonzero, read from side-meta). The byte-size walk below and
     * the uncompressed_size cross-check must both use this, not n_elems. */
    int64_t residual_len = driver_model_residual_len(
        (tdc_model_id)hdr.model_id, n_elems,
        (hdr.side_meta_size > 0) ? side_meta_p : NULL,
        (size_t)hdr.side_meta_size);
    if (residual_len < 0) return TDC_E_CORRUPT;

    int       chain_len = 0;
    tdc_dtype xform_in[TDC_MAX_TRANSFORMS];
    size_t    xform_in_bytes[TDC_MAX_TRANSFORMS];
    tdc_dtype walk = residual_dtype;
    size_t    walk_bytes = (size_t)residual_len * residual_elem_size;
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
        walk_bytes = (size_t)residual_len * next_elem_size;
        chain_len  = i + 1;
    }

    /* Zero-residual short-circuit: when the encoder set
     * TDC_BLOCK_FLAG_ZERO_RESIDUAL, the xform + entropy chains were
     * skipped entirely and payload_size and xform_params_size are both
     * zero. uncompressed_size still carries the logical residual byte
     * count, reinterpreted as residual_len * residual_elem_size directly. */
    const int zero_residual = (hdr.flags & TDC_BLOCK_FLAG_ZERO_RESIDUAL) != 0;
    if (zero_residual) {
        if (hdr.payload_size != 0u || hdr.xform_params_size != 0u) return TDC_E_CORRUPT;
        size_t residual_bytes = (size_t)residual_len * residual_elem_size;
        if ((uint64_t)residual_bytes != hdr.uncompressed_size) return TDC_E_CORRUPT;
    } else {
        if ((uint64_t)walk_bytes != hdr.uncompressed_size) return TDC_E_CORRUPT;
    }

    /* ----- Stage 2: scratch buffers + entropy decode ------------------ */

    tdc_buffer bufs[2];
    driver_scratch_init(&bufs[0], scratch_parent);
    driver_scratch_init(&bufs[1], scratch_parent);

    int cur = 0;

    if (hdr.uncompressed_size > SIZE_MAX) { st = TDC_E_INVAL; goto cleanup; }
    size_t entropy_out_size = (size_t)hdr.uncompressed_size;

    /* Reserve at least 1 byte even for empty inputs so bufs[cur].data
     * is non-NULL. Add 16 bytes of wildcopy slack so LZ-family decoders
     * can safely touch the 15-byte tail past their declared output. */
    st = tdc_buf_reserve(&bufs[cur],
                         (entropy_out_size > 0 ? entropy_out_size : 1u) + 16u);
    if (st != TDC_OK) goto cleanup;

    const int dec_timing_on = tdc_stage_timers_enabled();
    double t_entropy = 0.0, t_xform = 0.0, t_model = 0.0;
    double dt0 = dec_timing_on ? tdc_now_secs() : 0.0;

    /* Zero-residual short-circuit: hand the model an uninitialized
     * residual buffer — when the side_meta carries a NORES flag the
     * model body never reads from it. The buffer's *size* must be set
     * correctly because the model validates residual_size == bytes. */
    if (zero_residual) {
        bufs[cur].size = entropy_out_size;
        if (dec_timing_on) { double t1 = tdc_now_secs(); t_entropy = t1 - dt0; dt0 = t1; t_xform = 0.0; }
        goto run_model;
    }

    /* Entropy decode: walk the chain RTL. An empty chain is an implicit
     * NONE passthrough. */
    if (entropy_len == 0) {
        if ((size_t)hdr.payload_size != entropy_out_size) {
            st = TDC_E_CORRUPT; goto cleanup;
        }
        if (entropy_out_size > 0) {
            memcpy(bufs[cur].data, payload_p, entropy_out_size);
        }
        bufs[cur].size = entropy_out_size;
    } else {
        /* Parse stage-input sizes table from the start of the payload. */
        size_t sizes_bytes = (size_t)entropy_len * sizeof(uint32_t);
        if ((size_t)hdr.payload_size < sizes_bytes) {
            st = TDC_E_CORRUPT; goto cleanup;
        }
        uint32_t stage_in_sizes[TDC_MAX_ENTROPY];
        for (int i = 0; i < entropy_len; ++i) {
            uint32_t le;
            memcpy(&le, payload_p + (size_t)i * sizeof(uint32_t), sizeof(le));
            stage_in_sizes[i] = le;
        }
        if ((uint64_t)stage_in_sizes[0] != hdr.uncompressed_size) {
            st = TDC_E_CORRUPT; goto cleanup;
        }

        const uint8_t *stage_src      = payload_p      + sizes_bytes;
        size_t         stage_src_size = (size_t)hdr.payload_size - sizes_bytes;

        for (int i = entropy_len - 1; i >= 0; --i) {
            tdc_entropy_id eid = (tdc_entropy_id)hdr.entropy_ids[i];
            const tdc_entropy_vt *evt = tdc_entropy_get(eid);
            if (!evt) { st = TDC_E_UNSUPPORTED; goto cleanup; }

            size_t dst_size = (size_t)stage_in_sizes[i];

            st = tdc_buf_reserve(&bufs[1 - cur],
                                 (dst_size > 0 ? dst_size : 1u) + 16u);
            if (st != TDC_OK) goto cleanup;

            st = evt->decode(stage_src, stage_src_size,
                             bufs[1 - cur].data, dst_size);
            if (st != TDC_OK) goto cleanup;
            bufs[1 - cur].size = dst_size;

            stage_src      = bufs[1 - cur].data;
            stage_src_size = dst_size;
            cur = 1 - cur;
        }
        if (bufs[cur].size != entropy_out_size) {
            st = TDC_E_CORRUPT; goto cleanup;
        }
    }
    if (dec_timing_on) { double t1 = tdc_now_secs(); t_entropy = t1 - dt0; dt0 = t1; }

    /* ----- Stage 3: reverse the transform chain ----------------------- */

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

    if (dec_timing_on) { double t1 = tdc_now_secs(); t_xform = t1 - dt0; dt0 = t1; }

    /* ----- Stage 4: model decode -------------------------------------- */

run_model:
    /* Pre-model hook: variable-width entry points size dst->data and
     * dst->offsets from the decoded residual + side metadata here. The
     * hook may grow caller-owned buffers and must satisfy the dst->data
     * non-NULL contract before the model runs. */
    if (hook) {
        st = hook(dst, bufs[cur].data, bufs[cur].size,
                  (hdr.side_meta_size > 0) ? side_meta_p : NULL,
                  hdr.side_meta_size,
                  residual_dtype,
                  (tdc_model_id)hdr.model_id,
                  hook_user);
        if (st != TDC_OK) goto cleanup;
        /* The hook satisfies the dst->data contract the early check
         * enforces in the no-hook path. For variable-width dtypes the
         * hook may legitimately leave dst->data NULL when the residual
         * resolves to a zero-byte heap (e.g. all-empty strings); the
         * model write loop must guard with `slen > 0` before deref. */
        if (n_elems > 0 && dst->data == NULL &&
            !tdc_dtype_is_variable_length(dst->dtype)) {
            st = TDC_E_INVAL; goto cleanup;
        }
    }

    /* run_model == 0 lets a hook that has already extracted everything it
     * needs from the residual + side meta (e.g. the dict-defer entry point,
     * which keeps the raw dictionary + indices rather than the flattened
     * column) skip the model's reconstruction step entirely. */
    if (!run_model) { st = TDC_OK; goto cleanup; }

    st = model_vt->decode(dst, NULL, residual_dtype,
                          bufs[cur].data, bufs[cur].size,
                          (hdr.side_meta_size > 0) ? side_meta_p : NULL,
                          hdr.side_meta_size);
    if (st != TDC_OK) goto cleanup;
    if (dec_timing_on) { double t1 = tdc_now_secs(); t_model = t1 - dt0; }

    if (dec_timing_on) {
        size_t raw = (size_t)n_elems * tdc_dtype_size(dst->dtype);
        double raw_mib = (double)raw / (1024.0 * 1024.0);
        const char *tag = timer_tag ? timer_tag : "dec  ";
        TDC_LOG("[%-5s] entropy=%6.2f ms (%7.1f MB/s) "
                "xform=%6.2f ms (%7.1f MB/s) "
                "model=%6.2f ms (%7.1f MB/s) "
                "raw=%.2f MiB enc=%llu B\n",
                tag,
                t_entropy * 1000.0,
                t_entropy > 0 ? raw_mib / t_entropy : 0.0,
                t_xform   * 1000.0,
                t_xform   > 0 ? raw_mib / t_xform   : 0.0,
                t_model   * 1000.0,
                t_model   > 0 ? raw_mib / t_model   : 0.0,
                raw_mib, (unsigned long long)hdr.payload_size);
    }

    st = TDC_OK;

cleanup:
    driver_scratch_free(&bufs[0]);
    driver_scratch_free(&bufs[1]);
    return st;
}
