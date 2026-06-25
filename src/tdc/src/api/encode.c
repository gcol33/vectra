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
#include "driver_internal.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ----- Optional stage timers (env-gated) ---------------------------------
 * Set TDC_STAGE_TIMERS=1 to dump per-stage wallclock for every encode call.
 * Used for SPEEDUP-TODO P2.x diagnostics — confirms which stage actually
 * dominates a pipeline before reaching for SIMD or rewrites. */
#include "../core/timer.h"

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

    /* Resolve the model vtable up front. Transforms and entropy stages
     * are looked up lazily inside their chain loops because either
     * chain may be empty. */
    const tdc_model_vt *model_vt = tdc_model_get(spec->model);
    if (!model_vt) return TDC_E_UNSUPPORTED;

    /* Pre-walk the entropy chain to validate ids AND enforce the
     * sticky-terminator rule without needing a second pass later. */
    int entropy_len = 0;
    for (int i = 0; i < TDC_MAX_ENTROPY; ++i) {
        tdc_entropy_id eid = spec->entropy[i];
        if (eid == TDC_ENTROPY_NONE) {
            /* Sticky: everything after must also be NONE. */
            for (int j = i + 1; j < TDC_MAX_ENTROPY; ++j) {
                if (spec->entropy[j] != TDC_ENTROPY_NONE) return TDC_E_INVAL;
            }
            break;
        }
        if (!tdc_entropy_get(eid)) return TDC_E_UNSUPPORTED;
        entropy_len = i + 1;
    }

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

    const int timing_on = tdc_stage_timers_enabled();
    double t_model = 0.0, t_xform = 0.0, t_entropy = 0.0;
    double t0 = timing_on ? tdc_now_secs() : 0.0;

    st = model_vt->encode(src, spec->model_params,
                          &bufs[cur], &cur_dtype, &side_meta);
    if (st != TDC_OK) goto cleanup;
    if (timing_on) { double t1 = tdc_now_secs(); t_model = t1 - t0; t0 = t1; }

    /* Zero-residual signal: a non-empty block whose model emitted zero
     * residual bytes means the model fits the data exactly and the
     * residual stream would be all-zero after the transform chain as
     * well (every v0 transform is shape-preserving on zero input). The
     * driver sets TDC_BLOCK_FLAG_ZERO_RESIDUAL on the block record, skips
     * the xform + entropy chains entirely, and stores a 0-byte payload.
     * Decode reconstructs the zero residual before calling model.decode.
     *
     * The logical uncompressed size is still n_elems * residual_elem_size
     * so the decoder's shape-vs-size cross-check keeps working. We stash
     * it here because this is the only point where we have both n_elems
     * and the model's residual dtype in scope without recomputing them. */
    int64_t n_elems_top = shape_n_elems(&src->shape);
    const int zero_residual = (n_elems_top > 0) && (bufs[cur].size == 0);
    size_t   zero_residual_bytes = 0;
    if (zero_residual) {
        size_t relem = tdc_dtype_size(cur_dtype);
        if (relem == 0) { st = TDC_E_UNSUPPORTED; goto cleanup; }
        zero_residual_bytes = (size_t)n_elems_top * relem;
    }

    /* ----- Stage 2: transform chain ------------------------------------ */
    /*
     * The xform + entropy chains both short-circuit in the zero-residual
     * case so that the hot path on piecewise-planar inputs (all the
     * plane2d + BSHUF + LZ benchmarks) collapses to "emit side meta,
     * stamp flag, write header". The inner loops below are unchanged;
     * only the guard is new.
     */

    size_t uncompressed_size = 0;

    if (!zero_residual) {

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
    uncompressed_size = bufs[cur].size;
    if (timing_on) { double t1 = tdc_now_secs(); t_xform = t1 - t0; t0 = t1; }

    /* ----- Stage 3: entropy chain -------------------------------------- *
     *
     * Same ping-pong shape as the transform chain: each entropy stage
     * takes bytes in and produces bytes out. The chain is walked LTR on
     * encode and RTL on decode. An empty chain (entropy_len == 0) is an
     * implicit NONE passthrough: the payload is the residual stream
     * verbatim.
     *
     * Intermediate sizes between stages are NOT stored in the 80-byte
     * block header. Instead, for a non-empty chain we prepend a small
     * size table to the payload:
     *
     *     [ u32 * entropy_len ]  stage-input sizes (stage 0 first, LE)
     *     [ final entropy-encoded bytes ]
     *
     * The innermost stage's input size is redundant with
     * hdr.uncompressed_size — kept in the table anyway as a cross-check
     * and to avoid a special-case branch in the decoder.
     */

    if (entropy_len == 0) {
        /* Passthrough: copy residuals into payload directly. bufs[cur]
         * is scratch memory we can't hand to the caller (freed at
         * cleanup), so a memcpy into the dedicated payload buffer is
         * required. */
        st = tdc_buf_reserve(&payload, uncompressed_size > 0 ? uncompressed_size : 1u);
        if (st != TDC_OK) goto cleanup;
        if (uncompressed_size > 0) {
            memcpy(payload.data, bufs[cur].data, uncompressed_size);
        }
        payload.size = uncompressed_size;
    } else {
        /* Step 1: walk the chain, ping-ponging bufs[cur]/bufs[1-cur].
         * Record the input size of each stage for the decode-side
         * RTL walk. */
        uint32_t stage_in_sizes[TDC_MAX_ENTROPY];
        const uint8_t *stage_src      = bufs[cur].data;
        size_t         stage_src_size = uncompressed_size;

        for (int i = 0; i < entropy_len; ++i) {
            if (stage_src_size > UINT32_MAX) { st = TDC_E_INVAL; goto cleanup; }
            stage_in_sizes[i] = (uint32_t)stage_src_size;

            tdc_entropy_id eid = spec->entropy[i];
            const tdc_entropy_vt *evt = tdc_entropy_get(eid);
            if (!evt) { st = TDC_E_UNSUPPORTED; goto cleanup; }

            bufs[1 - cur].size = 0;
            st = evt->encode(stage_src, stage_src_size,
                             spec->entropy_params[i], &bufs[1 - cur]);
            if (st != TDC_OK) goto cleanup;

            stage_src      = bufs[1 - cur].data;
            stage_src_size = bufs[1 - cur].size;
            cur = 1 - cur;
        }

        /* Step 2: assemble payload = [sizes table][final stage bytes].
         * bufs[cur] currently holds the final entropy-encoded bytes. */
        size_t sizes_bytes  = (size_t)entropy_len * sizeof(uint32_t);
        size_t final_bytes  = stage_src_size;
        size_t total_payload = sizes_bytes + final_bytes;
        if (total_payload < sizes_bytes) { st = TDC_E_INVAL; goto cleanup; } /* overflow */
        st = tdc_buf_reserve(&payload, total_payload > 0 ? total_payload : 1u);
        if (st != TDC_OK) goto cleanup;
        for (int i = 0; i < entropy_len; ++i) {
            uint32_t le = stage_in_sizes[i];
            memcpy(payload.data + (size_t)i * sizeof(uint32_t), &le, sizeof(le));
        }
        if (final_bytes > 0) {
            memcpy(payload.data + sizes_bytes, bufs[cur].data, final_bytes);
        }
        payload.size = total_payload;
    }
    if (timing_on) { double t1 = tdc_now_secs(); t_entropy = t1 - t0; t0 = t1; }
    if (timing_on) {
        int64_t n = shape_n_elems(&src->shape);
        size_t  raw = (size_t)n * tdc_dtype_size(src->dtype);
        double  raw_mib = (double)raw / (1024.0 * 1024.0);
        TDC_LOG("[stage] model=%6.2f ms (%7.1f MB/s) "
                "xform=%6.2f ms (%7.1f MB/s) "
                "entropy=%6.2f ms (%7.1f MB/s) "
                "raw=%.2f MiB enc=%zu B\n",
                t_model    * 1000.0,
                t_model    > 0 ? raw_mib / t_model    : 0.0,
                t_xform    * 1000.0,
                t_xform    > 0 ? raw_mib / t_xform    : 0.0,
                t_entropy  * 1000.0,
                t_entropy  > 0 ? raw_mib / t_entropy  : 0.0,
                raw_mib, payload.size);
    }

    } /* end if (!zero_residual) */

    /* ----- Stage 4: assemble block record ------------------------------ */

    int64_t n_elems = n_elems_top;
    size_t  vbytes  = (src->validity != NULL) ? validity_bytes_for(n_elems) : 0;

    /* Zero-residual short-circuit: xform + entropy chains were skipped,
     * payload and xform_params are both empty, and the header stores the
     * logical residual byte count in uncompressed_size so the decoder's
     * cross-check still passes even though no bytes are on disk. The
     * caller's xform/entropy spec is intentionally preserved in
     * hdr.xform_ids/entropy_ids below — the flag is what gates whether
     * the decoder actually walks them. */
    size_t effective_uncompressed_size =
        zero_residual ? zero_residual_bytes : uncompressed_size;

    if (side_meta.size    > UINT32_MAX ||
        xform_params.size > UINT32_MAX ||
        payload.size      > UINT32_MAX ||
        vbytes            > UINT32_MAX) {
        st = TDC_E_INVAL; goto cleanup;
    }
    if (effective_uncompressed_size > UINT64_MAX) { /* always false; documents intent */
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
    if (zero_residual)         hdr.flags |= TDC_BLOCK_FLAG_ZERO_RESIDUAL;
    hdr.model_id   = (uint16_t)spec->model;
    for (int i = 0; i < TDC_MAX_TRANSFORMS; ++i) {
        hdr.xform_ids[i] = (uint16_t)spec->xform[i];
    }
    for (int i = 0; i < TDC_MAX_ENTROPY; ++i) {
        hdr.entropy_ids[i] = (uint16_t)spec->entropy[i];
    }
    hdr.dtype             = (uint8_t)src->dtype;
    hdr.layout            = (uint8_t)src->layout;
    hdr.rank              = src->shape.rank;
    for (uint8_t i = 0; i < TDC_MAX_RANK; ++i) {
        hdr.dim[i] = (i < src->shape.rank) ? src->shape.dim[i] : 0;
    }
    hdr.uncompressed_size = (uint64_t)effective_uncompressed_size;
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
