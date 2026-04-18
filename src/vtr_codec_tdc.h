#ifndef VECTRA_VTR_CODEC_TDC_H
#define VECTRA_VTR_CODEC_TDC_H

/*
 * vtr_codec_tdc.h — tdc-backed encode bridge (sole encode path).
 *
 * The caller hands tdc a tdc_block view over a VecArray, picks a
 * tdc_codec_spec from comp_level / qspec / sspec, and tdc emits a
 * complete self-describing block record (header + side_meta + payload +
 * validity). The reader side mirrors this with a single decode call.
 */

#include "types.h"
#include "vtr_codec.h"
#include "tdc.h"

#include <stdint.h>

/*
 * Encode one column into a self-describing tdc block record.
 *
 *   col         Source column. type / length / data / validity are read.
 *               Caller retains ownership.
 *   n_rows      Number of rows to encode. Must equal col->length.
 *   comp_level  VTR_COMPRESS_NONE / _FAST / _SMALL.
 *               NONE  -> RAW + no entropy (passthrough).
 *               FAST  -> default model heuristic + BSHUF + LZ.
 *               SMALL -> currently identical to FAST; try-all-pick-smallest
 *                       over multiple tdc specs is not yet ported.
 *   qspec       Optional lossy quantization (VEC_DOUBLE only). NULL or
 *               qspec->enabled == 0 disables quantization.
 *   sspec       Optional 2D spatial predictor. NULL or sspec->enabled == 0
 *               disables spatial prediction.
 *   block_out   Caller-owned growable byte buffer. realloc_fn must be set
 *               before calling. On success, block_out->data holds the
 *               complete tdc_block_record bytes and block_out->size is
 *               its length. On failure, the buffer is left empty (any
 *               partial allocation is freed via realloc_fn).
 *
 * Returns TDC_OK on success, TDC_E_* otherwise. On failure, an R-level
 * error is NOT raised — the caller decides whether to fall back to the
 * legacy path or surface the error.
 */
tdc_status vtr_encode_column_tdc(const VecArray         *col,
                                 int64_t                 n_rows,
                                 int                     comp_level,
                                 const VtrQuantizeSpec  *qspec,
                                 const VtrSpatialSpec   *sspec,
                                 tdc_buffer             *block_out);

/*
 * Map a vectra VecType to its tdc dtype counterpart.
 *   VEC_INT64  -> TDC_DT_I64
 *   VEC_INT32  -> TDC_DT_I32
 *   VEC_INT16  -> TDC_DT_I16
 *   VEC_INT8   -> TDC_DT_I8
 *   VEC_DOUBLE -> TDC_DT_F64
 *   VEC_BOOL   -> TDC_DT_U8   (vectra stores booleans as 0/1 bytes)
 *   VEC_STRING -> TDC_DT_STRING
 * Returns 0 for unknown VecType (validation should reject earlier).
 */
tdc_dtype vtr_type_to_tdc_dtype(VecType t);

/* ----- Encode request (shared between block-at-a-time and streaming) ----- *
 *
 * vtr_encode_column_tdc and the streaming row-group writer both need to:
 *   1. build a tdc_block view over a VecArray,
 *   2. derive a tdc_codec_spec from comp_level/qspec/sspec,
 *   3. stack-allocate the per-stage param structs that spec.* points into,
 *   4. potentially allocate a uint32_t string offsets buffer (cast from
 *      vectra's int64 offsets).
 *
 * VtrTdcEncodeRequest packages all four together so the spec-selection
 * logic lives in one place. Lifetime: stack-allocated by the caller and
 * released via vtr_codec_tdc_release_request after the encode call.
 *
 *   VtrTdcEncodeRequest req;
 *   tdc_status st = vtr_codec_tdc_prepare_request(&req, col, n_rows,
 *                                                  comp_level, qspec, sspec,
 *                                                  realloc_fn, alloc_user);
 *   if (st == TDC_OK) {
 *       st = tdc_encode_block(&req.block, &req.spec, out);  // or _stream_*
 *       vtr_codec_tdc_release_request(&req, realloc_fn, alloc_user);
 *   }
 *
 * spec.model_params / xform_params point into req.qp / req.pp / req.plp;
 * the request must outlive the encode call. */

typedef struct {
    tdc_block                    block;
    tdc_codec_spec               spec;
    tdc_quantize_params          qp;
    tdc_pred2d_params            pp;
    tdc_plane2d_params           plp;
    tdc_quantize_pred2d_params   qpp;  /* used when spatial + quantize fuse */
    uint32_t                    *str_offsets_owned;  /* NULL unless VEC_STRING */
} VtrTdcEncodeRequest;

tdc_status vtr_codec_tdc_prepare_request(
    VtrTdcEncodeRequest    *req,
    const VecArray         *col,
    int64_t                 n_rows,
    int                     comp_level,
    const VtrQuantizeSpec  *qspec,
    const VtrSpatialSpec   *sspec,
    void                  *(*realloc_fn)(void *user, void *ptr, size_t n),
    void                   *alloc_user);

void vtr_codec_tdc_release_request(
    VtrTdcEncodeRequest *req,
    void               *(*realloc_fn)(void *user, void *ptr, size_t n),
    void                *alloc_user);

/*
 * Decode bridge.
 *
 * Decode one tdc block record into a pre-allocated VecArray.
 *
 *   col_out      Caller-allocated array (from vec_array_alloc). On entry:
 *                  - col_out->type    : expected VecType
 *                  - col_out->length  : expected element count
 *                  - col_out->buf.<typed pointer> : destination buffer
 *                  - col_out->validity : validity bitmap (vec_array_alloc
 *                    zeroes it; the bridge overwrites if HAS_VALIDITY).
 *                Type/length are validated against the record header
 *                (TDC_E_DTYPE / TDC_E_SHAPE on mismatch).
 *   src          Pointer to the start of a tdc_block_record.
 *   src_size     Bytes available at src.
 *
 * On TDC_OK, col_out->buf.* holds the decoded values and col_out->validity
 * has been overwritten from the record (or set all-valid if the record has
 * no validity bitmap).
 *
 * VEC_STRING goes through tdc_decode_block_varlen, which sizes the output
 * heap from the encoded residual + dictionary side-meta and allocates
 * dst->offsets and dst->data via libc realloc. The placeholder offsets/data
 * installed by vec_array_alloc(VEC_STRING, n) are released and replaced
 * with malloc'd buffers; col_out retains owns_data=1.
 */
tdc_status vtr_decode_column_tdc(VecArray       *col_out,
                                 const uint8_t  *src,
                                 size_t          src_size);

/*
 * Lower-level decode-into that bypasses VecArray construction. Used by
 * the direct-write fast path to land elements directly in a memory-mapped
 * destination buffer (e.g. an SEXP backing store).
 *
 *   type           Expected element type. Validated against the header.
 *   n_rows         Expected element count. Validated against the header.
 *   dst_data       Caller-owned, n_rows-sized, dtype-correct buffer.
 *   dst_validity   Optional. If non-NULL and the record carries validity,
 *                  receives ceil(n_rows/8) bytes. If NULL, validity bytes
 *                  in the record are dropped (caller doesn't care). If
 *                  non-NULL and the record carries no validity, the
 *                  bitmap is filled with all-ones (all valid).
 *
 * Returns TDC_OK / TDC_E_DTYPE / TDC_E_SHAPE / TDC_E_UNSUPPORTED (string)
 * / other TDC_E_*.
 */
tdc_status vtr_decode_column_tdc_into(VecType         type,
                                      int64_t         n_rows,
                                      void           *dst_data,
                                      uint8_t        *dst_validity,
                                      const uint8_t  *src,
                                      size_t          src_size);

#endif /* VECTRA_VTR_CODEC_TDC_H */
