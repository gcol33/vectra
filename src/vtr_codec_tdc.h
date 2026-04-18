#ifndef VECTRA_VTR_CODEC_TDC_H
#define VECTRA_VTR_CODEC_TDC_H

/*
 * vtr_codec_tdc.h — side-by-side tdc-backed encode bridge (P2a).
 *
 * Replaces the (encoding, compression, side-metadata) triple from
 * VtrEncodedCol with a single self-describing tdc block record. The
 * caller hands tdc a tdc_block view over a VecArray, picks a
 * tdc_codec_spec from comp_level / qspec / sspec, and tdc emits a
 * complete block record (header + side_meta + payload + validity).
 *
 * P2a is purely additive: nothing on the read or write path calls this
 * yet. The legacy vtr_encode_column / _ex / _q / _qs in vtr_codec.c
 * remain the production encode entry points until P4 swaps them out.
 *
 * See VECTRA_REWIRE.md (in the tdc repo) for the surrounding plan.
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
 *               SMALL -> at this stage, treated identically to FAST.
 *                       The vectra-side outer try-all-pick-smallest loop
 *                       (vtr_compress.c) stays in the legacy path; once
 *                       P4 lands, SMALL will be promoted by trying
 *                       multiple specs at the call site.
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

#endif /* VECTRA_VTR_CODEC_TDC_H */
