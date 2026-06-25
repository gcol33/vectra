/*
 * src/model/pred2d_internal.h
 *
 * Cross-TU access to the PRED_2D sweep dispatchers, used by the
 * TDC_MODEL_QUANTIZE_PRED_2D composite model in src/model/quantize_pred2d.c.
 *
 * Not part of the public include/ tree. Other models that want to fuse
 * PRED_2D's predictor logic into their own pipeline call these directly
 * on a typed integer raster — bypassing the model.encode boilerplate
 * (side-meta serialization, bounds validation, vtable indirection) so
 * the composite remains a single self-contained model record on disk.
 */

#ifndef TDC_PRED2D_INTERNAL_H
#define TDC_PRED2D_INTERNAL_H

#include "tdc/codec.h"
#include "tdc/types.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

void pred2d_encode_sweep(tdc_dtype dt, tdc_pred2d_kind kind,
                         const uint8_t *src,
                         uint8_t *res,
                         int64_t nx, int64_t ny);

void pred2d_decode_sweep(tdc_dtype dt, tdc_pred2d_kind kind,
                         const uint8_t *res,
                         uint8_t *dst,
                         int64_t nx, int64_t ny);

tdc_pred2d_kind pred2d_auto_select(tdc_dtype dt, const uint8_t *src,
                                   int64_t nx, int64_t ny);

/* ---- u16 PAETH wavefront kernels (exposed for cross-kernel tests) -------
 * These are wired into pred2d_decode_sweep's tiered dispatcher. They are
 * exported here so tests can cross-check that the wider wavefronts
 * (wf4 SSE2, wf8 AVX2) agree bit-for-bit with the 2-row reference
 * (which itself round-trips against the scalar typed kernel via the
 * pred2d_roundtrip test). Not part of the public API.
 *
 * Each kernel has its own minimum-shape contract:
 *   wavefront — any (handles row 0 LEFT predictor + scalar trailing).
 *   wf4       — nx >= 4, ny >= 5 for the steady state to enter.
 *   wf8       — nx >= 8, ny >= 9 (only declared when TDC_PRED2D_HAVE_AVX2).
 * Below the threshold each kernel still produces correct output via its
 * own scalar fall-through; the dispatcher just prefers the widest one
 * whose steady-state runs at least one iteration. */
void pred2d_dec_u16_paeth_wavefront_export(const uint16_t *res, uint16_t *dst,
                                            int64_t nx, int64_t ny);
void pred2d_dec_u16_paeth_wf4_export(const uint16_t *res, uint16_t *dst,
                                      int64_t nx, int64_t ny);
void pred2d_dec_u16_paeth_wf8_export(const uint16_t *res, uint16_t *dst,
                                      int64_t nx, int64_t ny);

/* Compile-time capability flag — tests can skip wf8 sections when AVX2
 * was not enabled for the build. */
int pred2d_have_avx2(void);

#ifdef __cplusplus
}
#endif

#endif /* TDC_PRED2D_INTERNAL_H */
