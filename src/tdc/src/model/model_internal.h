/*
 * src/model/model_internal.h
 *
 * Internal header listing extern declarations for every model vtable.
 * Not part of the public include/ tree. The single consumer is
 * src/core/registry.c, which dispatches a tdc_model_id to the matching
 * vtable address.
 *
 * Adding a new model backend means:
 *   1. defining `const tdc_model_vt tdc_model_<name>_vt` in the
 *      backend's .c file (no `static`),
 *   2. adding an `extern` declaration here,
 *   3. adding the case to tdc_model_get() in src/core/registry.c.
 */

#ifndef TDC_MODEL_INTERNAL_H
#define TDC_MODEL_INTERNAL_H

#include "tdc/model.h"

#ifdef __cplusplus
extern "C" {
#endif

extern const tdc_model_vt tdc_model_raw_vt;
extern const tdc_model_vt tdc_model_delta1d_vt;
extern const tdc_model_vt tdc_model_dict1d_vt;
extern const tdc_model_vt tdc_model_pred2d_vt;
extern const tdc_model_vt tdc_model_stack2d_vt;
extern const tdc_model_vt tdc_model_pred3d_vt;
extern const tdc_model_vt tdc_model_plane2d_vt;
extern const tdc_model_vt tdc_model_delta2_1d_vt;
extern const tdc_model_vt tdc_model_fpc1d_vt;
extern const tdc_model_vt tdc_model_dict_numeric_1d_vt;
extern const tdc_model_vt tdc_model_sparse_zero_1d_vt;
extern const tdc_model_vt tdc_model_quantize_pred2d_vt;

/* ----- Cross-stage residual dtype helpers ---------------------------------
 *
 * Composite models whose residual dtype depends on per-block side meta
 * (rather than being fixed for the model id) expose a small peek helper
 * that the driver calls before model.decode runs. Lives in the matching
 * model .c file alongside its side-meta layout.
 *
 * QUANTIZE_PRED_2D's side meta byte 0 is the integer target dtype that
 * the predictor actually emitted. Returning (tdc_dtype)0 signals corrupt
 * side meta and aborts the decode.
 */
tdc_dtype qp2_residual_dtype_from_side_meta(const uint8_t *side_meta,
                                            size_t         side_meta_size);

/* ----- Variable-width sizing helpers --------------------------------------
 *
 * Public-API helper used by tdc_decode_block_varlen to size the output
 * heap before the model.decode runs. Lives next to dict1d_decode in
 * src/model/dict.c. Pure: validates the side-meta header + offsets
 * table, then walks the residual indices to sum the output byte count.
 *
 * Returns TDC_OK on success with *out_heap_bytes set; on error the
 * status is forwarded and *out_heap_bytes is zero.
 */
tdc_status dict1d_compute_output_size(const uint8_t *residuals,
                                      size_t         residual_size,
                                      const uint8_t *side_meta,
                                      size_t         side_size,
                                      int64_t        n_elems,
                                      size_t        *out_heap_bytes);

#ifdef __cplusplus
}
#endif

#endif /* TDC_MODEL_INTERNAL_H */
