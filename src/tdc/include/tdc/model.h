/*
 * tdc/model.h — frozen v0
 *
 * Model (predictor) vtable.
 *
 * A model takes a tdc_block and produces a residual stream + side metadata.
 * Models are dimension-aware: they own iteration order, neighborhood access,
 * and prediction rules. They do NOT do byte shuffling, quantization, or
 * entropy coding — those live in transform/ and entropy/.
 *
 * Hard boundary with layout/:
 *   layout/  answers "how do I iterate this block?" (raster scan, tile, slice)
 *   model/   answers "given accessible neighbors, how do I predict?"
 * Models call into layout helpers; layout helpers never call models.
 *
 * v0 registry is STATIC. Models are looked up by tdc_model_id via an internal
 * table in src/core/registry.c. There is no public registration API in v0;
 * adding a model means adding a vtable and an enum entry, recompiling tdc.
 */

#ifndef TDC_MODEL_H
#define TDC_MODEL_H

#include "types.h"
#include "codec.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Bitmask helpers for accepted_dtypes / accepted_layouts. */
#define TDC_DT_MASK(dt)     (1u << (dt))
#define TDC_LAYOUT_MASK(lo) (1u << (lo))

#define TDC_DT_INTEGER_MASK ( \
    TDC_DT_MASK(TDC_DT_I8)  | TDC_DT_MASK(TDC_DT_I16) | \
    TDC_DT_MASK(TDC_DT_I32) | TDC_DT_MASK(TDC_DT_I64) | \
    TDC_DT_MASK(TDC_DT_U8)  | TDC_DT_MASK(TDC_DT_U16) | \
    TDC_DT_MASK(TDC_DT_U32) | TDC_DT_MASK(TDC_DT_U64))

#define TDC_DT_FLOAT_MASK ( \
    TDC_DT_MASK(TDC_DT_F32) | TDC_DT_MASK(TDC_DT_F64))

#define TDC_DT_NUMERIC_MASK (TDC_DT_INTEGER_MASK | TDC_DT_FLOAT_MASK)

typedef struct tdc_model_vt {
    tdc_model_id id;
    const char  *name;

    uint32_t accepted_dtypes;   /* bitmask over tdc_dtype */
    uint32_t accepted_layouts;  /* bitmask over tdc_layout */

    /*
     * Forward: read in, write residual + side metadata to out.
     *
     *   in              source block (caller-owned)
     *   params          model-specific params struct, may be NULL
     *   residual_out    grown via realloc_fn; receives the residual stream
     *                   (typed bytes; downstream transforms operate on this)
     *   residual_dtype  dtype of the residual stream (often = in->dtype, but
     *                   e.g. PRED_2D over u8 may emit i16 residuals)
     *   side_out        grown via realloc_fn; receives serialized model state
     *
     * Returns TDC_OK on success.
     */
    tdc_status (*encode)(const tdc_block *in,
                         const void      *params,
                         tdc_buffer      *residual_out,
                         tdc_dtype       *residual_dtype,
                         tdc_buffer      *side_out);

    /*
     * Inverse: reconstruct out->data from residuals + side metadata.
     *
     * out must already have data, dtype, layout, shape, offsets set up by
     * the caller (the block record header was used to size everything). The
     * model fills out->data in place. n_elems and dim are taken from out->shape.
     */
    tdc_status (*decode)(tdc_block      *out,
                         const void     *params,
                         tdc_dtype       residual_dtype,
                         const uint8_t  *residuals, size_t residual_size,
                         const uint8_t  *side_meta, size_t side_size);
} tdc_model_vt;

/* Lookup. Returns NULL if id is not registered. */
const tdc_model_vt *tdc_model_get(tdc_model_id id);

#ifdef __cplusplus
}
#endif
#endif /* TDC_MODEL_H */
