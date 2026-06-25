/*
 * src/model/model_load_store.h
 *
 * Shared helpers for loading / storing typed elements from raw byte buffers.
 * Used by pred2d, pred3d, plane2d, and any future model that needs
 * element-wise dtype-dispatched access.
 *
 * tdc_model_load:  read element i as int64_t (float dtypes go through
 *                  ordered-integer mapping so delta/predictor residuals
 *                  are well-defined).
 *
 * tdc_model_load_int: integer-only variant (no float cases). Use when
 *                     the caller's accepted-dtype mask excludes floats.
 *
 * tdc_model_store_int: write a truncated int64_t back to element i.
 *                      Integer-only (no float inverse mapping).
 *
 * Static inline: each TU gets its own copy, no link dependency.
 */

#ifndef TDC_MODEL_LOAD_STORE_H
#define TDC_MODEL_LOAD_STORE_H

#include "tdc/types.h"
#include "../core/float_order.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ---- acceptance bitmask helpers ------------------------------------------ */

/* Build a single-bit mask for a dtype or layout enum value.  Used to
 * construct ACCEPTED_DTYPES / ACCEPTED_LAYOUTS constants per model. */
#define TDC_DT_BIT(dt)     (1u << (uint32_t)(dt))
#define TDC_LAYOUT_BIT(lo) (1u << (uint32_t)(lo))

/* Test whether a dtype/layout is in the model's accepted bitmask. */
static inline int
tdc_model_dtype_accepted(uint32_t accepted_mask, tdc_dtype dt)
{
    return (accepted_mask & TDC_DT_BIT(dt)) != 0u;
}

static inline int
tdc_model_layout_accepted(uint32_t accepted_mask, tdc_layout lo)
{
    return (accepted_mask & TDC_LAYOUT_BIT(lo)) != 0u;
}

/* ---- load (all dtypes, float → ordered) --------------------------------- */

static inline int64_t
tdc_model_load(tdc_dtype dt, const uint8_t *base, int64_t i)
{
    switch (dt) {
        case TDC_DT_I8:  { int8_t   v; memcpy(&v, base + (size_t)i,       1u); return (int64_t)v; }
        case TDC_DT_I16: { int16_t  v; memcpy(&v, base + (size_t)i * 2u,  2u); return (int64_t)v; }
        case TDC_DT_I32: { int32_t  v; memcpy(&v, base + (size_t)i * 4u,  4u); return (int64_t)v; }
        case TDC_DT_I64: { int64_t  v; memcpy(&v, base + (size_t)i * 8u,  8u); return v; }
        case TDC_DT_U8:  { uint8_t  v; memcpy(&v, base + (size_t)i,       1u); return (int64_t)v; }
        case TDC_DT_U16: { uint16_t v; memcpy(&v, base + (size_t)i * 2u,  2u); return (int64_t)v; }
        case TDC_DT_U32: { uint32_t v; memcpy(&v, base + (size_t)i * 4u,  4u); return (int64_t)v; }
        case TDC_DT_U64: { uint64_t v; memcpy(&v, base + (size_t)i * 8u,  8u); return (int64_t)v; }
        case TDC_DT_F16: return (int64_t)tdc_f16_to_ordered(
                            tdc_load_f16_bits(base + (size_t)i * 2u));
        case TDC_DT_F32: return (int64_t)tdc_f32_to_ordered(
                            tdc_load_f32_bits(base + (size_t)i * 4u));
        case TDC_DT_F64: return (int64_t)tdc_f64_to_ordered(
                            tdc_load_f64_bits(base + (size_t)i * 8u));
        default:         return 0;
    }
}

/* ---- load (integer dtypes only) ----------------------------------------- */

static inline int64_t
tdc_model_load_int(tdc_dtype dt, const uint8_t *base, int64_t i)
{
    switch (dt) {
        case TDC_DT_I8:  { int8_t   v; memcpy(&v, base + (size_t)i,       1u); return (int64_t)v; }
        case TDC_DT_I16: { int16_t  v; memcpy(&v, base + (size_t)i * 2u,  2u); return (int64_t)v; }
        case TDC_DT_I32: { int32_t  v; memcpy(&v, base + (size_t)i * 4u,  4u); return (int64_t)v; }
        case TDC_DT_I64: { int64_t  v; memcpy(&v, base + (size_t)i * 8u,  8u); return v; }
        case TDC_DT_U8:  { uint8_t  v; memcpy(&v, base + (size_t)i,       1u); return (int64_t)v; }
        case TDC_DT_U16: { uint16_t v; memcpy(&v, base + (size_t)i * 2u,  2u); return (int64_t)v; }
        case TDC_DT_U32: { uint32_t v; memcpy(&v, base + (size_t)i * 4u,  4u); return (int64_t)v; }
        case TDC_DT_U64: { uint64_t v; memcpy(&v, base + (size_t)i * 8u,  8u); return (int64_t)v; }
        default:         return 0;
    }
}

/* ---- store (integer dtypes only) ---------------------------------------- */

static inline void
tdc_model_store_int(tdc_dtype dt, uint8_t *base, int64_t i, int64_t v)
{
    switch (dt) {
        case TDC_DT_I8:
        case TDC_DT_U8:  { uint8_t  x = (uint8_t)(uint64_t)v;  memcpy(base + (size_t)i,       &x, 1u); break; }
        case TDC_DT_I16:
        case TDC_DT_U16: { uint16_t x = (uint16_t)(uint64_t)v; memcpy(base + (size_t)i * 2u,  &x, 2u); break; }
        case TDC_DT_I32:
        case TDC_DT_U32: { uint32_t x = (uint32_t)(uint64_t)v; memcpy(base + (size_t)i * 4u,  &x, 4u); break; }
        default: break;
    }
}

#endif /* TDC_MODEL_LOAD_STORE_H */
