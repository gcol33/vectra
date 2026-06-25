/*
 * src/model/float_pred_helpers.h
 *
 * Shared helpers for float-path prediction in pred2d.c and pred3d.c.
 *
 * These functions are the single source of truth for:
 *   - loading float data as ordered uint64 values
 *   - storing ordered residuals
 *   - loading residuals back
 *   - converting ordered values back to float bits
 *   - Paeth prediction in ordered uint64 space
 *
 * Static inline: each TU gets its own copy, no link dependency.
 */

#ifndef TDC_MODEL_FLOAT_PRED_HELPERS_H
#define TDC_MODEL_FLOAT_PRED_HELPERS_H

#include "../core/float_order.h"
#include "tdc/types.h"

#include <stdint.h>
#include <string.h>

/* Load float data as ordered uint64 for integer-domain prediction. */
static inline uint64_t tdc_float_load_ordered(tdc_dtype dt,
                                              const uint8_t *base, int64_t i) {
    switch (dt) {
        case TDC_DT_F16: return (uint64_t)tdc_f16_to_ordered(
                            tdc_load_f16_bits(base + (size_t)i * 2u));
        case TDC_DT_F32: return (uint64_t)tdc_f32_to_ordered(
                            tdc_load_f32_bits(base + (size_t)i * 4u));
        case TDC_DT_F64: return tdc_f64_to_ordered(
                            tdc_load_f64_bits(base + (size_t)i * 8u));
        default:         return 0;
    }
}

/* Store an unsigned residual at element width. */
static inline void tdc_float_store_ordered_residual(tdc_dtype dt,
                                                    uint8_t *base, int64_t i,
                                                    uint64_t v) {
    switch (dt) {
        case TDC_DT_F16: tdc_store_u16(base + (size_t)i * 2u, (uint16_t)v); break;
        case TDC_DT_F32: tdc_store_u32(base + (size_t)i * 4u, (uint32_t)v); break;
        case TDC_DT_F64: tdc_store_u64(base + (size_t)i * 8u, v); break;
        default: break;
    }
}

/* Load a residual (raw unsigned integer at element width). */
static inline uint64_t tdc_float_load_residual(tdc_dtype dt,
                                               const uint8_t *base, int64_t i) {
    switch (dt) {
        case TDC_DT_F16: { uint16_t v; memcpy(&v, base + (size_t)i * 2u, 2u);
                           return (uint64_t)v; }
        case TDC_DT_F32: { uint32_t v; memcpy(&v, base + (size_t)i * 4u, 4u);
                           return (uint64_t)v; }
        case TDC_DT_F64: { uint64_t v; memcpy(&v, base + (size_t)i * 8u, 8u);
                           return v; }
        default:         return 0;
    }
}

/* Store an ordered value back as float bits. */
static inline void tdc_float_store_float(tdc_dtype dt, uint8_t *base,
                                         int64_t i, uint64_t ordered) {
    switch (dt) {
        case TDC_DT_F16: tdc_store_u16(base + (size_t)i * 2u,
                            tdc_ordered_to_f16((uint16_t)ordered)); break;
        case TDC_DT_F32: tdc_store_u32(base + (size_t)i * 4u,
                            tdc_ordered_to_f32((uint32_t)ordered)); break;
        case TDC_DT_F64: tdc_store_u64(base + (size_t)i * 8u,
                            tdc_ordered_to_f64(ordered)); break;
        default: break;
    }
}

/* Paeth prediction in ordered uint64 space (PNG-style).
 *
 * The float-ordered mapping spans the full int64 bit pattern range, so
 * casting to int64 and computing p - a etc. with signed arithmetic
 * overflows under UBSAN for values near the type boundary. Stay in uint64
 * throughout: a + b - c is well-defined as wraparound, and the Paeth
 * "closest predictor" rule only needs the unsigned wrap-distance between
 * p and each candidate. The wrap-distance equals the signed |p - a| when
 * the difference fits in int64; outside that range the score is
 * approximate, which is fine — a, b, c are still the only candidates and
 * the selection stays deterministic. */
static inline uint64_t tdc_float_paeth_ordered(uint64_t left, uint64_t up,
                                               uint64_t upleft) {
    uint64_t p   = left + up - upleft;
    uint64_t pa_d = p - left,   pa_n = left   - p;
    uint64_t pb_d = p - up,     pb_n = up     - p;
    uint64_t pc_d = p - upleft, pc_n = upleft - p;
    uint64_t pa = (pa_d <= pa_n) ? pa_d : pa_n;
    uint64_t pb = (pb_d <= pb_n) ? pb_d : pb_n;
    uint64_t pc = (pc_d <= pc_n) ? pc_d : pc_n;
    if (pa <= pb && pa <= pc) return left;
    if (pb <= pc) return up;
    return upleft;
}

#endif /* TDC_MODEL_FLOAT_PRED_HELPERS_H */
