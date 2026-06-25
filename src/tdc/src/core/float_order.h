/*
 * float_order.h — ordered-integer mapping for IEEE 754 floats
 *
 * Maps float bit patterns to unsigned integers that preserve numerical
 * ordering:  if a < b (numerically), then to_ordered(a) < to_ordered(b)
 * (as unsigned integers). This makes unsigned subtraction of ordered
 * representations produce small values for nearby floats — exactly what
 * delta / predictor models need for lossless float compression.
 *
 * The mapping:
 *   positive float (sign bit 0): flip sign bit  → large unsigned
 *   negative float (sign bit 1): flip all bits   → small unsigned
 *
 * NaN ordering is implementation-defined (NaN has no numerical order)
 * but the mapping is still bijective: every bit pattern round-trips.
 *
 * Also provides f16 <-> f32 conversion helpers (pure bit manipulation,
 * no library dependency, no _Float16 requirement).
 */

#ifndef TDC_FLOAT_ORDER_H
#define TDC_FLOAT_ORDER_H

#include <stdint.h>
#include <string.h>

/* ---- f16 ordered mapping ------------------------------------------------ */

static inline uint16_t tdc_f16_to_ordered(uint16_t bits) {
    return (bits & 0x8000u) ? ~bits : (bits ^ 0x8000u);
}

static inline uint16_t tdc_ordered_to_f16(uint16_t s) {
    return (s & 0x8000u) ? (s ^ 0x8000u) : ~s;
}

/* ---- f32 ordered mapping ------------------------------------------------ */

static inline uint32_t tdc_f32_to_ordered(uint32_t bits) {
    return (bits & 0x80000000u) ? ~bits : (bits ^ 0x80000000u);
}

static inline uint32_t tdc_ordered_to_f32(uint32_t s) {
    return (s & 0x80000000u) ? (s ^ 0x80000000u) : ~s;
}

/* ---- f64 ordered mapping ------------------------------------------------ */

static inline uint64_t tdc_f64_to_ordered(uint64_t bits) {
    return (bits & 0x8000000000000000ull) ? ~bits : (bits ^ 0x8000000000000000ull);
}

static inline uint64_t tdc_ordered_to_f64(uint64_t s) {
    return (s & 0x8000000000000000ull) ? (s ^ 0x8000000000000000ull) : ~s;
}

/* ---- Convenience: load float bits via memcpy ---------------------------- */

static inline uint16_t tdc_load_f16_bits(const uint8_t *p) {
    uint16_t b; memcpy(&b, p, 2); return b;
}

static inline uint32_t tdc_load_f32_bits(const uint8_t *p) {
    uint32_t b; memcpy(&b, p, 4); return b;
}

static inline uint64_t tdc_load_f64_bits(const uint8_t *p) {
    uint64_t b; memcpy(&b, p, 8); return b;
}

static inline void tdc_store_u16(uint8_t *p, uint16_t v) {
    memcpy(p, &v, 2);
}

static inline void tdc_store_u32(uint8_t *p, uint32_t v) {
    memcpy(p, &v, 4);
}

static inline void tdc_store_u64(uint8_t *p, uint64_t v) {
    memcpy(p, &v, 8);
}

/* ---- f16 <-> f32 conversion --------------------------------------------- */
/*
 * IEEE 754 binary16: 1 sign + 5 exponent + 10 mantissa
 * IEEE 754 binary32: 1 sign + 8 exponent + 23 mantissa
 *
 * These are pure bit-manipulation conversions, no library dependency.
 * Handles: normals, subnormals, ±0, ±Inf, NaN (quiet NaN preserved,
 * signaling NaN may become quiet — this is standard behavior).
 */

static inline float tdc_f16_to_f32(uint16_t h) {
    uint32_t sign = (uint32_t)(h >> 15) << 31;
    uint32_t exp  = (h >> 10) & 0x1Fu;
    uint32_t mant = h & 0x03FFu;
    uint32_t result;

    if (exp == 0u) {
        if (mant == 0u) {
            /* ±0 */
            result = sign;
        } else {
            /* Subnormal f16 → normal f32. Normalize by shifting mantissa
             * up until the implicit leading 1 is in position. */
            exp = 1u;
            while ((mant & 0x0400u) == 0u) {
                mant <<= 1;
                exp++;
            }
            mant &= 0x03FFu;           /* remove the leading 1 */
            result = sign | ((127u - 15u + 1u - exp) << 23) | (mant << 13);
        }
    } else if (exp == 0x1Fu) {
        /* Inf or NaN */
        result = sign | 0x7F800000u | (mant << 13);
    } else {
        /* Normal: rebias exponent from f16 bias (15) to f32 bias (127) */
        result = sign | ((exp + 127u - 15u) << 23) | (mant << 13);
    }

    float f;
    memcpy(&f, &result, 4);
    return f;
}

static inline uint16_t tdc_f32_to_f16(float f) {
    uint32_t bits;
    memcpy(&bits, &f, 4);

    uint16_t sign = (uint16_t)((bits >> 16) & 0x8000u);
    int32_t  exp  = (int32_t)((bits >> 23) & 0xFFu) - 127 + 15;
    uint32_t mant = bits & 0x007FFFFFu;

    if (((bits >> 23) & 0xFFu) == 0xFFu) {
        /* Inf or NaN */
        if (mant == 0u)
            return sign | 0x7C00u;              /* ±Inf */
        else
            return sign | 0x7C00u | (uint16_t)(mant >> 13) | 1u; /* NaN */
    }

    if (exp >= 0x1F) {
        /* Overflow → ±Inf */
        return sign | 0x7C00u;
    }

    if (exp <= 0) {
        /* Underflow → subnormal or zero */
        if (exp < -10)
            return sign;                        /* too small → ±0 */
        mant |= 0x00800000u;                    /* restore implicit 1 */
        uint32_t shift = (uint32_t)(1 - exp);
        /* Round to nearest even */
        uint32_t half = 1u << (shift + 12);
        uint32_t round_bit = (mant >> (shift + 13)) & 1u;
        mant += half - 1u + round_bit;
        return sign | (uint16_t)(mant >> (shift + 13));
    }

    /* Normal: round to nearest even */
    uint32_t round_bit = (mant >> 12) & 1u;
    mant += 0x00000FFFu + round_bit;
    if (mant & 0x00800000u) {
        mant = 0;
        exp++;
        if (exp >= 0x1F)
            return sign | 0x7C00u;              /* rounded to Inf */
    }
    return sign | (uint16_t)((uint32_t)exp << 10) | (uint16_t)(mant >> 13);
}

#endif /* TDC_FLOAT_ORDER_H */
