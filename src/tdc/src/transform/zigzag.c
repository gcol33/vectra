/*
 * src/transform/zigzag.c
 *
 * TDC_XFORM_ZIGZAG — signed-to-unsigned interleaved mapping that puts
 * small magnitudes (positive or negative) near zero in the unsigned
 * codomain:
 *
 *      0 -> 0,  -1 -> 1,  1 -> 2,  -2 -> 3,  2 -> 4, ...
 *
 * Forward kernel (per element of width N bits, with arithmetic shift):
 *
 *      zigzag(x)   = (uN)((x << 1) ^ (x >> (N - 1)))
 *      unzigzag(z) = (sN)((z >> 1) ^ -(sN)(z & 1))
 *
 * The forward expression assumes a two's-complement signed shift; C
 * leaves `int >> n` implementation-defined for negative operands, so we
 * implement the kernel via the equivalent unsigned form
 *
 *      zigzag(x) = ((uN)x << 1) ^ ((uN)(int_least_t)(x >> (N - 1)))
 *
 * which side-steps both UB on the left shift of a negative signed value
 * (UB in C11 6.5.7p4) and the implementation-defined right shift. The
 * helpers below are written so the optimizer collapses them to one
 * shift + one xor on every supported target.
 *
 * Accepts I8/I16/I32/I64. Output dtype is the matching unsigned type
 * (U8/U16/U32/U64). Output width is identical to input width — zigzag
 * is a pure relabelling, never a length change. Output buffer size is
 * therefore exactly src_size, which is what tdc_buf_reserve is asked
 * to grow to.
 *
 * NEW in tdc v0 — vectra never had an explicit zigzag step (it relied
 * on LZ to handle signed bytes). Adding zigzag in front of LZ should
 * improve compression of residual streams from delta and 2D predictors,
 * because LZ's literal stream tokenizes by raw byte and the high bytes
 * of small negative integers are 0xFF... rather than 0x00.
 *
 * Decode dtype convention:
 *   in_dtype passed to decode is the *original signed* dtype (matching
 *   the encoder's in_dtype), not the dtype of the source bytes. The
 *   chain driver knows that zigzag flips signedness internally and
 *   routes dtypes accordingly. This matches the convention established
 *   by src/transform/quantize.c — every transform's in_dtype is the
 *   encoder-side input dtype on both encode and decode, never the
 *   dtype of the on-the-wire bytes.
 *
 * Properties:
 *   accepted_dtypes = I8 | I16 | I32 | I64
 *   is_lossy        = 0      (perfectly reversible)
 *   can_inplace     = 1      (kernel is element-local)
 */

#include "tdc/transform.h"
#include "transform_internal.h"
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Per-width kernels -------------------------------------------------- */
/*
 * memcpy-based load/store avoids -Wcast-align on strict-alignment targets
 * (the input buffer is a flat uint8_t * and need not be aligned to elem
 * size). The compiler folds the 1/2/4/8-byte memcpys into single loads /
 * stores on x86_64 and aarch64.
 */

static inline uint8_t  zigzag_enc8 (int8_t  v) {
    return (uint8_t) (((uint8_t) v << 1) ^ (uint8_t) -(int8_t) ((uint8_t) v >> 7));
}
static inline uint16_t zigzag_enc16(int16_t v) {
    return (uint16_t)(((uint16_t)v << 1) ^ (uint16_t)-(int16_t)((uint16_t)v >> 15));
}
static inline uint32_t zigzag_enc32(int32_t v) {
    return ((uint32_t)v << 1) ^ (uint32_t)-(int32_t)((uint32_t)v >> 31);
}
static inline uint64_t zigzag_enc64(int64_t v) {
    return ((uint64_t)v << 1) ^ (uint64_t)-(int64_t)((uint64_t)v >> 63);
}

static inline int8_t   zigzag_dec8 (uint8_t  z) {
    return (int8_t) ((z >> 1) ^ (uint8_t) -(int8_t) (z & 1u));
}
static inline int16_t  zigzag_dec16(uint16_t z) {
    return (int16_t)((z >> 1) ^ (uint16_t)-(int16_t)(z & 1u));
}
static inline int32_t  zigzag_dec32(uint32_t z) {
    return (int32_t)((z >> 1) ^ (uint32_t)-(int32_t)(z & 1u));
}
static inline int64_t  zigzag_dec64(uint64_t z) {
    return (int64_t)((z >> 1) ^ (uint64_t)-(int64_t)(z & 1u));
}

/* Map signed dtype id to its unsigned counterpart of the same width. */
static tdc_dtype zigzag_unsigned_of(tdc_dtype dt) {
    switch (dt) {
        case TDC_DT_I8:  return TDC_DT_U8;
        case TDC_DT_I16: return TDC_DT_U16;
        case TDC_DT_I32: return TDC_DT_U32;
        case TDC_DT_I64: return TDC_DT_U64;
        default:         return (tdc_dtype)0;
    }
}

/* ----- Macro-generated encode/decode loops -------------------------------- */
/*
 * Each width shares the same loop shape: load via memcpy, apply the
 * zigzag kernel, store via memcpy. The macro generates the switch cases
 * to avoid maintaining four identical copies per direction.
 */

#define ZIGZAG_ENC_CASE(DT, ST, UT, W, ENC_FN)                \
    case DT: {                                                  \
        for (size_t i = 0; i < n; ++i) {                        \
            ST v; memcpy(&v, src + i * (W), (W));               \
            UT z = ENC_FN(v);                                    \
            memcpy(dst->data + i * (W), &z, (W));               \
        }                                                        \
        break;                                                   \
    }

#define ZIGZAG_DEC_CASE(DT, ST, UT, W, DEC_FN)                \
    case DT: {                                                  \
        for (size_t i = 0; i < n; ++i) {                        \
            UT z; memcpy(&z, src + i * (W), (W));               \
            ST v = DEC_FN(z);                                    \
            memcpy(dst + i * (W), &v, (W));                     \
        }                                                        \
        break;                                                   \
    }

/* ----- Encode ------------------------------------------------------------- */

static tdc_status zigzag_encode(const uint8_t *src, size_t src_size,
                                tdc_dtype      in_dtype,
                                const void    *params,
                                tdc_buffer    *dst,
                                tdc_dtype     *out_dtype) {
    (void)params;
    if (!dst || !dst->realloc_fn) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;

    tdc_dtype udt = zigzag_unsigned_of(in_dtype);
    if (udt == (tdc_dtype)0) return TDC_E_DTYPE;

    size_t elem_size = tdc_dtype_size(in_dtype);
    if (elem_size == 0) return TDC_E_DTYPE;
    if (src_size % elem_size != 0) return TDC_E_INVAL;

    tdc_status st = tdc_buf_reserve(dst, src_size);
    if (st != TDC_OK) return st;

    if (out_dtype) *out_dtype = udt;

    if (src_size == 0) {
        dst->size = 0;
        return TDC_OK;
    }

    size_t n = src_size / elem_size;

    switch (in_dtype) {
        ZIGZAG_ENC_CASE(TDC_DT_I8,  int8_t,  uint8_t,  1u, zigzag_enc8)
        ZIGZAG_ENC_CASE(TDC_DT_I16, int16_t, uint16_t, 2u, zigzag_enc16)
        ZIGZAG_ENC_CASE(TDC_DT_I32, int32_t, uint32_t, 4u, zigzag_enc32)
        ZIGZAG_ENC_CASE(TDC_DT_I64, int64_t, uint64_t, 8u, zigzag_enc64)
        default:
            return TDC_E_DTYPE; /* unreachable: filtered above */
    }

    dst->size = src_size;
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status zigzag_decode(const uint8_t *src, size_t src_size,
                                tdc_dtype      in_dtype,
                                const void    *params,
                                uint8_t       *dst, size_t dst_size,
                                tdc_dtype     *out_dtype) {
    (void)params;
    if (src_size > 0 && !src) return TDC_E_INVAL;
    if (dst_size > 0 && !dst) return TDC_E_INVAL;

    if (zigzag_unsigned_of(in_dtype) == (tdc_dtype)0) return TDC_E_DTYPE;

    size_t elem_size = tdc_dtype_size(in_dtype);
    if (elem_size == 0) return TDC_E_DTYPE;
    if (src_size % elem_size != 0) return TDC_E_CORRUPT;
    if (dst_size != src_size)      return TDC_E_CORRUPT;

    if (out_dtype) *out_dtype = in_dtype;
    if (src_size == 0) return TDC_OK;

    size_t n = src_size / elem_size;

    switch (in_dtype) {
        ZIGZAG_DEC_CASE(TDC_DT_I8,  int8_t,  uint8_t,  1u, zigzag_dec8)
        ZIGZAG_DEC_CASE(TDC_DT_I16, int16_t, uint16_t, 2u, zigzag_dec16)
        ZIGZAG_DEC_CASE(TDC_DT_I32, int32_t, uint32_t, 4u, zigzag_dec32)
        ZIGZAG_DEC_CASE(TDC_DT_I64, int64_t, uint64_t, 8u, zigzag_dec64)
        default:
            return TDC_E_DTYPE; /* unreachable */
    }

    return TDC_OK;
}

#undef ZIGZAG_ENC_CASE
#undef ZIGZAG_DEC_CASE

/* ----- Vtable ------------------------------------------------------------- */

#define ZIGZAG_DTYPE_BIT(dt) (1u << (uint32_t)(dt))
#define ZIGZAG_ACCEPTED_DTYPES (        \
    ZIGZAG_DTYPE_BIT(TDC_DT_I8)  |      \
    ZIGZAG_DTYPE_BIT(TDC_DT_I16) |      \
    ZIGZAG_DTYPE_BIT(TDC_DT_I32) |      \
    ZIGZAG_DTYPE_BIT(TDC_DT_I64))

const tdc_xform_vt tdc_xform_zigzag_vt = {
    .id              = TDC_XFORM_ZIGZAG,
    .name            = "zigzag",
    .accepted_dtypes = ZIGZAG_ACCEPTED_DTYPES,
    .can_inplace     = 1,
    .is_lossy        = 0,
    .encode          = zigzag_encode,
    .decode          = zigzag_decode,
};
