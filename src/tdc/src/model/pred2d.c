/*
 * src/model/pred2d.c
 *
 * TDC_MODEL_PRED_2D — 2D spatial predictor family for RASTER_2D blocks.
 *
 * Predictor kinds (tdc_pred2d_kind):
 *   LEFT     — pred = val[r][c-1]
 *   UP       — pred = val[r-1][c]
 *   AVERAGE  — pred = (left + up) / 2     (C truncation, vectra-compatible)
 *   PAETH    — PNG-style: of {left, up, upleft}, pick the one closest to
 *              the linear predictor p = left + up - upleft
 *   AUTO     — encoder picks the LEFT/UP/AVERAGE/PAETH variant that
 *              minimizes sum of |residual| on a sample of up to 10000
 *              elements
 *
 * PLANE is intentionally NOT in this file. It needs side metadata
 * (per-tile coefficients), a different params struct (tdc_plane2d_params),
 * and is large enough to live in its own src/model/plane2d.c when it
 * lands.
 *
 * Accepted dtypes: i8, i16, i32, i64, u8, u16, u32, u64, f16, f32, f64.
 * The 64-bit integer kernels reuse paeth64 for the working arithmetic;
 * `a + b - c` can overflow at the extremes of the int64 range, but no
 * realistic raster has neighboring pixels separated by ~INT64_MAX, and
 * the residual store goes through the unsigned narrow counterpart so
 * encode and decode wrap symmetrically. Floats use the ordered-uint
 * mapping path described under "Float predictor path" below.
 *
 * Accepted layouts: RASTER_2D only. shape.rank must be 2 with row-major
 * contiguous storage:
 *   ny = shape.dim[0]   (number of rows)
 *   nx = shape.dim[1]   (row length)
 *   idx = row * nx + col
 *
 * Residual dtype: same as input. The hot path uses typed kernels with a
 * signed wide working type (int32_t for 8/16-bit dtypes, int64_t for
 * 32-bit dtypes) — wide enough to keep `a + b - c` overflow-free in the
 * Paeth case. Residuals are written through the unsigned narrow
 * counterpart of the dtype (well-defined modular conversion in C),
 * matching the IDB-avoiding store convention of the original int64
 * reference path. Decode is the modular inverse and round-trips
 * exactly because every operation is modular at width N once written
 * back through the typed store.
 *
 * Side metadata: 1 byte = the resolved predictor kind (LEFT, UP, AVERAGE,
 * or PAETH — never AUTO). Even when the caller passes a non-AUTO kind,
 * the resolved kind is recorded so the decoder dispatches identically
 * regardless of how the encoder selected it. This is the simplest
 * forward-compatible shape and matches the design rule that decoders
 * never re-derive encoder choices.
 *
 * Validity bitmap: ignored, same convention as every other v0 model. The
 * encode driver carries the validity bitmap around the model stage; the
 * model itself only round-trips bytes.
 *
 * Source: vectra/src/vtr_codec.c lines 1572-1813 (paeth_predict,
 * spatial_encode_int, spatial_decode_int, auto_select_predictor). The
 * predictor kernels and the auto-select scoring loop are preserved
 * conceptually; the outer wrapping is rewritten for tdc allocation,
 * error returns, dtype generality, side metadata convention, and per-
 * dtype kernel specialization. Vectra's path was int64-only and
 * longjmp'd on alloc failure.
 */

#include "tdc/model.h"
#include "tdc/codec.h"
#include "model_internal.h"
#include "model_load_store.h"
#include "../core/buffer.h"
#include "../core/float_order.h"
#include "float_pred_helpers.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Optional SIMD: vectorized UP-predictor row operations -------------- *
 * The UP predictor has no left-pixel dependency — each output element only
 * depends on the element directly above. Both encode (sub) and decode (add)
 * are therefore fully data-parallel within a row and vectorize cleanly.
 *
 * We support four element widths (1/2/4/8 bytes). Byte-level subtraction is
 * NOT used for multi-byte elements: borrows cross byte boundaries, so we
 * dispatch on element width and use the matching integer intrinsic width.
 *
 * Tail handling is inside each helper: the bulk SIMD loop processes as many
 * full vectors as possible; the remainder falls through to the scalar tail.
 *
 * LEFT/AVERAGE/PAETH are not vectorized here because they have a left-pixel
 * read-after-write dependency on decode (each pixel reads dst[col-1] which
 * was just written). Only UP is dependency-free.
 */
#if defined(__SSE2__) || defined(_M_X64) || \
    (defined(_M_IX86_FP) && _M_IX86_FP >= 2)
#  include <emmintrin.h>
#  define TDC_PRED2D_HAVE_SSE2 1
#endif

/* AVX2 is opt-in: the 8-row PAETH wavefront below uses _mm256_* intrinsics
 * for double the ILP of the 4-row SSE2 path. CMake's TDC_ENABLE_AVX2 option
 * passes /arch:AVX2 (MSVC) or -mavx2 (GCC/Clang) which defines __AVX2__.
 * Without it we fall back transparently to the SSE2 wf4 kernel. */
#if defined(__AVX2__)
#  include <immintrin.h>
#  define TDC_PRED2D_HAVE_AVX2 1
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#  include <arm_neon.h>
#  define TDC_PRED2D_HAVE_NEON 1
#endif

/* --- SSE2 UP helpers ------------------------------------------------------- */
#ifdef TDC_PRED2D_HAVE_SSE2

/* Encode: res[i] = cur[i] - prev[i] at element width `esz`. */
static void pred2d_up_enc_row_sse2(uint8_t *res, const uint8_t *cur,
                                   const uint8_t *prev,
                                   size_t nbytes, size_t esz) {
    size_t i = 0;
    if (esz == 1) {
        for (; i + 16 <= nbytes; i += 16) {
            __m128i a = _mm_loadu_si128((const __m128i *)(cur  + i));
            __m128i b = _mm_loadu_si128((const __m128i *)(prev + i));
            _mm_storeu_si128((__m128i *)(res + i), _mm_sub_epi8(a, b));
        }
        for (; i < nbytes; ++i) res[i] = (uint8_t)(cur[i] - prev[i]);
    } else if (esz == 2) {
        for (; i + 16 <= nbytes; i += 16) {
            __m128i a = _mm_loadu_si128((const __m128i *)(cur  + i));
            __m128i b = _mm_loadu_si128((const __m128i *)(prev + i));
            _mm_storeu_si128((__m128i *)(res + i), _mm_sub_epi16(a, b));
        }
        for (; i < nbytes; i += 2) {
            uint16_t a, b;
            memcpy(&a, cur  + i, 2);
            memcpy(&b, prev + i, 2);
            uint16_t r = (uint16_t)(a - b);
            memcpy(res + i, &r, 2);
        }
    } else if (esz == 4) {
        for (; i + 16 <= nbytes; i += 16) {
            __m128i a = _mm_loadu_si128((const __m128i *)(cur  + i));
            __m128i b = _mm_loadu_si128((const __m128i *)(prev + i));
            _mm_storeu_si128((__m128i *)(res + i), _mm_sub_epi32(a, b));
        }
        for (; i < nbytes; i += 4) {
            uint32_t a, b;
            memcpy(&a, cur  + i, 4);
            memcpy(&b, prev + i, 4);
            uint32_t r = a - b;
            memcpy(res + i, &r, 4);
        }
    } else { /* esz == 8 */
        for (; i + 16 <= nbytes; i += 16) {
            __m128i a = _mm_loadu_si128((const __m128i *)(cur  + i));
            __m128i b = _mm_loadu_si128((const __m128i *)(prev + i));
            _mm_storeu_si128((__m128i *)(res + i), _mm_sub_epi64(a, b));
        }
        for (; i < nbytes; i += 8) {
            uint64_t a, b;
            memcpy(&a, cur  + i, 8);
            memcpy(&b, prev + i, 8);
            uint64_t r = a - b;
            memcpy(res + i, &r, 8);
        }
    }
}

/* Decode: dst[i] = res[i] + prev[i] at element width `esz`. */
static void pred2d_up_dec_row_sse2(uint8_t *dst, const uint8_t *res,
                                   const uint8_t *prev,
                                   size_t nbytes, size_t esz) {
    size_t i = 0;
    if (esz == 1) {
        for (; i + 16 <= nbytes; i += 16) {
            __m128i a = _mm_loadu_si128((const __m128i *)(res  + i));
            __m128i b = _mm_loadu_si128((const __m128i *)(prev + i));
            _mm_storeu_si128((__m128i *)(dst + i), _mm_add_epi8(a, b));
        }
        for (; i < nbytes; ++i) dst[i] = (uint8_t)(res[i] + prev[i]);
    } else if (esz == 2) {
        for (; i + 16 <= nbytes; i += 16) {
            __m128i a = _mm_loadu_si128((const __m128i *)(res  + i));
            __m128i b = _mm_loadu_si128((const __m128i *)(prev + i));
            _mm_storeu_si128((__m128i *)(dst + i), _mm_add_epi16(a, b));
        }
        for (; i < nbytes; i += 2) {
            uint16_t a, b;
            memcpy(&a, res  + i, 2);
            memcpy(&b, prev + i, 2);
            uint16_t r = (uint16_t)(a + b);
            memcpy(dst + i, &r, 2);
        }
    } else if (esz == 4) {
        for (; i + 16 <= nbytes; i += 16) {
            __m128i a = _mm_loadu_si128((const __m128i *)(res  + i));
            __m128i b = _mm_loadu_si128((const __m128i *)(prev + i));
            _mm_storeu_si128((__m128i *)(dst + i), _mm_add_epi32(a, b));
        }
        for (; i < nbytes; i += 4) {
            uint32_t a, b;
            memcpy(&a, res  + i, 4);
            memcpy(&b, prev + i, 4);
            uint32_t r = a + b;
            memcpy(dst + i, &r, 4);
        }
    } else { /* esz == 8 */
        for (; i + 16 <= nbytes; i += 16) {
            __m128i a = _mm_loadu_si128((const __m128i *)(res  + i));
            __m128i b = _mm_loadu_si128((const __m128i *)(prev + i));
            _mm_storeu_si128((__m128i *)(dst + i), _mm_add_epi64(a, b));
        }
        for (; i < nbytes; i += 8) {
            uint64_t a, b;
            memcpy(&a, res  + i, 8);
            memcpy(&b, prev + i, 8);
            uint64_t r = a + b;
            memcpy(dst + i, &r, 8);
        }
    }
}
/* --- SSE2 PAETH helper (4 × int32 lanes) ---------------------------------- *
 * Vectorized paeth across 4 independent int32 lanes.  Same algorithm and
 * tie-break order as scalar paeth32 — see its comments.  Used by the
 * wavefront PAETH decoder to compute 4 staggered rows in one operation.
 *
 * SSE2 does not have _mm_abs_epi32 (SSSE3), so abs(x) is implemented as:
 *     sign = x >> 31  (arithmetic shift: all-1s if negative)
 *     abs  = (x ^ sign) - sign
 */
static inline __m128i paeth_4x_sse2(__m128i a, __m128i b, __m128i c) {
    __m128i bc = _mm_sub_epi32(b, c);
    __m128i ac = _mm_sub_epi32(a, c);

    /* pa = |b - c| */
    __m128i bc_s = _mm_srai_epi32(bc, 31);
    __m128i pa   = _mm_sub_epi32(_mm_xor_si128(bc, bc_s), bc_s);

    /* pb = |a - c| */
    __m128i ac_s = _mm_srai_epi32(ac, 31);
    __m128i pb   = _mm_sub_epi32(_mm_xor_si128(ac, ac_s), ac_s);

    /* pc = |(b - c) + (a - c)| */
    __m128i pcs  = _mm_add_epi32(bc, ac);
    __m128i pcs_s = _mm_srai_epi32(pcs, 31);
    __m128i pc   = _mm_sub_epi32(_mm_xor_si128(pcs, pcs_s), pcs_s);

    /* r = (pb <= pc) ? b : c
     * pb > pc is the "pick c" mask; otherwise pick b. */
    __m128i pb_gt_pc = _mm_cmpgt_epi32(pb, pc);
    __m128i r = _mm_or_si128(_mm_andnot_si128(pb_gt_pc, b),
                             _mm_and_si128(pb_gt_pc, c));

    /* result = (pa <= pb && pa <= pc) ? a : r
     * not_a = (pa > pb) | (pa > pc) */
    __m128i not_a = _mm_or_si128(_mm_cmpgt_epi32(pa, pb),
                                 _mm_cmpgt_epi32(pa, pc));
    return _mm_or_si128(_mm_andnot_si128(not_a, a),
                        _mm_and_si128(not_a, r));
}
#endif /* TDC_PRED2D_HAVE_SSE2 */

/* --- AVX2 PAETH helper (8 × int32 lanes) ---------------------------------- *
 * Same algorithm and tie-break order as scalar paeth32 — 8 independent
 * lanes per __m256i. Used by the AVX2 wf8 wavefront decoder.
 *
 * AVX2 has _mm256_abs_epi32 (single instruction) so the SSE2 xor-shift
 * trick is unnecessary here.
 *
 * Tie-break MUST match paeth32 bit-for-bit:
 *     return (pa <= pb && pa <= pc) ? a
 *          : (pb <= pc)             ? b
 *                                   : c;
 * Implemented via _mm256_blendv_epi8 over the >  masks, identical to the
 * SSE2 helper above.
 */
#ifdef TDC_PRED2D_HAVE_AVX2
static inline __m256i paeth_8x_avx2(__m256i a, __m256i b, __m256i c) {
    __m256i bc = _mm256_sub_epi32(b, c);
    __m256i ac = _mm256_sub_epi32(a, c);

    __m256i pa  = _mm256_abs_epi32(bc);              /* |b - c| */
    __m256i pb  = _mm256_abs_epi32(ac);              /* |a - c| */
    __m256i pc  = _mm256_abs_epi32(_mm256_add_epi32(bc, ac)); /* |(b-c)+(a-c)| */

    /* r = (pb > pc) ? c : b */
    __m256i pb_gt_pc = _mm256_cmpgt_epi32(pb, pc);
    __m256i r = _mm256_blendv_epi8(b, c, pb_gt_pc);

    /* not_a = (pa > pb) | (pa > pc); result = not_a ? r : a */
    __m256i not_a = _mm256_or_si256(_mm256_cmpgt_epi32(pa, pb),
                                    _mm256_cmpgt_epi32(pa, pc));
    return _mm256_blendv_epi8(a, r, not_a);
}
#endif /* TDC_PRED2D_HAVE_AVX2 */

/* --- NEON UP helpers ------------------------------------------------------- */
#ifdef TDC_PRED2D_HAVE_NEON

static void pred2d_up_enc_row_neon(uint8_t *res, const uint8_t *cur,
                                   const uint8_t *prev,
                                   size_t nbytes, size_t esz) {
    size_t i = 0;
    if (esz == 1) {
        for (; i + 16 <= nbytes; i += 16) {
            uint8x16_t a = vld1q_u8(cur  + i);
            uint8x16_t b = vld1q_u8(prev + i);
            vst1q_u8(res + i, vsubq_u8(a, b));
        }
        for (; i < nbytes; ++i) res[i] = (uint8_t)(cur[i] - prev[i]);
    } else if (esz == 2) {
        for (; i + 16 <= nbytes; i += 16) {
            uint16x8_t a = vld1q_u16((const uint16_t *)(cur  + i));
            uint16x8_t b = vld1q_u16((const uint16_t *)(prev + i));
            vst1q_u16((uint16_t *)(res + i), vsubq_u16(a, b));
        }
        for (; i < nbytes; i += 2) {
            uint16_t a, b;
            memcpy(&a, cur  + i, 2);
            memcpy(&b, prev + i, 2);
            uint16_t r = (uint16_t)(a - b);
            memcpy(res + i, &r, 2);
        }
    } else if (esz == 4) {
        for (; i + 16 <= nbytes; i += 16) {
            uint32x4_t a = vld1q_u32((const uint32_t *)(cur  + i));
            uint32x4_t b = vld1q_u32((const uint32_t *)(prev + i));
            vst1q_u32((uint32_t *)(res + i), vsubq_u32(a, b));
        }
        for (; i < nbytes; i += 4) {
            uint32_t a, b;
            memcpy(&a, cur  + i, 4);
            memcpy(&b, prev + i, 4);
            uint32_t r = a - b;
            memcpy(res + i, &r, 4);
        }
    } else { /* esz == 8 */
        for (; i + 16 <= nbytes; i += 16) {
            uint64x2_t a = vld1q_u64((const uint64_t *)(cur  + i));
            uint64x2_t b = vld1q_u64((const uint64_t *)(prev + i));
            vst1q_u64((uint64_t *)(res + i), vsubq_u64(a, b));
        }
        for (; i < nbytes; i += 8) {
            uint64_t a, b;
            memcpy(&a, cur  + i, 8);
            memcpy(&b, prev + i, 8);
            uint64_t r = a - b;
            memcpy(res + i, &r, 8);
        }
    }
}

static void pred2d_up_dec_row_neon(uint8_t *dst, const uint8_t *res,
                                   const uint8_t *prev,
                                   size_t nbytes, size_t esz) {
    size_t i = 0;
    if (esz == 1) {
        for (; i + 16 <= nbytes; i += 16) {
            uint8x16_t a = vld1q_u8(res  + i);
            uint8x16_t b = vld1q_u8(prev + i);
            vst1q_u8(dst + i, vaddq_u8(a, b));
        }
        for (; i < nbytes; ++i) dst[i] = (uint8_t)(res[i] + prev[i]);
    } else if (esz == 2) {
        for (; i + 16 <= nbytes; i += 16) {
            uint16x8_t a = vld1q_u16((const uint16_t *)(res  + i));
            uint16x8_t b = vld1q_u16((const uint16_t *)(prev + i));
            vst1q_u16((uint16_t *)(dst + i), vaddq_u16(a, b));
        }
        for (; i < nbytes; i += 2) {
            uint16_t a, b;
            memcpy(&a, res  + i, 2);
            memcpy(&b, prev + i, 2);
            uint16_t r = (uint16_t)(a + b);
            memcpy(dst + i, &r, 2);
        }
    } else if (esz == 4) {
        for (; i + 16 <= nbytes; i += 16) {
            uint32x4_t a = vld1q_u32((const uint32_t *)(res  + i));
            uint32x4_t b = vld1q_u32((const uint32_t *)(prev + i));
            vst1q_u32((uint32_t *)(dst + i), vaddq_u32(a, b));
        }
        for (; i < nbytes; i += 4) {
            uint32_t a, b;
            memcpy(&a, res  + i, 4);
            memcpy(&b, prev + i, 4);
            uint32_t r = a + b;
            memcpy(dst + i, &r, 4);
        }
    } else { /* esz == 8 */
        for (; i + 16 <= nbytes; i += 16) {
            uint64x2_t a = vld1q_u64((const uint64_t *)(res  + i));
            uint64x2_t b = vld1q_u64((const uint64_t *)(prev + i));
            vst1q_u64((uint64_t *)(dst + i), vaddq_u64(a, b));
        }
        for (; i < nbytes; i += 8) {
            uint64_t a, b;
            memcpy(&a, res  + i, 8);
            memcpy(&b, prev + i, 8);
            uint64_t r = a + b;
            memcpy(dst + i, &r, 8);
        }
    }
}
#endif /* TDC_PRED2D_HAVE_NEON */

/* ----- Acceptance bitmasks ----------------------------------------------- */

#define PRED2D_ACCEPTED_DTYPES (         \
    TDC_DT_BIT(TDC_DT_I8)  |             \
    TDC_DT_BIT(TDC_DT_I16) |             \
    TDC_DT_BIT(TDC_DT_I32) |             \
    TDC_DT_BIT(TDC_DT_I64) |             \
    TDC_DT_BIT(TDC_DT_U8)  |             \
    TDC_DT_BIT(TDC_DT_U16) |             \
    TDC_DT_BIT(TDC_DT_U32) |             \
    TDC_DT_BIT(TDC_DT_U64) |             \
    TDC_DT_BIT(TDC_DT_F16) |             \
    TDC_DT_BIT(TDC_DT_F32) |             \
    TDC_DT_BIT(TDC_DT_F64))

#define PRED2D_ACCEPTED_LAYOUTS TDC_LAYOUT_BIT(TDC_LAYOUT_RASTER_2D)

static int pred2d_dtype_accepted(tdc_dtype dt) {
    return tdc_model_dtype_accepted(PRED2D_ACCEPTED_DTYPES, dt);
}

/* ----- Paeth (typed) ----------------------------------------------------- */
/*
 * Two width-specialized Paeth helpers. Marked inline so the typed sweeps
 * below can fold the call away. The ternary chains compile to branch-
 * free cmov / csel sequences on x86_64 / aarch64 under -O2 and above.
 *
 * Tie-break order matches the original paeth_predict: pa<=pb && pa<=pc
 * → a, else pb<=pc → b, else c. This is the PNG convention and the
 * vectra reference path; preserving it keeps existing on-disk side
 * metadata round-tripping bit-identical regardless of which kernel
 * runs.
 */

static inline int32_t paeth32(int32_t a, int32_t b, int32_t c) {
    /* Algebraic identities for the abs-of-difference terms:
     *   p - a = (a+b-c) - a = b - c
     *   p - b = (a+b-c) - b = a - c
     *   p - c = (a+b-c) - c = (b-c) + (a-c)
     * Hoisting bc and ac as shared subexpressions saves two subtractions
     * and — more importantly — shortens the dependency chain feeding pc.
     */
    int32_t bc = b - c;
    int32_t ac = a - c;
    int32_t pa = bc < 0 ? -bc : bc;
    int32_t pb = ac < 0 ? -ac : ac;
    int32_t pcs = bc + ac;
    int32_t pc = pcs < 0 ? -pcs : pcs;
    int32_t r  = (pb <= pc) ? b : c;
    /* Bitwise & instead of && so neither branch nor short-circuit blocks
     * the compiler from emitting a flat cmov chain. The mask-arithmetic
     * form was tried and benchmarked ~3-5% slower under MSVC /O2 — the
     * compiler already lowers this ternary chain to cmov sequences. */
    return ((pa <= pb) & (pa <= pc)) ? a : r;
}

static inline int64_t paeth64(int64_t a, int64_t b, int64_t c) {
    int64_t p  = a + b - c;
    int64_t pa = p > a ? p - a : a - p;
    int64_t pb = p > b ? p - b : b - p;
    int64_t pc = p > c ? p - c : c - p;
    int64_t r  = (pb <= pc) ? b : c;
    return ((pa <= pb) & (pa <= pc)) ? a : r;
}

/* ----- Typed kernel template --------------------------------------------- */
/*
 * For each accepted dtype we generate a pair of fully-specialized typed
 * encode/decode sweeps. The kind dispatch is hoisted to the top of each
 * function so the inner loops are branch-free on (dtype, kind, boundary).
 * Boundary cases (pixel (0,0), row 0 col >= 1, col 0 row >= 1) are
 * handled in dedicated prologues so the main inner loop has no
 * `(col > 0)` / `(row > 0)` ternaries.
 *
 * Reads from typed pointers to the wide working type W use C's natural
 * integer promotion: signed narrow -> sign-extend, unsigned narrow ->
 * zero-extend. This matches the original int64-based reference path
 * bit-for-bit.
 *
 * Writes go through the unsigned narrow counterpart U of T to keep the
 * narrowing well-defined per C11. Aliasing T<->U through pointer cast
 * is permitted by C11 §6.5p7 (signed/unsigned of the same effective
 * type).
 *
 * Encode has no inter-pixel dependency (all neighbors come from the
 * read-only src buffer) and vectorizes for any predictor. Decode reads
 * `left` from the dst buffer being written, so LEFT/AVERAGE/PAETH
 * decode is inherently scalar; only the UP variant has no left-of
 * dependency and vectorizes.
 */

/* Dispatch shims for the UP SIMD fast path inside DEFINE_PRED2D_TYPED.
 * Exactly one of IF_SSE2/IF_NEON is active; IF_SCALAR is its complement. */
#if defined(TDC_PRED2D_HAVE_SSE2)
#  define IF_SSE2(code)   code
#  define IF_NEON(code)
#  define IF_SCALAR(code)
#elif defined(TDC_PRED2D_HAVE_NEON)
#  define IF_SSE2(code)
#  define IF_NEON(code)   code
#  define IF_SCALAR(code)
#else
#  define IF_SSE2(code)
#  define IF_NEON(code)
#  define IF_SCALAR(code) code
#endif

#define DEFINE_PRED2D_TYPED(SUFFIX, T, U, W, PAETH)                            \
                                                                               \
static void pred2d_enc_##SUFFIX(tdc_pred2d_kind kind,                          \
                                const T *src, T *res,                          \
                                int64_t nx, int64_t ny) {                      \
    if (nx <= 0 || ny <= 0) return;                                            \
                                                                               \
    /* (0,0): all neighbors are 0, residual = src[0]. */                       \
    *(U *)&res[0] = (U)(W)src[0];                                              \
                                                                               \
    /* row 0, col >= 1 — only `left` is in-bounds */                           \
    switch (kind) {                                                            \
        case TDC_PRED2D_LEFT:                                                  \
        case TDC_PRED2D_PAETH:                                                 \
            for (int64_t col = 1; col < nx; ++col) {                           \
                W val  = (W)src[col];                                          \
                W left = (W)src[col - 1];                                      \
                *(U *)&res[col] = (U)(val - left);                             \
            }                                                                  \
            break;                                                             \
        case TDC_PRED2D_UP:                                                    \
            for (int64_t col = 1; col < nx; ++col)                             \
                *(U *)&res[col] = (U)(W)src[col];                              \
            break;                                                             \
        case TDC_PRED2D_AVERAGE:                                               \
            for (int64_t col = 1; col < nx; ++col) {                           \
                W val  = (W)src[col];                                          \
                W left = (W)src[col - 1];                                      \
                *(U *)&res[col] = (U)(val - left / 2);                         \
            }                                                                  \
            break;                                                             \
        default: break;                                                        \
    }                                                                          \
                                                                               \
    /* rows >= 1 */                                                            \
    switch (kind) {                                                            \
        case TDC_PRED2D_LEFT: {                                                \
            for (int64_t row = 1; row < ny; ++row) {                           \
                const T *s0 = src + row * nx;                                  \
                T       *r0 = res + row * nx;                                  \
                *(U *)&r0[0] = (U)(W)s0[0]; /* col 0: left = 0 */              \
                for (int64_t col = 1; col < nx; ++col) {                       \
                    W val  = (W)s0[col];                                       \
                    W left = (W)s0[col - 1];                                   \
                    *(U *)&r0[col] = (U)(val - left);                          \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED2D_UP: {                                                  \
            for (int64_t row = 1; row < ny; ++row) {                           \
                const T *s0 = src + row * nx;                                  \
                const T *s1 = src + (row - 1) * nx;                            \
                T       *r0 = res + row * nx;                                  \
                size_t   _nb = (size_t)nx * sizeof(T);                         \
                size_t   _esz = sizeof(T);                                     \
                (void)_esz; /* used only when SIMD is enabled */               \
                (void)_nb;                                                     \
IF_SSE2(        pred2d_up_enc_row_sse2((uint8_t *)r0,                          \
                    (const uint8_t *)s0, (const uint8_t *)s1, _nb, _esz);)    \
IF_NEON(        pred2d_up_enc_row_neon((uint8_t *)r0,                          \
                    (const uint8_t *)s0, (const uint8_t *)s1, _nb, _esz);)    \
IF_SCALAR(      for (int64_t col = 0; col < nx; ++col) {                       \
                    W val = (W)s0[col];                                        \
                    W up  = (W)s1[col];                                        \
                    *(U *)&r0[col] = (U)(val - up);                            \
                })                                                             \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED2D_AVERAGE: {                                             \
            for (int64_t row = 1; row < ny; ++row) {                           \
                const T *s0 = src + row * nx;                                  \
                const T *s1 = src + (row - 1) * nx;                            \
                T       *r0 = res + row * nx;                                  \
                /* col 0: left = 0 -> pred = up / 2 */                         \
                {                                                              \
                    W val = (W)s0[0];                                          \
                    W up  = (W)s1[0];                                          \
                    *(U *)&r0[0] = (U)(val - up / 2);                          \
                }                                                              \
                for (int64_t col = 1; col < nx; ++col) {                       \
                    W val  = (W)s0[col];                                       \
                    W left = (W)s0[col - 1];                                   \
                    W up   = (W)s1[col];                                       \
                    *(U *)&r0[col] = (U)(val - (left + up) / 2);               \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED2D_PAETH: {                                               \
            for (int64_t row = 1; row < ny; ++row) {                           \
                const T *s0 = src + row * nx;                                  \
                const T *s1 = src + (row - 1) * nx;                            \
                T       *r0 = res + row * nx;                                  \
                /* col 0: left = upleft = 0 -> paeth(0, up, 0) = up */         \
                {                                                              \
                    W val = (W)s0[0];                                          \
                    W up  = (W)s1[0];                                          \
                    *(U *)&r0[0] = (U)(val - up);                              \
                }                                                              \
                for (int64_t col = 1; col < nx; ++col) {                       \
                    W val    = (W)s0[col];                                     \
                    W left   = (W)s0[col - 1];                                 \
                    W up     = (W)s1[col];                                     \
                    W upleft = (W)s1[col - 1];                                 \
                    W pred   = PAETH(left, up, upleft);                        \
                    *(U *)&r0[col] = (U)(val - pred);                          \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        default: break;                                                        \
    }                                                                          \
}                                                                              \
                                                                               \
static void pred2d_dec_##SUFFIX(tdc_pred2d_kind kind,                          \
                                const T *res, T *dst,                          \
                                int64_t nx, int64_t ny) {                      \
    if (nx <= 0 || ny <= 0) return;                                            \
                                                                               \
    *(U *)&dst[0] = (U)(W)res[0];                                              \
                                                                               \
    switch (kind) {                                                            \
        case TDC_PRED2D_LEFT:                                                  \
        case TDC_PRED2D_PAETH:                                                 \
            for (int64_t col = 1; col < nx; ++col) {                           \
                W rv   = (W)res[col];                                          \
                W left = (W)dst[col - 1];                                      \
                *(U *)&dst[col] = (U)(rv + left);                              \
            }                                                                  \
            break;                                                             \
        case TDC_PRED2D_UP:                                                    \
            for (int64_t col = 1; col < nx; ++col)                             \
                *(U *)&dst[col] = (U)(W)res[col];                              \
            break;                                                             \
        case TDC_PRED2D_AVERAGE:                                               \
            for (int64_t col = 1; col < nx; ++col) {                           \
                W rv   = (W)res[col];                                          \
                W left = (W)dst[col - 1];                                      \
                *(U *)&dst[col] = (U)(rv + left / 2);                          \
            }                                                                  \
            break;                                                             \
        default: break;                                                        \
    }                                                                          \
                                                                               \
    switch (kind) {                                                            \
        case TDC_PRED2D_LEFT: {                                                \
            for (int64_t row = 1; row < ny; ++row) {                           \
                const T *r0 = res + row * nx;                                  \
                T       *d0 = dst + row * nx;                                  \
                *(U *)&d0[0] = (U)(W)r0[0];                                    \
                for (int64_t col = 1; col < nx; ++col) {                       \
                    W rv   = (W)r0[col];                                       \
                    W left = (W)d0[col - 1];                                   \
                    *(U *)&d0[col] = (U)(rv + left);                           \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED2D_UP: {                                                  \
            for (int64_t row = 1; row < ny; ++row) {                           \
                const T *r0 = res + row * nx;                                  \
                T       *d0 = dst + row * nx;                                  \
                const T *d1 = dst + (row - 1) * nx;                            \
                size_t   _nb = (size_t)nx * sizeof(T);                         \
                size_t   _esz = sizeof(T);                                     \
                (void)_esz;                                                    \
                (void)_nb;                                                     \
IF_SSE2(        pred2d_up_dec_row_sse2((uint8_t *)d0,                          \
                    (const uint8_t *)r0, (const uint8_t *)d1, _nb, _esz);)    \
IF_NEON(        pred2d_up_dec_row_neon((uint8_t *)d0,                          \
                    (const uint8_t *)r0, (const uint8_t *)d1, _nb, _esz);)    \
IF_SCALAR(      for (int64_t col = 0; col < nx; ++col) {                       \
                    W rv = (W)r0[col];                                          \
                    W up = (W)d1[col];                                          \
                    *(U *)&d0[col] = (U)(rv + up);                              \
                })                                                             \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED2D_AVERAGE: {                                             \
            for (int64_t row = 1; row < ny; ++row) {                           \
                const T *r0 = res + row * nx;                                  \
                T       *d0 = dst + row * nx;                                  \
                const T *d1 = dst + (row - 1) * nx;                            \
                {                                                              \
                    W rv = (W)r0[0];                                           \
                    W up = (W)d1[0];                                           \
                    *(U *)&d0[0] = (U)(rv + up / 2);                           \
                }                                                              \
                for (int64_t col = 1; col < nx; ++col) {                       \
                    W rv   = (W)r0[col];                                       \
                    W left = (W)d0[col - 1];                                   \
                    W up   = (W)d1[col];                                       \
                    *(U *)&d0[col] = (U)(rv + (left + up) / 2);                \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED2D_PAETH: {                                               \
            for (int64_t row = 1; row < ny; ++row) {                           \
                const T *r0 = res + row * nx;                                  \
                T       *d0 = dst + row * nx;                                  \
                const T *d1 = dst + (row - 1) * nx;                            \
                {                                                              \
                    W rv = (W)r0[0];                                           \
                    W up = (W)d1[0];                                           \
                    *(U *)&d0[0] = (U)(rv + up);                               \
                }                                                              \
                for (int64_t col = 1; col < nx; ++col) {                       \
                    W rv     = (W)r0[col];                                     \
                    W left   = (W)d0[col - 1];                                 \
                    W up     = (W)d1[col];                                     \
                    W upleft = (W)d1[col - 1];                                 \
                    W pred   = PAETH(left, up, upleft);                        \
                    *(U *)&d0[col] = (U)(rv + pred);                           \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        default: break;                                                        \
    }                                                                          \
}

DEFINE_PRED2D_TYPED(i8,  int8_t,   uint8_t,  int32_t, paeth32)
DEFINE_PRED2D_TYPED(u8,  uint8_t,  uint8_t,  int32_t, paeth32)
DEFINE_PRED2D_TYPED(i16, int16_t,  uint16_t, int32_t, paeth32)
DEFINE_PRED2D_TYPED(u16, uint16_t, uint16_t, int32_t, paeth32)
DEFINE_PRED2D_TYPED(i32, int32_t,  uint32_t, int64_t, paeth64)
DEFINE_PRED2D_TYPED(u32, uint32_t, uint32_t, int64_t, paeth64)
/* 64-bit kernels reuse paeth64 / int64 working type. The paeth term
 * `a + b - c` can overflow at the int64 extremes, but real raster data
 * never spans that range; encode and decode wrap symmetrically through
 * the unsigned narrow store, so any path that survives encode also
 * round-trips on decode. */
DEFINE_PRED2D_TYPED(i64, int64_t,  uint64_t, int64_t, paeth64)
DEFINE_PRED2D_TYPED(u64, uint64_t, uint64_t, int64_t, paeth64)

#undef DEFINE_PRED2D_TYPED
#undef IF_SSE2
#undef IF_NEON
#undef IF_SCALAR

/* ----- 2-row wavefront PAETH decode (16-bit) ----------------------------- *
 * The scalar PAETH decode is dependency-bound: each pixel reads dst[r][c-1]
 * which was just stored, so the inner loop runs at the latency of the
 * paeth+add+store-to-load chain (~6 cycles/pixel measured). The CPU's OoO
 * cannot find independent work because the dependency chain is one row
 * deep.
 *
 * The wavefront breaks the bottleneck by processing TWO consecutive rows
 * in lockstep, with row R+1 running one column behind row R. At iteration
 * `c` we compute pixel (R, c) AND pixel (R+1, c-1). The two computations
 * are *independent within a single iteration*:
 *
 *   lane 0: pred0 = paeth(prev_R,  dRm[c],     dRm[c-1])
 *           out0  = res[R][c]   + pred0
 *           dst[R][c]   = out0
 *
 *   lane 1: pred1 = paeth(prev_R1, prev_R,     prev_R_minus)
 *           out1  = res[R+1][c-1] + pred1
 *           dst[R+1][c-1] = out1
 *
 * Lane 1's `up` = dst[R][c-1] = `prev_R` (carried from the *previous*
 * iteration, NOT lane 0's just-computed out0). Lane 1's `upleft` =
 * dst[R][c-2] = `prev_R_minus`. So the two predictor chains share no
 * data within an iteration — only across iterations via the carried
 * scalars. This unblocks two independent dependency chains for the
 * scheduler and roughly doubles useful IPC on the inner loop.
 *
 * Falls back to the scalar typed kernel when the raster is too small
 * (ny < 3 or nx < 2) or rows are odd (the trailing row is decoded by
 * the scalar kernel after the wavefront pair loop).
 *
 * Why 16-bit only: this is the dtype the bench loses on
 * (`PRED2D(PAETH)+BSHUF+LZ rast2d u16 2048x2048`). i32/u32 uses 64-bit
 * paeth and gains less from the trick because each lane is wider; the
 * 8-bit case is rare in real raster data. If a future bench shows a
 * loser at another width, the same template applies — copy and adjust
 * the storage type.
 */
static void pred2d_dec_u16_paeth_wavefront(const uint16_t *res, uint16_t *dst,
                                            int64_t nx, int64_t ny) {
    /* Row 0: degenerates to LEFT predictor (no row above). */
    dst[0] = res[0];
    for (int64_t c = 1; c < nx; ++c) {
        dst[c] = (uint16_t)(res[c] + dst[c - 1]);
    }

    int64_t row = 1;
    for (; row + 1 < ny; row += 2) {
        const uint16_t *rR  = res + row * nx;
        const uint16_t *rR1 = res + (row + 1) * nx;
        uint16_t       *dR  = dst + row * nx;
        uint16_t       *dR1 = dst + (row + 1) * nx;
        const uint16_t *dRm = dst + (row - 1) * nx;

        /* col 0 of both rows: paeth(0, up, 0) = up. */
        uint16_t out_R_0 = (uint16_t)(rR[0]  + dRm[0]);
        dR[0] = out_R_0;
        uint16_t out_R1_0 = (uint16_t)(rR1[0] + out_R_0);
        dR1[0] = out_R1_0;

        if (nx < 2) continue;

        /* col 1 of row R: standard PAETH (needed before wavefront). */
        uint16_t out_R_1;
        {
            int32_t left   = (int32_t)out_R_0;
            int32_t up     = (int32_t)dRm[1];
            int32_t upleft = (int32_t)dRm[0];
            int32_t pred   = paeth32(left, up, upleft);
            out_R_1 = (uint16_t)((int32_t)rR[1] + pred);
            dR[1]   = out_R_1;
        }

        /* Wavefront: at iter c, compute (R, c) and (R+1, c-1) in parallel. */
        int32_t prev_R       = (int32_t)(uint32_t)out_R_1;   /* dst[R][c-1]   */
        int32_t prev_R_minus = (int32_t)(uint32_t)out_R_0;   /* dst[R][c-2]   */
        int32_t prev_R1      = (int32_t)(uint32_t)out_R1_0;  /* dst[R+1][c-2] */

        for (int64_t c = 2; c < nx; ++c) {
            int32_t up0     = (int32_t)dRm[c];
            int32_t upleft0 = (int32_t)dRm[c - 1];
            int32_t rv0     = (int32_t)rR[c];
            int32_t pred0   = paeth32(prev_R, up0, upleft0);
            int32_t out0    = rv0 + pred0;
            dR[c] = (uint16_t)out0;

            int32_t rv1   = (int32_t)rR1[c - 1];
            int32_t pred1 = paeth32(prev_R1, prev_R, prev_R_minus);
            int32_t out1  = rv1 + pred1;
            dR1[c - 1] = (uint16_t)out1;

            prev_R_minus = prev_R;
            prev_R       = (int32_t)(uint32_t)(uint16_t)out0;
            prev_R1      = (int32_t)(uint32_t)(uint16_t)out1;
        }

        /* Epilogue: (R+1, nx-1) was deferred — left, up, upleft are all
         * carried in scalars from the last wavefront iter. */
        {
            int32_t pred = paeth32(prev_R1, prev_R, prev_R_minus);
            int32_t out  = (int32_t)rR1[nx - 1] + pred;
            dR1[nx - 1] = (uint16_t)out;
        }
    }

    /* Trailing odd row, if any. */
    if (row < ny) {
        const uint16_t *r0 = res + row * nx;
        uint16_t       *d0 = dst + row * nx;
        const uint16_t *d1 = dst + (row - 1) * nx;
        d0[0] = (uint16_t)((int32_t)r0[0] + (int32_t)d1[0]);
        for (int64_t c = 1; c < nx; ++c) {
            int32_t left   = (int32_t)d0[c - 1];
            int32_t up     = (int32_t)d1[c];
            int32_t upleft = (int32_t)d1[c - 1];
            int32_t pred   = paeth32(left, up, upleft);
            d0[c] = (uint16_t)((int32_t)r0[c] + pred);
        }
    }
}

/* ----- 4-row wavefront PAETH decode (16-bit) ----------------------------- *
 * Extends the 2-row wavefront to FOUR staggered rows, quadrupling the
 * independent work available to the OoO scheduler. At iteration `c` the
 * four lanes compute:
 *
 *   lane 0: (R,   c  )   left = L0,  up = dRm[c],   upleft = dRm[c-1]
 *   lane 1: (R+1, c-1)   left = L1,  up = L0_prev,  upleft = P0
 *   lane 2: (R+2, c-2)   left = L2,  up = L1_prev,  upleft = P1
 *   lane 3: (R+3, c-3)   left = L3,  up = L2_prev,  upleft = P2
 *
 * where L[i] is lane i's output from the PREVIOUS iteration (carried),
 * and P[i] is lane i's output from TWO iterations ago. Within a single
 * iteration all four paeth+add chains are independent — they share data
 * only across iterations via the 7 carried scalars (L0..L3, P0..P2).
 *
 * Requires nx >= 4 for the triangular prologue. The caller (dispatch)
 * gates this; rasters with nx < 4 use the 2-row wavefront instead.
 *
 * Trailing 0-3 rows after the last complete quad are decoded with scalar
 * paeth — at most 3 rows out of potentially thousands, so the overhead
 * is negligible.
 */
static void pred2d_dec_u16_paeth_wf4(const uint16_t *res, uint16_t *dst,
                                      int64_t nx, int64_t ny) {
    /* Row 0: LEFT predictor (no row above). */
    dst[0] = res[0];
    for (int64_t c = 1; c < nx; ++c)
        dst[c] = (uint16_t)(res[c] + dst[c - 1]);

    int64_t row = 1;

    /* ---------- 4-row wavefront groups ---------- */
    for (; row + 3 < ny; row += 4) {
        const uint16_t *rR  = res + row       * nx;
        const uint16_t *rR1 = res + (row + 1) * nx;
        const uint16_t *rR2 = res + (row + 2) * nx;
        const uint16_t *rR3 = res + (row + 3) * nx;
        uint16_t       *dR  = dst + row       * nx;
        uint16_t       *dR1 = dst + (row + 1) * nx;
        uint16_t       *dR2 = dst + (row + 2) * nx;
        uint16_t       *dR3 = dst + (row + 3) * nx;
        const uint16_t *dRm = dst + (row - 1) * nx;

        /* Col 0 of all 4 rows: paeth(0, up, 0) = up. */
        int32_t c0_R  = (int32_t)(uint16_t)((int32_t)rR [0] + (int32_t)dRm[0]);
        dR[0] = (uint16_t)c0_R;
        int32_t c0_R1 = (int32_t)(uint16_t)((int32_t)rR1[0] + c0_R);
        dR1[0] = (uint16_t)c0_R1;
        int32_t c0_R2 = (int32_t)(uint16_t)((int32_t)rR2[0] + c0_R1);
        dR2[0] = (uint16_t)c0_R2;
        int32_t c0_R3 = (int32_t)(uint16_t)((int32_t)rR3[0] + c0_R2);
        dR3[0] = (uint16_t)c0_R3;

        /* Carried state: L[i] = last output of lane i (left for next iter),
         * P[i] = output of lane i from one iter earlier (upleft for lane i+1). */
        int32_t L0 = c0_R, L1 = c0_R1, L2 = c0_R2, L3 = c0_R3;
        int32_t P0 = 0, P1 = 0, P2 = 0;

        /* Prologue step 1 (c=1): 1 lane — row R col 1. */
        {
            int32_t pred = paeth32(L0, (int32_t)dRm[1], (int32_t)dRm[0]);
            int32_t v = (int32_t)rR[1] + pred;
            dR[1] = (uint16_t)v;
            P0 = L0;
            L0 = (int32_t)(uint16_t)v;
        }

        /* Prologue step 2 (c=2): 2 lanes — R col 2, R+1 col 1. */
        {
            int32_t pred0 = paeth32(L0, (int32_t)dRm[2], (int32_t)dRm[1]);
            int32_t v0 = (int32_t)rR[2] + pred0;
            dR[2] = (uint16_t)v0;

            int32_t pred1 = paeth32(L1, L0, P0);
            int32_t v1 = (int32_t)rR1[1] + pred1;
            dR1[1] = (uint16_t)v1;

            P0 = L0; P1 = L1;
            L0 = (int32_t)(uint16_t)v0;
            L1 = (int32_t)(uint16_t)v1;
        }

        /* Prologue step 3 (c=3): 3 lanes — R col 3, R+1 col 2, R+2 col 1. */
        {
            int32_t pred0 = paeth32(L0, (int32_t)dRm[3], (int32_t)dRm[2]);
            int32_t v0 = (int32_t)rR[3] + pred0;
            dR[3] = (uint16_t)v0;

            int32_t pred1 = paeth32(L1, L0, P0);
            int32_t v1 = (int32_t)rR1[2] + pred1;
            dR1[2] = (uint16_t)v1;

            int32_t pred2 = paeth32(L2, L1, P1);
            int32_t v2 = (int32_t)rR2[1] + pred2;
            dR2[1] = (uint16_t)v2;

            P0 = L0; P1 = L1; P2 = L2;
            L0 = (int32_t)(uint16_t)v0;
            L1 = (int32_t)(uint16_t)v1;
            L2 = (int32_t)(uint16_t)v2;
        }

        /* Steady state: all 4 lanes, c = 4..nx-1.
         * SSE2 path: pack the 4 paeth inputs into XMM registers, compute
         * all 4 predictions in one SIMD call, then scatter-store. The L
         * and P state vectors are maintained as __m128i across iterations
         * to minimise pack/unpack overhead. */
#ifdef TDC_PRED2D_HAVE_SSE2
        {
            /* L = {L0, L1, L2, L3} in lanes 0-3 (lane 0 = lowest). */
            __m128i vL = _mm_set_epi32(L3, L2, L1, L0);
            /* P = {P0, P1, P2, 0} — only 3 lanes meaningful. */
            __m128i vP = _mm_set_epi32(0, P2, P1, P0);
            const __m128i mask16 = _mm_set1_epi32(0xFFFF);

            for (int64_t c = 4; c < nx; ++c) {
                /* a = {L0, L1, L2, L3} — directly from vL. */

                /* b = {dRm[c], L0_old, L1_old, L2_old}
                 *   = shift vL left by one lane, insert dRm[c] at lane 0. */
                __m128i vB = _mm_or_si128(
                    _mm_slli_si128(vL, 4),
                    _mm_cvtsi32_si128((int32_t)dRm[c]));

                /* c = {dRm[c-1], P0, P1, P2}
                 *   = shift vP left by one lane, insert dRm[c-1] at lane 0. */
                __m128i vC = _mm_or_si128(
                    _mm_slli_si128(vP, 4),
                    _mm_cvtsi32_si128((int32_t)dRm[c - 1]));

                __m128i pred = paeth_4x_sse2(vL, vB, vC);

                /* Gather residuals from 4 staggered positions. */
                __m128i vRes = _mm_set_epi32(
                    (int32_t)rR3[c - 3], (int32_t)rR2[c - 2],
                    (int32_t)rR1[c - 1], (int32_t)rR[c]);

                __m128i vV = _mm_add_epi32(vRes, pred);

                /* Update P = old L, L = truncate(v) to u16. */
                vP = vL;
                vL = _mm_and_si128(vV, mask16);

                /* Scatter-store: extract each lane and write. */
                dR [c]     = (uint16_t)(uint32_t)_mm_cvtsi128_si32(vV);
                dR1[c - 1] = (uint16_t)(uint32_t)_mm_cvtsi128_si32(_mm_srli_si128(vV, 4));
                dR2[c - 2] = (uint16_t)(uint32_t)_mm_cvtsi128_si32(_mm_srli_si128(vV, 8));
                dR3[c - 3] = (uint16_t)(uint32_t)_mm_cvtsi128_si32(_mm_srli_si128(vV, 12));
            }

            /* Unpack L/P back to scalars for the epilogue. */
            L0 = _mm_cvtsi128_si32(vL);
            L1 = _mm_cvtsi128_si32(_mm_srli_si128(vL, 4));
            L2 = _mm_cvtsi128_si32(_mm_srli_si128(vL, 8));
            L3 = _mm_cvtsi128_si32(_mm_srli_si128(vL, 12));
            P0 = _mm_cvtsi128_si32(vP);
            P1 = _mm_cvtsi128_si32(_mm_srli_si128(vP, 4));
            P2 = _mm_cvtsi128_si32(_mm_srli_si128(vP, 8));
        }
#else
        for (int64_t c = 4; c < nx; ++c) {
            int32_t pred0 = paeth32(L0, (int32_t)dRm[c], (int32_t)dRm[c - 1]);
            int32_t v0 = (int32_t)rR[c] + pred0;
            dR[c] = (uint16_t)v0;

            int32_t pred1 = paeth32(L1, L0, P0);
            int32_t v1 = (int32_t)rR1[c - 1] + pred1;
            dR1[c - 1] = (uint16_t)v1;

            int32_t pred2 = paeth32(L2, L1, P1);
            int32_t v2 = (int32_t)rR2[c - 2] + pred2;
            dR2[c - 2] = (uint16_t)v2;

            int32_t pred3 = paeth32(L3, L2, P2);
            int32_t v3 = (int32_t)rR3[c - 3] + pred3;
            dR3[c - 3] = (uint16_t)v3;

            P0 = L0; P1 = L1; P2 = L2;
            L0 = (int32_t)(uint16_t)v0;
            L1 = (int32_t)(uint16_t)v1;
            L2 = (int32_t)(uint16_t)v2;
            L3 = (int32_t)(uint16_t)v3;
        }
#endif

        /* Epilogue: drain the staggered triangle (3 + 2 + 1 pixels). */

        /* 3-lane: (R+1, nx-1), (R+2, nx-2), (R+3, nx-3). */
        {
            int32_t pred1 = paeth32(L1, L0, P0);
            int32_t v1 = (int32_t)rR1[nx - 1] + pred1;
            dR1[nx - 1] = (uint16_t)v1;

            int32_t pred2 = paeth32(L2, L1, P1);
            int32_t v2 = (int32_t)rR2[nx - 2] + pred2;
            dR2[nx - 2] = (uint16_t)v2;

            int32_t pred3 = paeth32(L3, L2, P2);
            int32_t v3 = (int32_t)rR3[nx - 3] + pred3;
            dR3[nx - 3] = (uint16_t)v3;

            P1 = L1; P2 = L2;
            L1 = (int32_t)(uint16_t)v1;
            L2 = (int32_t)(uint16_t)v2;
            L3 = (int32_t)(uint16_t)v3;
        }
        /* 2-lane: (R+2, nx-1), (R+3, nx-2). */
        {
            int32_t pred2 = paeth32(L2, L1, P1);
            int32_t v2 = (int32_t)rR2[nx - 1] + pred2;
            dR2[nx - 1] = (uint16_t)v2;

            int32_t pred3 = paeth32(L3, L2, P2);
            int32_t v3 = (int32_t)rR3[nx - 2] + pred3;
            dR3[nx - 2] = (uint16_t)v3;

            P2 = L2;
            L2 = (int32_t)(uint16_t)v2;
            L3 = (int32_t)(uint16_t)v3;
        }
        /* 1-lane: (R+3, nx-1). */
        {
            int32_t pred3 = paeth32(L3, L2, P2);
            int32_t v3 = (int32_t)rR3[nx - 1] + pred3;
            dR3[nx - 1] = (uint16_t)v3;
        }
    }

    /* Trailing rows (at most 3): scalar PAETH — negligible cost. */
    for (; row < ny; ++row) {
        const uint16_t *rr = res + row * nx;
        uint16_t       *dr = dst + row * nx;
        const uint16_t *da = dst + (row - 1) * nx;
        dr[0] = (uint16_t)((int32_t)rr[0] + (int32_t)da[0]);
        for (int64_t c = 1; c < nx; ++c) {
            int32_t left   = (int32_t)dr[c - 1];
            int32_t up     = (int32_t)da[c];
            int32_t upleft = (int32_t)da[c - 1];
            dr[c] = (uint16_t)((int32_t)rr[c] + paeth32(left, up, upleft));
        }
    }
}

/* ----- 8-row wavefront PAETH decode (16-bit, AVX2) ----------------------- *
 * Doubles the lane count of wf4 from 4 (SSE2 __m128i) to 8 (AVX2 __m256i).
 * Same staggered-row trick as wf4 — at iteration `c` lane i computes
 * (R+i, c-i) for i in 0..7 — but with twice the independent dependency
 * chains running in parallel.
 *
 * Prologue: 7 staggered steps where lane i comes online at c = i+1.
 * Steady state c=8..nx-1: all 8 lanes active.
 * Epilogue: 7 staggered drain steps mirroring the prologue.
 *
 * Carried state across the steady-state loop:
 *   L = {L0..L7}  — lane i's last output (= left for next iter)
 *   P = {P0..P6}  — lane i's output from 1 iter ago (= upleft for lane i+1)
 *
 * Lane shifts: SSE2 wf4 uses _mm_slli_si128(vL, 4) to shift the entire
 * 128-bit vector by one int32 lane. AVX2 _mm256_slli_si256 only shifts
 * within each 128-bit half. To shift all 256 bits by 4 bytes we need:
 *   shifted = alignr_epi8(vL, permute2x128(vL, vL, 0x08), 12)
 *   where permute2x128(.., 0x08) gives {vL_lo (high), 0 (low)} so
 *   alignr_epi8(vL, that, 12) yields a full 256-bit left-shift by one lane.
 * Insert the new lane-0 value via _mm256_or_si256 with a singleton vector.
 *
 * Scatter: _mm256_extract_epi32 takes a compile-time index, so we open-code
 * 8 lane extracts after the add. (No AVX2 scatter for 32-bit indices —
 * AVX-512 has scatter but we're targeting AVX2 only.)
 *
 * Requires nx >= 8 for the prologue and ny >= 9 (1 LEFT-row + 8 wf rows)
 * for at least one full octet. Below that the dispatcher routes to wf4.
 */
#ifdef TDC_PRED2D_HAVE_AVX2
static void pred2d_dec_u16_paeth_wf8(const uint16_t *res, uint16_t *dst,
                                     int64_t nx, int64_t ny) {
    /* Row 0: LEFT predictor (no row above). */
    dst[0] = res[0];
    for (int64_t c = 1; c < nx; ++c)
        dst[c] = (uint16_t)(res[c] + dst[c - 1]);

    int64_t row = 1;

    /* ---------- 8-row wavefront groups ---------- */
    for (; row + 7 < ny; row += 8) {
        const uint16_t *rR  = res + (row + 0) * nx;
        const uint16_t *rR1 = res + (row + 1) * nx;
        const uint16_t *rR2 = res + (row + 2) * nx;
        const uint16_t *rR3 = res + (row + 3) * nx;
        const uint16_t *rR4 = res + (row + 4) * nx;
        const uint16_t *rR5 = res + (row + 5) * nx;
        const uint16_t *rR6 = res + (row + 6) * nx;
        const uint16_t *rR7 = res + (row + 7) * nx;
        uint16_t       *dR  = dst + (row + 0) * nx;
        uint16_t       *dR1 = dst + (row + 1) * nx;
        uint16_t       *dR2 = dst + (row + 2) * nx;
        uint16_t       *dR3 = dst + (row + 3) * nx;
        uint16_t       *dR4 = dst + (row + 4) * nx;
        uint16_t       *dR5 = dst + (row + 5) * nx;
        uint16_t       *dR6 = dst + (row + 6) * nx;
        uint16_t       *dR7 = dst + (row + 7) * nx;
        const uint16_t *dRm = dst + (row - 1) * nx;

        /* Col 0 of all 8 rows: paeth(0, up, 0) = up. The output of (R+i, 0)
         * feeds (R+i+1, 0) as the "up" — chained add. */
        int32_t c0_R  = (int32_t)(uint16_t)((int32_t)rR [0] + (int32_t)dRm[0]);
        dR [0] = (uint16_t)c0_R;
        int32_t c0_R1 = (int32_t)(uint16_t)((int32_t)rR1[0] + c0_R);
        dR1[0] = (uint16_t)c0_R1;
        int32_t c0_R2 = (int32_t)(uint16_t)((int32_t)rR2[0] + c0_R1);
        dR2[0] = (uint16_t)c0_R2;
        int32_t c0_R3 = (int32_t)(uint16_t)((int32_t)rR3[0] + c0_R2);
        dR3[0] = (uint16_t)c0_R3;
        int32_t c0_R4 = (int32_t)(uint16_t)((int32_t)rR4[0] + c0_R3);
        dR4[0] = (uint16_t)c0_R4;
        int32_t c0_R5 = (int32_t)(uint16_t)((int32_t)rR5[0] + c0_R4);
        dR5[0] = (uint16_t)c0_R5;
        int32_t c0_R6 = (int32_t)(uint16_t)((int32_t)rR6[0] + c0_R5);
        dR6[0] = (uint16_t)c0_R6;
        int32_t c0_R7 = (int32_t)(uint16_t)((int32_t)rR7[0] + c0_R6);
        dR7[0] = (uint16_t)c0_R7;

        /* Carried state for the prologue (open-coded; SIMD steady-state
         * below repacks them into __m256i registers). */
        int32_t L0 = c0_R,  L1 = c0_R1, L2 = c0_R2, L3 = c0_R3;
        int32_t L4 = c0_R4, L5 = c0_R5, L6 = c0_R6, L7 = c0_R7;
        int32_t P0 = 0, P1 = 0, P2 = 0, P3 = 0, P4 = 0, P5 = 0, P6 = 0;

        /* Prologue step 1 (c=1): 1 lane — row R col 1. */
        {
            int32_t pred = paeth32(L0, (int32_t)dRm[1], (int32_t)dRm[0]);
            int32_t v = (int32_t)rR[1] + pred;
            dR[1] = (uint16_t)v;
            P0 = L0;
            L0 = (int32_t)(uint16_t)v;
        }
        /* Prologue step 2 (c=2): 2 lanes. */
        {
            int32_t pr0 = paeth32(L0, (int32_t)dRm[2], (int32_t)dRm[1]);
            int32_t v0 = (int32_t)rR[2] + pr0; dR[2] = (uint16_t)v0;
            int32_t pr1 = paeth32(L1, L0, P0);
            int32_t v1 = (int32_t)rR1[1] + pr1; dR1[1] = (uint16_t)v1;
            P0 = L0; P1 = L1;
            L0 = (int32_t)(uint16_t)v0;
            L1 = (int32_t)(uint16_t)v1;
        }
        /* Prologue step 3 (c=3): 3 lanes. */
        {
            int32_t pr0 = paeth32(L0, (int32_t)dRm[3], (int32_t)dRm[2]);
            int32_t v0 = (int32_t)rR[3] + pr0; dR[3] = (uint16_t)v0;
            int32_t pr1 = paeth32(L1, L0, P0);
            int32_t v1 = (int32_t)rR1[2] + pr1; dR1[2] = (uint16_t)v1;
            int32_t pr2 = paeth32(L2, L1, P1);
            int32_t v2 = (int32_t)rR2[1] + pr2; dR2[1] = (uint16_t)v2;
            P0 = L0; P1 = L1; P2 = L2;
            L0 = (int32_t)(uint16_t)v0;
            L1 = (int32_t)(uint16_t)v1;
            L2 = (int32_t)(uint16_t)v2;
        }
        /* Prologue step 4 (c=4): 4 lanes. */
        {
            int32_t pr0 = paeth32(L0, (int32_t)dRm[4], (int32_t)dRm[3]);
            int32_t v0 = (int32_t)rR[4] + pr0; dR[4] = (uint16_t)v0;
            int32_t pr1 = paeth32(L1, L0, P0);
            int32_t v1 = (int32_t)rR1[3] + pr1; dR1[3] = (uint16_t)v1;
            int32_t pr2 = paeth32(L2, L1, P1);
            int32_t v2 = (int32_t)rR2[2] + pr2; dR2[2] = (uint16_t)v2;
            int32_t pr3 = paeth32(L3, L2, P2);
            int32_t v3 = (int32_t)rR3[1] + pr3; dR3[1] = (uint16_t)v3;
            P0 = L0; P1 = L1; P2 = L2; P3 = L3;
            L0 = (int32_t)(uint16_t)v0;
            L1 = (int32_t)(uint16_t)v1;
            L2 = (int32_t)(uint16_t)v2;
            L3 = (int32_t)(uint16_t)v3;
        }
        /* Prologue step 5 (c=5): 5 lanes. */
        {
            int32_t pr0 = paeth32(L0, (int32_t)dRm[5], (int32_t)dRm[4]);
            int32_t v0 = (int32_t)rR[5] + pr0; dR[5] = (uint16_t)v0;
            int32_t pr1 = paeth32(L1, L0, P0);
            int32_t v1 = (int32_t)rR1[4] + pr1; dR1[4] = (uint16_t)v1;
            int32_t pr2 = paeth32(L2, L1, P1);
            int32_t v2 = (int32_t)rR2[3] + pr2; dR2[3] = (uint16_t)v2;
            int32_t pr3 = paeth32(L3, L2, P2);
            int32_t v3 = (int32_t)rR3[2] + pr3; dR3[2] = (uint16_t)v3;
            int32_t pr4 = paeth32(L4, L3, P3);
            int32_t v4 = (int32_t)rR4[1] + pr4; dR4[1] = (uint16_t)v4;
            P0 = L0; P1 = L1; P2 = L2; P3 = L3; P4 = L4;
            L0 = (int32_t)(uint16_t)v0;
            L1 = (int32_t)(uint16_t)v1;
            L2 = (int32_t)(uint16_t)v2;
            L3 = (int32_t)(uint16_t)v3;
            L4 = (int32_t)(uint16_t)v4;
        }
        /* Prologue step 6 (c=6): 6 lanes. */
        {
            int32_t pr0 = paeth32(L0, (int32_t)dRm[6], (int32_t)dRm[5]);
            int32_t v0 = (int32_t)rR[6] + pr0; dR[6] = (uint16_t)v0;
            int32_t pr1 = paeth32(L1, L0, P0);
            int32_t v1 = (int32_t)rR1[5] + pr1; dR1[5] = (uint16_t)v1;
            int32_t pr2 = paeth32(L2, L1, P1);
            int32_t v2 = (int32_t)rR2[4] + pr2; dR2[4] = (uint16_t)v2;
            int32_t pr3 = paeth32(L3, L2, P2);
            int32_t v3 = (int32_t)rR3[3] + pr3; dR3[3] = (uint16_t)v3;
            int32_t pr4 = paeth32(L4, L3, P3);
            int32_t v4 = (int32_t)rR4[2] + pr4; dR4[2] = (uint16_t)v4;
            int32_t pr5 = paeth32(L5, L4, P4);
            int32_t v5 = (int32_t)rR5[1] + pr5; dR5[1] = (uint16_t)v5;
            P0 = L0; P1 = L1; P2 = L2; P3 = L3; P4 = L4; P5 = L5;
            L0 = (int32_t)(uint16_t)v0;
            L1 = (int32_t)(uint16_t)v1;
            L2 = (int32_t)(uint16_t)v2;
            L3 = (int32_t)(uint16_t)v3;
            L4 = (int32_t)(uint16_t)v4;
            L5 = (int32_t)(uint16_t)v5;
        }
        /* Prologue step 7 (c=7): 7 lanes. */
        {
            int32_t pr0 = paeth32(L0, (int32_t)dRm[7], (int32_t)dRm[6]);
            int32_t v0 = (int32_t)rR[7] + pr0; dR[7] = (uint16_t)v0;
            int32_t pr1 = paeth32(L1, L0, P0);
            int32_t v1 = (int32_t)rR1[6] + pr1; dR1[6] = (uint16_t)v1;
            int32_t pr2 = paeth32(L2, L1, P1);
            int32_t v2 = (int32_t)rR2[5] + pr2; dR2[5] = (uint16_t)v2;
            int32_t pr3 = paeth32(L3, L2, P2);
            int32_t v3 = (int32_t)rR3[4] + pr3; dR3[4] = (uint16_t)v3;
            int32_t pr4 = paeth32(L4, L3, P3);
            int32_t v4 = (int32_t)rR4[3] + pr4; dR4[3] = (uint16_t)v4;
            int32_t pr5 = paeth32(L5, L4, P4);
            int32_t v5 = (int32_t)rR5[2] + pr5; dR5[2] = (uint16_t)v5;
            int32_t pr6 = paeth32(L6, L5, P5);
            int32_t v6 = (int32_t)rR6[1] + pr6; dR6[1] = (uint16_t)v6;
            P0 = L0; P1 = L1; P2 = L2; P3 = L3; P4 = L4; P5 = L5; P6 = L6;
            L0 = (int32_t)(uint16_t)v0;
            L1 = (int32_t)(uint16_t)v1;
            L2 = (int32_t)(uint16_t)v2;
            L3 = (int32_t)(uint16_t)v3;
            L4 = (int32_t)(uint16_t)v4;
            L5 = (int32_t)(uint16_t)v5;
            L6 = (int32_t)(uint16_t)v6;
        }

        /* Steady state c = 8..nx-1: all 8 lanes active in __m256i. */
        {
            __m256i vL = _mm256_setr_epi32(L0, L1, L2, L3, L4, L5, L6, L7);
            /* P[7] is meaningless (only P0..P6 used). Pack 0 in lane 7. */
            __m256i vP = _mm256_setr_epi32(P0, P1, P2, P3, P4, P5, P6, 0);
            const __m256i mask16 = _mm256_set1_epi32(0xFFFF);

            for (int64_t c = 8; c < nx; ++c) {
                /* Build vB by shifting vL left by one int32 lane and inserting
                 * dRm[c] at lane 0. Cross-128-bit-lane shift requires a
                 * permute2x128 + alignr_epi8(12) sequence. */
                __m256i vL_swap = _mm256_permute2x128_si256(vL, vL, 0x08);
                __m256i vB_shift = _mm256_alignr_epi8(vL, vL_swap, 12);
                __m256i vB = _mm256_or_si256(
                    vB_shift,
                    _mm256_castsi128_si256(_mm_cvtsi32_si128((int32_t)dRm[c])));

                __m256i vP_swap = _mm256_permute2x128_si256(vP, vP, 0x08);
                __m256i vC_shift = _mm256_alignr_epi8(vP, vP_swap, 12);
                __m256i vC = _mm256_or_si256(
                    vC_shift,
                    _mm256_castsi128_si256(_mm_cvtsi32_si128((int32_t)dRm[c - 1])));

                __m256i pred = paeth_8x_avx2(vL, vB, vC);

                /* Gather 8 staggered residuals. */
                __m256i vRes = _mm256_setr_epi32(
                    (int32_t)rR [c    ], (int32_t)rR1[c - 1],
                    (int32_t)rR2[c - 2], (int32_t)rR3[c - 3],
                    (int32_t)rR4[c - 4], (int32_t)rR5[c - 5],
                    (int32_t)rR6[c - 6], (int32_t)rR7[c - 7]);

                __m256i vV = _mm256_add_epi32(vRes, pred);

                /* Update carried state. */
                vP = vL;
                vL = _mm256_and_si256(vV, mask16);

                /* Scatter store: _mm256_extract_epi32 needs a const index. */
                dR [c    ] = (uint16_t)(uint32_t)_mm256_extract_epi32(vV, 0);
                dR1[c - 1] = (uint16_t)(uint32_t)_mm256_extract_epi32(vV, 1);
                dR2[c - 2] = (uint16_t)(uint32_t)_mm256_extract_epi32(vV, 2);
                dR3[c - 3] = (uint16_t)(uint32_t)_mm256_extract_epi32(vV, 3);
                dR4[c - 4] = (uint16_t)(uint32_t)_mm256_extract_epi32(vV, 4);
                dR5[c - 5] = (uint16_t)(uint32_t)_mm256_extract_epi32(vV, 5);
                dR6[c - 6] = (uint16_t)(uint32_t)_mm256_extract_epi32(vV, 6);
                dR7[c - 7] = (uint16_t)(uint32_t)_mm256_extract_epi32(vV, 7);
            }

            /* Unpack vL/vP back to scalars for the epilogue. */
            L0 = _mm256_extract_epi32(vL, 0);
            L1 = _mm256_extract_epi32(vL, 1);
            L2 = _mm256_extract_epi32(vL, 2);
            L3 = _mm256_extract_epi32(vL, 3);
            L4 = _mm256_extract_epi32(vL, 4);
            L5 = _mm256_extract_epi32(vL, 5);
            L6 = _mm256_extract_epi32(vL, 6);
            L7 = _mm256_extract_epi32(vL, 7);
            P0 = _mm256_extract_epi32(vP, 0);
            P1 = _mm256_extract_epi32(vP, 1);
            P2 = _mm256_extract_epi32(vP, 2);
            P3 = _mm256_extract_epi32(vP, 3);
            P4 = _mm256_extract_epi32(vP, 4);
            P5 = _mm256_extract_epi32(vP, 5);
            P6 = _mm256_extract_epi32(vP, 6);
        }

        /* Epilogue: drain the staggered triangle. After the steady-state
         * loop, lane 0 has finished (it computed up to col nx-1), but
         * lanes 1..7 still owe their last 1..7 columns. */

        /* 7-lane drain: lane 1 col nx-1, lane 2 col nx-2, ..., lane 7 col nx-7. */
        {
            int32_t pr1 = paeth32(L1, L0, P0);
            int32_t v1 = (int32_t)rR1[nx - 1] + pr1; dR1[nx - 1] = (uint16_t)v1;
            int32_t pr2 = paeth32(L2, L1, P1);
            int32_t v2 = (int32_t)rR2[nx - 2] + pr2; dR2[nx - 2] = (uint16_t)v2;
            int32_t pr3 = paeth32(L3, L2, P2);
            int32_t v3 = (int32_t)rR3[nx - 3] + pr3; dR3[nx - 3] = (uint16_t)v3;
            int32_t pr4 = paeth32(L4, L3, P3);
            int32_t v4 = (int32_t)rR4[nx - 4] + pr4; dR4[nx - 4] = (uint16_t)v4;
            int32_t pr5 = paeth32(L5, L4, P4);
            int32_t v5 = (int32_t)rR5[nx - 5] + pr5; dR5[nx - 5] = (uint16_t)v5;
            int32_t pr6 = paeth32(L6, L5, P5);
            int32_t v6 = (int32_t)rR6[nx - 6] + pr6; dR6[nx - 6] = (uint16_t)v6;
            int32_t pr7 = paeth32(L7, L6, P6);
            int32_t v7 = (int32_t)rR7[nx - 7] + pr7; dR7[nx - 7] = (uint16_t)v7;
            P1 = L1; P2 = L2; P3 = L3; P4 = L4; P5 = L5; P6 = L6;
            L1 = (int32_t)(uint16_t)v1;
            L2 = (int32_t)(uint16_t)v2;
            L3 = (int32_t)(uint16_t)v3;
            L4 = (int32_t)(uint16_t)v4;
            L5 = (int32_t)(uint16_t)v5;
            L6 = (int32_t)(uint16_t)v6;
            L7 = (int32_t)(uint16_t)v7;
        }
        /* 6-lane drain. */
        {
            int32_t pr2 = paeth32(L2, L1, P1);
            int32_t v2 = (int32_t)rR2[nx - 1] + pr2; dR2[nx - 1] = (uint16_t)v2;
            int32_t pr3 = paeth32(L3, L2, P2);
            int32_t v3 = (int32_t)rR3[nx - 2] + pr3; dR3[nx - 2] = (uint16_t)v3;
            int32_t pr4 = paeth32(L4, L3, P3);
            int32_t v4 = (int32_t)rR4[nx - 3] + pr4; dR4[nx - 3] = (uint16_t)v4;
            int32_t pr5 = paeth32(L5, L4, P4);
            int32_t v5 = (int32_t)rR5[nx - 4] + pr5; dR5[nx - 4] = (uint16_t)v5;
            int32_t pr6 = paeth32(L6, L5, P5);
            int32_t v6 = (int32_t)rR6[nx - 5] + pr6; dR6[nx - 5] = (uint16_t)v6;
            int32_t pr7 = paeth32(L7, L6, P6);
            int32_t v7 = (int32_t)rR7[nx - 6] + pr7; dR7[nx - 6] = (uint16_t)v7;
            P2 = L2; P3 = L3; P4 = L4; P5 = L5; P6 = L6;
            L2 = (int32_t)(uint16_t)v2;
            L3 = (int32_t)(uint16_t)v3;
            L4 = (int32_t)(uint16_t)v4;
            L5 = (int32_t)(uint16_t)v5;
            L6 = (int32_t)(uint16_t)v6;
            L7 = (int32_t)(uint16_t)v7;
        }
        /* 5-lane drain. */
        {
            int32_t pr3 = paeth32(L3, L2, P2);
            int32_t v3 = (int32_t)rR3[nx - 1] + pr3; dR3[nx - 1] = (uint16_t)v3;
            int32_t pr4 = paeth32(L4, L3, P3);
            int32_t v4 = (int32_t)rR4[nx - 2] + pr4; dR4[nx - 2] = (uint16_t)v4;
            int32_t pr5 = paeth32(L5, L4, P4);
            int32_t v5 = (int32_t)rR5[nx - 3] + pr5; dR5[nx - 3] = (uint16_t)v5;
            int32_t pr6 = paeth32(L6, L5, P5);
            int32_t v6 = (int32_t)rR6[nx - 4] + pr6; dR6[nx - 4] = (uint16_t)v6;
            int32_t pr7 = paeth32(L7, L6, P6);
            int32_t v7 = (int32_t)rR7[nx - 5] + pr7; dR7[nx - 5] = (uint16_t)v7;
            P3 = L3; P4 = L4; P5 = L5; P6 = L6;
            L3 = (int32_t)(uint16_t)v3;
            L4 = (int32_t)(uint16_t)v4;
            L5 = (int32_t)(uint16_t)v5;
            L6 = (int32_t)(uint16_t)v6;
            L7 = (int32_t)(uint16_t)v7;
        }
        /* 4-lane drain. */
        {
            int32_t pr4 = paeth32(L4, L3, P3);
            int32_t v4 = (int32_t)rR4[nx - 1] + pr4; dR4[nx - 1] = (uint16_t)v4;
            int32_t pr5 = paeth32(L5, L4, P4);
            int32_t v5 = (int32_t)rR5[nx - 2] + pr5; dR5[nx - 2] = (uint16_t)v5;
            int32_t pr6 = paeth32(L6, L5, P5);
            int32_t v6 = (int32_t)rR6[nx - 3] + pr6; dR6[nx - 3] = (uint16_t)v6;
            int32_t pr7 = paeth32(L7, L6, P6);
            int32_t v7 = (int32_t)rR7[nx - 4] + pr7; dR7[nx - 4] = (uint16_t)v7;
            P4 = L4; P5 = L5; P6 = L6;
            L4 = (int32_t)(uint16_t)v4;
            L5 = (int32_t)(uint16_t)v5;
            L6 = (int32_t)(uint16_t)v6;
            L7 = (int32_t)(uint16_t)v7;
        }
        /* 3-lane drain. */
        {
            int32_t pr5 = paeth32(L5, L4, P4);
            int32_t v5 = (int32_t)rR5[nx - 1] + pr5; dR5[nx - 1] = (uint16_t)v5;
            int32_t pr6 = paeth32(L6, L5, P5);
            int32_t v6 = (int32_t)rR6[nx - 2] + pr6; dR6[nx - 2] = (uint16_t)v6;
            int32_t pr7 = paeth32(L7, L6, P6);
            int32_t v7 = (int32_t)rR7[nx - 3] + pr7; dR7[nx - 3] = (uint16_t)v7;
            P5 = L5; P6 = L6;
            L5 = (int32_t)(uint16_t)v5;
            L6 = (int32_t)(uint16_t)v6;
            L7 = (int32_t)(uint16_t)v7;
        }
        /* 2-lane drain. */
        {
            int32_t pr6 = paeth32(L6, L5, P5);
            int32_t v6 = (int32_t)rR6[nx - 1] + pr6; dR6[nx - 1] = (uint16_t)v6;
            int32_t pr7 = paeth32(L7, L6, P6);
            int32_t v7 = (int32_t)rR7[nx - 2] + pr7; dR7[nx - 2] = (uint16_t)v7;
            P6 = L6;
            L6 = (int32_t)(uint16_t)v6;
            L7 = (int32_t)(uint16_t)v7;
        }
        /* 1-lane drain. */
        {
            int32_t pr7 = paeth32(L7, L6, P6);
            int32_t v7 = (int32_t)rR7[nx - 1] + pr7; dR7[nx - 1] = (uint16_t)v7;
        }
    }

    /* Trailing rows (at most 7): scalar PAETH — negligible cost. */
    for (; row < ny; ++row) {
        const uint16_t *rr = res + row * nx;
        uint16_t       *dr = dst + row * nx;
        const uint16_t *da = dst + (row - 1) * nx;
        dr[0] = (uint16_t)((int32_t)rr[0] + (int32_t)da[0]);
        for (int64_t c = 1; c < nx; ++c) {
            int32_t left   = (int32_t)dr[c - 1];
            int32_t up     = (int32_t)da[c];
            int32_t upleft = (int32_t)da[c - 1];
            dr[c] = (uint16_t)((int32_t)rr[c] + paeth32(left, up, upleft));
        }
    }
}
#endif /* TDC_PRED2D_HAVE_AVX2 */

/* Forward declarations for float path (defined after auto-select). */
static void pred2d_encode_float(tdc_dtype dt, tdc_pred2d_kind kind,
                                const uint8_t *src, uint8_t *res,
                                int64_t nx, int64_t ny);
static void pred2d_decode_float(tdc_dtype dt, tdc_pred2d_kind kind,
                                const uint8_t *res, uint8_t *dst,
                                int64_t nx, int64_t ny);

/* ----- Test-only exports for u16 PAETH wavefront kernels ----------------- *
 * Thin shims so tests/test_pred2d_wf_consistency.c can compare wf2/wf4/wf8
 * against each other and against scalar paeth32 on identical input. Kept
 * as separate symbols so the static inlining of the kernels themselves is
 * preserved on the hot dispatch path. */
void pred2d_dec_u16_paeth_wavefront_export(const uint16_t *res, uint16_t *dst,
                                            int64_t nx, int64_t ny) {
    pred2d_dec_u16_paeth_wavefront(res, dst, nx, ny);
}
void pred2d_dec_u16_paeth_wf4_export(const uint16_t *res, uint16_t *dst,
                                      int64_t nx, int64_t ny) {
    pred2d_dec_u16_paeth_wf4(res, dst, nx, ny);
}
void pred2d_dec_u16_paeth_wf8_export(const uint16_t *res, uint16_t *dst,
                                      int64_t nx, int64_t ny) {
#ifdef TDC_PRED2D_HAVE_AVX2
    pred2d_dec_u16_paeth_wf8(res, dst, nx, ny);
#else
    /* Without AVX2 the symbol still resolves so the test can link; it
     * just falls back to wf4 which the test compares against. */
    pred2d_dec_u16_paeth_wf4(res, dst, nx, ny);
#endif
}
int pred2d_have_avx2(void) {
#ifdef TDC_PRED2D_HAVE_AVX2
    return 1;
#else
    return 0;
#endif
}

/* ----- Sweep dispatchers ------------------------------------------------- */
/* Non-static so the QUANTIZE_PRED_2D composite model in
 * src/model/quantize_pred2d.c can call them directly on the quantized
 * integer raster. Declared in src/model/pred2d_internal.h. */

void pred2d_encode_sweep(tdc_dtype dt, tdc_pred2d_kind kind,
                         const uint8_t *src,
                         uint8_t *res,
                         int64_t nx, int64_t ny) {
    if (tdc_dtype_is_float(dt)) {
        pred2d_encode_float(dt, kind, src, res, nx, ny);
        return;
    }
    switch (dt) {
        case TDC_DT_I8:  pred2d_enc_i8 (kind, (const int8_t   *)src, (int8_t   *)res, nx, ny); break;
        case TDC_DT_U8:  pred2d_enc_u8 (kind, (const uint8_t  *)src, (uint8_t  *)res, nx, ny); break;
        case TDC_DT_I16: pred2d_enc_i16(kind, (const int16_t  *)src, (int16_t  *)res, nx, ny); break;
        case TDC_DT_U16: pred2d_enc_u16(kind, (const uint16_t *)src, (uint16_t *)res, nx, ny); break;
        case TDC_DT_I32: pred2d_enc_i32(kind, (const int32_t  *)src, (int32_t  *)res, nx, ny); break;
        case TDC_DT_U32: pred2d_enc_u32(kind, (const uint32_t *)src, (uint32_t *)res, nx, ny); break;
        case TDC_DT_I64: pred2d_enc_i64(kind, (const int64_t  *)src, (int64_t  *)res, nx, ny); break;
        case TDC_DT_U64: pred2d_enc_u64(kind, (const uint64_t *)src, (uint64_t *)res, nx, ny); break;
        default: break;
    }
}

void pred2d_decode_sweep(tdc_dtype dt, tdc_pred2d_kind kind,
                         const uint8_t *res,
                         uint8_t *dst,
                         int64_t nx, int64_t ny) {
    /* u16 PAETH is the bench loser; route it through the wavefront kernel
     * which runs two independent dependency chains in parallel. The
     * fallback nx<2 case is handled by the wavefront itself; the ny<3
     * case still calls the wavefront which produces a one-row left-pred
     * row 0 and (if ny==2) a scalar trailing row. Round-trip is verified
     * by tests/test_pred2d_roundtrip.c on small rasters. */
    if (tdc_dtype_is_float(dt)) {
        pred2d_decode_float(dt, kind, res, dst, nx, ny);
        return;
    }
    if (dt == TDC_DT_U16 && kind == TDC_PRED2D_PAETH && nx > 0 && ny > 0) {
        /* Tiered dispatch: 4-row SSE2 wavefront for the steady state, 2-row
         * for smaller rasters, scalar for the smallest. The 4-row kernel
         * needs nx >= 4 for its triangular prologue and at least one full
         * quad after row 0 (ny >= 5); below that the 2-row version still
         * beats scalar and is always safe.
         *
         * AVX2 8-row wavefront (pred2d_dec_u16_paeth_wf8) is implemented
         * and round-trip-tested but NOT dispatched: bench on Raptor Lake
         * (i9-14900K) measures 0.41x of wf4 throughput. The cross-128-bit-
         * lane permute2x128 + alignr_epi8 adds ~3 cycles to the per-iter
         * critical path, and 8 _mm256_extract_epi32 scatter-stores vs 4
         * SSE2 extracts double the store-port pressure — neither helps
         * because the bottleneck is the row-above memory dependency, not
         * lane width. Confirms SPEEDUP-TODO N4 ("4-row wavefront is near
         * the uarch ceiling on x86_64"). The wf8 kernel is kept compiled
         * and round-trip-tested so a future re-bench (newer uarch, MSVC,
         * larger raster, fused pipeline) can re-enable it without redoing
         * the work. */
        if (nx >= 4 && ny >= 5) {
            pred2d_dec_u16_paeth_wf4((const uint16_t *)res,
                                      (uint16_t *)dst, nx, ny);
        } else {
            pred2d_dec_u16_paeth_wavefront((const uint16_t *)res,
                                            (uint16_t *)dst, nx, ny);
        }
        return;
    }
    switch (dt) {
        case TDC_DT_I8:  pred2d_dec_i8 (kind, (const int8_t   *)res, (int8_t   *)dst, nx, ny); break;
        case TDC_DT_U8:  pred2d_dec_u8 (kind, (const uint8_t  *)res, (uint8_t  *)dst, nx, ny); break;
        case TDC_DT_I16: pred2d_dec_i16(kind, (const int16_t  *)res, (int16_t  *)dst, nx, ny); break;
        case TDC_DT_U16: pred2d_dec_u16(kind, (const uint16_t *)res, (uint16_t *)dst, nx, ny); break;
        case TDC_DT_I32: pred2d_dec_i32(kind, (const int32_t  *)res, (int32_t  *)dst, nx, ny); break;
        case TDC_DT_U32: pred2d_dec_u32(kind, (const uint32_t *)res, (uint32_t *)dst, nx, ny); break;
        case TDC_DT_I64: pred2d_dec_i64(kind, (const int64_t  *)res, (int64_t  *)dst, nx, ny); break;
        case TDC_DT_U64: pred2d_dec_u64(kind, (const uint64_t *)res, (uint64_t *)dst, nx, ny); break;
        default: break;
    }
}

/* ----- Float predictor path ---------------------------------------------- */
/*
 * Float dtypes use ordered-integer mapping for lossless round-trip:
 *   1. Load float bits → ordered uint (preserves numerical ordering)
 *   2. Compute prediction in uint64 space (same predictor logic as integers)
 *   3. Store residual = value_ordered - pred_ordered (unsigned wrap)
 *
 * Uses uint64_t as the working type for all float widths. For f64, the
 * Paeth formula casts to int64 — the relative distances are preserved
 * because the ordered mapping is monotone. Overflow of p = a + b - c in
 * int64 is theoretically possible but requires neighboring pixels to span
 * the full float64 range, which never arises in real raster data.
 *
 * The switch-on-dtype load/store is slower than the typed macro kernels
 * used for integers but correct and maintainable. Optimization (per-width
 * typed float kernels) can be added later if profiling shows a need.
 */

/* Float prediction helpers: shared implementation in float_pred_helpers.h. */
#define pred2d_load_ordered        tdc_float_load_ordered
#define pred2d_store_ordered_residual tdc_float_store_ordered_residual
#define pred2d_load_residual       tdc_float_load_residual
#define pred2d_store_float         tdc_float_store_float
#define paeth_ordered              tdc_float_paeth_ordered

static inline uint64_t pred2d_float_predict(tdc_pred2d_kind kind,
                                            uint64_t left, uint64_t up,
                                            uint64_t upleft) {
    switch (kind) {
        case TDC_PRED2D_LEFT:    return left;
        case TDC_PRED2D_UP:      return up;
        case TDC_PRED2D_AVERAGE: return (left + up) / 2u;
        case TDC_PRED2D_PAETH:   return paeth_ordered(left, up, upleft);
        default:                 return 0;
    }
}

static void pred2d_encode_float(tdc_dtype dt, tdc_pred2d_kind kind,
                                const uint8_t *src, uint8_t *res,
                                int64_t nx, int64_t ny) {
    if (nx <= 0 || ny <= 0) return;

    /* (0,0): pred = 0. */
    uint64_t val00 = pred2d_load_ordered(dt, src, 0);
    pred2d_store_ordered_residual(dt, res, 0, val00);

    /* row 0, col >= 1: only `left` is in-bounds. */
    for (int64_t col = 1; col < nx; ++col) {
        uint64_t val  = pred2d_load_ordered(dt, src, col);
        uint64_t left = pred2d_load_ordered(dt, src, col - 1);
        uint64_t pred = (kind == TDC_PRED2D_UP) ? 0 :
                        (kind == TDC_PRED2D_AVERAGE) ? left / 2u : left;
        pred2d_store_ordered_residual(dt, res, col, val - pred);
    }

    /* rows >= 1. */
    for (int64_t row = 1; row < ny; ++row) {
        int64_t r0 = row * nx;
        /* col 0: only `up` is in-bounds. */
        {
            uint64_t val = pred2d_load_ordered(dt, src, r0);
            uint64_t up  = pred2d_load_ordered(dt, src, r0 - nx);
            uint64_t pred = (kind == TDC_PRED2D_LEFT) ? 0 :
                            (kind == TDC_PRED2D_AVERAGE) ? up / 2u : up;
            pred2d_store_ordered_residual(dt, res, r0, val - pred);
        }
        for (int64_t col = 1; col < nx; ++col) {
            int64_t i      = r0 + col;
            uint64_t val    = pred2d_load_ordered(dt, src, i);
            uint64_t left   = pred2d_load_ordered(dt, src, i - 1);
            uint64_t up     = pred2d_load_ordered(dt, src, i - nx);
            uint64_t upleft = pred2d_load_ordered(dt, src, i - nx - 1);
            uint64_t pred   = pred2d_float_predict(kind, left, up, upleft);
            pred2d_store_ordered_residual(dt, res, i, val - pred);
        }
    }
}

static void pred2d_decode_float(tdc_dtype dt, tdc_pred2d_kind kind,
                                const uint8_t *res, uint8_t *dst,
                                int64_t nx, int64_t ny) {
    if (nx <= 0 || ny <= 0) return;

    /* (0,0): pred = 0 → ordered = residual. */
    uint64_t val00 = pred2d_load_residual(dt, res, 0);
    pred2d_store_float(dt, dst, 0, val00);

    /* row 0, col >= 1. */
    for (int64_t col = 1; col < nx; ++col) {
        uint64_t rv   = pred2d_load_residual(dt, res, col);
        uint64_t left = pred2d_load_ordered(dt, dst, col - 1);
        uint64_t pred = (kind == TDC_PRED2D_UP) ? 0 :
                        (kind == TDC_PRED2D_AVERAGE) ? left / 2u : left;
        uint64_t ordered = rv + pred;
        pred2d_store_float(dt, dst, col, ordered);
    }

    /* rows >= 1. */
    for (int64_t row = 1; row < ny; ++row) {
        int64_t r0 = row * nx;
        /* col 0. */
        {
            uint64_t rv = pred2d_load_residual(dt, res, r0);
            uint64_t up = pred2d_load_ordered(dt, dst, r0 - nx);
            uint64_t pred = (kind == TDC_PRED2D_LEFT) ? 0 :
                            (kind == TDC_PRED2D_AVERAGE) ? up / 2u : up;
            pred2d_store_float(dt, dst, r0, rv + pred);
        }
        for (int64_t col = 1; col < nx; ++col) {
            int64_t i      = r0 + col;
            uint64_t rv     = pred2d_load_residual(dt, res, i);
            uint64_t left   = pred2d_load_ordered(dt, dst, i - 1);
            uint64_t up     = pred2d_load_ordered(dt, dst, i - nx);
            uint64_t upleft = pred2d_load_ordered(dt, dst, i - nx - 1);
            uint64_t pred   = pred2d_float_predict(kind, left, up, upleft);
            pred2d_store_float(dt, dst, i, rv + pred);
        }
    }
}

/* ----- Auto-select (cold path) ------------------------------------------- */
/*
 * Score each of LEFT/UP/AVERAGE/PAETH on a prefix of up to 10000 elements
 * and return the kind with the smallest sum of absolute residuals. Same
 * heuristic as vectra (vtr_codec.c:auto_select_predictor) — cheap, makes
 * no allocations, runs once per encode call. Kept on the int64 reference
 * path because the prefix is small enough that specialization wouldn't
 * change end-to-end throughput.
 */

static inline uint64_t pred2d_load_u(tdc_dtype dt, const uint8_t *base, int64_t i) {
    return (uint64_t)tdc_model_load(dt, base, i);
}

/* Unsigned wrap-min absolute difference: equals |int64(a) - int64(b)| when
 * the difference fits in int64, and the smaller wrap-distance otherwise.
 * Used for predictor scoring where exact precision at the int64 boundary is
 * not required. */
static inline uint64_t pred2d_abs_diff_u(uint64_t a, uint64_t b) {
    uint64_t d = a - b;
    uint64_t nd = (uint64_t)0 - d;
    return (d <= nd) ? d : nd;
}

/* Unsigned-arithmetic Paeth used by auto-select. Same selection rule as
 * paeth64 but every step uses well-defined unsigned wraparound, so float
 * bit-pattern inputs (full int64 range) cannot trip UBSAN's signed-overflow
 * sanitizer. The choice is between {a, b, c}; for inputs that fit in int64
 * the result equals paeth64. */
static inline uint64_t paeth64_u(uint64_t a, uint64_t b, uint64_t c) {
    uint64_t p  = a + b - c;
    uint64_t pa = pred2d_abs_diff_u(p, a);
    uint64_t pb = pred2d_abs_diff_u(p, b);
    uint64_t pc = pred2d_abs_diff_u(p, c);
    uint64_t r  = (pb <= pc) ? b : c;
    return ((pa <= pb) && (pa <= pc)) ? a : r;
}

static uint64_t pred2d_compute_u(tdc_pred2d_kind kind,
                                 uint64_t left, uint64_t up, uint64_t upleft) {
    switch (kind) {
        case TDC_PRED2D_LEFT:    return left;
        case TDC_PRED2D_UP:      return up;
        /* Bit-trick unsigned average — no carry overflow, no /2 of a wrapped
         * sum. Equals (left + up) / 2 in mathematical sense for unsigned. */
        case TDC_PRED2D_AVERAGE: return (left & up) + ((left ^ up) >> 1);
        case TDC_PRED2D_PAETH:   return paeth64_u(left, up, upleft);
        default:                 return 0;
    }
}

#define PRED2D_AUTO_SAMPLE 10000

tdc_pred2d_kind pred2d_auto_select(tdc_dtype dt, const uint8_t *src,
                                   int64_t nx, int64_t ny) {
    int64_t n = nx * ny;
    int64_t sample_n = n < PRED2D_AUTO_SAMPLE ? n : PRED2D_AUTO_SAMPLE;
    /* sample is a row-aligned prefix so the predictor sees the same
     * neighborhood structure it will see at full size. */
    int64_t sample_rows = sample_n / nx;
    if (sample_rows < 2) sample_rows = ny < 2 ? ny : 2; /* always score at least 2 rows when possible */
    if (sample_rows > ny) sample_rows = ny;

    tdc_pred2d_kind best_kind = TDC_PRED2D_AVERAGE;
    uint64_t        best_sum  = UINT64_MAX;

    static const tdc_pred2d_kind candidates[4] = {
        TDC_PRED2D_LEFT, TDC_PRED2D_UP, TDC_PRED2D_AVERAGE, TDC_PRED2D_PAETH
    };

    for (int k = 0; k < 4; ++k) {
        tdc_pred2d_kind kind = candidates[k];
        uint64_t sum = 0;
        for (int64_t row = 0; row < sample_rows; ++row) {
            for (int64_t col = 0; col < nx; ++col) {
                int64_t i       = row * nx + col;
                uint64_t val    = pred2d_load_u(dt, src, i);
                uint64_t left   = (col > 0)              ? pred2d_load_u(dt, src, i - 1)      : 0;
                uint64_t up     = (row > 0)              ? pred2d_load_u(dt, src, i - nx)     : 0;
                uint64_t upleft = (col > 0 && row > 0)   ? pred2d_load_u(dt, src, i - nx - 1) : 0;
                uint64_t pred   = pred2d_compute_u(kind, left, up, upleft);
                sum += pred2d_abs_diff_u(val, pred);
            }
        }
        if (sum < best_sum) {
            best_sum  = sum;
            best_kind = kind;
        }
    }

    return best_kind;
}

/* ----- Encode ------------------------------------------------------------- */

static tdc_status pred2d_encode(const tdc_block *in,
                                const void      *params,
                                tdc_buffer      *residual_out,
                                tdc_dtype       *residual_dtype,
                                tdc_buffer      *side_out) {
    if (!in || !residual_out || !residual_out->realloc_fn) return TDC_E_INVAL;
    if (!side_out || !side_out->realloc_fn)                return TDC_E_INVAL;
    if (in->layout != TDC_LAYOUT_RASTER_2D) return TDC_E_LAYOUT;
    if (in->shape.rank != 2)                return TDC_E_SHAPE;
    if (!pred2d_dtype_accepted(in->dtype))  return TDC_E_DTYPE;

    int64_t ny = in->shape.dim[0];
    int64_t nx = in->shape.dim[1];
    if (nx < 0 || ny < 0)                                     return TDC_E_SHAPE;
    if (nx != 0 && ny != 0 && nx > INT64_MAX / ny)            return TDC_E_SHAPE;

    size_t  elem_size = tdc_dtype_size(in->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    /* Resolve predictor kind. */
    tdc_pred2d_kind kind = TDC_PRED2D_AUTO;
    if (params) {
        const tdc_pred2d_params *p = (const tdc_pred2d_params *)params;
        kind = p->kind;
    }

    int64_t n = nx * ny;
    if (kind == TDC_PRED2D_AUTO) {
        if (n > 0) {
            if (!in->data) return TDC_E_INVAL;
            kind = pred2d_auto_select(in->dtype, (const uint8_t *)in->data, nx, ny);
        } else {
            kind = TDC_PRED2D_AVERAGE; /* arbitrary; nothing to encode */
        }
    } else if (kind != TDC_PRED2D_LEFT && kind != TDC_PRED2D_UP &&
               kind != TDC_PRED2D_AVERAGE && kind != TDC_PRED2D_PAETH) {
        return TDC_E_INVAL; /* PLANE / unknown — not handled by this file */
    }

    /* Side metadata: 1 byte = resolved kind. */
    tdc_status st = tdc_buf_reserve(side_out, 1u);
    if (st != TDC_OK) return st;
    side_out->data[0] = (uint8_t)kind;
    side_out->size    = 1u;

    /* Reserve residual output. */
    size_t bytes = (size_t)n * elem_size;
    st = tdc_buf_reserve(residual_out, bytes);
    if (st != TDC_OK) return st;

    if (residual_dtype) *residual_dtype = in->dtype;

    if (n == 0) {
        residual_out->size = 0;
        return TDC_OK;
    }

    if (!in->data) return TDC_E_INVAL;

    pred2d_encode_sweep(in->dtype, kind,
                        (const uint8_t *)in->data,
                        residual_out->data,
                        nx, ny);
    residual_out->size = bytes;
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status pred2d_decode(tdc_block      *out,
                                const void     *params,
                                tdc_dtype       residual_dtype,
                                const uint8_t  *residuals, size_t residual_size,
                                const uint8_t  *side_meta, size_t side_size) {
    (void)params;
    if (!out) return TDC_E_INVAL;
    if (out->layout != TDC_LAYOUT_RASTER_2D) return TDC_E_LAYOUT;
    if (out->shape.rank != 2)                return TDC_E_SHAPE;
    if (residual_dtype != out->dtype)        return TDC_E_DTYPE;
    if (!pred2d_dtype_accepted(out->dtype))  return TDC_E_DTYPE;

    int64_t ny = out->shape.dim[0];
    int64_t nx = out->shape.dim[1];
    if (nx < 0 || ny < 0)                                     return TDC_E_SHAPE;
    if (nx != 0 && ny != 0 && nx > INT64_MAX / ny)            return TDC_E_SHAPE;

    size_t elem_size = tdc_dtype_size(out->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n     = nx * ny;
    size_t  bytes = (size_t)n * elem_size;
    if (residual_size != bytes) return TDC_E_CORRUPT;

    /* Side metadata: exactly 1 byte = the resolved predictor kind. */
    if (side_size != 1u || side_meta == NULL) return TDC_E_CORRUPT;
    tdc_pred2d_kind kind = (tdc_pred2d_kind)side_meta[0];
    if (kind != TDC_PRED2D_LEFT && kind != TDC_PRED2D_UP &&
        kind != TDC_PRED2D_AVERAGE && kind != TDC_PRED2D_PAETH) {
        return TDC_E_CORRUPT;
    }

    if (n == 0) return TDC_OK;
    if (!out->data || !residuals) return TDC_E_INVAL;

    pred2d_decode_sweep(out->dtype, kind, residuals, (uint8_t *)out->data, nx, ny);
    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_model_vt tdc_model_pred2d_vt = {
    .id               = TDC_MODEL_PRED_2D,
    .name             = "pred2d",
    .accepted_dtypes  = PRED2D_ACCEPTED_DTYPES,
    .accepted_layouts = PRED2D_ACCEPTED_LAYOUTS,
    .encode           = pred2d_encode,
    .decode           = pred2d_decode,
};
