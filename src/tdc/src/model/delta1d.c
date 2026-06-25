/*
 * src/model/delta1d.c
 *
 * TDC_MODEL_DELTA_1D — first-order differencing along VECTOR_1D blocks.
 *
 *   residual[0] = data[0]
 *   residual[i] = data[i] - data[i-1]      (i >= 1, modular at width N)
 *
 *   data[0]     = residual[0]
 *   data[i]     = data[i-1] + residual[i]  (i >= 1, modular at width N)
 *
 * Accepts every fixed-width integer dtype (i8/i16/i32/i64/u8/u16/u32/u64).
 * The residual_dtype reported to the chain is the input dtype unchanged
 * — the residual stream has the same width and signedness as the input.
 * Slowly varying or monotonic columns produce small-magnitude residuals
 * which feed zigzag → byte-shuffle → LZ well.
 *
 * Side metadata: NONE. The seed value lives in residual[0]. side_out is
 * left at size = 0 on encode and a non-zero side_size on decode is
 * rejected as TDC_E_CORRUPT. (No side metadata is the cleanest possible
 * extraction of vectra's delta path: there is genuinely nothing to
 * carry — the encoder writes deltas directly into a contiguous output
 * stream and the seed is the first element of that stream.)
 *
 * Modular arithmetic note:
 *   The kernel is written entirely in unsigned arithmetic of the matching
 *   width. Subtraction and addition wrap modulo 2^N, which is well-defined
 *   in C for unsigned types and is the only formulation that round-trips
 *   correctly across the full range of any input — including the i64 case
 *   where (data[i] - data[i-1]) would otherwise overflow signed range.
 *   The signed → unsigned reinterpretation is bit-preserving on every
 *   two's-complement target (which tdc requires).
 *
 * Validity bitmap:
 *   Ignored. The residual stream covers all n_elems regardless of the
 *   per-element validity flag. NA-aware delta is a future model concern;
 *   for now, vectra's caller carries the validity bitmap separately and
 *   the model just reproduces whatever bytes were in the input.
 *
 * Source today: vectra/src/vtr_codec.c:1380-1408 (delta_encode/delta_decode).
 * The vectra path was i64-only, single-threaded, and longjmp'd on alloc
 * failure. tdc generalizes to all integer widths, uses realloc_fn, and
 * returns status codes.
 *
 * Properties:
 *   accepted_dtypes  = I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64
 *   accepted_layouts = VECTOR_1D
 */

#include "tdc/model.h"
#include "model_internal.h"
#include "model_load_store.h"
#include "../core/buffer.h"
#include "../core/float_order.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Optional SIMD: vectorized inverse-delta (prefix sum) --------------- *
 * The inverse delta is one prefix-sum pass over the residual stream. SSE2
 * and NEON both have a clean log2(n) shift+add lowering. We only specialize
 * the 4-byte and 8-byte element widths because they are what the bench
 * exercises and what real signed integer rasters / time-series tend to use.
 * Scalar fallback is the byte-by-byte loop in the switch below.
 *
 * Acceptance: DELTA1D+LZ decode on the i32 ramp ≥ 6 GB/s, i.e. **above**
 * the API memcpy ceiling — which is the right outcome when the entropy
 * stage produces fewer bytes than the output. SPEEDUP-TODO P2.2.
 */
#if defined(__SSE2__) || defined(_M_X64) || \
    (defined(_M_IX86_FP) && _M_IX86_FP >= 2)
#  include <emmintrin.h>
#  define TDC_DELTA1D_HAVE_SSE2 1
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#  include <arm_neon.h>
#  define TDC_DELTA1D_HAVE_NEON 1
#endif

#ifdef TDC_DELTA1D_HAVE_SSE2
/* Inverse delta for 4-byte elements: 4-lane SSE2 prefix-sum. */
static void inverse_delta_4_sse2(uint8_t *dst, const uint8_t *src, int64_t n) {
    if (n <= 0) return;
    uint32_t acc;
    memcpy(&acc, src, 4u);
    memcpy(dst, &acc, 4u);
    int64_t i = 1;
    /* Run a 4-lane chunk loop once we are 4-aligned in element index. */
    __m128i carry = _mm_set1_epi32((int)acc);
    /* First, scalar until i is at the start of a 4-element chunk. */
    while (i < n && (i & 3) != 0) {
        uint32_t d;
        memcpy(&d, src + (size_t)i * 4u, 4u);
        acc = acc + d;
        memcpy(dst + (size_t)i * 4u, &acc, 4u);
        ++i;
    }
    carry = _mm_set1_epi32((int)acc);
    for (; i + 4 <= n; i += 4) {
        __m128i v = _mm_loadu_si128((const __m128i *)(src + (size_t)i * 4u));
        /* prefix sum within the 4 lanes */
        v = _mm_add_epi32(v, _mm_slli_si128(v, 4));
        v = _mm_add_epi32(v, _mm_slli_si128(v, 8));
        v = _mm_add_epi32(v, carry);
        _mm_storeu_si128((__m128i *)(dst + (size_t)i * 4u), v);
        /* broadcast the last lane as the next carry */
        carry = _mm_shuffle_epi32(v, _MM_SHUFFLE(3, 3, 3, 3));
    }
    /* Recover scalar acc from carry for the tail. */
    {
        uint32_t tmp[4];
        _mm_storeu_si128((__m128i *)tmp, carry);
        acc = tmp[3];
    }
    for (; i < n; ++i) {
        uint32_t d;
        memcpy(&d, src + (size_t)i * 4u, 4u);
        acc = acc + d;
        memcpy(dst + (size_t)i * 4u, &acc, 4u);
    }
}

/* Inverse delta for 8-byte elements: 2-lane SSE2 prefix-sum. */
static void inverse_delta_8_sse2(uint8_t *dst, const uint8_t *src, int64_t n) {
    if (n <= 0) return;
    uint64_t acc;
    memcpy(&acc, src, 8u);
    memcpy(dst, &acc, 8u);
    int64_t i = 1;
    while (i < n && (i & 1) != 0) {
        uint64_t d;
        memcpy(&d, src + (size_t)i * 8u, 8u);
        acc = acc + d;
        memcpy(dst + (size_t)i * 8u, &acc, 8u);
        ++i;
    }
    __m128i carry = _mm_set_epi64x((long long)acc, (long long)acc);
    for (; i + 2 <= n; i += 2) {
        __m128i v = _mm_loadu_si128((const __m128i *)(src + (size_t)i * 8u));
        /* prefix sum: v[1] += v[0] */
        v = _mm_add_epi64(v, _mm_slli_si128(v, 8));
        v = _mm_add_epi64(v, carry);
        _mm_storeu_si128((__m128i *)(dst + (size_t)i * 8u), v);
        /* broadcast the high lane as the next carry */
        carry = _mm_shuffle_epi32(v, _MM_SHUFFLE(3, 2, 3, 2));
    }
    {
        uint64_t tmp[2];
        _mm_storeu_si128((__m128i *)tmp, carry);
        acc = tmp[1];
    }
    for (; i < n; ++i) {
        uint64_t d;
        memcpy(&d, src + (size_t)i * 8u, 8u);
        acc = acc + d;
        memcpy(dst + (size_t)i * 8u, &acc, 8u);
    }
}
#endif /* TDC_DELTA1D_HAVE_SSE2 */

#ifdef TDC_DELTA1D_HAVE_NEON
static void inverse_delta_4_neon(uint8_t *dst, const uint8_t *src, int64_t n) {
    if (n <= 0) return;
    uint32_t acc;
    memcpy(&acc, src, 4u);
    memcpy(dst, &acc, 4u);
    int64_t i = 1;
    while (i < n && (i & 3) != 0) {
        uint32_t d;
        memcpy(&d, src + (size_t)i * 4u, 4u);
        acc = acc + d;
        memcpy(dst + (size_t)i * 4u, &acc, 4u);
        ++i;
    }
    uint32x4_t carry = vdupq_n_u32(acc);
    const uint32x4_t zero = vdupq_n_u32(0);
    for (; i + 4 <= n; i += 4) {
        uint32x4_t v = vld1q_u32((const uint32_t *)(src + (size_t)i * 4u));
        v = vaddq_u32(v, vextq_u32(zero, v, 3));  /* shift-left 1 lane */
        v = vaddq_u32(v, vextq_u32(zero, v, 2));  /* shift-left 2 lanes */
        v = vaddq_u32(v, carry);
        vst1q_u32((uint32_t *)(dst + (size_t)i * 4u), v);
        carry = vdupq_n_u32(vgetq_lane_u32(v, 3));
    }
    acc = vgetq_lane_u32(carry, 3);
    for (; i < n; ++i) {
        uint32_t d;
        memcpy(&d, src + (size_t)i * 4u, 4u);
        acc = acc + d;
        memcpy(dst + (size_t)i * 4u, &acc, 4u);
    }
}

static void inverse_delta_8_neon(uint8_t *dst, const uint8_t *src, int64_t n) {
    if (n <= 0) return;
    uint64_t acc;
    memcpy(&acc, src, 8u);
    memcpy(dst, &acc, 8u);
    int64_t i = 1;
    while (i < n && (i & 1) != 0) {
        uint64_t d;
        memcpy(&d, src + (size_t)i * 8u, 8u);
        acc = acc + d;
        memcpy(dst + (size_t)i * 8u, &acc, 8u);
        ++i;
    }
    uint64x2_t carry = vdupq_n_u64(acc);
    const uint64x2_t zero = vdupq_n_u64(0);
    for (; i + 2 <= n; i += 2) {
        uint64x2_t v = vld1q_u64((const uint64_t *)(src + (size_t)i * 8u));
        v = vaddq_u64(v, vextq_u64(zero, v, 1));
        v = vaddq_u64(v, carry);
        vst1q_u64((uint64_t *)(dst + (size_t)i * 8u), v);
        carry = vdupq_n_u64(vgetq_lane_u64(v, 1));
    }
    acc = vgetq_lane_u64(carry, 1);
    for (; i < n; ++i) {
        uint64_t d;
        memcpy(&d, src + (size_t)i * 8u, 8u);
        acc = acc + d;
        memcpy(dst + (size_t)i * 8u, &acc, 8u);
    }
}
#endif /* TDC_DELTA1D_HAVE_NEON */

#ifdef TDC_DELTA1D_HAVE_SSE2
/*
 * Forward delta for 2-byte elements: 8-lane epi16.
 *
 * In a chunk [a0..a7], the desired output is [a0-prev, a1-a0, ..., a7-a6].
 * That is: chunk - left_shift_1(chunk) + carry_correction.
 *
 *   shifted  = [prev_last, a0, a1, ..., a6]   (left-shift-by-1-lane)
 *   out      = chunk - shifted
 *
 * Carry into the next iteration is a7 (the last lane of this chunk).
 */
static void forward_delta_2_sse2(const uint8_t *src, uint8_t *dst, int64_t n) {
    if (n <= 0) return;
    /* Element 0 is the seed — copy verbatim. */
    memcpy(dst, src, 2u);
    if (n == 1) return;

    int64_t i = 1;
    uint16_t prev;
    memcpy(&prev, src, 2u);

    /* Scalar until we reach an 8-element chunk boundary. */
    while (i < n && (i & 7) != 0) {
        uint16_t cur;
        memcpy(&cur, src + (size_t)i * 2u, 2u);
        uint16_t d = (uint16_t)(cur - prev);
        memcpy(dst + (size_t)i * 2u, &d, 2u);
        prev = cur;
        ++i;
    }

    /* Load prev into high lane of carry register.
     * _mm_slli_si128 shifts the whole 128-bit register left by N bytes.
     * For epi16, shifting by 2 bytes moves lanes right by one position,
     * inserting a zero in lane 0.  We inject `prev` by constructing a
     * carry vector = [prev, 0, 0, 0, 0, 0, 0, 0] and then using it to
     * fill lane 0 after the shift. */
    __m128i carry16 = _mm_setzero_si128();
    carry16 = _mm_insert_epi16(carry16, (int)(uint16_t)prev, 0);

    for (; i + 8 <= n; i += 8) {
        __m128i v = _mm_loadu_si128((const __m128i *)(src + (size_t)i * 2u));
        /* shifted = [prev_last, v0, v1, ..., v6]  (bytes shifted left by 2) */
        __m128i shifted = _mm_or_si128(_mm_slli_si128(v, 2), carry16);
        __m128i out = _mm_sub_epi16(v, shifted);
        _mm_storeu_si128((__m128i *)(dst + (size_t)i * 2u), out);
        /* carry16 = [v7, 0, 0, ...]  ready for next iteration */
        carry16 = _mm_srli_si128(v, 14); /* shift right by 14 bytes -> lane 7 -> byte 0 */
    }

    /* Recover scalar prev from carry16. */
    {
        uint16_t tmp[8];
        _mm_storeu_si128((__m128i *)tmp, carry16);
        prev = tmp[0];
    }

    for (; i < n; ++i) {
        uint16_t cur;
        memcpy(&cur, src + (size_t)i * 2u, 2u);
        uint16_t d = (uint16_t)(cur - prev);
        memcpy(dst + (size_t)i * 2u, &d, 2u);
        prev = cur;
    }
}

/*
 * Forward delta for 4-byte elements: 4-lane epi32.
 * Same idea: shifted = [prev_last, v0, v1, v2], out = v - shifted.
 */
static void forward_delta_4_sse2(const uint8_t *src, uint8_t *dst, int64_t n) {
    if (n <= 0) return;
    memcpy(dst, src, 4u);
    if (n == 1) return;

    int64_t i = 1;
    uint32_t prev;
    memcpy(&prev, src, 4u);

    while (i < n && (i & 3) != 0) {
        uint32_t cur;
        memcpy(&cur, src + (size_t)i * 4u, 4u);
        uint32_t d = cur - prev;
        memcpy(dst + (size_t)i * 4u, &d, 4u);
        prev = cur;
        ++i;
    }

    /* carry = [prev, 0, 0, 0] */
    __m128i carry32 = _mm_cvtsi32_si128((int)(uint32_t)prev);

    for (; i + 4 <= n; i += 4) {
        __m128i v = _mm_loadu_si128((const __m128i *)(src + (size_t)i * 4u));
        /* shifted = [prev_last, v0, v1, v2] */
        __m128i shifted = _mm_or_si128(_mm_slli_si128(v, 4), carry32);
        __m128i out = _mm_sub_epi32(v, shifted);
        _mm_storeu_si128((__m128i *)(dst + (size_t)i * 4u), out);
        /* carry32 = [v3, 0, 0, 0] */
        carry32 = _mm_srli_si128(v, 12);
    }

    {
        uint32_t tmp[4];
        _mm_storeu_si128((__m128i *)tmp, carry32);
        prev = tmp[0];
    }

    for (; i < n; ++i) {
        uint32_t cur;
        memcpy(&cur, src + (size_t)i * 4u, 4u);
        uint32_t d = cur - prev;
        memcpy(dst + (size_t)i * 4u, &d, 4u);
        prev = cur;
    }
}

/*
 * Forward delta for 8-byte elements: 2-lane epi64.
 * shifted = [prev_last, v0], out = v - shifted.
 */
static void forward_delta_8_sse2(const uint8_t *src, uint8_t *dst, int64_t n) {
    if (n <= 0) return;
    memcpy(dst, src, 8u);
    if (n == 1) return;

    int64_t i = 1;
    uint64_t prev;
    memcpy(&prev, src, 8u);

    while (i < n && (i & 1) != 0) {
        uint64_t cur;
        memcpy(&cur, src + (size_t)i * 8u, 8u);
        uint64_t d = cur - prev;
        memcpy(dst + (size_t)i * 8u, &d, 8u);
        prev = cur;
        ++i;
    }

    /* carry = [prev, 0] (low 64 bits) */
    __m128i carry64 = _mm_set_epi64x(0, (long long)(uint64_t)prev);

    for (; i + 2 <= n; i += 2) {
        __m128i v = _mm_loadu_si128((const __m128i *)(src + (size_t)i * 8u));
        /* shifted = [prev_last, v0] */
        __m128i shifted = _mm_or_si128(_mm_slli_si128(v, 8), carry64);
        __m128i out = _mm_sub_epi64(v, shifted);
        _mm_storeu_si128((__m128i *)(dst + (size_t)i * 8u), out);
        /* carry64 = [v1, 0] */
        carry64 = _mm_srli_si128(v, 8);
    }

    {
        uint64_t tmp[2];
        _mm_storeu_si128((__m128i *)tmp, carry64);
        prev = tmp[0];
    }

    for (; i < n; ++i) {
        uint64_t cur;
        memcpy(&cur, src + (size_t)i * 8u, 8u);
        uint64_t d = cur - prev;
        memcpy(dst + (size_t)i * 8u, &d, 8u);
        prev = cur;
    }
}

/*
 * Inverse delta for 2-byte elements: 8-lane epi16 prefix-sum.
 * Mirrors inverse_delta_4_sse2 but uses epi16 and 8 lanes.
 */
static void inverse_delta_2_sse2(uint8_t *dst, const uint8_t *src, int64_t n) {
    if (n <= 0) return;
    uint16_t acc;
    memcpy(&acc, src, 2u);
    memcpy(dst, &acc, 2u);
    int64_t i = 1;
    while (i < n && (i & 7) != 0) {
        uint16_t d;
        memcpy(&d, src + (size_t)i * 2u, 2u);
        acc = (uint16_t)(acc + d);
        memcpy(dst + (size_t)i * 2u, &acc, 2u);
        ++i;
    }
    __m128i carry = _mm_set1_epi16((short)(uint16_t)acc);
    for (; i + 8 <= n; i += 8) {
        __m128i v = _mm_loadu_si128((const __m128i *)(src + (size_t)i * 2u));
        /* 8-lane prefix sum via log2(8)=3 shift+add passes */
        v = _mm_add_epi16(v, _mm_slli_si128(v,  2)); /* +1 lane */
        v = _mm_add_epi16(v, _mm_slli_si128(v,  4)); /* +2 lanes */
        v = _mm_add_epi16(v, _mm_slli_si128(v,  8)); /* +4 lanes */
        v = _mm_add_epi16(v, carry);
        _mm_storeu_si128((__m128i *)(dst + (size_t)i * 2u), v);
        /* broadcast lane 7 (the last epi16 in the register) */
        carry = _mm_shufflelo_epi16(_mm_srli_si128(v, 14), _MM_SHUFFLE(0, 0, 0, 0));
        carry = _mm_unpacklo_epi16(carry, carry);
        carry = _mm_unpacklo_epi32(carry, carry);
        carry = _mm_unpacklo_epi64(carry, carry);
    }
    {
        uint16_t tmp[8];
        _mm_storeu_si128((__m128i *)tmp, carry);
        acc = tmp[0];
    }
    for (; i < n; ++i) {
        uint16_t d;
        memcpy(&d, src + (size_t)i * 2u, 2u);
        acc = (uint16_t)(acc + d);
        memcpy(dst + (size_t)i * 2u, &acc, 2u);
    }
}
#endif /* TDC_DELTA1D_HAVE_SSE2 */

#ifdef TDC_DELTA1D_HAVE_NEON
/*
 * Forward delta for 2-byte elements: 8-lane uint16x8_t.
 * shifted = vextq_u16(carry, v, 7)  = [carry_lane0, v0..v6]
 * out     = v - shifted
 */
static void forward_delta_2_neon(const uint8_t *src, uint8_t *dst, int64_t n) {
    if (n <= 0) return;
    memcpy(dst, src, 2u);
    if (n == 1) return;

    int64_t i = 1;
    uint16_t prev;
    memcpy(&prev, src, 2u);

    while (i < n && (i & 7) != 0) {
        uint16_t cur;
        memcpy(&cur, src + (size_t)i * 2u, 2u);
        uint16_t d = (uint16_t)(cur - prev);
        memcpy(dst + (size_t)i * 2u, &d, 2u);
        prev = cur;
        ++i;
    }

    uint16x8_t carry = vdupq_n_u16(0);
    carry = vsetq_lane_u16(prev, carry, 7);

    for (; i + 8 <= n; i += 8) {
        uint16x8_t v = vld1q_u16((const uint16_t *)(src + (size_t)i * 2u));
        /* shifted = [carry[7], v0, v1, ..., v6] */
        uint16x8_t shifted = vextq_u16(carry, v, 7);
        uint16x8_t out = vsubq_u16(v, shifted);
        vst1q_u16((uint16_t *)(dst + (size_t)i * 2u), out);
        /* carry = v for next iteration (only lane 7 matters) */
        carry = v;
        prev = vgetq_lane_u16(v, 7);
    }
    (void)prev; /* used in scalar tail */
    {
        /* recover prev from carry lane 7 */
        prev = vgetq_lane_u16(carry, 7);
    }

    for (; i < n; ++i) {
        uint16_t cur;
        memcpy(&cur, src + (size_t)i * 2u, 2u);
        uint16_t d = (uint16_t)(cur - prev);
        memcpy(dst + (size_t)i * 2u, &d, 2u);
        prev = cur;
    }
}

/*
 * Forward delta for 4-byte elements: 4-lane uint32x4_t.
 * shifted = vextq_u32(carry, v, 3) = [carry[3], v0, v1, v2]
 */
static void forward_delta_4_neon(const uint8_t *src, uint8_t *dst, int64_t n) {
    if (n <= 0) return;
    memcpy(dst, src, 4u);
    if (n == 1) return;

    int64_t i = 1;
    uint32_t prev;
    memcpy(&prev, src, 4u);

    while (i < n && (i & 3) != 0) {
        uint32_t cur;
        memcpy(&cur, src + (size_t)i * 4u, 4u);
        uint32_t d = cur - prev;
        memcpy(dst + (size_t)i * 4u, &d, 4u);
        prev = cur;
        ++i;
    }

    uint32x4_t carry = vdupq_n_u32(0);
    carry = vsetq_lane_u32(prev, carry, 3);

    for (; i + 4 <= n; i += 4) {
        uint32x4_t v = vld1q_u32((const uint32_t *)(src + (size_t)i * 4u));
        uint32x4_t shifted = vextq_u32(carry, v, 3);
        uint32x4_t out = vsubq_u32(v, shifted);
        vst1q_u32((uint32_t *)(dst + (size_t)i * 4u), out);
        carry = v;
    }
    prev = vgetq_lane_u32(carry, 3);

    for (; i < n; ++i) {
        uint32_t cur;
        memcpy(&cur, src + (size_t)i * 4u, 4u);
        uint32_t d = cur - prev;
        memcpy(dst + (size_t)i * 4u, &d, 4u);
        prev = cur;
    }
}

/*
 * Forward delta for 8-byte elements: 2-lane uint64x2_t.
 * shifted = vextq_u64(carry, v, 1) = [carry[1], v0]
 */
static void forward_delta_8_neon(const uint8_t *src, uint8_t *dst, int64_t n) {
    if (n <= 0) return;
    memcpy(dst, src, 8u);
    if (n == 1) return;

    int64_t i = 1;
    uint64_t prev;
    memcpy(&prev, src, 8u);

    while (i < n && (i & 1) != 0) {
        uint64_t cur;
        memcpy(&cur, src + (size_t)i * 8u, 8u);
        uint64_t d = cur - prev;
        memcpy(dst + (size_t)i * 8u, &d, 8u);
        prev = cur;
        ++i;
    }

    uint64x2_t carry = vdupq_n_u64(0);
    carry = vsetq_lane_u64(prev, carry, 1);

    for (; i + 2 <= n; i += 2) {
        uint64x2_t v = vld1q_u64((const uint64_t *)(src + (size_t)i * 8u));
        uint64x2_t shifted = vextq_u64(carry, v, 1);
        uint64x2_t out = vsubq_u64(v, shifted);
        vst1q_u64((uint64_t *)(dst + (size_t)i * 8u), out);
        carry = v;
    }
    prev = vgetq_lane_u64(carry, 1);

    for (; i < n; ++i) {
        uint64_t cur;
        memcpy(&cur, src + (size_t)i * 8u, 8u);
        uint64_t d = cur - prev;
        memcpy(dst + (size_t)i * 8u, &d, 8u);
        prev = cur;
    }
}

/*
 * Inverse delta for 2-byte elements: 8-lane uint16x8_t prefix-sum.
 * Mirrors inverse_delta_4_neon but with uint16x8_t and 3 passes.
 */
static void inverse_delta_2_neon(uint8_t *dst, const uint8_t *src, int64_t n) {
    if (n <= 0) return;
    uint16_t acc;
    memcpy(&acc, src, 2u);
    memcpy(dst, &acc, 2u);
    int64_t i = 1;
    while (i < n && (i & 7) != 0) {
        uint16_t d;
        memcpy(&d, src + (size_t)i * 2u, 2u);
        acc = (uint16_t)(acc + d);
        memcpy(dst + (size_t)i * 2u, &acc, 2u);
        ++i;
    }
    uint16x8_t carry = vdupq_n_u16(acc);
    const uint16x8_t zero = vdupq_n_u16(0);
    for (; i + 8 <= n; i += 8) {
        uint16x8_t v = vld1q_u16((const uint16_t *)(src + (size_t)i * 2u));
        /* prefix sum: 3 passes for 8 lanes */
        v = vaddq_u16(v, vextq_u16(zero, v, 7)); /* +1 lane */
        v = vaddq_u16(v, vextq_u16(zero, v, 6)); /* +2 lanes */
        v = vaddq_u16(v, vextq_u16(zero, v, 4)); /* +4 lanes */
        v = vaddq_u16(v, carry);
        vst1q_u16((uint16_t *)(dst + (size_t)i * 2u), v);
        carry = vdupq_n_u16(vgetq_lane_u16(v, 7));
    }
    acc = vgetq_lane_u16(carry, 7);
    for (; i < n; ++i) {
        uint16_t d;
        memcpy(&d, src + (size_t)i * 2u, 2u);
        acc = (uint16_t)(acc + d);
        memcpy(dst + (size_t)i * 2u, &acc, 2u);
    }
}
#endif /* TDC_DELTA1D_HAVE_NEON */

/* ----- Acceptance bitmasks ----------------------------------------------- */

#define DELTA1D_ACCEPTED_DTYPES (         \
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

#define DELTA1D_ACCEPTED_LAYOUTS TDC_LAYOUT_BIT(TDC_LAYOUT_VECTOR_1D)

static int delta1d_dtype_accepted(tdc_dtype dt) {
    return tdc_model_dtype_accepted(DELTA1D_ACCEPTED_DTYPES, dt);
}

/* ----- Float XOR-delta --------------------------------------------------- */
/*
 * For float dtypes, XOR-delta on raw bit patterns:
 *
 *   residual[0] = bits(data[0])                         (seed)
 *   residual[i] = bits(data[i]) XOR bits(data[i-1])     (i >= 1)
 *
 * decode:
 *   bits[0]     = residual[0]
 *   bits[i]     = bits[i-1] XOR residual[i]
 *
 * Why XOR, not ordered-subtract?
 *
 * Adjacent float samples with the same exponent share a long common bit
 * prefix (sign + exponent + high mantissa). XOR of those bit patterns
 * produces many leading zero bits — typically 4-5 leading zero BYTES for
 * smooth f64 time series. LZ exploits these zero runs directly.
 *
 * Ordered-subtract (the previous approach) preserves numerical ordering
 * but produces residuals proportional to float_delta * 2^(52 - exponent),
 * which spread across ~46 bits even for small deltas near magnitude 15000.
 * Those residuals have only ~2 leading zero bytes — far less compressible.
 *
 * On sign-crossing pairs (rare in real time series), both approaches
 * produce large residuals. XOR is no worse than ordered-subtract there
 * and is significantly better on the 99%+ same-sign pairs that dominate.
 *
 * Bench evidence: USGS streamflow f64 with ordered-subtract DELTA1D+LZ
 * scored 2.61x (worse than no-model BSHUF+LZ at 3.02x). XOR-delta
 * closes the gap against zstd. See SPEEDUP-TODO N1.
 */

#define DEFINE_DELTA1D_ENCODE_FLOAT(SUFFIX, UT, W)                        \
static void delta1d_encode_##SUFFIX(const uint8_t *src, uint8_t *dst, int64_t n) { \
    UT prev;                                                              \
    memcpy(&prev, src, (W));                                              \
    memcpy(dst, &prev, (W));                                              \
    for (int64_t i = 1; i < n; ++i) {                                    \
        UT cur;                                                           \
        memcpy(&cur, src + (size_t)i * (W), (W));                        \
        UT xd = cur ^ prev;                                              \
        memcpy(dst + (size_t)i * (W), &xd, (W));                        \
        prev = cur;                                                      \
    }                                                                    \
}

#define DEFINE_DELTA1D_DECODE_FLOAT(SUFFIX, UT, W)                        \
static void delta1d_decode_##SUFFIX(const uint8_t *residuals, uint8_t *dst, int64_t n) { \
    UT prev;                                                              \
    memcpy(&prev, residuals, (W));                                        \
    memcpy(dst, &prev, (W));                                              \
    for (int64_t i = 1; i < n; ++i) {                                    \
        UT xd;                                                            \
        memcpy(&xd, residuals + (size_t)i * (W), (W));                   \
        prev = prev ^ xd;                                                \
        memcpy(dst + (size_t)i * (W), &prev, (W));                       \
    }                                                                    \
}

DEFINE_DELTA1D_ENCODE_FLOAT(f16, uint16_t, 2u)
DEFINE_DELTA1D_ENCODE_FLOAT(f32, uint32_t, 4u)
DEFINE_DELTA1D_ENCODE_FLOAT(f64, uint64_t, 8u)

DEFINE_DELTA1D_DECODE_FLOAT(f16, uint16_t, 2u)
DEFINE_DELTA1D_DECODE_FLOAT(f32, uint32_t, 4u)
DEFINE_DELTA1D_DECODE_FLOAT(f64, uint64_t, 8u)

#undef DEFINE_DELTA1D_ENCODE_FLOAT
#undef DEFINE_DELTA1D_DECODE_FLOAT

/* ----- Encode ------------------------------------------------------------- */

static tdc_status delta1d_encode(const tdc_block *in,
                                 const void      *params,
                                 tdc_buffer      *residual_out,
                                 tdc_dtype       *residual_dtype,
                                 tdc_buffer      *side_out) {
    (void)params;
    if (!in || !residual_out || !residual_out->realloc_fn) return TDC_E_INVAL;
    if (in->layout != TDC_LAYOUT_VECTOR_1D) return TDC_E_LAYOUT;
    if (in->shape.rank != 1)                return TDC_E_SHAPE;
    if (!delta1d_dtype_accepted(in->dtype)) return TDC_E_DTYPE;

    size_t elem_size = tdc_dtype_size(in->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n = in->shape.dim[0];
    if (n < 0) return TDC_E_SHAPE;

    size_t bytes = (size_t)n * elem_size;
    tdc_status st = tdc_buf_reserve(residual_out, bytes);
    if (st != TDC_OK) return st;

    if (residual_dtype) *residual_dtype = in->dtype;
    if (side_out)        side_out->size  = 0; /* no side metadata */

    if (n == 0) {
        residual_out->size = 0;
        return TDC_OK;
    }

    if (!in->data) return TDC_E_INVAL;

    const uint8_t *src = (const uint8_t *)in->data;
    uint8_t       *dst = residual_out->data;

    /* Float dtypes use ordered-integer mapping for lossless delta. */
    if (tdc_dtype_is_float(in->dtype)) {
        switch (in->dtype) {
            case TDC_DT_F16: delta1d_encode_f16(src, dst, n); break;
            case TDC_DT_F32: delta1d_encode_f32(src, dst, n); break;
            case TDC_DT_F64: delta1d_encode_f64(src, dst, n); break;
            default: return TDC_E_DTYPE;
        }
        residual_out->size = bytes;
        return TDC_OK;
    }

    /* Integer path: raw unsigned subtraction at element width. */
    /* residual[0] = data[0] (seed) */
    memcpy(dst, src, elem_size);

    switch (elem_size) {
        case 1: {
            uint8_t prev;
            memcpy(&prev, src, 1u);
            for (int64_t i = 1; i < n; ++i) {
                uint8_t cur;
                memcpy(&cur, src + (size_t)i, 1u);
                uint8_t d = (uint8_t)(cur - prev);
                memcpy(dst + (size_t)i, &d, 1u);
                prev = cur;
            }
            break;
        }
        case 2: {
#if defined(TDC_DELTA1D_HAVE_SSE2)
            forward_delta_2_sse2(src, dst, n);
#elif defined(TDC_DELTA1D_HAVE_NEON)
            forward_delta_2_neon(src, dst, n);
#else
            uint16_t prev;
            memcpy(&prev, src, 2u);
            for (int64_t i = 1; i < n; ++i) {
                uint16_t cur;
                memcpy(&cur, src + (size_t)i * 2u, 2u);
                uint16_t d = (uint16_t)(cur - prev);
                memcpy(dst + (size_t)i * 2u, &d, 2u);
                prev = cur;
            }
#endif
            break;
        }
        case 4: {
#if defined(TDC_DELTA1D_HAVE_SSE2)
            forward_delta_4_sse2(src, dst, n);
#elif defined(TDC_DELTA1D_HAVE_NEON)
            forward_delta_4_neon(src, dst, n);
#else
            uint32_t prev;
            memcpy(&prev, src, 4u);
            for (int64_t i = 1; i < n; ++i) {
                uint32_t cur;
                memcpy(&cur, src + (size_t)i * 4u, 4u);
                uint32_t d = cur - prev;
                memcpy(dst + (size_t)i * 4u, &d, 4u);
                prev = cur;
            }
#endif
            break;
        }
        case 8: {
#if defined(TDC_DELTA1D_HAVE_SSE2)
            forward_delta_8_sse2(src, dst, n);
#elif defined(TDC_DELTA1D_HAVE_NEON)
            forward_delta_8_neon(src, dst, n);
#else
            uint64_t prev;
            memcpy(&prev, src, 8u);
            for (int64_t i = 1; i < n; ++i) {
                uint64_t cur;
                memcpy(&cur, src + (size_t)i * 8u, 8u);
                uint64_t d = cur - prev;
                memcpy(dst + (size_t)i * 8u, &d, 8u);
                prev = cur;
            }
#endif
            break;
        }
        default:
            return TDC_E_DTYPE; /* unreachable: filtered by accepted_dtypes */
    }

    residual_out->size = bytes;
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status delta1d_decode(tdc_block      *out,
                                 const void     *params,
                                 tdc_dtype       residual_dtype,
                                 const uint8_t  *residuals, size_t residual_size,
                                 const uint8_t  *side_meta, size_t side_size) {
    (void)params;
    (void)side_meta;
    if (side_size != 0) return TDC_E_CORRUPT; /* delta1d carries no side meta */
    if (!out) return TDC_E_INVAL;
    if (out->layout != TDC_LAYOUT_VECTOR_1D) return TDC_E_LAYOUT;
    if (out->shape.rank != 1)                return TDC_E_SHAPE;
    if (residual_dtype != out->dtype)        return TDC_E_DTYPE;
    if (!delta1d_dtype_accepted(out->dtype)) return TDC_E_DTYPE;

    size_t elem_size = tdc_dtype_size(out->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n = out->shape.dim[0];
    if (n < 0) return TDC_E_SHAPE;

    size_t bytes = (size_t)n * elem_size;
    if (residual_size != bytes) return TDC_E_CORRUPT;

    if (n == 0) return TDC_OK;
    if (!out->data || !residuals) return TDC_E_INVAL;

    uint8_t *dst = (uint8_t *)out->data;

    /* Float dtypes: reverse the ordered-integer delta. */
    if (tdc_dtype_is_float(out->dtype)) {
        switch (out->dtype) {
            case TDC_DT_F16: delta1d_decode_f16(residuals, dst, n); break;
            case TDC_DT_F32: delta1d_decode_f32(residuals, dst, n); break;
            case TDC_DT_F64: delta1d_decode_f64(residuals, dst, n); break;
            default: return TDC_E_DTYPE;
        }
        return TDC_OK;
    }

    /* Integer path: prefix sum at element width. */
    /* data[0] = residual[0] (seed) */
    memcpy(dst, residuals, elem_size);

    switch (elem_size) {
        case 1: {
            uint8_t acc;
            memcpy(&acc, residuals, 1u);
            for (int64_t i = 1; i < n; ++i) {
                uint8_t d;
                memcpy(&d, residuals + (size_t)i, 1u);
                acc = (uint8_t)(acc + d);
                memcpy(dst + (size_t)i, &acc, 1u);
            }
            break;
        }
        case 2: {
#if defined(TDC_DELTA1D_HAVE_SSE2)
            inverse_delta_2_sse2(dst, residuals, n);
#elif defined(TDC_DELTA1D_HAVE_NEON)
            inverse_delta_2_neon(dst, residuals, n);
#else
            uint16_t acc;
            memcpy(&acc, residuals, 2u);
            for (int64_t i = 1; i < n; ++i) {
                uint16_t d;
                memcpy(&d, residuals + (size_t)i * 2u, 2u);
                acc = (uint16_t)(acc + d);
                memcpy(dst + (size_t)i * 2u, &acc, 2u);
            }
#endif
            break;
        }
        case 4: {
#if defined(TDC_DELTA1D_HAVE_SSE2)
            inverse_delta_4_sse2(dst, residuals, n);
#elif defined(TDC_DELTA1D_HAVE_NEON)
            inverse_delta_4_neon(dst, residuals, n);
#else
            uint32_t acc;
            memcpy(&acc, residuals, 4u);
            for (int64_t i = 1; i < n; ++i) {
                uint32_t d;
                memcpy(&d, residuals + (size_t)i * 4u, 4u);
                acc = acc + d;
                memcpy(dst + (size_t)i * 4u, &acc, 4u);
            }
#endif
            break;
        }
        case 8: {
#if defined(TDC_DELTA1D_HAVE_SSE2)
            inverse_delta_8_sse2(dst, residuals, n);
#elif defined(TDC_DELTA1D_HAVE_NEON)
            inverse_delta_8_neon(dst, residuals, n);
#else
            uint64_t acc;
            memcpy(&acc, residuals, 8u);
            for (int64_t i = 1; i < n; ++i) {
                uint64_t d;
                memcpy(&d, residuals + (size_t)i * 8u, 8u);
                acc = acc + d;
                memcpy(dst + (size_t)i * 8u, &acc, 8u);
            }
#endif
            break;
        }
        default:
            return TDC_E_DTYPE; /* unreachable */
    }

    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_model_vt tdc_model_delta1d_vt = {
    .id               = TDC_MODEL_DELTA_1D,
    .name             = "delta1d",
    .accepted_dtypes  = DELTA1D_ACCEPTED_DTYPES,
    .accepted_layouts = DELTA1D_ACCEPTED_LAYOUTS,
    .encode           = delta1d_encode,
    .decode           = delta1d_decode,
};
