/*
 * src/transform/shuffle.c
 *
 * TDC_XFORM_BYTE_SHUFFLE — transpose a fixed-width element stream by
 * byte lane.
 *
 *   row-major: [e0:b0 b1..bE-1] [e1:b0 b1..bE-1] ...
 *   shuffled : [e0:b0 e1:b0 ..] [e0:b1 e1:b1 ..] ... [e0:bE-1 ..]
 *
 * Same-significance bytes become adjacent, which dramatically improves
 * the ratio of any subsequent entropy coder on numeric data: the high
 * mantissa / exponent bytes of nearby float64s tend to repeat, the
 * sign-extended high bytes of small int64s are nearly all zero, and so on.
 *
 * Element size is derived from the input dtype (tdc_dtype_size). The
 * transform takes no params and accepts every fixed-width numeric dtype.
 * elem_size == 1 (i8/u8) is a no-op memcpy by definition. The on-disk
 * format is bit-for-bit identical to the input — no header, no length
 * field — because the inverse always knows src_size and elem_size and
 * therefore n_elems = src_size / elem_size.
 *
 * Source: extracted from vectra/src/vtr_codec.c lines 220-409
 *         (byte_shuffle, byte_unshuffle, byte_unshuffle_8_sse2,
 *         byte_unshuffle_8_neon). The SIMD inner loops are byte-identical
 *         to vectra; the outer wrapping is rewritten for tdc allocation
 *         and error conventions.
 *
 * Differences vs vectra:
 *   - All scratch goes through tdc_buffer::realloc_fn (encode side).
 *     The decode side writes directly into the caller-provided dst, so
 *     it needs no scratch at all. Vectra's vtr_byte_unshuffle did an
 *     in-place malloc-and-copy round-trip; that path doesn't exist here.
 *   - Vectra carried a separate VTR_HAS_SSE2 / VTR_HAS_NEON macro pair
 *     wired up in vtr_codec.h. tdc detects SSE2/NEON inline below.
 *   - Decode validates that src_size is a multiple of elem_size and
 *     returns TDC_E_CORRUPT on mismatch instead of silently truncating.
 *   - elem_size 1 short-circuits to memcpy in both directions.
 */

#include "tdc/transform.h"
#include "../core/buffer.h"
#include "transform_internal.h"

#include <string.h>
#include <stdint.h>
#include <stddef.h>

/* ----- SIMD detection ----------------------------------------------------- */
/*
 * MSVC on x64 always supports SSE2 but does not define __SSE2__. Detect via
 * the _M_X64 / _M_IX86_FP probe in addition to the GCC/Clang macro.
 */
#if defined(__SSE2__) || defined(_M_X64) || (defined(_M_IX86_FP) && _M_IX86_FP >= 2)
#  define TDC_SHUFFLE_HAVE_SSE2 1
#  include <emmintrin.h>
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#  define TDC_SHUFFLE_HAVE_NEON 1
#  include <arm_neon.h>
#endif

/* ----- Scalar shuffle / unshuffle ----------------------------------------- */

static void shuffle_scalar(uint8_t *dst, const uint8_t *src,
                           uint32_t n_elems, uint32_t elem_size) {
    for (uint32_t b = 0; b < elem_size; b++) {
        uint32_t lane = b * n_elems;
        for (uint32_t i = 0; i < n_elems; i++) {
            dst[lane + i] = src[i * elem_size + b];
        }
    }
}

static void unshuffle_scalar(uint8_t *dst, const uint8_t *src,
                             uint32_t n_elems, uint32_t elem_size) {
    for (uint32_t b = 0; b < elem_size; b++) {
        uint32_t lane = b * n_elems;
        for (uint32_t i = 0; i < n_elems; i++) {
            dst[i * elem_size + b] = src[lane + i];
        }
    }
}

/* ----- SSE2 fast path: 8-byte elements, 16 elems/iter -------------------- */
/*
 * Classic 3-stage SSE2 byte transpose: unpacklo/hi at epi8 -> epi16 -> epi32.
 * Reads 16 bytes from each of the 8 byte-lanes per iteration; writes 16
 * complete 8-byte elements (128 bytes).
 *
 * Inner loop is byte-identical to vectra's byte_unshuffle_8_sse2 — see
 * vectra/CLAUDE.md for the design notes. Do not "improve" without
 * benchmarks.
 */
/* ----- SSE2 fast path: 2-byte elements, 16 elems/iter ------------------- */
/*
 * Single-stage SSE2 byte interleave for elem_size == 2 (i16/u16/f16).
 * One unpacklo + one unpackhi produces 16 fully-interleaved 2-byte
 * elements per iteration. Much lighter than the 8-byte path because the
 * byte groups are already adjacent after the first unpack.
 *
 * Not present in vectra — vectra only shipped the 8-byte SIMD variant.
 * Added here because i16/u16 are the dominant residual types for
 * DELTA1D and PRED2D pipelines and the scalar path was the visible
 * bottleneck in the bench (see notes.md 2026-04-09).
 */
#ifdef TDC_SHUFFLE_HAVE_SSE2
static void unshuffle_2_sse2(uint8_t *dst, const uint8_t *src, uint32_t n) {
    uint32_t i = 0;
    for (; i + 16 <= n; i += 16) {
        __m128i r0 = _mm_loadu_si128((const __m128i *)(src + 0u*n + i));
        __m128i r1 = _mm_loadu_si128((const __m128i *)(src + 1u*n + i));

        __m128i b0 = _mm_unpacklo_epi8(r0, r1);   /* elems 0..7, fully paired */
        __m128i b1 = _mm_unpackhi_epi8(r0, r1);   /* elems 8..15 */

        uint8_t *out = dst + (size_t)i * 2u;
        _mm_storeu_si128((__m128i *)(out +  0), b0);
        _mm_storeu_si128((__m128i *)(out + 16), b1);
    }
    for (; i < n; i++) {
        dst[i * 2u + 0] = src[0u * n + i];
        dst[i * 2u + 1] = src[1u * n + i];
    }
}

/* ----- SSE2 fast path: 4-byte elements, 16 elems/iter ------------------- */
/*
 * Two-stage SSE2 byte transpose for elem_size == 4 (i32/u32/f32).
 * First stage unpacks at epi8 to produce (b0,b1) and (b2,b3) pairs per
 * element; second stage unpacks at epi16 to glue them into the full
 * 4-byte groups. 16 input elements per iteration, 64 bytes of output.
 *
 * Not present in vectra. Added here because i32/u32/f32 is the bulk of
 * real raster / time-series workloads and the scalar path was capping
 * the PLANE2D decode bench at ~1.5 GB/s xform stage.
 */
static void unshuffle_4_sse2(uint8_t *dst, const uint8_t *src, uint32_t n) {
    uint32_t i = 0;
    for (; i + 16 <= n; i += 16) {
        __m128i r0 = _mm_loadu_si128((const __m128i *)(src + 0u*n + i));
        __m128i r1 = _mm_loadu_si128((const __m128i *)(src + 1u*n + i));
        __m128i r2 = _mm_loadu_si128((const __m128i *)(src + 2u*n + i));
        __m128i r3 = _mm_loadu_si128((const __m128i *)(src + 3u*n + i));

        __m128i a0 = _mm_unpacklo_epi8(r0, r1);   /* e0..e7  : (b0,b1) pairs */
        __m128i a1 = _mm_unpackhi_epi8(r0, r1);   /* e8..e15 : (b0,b1) pairs */
        __m128i a2 = _mm_unpacklo_epi8(r2, r3);   /* e0..e7  : (b2,b3) pairs */
        __m128i a3 = _mm_unpackhi_epi8(r2, r3);   /* e8..e15 : (b2,b3) pairs */

        __m128i b0 = _mm_unpacklo_epi16(a0, a2);  /* e0..e3  full */
        __m128i b1 = _mm_unpackhi_epi16(a0, a2);  /* e4..e7  full */
        __m128i b2 = _mm_unpacklo_epi16(a1, a3);  /* e8..e11 full */
        __m128i b3 = _mm_unpackhi_epi16(a1, a3);  /* e12..e15 full */

        uint8_t *out = dst + (size_t)i * 4u;
        _mm_storeu_si128((__m128i *)(out +  0), b0);
        _mm_storeu_si128((__m128i *)(out + 16), b1);
        _mm_storeu_si128((__m128i *)(out + 32), b2);
        _mm_storeu_si128((__m128i *)(out + 48), b3);
    }
    for (; i < n; i++) {
        for (uint32_t b = 0; b < 4u; b++) {
            dst[i * 4u + b] = src[b * n + i];
        }
    }
}

static void unshuffle_8_sse2(uint8_t *dst, const uint8_t *src, uint32_t n) {
    uint32_t i = 0;
    for (; i + 16 <= n; i += 16) {
        __m128i r0 = _mm_loadu_si128((const __m128i *)(src + 0u*n + i));
        __m128i r1 = _mm_loadu_si128((const __m128i *)(src + 1u*n + i));
        __m128i r2 = _mm_loadu_si128((const __m128i *)(src + 2u*n + i));
        __m128i r3 = _mm_loadu_si128((const __m128i *)(src + 3u*n + i));
        __m128i r4 = _mm_loadu_si128((const __m128i *)(src + 4u*n + i));
        __m128i r5 = _mm_loadu_si128((const __m128i *)(src + 5u*n + i));
        __m128i r6 = _mm_loadu_si128((const __m128i *)(src + 6u*n + i));
        __m128i r7 = _mm_loadu_si128((const __m128i *)(src + 7u*n + i));

        __m128i a0 = _mm_unpacklo_epi8(r0, r1);
        __m128i a1 = _mm_unpackhi_epi8(r0, r1);
        __m128i a2 = _mm_unpacklo_epi8(r2, r3);
        __m128i a3 = _mm_unpackhi_epi8(r2, r3);
        __m128i a4 = _mm_unpacklo_epi8(r4, r5);
        __m128i a5 = _mm_unpackhi_epi8(r4, r5);
        __m128i a6 = _mm_unpacklo_epi8(r6, r7);
        __m128i a7 = _mm_unpackhi_epi8(r6, r7);

        __m128i b0 = _mm_unpacklo_epi16(a0, a2);
        __m128i b1 = _mm_unpackhi_epi16(a0, a2);
        __m128i b2 = _mm_unpacklo_epi16(a1, a3);
        __m128i b3 = _mm_unpackhi_epi16(a1, a3);
        __m128i b4 = _mm_unpacklo_epi16(a4, a6);
        __m128i b5 = _mm_unpackhi_epi16(a4, a6);
        __m128i b6 = _mm_unpacklo_epi16(a5, a7);
        __m128i b7 = _mm_unpackhi_epi16(a5, a7);

        __m128i c0 = _mm_unpacklo_epi32(b0, b4);
        __m128i c1 = _mm_unpackhi_epi32(b0, b4);
        __m128i c2 = _mm_unpacklo_epi32(b1, b5);
        __m128i c3 = _mm_unpackhi_epi32(b1, b5);
        __m128i c4 = _mm_unpacklo_epi32(b2, b6);
        __m128i c5 = _mm_unpackhi_epi32(b2, b6);
        __m128i c6 = _mm_unpacklo_epi32(b3, b7);
        __m128i c7 = _mm_unpackhi_epi32(b3, b7);

        uint8_t *out = dst + (size_t)i * 8u;
        _mm_storeu_si128((__m128i *)(out +   0), c0);
        _mm_storeu_si128((__m128i *)(out +  16), c1);
        _mm_storeu_si128((__m128i *)(out +  32), c2);
        _mm_storeu_si128((__m128i *)(out +  48), c3);
        _mm_storeu_si128((__m128i *)(out +  64), c4);
        _mm_storeu_si128((__m128i *)(out +  80), c5);
        _mm_storeu_si128((__m128i *)(out +  96), c6);
        _mm_storeu_si128((__m128i *)(out + 112), c7);
    }
    /* Scalar tail */
    for (; i < n; i++) {
        for (uint32_t b = 0; b < 8u; b++) {
            dst[i * 8u + b] = src[b * n + i];
        }
    }
}
#endif /* TDC_SHUFFLE_HAVE_SSE2 */

/* ----- NEON fast path: 8-byte elements, 16 elems/iter -------------------- */
/*
 * AArch64 vzip cascade. Inner loop is byte-identical to vectra's
 * byte_unshuffle_8_neon.
 */
#ifdef TDC_SHUFFLE_HAVE_NEON
static void unshuffle_2_neon(uint8_t *dst, const uint8_t *src, uint32_t n) {
    uint32_t i = 0;
    for (; i + 16 <= n; i += 16) {
        uint8x16_t r0 = vld1q_u8(src + 0u*n + i);
        uint8x16_t r1 = vld1q_u8(src + 1u*n + i);

        uint8x16_t b0 = vzip1q_u8(r0, r1);
        uint8x16_t b1 = vzip2q_u8(r0, r1);

        uint8_t *out = dst + (size_t)i * 2u;
        vst1q_u8(out +  0, b0);
        vst1q_u8(out + 16, b1);
    }
    for (; i < n; i++) {
        dst[i * 2u + 0] = src[0u * n + i];
        dst[i * 2u + 1] = src[1u * n + i];
    }
}

static void unshuffle_4_neon(uint8_t *dst, const uint8_t *src, uint32_t n) {
    uint32_t i = 0;
    for (; i + 16 <= n; i += 16) {
        uint8x16_t r0 = vld1q_u8(src + 0u*n + i);
        uint8x16_t r1 = vld1q_u8(src + 1u*n + i);
        uint8x16_t r2 = vld1q_u8(src + 2u*n + i);
        uint8x16_t r3 = vld1q_u8(src + 3u*n + i);

        uint8x16_t a0 = vzip1q_u8(r0, r1);
        uint8x16_t a1 = vzip2q_u8(r0, r1);
        uint8x16_t a2 = vzip1q_u8(r2, r3);
        uint8x16_t a3 = vzip2q_u8(r2, r3);

        uint16x8_t b0 = vzip1q_u16(vreinterpretq_u16_u8(a0), vreinterpretq_u16_u8(a2));
        uint16x8_t b1 = vzip2q_u16(vreinterpretq_u16_u8(a0), vreinterpretq_u16_u8(a2));
        uint16x8_t b2 = vzip1q_u16(vreinterpretq_u16_u8(a1), vreinterpretq_u16_u8(a3));
        uint16x8_t b3 = vzip2q_u16(vreinterpretq_u16_u8(a1), vreinterpretq_u16_u8(a3));

        uint8_t *out = dst + (size_t)i * 4u;
        vst1q_u8(out +  0, vreinterpretq_u8_u16(b0));
        vst1q_u8(out + 16, vreinterpretq_u8_u16(b1));
        vst1q_u8(out + 32, vreinterpretq_u8_u16(b2));
        vst1q_u8(out + 48, vreinterpretq_u8_u16(b3));
    }
    for (; i < n; i++) {
        for (uint32_t b = 0; b < 4u; b++) {
            dst[i * 4u + b] = src[b * n + i];
        }
    }
}

static void unshuffle_8_neon(uint8_t *dst, const uint8_t *src, uint32_t n) {
    uint32_t i = 0;
    for (; i + 16 <= n; i += 16) {
        uint8x16_t r0 = vld1q_u8(src + 0u*n + i);
        uint8x16_t r1 = vld1q_u8(src + 1u*n + i);
        uint8x16_t r2 = vld1q_u8(src + 2u*n + i);
        uint8x16_t r3 = vld1q_u8(src + 3u*n + i);
        uint8x16_t r4 = vld1q_u8(src + 4u*n + i);
        uint8x16_t r5 = vld1q_u8(src + 5u*n + i);
        uint8x16_t r6 = vld1q_u8(src + 6u*n + i);
        uint8x16_t r7 = vld1q_u8(src + 7u*n + i);

        uint8x16_t a0 = vzip1q_u8(r0, r1);
        uint8x16_t a1 = vzip2q_u8(r0, r1);
        uint8x16_t a2 = vzip1q_u8(r2, r3);
        uint8x16_t a3 = vzip2q_u8(r2, r3);
        uint8x16_t a4 = vzip1q_u8(r4, r5);
        uint8x16_t a5 = vzip2q_u8(r4, r5);
        uint8x16_t a6 = vzip1q_u8(r6, r7);
        uint8x16_t a7 = vzip2q_u8(r6, r7);

        uint16x8_t b0 = vzip1q_u16(vreinterpretq_u16_u8(a0), vreinterpretq_u16_u8(a2));
        uint16x8_t b1 = vzip2q_u16(vreinterpretq_u16_u8(a0), vreinterpretq_u16_u8(a2));
        uint16x8_t b2 = vzip1q_u16(vreinterpretq_u16_u8(a1), vreinterpretq_u16_u8(a3));
        uint16x8_t b3 = vzip2q_u16(vreinterpretq_u16_u8(a1), vreinterpretq_u16_u8(a3));
        uint16x8_t b4 = vzip1q_u16(vreinterpretq_u16_u8(a4), vreinterpretq_u16_u8(a6));
        uint16x8_t b5 = vzip2q_u16(vreinterpretq_u16_u8(a4), vreinterpretq_u16_u8(a6));
        uint16x8_t b6 = vzip1q_u16(vreinterpretq_u16_u8(a5), vreinterpretq_u16_u8(a7));
        uint16x8_t b7 = vzip2q_u16(vreinterpretq_u16_u8(a5), vreinterpretq_u16_u8(a7));

        uint32x4_t c0 = vzip1q_u32(vreinterpretq_u32_u16(b0), vreinterpretq_u32_u16(b4));
        uint32x4_t c1 = vzip2q_u32(vreinterpretq_u32_u16(b0), vreinterpretq_u32_u16(b4));
        uint32x4_t c2 = vzip1q_u32(vreinterpretq_u32_u16(b1), vreinterpretq_u32_u16(b5));
        uint32x4_t c3 = vzip2q_u32(vreinterpretq_u32_u16(b1), vreinterpretq_u32_u16(b5));
        uint32x4_t c4 = vzip1q_u32(vreinterpretq_u32_u16(b2), vreinterpretq_u32_u16(b6));
        uint32x4_t c5 = vzip2q_u32(vreinterpretq_u32_u16(b2), vreinterpretq_u32_u16(b6));
        uint32x4_t c6 = vzip1q_u32(vreinterpretq_u32_u16(b3), vreinterpretq_u32_u16(b7));
        uint32x4_t c7 = vzip2q_u32(vreinterpretq_u32_u16(b3), vreinterpretq_u32_u16(b7));

        uint8_t *out = dst + (size_t)i * 8u;
        vst1q_u8(out +   0, vreinterpretq_u8_u32(c0));
        vst1q_u8(out +  16, vreinterpretq_u8_u32(c1));
        vst1q_u8(out +  32, vreinterpretq_u8_u32(c2));
        vst1q_u8(out +  48, vreinterpretq_u8_u32(c3));
        vst1q_u8(out +  64, vreinterpretq_u8_u32(c4));
        vst1q_u8(out +  80, vreinterpretq_u8_u32(c5));
        vst1q_u8(out +  96, vreinterpretq_u8_u32(c6));
        vst1q_u8(out + 112, vreinterpretq_u8_u32(c7));
    }
    for (; i < n; i++) {
        for (uint32_t b = 0; b < 8u; b++) {
            dst[i * 8u + b] = src[b * n + i];
        }
    }
}
#endif /* TDC_SHUFFLE_HAVE_NEON */

/* ----- Dispatcher --------------------------------------------------------- */

static void unshuffle_dispatch(uint8_t *dst, const uint8_t *src,
                               uint32_t n_elems, uint32_t elem_size) {
#ifdef TDC_SHUFFLE_HAVE_SSE2
    if (elem_size == 8u) { unshuffle_8_sse2(dst, src, n_elems); return; }
    if (elem_size == 4u) { unshuffle_4_sse2(dst, src, n_elems); return; }
    if (elem_size == 2u) { unshuffle_2_sse2(dst, src, n_elems); return; }
#elif defined(TDC_SHUFFLE_HAVE_NEON)
    if (elem_size == 8u) { unshuffle_8_neon(dst, src, n_elems); return; }
    if (elem_size == 4u) { unshuffle_4_neon(dst, src, n_elems); return; }
    if (elem_size == 2u) { unshuffle_2_neon(dst, src, n_elems); return; }
#endif
    unshuffle_scalar(dst, src, n_elems, elem_size);
}

/* Output-buffer growth uses the shared tdc_buf_reserve helper from
 * src/core/buffer.h. The previous local shuffle_buf_reserve copy was
 * lifted along with the lz and quantize copies into a single source
 * of truth. */

/* ----- Vtable hooks ------------------------------------------------------- */

/*
 * Bitmask of accepted input dtypes. Convention: bit (1u << dtype_id) is set
 * iff that dtype is accepted. The shuffle accepts every fixed-width numeric
 * dtype (1..10, 12) and rejects TDC_DT_STRING (11).
 */
#define SHUFFLE_DTYPE_BIT(dt) (1u << (uint32_t)(dt))
#define SHUFFLE_ACCEPTED_DTYPES (                       \
    SHUFFLE_DTYPE_BIT(TDC_DT_I8)  |                     \
    SHUFFLE_DTYPE_BIT(TDC_DT_I16) |                     \
    SHUFFLE_DTYPE_BIT(TDC_DT_I32) |                     \
    SHUFFLE_DTYPE_BIT(TDC_DT_I64) |                     \
    SHUFFLE_DTYPE_BIT(TDC_DT_U8)  |                     \
    SHUFFLE_DTYPE_BIT(TDC_DT_U16) |                     \
    SHUFFLE_DTYPE_BIT(TDC_DT_U32) |                     \
    SHUFFLE_DTYPE_BIT(TDC_DT_U64) |                     \
    SHUFFLE_DTYPE_BIT(TDC_DT_F16) |                     \
    SHUFFLE_DTYPE_BIT(TDC_DT_F32) |                     \
    SHUFFLE_DTYPE_BIT(TDC_DT_F64))

static tdc_status shuffle_encode(const uint8_t *src, size_t src_size,
                                 tdc_dtype      in_dtype,
                                 const void    *params,
                                 tdc_buffer    *dst,
                                 tdc_dtype     *out_dtype) {
    (void)params;
    if (!dst || !dst->realloc_fn) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;

    size_t elem_size = tdc_dtype_size(in_dtype);
    if (elem_size == 0) return TDC_E_DTYPE;
    if (src_size % elem_size != 0) return TDC_E_INVAL;

    tdc_status st = tdc_buf_reserve(dst, src_size);
    if (st != TDC_OK) return st;

    if (out_dtype) *out_dtype = in_dtype;

    if (src_size == 0) {
        dst->size = 0;
        return TDC_OK;
    }

    if (elem_size == 1) {
        memcpy(dst->data, src, src_size);
        dst->size = src_size;
        return TDC_OK;
    }

    size_t n_elems_sz = src_size / elem_size;
    if (n_elems_sz > UINT32_MAX) return TDC_E_INVAL;

    shuffle_scalar(dst->data, src,
                   (uint32_t)n_elems_sz, (uint32_t)elem_size);
    dst->size = src_size;
    return TDC_OK;
}

static tdc_status shuffle_decode(const uint8_t *src, size_t src_size,
                                 tdc_dtype      in_dtype,
                                 const void    *params,
                                 uint8_t       *dst, size_t dst_size,
                                 tdc_dtype     *out_dtype) {
    (void)params;
    if (src_size > 0 && !src) return TDC_E_INVAL;
    if (dst_size > 0 && !dst) return TDC_E_INVAL;
    if (src_size != dst_size) return TDC_E_CORRUPT;

    size_t elem_size = tdc_dtype_size(in_dtype);
    if (elem_size == 0) return TDC_E_DTYPE;
    if (src_size % elem_size != 0) return TDC_E_CORRUPT;

    if (out_dtype) *out_dtype = in_dtype;

    if (src_size == 0) return TDC_OK;

    if (elem_size == 1) {
        memcpy(dst, src, src_size);
        return TDC_OK;
    }

    size_t n_elems_sz = src_size / elem_size;
    if (n_elems_sz > UINT32_MAX) return TDC_E_INVAL;

    unshuffle_dispatch(dst, src,
                       (uint32_t)n_elems_sz, (uint32_t)elem_size);
    return TDC_OK;
}

const tdc_xform_vt tdc_xform_byte_shuffle_vt = {
    .id              = TDC_XFORM_BYTE_SHUFFLE,
    .name            = "byte_shuffle",
    .accepted_dtypes = SHUFFLE_ACCEPTED_DTYPES,
    .can_inplace     = 0,
    .is_lossy        = 0,
    .encode          = shuffle_encode,
    .decode          = shuffle_decode,
};
