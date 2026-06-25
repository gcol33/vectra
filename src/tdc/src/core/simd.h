/*
 * src/core/simd.h
 *
 * SIMD detection, copy primitives, and prefetch hints.
 * All functions have scalar fallbacks so callers never need #ifdef guards.
 */

#ifndef TDC_SIMD_H
#define TDC_SIMD_H

#include <stdint.h>
#include <string.h>

/* ---- ISA detection ---------------------------------------------------- */

#if defined(__SSE2__) || defined(_M_X64) || \
    (defined(_M_IX86_FP) && _M_IX86_FP >= 2)
#  include <emmintrin.h>
#  define TDC_HAVE_SSE2 1
#else
#  define TDC_HAVE_SSE2 0
#endif

#if defined(__AVX2__) || (defined(_MSC_VER) && defined(__AVX2__))
#  include <immintrin.h>
#  define TDC_HAVE_AVX2 1
#else
#  define TDC_HAVE_AVX2 0
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#  include <arm_neon.h>
#  define TDC_HAVE_NEON 1
#else
#  define TDC_HAVE_NEON 0
#endif

/* ---- Prefetch --------------------------------------------------------- */

#if defined(__GNUC__) || defined(__clang__)
#  define TDC_PREFETCH_L1(p) __builtin_prefetch((p), 0, 3)
#elif defined(_MSC_VER)
#  include <intrin.h>
#  define TDC_PREFETCH_L1(p) _mm_prefetch((const char*)(p), _MM_HINT_T0)
#else
#  define TDC_PREFETCH_L1(p) ((void)(p))
#endif

/* ---- Copy primitives -------------------------------------------------- */

/* 16-byte unaligned copy.  SSE2 is guaranteed on x86_64. */
static inline void tdc_copy16(void *dst, const void *src) {
#if TDC_HAVE_SSE2
    _mm_storeu_si128((__m128i *)dst,
                     _mm_loadu_si128((const __m128i *)src));
#elif TDC_HAVE_NEON
    vst1q_u8((uint8_t *)dst, vld1q_u8((const uint8_t *)src));
#else
    memcpy(dst, src, 16);
#endif
}

/* 8-byte unaligned copy (for overlap bootstrap remainder). */
static inline void tdc_copy8(void *dst, const void *src) {
    memcpy(dst, src, 8);
}

/* 4-byte unaligned copy. */
static inline void tdc_copy4(void *dst, const void *src) {
    memcpy(dst, src, 4);
}

/* 16-byte wildcopy: copies len bytes in 16-byte steps.
 * May overwrite up to 15 bytes past len.  Caller must ensure room. */
static inline void tdc_wildcopy16(uint8_t *dst, const uint8_t *src,
                                   uint32_t len) {
    const uint8_t *end = dst + len;
    do {
        tdc_copy16(dst, src);
        dst += 16;
        src += 16;
    } while (dst < end);
}

/* 32-byte unaligned copy.  AVX2 only; no scalar fallback — callers
 * must gate on TDC_HAVE_AVX2 before calling. */
#if TDC_HAVE_AVX2
static inline void tdc_copy32(void *dst, const void *src) {
    _mm256_storeu_si256((__m256i *)dst,
                        _mm256_loadu_si256((const __m256i *)src));
}

/* 32-byte wildcopy: copies len bytes in 32-byte steps.
 * May overwrite up to 31 bytes past len.  Caller must ensure room. */
static inline void tdc_wildcopy32(uint8_t *dst, const uint8_t *src,
                                   uint32_t len) {
    const uint8_t *end = dst + len;
    do {
        tdc_copy32(dst, src);
        dst += 32;
        src += 32;
    } while (dst < end);
}
#endif

/* Full match copy dispatcher.  Handles all offsets and lengths.
 * op points to the current output position; off is the back-reference
 * distance (op - match); mlen is the number of bytes to copy.
 *
 * Caller must reserve TDC_WILDCOPY_SLACK bytes past op + mlen: the
 * AVX2-enabled off >= 32 path dispatches to wildcopy32 which may
 * overshoot by up to 31 bytes; the off >= 16 path overshoots by up
 * to 15. Callers that conservatively use the maximum (31) are safe
 * regardless of which internal path fires. */
#if TDC_HAVE_AVX2
#  define TDC_WILDCOPY_SLACK 31
#else
#  define TDC_WILDCOPY_SLACK 15
#endif

static inline void tdc_match_copy(uint8_t *op, uint32_t off, uint32_t mlen) {
    const uint8_t *match = op - off;

#if TDC_HAVE_AVX2
    if (off >= 32) {
        tdc_wildcopy32(op, match, mlen);
        return;
    }
#endif
    if (off >= 16) {
        tdc_wildcopy16(op, match, mlen);
    } else if (off >= 8) {
        uint8_t *oend = op + mlen;
        do {
            tdc_copy8(op, match);
            op += 8;
            match += 8;
        } while (op < oend);
    } else {
        /* Small offset (1-7): seed off bytes, double until we have 16
         * bytes of valid pattern, then use 16-byte copies for the rest.
         * O(log(off)) doublings reach 16 from any starting offset 1-7. */
        for (uint32_t k = 0; k < off; k++)
            op[k] = match[k];
        uint8_t *fill = op + off;
        uint8_t *fend = op + mlen;
        uint32_t filled = off;
        while (filled < 16 && fill < fend) {
            uint32_t chunk = filled;
            if (fill + chunk > fend) chunk = (uint32_t)(fend - fill);
            memcpy(fill, op, chunk);
            fill += chunk;
            filled += chunk;
        }
        while (fill < fend) {
            tdc_copy16(fill, fill - filled);
            fill += 16;
        }
    }
}

#endif /* TDC_SIMD_H */
