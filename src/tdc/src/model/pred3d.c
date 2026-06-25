/*
 * src/model/pred3d.c
 *
 * TDC_MODEL_PRED_3D — true 3D neighborhood predictor over a VOLUME_3D
 * block. Predictor kinds (tdc_pred3d_kind):
 *
 *   LEFT    — pred = val[z  ][y  ][x-1]
 *   UP      — pred = val[z  ][y-1][x  ]
 *   FRONT   — pred = val[z-1][y  ][x  ]
 *   AVG3    — mean of in-bounds face neighbors among {a, b, c}. With the
 *             Tier 3 octant decomposition below, "count of in-bounds
 *             neighbors" is a per-octant compile-time constant: cnt=3
 *             on the inner box (folded to multiply-by-magic), cnt=2 on
 *             the three face slabs (a single shift), cnt=1 on the three
 *             edge runs, cnt=0 in the corner.
 *   GRAD3D  — trilinear linear predictor:
 *               pred = a + b + c - ab - ac - bc + abc
 *             which collapses naturally per octant (e.g. on the z=0
 *             face slab the formula is exactly a + b - ab, the 2D plane
 *             predictor; on the (z=0, y=0) row it is a, etc.).
 *   PAETH3D — On the inner box, of {a, b, c} pick the one closest to
 *             the GRAD3D linear predictor p = a + b + c - ab - ac - bc
 *             + abc. On each face slab the rule reduces *exactly* to
 *             standard 2D Paeth on the in-bounds triple — e.g. the z=0
 *             face uses paeth(a, b, ab, a + b - ab), which is identical
 *             to PNG Paeth in 2D. On edge runs the rule reduces to the
 *             single in-bounds neighbor.  Tie-break order matches
 *             paeth32/paeth64: pa<=pb && pa<=pc → a, else pb<=pc → b,
 *             else c.
 *   AUTO    — encoder scores all six kinds on a sample prefix and picks
 *             the smallest sum of |residual|. Never written to disk.
 *
 * NEW in tdc v0 — no vectra source.
 *
 * Accepted dtypes: i8/i16/i32/u8/u16/u32 + f16/f32/f64.
 * Working type: int32_t for 8/16-bit dtypes, int64_t for 32-bit. Wide
 * enough that the GRAD3D sum (up to 7 neighbors of magnitude 2^31)
 * stays inside int64.
 *
 * Float support: ordered-integer mapping (float_order.h) converts float
 * bits to unsigned integers preserving numerical order. Predictions are
 * computed in uint64 (int64 for GRAD3D/PAETH3D sums), residuals are
 * unsigned wrapping differences. Round-trip is exact because everything
 * is integer arithmetic — no float subtraction.
 *
 * Layout: VOLUME_3D, rank 3, row-major contiguous storage:
 *   nz = shape.dim[0]   (slice count, outermost)
 *   ny = shape.dim[1]
 *   nx = shape.dim[2]   (innermost)
 *   slab = nx * ny
 *   idx  = z*slab + y*nx + x
 *
 * Side metadata: 1 byte = the resolved predictor kind. Same convention
 * as pred2d.
 *
 * Validity bitmap: ignored, like every other v0 model.
 *
 * Optimization status: Tier 1 (per-dtype typed kernels) + Tier 3 (per-
 * octant prologues for all kinds) are in place.  Each of the eight
 * (z,y,x) octants is its own loop with no boundary branches inside; the
 * encode side of the inner box is fully vectorizable for GRAD3D and
 * AVG3 (no inter-pixel dependency on the read-only src buffer). Decode
 * of LEFT/UP/FRONT vectorizes for two of the three (UP and FRONT have
 * no left-of dependency); decode of AVG3/GRAD3D/PAETH3D inner box stays
 * scalar because the `a = dst[x-1]` dependency is intrinsic.
 *
 * Side benefit of the per-octant decomposition: AVG3 picks up a
 * compile-time-constant divisor in every octant, so the encode hot loop
 * runs ~4x faster than the previous integrated branchy /count loop
 * (measured on bench/bench_pred3d_avg3.c — see notes.md).
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

/* ----- SIMD: row / slab / inner-box kernels ------------------------------ *
 *
 * Every helper is modular at element width `esz` (1/2/4/8 bytes). The
 * scalar path computes in a signed wider working type and truncates on
 * store — the residual written to memory is the same bit pattern as the
 * element-width modular computation, so SIMDing at native element width
 * is bit-equivalent without ever needing sign extension.
 *
 * Three kernels cover every vectorizable site in pred3d_{enc,dec}:
 *
 *   sub_row     r[i] = a[i] - b[i]        — LEFT/UP/FRONT encode
 *   add_row     d[i] = r[i] + b[i]        — UP/FRONT decode
 *   grad3d_enc  r[x] = val - pred         — GRAD3D inner-box (O8) encode
 *
 * AVG3 inner-box stays scalar: its `(a+b+c)/3` step needs wider lanes
 * and magic-multiply for SIMD, and AUTO picks GRAD3D on smooth data
 * where the hot bench rows live. PAETH3D inner-box stays scalar: the
 * branchy tie-break chain is not worth vectorizing relative to the
 * Paeth-lite gain already built into pred2d. Decode LEFT/AVG3/GRAD3D/
 * PAETH3D inner-box stays scalar because `a = dst[x-1]` is an
 * intrinsic read-after-write dependency.
 */
#if defined(__SSE2__) || defined(_M_X64) || \
    (defined(_M_IX86_FP) && _M_IX86_FP >= 2)
#  include <emmintrin.h>
#  define TDC_PRED3D_HAVE_SSE2 1
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#  include <arm_neon.h>
#  define TDC_PRED3D_HAVE_NEON 1
#endif

#ifdef TDC_PRED3D_HAVE_SSE2

static void pred3d_sub_row_sse2(uint8_t *dst, const uint8_t *a,
                                const uint8_t *b,
                                size_t nbytes, size_t esz) {
    size_t i = 0;
    if (esz == 1) {
        for (; i + 16 <= nbytes; i += 16) {
            __m128i va = _mm_loadu_si128((const __m128i *)(a + i));
            __m128i vb = _mm_loadu_si128((const __m128i *)(b + i));
            _mm_storeu_si128((__m128i *)(dst + i), _mm_sub_epi8(va, vb));
        }
        for (; i < nbytes; ++i) dst[i] = (uint8_t)(a[i] - b[i]);
    } else if (esz == 2) {
        for (; i + 16 <= nbytes; i += 16) {
            __m128i va = _mm_loadu_si128((const __m128i *)(a + i));
            __m128i vb = _mm_loadu_si128((const __m128i *)(b + i));
            _mm_storeu_si128((__m128i *)(dst + i), _mm_sub_epi16(va, vb));
        }
        for (; i + 2 <= nbytes; i += 2) {
            uint16_t x, y;
            memcpy(&x, a + i, 2); memcpy(&y, b + i, 2);
            uint16_t r = (uint16_t)(x - y);
            memcpy(dst + i, &r, 2);
        }
    } else if (esz == 4) {
        for (; i + 16 <= nbytes; i += 16) {
            __m128i va = _mm_loadu_si128((const __m128i *)(a + i));
            __m128i vb = _mm_loadu_si128((const __m128i *)(b + i));
            _mm_storeu_si128((__m128i *)(dst + i), _mm_sub_epi32(va, vb));
        }
        for (; i + 4 <= nbytes; i += 4) {
            uint32_t x, y;
            memcpy(&x, a + i, 4); memcpy(&y, b + i, 4);
            uint32_t r = x - y;
            memcpy(dst + i, &r, 4);
        }
    } else { /* esz == 8 */
        for (; i + 16 <= nbytes; i += 16) {
            __m128i va = _mm_loadu_si128((const __m128i *)(a + i));
            __m128i vb = _mm_loadu_si128((const __m128i *)(b + i));
            _mm_storeu_si128((__m128i *)(dst + i), _mm_sub_epi64(va, vb));
        }
        for (; i + 8 <= nbytes; i += 8) {
            uint64_t x, y;
            memcpy(&x, a + i, 8); memcpy(&y, b + i, 8);
            uint64_t r = x - y;
            memcpy(dst + i, &r, 8);
        }
    }
}

static void pred3d_add_row_sse2(uint8_t *dst, const uint8_t *a,
                                const uint8_t *b,
                                size_t nbytes, size_t esz) {
    size_t i = 0;
    if (esz == 1) {
        for (; i + 16 <= nbytes; i += 16) {
            __m128i va = _mm_loadu_si128((const __m128i *)(a + i));
            __m128i vb = _mm_loadu_si128((const __m128i *)(b + i));
            _mm_storeu_si128((__m128i *)(dst + i), _mm_add_epi8(va, vb));
        }
        for (; i < nbytes; ++i) dst[i] = (uint8_t)(a[i] + b[i]);
    } else if (esz == 2) {
        for (; i + 16 <= nbytes; i += 16) {
            __m128i va = _mm_loadu_si128((const __m128i *)(a + i));
            __m128i vb = _mm_loadu_si128((const __m128i *)(b + i));
            _mm_storeu_si128((__m128i *)(dst + i), _mm_add_epi16(va, vb));
        }
        for (; i + 2 <= nbytes; i += 2) {
            uint16_t x, y;
            memcpy(&x, a + i, 2); memcpy(&y, b + i, 2);
            uint16_t r = (uint16_t)(x + y);
            memcpy(dst + i, &r, 2);
        }
    } else if (esz == 4) {
        for (; i + 16 <= nbytes; i += 16) {
            __m128i va = _mm_loadu_si128((const __m128i *)(a + i));
            __m128i vb = _mm_loadu_si128((const __m128i *)(b + i));
            _mm_storeu_si128((__m128i *)(dst + i), _mm_add_epi32(va, vb));
        }
        for (; i + 4 <= nbytes; i += 4) {
            uint32_t x, y;
            memcpy(&x, a + i, 4); memcpy(&y, b + i, 4);
            uint32_t r = x + y;
            memcpy(dst + i, &r, 4);
        }
    } else { /* esz == 8 */
        for (; i + 16 <= nbytes; i += 16) {
            __m128i va = _mm_loadu_si128((const __m128i *)(a + i));
            __m128i vb = _mm_loadu_si128((const __m128i *)(b + i));
            _mm_storeu_si128((__m128i *)(dst + i), _mm_add_epi64(va, vb));
        }
        for (; i + 8 <= nbytes; i += 8) {
            uint64_t x, y;
            memcpy(&x, a + i, 8); memcpy(&y, b + i, 8);
            uint64_t r = x + y;
            memcpy(dst + i, &r, 8);
        }
    }
}

/* GRAD3D inner-box (O8) encode, one row.
 *
 * Pointers are element-0 of the row for each neighbor plane:
 *   s     = row at (z,   y  )   (val)
 *   sy    = row at (z,   y-1)
 *   sz    = row at (z-1, y  )
 *   syz   = row at (z-1, y-1)
 * The helper walks x=1..nx-1, reading s[x-1] etc. as shifted loads.
 *
 * Identity (modular at element width):
 *   res = val - (a + b + c - ab - ac - bc + abc)
 *       = val - a - b - c + ab + ac + bc - abc
 * with
 *   val = s[x], a = s[x-1],
 *   b   = sy[x], ab = sy[x-1],
 *   c   = sz[x], ac = sz[x-1],
 *   bc  = syz[x], abc = syz[x-1].
 */
static void pred3d_grad3d_enc_row_sse2(
    uint8_t       *dst,
    const uint8_t *s,   const uint8_t *sy,
    const uint8_t *sz,  const uint8_t *syz,
    int64_t nx, size_t esz) {
    if (nx < 2) return;
    const size_t nbytes = (size_t)(nx - 1) * esz;
    uint8_t       *rp   = dst + esz;        /* writes start at x=1     */
    const uint8_t *p_s  = s   + esz, *p_sm  = s;    /* s[x], s[x-1]    */
    const uint8_t *p_sy = sy  + esz, *p_sym = sy;   /* sy[x], sy[x-1]  */
    const uint8_t *p_sz = sz  + esz, *p_szm = sz;   /* sz[x], sz[x-1]  */
    const uint8_t *p_sz2= syz + esz, *p_sz2m= syz;  /* syz[x], syz[x-1]*/
    size_t i = 0;

#define VEC_STEP(ADD, SUB) \
    do { \
        __m128i vs   = _mm_loadu_si128((const __m128i *)(p_s   + i)); \
        __m128i vsm  = _mm_loadu_si128((const __m128i *)(p_sm  + i)); \
        __m128i vsy  = _mm_loadu_si128((const __m128i *)(p_sy  + i)); \
        __m128i vsym = _mm_loadu_si128((const __m128i *)(p_sym + i)); \
        __m128i vsz  = _mm_loadu_si128((const __m128i *)(p_sz  + i)); \
        __m128i vszm = _mm_loadu_si128((const __m128i *)(p_szm + i)); \
        __m128i vsyz = _mm_loadu_si128((const __m128i *)(p_sz2 + i)); \
        __m128i vsyzm= _mm_loadu_si128((const __m128i *)(p_sz2m+ i)); \
        __m128i t = SUB(vs,  vsm); \
        t = SUB(t, vsy); \
        t = SUB(t, vsz); \
        t = ADD(t, vsym); \
        t = ADD(t, vszm); \
        t = ADD(t, vsyz); \
        t = SUB(t, vsyzm); \
        _mm_storeu_si128((__m128i *)(rp + i), t); \
    } while (0)

    if (esz == 1) {
        for (; i + 16 <= nbytes; i += 16) VEC_STEP(_mm_add_epi8, _mm_sub_epi8);
        for (; i < nbytes; ++i) {
            uint8_t r = (uint8_t)(p_s[i] - p_sm[i] - p_sy[i] - p_sz[i]
                                 + p_sym[i] + p_szm[i] + p_sz2[i] - p_sz2m[i]);
            rp[i] = r;
        }
    } else if (esz == 2) {
        for (; i + 16 <= nbytes; i += 16) VEC_STEP(_mm_add_epi16, _mm_sub_epi16);
        for (; i + 2 <= nbytes; i += 2) {
            uint16_t a0, a1, a2, a3, a4, a5, a6, a7;
            memcpy(&a0, p_s   + i, 2); memcpy(&a1, p_sm   + i, 2);
            memcpy(&a2, p_sy  + i, 2); memcpy(&a3, p_sym  + i, 2);
            memcpy(&a4, p_sz  + i, 2); memcpy(&a5, p_szm  + i, 2);
            memcpy(&a6, p_sz2 + i, 2); memcpy(&a7, p_sz2m + i, 2);
            uint16_t r = (uint16_t)(a0 - a1 - a2 - a4 + a3 + a5 + a6 - a7);
            memcpy(rp + i, &r, 2);
        }
    } else if (esz == 4) {
        for (; i + 16 <= nbytes; i += 16) VEC_STEP(_mm_add_epi32, _mm_sub_epi32);
        for (; i + 4 <= nbytes; i += 4) {
            uint32_t a0, a1, a2, a3, a4, a5, a6, a7;
            memcpy(&a0, p_s   + i, 4); memcpy(&a1, p_sm   + i, 4);
            memcpy(&a2, p_sy  + i, 4); memcpy(&a3, p_sym  + i, 4);
            memcpy(&a4, p_sz  + i, 4); memcpy(&a5, p_szm  + i, 4);
            memcpy(&a6, p_sz2 + i, 4); memcpy(&a7, p_sz2m + i, 4);
            uint32_t r = a0 - a1 - a2 - a4 + a3 + a5 + a6 - a7;
            memcpy(rp + i, &r, 4);
        }
    } else { /* esz == 8 */
        for (; i + 16 <= nbytes; i += 16) VEC_STEP(_mm_add_epi64, _mm_sub_epi64);
        for (; i + 8 <= nbytes; i += 8) {
            uint64_t a0, a1, a2, a3, a4, a5, a6, a7;
            memcpy(&a0, p_s   + i, 8); memcpy(&a1, p_sm   + i, 8);
            memcpy(&a2, p_sy  + i, 8); memcpy(&a3, p_sym  + i, 8);
            memcpy(&a4, p_sz  + i, 8); memcpy(&a5, p_szm  + i, 8);
            memcpy(&a6, p_sz2 + i, 8); memcpy(&a7, p_sz2m + i, 8);
            uint64_t r = a0 - a1 - a2 - a4 + a3 + a5 + a6 - a7;
            memcpy(rp + i, &r, 8);
        }
    }
#undef VEC_STEP
}
#endif /* TDC_PRED3D_HAVE_SSE2 */

#ifdef TDC_PRED3D_HAVE_NEON

static void pred3d_sub_row_neon(uint8_t *dst, const uint8_t *a,
                                const uint8_t *b,
                                size_t nbytes, size_t esz) {
    size_t i = 0;
    if (esz == 1) {
        for (; i + 16 <= nbytes; i += 16) {
            uint8x16_t va = vld1q_u8(a + i);
            uint8x16_t vb = vld1q_u8(b + i);
            vst1q_u8(dst + i, vsubq_u8(va, vb));
        }
        for (; i < nbytes; ++i) dst[i] = (uint8_t)(a[i] - b[i]);
    } else if (esz == 2) {
        for (; i + 16 <= nbytes; i += 16) {
            uint16x8_t va = vld1q_u16((const uint16_t *)(a + i));
            uint16x8_t vb = vld1q_u16((const uint16_t *)(b + i));
            vst1q_u16((uint16_t *)(dst + i), vsubq_u16(va, vb));
        }
        for (; i + 2 <= nbytes; i += 2) {
            uint16_t x, y;
            memcpy(&x, a + i, 2); memcpy(&y, b + i, 2);
            uint16_t r = (uint16_t)(x - y);
            memcpy(dst + i, &r, 2);
        }
    } else if (esz == 4) {
        for (; i + 16 <= nbytes; i += 16) {
            uint32x4_t va = vld1q_u32((const uint32_t *)(a + i));
            uint32x4_t vb = vld1q_u32((const uint32_t *)(b + i));
            vst1q_u32((uint32_t *)(dst + i), vsubq_u32(va, vb));
        }
        for (; i + 4 <= nbytes; i += 4) {
            uint32_t x, y;
            memcpy(&x, a + i, 4); memcpy(&y, b + i, 4);
            uint32_t r = x - y;
            memcpy(dst + i, &r, 4);
        }
    } else {
        for (; i + 16 <= nbytes; i += 16) {
            uint64x2_t va = vld1q_u64((const uint64_t *)(a + i));
            uint64x2_t vb = vld1q_u64((const uint64_t *)(b + i));
            vst1q_u64((uint64_t *)(dst + i), vsubq_u64(va, vb));
        }
        for (; i + 8 <= nbytes; i += 8) {
            uint64_t x, y;
            memcpy(&x, a + i, 8); memcpy(&y, b + i, 8);
            uint64_t r = x - y;
            memcpy(dst + i, &r, 8);
        }
    }
}

static void pred3d_add_row_neon(uint8_t *dst, const uint8_t *a,
                                const uint8_t *b,
                                size_t nbytes, size_t esz) {
    size_t i = 0;
    if (esz == 1) {
        for (; i + 16 <= nbytes; i += 16) {
            uint8x16_t va = vld1q_u8(a + i);
            uint8x16_t vb = vld1q_u8(b + i);
            vst1q_u8(dst + i, vaddq_u8(va, vb));
        }
        for (; i < nbytes; ++i) dst[i] = (uint8_t)(a[i] + b[i]);
    } else if (esz == 2) {
        for (; i + 16 <= nbytes; i += 16) {
            uint16x8_t va = vld1q_u16((const uint16_t *)(a + i));
            uint16x8_t vb = vld1q_u16((const uint16_t *)(b + i));
            vst1q_u16((uint16_t *)(dst + i), vaddq_u16(va, vb));
        }
        for (; i + 2 <= nbytes; i += 2) {
            uint16_t x, y;
            memcpy(&x, a + i, 2); memcpy(&y, b + i, 2);
            uint16_t r = (uint16_t)(x + y);
            memcpy(dst + i, &r, 2);
        }
    } else if (esz == 4) {
        for (; i + 16 <= nbytes; i += 16) {
            uint32x4_t va = vld1q_u32((const uint32_t *)(a + i));
            uint32x4_t vb = vld1q_u32((const uint32_t *)(b + i));
            vst1q_u32((uint32_t *)(dst + i), vaddq_u32(va, vb));
        }
        for (; i + 4 <= nbytes; i += 4) {
            uint32_t x, y;
            memcpy(&x, a + i, 4); memcpy(&y, b + i, 4);
            uint32_t r = x + y;
            memcpy(dst + i, &r, 4);
        }
    } else {
        for (; i + 16 <= nbytes; i += 16) {
            uint64x2_t va = vld1q_u64((const uint64_t *)(a + i));
            uint64x2_t vb = vld1q_u64((const uint64_t *)(b + i));
            vst1q_u64((uint64_t *)(dst + i), vaddq_u64(va, vb));
        }
        for (; i + 8 <= nbytes; i += 8) {
            uint64_t x, y;
            memcpy(&x, a + i, 8); memcpy(&y, b + i, 8);
            uint64_t r = x + y;
            memcpy(dst + i, &r, 8);
        }
    }
}

static void pred3d_grad3d_enc_row_neon(
    uint8_t       *dst,
    const uint8_t *s,   const uint8_t *sy,
    const uint8_t *sz,  const uint8_t *syz,
    int64_t nx, size_t esz) {
    if (nx < 2) return;
    const size_t nbytes = (size_t)(nx - 1) * esz;
    uint8_t       *rp   = dst + esz;
    const uint8_t *p_s  = s   + esz, *p_sm  = s;
    const uint8_t *p_sy = sy  + esz, *p_sym = sy;
    const uint8_t *p_sz = sz  + esz, *p_szm = sz;
    const uint8_t *p_sz2= syz + esz, *p_sz2m= syz;
    size_t i = 0;

#define VEC_STEP_NEON(LD, ST, ADD, SUB, TY) \
    do { \
        TY vs   = LD((const void *)(p_s   + i)); \
        TY vsm  = LD((const void *)(p_sm  + i)); \
        TY vsy  = LD((const void *)(p_sy  + i)); \
        TY vsym = LD((const void *)(p_sym + i)); \
        TY vsz  = LD((const void *)(p_sz  + i)); \
        TY vszm = LD((const void *)(p_szm + i)); \
        TY vsyz = LD((const void *)(p_sz2 + i)); \
        TY vsyzm= LD((const void *)(p_sz2m+ i)); \
        TY t = SUB(vs,  vsm); \
        t = SUB(t, vsy); \
        t = SUB(t, vsz); \
        t = ADD(t, vsym); \
        t = ADD(t, vszm); \
        t = ADD(t, vsyz); \
        t = SUB(t, vsyzm); \
        ST((void *)(rp + i), t); \
    } while (0)

    if (esz == 1) {
        for (; i + 16 <= nbytes; i += 16)
            VEC_STEP_NEON(vld1q_u8, vst1q_u8, vaddq_u8, vsubq_u8, uint8x16_t);
        for (; i < nbytes; ++i) {
            uint8_t r = (uint8_t)(p_s[i] - p_sm[i] - p_sy[i] - p_sz[i]
                                 + p_sym[i] + p_szm[i] + p_sz2[i] - p_sz2m[i]);
            rp[i] = r;
        }
    } else if (esz == 2) {
#define LD16(p) vld1q_u16((const uint16_t *)(p))
#define ST16(p, v) vst1q_u16((uint16_t *)(p), (v))
        for (; i + 16 <= nbytes; i += 16)
            VEC_STEP_NEON(LD16, ST16, vaddq_u16, vsubq_u16, uint16x8_t);
#undef LD16
#undef ST16
        for (; i + 2 <= nbytes; i += 2) {
            uint16_t a0,a1,a2,a3,a4,a5,a6,a7;
            memcpy(&a0, p_s   + i, 2); memcpy(&a1, p_sm   + i, 2);
            memcpy(&a2, p_sy  + i, 2); memcpy(&a3, p_sym  + i, 2);
            memcpy(&a4, p_sz  + i, 2); memcpy(&a5, p_szm  + i, 2);
            memcpy(&a6, p_sz2 + i, 2); memcpy(&a7, p_sz2m + i, 2);
            uint16_t r = (uint16_t)(a0 - a1 - a2 - a4 + a3 + a5 + a6 - a7);
            memcpy(rp + i, &r, 2);
        }
    } else if (esz == 4) {
#define LD32(p) vld1q_u32((const uint32_t *)(p))
#define ST32(p, v) vst1q_u32((uint32_t *)(p), (v))
        for (; i + 16 <= nbytes; i += 16)
            VEC_STEP_NEON(LD32, ST32, vaddq_u32, vsubq_u32, uint32x4_t);
#undef LD32
#undef ST32
        for (; i + 4 <= nbytes; i += 4) {
            uint32_t a0,a1,a2,a3,a4,a5,a6,a7;
            memcpy(&a0, p_s   + i, 4); memcpy(&a1, p_sm   + i, 4);
            memcpy(&a2, p_sy  + i, 4); memcpy(&a3, p_sym  + i, 4);
            memcpy(&a4, p_sz  + i, 4); memcpy(&a5, p_szm  + i, 4);
            memcpy(&a6, p_sz2 + i, 4); memcpy(&a7, p_sz2m + i, 4);
            uint32_t r = a0 - a1 - a2 - a4 + a3 + a5 + a6 - a7;
            memcpy(rp + i, &r, 4);
        }
    } else {
#define LD64(p) vld1q_u64((const uint64_t *)(p))
#define ST64(p, v) vst1q_u64((uint64_t *)(p), (v))
        for (; i + 16 <= nbytes; i += 16)
            VEC_STEP_NEON(LD64, ST64, vaddq_u64, vsubq_u64, uint64x2_t);
#undef LD64
#undef ST64
        for (; i + 8 <= nbytes; i += 8) {
            uint64_t a0,a1,a2,a3,a4,a5,a6,a7;
            memcpy(&a0, p_s   + i, 8); memcpy(&a1, p_sm   + i, 8);
            memcpy(&a2, p_sy  + i, 8); memcpy(&a3, p_sym  + i, 8);
            memcpy(&a4, p_sz  + i, 8); memcpy(&a5, p_szm  + i, 8);
            memcpy(&a6, p_sz2 + i, 8); memcpy(&a7, p_sz2m + i, 8);
            uint64_t r = a0 - a1 - a2 - a4 + a3 + a5 + a6 - a7;
            memcpy(rp + i, &r, 8);
        }
    }
#undef VEC_STEP_NEON
}

#endif /* TDC_PRED3D_HAVE_NEON */

/* Dispatch shims used inside DEFINE_PRED3D_TYPED. */
#if defined(TDC_PRED3D_HAVE_SSE2)
#  define PRED3D_SUB_ROW(dst, a, b, nb, esz) \
     pred3d_sub_row_sse2((uint8_t *)(dst), (const uint8_t *)(a), \
                         (const uint8_t *)(b), (nb), (esz))
#  define PRED3D_ADD_ROW(dst, a, b, nb, esz) \
     pred3d_add_row_sse2((uint8_t *)(dst), (const uint8_t *)(a), \
                         (const uint8_t *)(b), (nb), (esz))
#  define PRED3D_GRAD3D_ENC_ROW(dst, s, sy, sz, syz, nx, esz) \
     pred3d_grad3d_enc_row_sse2((uint8_t *)(dst), \
         (const uint8_t *)(s), (const uint8_t *)(sy), \
         (const uint8_t *)(sz), (const uint8_t *)(syz), (nx), (esz))
#  define PRED3D_HAVE_SIMD 1
#elif defined(TDC_PRED3D_HAVE_NEON)
#  define PRED3D_SUB_ROW(dst, a, b, nb, esz) \
     pred3d_sub_row_neon((uint8_t *)(dst), (const uint8_t *)(a), \
                         (const uint8_t *)(b), (nb), (esz))
#  define PRED3D_ADD_ROW(dst, a, b, nb, esz) \
     pred3d_add_row_neon((uint8_t *)(dst), (const uint8_t *)(a), \
                         (const uint8_t *)(b), (nb), (esz))
#  define PRED3D_GRAD3D_ENC_ROW(dst, s, sy, sz, syz, nx, esz) \
     pred3d_grad3d_enc_row_neon((uint8_t *)(dst), \
         (const uint8_t *)(s), (const uint8_t *)(sy), \
         (const uint8_t *)(sz), (const uint8_t *)(syz), (nx), (esz))
#  define PRED3D_HAVE_SIMD 1
#else
#  define PRED3D_HAVE_SIMD 0
#endif

#if PRED3D_HAVE_SIMD
#  define IF_SIMD(code)   code
#  define IF_NOSIMD(code)
#else
#  define IF_SIMD(code)
#  define IF_NOSIMD(code) code
#endif

/* ----- Acceptance bitmasks ----------------------------------------------- */

#define PRED3D_ACCEPTED_DTYPES (         \
    TDC_DT_BIT(TDC_DT_I8)  |             \
    TDC_DT_BIT(TDC_DT_I16) |             \
    TDC_DT_BIT(TDC_DT_I32) |             \
    TDC_DT_BIT(TDC_DT_U8)  |             \
    TDC_DT_BIT(TDC_DT_U16) |             \
    TDC_DT_BIT(TDC_DT_U32) |             \
    TDC_DT_BIT(TDC_DT_F16) |             \
    TDC_DT_BIT(TDC_DT_F32) |             \
    TDC_DT_BIT(TDC_DT_F64))

#define PRED3D_ACCEPTED_LAYOUTS TDC_LAYOUT_BIT(TDC_LAYOUT_VOLUME_3D)

static int pred3d_dtype_accepted(tdc_dtype dt) {
    return tdc_model_dtype_accepted(PRED3D_ACCEPTED_DTYPES, dt);
}

/* ----- Paeth (typed) ----------------------------------------------------- */
/*
 * Same tie-break as pred2d: pa<=pb && pa<=pc → a, else pb<=pc → b,
 * else c. Here the linear predictor `p` is taken as a parameter rather
 * than computed inside the helper, because GRAD3D and the per-octant
 * 2D-Paeth callers need `p` separately and recomputing it would be
 * wasteful. The ternary chains compile to branch-free cmov/csel under
 * -O2 and above on x86_64 / aarch64.
 */

static inline int32_t paeth32(int32_t a, int32_t b, int32_t c,
                              int32_t p) {
    int32_t pa = p > a ? p - a : a - p;
    int32_t pb = p > b ? p - b : b - p;
    int32_t pc = p > c ? p - c : c - p;
    int32_t r  = (pb <= pc) ? b : c;
    return (pa <= pb && pa <= pc) ? a : r;
}

static inline int64_t paeth64(int64_t a, int64_t b, int64_t c,
                              int64_t p) {
    int64_t pa = p > a ? p - a : a - p;
    int64_t pb = p > b ? p - b : b - p;
    int64_t pc = p > c ? p - c : c - p;
    int64_t r  = (pb <= pc) ? b : c;
    return (pa <= pb && pa <= pc) ? a : r;
}

/* ----- Typed kernel template --------------------------------------------- */
/*
 * Per-dtype encode + decode pair. Both functions switch on `kind` once
 * at the top. For LEFT/UP/FRONT a single inner loop suffices because
 * the predictor depends on only one axis. For AVG3/GRAD3D/PAETH3D the
 * eight (z,y,x) octants are written out in dependency order:
 *
 *   O1: (z=0, y=0, x=0)             corner
 *   O2: (z=0, y=0, x>=1)            x edge
 *   O3: (z=0, y>=1, x=0)            y edge
 *   O5: (z>=1, y=0, x=0)            z edge
 *   O4: (z=0, y>=1, x>=1)           z=0 face
 *   O6: (z>=1, y=0, x>=1)           y=0 face
 *   O7: (z>=1, y>=1, x=0)           x=0 face
 *   O8: (z>=1, y>=1, x>=1)          inner box (hot)
 *
 * The order is a topological sort of the decode dependency graph; the
 * encode side could use any order but uses the same one for symmetry.
 */

#define DEFINE_PRED3D_TYPED(SUFFIX, T, U, W, PAETH)                            \
                                                                               \
static void pred3d_enc_##SUFFIX(tdc_pred3d_kind kind,                          \
                                const T *src, T *res,                          \
                                int64_t nx, int64_t ny, int64_t nz) {          \
    if (nx <= 0 || ny <= 0 || nz <= 0) return;                                 \
    const int64_t slab = nx * ny;                                              \
    switch (kind) {                                                            \
        case TDC_PRED3D_LEFT: {                                                \
            for (int64_t z = 0; z < nz; ++z) {                                 \
                for (int64_t y = 0; y < ny; ++y) {                             \
                    const T *s = src + z * slab + y * nx;                      \
                    T       *r = res + z * slab + y * nx;                      \
                    *(U *)&r[0] = (U)(W)s[0];                                  \
IF_SIMD(            PRED3D_SUB_ROW(r + 1, s + 1, s,                            \
                         (size_t)(nx > 1 ? nx - 1 : 0) * sizeof(T),            \
                         sizeof(T));)                                          \
IF_NOSIMD(          for (int64_t x = 1; x < nx; ++x) {                         \
                        W val  = (W)s[x];                                      \
                        W left = (W)s[x - 1];                                  \
                        *(U *)&r[x] = (U)(val - left);                         \
                    })                                                         \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED3D_UP: {                                                  \
            for (int64_t z = 0; z < nz; ++z) {                                 \
                {                                                              \
                    const T *s = src + z * slab;                               \
                    T       *r = res + z * slab;                               \
                    for (int64_t x = 0; x < nx; ++x)                           \
                        *(U *)&r[x] = (U)(W)s[x];                              \
                }                                                              \
                for (int64_t y = 1; y < ny; ++y) {                             \
                    const T *s  = src + z * slab + y       * nx;               \
                    const T *sy = src + z * slab + (y - 1) * nx;               \
                    T       *r  = res + z * slab + y       * nx;               \
IF_SIMD(            PRED3D_SUB_ROW(r, s, sy, (size_t)nx * sizeof(T), sizeof(T));)\
IF_NOSIMD(          for (int64_t x = 0; x < nx; ++x) {                         \
                        W val = (W)s[x];                                       \
                        W up  = (W)sy[x];                                      \
                        *(U *)&r[x] = (U)(val - up);                           \
                    })                                                         \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED3D_FRONT: {                                               \
            {                                                                  \
                const T *s = src;                                              \
                T       *r = res;                                              \
                for (int64_t i = 0; i < slab; ++i)                             \
                    *(U *)&r[i] = (U)(W)s[i];                                  \
            }                                                                  \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                const T *s  = src + z       * slab;                            \
                const T *sz = src + (z - 1) * slab;                            \
                T       *r  = res + z       * slab;                            \
IF_SIMD(        PRED3D_SUB_ROW(r, s, sz, (size_t)slab * sizeof(T), sizeof(T));)\
IF_NOSIMD(      for (int64_t i = 0; i < slab; ++i) {                           \
                    W val   = (W)s[i];                                         \
                    W front = (W)sz[i];                                        \
                    *(U *)&r[i] = (U)(val - front);                            \
                })                                                             \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED3D_AVG3: {                                                \
            /* O1 */                                                           \
            *(U *)&res[0] = (U)(W)src[0];                                      \
            /* O2: pred = a */                                                 \
            for (int64_t x = 1; x < nx; ++x) {                                 \
                W val = (W)src[x], a = (W)src[x - 1];                          \
                *(U *)&res[x] = (U)(val - a);                                  \
            }                                                                  \
            /* O3: pred = b */                                                 \
            for (int64_t y = 1; y < ny; ++y) {                                 \
                const T *s  = src + y       * nx;                              \
                const T *sy = src + (y - 1) * nx;                              \
                T       *r  = res + y       * nx;                              \
                W val = (W)s[0], b = (W)sy[0];                                 \
                *(U *)&r[0] = (U)(val - b);                                    \
            }                                                                  \
            /* O5: pred = c */                                                 \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                const T *s  = src + z       * slab;                            \
                const T *sz = src + (z - 1) * slab;                            \
                T       *r  = res + z       * slab;                            \
                W val = (W)s[0], c = (W)sz[0];                                 \
                *(U *)&r[0] = (U)(val - c);                                    \
            }                                                                  \
            /* O4: pred = (a + b) / 2 */                                       \
            for (int64_t y = 1; y < ny; ++y) {                                 \
                const T *s  = src + y       * nx;                              \
                const T *sy = src + (y - 1) * nx;                              \
                T       *r  = res + y       * nx;                              \
                for (int64_t x = 1; x < nx; ++x) {                             \
                    W val = (W)s[x], a = (W)s[x - 1], b = (W)sy[x];            \
                    W pred = (a + b) / 2;                                      \
                    *(U *)&r[x] = (U)(val - pred);                             \
                }                                                              \
            }                                                                  \
            /* O6: pred = (a + c) / 2 */                                       \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                const T *s  = src + z       * slab;                            \
                const T *sz = src + (z - 1) * slab;                            \
                T       *r  = res + z       * slab;                            \
                for (int64_t x = 1; x < nx; ++x) {                             \
                    W val = (W)s[x], a = (W)s[x - 1], c = (W)sz[x];            \
                    W pred = (a + c) / 2;                                      \
                    *(U *)&r[x] = (U)(val - pred);                             \
                }                                                              \
            }                                                                  \
            /* O7: pred = (b + c) / 2 */                                       \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                for (int64_t y = 1; y < ny; ++y) {                             \
                    const T *s  = src + z       * slab + y       * nx;         \
                    const T *sy = src + z       * slab + (y - 1) * nx;         \
                    const T *sz = src + (z - 1) * slab + y       * nx;         \
                    T       *r  = res + z       * slab + y       * nx;         \
                    W val = (W)s[0], b = (W)sy[0], c = (W)sz[0];               \
                    W pred = (b + c) / 2;                                      \
                    *(U *)&r[0] = (U)(val - pred);                             \
                }                                                              \
            }                                                                  \
            /* O8: pred = (a + b + c) / 3 */                                   \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                for (int64_t y = 1; y < ny; ++y) {                             \
                    const T *s  = src + z       * slab + y       * nx;         \
                    const T *sy = src + z       * slab + (y - 1) * nx;         \
                    const T *sz = src + (z - 1) * slab + y       * nx;         \
                    T       *r  = res + z       * slab + y       * nx;         \
                    for (int64_t x = 1; x < nx; ++x) {                         \
                        W val = (W)s[x];                                       \
                        W a = (W)s[x - 1], b = (W)sy[x], c = (W)sz[x];         \
                        W pred = (a + b + c) / 3;                              \
                        *(U *)&r[x] = (U)(val - pred);                         \
                    }                                                          \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED3D_GRAD3D: {                                              \
            /* O1 */                                                           \
            *(U *)&res[0] = (U)(W)src[0];                                      \
            /* O2: pred = a */                                                 \
            for (int64_t x = 1; x < nx; ++x) {                                 \
                W val = (W)src[x], a = (W)src[x - 1];                          \
                *(U *)&res[x] = (U)(val - a);                                  \
            }                                                                  \
            /* O3: pred = b */                                                 \
            for (int64_t y = 1; y < ny; ++y) {                                 \
                const T *s  = src + y       * nx;                              \
                const T *sy = src + (y - 1) * nx;                              \
                T       *r  = res + y       * nx;                              \
                W val = (W)s[0], b = (W)sy[0];                                 \
                *(U *)&r[0] = (U)(val - b);                                    \
            }                                                                  \
            /* O5: pred = c */                                                 \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                const T *s  = src + z       * slab;                            \
                const T *sz = src + (z - 1) * slab;                            \
                T       *r  = res + z       * slab;                            \
                W val = (W)s[0], c = (W)sz[0];                                 \
                *(U *)&r[0] = (U)(val - c);                                    \
            }                                                                  \
            /* O4: pred = a + b - ab  (2D plane on z=0) */                     \
            for (int64_t y = 1; y < ny; ++y) {                                 \
                const T *s  = src + y       * nx;                              \
                const T *sy = src + (y - 1) * nx;                              \
                T       *r  = res + y       * nx;                              \
                for (int64_t x = 1; x < nx; ++x) {                             \
                    W val = (W)s[x];                                           \
                    W a = (W)s[x - 1], b = (W)sy[x], ab = (W)sy[x - 1];        \
                    W pred = a + b - ab;                                       \
                    *(U *)&r[x] = (U)(val - pred);                             \
                }                                                              \
            }                                                                  \
            /* O6: pred = a + c - ac  (2D plane on y=0) */                     \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                const T *s  = src + z       * slab;                            \
                const T *sz = src + (z - 1) * slab;                            \
                T       *r  = res + z       * slab;                            \
                for (int64_t x = 1; x < nx; ++x) {                             \
                    W val = (W)s[x];                                           \
                    W a = (W)s[x - 1], c = (W)sz[x], ac = (W)sz[x - 1];        \
                    W pred = a + c - ac;                                       \
                    *(U *)&r[x] = (U)(val - pred);                             \
                }                                                              \
            }                                                                  \
            /* O7: pred = b + c - bc  (2D plane on x=0) */                     \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                for (int64_t y = 1; y < ny; ++y) {                             \
                    const T *s   = src + z       * slab + y       * nx;        \
                    const T *sy  = src + z       * slab + (y - 1) * nx;        \
                    const T *sz  = src + (z - 1) * slab + y       * nx;        \
                    const T *syz = src + (z - 1) * slab + (y - 1) * nx;        \
                    T       *r   = res + z       * slab + y       * nx;        \
                    W val = (W)s[0];                                           \
                    W b = (W)sy[0], c = (W)sz[0], bc = (W)syz[0];              \
                    W pred = b + c - bc;                                       \
                    *(U *)&r[0] = (U)(val - pred);                             \
                }                                                              \
            }                                                                  \
            /* O8: pred = a + b + c - ab - ac - bc + abc */                    \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                for (int64_t y = 1; y < ny; ++y) {                             \
                    const T *s   = src + z       * slab + y       * nx;        \
                    const T *sy  = src + z       * slab + (y - 1) * nx;        \
                    const T *sz  = src + (z - 1) * slab + y       * nx;        \
                    const T *syz = src + (z - 1) * slab + (y - 1) * nx;        \
                    T       *r   = res + z       * slab + y       * nx;        \
                    /* x=0 column handled in O7 above */                       \
IF_SIMD(            PRED3D_GRAD3D_ENC_ROW(r, s, sy, sz, syz, nx, sizeof(T));)  \
IF_NOSIMD(          for (int64_t x = 1; x < nx; ++x) {                         \
                        W val = (W)s[x];                                       \
                        W a   = (W)s[x - 1];                                   \
                        W b   = (W)sy[x];                                      \
                        W c   = (W)sz[x];                                      \
                        W ab  = (W)sy[x - 1];                                  \
                        W ac  = (W)sz[x - 1];                                  \
                        W bc  = (W)syz[x];                                     \
                        W abc = (W)syz[x - 1];                                 \
                        W pred = a + b + c - ab - ac - bc + abc;               \
                        *(U *)&r[x] = (U)(val - pred);                         \
                    })                                                         \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED3D_PAETH3D: {                                             \
            /* O1 */                                                           \
            *(U *)&res[0] = (U)(W)src[0];                                      \
            /* O2: pred = a */                                                 \
            for (int64_t x = 1; x < nx; ++x) {                                 \
                W val = (W)src[x], a = (W)src[x - 1];                          \
                *(U *)&res[x] = (U)(val - a);                                  \
            }                                                                  \
            /* O3: pred = b */                                                 \
            for (int64_t y = 1; y < ny; ++y) {                                 \
                const T *s  = src + y       * nx;                              \
                const T *sy = src + (y - 1) * nx;                              \
                T       *r  = res + y       * nx;                              \
                W val = (W)s[0], b = (W)sy[0];                                 \
                *(U *)&r[0] = (U)(val - b);                                    \
            }                                                                  \
            /* O5: pred = c */                                                 \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                const T *s  = src + z       * slab;                            \
                const T *sz = src + (z - 1) * slab;                            \
                T       *r  = res + z       * slab;                            \
                W val = (W)s[0], c = (W)sz[0];                                 \
                *(U *)&r[0] = (U)(val - c);                                    \
            }                                                                  \
            /* O4: 2D Paeth on (a, b, ab) — z=0 face */                        \
            for (int64_t y = 1; y < ny; ++y) {                                 \
                const T *s  = src + y       * nx;                              \
                const T *sy = src + (y - 1) * nx;                              \
                T       *r  = res + y       * nx;                              \
                for (int64_t x = 1; x < nx; ++x) {                             \
                    W val = (W)s[x];                                           \
                    W a = (W)s[x - 1], b = (W)sy[x], ab = (W)sy[x - 1];        \
                    W pred = PAETH(a, b, ab, a + b - ab);                      \
                    *(U *)&r[x] = (U)(val - pred);                             \
                }                                                              \
            }                                                                  \
            /* O6: 2D Paeth on (a, c, ac) — y=0 face */                        \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                const T *s  = src + z       * slab;                            \
                const T *sz = src + (z - 1) * slab;                            \
                T       *r  = res + z       * slab;                            \
                for (int64_t x = 1; x < nx; ++x) {                             \
                    W val = (W)s[x];                                           \
                    W a = (W)s[x - 1], c = (W)sz[x], ac = (W)sz[x - 1];        \
                    W pred = PAETH(a, c, ac, a + c - ac);                      \
                    *(U *)&r[x] = (U)(val - pred);                             \
                }                                                              \
            }                                                                  \
            /* O7: 2D Paeth on (b, c, bc) — x=0 face */                        \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                for (int64_t y = 1; y < ny; ++y) {                             \
                    const T *s   = src + z       * slab + y       * nx;        \
                    const T *sy  = src + z       * slab + (y - 1) * nx;        \
                    const T *sz  = src + (z - 1) * slab + y       * nx;        \
                    const T *syz = src + (z - 1) * slab + (y - 1) * nx;        \
                    T       *r   = res + z       * slab + y       * nx;        \
                    W val = (W)s[0];                                           \
                    W b = (W)sy[0], c = (W)sz[0], bc = (W)syz[0];              \
                    W pred = PAETH(b, c, bc, b + c - bc);                      \
                    *(U *)&r[0] = (U)(val - pred);                             \
                }                                                              \
            }                                                                  \
            /* O8: 3D Paeth on (a, b, c) — inner box */                        \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                for (int64_t y = 1; y < ny; ++y) {                             \
                    const T *s   = src + z       * slab + y       * nx;        \
                    const T *sy  = src + z       * slab + (y - 1) * nx;        \
                    const T *sz  = src + (z - 1) * slab + y       * nx;        \
                    const T *syz = src + (z - 1) * slab + (y - 1) * nx;        \
                    T       *r   = res + z       * slab + y       * nx;        \
                    for (int64_t x = 1; x < nx; ++x) {                         \
                        W val = (W)s[x];                                       \
                        W a   = (W)s[x - 1];                                   \
                        W b   = (W)sy[x];                                      \
                        W c   = (W)sz[x];                                      \
                        W ab  = (W)sy[x - 1];                                  \
                        W ac  = (W)sz[x - 1];                                  \
                        W bc  = (W)syz[x];                                     \
                        W abc = (W)syz[x - 1];                                 \
                        W p    = a + b + c - ab - ac - bc + abc;               \
                        W pred = PAETH(a, b, c, p);                            \
                        *(U *)&r[x] = (U)(val - pred);                         \
                    }                                                          \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        default: break;                                                        \
    }                                                                          \
}                                                                              \
                                                                               \
static void pred3d_dec_##SUFFIX(tdc_pred3d_kind kind,                          \
                                const T *res, T *dst,                          \
                                int64_t nx, int64_t ny, int64_t nz) {          \
    if (nx <= 0 || ny <= 0 || nz <= 0) return;                                 \
    const int64_t slab = nx * ny;                                              \
    switch (kind) {                                                            \
        case TDC_PRED3D_LEFT: {                                                \
            for (int64_t z = 0; z < nz; ++z) {                                 \
                for (int64_t y = 0; y < ny; ++y) {                             \
                    const T *r = res + z * slab + y * nx;                      \
                    T       *d = dst + z * slab + y * nx;                      \
                    *(U *)&d[0] = (U)(W)r[0];                                  \
                    for (int64_t x = 1; x < nx; ++x) {                         \
                        W rv   = (W)r[x];                                      \
                        W left = (W)d[x - 1];                                  \
                        *(U *)&d[x] = (U)(rv + left);                          \
                    }                                                          \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED3D_UP: {                                                  \
            for (int64_t z = 0; z < nz; ++z) {                                 \
                {                                                              \
                    const T *r = res + z * slab;                               \
                    T       *d = dst + z * slab;                               \
                    for (int64_t x = 0; x < nx; ++x)                           \
                        *(U *)&d[x] = (U)(W)r[x];                              \
                }                                                              \
                for (int64_t y = 1; y < ny; ++y) {                             \
                    const T *r  = res + z * slab + y       * nx;               \
                    T       *d  = dst + z * slab + y       * nx;               \
                    const T *dy = dst + z * slab + (y - 1) * nx;               \
IF_SIMD(            PRED3D_ADD_ROW(d, r, dy, (size_t)nx * sizeof(T), sizeof(T));)\
IF_NOSIMD(          for (int64_t x = 0; x < nx; ++x) {                         \
                        W rv = (W)r[x];                                        \
                        W up = (W)dy[x];                                       \
                        *(U *)&d[x] = (U)(rv + up);                            \
                    })                                                         \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED3D_FRONT: {                                               \
            {                                                                  \
                const T *r = res;                                              \
                T       *d = dst;                                              \
                for (int64_t i = 0; i < slab; ++i)                             \
                    *(U *)&d[i] = (U)(W)r[i];                                  \
            }                                                                  \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                const T *r  = res + z       * slab;                            \
                T       *d  = dst + z       * slab;                            \
                const T *dz = dst + (z - 1) * slab;                            \
IF_SIMD(        PRED3D_ADD_ROW(d, r, dz, (size_t)slab * sizeof(T), sizeof(T));)\
IF_NOSIMD(      for (int64_t i = 0; i < slab; ++i) {                           \
                    W rv    = (W)r[i];                                         \
                    W front = (W)dz[i];                                        \
                    *(U *)&d[i] = (U)(rv + front);                             \
                })                                                             \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED3D_AVG3: {                                                \
            /* O1 */                                                           \
            *(U *)&dst[0] = (U)(W)res[0];                                      \
            /* O2: pred = a */                                                 \
            for (int64_t x = 1; x < nx; ++x) {                                 \
                W rv = (W)res[x], a = (W)dst[x - 1];                           \
                *(U *)&dst[x] = (U)(rv + a);                                   \
            }                                                                  \
            /* O3: pred = b */                                                 \
            for (int64_t y = 1; y < ny; ++y) {                                 \
                const T *r  = res + y       * nx;                              \
                T       *d  = dst + y       * nx;                              \
                const T *dy = dst + (y - 1) * nx;                              \
                W rv = (W)r[0], b = (W)dy[0];                                  \
                *(U *)&d[0] = (U)(rv + b);                                     \
            }                                                                  \
            /* O5: pred = c */                                                 \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                const T *r  = res + z       * slab;                            \
                T       *d  = dst + z       * slab;                            \
                const T *dz = dst + (z - 1) * slab;                            \
                W rv = (W)r[0], c = (W)dz[0];                                  \
                *(U *)&d[0] = (U)(rv + c);                                     \
            }                                                                  \
            /* O4: pred = (a + b) / 2 */                                       \
            for (int64_t y = 1; y < ny; ++y) {                                 \
                const T *r  = res + y       * nx;                              \
                T       *d  = dst + y       * nx;                              \
                const T *dy = dst + (y - 1) * nx;                              \
                for (int64_t x = 1; x < nx; ++x) {                             \
                    W rv = (W)r[x], a = (W)d[x - 1], b = (W)dy[x];             \
                    W pred = (a + b) / 2;                                      \
                    *(U *)&d[x] = (U)(rv + pred);                              \
                }                                                              \
            }                                                                  \
            /* O6: pred = (a + c) / 2 */                                       \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                const T *r  = res + z       * slab;                            \
                T       *d  = dst + z       * slab;                            \
                const T *dz = dst + (z - 1) * slab;                            \
                for (int64_t x = 1; x < nx; ++x) {                             \
                    W rv = (W)r[x], a = (W)d[x - 1], c = (W)dz[x];             \
                    W pred = (a + c) / 2;                                      \
                    *(U *)&d[x] = (U)(rv + pred);                              \
                }                                                              \
            }                                                                  \
            /* O7: pred = (b + c) / 2 */                                       \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                for (int64_t y = 1; y < ny; ++y) {                             \
                    const T *r  = res + z       * slab + y       * nx;         \
                    T       *d  = dst + z       * slab + y       * nx;         \
                    const T *dy = dst + z       * slab + (y - 1) * nx;         \
                    const T *dz = dst + (z - 1) * slab + y       * nx;         \
                    W rv = (W)r[0], b = (W)dy[0], c = (W)dz[0];                \
                    W pred = (b + c) / 2;                                      \
                    *(U *)&d[0] = (U)(rv + pred);                              \
                }                                                              \
            }                                                                  \
            /* O8: pred = (a + b + c) / 3 */                                   \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                for (int64_t y = 1; y < ny; ++y) {                             \
                    const T *r  = res + z       * slab + y       * nx;         \
                    T       *d  = dst + z       * slab + y       * nx;         \
                    const T *dy = dst + z       * slab + (y - 1) * nx;         \
                    const T *dz = dst + (z - 1) * slab + y       * nx;         \
                    for (int64_t x = 1; x < nx; ++x) {                         \
                        W rv = (W)r[x];                                        \
                        W a = (W)d[x - 1], b = (W)dy[x], c = (W)dz[x];         \
                        W pred = (a + b + c) / 3;                              \
                        *(U *)&d[x] = (U)(rv + pred);                          \
                    }                                                          \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED3D_GRAD3D: {                                              \
            /* O1 */                                                           \
            *(U *)&dst[0] = (U)(W)res[0];                                      \
            /* O2: pred = a */                                                 \
            for (int64_t x = 1; x < nx; ++x) {                                 \
                W rv = (W)res[x], a = (W)dst[x - 1];                           \
                *(U *)&dst[x] = (U)(rv + a);                                   \
            }                                                                  \
            /* O3: pred = b */                                                 \
            for (int64_t y = 1; y < ny; ++y) {                                 \
                const T *r  = res + y       * nx;                              \
                T       *d  = dst + y       * nx;                              \
                const T *dy = dst + (y - 1) * nx;                              \
                W rv = (W)r[0], b = (W)dy[0];                                  \
                *(U *)&d[0] = (U)(rv + b);                                     \
            }                                                                  \
            /* O5: pred = c */                                                 \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                const T *r  = res + z       * slab;                            \
                T       *d  = dst + z       * slab;                            \
                const T *dz = dst + (z - 1) * slab;                            \
                W rv = (W)r[0], c = (W)dz[0];                                  \
                *(U *)&d[0] = (U)(rv + c);                                     \
            }                                                                  \
            /* O4: pred = a + b - ab */                                        \
            for (int64_t y = 1; y < ny; ++y) {                                 \
                const T *r  = res + y       * nx;                              \
                T       *d  = dst + y       * nx;                              \
                const T *dy = dst + (y - 1) * nx;                              \
                for (int64_t x = 1; x < nx; ++x) {                             \
                    W rv = (W)r[x];                                            \
                    W a = (W)d[x - 1], b = (W)dy[x], ab = (W)dy[x - 1];        \
                    W pred = a + b - ab;                                       \
                    *(U *)&d[x] = (U)(rv + pred);                              \
                }                                                              \
            }                                                                  \
            /* O6: pred = a + c - ac */                                        \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                const T *r  = res + z       * slab;                            \
                T       *d  = dst + z       * slab;                            \
                const T *dz = dst + (z - 1) * slab;                            \
                for (int64_t x = 1; x < nx; ++x) {                             \
                    W rv = (W)r[x];                                            \
                    W a = (W)d[x - 1], c = (W)dz[x], ac = (W)dz[x - 1];        \
                    W pred = a + c - ac;                                       \
                    *(U *)&d[x] = (U)(rv + pred);                              \
                }                                                              \
            }                                                                  \
            /* O7: pred = b + c - bc */                                        \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                for (int64_t y = 1; y < ny; ++y) {                             \
                    const T *r   = res + z       * slab + y       * nx;        \
                    T       *d   = dst + z       * slab + y       * nx;        \
                    const T *dy  = dst + z       * slab + (y - 1) * nx;        \
                    const T *dz  = dst + (z - 1) * slab + y       * nx;        \
                    const T *dyz = dst + (z - 1) * slab + (y - 1) * nx;        \
                    W rv = (W)r[0];                                            \
                    W b = (W)dy[0], c = (W)dz[0], bc = (W)dyz[0];              \
                    W pred = b + c - bc;                                       \
                    *(U *)&d[0] = (U)(rv + pred);                              \
                }                                                              \
            }                                                                  \
            /* O8: pred = a + b + c - ab - ac - bc + abc */                    \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                for (int64_t y = 1; y < ny; ++y) {                             \
                    const T *r   = res + z       * slab + y       * nx;        \
                    T       *d   = dst + z       * slab + y       * nx;        \
                    const T *dy  = dst + z       * slab + (y - 1) * nx;        \
                    const T *dz  = dst + (z - 1) * slab + y       * nx;        \
                    const T *dyz = dst + (z - 1) * slab + (y - 1) * nx;        \
                    for (int64_t x = 1; x < nx; ++x) {                         \
                        W rv  = (W)r[x];                                       \
                        W a   = (W)d[x - 1];                                   \
                        W b   = (W)dy[x];                                      \
                        W c   = (W)dz[x];                                      \
                        W ab  = (W)dy[x - 1];                                  \
                        W ac  = (W)dz[x - 1];                                  \
                        W bc  = (W)dyz[x];                                     \
                        W abc = (W)dyz[x - 1];                                 \
                        W pred = a + b + c - ab - ac - bc + abc;               \
                        *(U *)&d[x] = (U)(rv + pred);                          \
                    }                                                          \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        case TDC_PRED3D_PAETH3D: {                                             \
            /* O1 */                                                           \
            *(U *)&dst[0] = (U)(W)res[0];                                      \
            /* O2: pred = a */                                                 \
            for (int64_t x = 1; x < nx; ++x) {                                 \
                W rv = (W)res[x], a = (W)dst[x - 1];                           \
                *(U *)&dst[x] = (U)(rv + a);                                   \
            }                                                                  \
            /* O3: pred = b */                                                 \
            for (int64_t y = 1; y < ny; ++y) {                                 \
                const T *r  = res + y       * nx;                              \
                T       *d  = dst + y       * nx;                              \
                const T *dy = dst + (y - 1) * nx;                              \
                W rv = (W)r[0], b = (W)dy[0];                                  \
                *(U *)&d[0] = (U)(rv + b);                                     \
            }                                                                  \
            /* O5: pred = c */                                                 \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                const T *r  = res + z       * slab;                            \
                T       *d  = dst + z       * slab;                            \
                const T *dz = dst + (z - 1) * slab;                            \
                W rv = (W)r[0], c = (W)dz[0];                                  \
                *(U *)&d[0] = (U)(rv + c);                                     \
            }                                                                  \
            /* O4: 2D Paeth on (a, b, ab) */                                   \
            for (int64_t y = 1; y < ny; ++y) {                                 \
                const T *r  = res + y       * nx;                              \
                T       *d  = dst + y       * nx;                              \
                const T *dy = dst + (y - 1) * nx;                              \
                for (int64_t x = 1; x < nx; ++x) {                             \
                    W rv = (W)r[x];                                            \
                    W a = (W)d[x - 1], b = (W)dy[x], ab = (W)dy[x - 1];        \
                    W pred = PAETH(a, b, ab, a + b - ab);                      \
                    *(U *)&d[x] = (U)(rv + pred);                              \
                }                                                              \
            }                                                                  \
            /* O6: 2D Paeth on (a, c, ac) */                                   \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                const T *r  = res + z       * slab;                            \
                T       *d  = dst + z       * slab;                            \
                const T *dz = dst + (z - 1) * slab;                            \
                for (int64_t x = 1; x < nx; ++x) {                             \
                    W rv = (W)r[x];                                            \
                    W a = (W)d[x - 1], c = (W)dz[x], ac = (W)dz[x - 1];        \
                    W pred = PAETH(a, c, ac, a + c - ac);                      \
                    *(U *)&d[x] = (U)(rv + pred);                              \
                }                                                              \
            }                                                                  \
            /* O7: 2D Paeth on (b, c, bc) */                                   \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                for (int64_t y = 1; y < ny; ++y) {                             \
                    const T *r   = res + z       * slab + y       * nx;        \
                    T       *d   = dst + z       * slab + y       * nx;        \
                    const T *dy  = dst + z       * slab + (y - 1) * nx;        \
                    const T *dz  = dst + (z - 1) * slab + y       * nx;        \
                    const T *dyz = dst + (z - 1) * slab + (y - 1) * nx;        \
                    W rv = (W)r[0];                                            \
                    W b = (W)dy[0], c = (W)dz[0], bc = (W)dyz[0];              \
                    W pred = PAETH(b, c, bc, b + c - bc);                      \
                    *(U *)&d[0] = (U)(rv + pred);                              \
                }                                                              \
            }                                                                  \
            /* O8: 3D Paeth on (a, b, c) */                                    \
            for (int64_t z = 1; z < nz; ++z) {                                 \
                for (int64_t y = 1; y < ny; ++y) {                             \
                    const T *r   = res + z       * slab + y       * nx;        \
                    T       *d   = dst + z       * slab + y       * nx;        \
                    const T *dy  = dst + z       * slab + (y - 1) * nx;        \
                    const T *dz  = dst + (z - 1) * slab + y       * nx;        \
                    const T *dyz = dst + (z - 1) * slab + (y - 1) * nx;        \
                    for (int64_t x = 1; x < nx; ++x) {                         \
                        W rv  = (W)r[x];                                       \
                        W a   = (W)d[x - 1];                                   \
                        W b   = (W)dy[x];                                      \
                        W c   = (W)dz[x];                                      \
                        W ab  = (W)dy[x - 1];                                  \
                        W ac  = (W)dz[x - 1];                                  \
                        W bc  = (W)dyz[x];                                     \
                        W abc = (W)dyz[x - 1];                                 \
                        W p    = a + b + c - ab - ac - bc + abc;               \
                        W pred = PAETH(a, b, c, p);                            \
                        *(U *)&d[x] = (U)(rv + pred);                          \
                    }                                                          \
                }                                                              \
            }                                                                  \
            break;                                                             \
        }                                                                      \
        default: break;                                                        \
    }                                                                          \
}

DEFINE_PRED3D_TYPED(i8,  int8_t,   uint8_t,  int32_t, paeth32)
DEFINE_PRED3D_TYPED(u8,  uint8_t,  uint8_t,  int32_t, paeth32)
DEFINE_PRED3D_TYPED(i16, int16_t,  uint16_t, int32_t, paeth32)
DEFINE_PRED3D_TYPED(u16, uint16_t, uint16_t, int32_t, paeth32)
DEFINE_PRED3D_TYPED(i32, int32_t,  uint32_t, int64_t, paeth64)
DEFINE_PRED3D_TYPED(u32, uint32_t, uint32_t, int64_t, paeth64)

#undef DEFINE_PRED3D_TYPED

/* Float encode/decode live in pred3d_float.c. */
void pred3d_encode_float(tdc_dtype dt, tdc_pred3d_kind kind,
                         const uint8_t *src, uint8_t *res,
                         int64_t nx, int64_t ny, int64_t nz);
void pred3d_decode_float(tdc_dtype dt, tdc_pred3d_kind kind,
                         const uint8_t *res, uint8_t *dst,
                         int64_t nx, int64_t ny, int64_t nz);

/* ----- Sweep dispatchers ------------------------------------------------- */

static void pred3d_encode_sweep(tdc_dtype dt, tdc_pred3d_kind kind,
                                const uint8_t *src,
                                uint8_t *res,
                                int64_t nx, int64_t ny, int64_t nz) {
    if (tdc_dtype_is_float(dt)) {
        pred3d_encode_float(dt, kind, src, res, nx, ny, nz);
        return;
    }
    switch (dt) {
        case TDC_DT_I8:  pred3d_enc_i8 (kind, (const int8_t   *)src, (int8_t   *)res, nx, ny, nz); break;
        case TDC_DT_U8:  pred3d_enc_u8 (kind, (const uint8_t  *)src, (uint8_t  *)res, nx, ny, nz); break;
        case TDC_DT_I16: pred3d_enc_i16(kind, (const int16_t  *)src, (int16_t  *)res, nx, ny, nz); break;
        case TDC_DT_U16: pred3d_enc_u16(kind, (const uint16_t *)src, (uint16_t *)res, nx, ny, nz); break;
        case TDC_DT_I32: pred3d_enc_i32(kind, (const int32_t  *)src, (int32_t  *)res, nx, ny, nz); break;
        case TDC_DT_U32: pred3d_enc_u32(kind, (const uint32_t *)src, (uint32_t *)res, nx, ny, nz); break;
        default: break;
    }
}

static void pred3d_decode_sweep(tdc_dtype dt, tdc_pred3d_kind kind,
                                const uint8_t *res,
                                uint8_t *dst,
                                int64_t nx, int64_t ny, int64_t nz) {
    if (tdc_dtype_is_float(dt)) {
        pred3d_decode_float(dt, kind, res, dst, nx, ny, nz);
        return;
    }
    switch (dt) {
        case TDC_DT_I8:  pred3d_dec_i8 (kind, (const int8_t   *)res, (int8_t   *)dst, nx, ny, nz); break;
        case TDC_DT_U8:  pred3d_dec_u8 (kind, (const uint8_t  *)res, (uint8_t  *)dst, nx, ny, nz); break;
        case TDC_DT_I16: pred3d_dec_i16(kind, (const int16_t  *)res, (int16_t  *)dst, nx, ny, nz); break;
        case TDC_DT_U16: pred3d_dec_u16(kind, (const uint16_t *)res, (uint16_t *)dst, nx, ny, nz); break;
        case TDC_DT_I32: pred3d_dec_i32(kind, (const int32_t  *)res, (int32_t  *)dst, nx, ny, nz); break;
        case TDC_DT_U32: pred3d_dec_u32(kind, (const uint32_t *)res, (uint32_t *)dst, nx, ny, nz); break;
        default: break;
    }
}

/* ----- Auto-select (cold path) ------------------------------------------- */
/*
 * Score each kind on a sample prefix and pick argmin sum of |residual|.
 * Uses an int64 reference path that mirrors the per-octant kernel
 * semantics — in particular, PAETH3D on a face slab uses 2D Paeth on
 * the in-bounds triple (a, b, ab) etc., not paeth(a, b, 0, ...). The
 * scoring loop therefore agrees with whatever the kernel will write.
 */

static inline uint64_t pred3d_load_u(tdc_dtype dt, const uint8_t *base, int64_t i) {
    return (uint64_t)tdc_model_load(dt, base, i);
}

/* Unsigned wrap-min absolute difference (matches the helper in pred2d). */
static inline uint64_t pred3d_abs_diff_u(uint64_t a, uint64_t b) {
    uint64_t d = a - b;
    uint64_t nd = (uint64_t)0 - d;
    return (d <= nd) ? d : nd;
}

/* PNG-style Paeth in uint64 wraparound space — same selection rule as
 * paeth64 but stays well-defined for full-range int64 inputs (e.g. float
 * data passed through the ordered mapping). */
static inline uint64_t paeth64_u(uint64_t a, uint64_t b, uint64_t c, uint64_t p) {
    uint64_t pa = pred3d_abs_diff_u(p, a);
    uint64_t pb = pred3d_abs_diff_u(p, b);
    uint64_t pc = pred3d_abs_diff_u(p, c);
    uint64_t r  = (pb <= pc) ? b : c;
    return ((pa <= pb) && (pa <= pc)) ? a : r;
}

/* Per-octant pred matching the typed kernel exactly, in uint64 arithmetic.
 * For inputs that fit in int64 the result is bit-equivalent to the previous
 * signed version. The unsigned average uses a bit-trick that avoids carry
 * overflow; the unsigned 3-way average loses up to 2 ulps of precision when
 * the wrap happens but stays deterministic. */
static uint64_t pred3d_compute_u(tdc_pred3d_kind kind,
                                 uint64_t a, uint64_t b, uint64_t c,
                                 uint64_t ab, uint64_t ac, uint64_t bc, uint64_t abc,
                                 int has_a, int has_b, int has_c) {
    int oct = (has_c << 2) | (has_b << 1) | has_a; /* 0..7 */
    switch (kind) {
        case TDC_PRED3D_LEFT:  return a;
        case TDC_PRED3D_UP:    return b;
        case TDC_PRED3D_FRONT: return c;
        case TDC_PRED3D_AVG3: {
            switch (oct) {
                case 0: return 0;
                case 1: return a;
                case 2: return b;
                case 4: return c;
                case 3: return (a & b) + ((a ^ b) >> 1);
                case 5: return (a & c) + ((a ^ c) >> 1);
                case 6: return (b & c) + ((b ^ c) >> 1);
                case 7: return (a + b + c) / 3u;  /* sum may wrap; deterministic */
                default: return 0;
            }
        }
        case TDC_PRED3D_GRAD3D:
            return a + b + c - ab - ac - bc + abc;
        case TDC_PRED3D_PAETH3D: {
            switch (oct) {
                case 0: return 0;
                case 1: return a;
                case 2: return b;
                case 4: return c;
                case 3: return paeth64_u(a, b, ab, a + b - ab);
                case 5: return paeth64_u(a, c, ac, a + c - ac);
                case 6: return paeth64_u(b, c, bc, b + c - bc);
                case 7: {
                    uint64_t p = a + b + c - ab - ac - bc + abc;
                    return paeth64_u(a, b, c, p);
                }
                default: return 0;
            }
        }
        default: return 0;
    }
}

#define PRED3D_AUTO_SAMPLE 10000

static tdc_pred3d_kind pred3d_auto_select(tdc_dtype dt, const uint8_t *src,
                                          int64_t nx, int64_t ny, int64_t nz) {
    int64_t slab = nx * ny;
    int64_t n    = slab * nz;
    int64_t sample_n = n < PRED3D_AUTO_SAMPLE ? n : PRED3D_AUTO_SAMPLE;

    int64_t sample_rows = sample_n / nx;
    if (sample_rows < 1) sample_rows = 1;
    int64_t total_rows = ny * nz;
    if (sample_rows > total_rows) sample_rows = total_rows;

    tdc_pred3d_kind best_kind = TDC_PRED3D_GRAD3D;
    uint64_t        best_sum  = UINT64_MAX;

    static const tdc_pred3d_kind candidates[6] = {
        TDC_PRED3D_LEFT, TDC_PRED3D_UP, TDC_PRED3D_FRONT,
        TDC_PRED3D_AVG3, TDC_PRED3D_GRAD3D, TDC_PRED3D_PAETH3D
    };

    for (int k = 0; k < 6; ++k) {
        tdc_pred3d_kind kind = candidates[k];
        uint64_t sum = 0;
        for (int64_t row = 0; row < sample_rows; ++row) {
            int64_t z = row / ny;
            int64_t y = row % ny;
            for (int64_t x = 0; x < nx; ++x) {
                int64_t i    = z * slab + y * nx + x;
                uint64_t val = pred3d_load_u(dt, src, i);
                int has_a = x > 0;
                int has_b = y > 0;
                int has_c = z > 0;
                uint64_t a   = has_a               ? pred3d_load_u(dt, src, i - 1)             : 0;
                uint64_t b   = has_b               ? pred3d_load_u(dt, src, i - nx)            : 0;
                uint64_t c   = has_c               ? pred3d_load_u(dt, src, i - slab)          : 0;
                uint64_t ab  = (has_a && has_b)    ? pred3d_load_u(dt, src, i - 1 - nx)        : 0;
                uint64_t ac  = (has_a && has_c)    ? pred3d_load_u(dt, src, i - 1 - slab)      : 0;
                uint64_t bc  = (has_b && has_c)    ? pred3d_load_u(dt, src, i - nx - slab)     : 0;
                uint64_t abc = (has_a && has_b && has_c)
                                                   ? pred3d_load_u(dt, src, i - 1 - nx - slab) : 0;
                uint64_t pred = pred3d_compute_u(kind, a, b, c, ab, ac, bc, abc,
                                                 has_a, has_b, has_c);
                sum += pred3d_abs_diff_u(val, pred);
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

static int pred3d_kind_is_resolved(tdc_pred3d_kind kind) {
    return kind == TDC_PRED3D_LEFT  || kind == TDC_PRED3D_UP     ||
           kind == TDC_PRED3D_FRONT || kind == TDC_PRED3D_AVG3   ||
           kind == TDC_PRED3D_GRAD3D || kind == TDC_PRED3D_PAETH3D;
}

static tdc_status pred3d_encode(const tdc_block *in,
                                const void      *params,
                                tdc_buffer      *residual_out,
                                tdc_dtype       *residual_dtype,
                                tdc_buffer      *side_out) {
    if (!in || !residual_out || !residual_out->realloc_fn) return TDC_E_INVAL;
    if (!side_out || !side_out->realloc_fn)                return TDC_E_INVAL;
    if (in->layout != TDC_LAYOUT_VOLUME_3D) return TDC_E_LAYOUT;
    if (in->shape.rank != 3)                return TDC_E_SHAPE;
    if (!pred3d_dtype_accepted(in->dtype))  return TDC_E_DTYPE;

    int64_t nz = in->shape.dim[0];
    int64_t ny = in->shape.dim[1];
    int64_t nx = in->shape.dim[2];
    if (nx < 0 || ny < 0 || nz < 0) return TDC_E_SHAPE;
    if (nx != 0 && ny != 0 && nx > INT64_MAX / ny)            return TDC_E_SHAPE;
    int64_t slab = nx * ny;
    if (nz != 0 && slab != 0 && slab > INT64_MAX / nz)        return TDC_E_SHAPE;

    size_t elem_size = tdc_dtype_size(in->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    tdc_pred3d_kind kind = TDC_PRED3D_AUTO;
    if (params) {
        const tdc_pred3d_params *p = (const tdc_pred3d_params *)params;
        kind = p->kind;
    }

    int64_t n = slab * nz;
    if (kind == TDC_PRED3D_AUTO) {
        if (n > 0) {
            if (!in->data) return TDC_E_INVAL;
            kind = pred3d_auto_select(in->dtype, (const uint8_t *)in->data, nx, ny, nz);
        } else {
            kind = TDC_PRED3D_GRAD3D;
        }
    } else if (!pred3d_kind_is_resolved(kind)) {
        return TDC_E_INVAL;
    }

    tdc_status st = tdc_buf_reserve(side_out, 1u);
    if (st != TDC_OK) return st;
    side_out->data[0] = (uint8_t)kind;
    side_out->size    = 1u;

    size_t bytes = (size_t)n * elem_size;
    st = tdc_buf_reserve(residual_out, bytes);
    if (st != TDC_OK) return st;

    if (residual_dtype) *residual_dtype = in->dtype;

    if (n == 0) {
        residual_out->size = 0;
        return TDC_OK;
    }

    if (!in->data) return TDC_E_INVAL;

    pred3d_encode_sweep(in->dtype, kind,
                        (const uint8_t *)in->data,
                        residual_out->data,
                        nx, ny, nz);
    residual_out->size = bytes;
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status pred3d_decode(tdc_block      *out,
                                const void     *params,
                                tdc_dtype       residual_dtype,
                                const uint8_t  *residuals, size_t residual_size,
                                const uint8_t  *side_meta, size_t side_size) {
    (void)params;
    if (!out) return TDC_E_INVAL;
    if (out->layout != TDC_LAYOUT_VOLUME_3D) return TDC_E_LAYOUT;
    if (out->shape.rank != 3)                return TDC_E_SHAPE;
    if (residual_dtype != out->dtype)        return TDC_E_DTYPE;
    if (!pred3d_dtype_accepted(out->dtype))  return TDC_E_DTYPE;

    int64_t nz = out->shape.dim[0];
    int64_t ny = out->shape.dim[1];
    int64_t nx = out->shape.dim[2];
    if (nx < 0 || ny < 0 || nz < 0) return TDC_E_SHAPE;
    if (nx != 0 && ny != 0 && nx > INT64_MAX / ny)            return TDC_E_SHAPE;
    int64_t slab = nx * ny;
    if (nz != 0 && slab != 0 && slab > INT64_MAX / nz)        return TDC_E_SHAPE;

    size_t elem_size = tdc_dtype_size(out->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n     = slab * nz;
    size_t  bytes = (size_t)n * elem_size;
    if (residual_size != bytes) return TDC_E_CORRUPT;

    if (side_size != 1u || side_meta == NULL) return TDC_E_CORRUPT;
    tdc_pred3d_kind kind = (tdc_pred3d_kind)side_meta[0];
    if (!pred3d_kind_is_resolved(kind)) return TDC_E_CORRUPT;

    if (n == 0) return TDC_OK;
    if (!out->data || !residuals) return TDC_E_INVAL;

    pred3d_decode_sweep(out->dtype, kind, residuals, (uint8_t *)out->data, nx, ny, nz);
    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_model_vt tdc_model_pred3d_vt = {
    .id               = TDC_MODEL_PRED_3D,
    .name             = "pred3d",
    .accepted_dtypes  = PRED3D_ACCEPTED_DTYPES,
    .accepted_layouts = PRED3D_ACCEPTED_LAYOUTS,
    .encode           = pred3d_encode,
    .decode           = pred3d_decode,
};
