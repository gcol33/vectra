/*
 * src/model/pred3d_float.c
 *
 * Float path for the 3D neighborhood predictor (TDC_MODEL_PRED_3D).
 *
 * Why this is separate from pred3d.c: the integer path is a set of
 * typed, per-dtype macro-expanded kernels with per-octant prologues
 * that the compiler vectorizes. The float path is a single triple-
 * nested loop with per-element octant classification — correct and
 * maintainable but slower, and conceptually disjoint from the integer
 * hot loops. Splitting it out keeps pred3d.c focused on the integer
 * fast path.
 *
 * Shared with pred3d.c via pred3d_internal.h: nothing — the only link
 * is the two entry points pred3d_{encode,decode}_float, forward-
 * declared in pred3d.c and called from the sweep dispatchers there.
 */

#include "tdc/codec.h"
#include "../core/float_order.h"
#include "float_pred_helpers.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* SSE2 detection — same gate as pred3d.c. */
#if defined(__SSE2__) || defined(_M_X64) || \
    (defined(_M_IX86_FP) && _M_IX86_FP >= 2)
#  include <emmintrin.h>
#  define TDC_PRED3D_FLOAT_HAVE_SSE2 1
#endif

/* Float prediction helpers: shared implementation in float_pred_helpers.h. */
#define pred3d_load_ordered           tdc_float_load_ordered
#define pred3d_store_ordered_residual tdc_float_store_ordered_residual
#define pred3d_load_residual          tdc_float_load_residual
#define pred3d_store_float            tdc_float_store_float
#define paeth_ordered_3d              tdc_float_paeth_ordered

/* Per-octant prediction in ordered uint64 space. oct is the bitmask
 * (has_c << 2) | (has_b << 1) | has_a. For GRAD3D and PAETH3D inner
 * box, casts to int64 for the trilinear sum — overflow is theoretical
 * only (would require neighboring voxels spanning the full float64 range). */
static inline uint64_t pred3d_float_compute(tdc_pred3d_kind kind,
                                            uint64_t a, uint64_t b, uint64_t c,
                                            uint64_t ab, uint64_t ac,
                                            uint64_t bc, uint64_t abc,
                                            int oct) {
    switch (kind) {
        case TDC_PRED3D_LEFT:  return a;
        case TDC_PRED3D_UP:    return b;
        case TDC_PRED3D_FRONT: return c;
        case TDC_PRED3D_AVG3:
            switch (oct) {
                case 0: return 0;
                case 1: return a;
                case 2: return b;
                case 4: return c;
                case 3: return (a + b) / 2u;
                case 5: return (a + c) / 2u;
                case 6: return (b + c) / 2u;
                case 7: return (a + b + c) / 3u;
                default: return 0;
            }
        case TDC_PRED3D_GRAD3D:
            /* Trilinear extrapolation in unsigned wraparound arithmetic;
             * stays well-defined for float-ordered values that span the
             * full int64 range. Bit-pattern equivalent to the previous
             * signed version for inputs that fit in int64. */
            return a + b + c - ab - ac - bc + abc;
        case TDC_PRED3D_PAETH3D:
            switch (oct) {
                case 0: return 0;
                case 1: return a;
                case 2: return b;
                case 4: return c;
                case 3: return paeth_ordered_3d(a, b, ab);
                case 5: return paeth_ordered_3d(a, c, ac);
                case 6: return paeth_ordered_3d(b, c, bc);
                case 7: {
                    /* Same selection rule as the inner-box Paeth (PNG-style),
                     * but in uint64 wraparound arithmetic so float-ordered
                     * extremes don't trigger UBSAN signed overflow. */
                    uint64_t p   = a + b + c - ab - ac - bc + abc;
                    uint64_t pa_d = p - a, pa_n = a - p;
                    uint64_t pb_d = p - b, pb_n = b - p;
                    uint64_t pc_d = p - c, pc_n = c - p;
                    uint64_t pa = (pa_d <= pa_n) ? pa_d : pa_n;
                    uint64_t pb = (pb_d <= pb_n) ? pb_d : pb_n;
                    uint64_t pc = (pc_d <= pc_n) ? pc_d : pc_n;
                    if (pa <= pb && pa <= pc) return a;
                    if (pb <= pc) return b;
                    return c;
                }
                default: return 0;
            }
        default: return 0;
    }
}

/* ----- f32 SSE2 row helpers --------------------------------------------- *
 *
 * The ordered-int mapping (float_order.h) maps f32 bits to a uint32 that
 * preserves numerical order. The encode residual r = ord(val) - ord(pred)
 * is element-width modular, so SIMDing at 32-bit lanes is bit-equivalent
 * to the scalar path.  Only f32 is done here; f64 would require
 * _mm_srai_epi64 (not in SSE2), and the f64 pred3d rows are not on the
 * hot-list in todo.md anyway.
 */
#ifdef TDC_PRED3D_FLOAT_HAVE_SSE2

/* bits → ordered(bits): ord = bits ^ (srai(bits, 31) | 0x80000000). */
static inline __m128i f32_ord_4x(__m128i bits) {
    __m128i sign = _mm_srai_epi32(bits, 31);
    __m128i hi   = _mm_set1_epi32((int)0x80000000);
    return _mm_xor_si128(bits, _mm_or_si128(sign, hi));
}

/* r[i] = ord(s[i]) - ord(prev[i]), raw uint32 store. */
static void pred3d_f32_sub_row_sse2(uint8_t *dst, const uint8_t *s,
                                    const uint8_t *prev, int64_t n) {
    int64_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m128i vs = f32_ord_4x(_mm_loadu_si128((const __m128i *)(s    + i*4)));
        __m128i vp = f32_ord_4x(_mm_loadu_si128((const __m128i *)(prev + i*4)));
        _mm_storeu_si128((__m128i *)(dst + i*4), _mm_sub_epi32(vs, vp));
    }
    for (; i < n; ++i) {
        uint32_t vs, vp;
        memcpy(&vs, s    + i*4, 4);
        memcpy(&vp, prev + i*4, 4);
        uint32_t r = tdc_f32_to_ordered(vs) - tdc_f32_to_ordered(vp);
        memcpy(dst + i*4, &r, 4);
    }
}

/* GRAD3D O8 inner-box encode, one row of (nx - 1) elements starting at x=1.
 * Pointers are element-0 of the row for each neighbor plane:
 *   s   = (z,   y  ), sy  = (z,   y-1),
 *   sz  = (z-1, y  ), syz = (z-1, y-1).
 */
static void pred3d_f32_grad3d_enc_row_sse2(
    uint8_t *dst, const uint8_t *s, const uint8_t *sy,
    const uint8_t *sz, const uint8_t *syz, int64_t nx) {
    if (nx < 2) return;
    const int64_t n = nx - 1;
    const uint8_t *p_s   = s   + 4, *p_sm   = s;
    const uint8_t *p_sy  = sy  + 4, *p_sym  = sy;
    const uint8_t *p_sz  = sz  + 4, *p_szm  = sz;
    const uint8_t *p_syz = syz + 4, *p_syzm = syz;
    uint8_t *rp = dst + 4;
    int64_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m128i vs   = f32_ord_4x(_mm_loadu_si128((const __m128i*)(p_s   + i*4)));
        __m128i vsm  = f32_ord_4x(_mm_loadu_si128((const __m128i*)(p_sm  + i*4)));
        __m128i vsy  = f32_ord_4x(_mm_loadu_si128((const __m128i*)(p_sy  + i*4)));
        __m128i vsym = f32_ord_4x(_mm_loadu_si128((const __m128i*)(p_sym + i*4)));
        __m128i vsz  = f32_ord_4x(_mm_loadu_si128((const __m128i*)(p_sz  + i*4)));
        __m128i vszm = f32_ord_4x(_mm_loadu_si128((const __m128i*)(p_szm + i*4)));
        __m128i vsyz = f32_ord_4x(_mm_loadu_si128((const __m128i*)(p_syz + i*4)));
        __m128i vsyzm= f32_ord_4x(_mm_loadu_si128((const __m128i*)(p_syzm+ i*4)));
        __m128i t = _mm_sub_epi32(vs, vsm);
        t = _mm_sub_epi32(t, vsy);
        t = _mm_sub_epi32(t, vsz);
        t = _mm_add_epi32(t, vsym);
        t = _mm_add_epi32(t, vszm);
        t = _mm_add_epi32(t, vsyz);
        t = _mm_sub_epi32(t, vsyzm);
        _mm_storeu_si128((__m128i *)(rp + i*4), t);
    }
    for (; i < n; ++i) {
        uint32_t b0,b1,b2,b3,b4,b5,b6,b7;
        memcpy(&b0, p_s   + i*4, 4); memcpy(&b1, p_sm   + i*4, 4);
        memcpy(&b2, p_sy  + i*4, 4); memcpy(&b3, p_sym  + i*4, 4);
        memcpy(&b4, p_sz  + i*4, 4); memcpy(&b5, p_szm  + i*4, 4);
        memcpy(&b6, p_syz + i*4, 4); memcpy(&b7, p_syzm + i*4, 4);
        uint32_t val = tdc_f32_to_ordered(b0), a  = tdc_f32_to_ordered(b1);
        uint32_t b   = tdc_f32_to_ordered(b2), ab = tdc_f32_to_ordered(b3);
        uint32_t c   = tdc_f32_to_ordered(b4), ac = tdc_f32_to_ordered(b5);
        uint32_t bc  = tdc_f32_to_ordered(b6), abc= tdc_f32_to_ordered(b7);
        uint32_t r   = val - a - b - c + ab + ac + bc - abc;
        memcpy(rp + i*4, &r, 4);
    }
}

#define PRED3D_F32_SUB_ROW  pred3d_f32_sub_row_sse2
#define PRED3D_F32_GRAD_ROW pred3d_f32_grad3d_enc_row_sse2

#else /* !TDC_PRED3D_FLOAT_HAVE_SSE2 */

static void pred3d_f32_sub_row_scalar(uint8_t *dst, const uint8_t *s,
                                      const uint8_t *prev, int64_t n) {
    for (int64_t i = 0; i < n; ++i) {
        uint32_t vs, vp;
        memcpy(&vs, s    + i*4, 4);
        memcpy(&vp, prev + i*4, 4);
        uint32_t r = tdc_f32_to_ordered(vs) - tdc_f32_to_ordered(vp);
        memcpy(dst + i*4, &r, 4);
    }
}

static void pred3d_f32_grad3d_enc_row_scalar(
    uint8_t *dst, const uint8_t *s, const uint8_t *sy,
    const uint8_t *sz, const uint8_t *syz, int64_t nx) {
    for (int64_t x = 1; x < nx; ++x) {
        uint32_t b0,b1,b2,b3,b4,b5,b6,b7;
        memcpy(&b0, s   + x*4,     4); memcpy(&b1, s   + (x-1)*4, 4);
        memcpy(&b2, sy  + x*4,     4); memcpy(&b3, sy  + (x-1)*4, 4);
        memcpy(&b4, sz  + x*4,     4); memcpy(&b5, sz  + (x-1)*4, 4);
        memcpy(&b6, syz + x*4,     4); memcpy(&b7, syz + (x-1)*4, 4);
        uint32_t val = tdc_f32_to_ordered(b0), a  = tdc_f32_to_ordered(b1);
        uint32_t b   = tdc_f32_to_ordered(b2), ab = tdc_f32_to_ordered(b3);
        uint32_t c   = tdc_f32_to_ordered(b4), ac = tdc_f32_to_ordered(b5);
        uint32_t bc  = tdc_f32_to_ordered(b6), abc= tdc_f32_to_ordered(b7);
        uint32_t r   = val - a - b - c + ab + ac + bc - abc;
        memcpy(dst + x*4, &r, 4);
    }
}

#define PRED3D_F32_SUB_ROW  pred3d_f32_sub_row_scalar
#define PRED3D_F32_GRAD_ROW pred3d_f32_grad3d_enc_row_scalar

#endif /* TDC_PRED3D_FLOAT_HAVE_SSE2 */

/* ----- f32 specialized encode paths ------------------------------------- *
 *
 * Each of these mirrors the per-octant decomposition in pred3d.c's typed
 * kernels, with SIMD-able rows delegated to the helpers above.
 */

static void pred3d_enc_f32_left(const uint8_t *src, uint8_t *res,
                                int64_t nx, int64_t ny, int64_t nz) {
    const int64_t slab = nx * ny;
    for (int64_t z = 0; z < nz; ++z) {
        for (int64_t y = 0; y < ny; ++y) {
            const uint8_t *s = src + (z * slab + y * nx) * 4u;
            uint8_t       *r = res + (z * slab + y * nx) * 4u;
            uint32_t b; memcpy(&b, s, 4);
            uint32_t o = tdc_f32_to_ordered(b);
            memcpy(r, &o, 4);
            if (nx > 1) PRED3D_F32_SUB_ROW(r + 4, s + 4, s, nx - 1);
        }
    }
}

static void pred3d_enc_f32_up(const uint8_t *src, uint8_t *res,
                              int64_t nx, int64_t ny, int64_t nz) {
    const int64_t slab = nx * ny;
    for (int64_t z = 0; z < nz; ++z) {
        {
            const uint8_t *s = src + z * slab * 4u;
            uint8_t       *r = res + z * slab * 4u;
            for (int64_t x = 0; x < nx; ++x) {
                uint32_t b; memcpy(&b, s + x*4, 4);
                uint32_t o = tdc_f32_to_ordered(b);
                memcpy(r + x*4, &o, 4);
            }
        }
        for (int64_t y = 1; y < ny; ++y) {
            const uint8_t *s  = src + (z * slab + y       * nx) * 4u;
            const uint8_t *sy = src + (z * slab + (y - 1) * nx) * 4u;
            uint8_t       *r  = res + (z * slab + y       * nx) * 4u;
            PRED3D_F32_SUB_ROW(r, s, sy, nx);
        }
    }
}

static void pred3d_enc_f32_front(const uint8_t *src, uint8_t *res,
                                 int64_t nx, int64_t ny, int64_t nz) {
    const int64_t slab = nx * ny;
    for (int64_t i = 0; i < slab; ++i) {
        uint32_t b; memcpy(&b, src + i*4, 4);
        uint32_t o = tdc_f32_to_ordered(b);
        memcpy(res + i*4, &o, 4);
    }
    for (int64_t z = 1; z < nz; ++z) {
        const uint8_t *s  = src + z       * slab * 4u;
        const uint8_t *sz = src + (z - 1) * slab * 4u;
        uint8_t       *r  = res + z       * slab * 4u;
        PRED3D_F32_SUB_ROW(r, s, sz, slab);
    }
}

static inline uint32_t pred3d_f32_ord_at(const uint8_t *p) {
    uint32_t b; memcpy(&b, p, 4);
    return tdc_f32_to_ordered(b);
}

static void pred3d_enc_f32_grad3d(const uint8_t *src, uint8_t *res,
                                  int64_t nx, int64_t ny, int64_t nz) {
    const int64_t slab = nx * ny;

    /* O1: (0,0,0), pred = 0 */
    {
        uint32_t o = pred3d_f32_ord_at(src);
        memcpy(res, &o, 4);
    }
    /* O2: (0,0,x>=1), pred = a */
    for (int64_t x = 1; x < nx; ++x) {
        uint32_t r = pred3d_f32_ord_at(src + x*4) - pred3d_f32_ord_at(src + (x-1)*4);
        memcpy(res + x*4, &r, 4);
    }
    /* O3: (0,y>=1,0), pred = b */
    for (int64_t y = 1; y < ny; ++y) {
        const uint8_t *s  = src + y       * nx * 4u;
        const uint8_t *sy = src + (y - 1) * nx * 4u;
        uint8_t       *r  = res + y       * nx * 4u;
        uint32_t rs = pred3d_f32_ord_at(s) - pred3d_f32_ord_at(sy);
        memcpy(r, &rs, 4);
    }
    /* O5: (z>=1,0,0), pred = c */
    for (int64_t z = 1; z < nz; ++z) {
        const uint8_t *s  = src + z       * slab * 4u;
        const uint8_t *sz = src + (z - 1) * slab * 4u;
        uint8_t       *r  = res + z       * slab * 4u;
        uint32_t rs = pred3d_f32_ord_at(s) - pred3d_f32_ord_at(sz);
        memcpy(r, &rs, 4);
    }
    /* O4: (0, y>=1, x>=1), pred = a + b - ab  (2D plane on z=0) */
    for (int64_t y = 1; y < ny; ++y) {
        const uint8_t *s  = src + y       * nx * 4u;
        const uint8_t *sy = src + (y - 1) * nx * 4u;
        uint8_t       *r  = res + y       * nx * 4u;
        for (int64_t x = 1; x < nx; ++x) {
            uint32_t val = pred3d_f32_ord_at(s  + x*4);
            uint32_t a   = pred3d_f32_ord_at(s  + (x-1)*4);
            uint32_t b   = pred3d_f32_ord_at(sy + x*4);
            uint32_t ab  = pred3d_f32_ord_at(sy + (x-1)*4);
            uint32_t rs  = val - (a + b - ab);
            memcpy(r + x*4, &rs, 4);
        }
    }
    /* O6: (z>=1, 0, x>=1), pred = a + c - ac  (2D plane on y=0) */
    for (int64_t z = 1; z < nz; ++z) {
        const uint8_t *s  = src + z       * slab * 4u;
        const uint8_t *sz = src + (z - 1) * slab * 4u;
        uint8_t       *r  = res + z       * slab * 4u;
        for (int64_t x = 1; x < nx; ++x) {
            uint32_t val = pred3d_f32_ord_at(s  + x*4);
            uint32_t a   = pred3d_f32_ord_at(s  + (x-1)*4);
            uint32_t c   = pred3d_f32_ord_at(sz + x*4);
            uint32_t ac  = pred3d_f32_ord_at(sz + (x-1)*4);
            uint32_t rs  = val - (a + c - ac);
            memcpy(r + x*4, &rs, 4);
        }
    }
    /* O7: (z>=1, y>=1, 0), pred = b + c - bc */
    for (int64_t z = 1; z < nz; ++z) {
        for (int64_t y = 1; y < ny; ++y) {
            const uint8_t *s   = src + (z       * slab + y       * nx) * 4u;
            const uint8_t *sy  = src + (z       * slab + (y - 1) * nx) * 4u;
            const uint8_t *sz  = src + ((z - 1) * slab + y       * nx) * 4u;
            const uint8_t *syz = src + ((z - 1) * slab + (y - 1) * nx) * 4u;
            uint8_t       *r   = res + (z       * slab + y       * nx) * 4u;
            uint32_t val = pred3d_f32_ord_at(s);
            uint32_t b   = pred3d_f32_ord_at(sy);
            uint32_t c   = pred3d_f32_ord_at(sz);
            uint32_t bc  = pred3d_f32_ord_at(syz);
            uint32_t rs  = val - (b + c - bc);
            memcpy(r, &rs, 4);
        }
    }
    /* O8 inner box: pred = a + b + c - ab - ac - bc + abc */
    for (int64_t z = 1; z < nz; ++z) {
        for (int64_t y = 1; y < ny; ++y) {
            const uint8_t *s   = src + (z       * slab + y       * nx) * 4u;
            const uint8_t *sy  = src + (z       * slab + (y - 1) * nx) * 4u;
            const uint8_t *sz  = src + ((z - 1) * slab + y       * nx) * 4u;
            const uint8_t *syz = src + ((z - 1) * slab + (y - 1) * nx) * 4u;
            uint8_t       *r   = res + (z       * slab + y       * nx) * 4u;
            PRED3D_F32_GRAD_ROW(r, s, sy, sz, syz, nx);
        }
    }
}

void pred3d_encode_float(tdc_dtype dt, tdc_pred3d_kind kind,
                         const uint8_t *src, uint8_t *res,
                         int64_t nx, int64_t ny, int64_t nz) {
    if (nx <= 0 || ny <= 0 || nz <= 0) return;

    if (dt == TDC_DT_F32) {
        switch (kind) {
            case TDC_PRED3D_LEFT:   pred3d_enc_f32_left  (src, res, nx, ny, nz); return;
            case TDC_PRED3D_UP:     pred3d_enc_f32_up    (src, res, nx, ny, nz); return;
            case TDC_PRED3D_FRONT:  pred3d_enc_f32_front (src, res, nx, ny, nz); return;
            case TDC_PRED3D_GRAD3D: pred3d_enc_f32_grad3d(src, res, nx, ny, nz); return;
            default: break;
        }
    }

    const int64_t slab = nx * ny;

    for (int64_t z = 0; z < nz; ++z) {
        for (int64_t y = 0; y < ny; ++y) {
            for (int64_t x = 0; x < nx; ++x) {
                int64_t i = z * slab + y * nx + x;
                uint64_t val = pred3d_load_ordered(dt, src, i);

                int has_a = x > 0, has_b = y > 0, has_c = z > 0;
                int oct = (has_c << 2) | (has_b << 1) | has_a;
                uint64_t a   = has_a               ? pred3d_load_ordered(dt, src, i - 1)             : 0;
                uint64_t b   = has_b               ? pred3d_load_ordered(dt, src, i - nx)            : 0;
                uint64_t c   = has_c               ? pred3d_load_ordered(dt, src, i - slab)          : 0;
                uint64_t ab  = (has_a && has_b)    ? pred3d_load_ordered(dt, src, i - 1 - nx)        : 0;
                uint64_t ac  = (has_a && has_c)    ? pred3d_load_ordered(dt, src, i - 1 - slab)      : 0;
                uint64_t bc  = (has_b && has_c)    ? pred3d_load_ordered(dt, src, i - nx - slab)     : 0;
                uint64_t abc = (has_a && has_b && has_c)
                                                   ? pred3d_load_ordered(dt, src, i - 1 - nx - slab) : 0;

                uint64_t pred = pred3d_float_compute(kind, a, b, c,
                                                     ab, ac, bc, abc, oct);
                pred3d_store_ordered_residual(dt, res, i, val - pred);
            }
        }
    }
}

void pred3d_decode_float(tdc_dtype dt, tdc_pred3d_kind kind,
                         const uint8_t *res, uint8_t *dst,
                         int64_t nx, int64_t ny, int64_t nz) {
    if (nx <= 0 || ny <= 0 || nz <= 0) return;
    const int64_t slab = nx * ny;

    for (int64_t z = 0; z < nz; ++z) {
        for (int64_t y = 0; y < ny; ++y) {
            for (int64_t x = 0; x < nx; ++x) {
                int64_t i = z * slab + y * nx + x;
                uint64_t rv = pred3d_load_residual(dt, res, i);

                int has_a = x > 0, has_b = y > 0, has_c = z > 0;
                int oct = (has_c << 2) | (has_b << 1) | has_a;
                uint64_t a   = has_a               ? pred3d_load_ordered(dt, dst, i - 1)             : 0;
                uint64_t b   = has_b               ? pred3d_load_ordered(dt, dst, i - nx)            : 0;
                uint64_t c   = has_c               ? pred3d_load_ordered(dt, dst, i - slab)          : 0;
                uint64_t ab  = (has_a && has_b)    ? pred3d_load_ordered(dt, dst, i - 1 - nx)        : 0;
                uint64_t ac  = (has_a && has_c)    ? pred3d_load_ordered(dt, dst, i - 1 - slab)      : 0;
                uint64_t bc  = (has_b && has_c)    ? pred3d_load_ordered(dt, dst, i - nx - slab)     : 0;
                uint64_t abc = (has_a && has_b && has_c)
                                                   ? pred3d_load_ordered(dt, dst, i - 1 - nx - slab) : 0;

                uint64_t pred = pred3d_float_compute(kind, a, b, c,
                                                     ab, ac, bc, abc, oct);
                pred3d_store_float(dt, dst, i, rv + pred);
            }
        }
    }
}
