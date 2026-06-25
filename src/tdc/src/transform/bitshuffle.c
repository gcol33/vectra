/*
 * src/transform/bitshuffle.c
 *
 * TDC_XFORM_BIT_SHUFFLE — transpose by bit lane instead of byte lane.
 *
 * For N elements of elem_size bytes each, the output is organized as
 * (elem_size * 8) bit-planes, each plane holding ceil(N/8) packed bytes.
 * This groups same-significance bits together: after quantization to a
 * narrow integer type, the high-order bits are mostly zero and bit-
 * shuffling makes them collapse into long runs of 0s, dramatically
 * improving subsequent entropy coding.
 *
 * Output size: n_planes * bytes_per_plane, where
 *   n_planes      = elem_size * 8
 *   bytes_per_plane = (n_elems + 7) / 8
 * This may be slightly larger than src_size when n_elems is not a
 * multiple of 8. The excess bits in the last byte of each plane are
 * zeroed on encode and ignored on decode.
 *
 * Scalar implementation. No SIMD in v0 — the scalar path is
 * straightforward and correct; SIMD bit-transpose (e.g. via
 * movemask) is a future optimization.
 */

#include "tdc/transform.h"
#include "transform_internal.h"
#include "../core/buffer.h"
#include "../core/simd.h"

#include <string.h>
#include <stdint.h>
#include <stddef.h>

/* ----- SSE2 bit-shuffle / unshuffle -------------------------------------- */

#if TDC_HAVE_SSE2

/*
 * SSE2 bit-shuffle: process 16 elements at a time.
 *
 * For each byte-lane k within elem_size, gather the 16 bytes from
 * elements e..e+15, then use AND + cmpeq + movemask to extract each
 * of the 8 bit-planes into 2 packed output bytes.  This replaces
 * 16 * 8 bit-extracts with 8 * (AND + cmpeq + movemask) per lane.
 */
static void bitshuffle_scatter_sse2(uint8_t *dst, const uint8_t *src,
                                     uint32_t n_elems, uint32_t elem_size) {
    uint32_t n_planes = elem_size * 8u;
    uint32_t bpp = (n_elems + 7u) / 8u;
    memset(dst, 0, (size_t)n_planes * bpp);

    uint32_t e = 0;

    for (; e + 16 <= n_elems; e += 16) {
        uint32_t out_byte = e / 8u;

        for (uint32_t k = 0; k < elem_size; ++k) {
            /* Gather 16 bytes from lane k. */
            uint8_t tmp[16];
            for (int j = 0; j < 16; ++j)
                tmp[j] = src[(size_t)(e + (uint32_t)j) * elem_size + k];
            __m128i v = _mm_loadu_si128((const __m128i *)tmp);

            /* Extract 8 bit-planes. For each bit position, isolate it
             * with AND, then cmpeq to promote to MSB, then movemask. */
            for (uint32_t bit = 0; bit < 8u; ++bit) {
                __m128i bit_mask = _mm_set1_epi8((char)(1u << bit));
                __m128i isolated = _mm_and_si128(v, bit_mask);
                __m128i cmp      = _mm_cmpeq_epi8(isolated, bit_mask);
                int mask = _mm_movemask_epi8(cmp);
                uint32_t plane = k * 8u + bit;
                dst[(size_t)plane * bpp + out_byte]     = (uint8_t)(mask & 0xFF);
                dst[(size_t)plane * bpp + out_byte + 1] = (uint8_t)((mask >> 8) & 0xFF);
            }
        }
    }

    /* Scalar tail. */
    for (; e < n_elems; ++e) {
        uint32_t out_byte_in_plane = e / 8u;
        uint32_t out_bit           = e % 8u;
        for (uint32_t b = 0; b < elem_size; ++b) {
            uint8_t byte_val = src[(size_t)e * elem_size + b];
            for (uint32_t bit = 0; bit < 8u; ++bit) {
                if (byte_val & (1u << bit)) {
                    uint32_t plane = b * 8u + bit;
                    dst[(size_t)plane * bpp + out_byte_in_plane] |=
                        (uint8_t)(1u << out_bit);
                }
            }
        }
    }
}

/*
 * SSE2 bit-unshuffle: reconstruct 16 elements at a time from bit-planes.
 *
 * For each byte-lane k, read 2 packed bytes from each of the 8 planes,
 * expand the 16-bit mask to a 16-byte vector, AND with (1 << bit) to
 * produce the contribution for that bit, and OR into an accumulator.
 */

/* Helper: expand a 16-bit mask to a 16-byte vector where byte j is
 * 0xFF if bit j of the mask is set, 0x00 otherwise. Pure SSE2. */
static inline __m128i bshuf_expand_mask16(uint16_t packed) {
    /* Byte select constants: bytes 0..7 test against the low byte of
     * packed, bytes 8..15 test against the high byte. */
    static const uint8_t TDC_BIT_MASKS[8] = {
        0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80
    };
    __m128i mask_vec = _mm_set_epi8(
        (char)TDC_BIT_MASKS[7], (char)TDC_BIT_MASKS[6],
        (char)TDC_BIT_MASKS[5], (char)TDC_BIT_MASKS[4],
        (char)TDC_BIT_MASKS[3], (char)TDC_BIT_MASKS[2],
        (char)TDC_BIT_MASKS[1], (char)TDC_BIT_MASKS[0],
        (char)TDC_BIT_MASKS[7], (char)TDC_BIT_MASKS[6],
        (char)TDC_BIT_MASKS[5], (char)TDC_BIT_MASKS[4],
        (char)TDC_BIT_MASKS[3], (char)TDC_BIT_MASKS[2],
        (char)TDC_BIT_MASKS[1], (char)TDC_BIT_MASKS[0]);
    /* Broadcast low byte to 0..7, high byte to 8..15.
     * _mm_unpacklo_epi64 of two set1 vectors gives exactly this layout. */
    __m128i lo8 = _mm_set1_epi8((char)(packed & 0xFF));
    __m128i hi8 = _mm_set1_epi8((char)((packed >> 8) & 0xFF));
    __m128i bcast = _mm_unpacklo_epi64(lo8, hi8);
    __m128i masked = _mm_and_si128(bcast, mask_vec);
    return _mm_cmpeq_epi8(masked, mask_vec);
}

static void bitshuffle_gather_sse2(uint8_t *dst, const uint8_t *src,
                                    uint32_t n_elems, uint32_t elem_size) {
    uint32_t n_planes = elem_size * 8u;
    uint32_t bpp = (n_elems + 7u) / 8u;
    memset(dst, 0, (size_t)n_elems * elem_size);

    uint32_t e = 0;

    for (; e + 16 <= n_elems; e += 16) {
        uint32_t in_byte = e / 8u;

        for (uint32_t k = 0; k < elem_size; ++k) {
            __m128i accum = _mm_setzero_si128();

            for (uint32_t bit = 0; bit < 8u; ++bit) {
                uint32_t plane = k * 8u + bit;
                uint16_t packed;
                memcpy(&packed, src + (size_t)plane * bpp + in_byte, 2);

                /* Expand to 16 bytes of 0xFF/0x00, then mask to (1 << bit). */
                __m128i expanded = bshuf_expand_mask16(packed);
                __m128i contribution = _mm_and_si128(
                    expanded, _mm_set1_epi8((char)(1u << bit)));
                accum = _mm_or_si128(accum, contribution);
            }

            /* Scatter the 16 result bytes back to strided element positions. */
            uint8_t result[16];
            _mm_storeu_si128((__m128i *)result, accum);
            for (int j = 0; j < 16; ++j)
                dst[(size_t)(e + (uint32_t)j) * elem_size + k] = result[j];
        }
    }

    /* Scalar tail. */
    for (; e < n_elems; ++e) {
        uint32_t in_byte_in_plane = e / 8u;
        uint32_t in_bit           = e % 8u;
        for (uint32_t plane = 0; plane < n_planes; ++plane) {
            if (src[(size_t)plane * bpp + in_byte_in_plane] & (1u << in_bit)) {
                uint32_t b   = plane / 8u;
                uint32_t bit = plane % 8u;
                dst[(size_t)e * elem_size + b] |= (uint8_t)(1u << bit);
            }
        }
    }
}

#endif /* TDC_HAVE_SSE2 */

/* ----- Scalar bit-shuffle / unshuffle ------------------------------------ */

/*
 * Bit-shuffle: scatter element bits into bit-planes.
 *
 * For each element, for each bit position in the element, set the
 * corresponding bit in the output plane. Output is organized as:
 *   plane[p] occupies dst[p * bpp .. (p+1) * bpp), where bpp = bytes_per_plane
 *   Within a plane, bit i of element e is stored at
 *   byte offset (e / 8), bit offset (e % 8).
 */
static void bitshuffle_scatter(uint8_t *dst, const uint8_t *src,
                               uint32_t n_elems, uint32_t elem_size) {
    uint32_t n_planes = elem_size * 8u;
    uint32_t bpp = (n_elems + 7u) / 8u; /* bytes per plane */
    size_t out_size = (size_t)n_planes * bpp;

    /* Zero output: tail bits in partial final bytes must be 0. */
    memset(dst, 0, out_size);

    for (uint32_t e = 0; e < n_elems; ++e) {
        uint32_t out_byte_in_plane = e / 8u;
        uint32_t out_bit           = e % 8u;

        for (uint32_t b = 0; b < elem_size; ++b) {
            uint8_t byte_val = src[(size_t)e * elem_size + b];
            for (uint32_t bit = 0; bit < 8u; ++bit) {
                if (byte_val & (1u << bit)) {
                    uint32_t plane = b * 8u + bit;
                    dst[(size_t)plane * bpp + out_byte_in_plane] |=
                        (uint8_t)(1u << out_bit);
                }
            }
        }
    }
}

/*
 * Bit-unshuffle: gather element bits from bit-planes.
 */
static void bitshuffle_gather(uint8_t *dst, const uint8_t *src,
                              uint32_t n_elems, uint32_t elem_size) {
    uint32_t n_planes = elem_size * 8u;
    uint32_t bpp = (n_elems + 7u) / 8u;

    /* Zero output in case of partial reads. */
    memset(dst, 0, (size_t)n_elems * elem_size);

    for (uint32_t e = 0; e < n_elems; ++e) {
        uint32_t in_byte_in_plane = e / 8u;
        uint32_t in_bit           = e % 8u;

        for (uint32_t plane = 0; plane < n_planes; ++plane) {
            if (src[(size_t)plane * bpp + in_byte_in_plane] & (1u << in_bit)) {
                uint32_t b   = plane / 8u;
                uint32_t bit = plane % 8u;
                dst[(size_t)e * elem_size + b] |= (uint8_t)(1u << bit);
            }
        }
    }
}

/* ----- Vtable hooks ------------------------------------------------------- */

#define BITSHUFFLE_DTYPE_BIT(dt) (1u << (uint32_t)(dt))
#define BITSHUFFLE_ACCEPTED_DTYPES (                    \
    BITSHUFFLE_DTYPE_BIT(TDC_DT_I8)  |                  \
    BITSHUFFLE_DTYPE_BIT(TDC_DT_I16) |                  \
    BITSHUFFLE_DTYPE_BIT(TDC_DT_I32) |                  \
    BITSHUFFLE_DTYPE_BIT(TDC_DT_I64) |                  \
    BITSHUFFLE_DTYPE_BIT(TDC_DT_U8)  |                  \
    BITSHUFFLE_DTYPE_BIT(TDC_DT_U16) |                  \
    BITSHUFFLE_DTYPE_BIT(TDC_DT_U32) |                  \
    BITSHUFFLE_DTYPE_BIT(TDC_DT_U64) |                  \
    BITSHUFFLE_DTYPE_BIT(TDC_DT_F16) |                  \
    BITSHUFFLE_DTYPE_BIT(TDC_DT_F32) |                  \
    BITSHUFFLE_DTYPE_BIT(TDC_DT_F64))

static tdc_status bitshuffle_encode(const uint8_t *src, size_t src_size,
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

    if (out_dtype) *out_dtype = in_dtype;

    if (src_size == 0) {
        dst->size = 0;
        return TDC_OK;
    }

    size_t n_elems_sz = src_size / elem_size;
    if (n_elems_sz > UINT32_MAX) return TDC_E_INVAL;
    uint32_t n_elems = (uint32_t)n_elems_sz;

    /* elem_size 1: bit-shuffle of single-byte elements. */
    uint32_t n_planes = (uint32_t)elem_size * 8u;
    uint32_t bpp = (n_elems + 7u) / 8u;
    size_t out_size = (size_t)n_planes * bpp;

    tdc_status st = tdc_buf_reserve(dst, out_size);
    if (st != TDC_OK) return st;

#if TDC_HAVE_SSE2
    bitshuffle_scatter_sse2(dst->data, src, n_elems, (uint32_t)elem_size);
#else
    bitshuffle_scatter(dst->data, src, n_elems, (uint32_t)elem_size);
#endif
    dst->size = out_size;
    return TDC_OK;
}

static tdc_status bitshuffle_decode(const uint8_t *src, size_t src_size,
                                    tdc_dtype      in_dtype,
                                    const void    *params,
                                    uint8_t       *dst, size_t dst_size,
                                    tdc_dtype     *out_dtype) {
    (void)params;
    if (src_size > 0 && !src) return TDC_E_INVAL;
    if (dst_size > 0 && !dst) return TDC_E_INVAL;

    size_t elem_size = tdc_dtype_size(in_dtype);
    if (elem_size == 0) return TDC_E_DTYPE;
    if (dst_size % elem_size != 0) return TDC_E_CORRUPT;

    if (out_dtype) *out_dtype = in_dtype;

    if (dst_size == 0) return TDC_OK;

    size_t n_elems_sz = dst_size / elem_size;
    if (n_elems_sz > UINT32_MAX) return TDC_E_INVAL;
    uint32_t n_elems = (uint32_t)n_elems_sz;

    /* Validate that src_size matches the expected bit-shuffled size. */
    uint32_t n_planes = (uint32_t)elem_size * 8u;
    uint32_t bpp = (n_elems + 7u) / 8u;
    size_t expected_src = (size_t)n_planes * bpp;
    if (src_size != expected_src) return TDC_E_CORRUPT;

#if TDC_HAVE_SSE2
    bitshuffle_gather_sse2(dst, src, n_elems, (uint32_t)elem_size);
#else
    bitshuffle_gather(dst, src, n_elems, (uint32_t)elem_size);
#endif
    return TDC_OK;
}

const tdc_xform_vt tdc_xform_bit_shuffle_vt = {
    .id              = TDC_XFORM_BIT_SHUFFLE,
    .name            = "bit_shuffle",
    .accepted_dtypes = BITSHUFFLE_ACCEPTED_DTYPES,
    .can_inplace     = 0,
    .is_lossy        = 0,
    .encode          = bitshuffle_encode,
    .decode          = bitshuffle_decode,
};
