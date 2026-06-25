/*
 * src/entropy/huffman_internal.h
 *
 * Shared primitives for the canonical Huffman entropy coders:
 *   - src/entropy/huffman.c    single-stream (TDC_ENTROPY_HUFFMAN)
 *   - src/entropy/huffman4.c   4-stream interleaved (TDC_ENTROPY_HUFFMAN4)
 *
 * The constants, bit-writer, bit-reader, and tree-building helpers are
 * identical for both coders. Inline helpers live in this header so each
 * TU can keep them in registers; the non-inline shared functions
 * (length computation, canonical code assignment, histogram, and the
 * full single-stream encode/decode used as the small-input fallback
 * from huffman4) are declared here and defined in huffman.c.
 *
 * Not part of the public ABI: lives under src/.
 */

#ifndef TDC_ENTROPY_HUFFMAN_INTERNAL_H
#define TDC_ENTROPY_HUFFMAN_INTERNAL_H

#include "tdc/entropy.h"
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#ifdef _MSC_VER
#include <intrin.h>
#endif

#define HUFFMAN_NSYMS      256
#define HUFFMAN_MAX_LEN    15
/* Fixed header prefix: src_size(4) + payload_bits(4) + n_lengths(2) = 10.
 * Full header size = 10 + n_lengths.  Worst case: 10 + 256 = 266. */
#define HUFFMAN_HDR_PREFIX 10u
#define HUFFMAN_HDR_MAX    (HUFFMAN_HDR_PREFIX + (uint32_t)HUFFMAN_NSYMS)
#define HUFFMAN_FAST_BITS  11
#define HUFFMAN_FAST_SIZE  (1u << HUFFMAN_FAST_BITS)

/* ----- Bit writer (64-bit accumulator) ----------------------------------- */

typedef struct {
    uint8_t *p;
    size_t   used;
    uint64_t buf;        /* MSB-aligned: valid bits sit at 63 .. 64-nbits */
    int      nbits;      /* number of valid bits in buf, 0..46 */
    uint64_t total_bits;
} bit_writer;

static inline void bw_init(bit_writer *bw, uint8_t *out) {
    bw->p          = out;
    bw->used       = 0;
    bw->buf        = 0;
    bw->nbits      = 0;
    bw->total_bits = 0;
}

static inline void bw_write_bits(bit_writer *bw, uint16_t code, int nbits) {
    bw->total_bits += (uint64_t)nbits;
    bw->buf   |= (uint64_t)code << (64 - bw->nbits - nbits);
    bw->nbits += nbits;
    if (bw->nbits >= 32) {
        uint32_t top = (uint32_t)(bw->buf >> 32);
        bw->p[bw->used + 0] = (uint8_t)(top >> 24);
        bw->p[bw->used + 1] = (uint8_t)(top >> 16);
        bw->p[bw->used + 2] = (uint8_t)(top >>  8);
        bw->p[bw->used + 3] = (uint8_t)(top);
        bw->used  += 4;
        bw->buf  <<= 32;
        bw->nbits -= 32;
    }
}

static inline void bw_flush(bit_writer *bw) {
    while (bw->nbits >= 8) {
        bw->p[bw->used++] = (uint8_t)(bw->buf >> 56);
        bw->buf  <<= 8;
        bw->nbits -= 8;
    }
    if (bw->nbits > 0) {
        /* Partial byte: valid bits are MSB-aligned, low bits already 0. */
        bw->p[bw->used++] = (uint8_t)(bw->buf >> 56);
        bw->buf   = 0;
        bw->nbits = 0;
    }
}

/* ----- 64-bit bit reader (decode) ---------------------------------------- */

static inline uint64_t huf_bswap64(uint64_t v) {
#if defined(_MSC_VER)
    return _byteswap_uint64(v);
#elif defined(__GNUC__) || defined(__clang__)
    return __builtin_bswap64(v);
#else
    v = ((v & 0x00FF00FF00FF00FFULL) <<  8) | ((v >>  8) & 0x00FF00FF00FF00FFULL);
    v = ((v & 0x0000FFFF0000FFFFULL) << 16) | ((v >> 16) & 0x0000FFFF0000FFFFULL);
    return (v << 32) | (v >> 32);
#endif
}

typedef struct {
    uint64_t       bits;   /* MSB-aligned: valid bits at 63..64-nbits */
    int            nbits;  /* 0..64 */
    const uint8_t *ptr;    /* next unread byte */
    const uint8_t *safe;   /* ptr < safe => safe to do 8-byte memcpy */
    const uint8_t *end;    /* one past last payload byte */
} huf_br;

static inline void huf_br_init(huf_br *br, const uint8_t *src, size_t n) {
    br->bits  = 0;
    br->nbits = 0;
    br->ptr   = src;
    br->end   = src + n;
    br->safe  = (n >= 8) ? src + n - 7 : src;
}

/* Bulk refill: one 8-byte load + bswap, consumes up to 7 new bytes.
 * Caller must ensure ptr < safe. */
static inline void huf_br_refill_fast(huf_br *br) {
    uint64_t next;
    memcpy(&next, br->ptr, 8);
    next = huf_bswap64(next);
    br->bits |= next >> br->nbits;
    unsigned fresh = (unsigned)(64 - br->nbits) >> 3;
    br->ptr   += fresh;
    br->nbits += (int)(fresh << 3);
}

/* Byte-at-a-time refill for end-of-stream safety. */
static inline void huf_br_refill_safe(huf_br *br) {
    while (br->nbits <= 56 && br->ptr < br->end) {
        br->bits |= (uint64_t)(*br->ptr++) << (56 - br->nbits);
        br->nbits += 8;
    }
}

static inline void huf_br_refill(huf_br *br) {
    if (br->ptr < br->safe) huf_br_refill_fast(br);
    else                     huf_br_refill_safe(br);
}

/* ----- Shared tree-building helpers (defined in huffman.c) --------------- */

void huffman_count_freq(const uint8_t *src, size_t src_size,
                        uint32_t freq[HUFFMAN_NSYMS]);

int  huffman_compute_lengths_limited(const uint32_t freq_in[HUFFMAN_NSYMS],
                                     uint8_t        lens[HUFFMAN_NSYMS]);

void huffman_build_canonical(const uint8_t lens[HUFFMAN_NSYMS],
                             uint16_t      codes[HUFFMAN_NSYMS]);

/* Single-stream encode/decode — used by huffman4 as the small-input
 * fallback below HUF4_SMALL_THRESH. */
tdc_status huffman_encode(const uint8_t *src, size_t src_size,
                          const void    *params,
                          tdc_buffer    *dst);

tdc_status huffman_decode(const uint8_t *src, size_t src_size,
                          uint8_t       *dst, size_t dst_size);

#endif /* TDC_ENTROPY_HUFFMAN_INTERNAL_H */
