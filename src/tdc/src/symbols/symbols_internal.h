/*
 * src/symbols/symbols_internal.h
 *
 * Internal header for symbol-stream helpers: zigzag mapping, varint
 * encoding, and run-length encoding. These are utility functions shared
 * across model/ and transform/ files — NOT separate pipeline stages.
 */

#ifndef TDC_SYMBOLS_INTERNAL_H
#define TDC_SYMBOLS_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ----- Zigzag mapping (signed <-> unsigned) ------------------------------ */
/*
 * Maps small magnitudes (positive or negative) to small unsigned values:
 *   0 -> 0,  -1 -> 1,  1 -> 2,  -2 -> 3,  2 -> 4, ...
 */

static inline uint8_t  tdc_zigzag_enc8 (int8_t  v) {
    return (uint8_t)(((uint8_t)v << 1) ^ (uint8_t)-(int8_t)((uint8_t)v >> 7));
}
static inline uint16_t tdc_zigzag_enc16(int16_t v) {
    return (uint16_t)(((uint16_t)v << 1) ^ (uint16_t)-(int16_t)((uint16_t)v >> 15));
}
static inline uint32_t tdc_zigzag_enc32(int32_t v) {
    return ((uint32_t)v << 1) ^ (uint32_t)-(int32_t)((uint32_t)v >> 31);
}
static inline uint64_t tdc_zigzag_enc64(int64_t v) {
    return ((uint64_t)v << 1) ^ (uint64_t)-(int64_t)((uint64_t)v >> 63);
}

static inline int8_t   tdc_zigzag_dec8 (uint8_t  z) {
    return (int8_t)((z >> 1) ^ (uint8_t)-(int8_t)(z & 1u));
}
static inline int16_t  tdc_zigzag_dec16(uint16_t z) {
    return (int16_t)((z >> 1) ^ (uint16_t)-(int16_t)(z & 1u));
}
static inline int32_t  tdc_zigzag_dec32(uint32_t z) {
    return (int32_t)((z >> 1) ^ (uint32_t)-(int32_t)(z & 1u));
}
static inline int64_t  tdc_zigzag_dec64(uint64_t z) {
    return (int64_t)((z >> 1) ^ (uint64_t)-(int64_t)(z & 1u));
}

/* ----- Varint (LEB128 unsigned) ------------------------------------------ */
/*
 * Variable-length encoding for unsigned integers, used by dictionary side
 * metadata, RLE counts, etc. 7 bits of payload + 1 continuation bit per
 * byte, little-endian byte order.
 */

/* Number of bytes needed to encode val. */
static inline size_t tdc_varint_size(uint64_t val) {
    size_t n = 1;
    while (val >= 128u) { val >>= 7; n++; }
    return n;
}

/* Encode val into out[]. Returns the number of bytes written.
 * Caller must ensure out has at least tdc_varint_size(val) bytes. */
static inline size_t tdc_varint_encode(uint64_t val, uint8_t *out) {
    size_t n = 0;
    while (val >= 128u) {
        out[n++] = (uint8_t)((val & 0x7Fu) | 0x80u);
        val >>= 7;
    }
    out[n++] = (uint8_t)val;
    return n;
}

/* Decode a varint from in[0..in_size). Returns 0 on error (truncated).
 * Otherwise returns the number of bytes consumed and writes to *val. */
static inline size_t tdc_varint_decode(const uint8_t *in, size_t in_size,
                                        uint64_t *val) {
    uint64_t result = 0;
    size_t shift = 0;
    for (size_t i = 0; i < in_size && i < 10u; ++i) {
        uint8_t byte = in[i];
        result |= ((uint64_t)(byte & 0x7Fu)) << shift;
        shift += 7;
        if (!(byte & 0x80u)) {
            *val = result;
            return i + 1;
        }
    }
    return 0; /* truncated */
}

/* ----- RLE (run-length encoding) ----------------------------------------- */
/*
 * Encodes a stream of uint32 values as (value, count) pairs using varint.
 * Used by model/dict.c for compressing dictionary index streams.
 *
 * On-wire format per run:
 *   varint(value)
 *   varint(count - 1)   (count >= 1, stored as count-1 for compactness)
 */

/* Compute the encoded size of an RLE stream without writing. */
size_t tdc_rle_encoded_size(const uint32_t *in, size_t n);

/* Encode n uint32 values into out[0..out_cap). Returns bytes written,
 * or 0 if out_cap is too small. */
size_t tdc_rle_encode(const uint32_t *in, size_t n,
                      uint8_t *out, size_t out_cap);

/* Decode an RLE stream into out[0..out_n). Returns bytes consumed from in,
 * or 0 on error (corrupt or output overflow). */
size_t tdc_rle_decode(const uint8_t *in, size_t in_size,
                      uint32_t *out, size_t out_n);

#ifdef __cplusplus
}
#endif

#endif /* TDC_SYMBOLS_INTERNAL_H */
