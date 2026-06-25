/*
 * src/core/stream.h
 *
 * Byte and bit stream readers/writers for entropy coders and the
 * block-record (de)serializer. Little-endian fixed.
 *
 * Defined as static inline (same convention as buffer.h, arena.h).
 */

#ifndef TDC_CORE_STREAM_H
#define TDC_CORE_STREAM_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ----- Byte-level write stream ------------------------------------------- */

typedef struct {
    uint8_t *data;
    size_t   pos;
    size_t   capacity;
} tdc_wstream;

static inline void tdc_wstream_init(tdc_wstream *s, uint8_t *data, size_t capacity) {
    s->data     = data;
    s->pos      = 0;
    s->capacity = capacity;
}

static inline int tdc_wstream_u8(tdc_wstream *s, uint8_t v) {
    if (s->pos >= s->capacity) return -1;
    s->data[s->pos++] = v;
    return 0;
}

static inline int tdc_wstream_u16(tdc_wstream *s, uint16_t v) {
    if (s->pos + 2u > s->capacity) return -1;
    memcpy(s->data + s->pos, &v, 2u);
    s->pos += 2u;
    return 0;
}

static inline int tdc_wstream_u32(tdc_wstream *s, uint32_t v) {
    if (s->pos + 4u > s->capacity) return -1;
    memcpy(s->data + s->pos, &v, 4u);
    s->pos += 4u;
    return 0;
}

static inline int tdc_wstream_u64(tdc_wstream *s, uint64_t v) {
    if (s->pos + 8u > s->capacity) return -1;
    memcpy(s->data + s->pos, &v, 8u);
    s->pos += 8u;
    return 0;
}

static inline int tdc_wstream_bytes(tdc_wstream *s, const uint8_t *src, size_t n) {
    if (s->pos + n > s->capacity) return -1;
    memcpy(s->data + s->pos, src, n);
    s->pos += n;
    return 0;
}

/* ----- Byte-level read stream -------------------------------------------- */

typedef struct {
    const uint8_t *data;
    size_t         pos;
    size_t         size;
} tdc_rstream;

static inline void tdc_rstream_init(tdc_rstream *s, const uint8_t *data, size_t size) {
    s->data = data;
    s->pos  = 0;
    s->size = size;
}

static inline int tdc_rstream_u8(tdc_rstream *s, uint8_t *v) {
    if (s->pos >= s->size) return -1;
    *v = s->data[s->pos++];
    return 0;
}

static inline int tdc_rstream_u16(tdc_rstream *s, uint16_t *v) {
    if (s->pos + 2u > s->size) return -1;
    memcpy(v, s->data + s->pos, 2u);
    s->pos += 2u;
    return 0;
}

static inline int tdc_rstream_u32(tdc_rstream *s, uint32_t *v) {
    if (s->pos + 4u > s->size) return -1;
    memcpy(v, s->data + s->pos, 4u);
    s->pos += 4u;
    return 0;
}

static inline int tdc_rstream_u64(tdc_rstream *s, uint64_t *v) {
    if (s->pos + 8u > s->size) return -1;
    memcpy(v, s->data + s->pos, 8u);
    s->pos += 8u;
    return 0;
}

static inline int tdc_rstream_bytes(tdc_rstream *s, uint8_t *dst, size_t n) {
    if (s->pos + n > s->size) return -1;
    memcpy(dst, s->data + s->pos, n);
    s->pos += n;
    return 0;
}

static inline size_t tdc_rstream_remaining(const tdc_rstream *s) {
    return s->size - s->pos;
}

/* ----- Bit-level write stream -------------------------------------------- */
/*
 * Accumulates bits in a 64-bit shift register, flushing complete bytes
 * to the underlying write stream. Bits are written LSB-first within each
 * byte (little-endian bit ordering, matching the on-disk convention).
 */

typedef struct {
    tdc_wstream *ws;
    uint64_t     bits;
    int          nbits;
} tdc_bitwriter;

static inline void tdc_bitwriter_init(tdc_bitwriter *bw, tdc_wstream *ws) {
    bw->ws    = ws;
    bw->bits  = 0;
    bw->nbits = 0;
}

static inline int tdc_bitwriter_put(tdc_bitwriter *bw, uint32_t val, int n) {
    bw->bits |= (uint64_t)val << bw->nbits;
    bw->nbits += n;
    while (bw->nbits >= 8) {
        if (tdc_wstream_u8(bw->ws, (uint8_t)(bw->bits & 0xFFu)) != 0) return -1;
        bw->bits >>= 8;
        bw->nbits -= 8;
    }
    return 0;
}

static inline int tdc_bitwriter_flush(tdc_bitwriter *bw) {
    if (bw->nbits > 0) {
        if (tdc_wstream_u8(bw->ws, (uint8_t)(bw->bits & 0xFFu)) != 0) return -1;
        bw->bits  = 0;
        bw->nbits = 0;
    }
    return 0;
}

/* ----- Bit-level read stream --------------------------------------------- */

typedef struct {
    tdc_rstream *rs;
    uint64_t     bits;
    int          nbits;
} tdc_bitreader;

static inline void tdc_bitreader_init(tdc_bitreader *br, tdc_rstream *rs) {
    br->rs    = rs;
    br->bits  = 0;
    br->nbits = 0;
}

static inline int tdc_bitreader_get(tdc_bitreader *br, int n, uint32_t *val) {
    while (br->nbits < n) {
        uint8_t byte;
        if (tdc_rstream_u8(br->rs, &byte) != 0) return -1;
        br->bits |= (uint64_t)byte << br->nbits;
        br->nbits += 8;
    }
    *val = (uint32_t)(br->bits & ((1ull << n) - 1u));
    br->bits >>= n;
    br->nbits -= n;
    return 0;
}

#ifdef __cplusplus
}
#endif

#endif /* TDC_CORE_STREAM_H */
