/*
 * src/format/metadata_internal.h
 *
 * Internal little-endian read/write primitives + a tiny bounds-checked
 * reader cursor for parsing side-metadata blobs.
 *
 * Why this exists: every model that emits side metadata (dict, plane2d,
 * pred2d, pred3d, ...) was reaching for the same memcpy(&u32, p, 4u)
 * idiom. The format is little-endian, the platforms are little-endian,
 * and the alignment is unknown — so memcpy is the only correct primitive.
 * This header is the single source of truth for that primitive so we
 * never end up with subtly different "load u32" helpers in five files.
 *
 * The primitives are static inline so each translation unit gets its own
 * copy with no link dependency. The companion src/format/metadata.c
 * exposes a non-inline tdc_buffer-growing writer for the cases where
 * inlines aren't enough (variable-length sections that need
 * tdc_buf_reserve).
 *
 * Not part of the public ABI: lives under src/, mirroring entropy_internal.h
 * and model_internal.h.
 */

#ifndef TDC_FORMAT_METADATA_INTERNAL_H
#define TDC_FORMAT_METADATA_INTERNAL_H

#include "tdc/types.h"
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ----- Unaligned little-endian load --------------------------------------- */

static inline uint16_t tdc_le_load_u16(const uint8_t *p) {
    uint16_t v;
    memcpy(&v, p, 2u);
    return v;
}

static inline uint32_t tdc_le_load_u32(const uint8_t *p) {
    uint32_t v;
    memcpy(&v, p, 4u);
    return v;
}

static inline uint64_t tdc_le_load_u64(const uint8_t *p) {
    uint64_t v;
    memcpy(&v, p, 8u);
    return v;
}

static inline int32_t tdc_le_load_i32(const uint8_t *p) {
    int32_t v;
    memcpy(&v, p, 4u);
    return v;
}

/* ----- Unaligned little-endian store -------------------------------------- */

static inline void tdc_le_store_u16(uint8_t *p, uint16_t v) {
    memcpy(p, &v, 2u);
}

static inline void tdc_le_store_u32(uint8_t *p, uint32_t v) {
    memcpy(p, &v, 4u);
}

static inline void tdc_le_store_u64(uint8_t *p, uint64_t v) {
    memcpy(p, &v, 8u);
}

static inline void tdc_le_store_i32(uint8_t *p, int32_t v) {
    memcpy(p, &v, 4u);
}

/* ----- Bounds-checked reader cursor --------------------------------------- */
/*
 * Tiny POD that walks an immutable byte buffer and refuses to read past
 * the end. The cursor's `ok` flag latches once any read fails, so callers
 * can chain reads and check `ok` exactly once at the end.
 *
 * Usage:
 *
 *     tdc_meta_reader r = tdc_meta_reader_init(side_meta, side_size);
 *     uint16_t tile_size = tdc_meta_read_u16(&r);
 *     uint32_t n_tiles   = tdc_meta_read_u32(&r);
 *     const uint8_t *coeffs = tdc_meta_read_bytes(&r, n_tiles * 12u);
 *     if (!r.ok) return TDC_E_CORRUPT;
 */

typedef struct {
    const uint8_t *base;
    size_t         size;
    size_t         off;
    int            ok;     /* sticky: 1 until any read fails */
} tdc_meta_reader;

static inline tdc_meta_reader tdc_meta_reader_init(const uint8_t *base, size_t size) {
    tdc_meta_reader r;
    r.base = base;
    r.size = size;
    r.off  = 0;
    r.ok   = (base != NULL || size == 0);
    return r;
}

static inline int tdc_meta_reader_remaining(const tdc_meta_reader *r) {
    return (int)(r->size - r->off);
}

static inline uint8_t tdc_meta_read_u8(tdc_meta_reader *r) {
    if (!r->ok || r->size - r->off < 1u) { r->ok = 0; return 0; }
    return r->base[r->off++];
}

#define DEFINE_META_READ(SUFFIX, TYPE, WIDTH, LOAD_FN)               \
static inline TYPE tdc_meta_read_##SUFFIX(tdc_meta_reader *r) {      \
    if (!r->ok || r->size - r->off < (WIDTH)) { r->ok = 0; return 0; } \
    TYPE v = LOAD_FN(r->base + r->off);                              \
    r->off += (WIDTH);                                               \
    return v;                                                        \
}

DEFINE_META_READ(u16, uint16_t, 2u, tdc_le_load_u16)
DEFINE_META_READ(u32, uint32_t, 4u, tdc_le_load_u32)
DEFINE_META_READ(u64, uint64_t, 8u, tdc_le_load_u64)

#undef DEFINE_META_READ

/* Returns a pointer into the underlying buffer (no copy). The returned
 * pointer is valid for the lifetime of the source buffer. Returns NULL
 * (and trips r->ok) if there aren't `n` bytes remaining. */
static inline const uint8_t *tdc_meta_read_bytes(tdc_meta_reader *r, size_t n) {
    if (!r->ok || r->size - r->off < n) { r->ok = 0; return NULL; }
    const uint8_t *p = r->base + r->off;
    r->off += n;
    return p;
}

/* ----- Growable writer (non-inline; defined in metadata.c) ---------------- */
/*
 * Append-style writer over a tdc_buffer. Each call grows the buffer via
 * realloc_fn as needed. Returns TDC_OK / TDC_E_NOMEM. The buffer's
 * `size` field is advanced to the new write head on success.
 *
 * These are the cases where a static inline isn't enough — the
 * tdc_buf_reserve call would force every TU to inline its own copy of
 * the growth policy. One non-inline owner keeps the policy in one place.
 */

tdc_status tdc_meta_write_u8   (tdc_buffer *out, uint8_t  v);
tdc_status tdc_meta_write_u16  (tdc_buffer *out, uint16_t v);
tdc_status tdc_meta_write_u32  (tdc_buffer *out, uint32_t v);
tdc_status tdc_meta_write_u64  (tdc_buffer *out, uint64_t v);
tdc_status tdc_meta_write_bytes(tdc_buffer *out, const void *src, size_t n);

#ifdef __cplusplus
}
#endif

#endif /* TDC_FORMAT_METADATA_INTERNAL_H */
