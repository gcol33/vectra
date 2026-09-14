#ifndef VECTRA_VTR_DIGEST_H
#define VECTRA_VTR_DIGEST_H

/*
 * VtrDigest: streaming 64-bit digest of a byte stream.
 *
 * Used to identify a .vtr store by its bytes: the writers feed every byte they
 * emit through one, and an index stamps the result, so an index is never probed
 * against a store other than the one it was built on (see vtr1_tdc.h).
 *
 * Words are absorbed with the XXH64 round and merge. Each absorb step is a
 * bijection of the state for a fixed input word (xor, rotate, multiply by an odd
 * constant, add), so two streams of equal length that differ in a single word
 * always end in different states; the finalizer is a bijection too. Arbitrary
 * differences collide with probability about 2^-64. It is not a cryptographic
 * hash and is not meant to resist a constructed collision.
 *
 * The digest depends only on the byte sequence, never on how it was split
 * across update calls.
 */

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define VTR_DG_P1 0x9E3779B185EBCA87ULL
#define VTR_DG_P2 0xC2B2AE3D27D4EB4FULL
#define VTR_DG_P3 0x165667B19E3779F9ULL
#define VTR_DG_P4 0x85EBCA77C2B2AE63ULL
#define VTR_DG_P5 0x27D4EB2F165667C5ULL

typedef struct {
    uint64_t h;
    uint64_t len;
    uint8_t  tail[8];
    unsigned n_tail;
} VtrDigest;

static inline uint64_t vtr_dg_rotl(uint64_t x, int r) {
    return (x << r) | (x >> (64 - r));
}

static inline void vtr_dg_absorb(VtrDigest *d, uint64_t w) {
    w *= VTR_DG_P2;
    w  = vtr_dg_rotl(w, 31);
    w *= VTR_DG_P1;
    d->h ^= w;
    d->h  = vtr_dg_rotl(d->h, 27) * VTR_DG_P1 + VTR_DG_P4;
}

static inline void vtr_digest_init(VtrDigest *d, uint64_t seed) {
    d->h = seed + VTR_DG_P5;
    d->len = 0;
    d->n_tail = 0;
}

static inline void vtr_digest_update(VtrDigest *d, const void *data, size_t n) {
    const uint8_t *p = (const uint8_t *)data;
    d->len += (uint64_t)n;
    if (d->n_tail) {
        while (n && d->n_tail < 8) { d->tail[d->n_tail++] = *p++; n--; }
        if (d->n_tail < 8) return;
        uint64_t w;
        memcpy(&w, d->tail, 8);
        vtr_dg_absorb(d, w);
        d->n_tail = 0;
    }
    while (n >= 8) {
        uint64_t w;
        memcpy(&w, p, 8);
        vtr_dg_absorb(d, w);
        p += 8;
        n -= 8;
    }
    while (n) { d->tail[d->n_tail++] = *p++; n--; }
}

static inline void vtr_digest_u64(VtrDigest *d, uint64_t v) {
    vtr_digest_update(d, &v, 8);
}

static inline uint64_t vtr_digest_final(const VtrDigest *d) {
    uint64_t h = d->h + d->len;
    for (unsigned i = 0; i < d->n_tail; i++) {
        h ^= (uint64_t)d->tail[i] * VTR_DG_P5;
        h  = vtr_dg_rotl(h, 11) * VTR_DG_P1;
    }
    h ^= h >> 33;
    h *= VTR_DG_P2;
    h ^= h >> 29;
    h *= VTR_DG_P3;
    h ^= h >> 32;
    return h;
}

#endif /* VECTRA_VTR_DIGEST_H */
