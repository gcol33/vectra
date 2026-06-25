/*
 * src/entropy/mf_hashchain.c
 *
 * Hash-chain match finder. Default backend behind tdc_lz_mf_hashchain_vt.
 *
 * Layout:
 *   htab[1 << hash_bits]   — head of chain per bucket (most recent pos)
 *   chain_prev[src_size]   — link to previous position sharing the same
 *                            bucket (allocated only when chain_depth > 0;
 *                            chain_depth == 0 collapses to flat hash)
 *
 * Hash function: 4-byte little-endian load, multiplied by the Fibonacci
 * constant 2654435761u, top hash_bits bits taken. Same constant the
 * greedy and optimal parsers used before the abstraction, preserving
 * bucket distribution.
 *
 * find_best / find_multi share the same chain walk:
 *   - quick-reject by tail byte (skip candidates whose byte at the
 *     current best length differs from src[pos + best_len])
 *   - 8-byte SIMD compare with CTZ for the extension hot loop
 *   - L1 prefetch on chain_prev[next_cand] and src[next_cand]
 *   - optional per-candidate extension cap with full-extend on cap-hit
 *     candidates that could be the new best (preserves the "longest
 *     match found" invariant required for opt ≤ greedy)
 */

#include "match_finder.h"
#include "lz_internal.h"
#include "../core/simd.h"

#include <string.h>
#include <stdint.h>
#include <stddef.h>

#if defined(_MSC_VER)
#  include <intrin.h>
#endif

#define MF_HASHCHAIN_DEFAULT_HASH_BITS  18u
#define MF_NULL                         0xFFFFFFFFu

struct tdc_lz_mf_ctx {
    const uint8_t *src;
    uint32_t       src_size;
    uint32_t      *htab;          /* head of chain per bucket */
    uint32_t      *chain_prev;    /* NULL when chain_depth == 0 */
    uint32_t       hash_bits;
    uint32_t       hash_shift;    /* 32 - hash_bits */
    uint32_t       chain_depth;   /* links past htab; 0 = flat hash */
};

/* ----- Hash + extension primitives --------------------------------------- */

static inline uint32_t mf_hash4(const uint8_t *p, uint32_t shift) {
    uint32_t h = ((uint32_t)p[0]) |
                 ((uint32_t)p[1] << 8) |
                 ((uint32_t)p[2] << 16) |
                 ((uint32_t)p[3] << 24);
    return (h * 2654435761u) >> shift;
}

static inline uint32_t mf_extend(const uint8_t *src, uint32_t pos,
                                  uint32_t cand, uint32_t max_len) {
    uint32_t len = 0u;
    while (len + 8u <= max_len) {
        uint64_t a, b;
        memcpy(&a, src + pos + len, 8);
        memcpy(&b, src + cand + len, 8);
        if (a != b) {
#if defined(__GNUC__) || defined(__clang__)
            len += (uint32_t)(__builtin_ctzll(a ^ b) >> 3);
#elif defined(_MSC_VER)
            unsigned long idx;
            _BitScanForward64(&idx, a ^ b);
            len += (uint32_t)(idx >> 3);
#else
            {
                uint64_t diff = a ^ b;
                while (!(diff & 0xFFu)) { diff >>= 8; len++; }
            }
#endif
            return len;
        }
        len += 8u;
    }
    while (len < max_len && src[pos + len] == src[cand + len]) len++;
    return len;
}

/* ----- Lifecycle --------------------------------------------------------- */

static tdc_lz_mf_ctx *mf_hashchain_create(const uint8_t *src, uint32_t src_size,
                                           const tdc_lz_mf_params *params,
                                           tdc_buffer *alloc) {
    if (!alloc || !alloc->realloc_fn) return NULL;

    uint32_t hash_bits = (params && params->hash_bits)
        ? params->hash_bits
        : MF_HASHCHAIN_DEFAULT_HASH_BITS;
    uint32_t chain_depth = params ? params->chain_depth : 0u;

    tdc_lz_mf_ctx *ctx = (tdc_lz_mf_ctx *)alloc->realloc_fn(
        alloc->user, NULL, sizeof(tdc_lz_mf_ctx));
    if (!ctx) return NULL;

    size_t htab_size = (size_t)1u << hash_bits;
    ctx->src         = src;
    ctx->src_size    = src_size;
    ctx->hash_bits   = hash_bits;
    ctx->hash_shift  = 32u - hash_bits;
    ctx->chain_depth = chain_depth;
    ctx->chain_prev  = NULL;

    ctx->htab = (uint32_t *)alloc->realloc_fn(
        alloc->user, NULL, htab_size * sizeof(uint32_t));
    if (!ctx->htab) {
        (void)alloc->realloc_fn(alloc->user, ctx, 0);
        return NULL;
    }
    memset(ctx->htab, 0xFF, htab_size * sizeof(uint32_t));

    if (chain_depth > 0u && src_size > 0u) {
        ctx->chain_prev = (uint32_t *)alloc->realloc_fn(
            alloc->user, NULL, (size_t)src_size * sizeof(uint32_t));
        if (!ctx->chain_prev) {
            (void)alloc->realloc_fn(alloc->user, ctx->htab, 0);
            (void)alloc->realloc_fn(alloc->user, ctx, 0);
            return NULL;
        }
        memset(ctx->chain_prev, 0xFF, (size_t)src_size * sizeof(uint32_t));
    }

    return ctx;
}

static void mf_hashchain_destroy(tdc_lz_mf_ctx *ctx, tdc_buffer *alloc) {
    if (!ctx) return;
    if (ctx->chain_prev) (void)alloc->realloc_fn(alloc->user, ctx->chain_prev, 0);
    if (ctx->htab)       (void)alloc->realloc_fn(alloc->user, ctx->htab, 0);
    (void)alloc->realloc_fn(alloc->user, ctx, 0);
}

static void mf_hashchain_insert(tdc_lz_mf_ctx *ctx, uint32_t pos) {
    if (pos + LZ_MIN_MATCH + 1u > ctx->src_size) return;
    uint32_t h = mf_hash4(ctx->src + pos, ctx->hash_shift);
    if (ctx->chain_prev) ctx->chain_prev[pos] = ctx->htab[h];
    ctx->htab[h] = pos;
}

/* ----- Find best (single longest match) --------------------------------- */

static uint32_t mf_hashchain_find_best(tdc_lz_mf_ctx *ctx, uint32_t pos,
                                        uint32_t extend_cap,
                                        uint32_t *out_off) {
    if (pos + LZ_MIN_MATCH + 1u > ctx->src_size) return 0u;

    const uint8_t *src        = ctx->src;
    const uint32_t *chain     = ctx->chain_prev;
    uint32_t h                = mf_hash4(src + pos, ctx->hash_shift);
    uint32_t cand             = ctx->htab[h];
    uint32_t depth            = ctx->chain_depth + 1u;
    uint32_t min_pos          = (pos > LZ_MAX_OFFSET) ? (pos - LZ_MAX_OFFSET) : 0u;
    uint32_t max_remain       = ctx->src_size - pos;
    uint32_t cap              = (extend_cap > 0u && extend_cap < max_remain)
                                  ? extend_cap : max_remain;
    uint32_t best_len         = LZ_MIN_MATCH - 1u;
    uint32_t best_off         = 0u;

    while (cand != MF_NULL && depth > 0u) {
        if (cand >= pos) goto step;        /* safety net: skip future positions */
        if (cand < min_pos) break;          /* older than window — older candidates only get worse */

        /* Prefetch next-iteration loads while we work on this candidate. */
        uint32_t next_cand = chain ? chain[cand] : MF_NULL;
        if (next_cand != MF_NULL && next_cand < pos) {
            TDC_PREFETCH_L1(src + next_cand);
            if (chain) TDC_PREFETCH_L1(&chain[next_cand]);
        }

        /* Quick reject: candidate can't beat best if the byte at best_len
         * already differs. Cheap one-byte compare gates the 8-byte
         * extension loop. */
        if (best_len >= LZ_MIN_MATCH &&
            src[cand + best_len] != src[pos + best_len])
            goto step_known;

        if (src[cand] == src[pos]) {
            uint32_t len = mf_extend(src, pos, cand, cap);
            /* Cap-hit candidate that could match or beat best — extend
             * past the cap to find the true length. Required for the
             * "longest match found" invariant. */
            if (len == cap && cap < max_remain && len >= best_len) {
                len += mf_extend(src, pos + cap, cand + cap,
                                  max_remain - cap);
            }
            if (len > best_len) {
                best_len = len;
                best_off = pos - cand;
                if (len == max_remain) break;  /* can't do better */
            }
        }

    step_known:
        cand = next_cand;
        depth--;
        continue;
    step:
        cand = chain ? chain[cand] : MF_NULL;
        depth--;
    }

    if (best_len >= LZ_MIN_MATCH) {
        *out_off = best_off;
        return best_len;
    }
    return 0u;
}

/* ----- Find multi (non-dominated len/off pairs) ------------------------- */

static uint32_t mf_hashchain_find_multi(tdc_lz_mf_ctx *ctx, uint32_t pos,
                                         uint32_t extend_cap,
                                         LzOptMatch *out, uint32_t max_matches) {
    if (max_matches == 0u) return 0u;
    if (pos + LZ_MIN_MATCH + 1u > ctx->src_size) return 0u;

    const uint8_t *src        = ctx->src;
    const uint32_t *chain     = ctx->chain_prev;
    uint32_t h                = mf_hash4(src + pos, ctx->hash_shift);
    uint32_t cand             = ctx->htab[h];
    uint32_t depth            = ctx->chain_depth + 1u;
    uint32_t min_pos          = (pos > LZ_MAX_OFFSET) ? (pos - LZ_MAX_OFFSET) : 0u;
    uint32_t max_remain       = ctx->src_size - pos;
    uint32_t cap              = (extend_cap > 0u && extend_cap < max_remain)
                                  ? extend_cap : max_remain;
    uint32_t prev_best_len    = LZ_MIN_MATCH - 1u;
    uint32_t n_matches        = 0u;

    while (cand != MF_NULL && depth > 0u) {
        if (cand >= pos) goto step;
        if (cand < min_pos) break;

        uint32_t next_cand = chain ? chain[cand] : MF_NULL;
        if (next_cand != MF_NULL && next_cand < pos) {
            TDC_PREFETCH_L1(src + next_cand);
            if (chain) TDC_PREFETCH_L1(&chain[next_cand]);
        }

        if (src[cand] == src[pos]) {
            uint32_t len = mf_extend(src, pos, cand, cap);
            if (len == cap && cap < max_remain && len > prev_best_len) {
                len += mf_extend(src, pos + cap, cand + cap,
                                  max_remain - cap);
            }
            if (len >= LZ_MIN_MATCH && len > prev_best_len) {
                uint32_t off = pos - cand;
                /* Closer-and-longer dominates: replace the previous entry
                 * if its offset is >= this one. Otherwise append. */
                if (n_matches > 0u && out[n_matches - 1u].off >= off) {
                    out[n_matches - 1u].len = len;
                    out[n_matches - 1u].off = off;
                } else if (n_matches < max_matches) {
                    out[n_matches].len = len;
                    out[n_matches].off = off;
                    n_matches++;
                }
                prev_best_len = len;
            }
        }

        cand = next_cand;
        depth--;
        continue;
    step:
        cand = chain ? chain[cand] : MF_NULL;
        depth--;
    }

    return n_matches;
}

/* ----- Vtable ------------------------------------------------------------ */

const tdc_lz_mf_vt tdc_lz_mf_hashchain_vt = {
    .name        = "hashchain",
    .create      = mf_hashchain_create,
    .find_best   = mf_hashchain_find_best,
    .find_multi  = mf_hashchain_find_multi,
    .insert      = mf_hashchain_insert,
    .destroy     = mf_hashchain_destroy,
};
