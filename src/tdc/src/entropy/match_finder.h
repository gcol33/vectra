/*
 * src/entropy/match_finder.h
 *
 * Match-finder abstraction for the LZ stage.
 *
 * The greedy parser (lz.c) and the three optimal parsers (lz_opt.c) all
 * walked their own near-duplicate hash chain. This vtable consolidates
 * the chain-walking logic in one backend (mf_hashchain.c) and lets us
 * plug in alternative match-finders (btree, suffix array) without
 * touching the parsers.
 *
 * The on-disk LZ stream format is unchanged — match-finder choice is a
 * pure encode-side optimization. The decoder in lz.c handles output
 * from any match-finder identically.
 *
 * Lifecycle:
 *
 *   ctx = vt->create(src, src_size, &params, alloc);
 *   for each position p in src:
 *       len = vt->find_best(ctx, p, extend_cap, &off);     // or find_multi
 *       vt->insert(ctx, p);                                // after the find
 *   vt->destroy(ctx, alloc);
 *
 * `extend_cap` controls per-candidate extension cost:
 *   0  => uncapped (greedy semantics — extend each candidate to end-of-input)
 *   >0 => cap each candidate's extension at this many bytes; if the
 *         capped length could be the new best, perform a full extension
 *         past the cap. This bounds per-position work on periodic inputs
 *         while preserving the "longest match found" invariant required
 *         for the "opt ≤ greedy" guarantee.
 */

#ifndef TDC_ENTROPY_MATCH_FINDER_H
#define TDC_ENTROPY_MATCH_FINDER_H

#include "tdc/entropy.h"

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* (length, offset) candidate returned by multi-match finders. The
 * optimal parser uses an array of these to drive offset-vs-length
 * trade-offs in its DP. */
typedef struct {
    uint32_t len;
    uint32_t off;
} LzOptMatch;

/* Opaque match-finder context. Allocated by vt->create, freed by
 * vt->destroy. Backends store hash tables, chain links, scratch state. */
typedef struct tdc_lz_mf_ctx tdc_lz_mf_ctx;

/* Parameters for vt->create. Backends ignore fields they don't use. */
typedef struct {
    /* Hashchain: number of chain links to walk past the htab entry per
     * find. 0 = flat hash (no chain_prev allocation). The htab entry is
     * always the first candidate, so total candidates per find is
     * chain_depth + 1. */
    uint32_t chain_depth;

    /* Hashchain: bucket count = 1 << hash_bits. 0 selects the backend
     * default (18 bits => 262144 buckets, ~1 MiB). */
    uint32_t hash_bits;
} tdc_lz_mf_params;

/* Match-finder vtable. */
typedef struct {
    const char *name;

    /* Create a context bound to (src, src_size). All allocation goes
     * through alloc->realloc_fn (POSIX-style). Returns NULL on alloc
     * failure or invalid params. */
    tdc_lz_mf_ctx *(*create)(const uint8_t *src, uint32_t src_size,
                              const tdc_lz_mf_params *params,
                              tdc_buffer *alloc);

    /* Find the longest match at `pos`. Returns match length (0 if none
     * >= LZ_MIN_MATCH). Sets *out_off on hit. extend_cap as above. */
    uint32_t (*find_best)(tdc_lz_mf_ctx *ctx, uint32_t pos,
                           uint32_t extend_cap,
                           uint32_t *out_off);

    /* Find up to `max_matches` non-dominated (length, offset) pairs at
     * `pos`. Each successive entry has strictly longer length; entries
     * at a strictly closer offset replace the previous entry rather
     * than appending. Returns the count written to `out`. */
    uint32_t (*find_multi)(tdc_lz_mf_ctx *ctx, uint32_t pos,
                            uint32_t extend_cap,
                            LzOptMatch *out, uint32_t max_matches);

    /* Insert `pos` into the index so future finds at later positions
     * can reference it. Typically called after find_best/find_multi at
     * `pos` so the find sees only strictly-earlier positions. */
    void (*insert)(tdc_lz_mf_ctx *ctx, uint32_t pos);

    /* Free the context via alloc->realloc_fn. No-op if ctx is NULL. */
    void (*destroy)(tdc_lz_mf_ctx *ctx, tdc_buffer *alloc);
} tdc_lz_mf_vt;

/* Default backend: Fibonacci-hash + singly-linked chain. */
extern const tdc_lz_mf_vt tdc_lz_mf_hashchain_vt;

/* bt4 backend: Fibonacci-hash + per-bucket binary search tree. Higher
 * scratch cost (2 * src_size * sizeof(uint32_t)) but finds higher-quality
 * matches because the descent is ordered by key (not by position), so
 * long matches surface earlier in the walk. Preferred for the optimal
 * parsers where ratio dominates memory and encode time. */
extern const tdc_lz_mf_vt tdc_lz_mf_btree_vt;

/* Match-finder selector.
 *
 *   prefer_quality = 0 — greedy parser caller.
 *   prefer_quality = 1 — optimal parser caller.
 *
 * Currently always returns the hashchain backend. The btree backend is
 * built and tested but does not (yet) earn its scratch-memory cost on
 * the current bench fixtures: on PRED2D+BSHUF+LZ_OPT it came in ~0.9%
 * worse on ratio without a speed win. Left in the tree behind the
 * explicit `tdc_lz_mf_btree_vt` vtable so it can be wired back in once
 * tuned (tree-build heuristics, chain-to-tree switchover size, depth
 * cap). The selector is the single knob for that future change.
 */
static inline const tdc_lz_mf_vt *tdc_lz_mf_select(uint32_t src_size,
                                                    int prefer_quality) {
    (void)src_size;
    (void)prefer_quality;
    return &tdc_lz_mf_hashchain_vt;
}

#ifdef __cplusplus
}
#endif

#endif /* TDC_ENTROPY_MATCH_FINDER_H */
