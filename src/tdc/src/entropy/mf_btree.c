/*
 * src/entropy/mf_btree.c
 *
 * Binary-tree (bt4) match finder — alternative backend for tdc_lz_mf_vt.
 *
 * Layout:
 *   htab[1 << hash_bits]   — head of tree per bucket (most recent pos)
 *   bt_left [src_size]     — per position: root of "keys < this pos" subtree
 *   bt_right[src_size]     — per position: root of "keys > this pos" subtree
 *
 * Each node is keyed on the string at its position and held in lexicographic
 * order (not by pos). When a new pos is inserted, it becomes the bucket's
 * root via the standard bt4 splay: during a single tree descent we classify
 * every visited old-tree node into pos's left or right subtree and link it
 * there, while also computing its match length against pos.
 *
 * Combined find+insert is the natural shape of bt4, so mf_bt_descent() does
 * both in one walk. find_best() and find_multi() call the descent and
 * extract the best match / non-dominated list. insert() skips when called
 * on the same pos that was just find'd (the documented find-then-insert
 * pattern) and otherwise runs the descent with match bookkeeping disabled.
 *
 * Tree invariant: every node's children were inserted strictly before it
 * (each new pos becomes its bucket's root, pushing prior entries down). So
 * children are always older than their parent — a subtree rooted at a node
 * that is out-of-window (cand < min_pos) is entirely out-of-window and can
 * be pruned without losing in-window candidates.
 *
 * Memory: 2 * src_size * sizeof(uint32_t) for the bt arrays plus the htab.
 * At 8 bytes per input byte this is ~8x the hash-chain MF — the selector
 * only routes through btree where the ratio lift (match quality from a
 * tree-ordered walk) is worth the scratch footprint.
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

#define MF_BT_DEFAULT_HASH_BITS  18u
#define MF_BT_DEFAULT_DEPTH_CAP  64u
#define MF_NULL                  0xFFFFFFFFu

/* Extend cap used for insert-only descents. Needs to be long enough to
 * disambiguate splay direction on typical data but bounded so periodic
 * inputs can't cascade into O(src_size) extends per candidate. Matches
 * LZ_OPT_EXTEND_CAP in lz_opt.c (kept local to avoid cross-module macro
 * leakage). */
#define MF_BT_INSERT_EXTEND_CAP  64u

struct tdc_lz_mf_ctx {
    const uint8_t *src;
    uint32_t       src_size;
    uint32_t      *htab;
    uint32_t      *bt_left;
    uint32_t      *bt_right;
    uint32_t       hash_bits;
    uint32_t       hash_shift;
    uint32_t       depth_cap;
    uint32_t       last_insert_pos;
};

/* ----- Hash + extend primitives ----------------------------------------- */

static inline uint32_t mf_bt_hash4(const uint8_t *p, uint32_t shift) {
    uint32_t h = ((uint32_t)p[0]) |
                 ((uint32_t)p[1] << 8) |
                 ((uint32_t)p[2] << 16) |
                 ((uint32_t)p[3] << 24);
    return (h * 2654435761u) >> shift;
}

/* Match length between src[pos..] and src[cur..], starting from `start`
 * bytes (caller has verified those already match). Capped at `max_len`. */
static inline uint32_t mf_bt_extend(const uint8_t *src, uint32_t pos,
                                     uint32_t cur, uint32_t start,
                                     uint32_t max_len) {
    uint32_t len = start;
    while (len + 8u <= max_len) {
        uint64_t a, b;
        memcpy(&a, src + pos + len, 8);
        memcpy(&b, src + cur + len, 8);
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
    while (len < max_len && src[pos + len] == src[cur + len]) len++;
    return len;
}

/* ----- Lifecycle --------------------------------------------------------- */

static tdc_lz_mf_ctx *mf_btree_create(const uint8_t *src, uint32_t src_size,
                                       const tdc_lz_mf_params *params,
                                       tdc_buffer *alloc) {
    if (!alloc || !alloc->realloc_fn) return NULL;

    uint32_t hash_bits = (params && params->hash_bits)
        ? params->hash_bits
        : MF_BT_DEFAULT_HASH_BITS;
    uint32_t depth_cap = (params && params->chain_depth)
        ? params->chain_depth
        : MF_BT_DEFAULT_DEPTH_CAP;

    tdc_lz_mf_ctx *ctx = (tdc_lz_mf_ctx *)alloc->realloc_fn(
        alloc->user, NULL, sizeof(tdc_lz_mf_ctx));
    if (!ctx) return NULL;

    size_t htab_size = (size_t)1u << hash_bits;
    ctx->src             = src;
    ctx->src_size        = src_size;
    ctx->hash_bits       = hash_bits;
    ctx->hash_shift      = 32u - hash_bits;
    ctx->depth_cap       = depth_cap;
    ctx->last_insert_pos = MF_NULL;
    ctx->htab            = NULL;
    ctx->bt_left         = NULL;
    ctx->bt_right        = NULL;

    ctx->htab = (uint32_t *)alloc->realloc_fn(
        alloc->user, NULL, htab_size * sizeof(uint32_t));
    if (!ctx->htab) goto fail;
    memset(ctx->htab, 0xFF, htab_size * sizeof(uint32_t));

    if (src_size > 0u) {
        ctx->bt_left = (uint32_t *)alloc->realloc_fn(
            alloc->user, NULL, (size_t)src_size * sizeof(uint32_t));
        if (!ctx->bt_left) goto fail;
        ctx->bt_right = (uint32_t *)alloc->realloc_fn(
            alloc->user, NULL, (size_t)src_size * sizeof(uint32_t));
        if (!ctx->bt_right) goto fail;
        /* bt_left/bt_right entries are written lazily during descent — any
         * slot read via a tree link was written when the link was placed.
         * No memset required (and skipping it avoids touching 128 MiB for
         * a 16 MiB input). */
    }

    return ctx;

fail:
    if (ctx->bt_right) (void)alloc->realloc_fn(alloc->user, ctx->bt_right, 0);
    if (ctx->bt_left)  (void)alloc->realloc_fn(alloc->user, ctx->bt_left, 0);
    if (ctx->htab)     (void)alloc->realloc_fn(alloc->user, ctx->htab, 0);
    (void)alloc->realloc_fn(alloc->user, ctx, 0);
    return NULL;
}

static void mf_btree_destroy(tdc_lz_mf_ctx *ctx, tdc_buffer *alloc) {
    if (!ctx) return;
    if (ctx->bt_right) (void)alloc->realloc_fn(alloc->user, ctx->bt_right, 0);
    if (ctx->bt_left)  (void)alloc->realloc_fn(alloc->user, ctx->bt_left, 0);
    if (ctx->htab)     (void)alloc->realloc_fn(alloc->user, ctx->htab, 0);
    (void)alloc->realloc_fn(alloc->user, ctx, 0);
}

/* ----- bt4 descent (find + insert, combined) ----------------------------- *
 *
 * Descends from the bucket's old root, splays pos in as the new root, and
 * collects matches against every visited node. If `matches` is non-NULL,
 * populates a non-dominated (len↑, off↓) Pareto list for the optimal
 * parser. Sets `*best_off_out` to the offset of the longest match.
 *
 * ptr_gt / ptr_lt track the "growing frontier" of pos's right/left
 * subtrees: each points to the slot where the next "greater/less than pos"
 * node found during descent should be linked. They start at pos's subtree
 * roots and migrate to cur's bt_left / bt_right as we descend past cur.
 *
 * len_gt / len_lt are the standard bt4 optimization: every node visited
 * on the "> pos" descent track shares at least `len_gt` bytes with pos
 * (ditto for "< pos"), so extension can start from min(len_gt, len_lt)
 * instead of 0 — saves redundant byte comparisons on near-identical keys.
 */
static uint32_t mf_bt_descent(tdc_lz_mf_ctx *ctx, uint32_t pos,
                               uint32_t extend_cap,
                               LzOptMatch *matches, uint32_t max_matches,
                               uint32_t *n_matches_out,
                               uint32_t *best_off_out) {
    const uint8_t *src      = ctx->src;
    uint32_t       src_size = ctx->src_size;

    if (pos + LZ_MIN_MATCH + 1u > src_size) {
        /* Tail of the buffer — no hashable 4-byte key, no match possible.
         * Skip htab insertion; pos is never linked into the tree, so its
         * bt_left/bt_right slots are never read by any descent. */
        if (best_off_out)  *best_off_out  = 0u;
        if (n_matches_out) *n_matches_out = 0u;
        ctx->last_insert_pos = pos;
        return 0u;
    }

    /* Insert-only descents don't care about match quality — only splay
     * direction. Skip the cap-hit full extend (which is O(max_remain))
     * and all match bookkeeping in that mode. This is what keeps insert
     * O(depth_cap * cap) on periodic data instead of O(depth_cap * src_size). */
    const int collect = (matches != NULL) || (best_off_out != NULL);

    uint32_t h   = mf_bt_hash4(src + pos, ctx->hash_shift);
    uint32_t cur = ctx->htab[h];
    ctx->htab[h] = pos;  /* pos becomes new bucket root */

    uint32_t *ptr_gt = &ctx->bt_right[pos];
    uint32_t *ptr_lt = &ctx->bt_left[pos];

    uint32_t min_pos    = (pos > LZ_MAX_OFFSET) ? (pos - LZ_MAX_OFFSET) : 0u;
    uint32_t max_remain = src_size - pos;
    uint32_t cap        = (extend_cap > 0u && extend_cap < max_remain)
                              ? extend_cap : max_remain;

    uint32_t len_gt = 0u;
    uint32_t len_lt = 0u;

    uint32_t best_len      = LZ_MIN_MATCH - 1u;
    uint32_t best_off      = 0u;
    uint32_t n_matches     = 0u;
    uint32_t prev_best_len = LZ_MIN_MATCH - 1u;
    uint32_t depth         = ctx->depth_cap;

    while (cur != MF_NULL && cur < pos && cur >= min_pos && depth > 0u) {
        uint32_t start = (len_lt < len_gt) ? len_lt : len_gt;
        uint32_t ml    = mf_bt_extend(src, pos, cur, start, cap);

        /* Cap-hit full extend for the "longest match found" invariant:
         * if the capped length is >= current best, a cap-truncated length
         * could mis-rank this candidate. Full-extend to get the true ml
         * so opt ≤ greedy holds. Insert-only mode skips this entirely. */
        uint32_t full_ml = ml;
        if (collect && ml == cap && cap < max_remain && ml >= best_len) {
            full_ml = ml + mf_bt_extend(src, pos + cap, cur + cap, 0u,
                                         max_remain - cap);
        }

        if (collect && full_ml > best_len) {
            best_len = full_ml;
            best_off = pos - cur;
        }

        /* Non-dominated match list (find_multi mode). Same replace-or-
         * append rule as mf_hashchain: a longer match at a closer offset
         * supersedes the previous entry; a longer match at a larger
         * offset appends a new Pareto point. The btree visits nodes in
         * tree-descent order (not offset order), so replacements are
         * more common here than in the hashchain's newest-first walk. */
        if (matches && full_ml >= LZ_MIN_MATCH && full_ml > prev_best_len) {
            uint32_t off = pos - cur;
            if (n_matches > 0u && matches[n_matches - 1u].off >= off) {
                matches[n_matches - 1u].len = full_ml;
                matches[n_matches - 1u].off = off;
            } else if (n_matches < max_matches) {
                matches[n_matches].len = full_ml;
                matches[n_matches].off = off;
                n_matches++;
            }
            prev_best_len = full_ml;
        }

        /* Match runs to the end of the buffer — the two keys are
         * indistinguishable at any cap we could apply, and also the
         * direction read src[pos+ml] would be out of bounds. Cross-link
         * cur's subtrees under pos so cur's descendants aren't orphaned.
         * In insert-only mode we only detect this when cap == max_remain
         * (ml reaches max_remain via the capped extend itself); that's
         * sufficient because cap < max_remain guarantees ml < max_remain,
         * so the direction read below is safe. */
        uint32_t eff_ml = collect ? full_ml : ml;
        if (eff_ml >= max_remain) {
            *ptr_lt = ctx->bt_right[cur];
            *ptr_gt = ctx->bt_left[cur];
            ctx->last_insert_pos = pos;
            if (best_off_out)  *best_off_out  = best_off;
            if (n_matches_out) *n_matches_out = n_matches;
            return (best_len >= LZ_MIN_MATCH) ? best_len : 0u;
        }

        /* Direction of descent driven by the first differing byte at `ml`
         * (below the cap). full_ml is only used for match bookkeeping;
         * for splay direction we must use the capped extension so the
         * len_lt / len_gt invariants hold uniformly. */
        if (src[pos + ml] < src[cur + ml]) {
            /* pos's key < cur's key — cur enters pos's right subtree. */
            *ptr_gt = cur;
            ptr_gt  = &ctx->bt_left[cur];
            cur     = ctx->bt_left[cur];
            len_gt  = ml;
        } else {
            /* pos's key > cur's key — cur enters pos's left subtree. */
            *ptr_lt = cur;
            ptr_lt  = &ctx->bt_right[cur];
            cur     = ctx->bt_right[cur];
            len_lt  = ml;
        }
        depth--;
    }

    /* Terminate pos's subtrees. If we stopped because cur < min_pos, the
     * subtree we would have linked through cur is entirely out of window
     * (children are always older than their parent in this tree), so
     * dropping it here is lossless for in-window matches. */
    *ptr_lt = MF_NULL;
    *ptr_gt = MF_NULL;

    ctx->last_insert_pos = pos;
    if (best_off_out)  *best_off_out  = best_off;
    if (n_matches_out) *n_matches_out = n_matches;
    return (best_len >= LZ_MIN_MATCH) ? best_len : 0u;
}

/* ----- Vtable methods ---------------------------------------------------- */

static uint32_t mf_btree_find_best(tdc_lz_mf_ctx *ctx, uint32_t pos,
                                    uint32_t extend_cap,
                                    uint32_t *out_off) {
    uint32_t best_off = 0u;
    uint32_t len = mf_bt_descent(ctx, pos, extend_cap,
                                  NULL, 0u, NULL, &best_off);
    *out_off = best_off;
    return len;
}

static uint32_t mf_btree_find_multi(tdc_lz_mf_ctx *ctx, uint32_t pos,
                                     uint32_t extend_cap,
                                     LzOptMatch *out, uint32_t max_matches) {
    if (max_matches == 0u) return 0u;
    uint32_t n = 0u;
    uint32_t dummy_off = 0u;
    (void)mf_bt_descent(ctx, pos, extend_cap,
                         out, max_matches, &n, &dummy_off);
    return n;
}

static void mf_btree_insert(tdc_lz_mf_ctx *ctx, uint32_t pos) {
    /* find_best / find_multi already insert (bt4 combines both). Skip the
     * redundant splay when the last descent was at this same pos. */
    if (ctx->last_insert_pos == pos) return;
    /* Bounded cap: insert only needs enough bytes to pick a splay direction.
     * The descent detects collect==false and skips the O(max_remain) cap-hit
     * full extend, so per-candidate work is O(cap). Without this bound,
     * periodic inputs would do O(src_size) extends per candidate and hang. */
    (void)mf_bt_descent(ctx, pos, MF_BT_INSERT_EXTEND_CAP, NULL, 0u, NULL, NULL);
}

/* ----- Vtable ------------------------------------------------------------ */

const tdc_lz_mf_vt tdc_lz_mf_btree_vt = {
    .name        = "btree",
    .create      = mf_btree_create,
    .find_best   = mf_btree_find_best,
    .find_multi  = mf_btree_find_multi,
    .insert      = mf_btree_insert,
    .destroy     = mf_btree_destroy,
};
