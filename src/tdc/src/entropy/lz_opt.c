/*
 * src/entropy/lz_opt.c
 *
 * TDC_ENTROPY_LZ_OPT — optimal-parsing LZ77 encoder.
 *
 * Emits the SAME on-disk format as TDC_ENTROPY_LZ (greedy). The decoder
 * (lz_decode_core in lz.c) round-trips both without change — optimal
 * parsing is a pure encode-side optimization.
 *
 * Greedy picks the longest match at each position (locally optimal, but
 * globally suboptimal: a shorter match now can let a much cheaper match
 * start one byte later). Optimal parsing computes the globally minimum
 * cost parse via forward DP over byte positions. Expected gain on
 * structured tabular data: 5–15% smaller output than greedy, at 5–10×
 * the encode time.
 *
 * Structure:
 *   1. Hash-chain match finder (deflate-style, 1 MiB window, depth ≤ 128).
 *   2. Forward DP with sliding-window minima per literal-run bracket.
 *   3. Backtrack → LZSeq array, serialized via the shared writer in
 *      lz.c (tdc_lz_serialize_sequences).
 */

#include "tdc/entropy.h"
#include "entropy_internal.h"
#include "lz_internal.h"
#include "match_finder.h"
#include "../core/buffer.h"
#include "../core/log.h"
#include "../core/simd.h"
#include "../core/timer.h"

#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>   /* malloc/free in lz_split_decode (decode-side temp buffers) */
#include <limits.h>

#if defined(_MSC_VER)
#  include <intrin.h>
#endif

/* Per-phase timing + branch counters for tdc_lz_parse_optimal_streams.
 * Enable via TDC_LZ_OPT_TIMING=1. One line per initial-parse call. */
static int lz_opt_timing_enabled(void) {
    static int checked = 0;
    static int enabled = 0;
    if (!checked) {
        const char *v = getenv("TDC_LZ_OPT_TIMING");
        enabled = (v && v[0] && v[0] != '0');
        checked = 1;
    }
    return enabled;
}

/* ----- Tunable constants ------------------------------------------------- */

#define LZ_OPT_HASH_BITS   18
#define LZ_OPT_HASH_SIZE   (1u << LZ_OPT_HASH_BITS)

/* Chain walk depth limit. Bounds per-position chain-walk cost.
 *
 * Sweep on PRED2D+BSHUF residuals (rast2d u16 2048×2048):
 *   chain=256 -> 0.3 MB/s @ 2.11x   (deep matches lose to leb128 offset cost)
 *   chain=64  -> 1.2 MB/s @ 2.14x
 *   chain=32  -> 2.1 MB/s @ 2.16x
 *   chain=16  -> 3.4 MB/s @ 2.15x   <-- elbow, settled here
 *   chain=8   -> 4.9 MB/s @ 2.11x   (now missing useful matches)
 *
 * On periodic data with very long matches, COMMIT_LEN's fast-skip absorbs
 * most of the cost regardless of chain depth, so 16 is also a non-loss
 * there. Revisit only if a workload surfaces that wants depth >16. */
#define LZ_OPT_MAX_CHAIN   16u

/* Per-candidate extension cap. Each chain candidate is extended by at most
 * this many bytes during the walk. Keeps chain-walk work bounded at
 * O(LZ_OPT_MAX_CHAIN × LZ_OPT_EXTEND_CAP) per position regardless of
 * input entropy. On periodic inputs the longest candidate would otherwise
 * extend to end-of-input, giving an O(N²) walk. */
#define LZ_OPT_EXTEND_CAP  64u

/* Commit-and-skip threshold. If the longest match at position r (after
 * greedy-extending past the cap) reaches this length, we emit the full
 * match as a single DP transition and fast-forward r past the match,
 * skipping the inner L loop and further match-finding inside the match.
 * This matches greedy parsing in long-match regions (preserving the
 * "opt ≤ greedy" size invariant) and eliminates the O(N²) behaviour on
 * repetitive inputs. 32 is comfortably above the leb128 first breakpoint
 * (match_len = 17) so typical structured matches still go through the
 * full DP inner loop. */
#define LZ_OPT_COMMIT_LEN  256u

/* Literal-run brackets. Bracket b covers lit_run ∈ [start[b], end[b]]
 * inclusive on both ends. Penalty charged at match emission: b bytes
 * (one byte per chained-255 varint position). Covers lit_run up to 1799;
 * longer runs are handled by a "tail" Pareto set that tracks non-dominated
 * (post_match[p], p) pairs among positions aged out of bracket 7, and
 * computes exact cost at use time. Simple-domination test: (pm_a, p_a)
 * dominates (pm_b, p_b) iff pm_a ≤ pm_b AND p_a ≥ p_b. A single-slot
 * tail (min pm only) is insufficient when a later position with slightly
 * larger pm but much larger p gives a cheaper run cost at query time —
 * this is exactly what the "opt ≤ greedy" invariant needs on mixed
 * inputs where greedy finds lucky late matches. */
#define LZ_OPT_NUM_BRACKETS 8
#define LZ_OPT_TAIL_CAP     16

static const uint32_t lz_opt_bracket_start[LZ_OPT_NUM_BRACKETS] = {
    0u,   15u,  270u, 525u, 780u, 1035u, 1290u, 1545u
};
static const uint32_t lz_opt_bracket_end[LZ_OPT_NUM_BRACKETS] = {
    14u,  269u, 524u, 779u, 1034u, 1289u, 1544u, 1799u
};

/* Monotonic deque capacity (per bracket). Max window width is 255
 * (brackets 1..7). 512 gives plenty of headroom and a power-of-two mask. */
#define LZ_OPT_DEQUE_CAP   512u
#define LZ_OPT_DEQUE_MASK  (LZ_OPT_DEQUE_CAP - 1u)

/* "Infinity" marker for unreachable DP states. Costs are in bits and
 * bounded by 8*N where N ≤ 2^32, so INT32_MAX is safe. */
#define LZ_OPT_INF INT32_MAX

/* ----- Allocation helpers (realloc_fn passthrough) ----------------------- */

static void *lz_opt_alloc(tdc_buffer *buf, size_t n) {
    return buf->realloc_fn(buf->user, NULL, n);
}

static void lz_opt_free(tdc_buffer *buf, void *p) {
    if (p) (void)buf->realloc_fn(buf->user, p, 0);
}

/* The 4-byte hash + chain walk + chain insert all live in mf_hashchain.c
 * now. The DP only sees the abstract match finder (tdc_lz_mf_vt) and
 * never reaches into the bucket array directly. */

/* ----- Match-length extension (bytes) ------------------------------------ *
 * Mirrors the ml_m3-branch in lz_seq_encoded_size: ml_m3 >= 15 iff
 * L >= LZ_MIN_MATCH + 15 = 18. The cost model must match the serializer
 * exactly or the "optimal ≤ greedy" invariant can break. */

static inline uint32_t lz_opt_match_ext(uint32_t L) {
    if (L < LZ_MIN_MATCH + 15u) return 0u;
    return lz_leb128_size(L - LZ_MIN_MATCH - 15u);
}

/* Literal-run extension bytes, exact. Mirrors the lit_len branch of
 * lz_seq_encoded_size: 0 bytes for run < 15, else (run-15)/255 + 1. */
static inline uint32_t lz_opt_lit_ext(uint32_t run) {
    if (run < 15u) return 0u;
    return (run - 15u) / 255u + 1u;
}

/* Extend a candidate at (pos, cand) up to `max_len` bytes. Returns the
 * match length (may be 0). Uses 8-byte compare + CTZ for speed. */
static inline uint32_t lz_opt_extend(const uint8_t *src, uint32_t pos,
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

/* Both find-paths previously implemented here (single-best
 * lz_opt_find_longest and multi-best lz_opt_find_matches) now go through
 * the match-finder vtable. The hashchain backend in mf_hashchain.c
 * carries the equivalent walk: chain depth bound, per-candidate
 * extension cap with full-extend on cap-hit best candidates, prefetch
 * on the next chain link. The LzOptMatch type lives in match_finder.h. */
#define LZ_OPT_MAX_MATCHES 6u

/* ----- Monotonic deque (sliding-window min) ------------------------------ *
 *
 * Each bracket owns one deque tracking (position, g) pairs where
 * g(p) = post_match[p] - 8*p. The deque is kept monotonically
 * increasing in g: front holds the minimum in the current window.
 *
 * Circular buffer with power-of-two capacity. head == tail means empty.
 */
typedef struct {
    uint32_t pos[LZ_OPT_DEQUE_CAP];
    int32_t  g[LZ_OPT_DEQUE_CAP];
    uint32_t head;
    uint32_t tail;
} Lz2OptDeque;

static inline void deque_init(Lz2OptDeque *q) {
    q->head = 0u;
    q->tail = 0u;
}

static inline int deque_empty(const Lz2OptDeque *q) {
    return q->head == q->tail;
}

static inline int32_t deque_front_g(const Lz2OptDeque *q) {
    return q->g[q->head];
}

static inline uint32_t deque_front_pos(const Lz2OptDeque *q) {
    return q->pos[q->head];
}

/* Push (pos, g) at the back. Drops any tail entries with g >= new g
 * (those positions are dominated and will never be the window min). */
static inline void deque_push_back(Lz2OptDeque *q, uint32_t pos, int32_t g) {
    while (q->head != q->tail) {
        uint32_t t = (q->tail - 1u) & LZ_OPT_DEQUE_MASK;
        if (q->g[t] < g) break;
        q->tail = t;
    }
    q->pos[q->tail] = pos;
    q->g[q->tail] = g;
    q->tail = (q->tail + 1u) & LZ_OPT_DEQUE_MASK;
}

/* If the front position equals `pos`, pop it. Called when a position
 * slides out of a bracket's window on the left. No-op if the front
 * entry was already dominated out by a later push_back. */
static inline void deque_pop_front_if(Lz2OptDeque *q, uint32_t pos) {
    if (q->head != q->tail && q->pos[q->head] == pos) {
        q->head = (q->head + 1u) & LZ_OPT_DEQUE_MASK;
    }
}

/* ----- Backtrack entry --------------------------------------------------- */

typedef struct {
    uint32_t prev_p;   /* source post-match state for this transition */
    uint32_t r_start;  /* match START position (prev_p + lit_run) */
    uint32_t L;        /* match length */
    uint32_t off;      /* back-reference offset */
} Lz2OptBacktrack;

/* ----- Forward DP -------------------------------------------------------- *
 *
 * State:
 *   post_match[p] = min cost (in bits) of encoding src[0..p) such that
 *                   position p is either 0 or immediately after a
 *                   completed match.
 *   best_start[r] = min cost of reaching r as a match-start position,
 *                   = min over p of (post_match[p] + S[r]-S[p] +
 *                                     8*bracket(r-p))
 *                   Computed via sliding-window minima of (post_match[p]
 *                   - S[p]) per bracket, where S is the Huffman prefix sum.
 *
 * Transitions:
 *   For each r with matches[r].len >= LZ_MIN_MATCH:
 *     hdr = 8*(1 + lz_offset_size(match_off))     // tag + varint offset
 *     For each L in [LZ_MIN_MATCH, matches[r].len]:
 *       candidate = best_start[r] + hdr + 8*match_ext(L)
 *       post_match[r + L] = min(., candidate)
 *
 * Final:
 *   answer = min over p of (post_match[p] + S[N]-S[p])
 *            -- trailing literals, no header.
 */
tdc_status tdc_lz_parse_optimal(const uint8_t *src, uint32_t src_size,
                                 tdc_buffer *dst,
                                 LZSeq **out_seqs, uint32_t *out_seq_count)
{
    *out_seqs = NULL;
    *out_seq_count = 0u;

    if (src_size < LZ_MIN_MATCH + 1u) {
        /* Below minimum — no sequences, caller will treat as literal-only. */
        return TDC_OK;
    }

    uint32_t N = src_size;

    /* ---- Allocate DP state and match-finder ---- */
    int32_t *post_match = (int32_t *)lz_opt_alloc(dst, (size_t)(N + 1u) * sizeof(int32_t));
    Lz2OptBacktrack *bt = (Lz2OptBacktrack *)lz_opt_alloc(dst,
        (size_t)(N + 1u) * sizeof(Lz2OptBacktrack));

    const tdc_lz_mf_vt *mf = tdc_lz_mf_select(src_size, /*prefer_quality=*/1);
    tdc_lz_mf_params mf_params = {
        .chain_depth = LZ_OPT_MAX_CHAIN - 1u,  /* MF walks chain_depth + 1 total */
        .hash_bits   = LZ_OPT_HASH_BITS,
    };
    tdc_lz_mf_ctx *mf_ctx = mf->create(src, src_size, &mf_params, dst);

    if (!post_match || !bt || !mf_ctx) {
        lz_opt_free(dst, post_match);
        lz_opt_free(dst, bt);
        if (mf_ctx) mf->destroy(mf_ctx, dst);
        return TDC_E_NOMEM;
    }

    for (uint32_t i = 0u; i <= N; i++) post_match[i] = LZ_OPT_INF;
    post_match[0] = 0;
    memset(bt, 0, (size_t)(N + 1u) * sizeof(Lz2OptBacktrack));

    /* Sliding-window deques, one per bracket. Stack-allocated (~32 KB). */
    Lz2OptDeque deques[LZ_OPT_NUM_BRACKETS];
    for (int i = 0; i < LZ_OPT_NUM_BRACKETS; i++) deque_init(&deques[i]);

    /* Tail Pareto set: non-dominated (pm, p) pairs among positions aged
     * out of bracket 7. Ordered by p (ascending = age-out order).
     * Invariant: pm is strictly decreasing from front to back, so no
     * entry is simply-dominated by any other. At query time, evaluate
     * all entries and take the min cost. */
    int32_t  tail_pm[LZ_OPT_TAIL_CAP];
    uint32_t tail_p[LZ_OPT_TAIL_CAP];
    uint32_t tail_size = 0u;

    /* Skip-ahead counter: when a long match is committed at position r,
     * positions (r, r + match_len) are fast-forwarded through the DP —
     * deque slide and chain insertion still run, but match finding and
     * the inner L loop are bypassed. */
    uint32_t skip_until = 0u;

    /* ---- Main DP loop ---- */
    for (uint32_t r = 0u; r < N; r++) {
        /* Step A: slide each bracket's window by one. For bracket b,
         * the window covers lit_run ∈ [bracket_start[b], bracket_end[b]],
         * i.e. p ∈ [r - bracket_end[b], r - bracket_start[b]]. As r
         * advances by 1, we add p = r - bracket_start[b] on the right
         * and pop p = r - bracket_end[b] - 1 from the left. The position
         * popped from the oldest bracket becomes a tail candidate. */
        for (int b = 0; b < LZ_OPT_NUM_BRACKETS; b++) {
            uint32_t bs = lz_opt_bracket_start[b];
            uint32_t be = lz_opt_bracket_end[b];

            /* Enter: new right edge. */
            if (r >= bs) {
                uint32_t p_enter = r - bs;
                if (post_match[p_enter] != LZ_OPT_INF) {
                    int32_t g = post_match[p_enter] - (int32_t)(8u * p_enter);
                    deque_push_back(&deques[b], p_enter, g);
                }
            }
            /* Exit: old left edge leaves the window. */
            if (r > be) {
                uint32_t p_exit = r - be - 1u;
                deque_pop_front_if(&deques[b], p_exit);
                /* Position p_exit just aged out of the oldest bracket
                 * into the tail Pareto set. p_exit is strictly larger
                 * than any previously-aged position (FIFO). Simple
                 * domination: new (pm, p_exit) with p_exit > all existing
                 * p's dominates any existing entry whose pm ≥ new pm.
                 * Pop back those, then push new if not dominated by the
                 * remaining front (which has smaller p; it dominates new
                 * only if front pm ≤ new pm, but by invariant front pm
                 * is the LARGEST in the set, so we only skip the push
                 * if all existing pm ≤ new pm, i.e. back pm ≤ new pm). */
                if (b == LZ_OPT_NUM_BRACKETS - 1 &&
                    post_match[p_exit] != LZ_OPT_INF) {
                    int32_t new_pm = post_match[p_exit];
                    while (tail_size > 0u &&
                           tail_pm[tail_size - 1u] >= new_pm) {
                        tail_size--;
                    }
                    /* After pop-back, surviving entries all have pm < new_pm.
                     * Append new_pm unless the capacity is exhausted (drop
                     * the oldest front entry to make room — it has the
                     * largest pm and is least likely to win). */
                    if (tail_size == LZ_OPT_TAIL_CAP) {
                        for (uint32_t i = 1u; i < tail_size; i++) {
                            tail_pm[i - 1u] = tail_pm[i];
                            tail_p[i - 1u]  = tail_p[i];
                        }
                        tail_size--;
                    }
                    tail_pm[tail_size] = new_pm;
                    tail_p[tail_size]  = p_exit;
                    tail_size++;
                }
            }
        }

        /* Step B: compute best_start[r] = min over brackets of
         *   (front g + 8*r + 8*b)  where b is the bracket index. */
        int32_t best_start = LZ_OPT_INF;
        uint32_t best_start_prev_p = 0u;
        for (int b = 0; b < LZ_OPT_NUM_BRACKETS; b++) {
            if (deque_empty(&deques[b])) continue;
            int32_t g_min = deque_front_g(&deques[b]);
            int32_t cand = g_min + (int32_t)(8u * r) + (int32_t)(8u * (uint32_t)b);
            if (cand < best_start) {
                best_start = cand;
                best_start_prev_p = deque_front_pos(&deques[b]);
            }
        }

        /* Tail candidates: positions aged out of bracket 7 can still reach r
         * through a long literal run. Cost = post_match[p] + 8*(r-p) +
         * 8*lit_ext(r-p), computed exactly at use time. Iterate the full
         * Pareto set — a single-slot tail (min pm only) misses the case
         * where a larger-p entry with slightly larger pm gives cheaper
         * total cost at specific r values. The tail is capped at
         * LZ_OPT_TAIL_CAP entries so this loop is O(1) per r. */
        for (uint32_t ti = 0u; ti < tail_size; ti++) {
            if (tail_p[ti] >= r) continue;
            uint32_t run = r - tail_p[ti];
            int32_t cand = tail_pm[ti] + (int32_t)(8u * run) +
                           (int32_t)(8u * lz_opt_lit_ext(run));
            if (cand < best_start) {
                best_start = cand;
                best_start_prev_p = tail_p[ti];
            }
        }

        /* Fast-forward through positions inside a previously committed
         * long match — still insert into the chain so later positions can
         * find matches into the skipped range, but skip match finding and
         * the inner L loop. */
        if (r < skip_until) {
            mf->insert(mf_ctx, r);
            continue;
        }

        if (best_start >= LZ_OPT_INF) {
            /* No reachable predecessor; still insert into the chain. */
            mf->insert(mf_ctx, r);
            continue;
        }

        /* Step C: find the longest match at r (before inserting r itself
         * into the chain, so the walk starts from strictly-earlier
         * positions). The MF caps each candidate's extension at
         * LZ_OPT_EXTEND_CAP and full-extends any cap-hitting candidate
         * that could be the new best — keeping the "opt ≤ greedy"
         * invariant while bounding per-position work on periodic data. */
        uint32_t match_off = 0u;
        uint32_t match_len = mf->find_best(mf_ctx, r, LZ_OPT_EXTEND_CAP,
                                            &match_off);

        /* Insert r into the chain after the lookup. */
        mf->insert(mf_ctx, r);

        if (match_len < LZ_MIN_MATCH) continue;

        /* Match header cost in bits: 1 tag byte + variable offset. The
         * offset is the same for all L at this position, so compute once. */
        uint32_t match_hdr_bits = 8u * (1u + lz_offset_size(match_off));

        /* Commit-and-skip path: on long matches, emit a single transition
         * (matching greedy's behaviour) and fast-forward past the match.
         * The next non-skipped r is at r + match_len. */
        if (match_len >= LZ_OPT_COMMIT_LEN) {
            uint32_t ext = lz_opt_match_ext(match_len);
            int32_t cand = best_start + (int32_t)match_hdr_bits + (int32_t)(8u * ext);
            uint32_t end = r + match_len;
            if (cand < post_match[end]) {
                post_match[end] = cand;
                bt[end].prev_p = best_start_prev_p;
                bt[end].r_start = r;
                bt[end].L = match_len;
                bt[end].off = match_off;
            }
            skip_until = end;
            continue;
        }

        /* Step D: explore every L in [LZ_MIN_MATCH, match_len]. For
         * short-to-moderate matches this is bounded by LZ_OPT_COMMIT_LEN
         * so the inner loop is O(1) per r. */
        for (uint32_t L = LZ_MIN_MATCH; L <= match_len; L++) {
            uint32_t ext = lz_opt_match_ext(L);
            int32_t cand = best_start + (int32_t)match_hdr_bits + (int32_t)(8u * ext);
            uint32_t end = r + L;
            if (cand < post_match[end]) {
                post_match[end] = cand;
                bt[end].prev_p = best_start_prev_p;
                bt[end].r_start = r;
                bt[end].L = L;
                bt[end].off = match_off;
            }
        }
    }

    /* ---- Final: best terminal (trailing literals, no header) ---- */
    uint32_t best_final_p = 0u;
    int32_t  best_final_cost = (int32_t)(8u * N); /* all-literal path */
    for (uint32_t p = 1u; p <= N; p++) {
        if (post_match[p] == LZ_OPT_INF) continue;
        int32_t c = post_match[p] + (int32_t)(8u * (N - p));
        if (c < best_final_cost) {
            best_final_cost = c;
            best_final_p = p;
        }
    }

    /* ---- Backtrack: reconstruct LZSeq array ---- */
    uint32_t seq_count = 0u;
    {
        uint32_t cur = best_final_p;
        while (cur > 0u) {
            seq_count++;
            cur = bt[cur].prev_p;
        }
    }

    LZSeq *seqs = NULL;
    if (seq_count > 0u) {
        seqs = (LZSeq *)lz_opt_alloc(dst, (size_t)seq_count * sizeof(LZSeq));
        if (!seqs) {
            lz_opt_free(dst, post_match);
            lz_opt_free(dst, bt);
            mf->destroy(mf_ctx, dst);
            return TDC_E_NOMEM;
        }
        uint32_t cur = best_final_p;
        uint32_t idx = seq_count;
        while (cur > 0u) {
            idx--;
            uint32_t prev_p = bt[cur].prev_p;
            seqs[idx].lit_len   = bt[cur].r_start - prev_p;
            seqs[idx].match_len = bt[cur].L;
            seqs[idx].match_off = bt[cur].off;
            cur = prev_p;
        }
    }

    /* ---- Free DP scratch ---- */
    lz_opt_free(dst, post_match);
    lz_opt_free(dst, bt);
    mf->destroy(mf_ctx, dst);

    *out_seqs = seqs;
    *out_seq_count = seq_count;
    return TDC_OK;
}

/* ----- STREAMS-aware optimal parse with repcode-aware cost model --------- *
 *
 * The legacy tdc_lz_parse_optimal above uses the single-stream cost model
 * (8 bits per literal, variable-width match header). For the multi-stream
 * STREAMS format, the post-entropy costs differ:
 *
 *   c_lit         ≈ 6   bits/byte  (measured H_lit on structured data)
 *   c_match_rep   ≈ 12  bits/seq   (lit_len + match_len + rep code ~2 bits)
 *   c_match_novel ≈ 37  bits/seq   (lit_len + match_len + full offset ~27 bits)
 *   match_ext     ≈ 0              (match_len folds into the ml stream)
 *   lit_ext       ≈ 0              (lit_len folds into the ll stream)
 *
 * The key insight: after repcode encoding and per-stream entropy coding, a
 * match at a recently-used offset (rep1/rep2/rep3) is ~3× cheaper than a
 * match at a novel offset. The previous flat-cost model (31 bits/match for
 * all offsets) caused the parser to emit thousands of short matches at
 * diverse offsets, inflating the match_off stream to 44% of total output.
 *
 * This version tracks rep state (rep1, rep2, rep3) through the forward DP.
 * At each position r, it checks:
 *   1. The longest match from the hash chain (as before)
 *   2. Matches at each rep offset (simple memcmp extension — cheap)
 * Rep matches get charged c_match_rep; all others get c_match_novel.
 *
 * Rep state is approximated: each position stores the rep state from the
 * single best-cost path reaching it (not all Pareto-optimal rep states).
 * This is the same approximation zstd's btopt uses.
 *
 * With lit_ext = 0, the DP remains a prefix-minimum — O(1) per r plus
 * the match-finder work, now with up to 4 transitions per position
 * (1 hash-chain + 3 rep candidates).
 *
 * Cost units: 8ths of a bit (integer arithmetic, no fractional loss).
 */

#define LZ_OPT_STREAMS_LIT_COST     56   /* 7.0 bits × 8 (lit stream entropy) */
#define LZ_OPT_STREAMS_REP_COST     80   /* 10.0 bits × 8: ll/ml/off syms + ll/ml extra */

/* Offset-aware approximate cost for a novel (non-repcode) match.
 * Base overhead: ll_sym + ml_sym + off_sym entropy ≈ 12 bits.
 * Plus floor(log2(off)) extra bits for the offset's raw remainder.
 * Units: 8ths of a bit (matching C_LIT and C_REP). */
static inline int64_t lz_opt_novel_cost(uint32_t off) {
    int64_t base = 96;  /* 12 bits × 8 */
    if (off <= 1u) return base;
    uint32_t lg = 0, t = off;
    while (t > 1u) { t >>= 1; lg++; }
    return base + (int64_t)lg * 8;
}

/* Repcode init values live in lz_internal.h (shared with greedy parser
 * in lz.c and streams serializer in lz_streams.c). */
#define LZ_OPT_REP_INIT_1   LZ_REP_INIT_1
#define LZ_OPT_REP_INIT_2   LZ_REP_INIT_2
#define LZ_OPT_REP_INIT_3   LZ_REP_INIT_3

/* Rep state at each DP position. Stored in a parallel array. */
typedef struct {
    uint32_t r1, r2, r3;
} Lz2OptRepState;

/* Compute the new rep state after emitting a match with offset `off`. */
static inline Lz2OptRepState lz_opt_rep_update(Lz2OptRepState st, uint32_t off) {
    if (off == st.r1) return st;                                  /* no change */
    if (off == st.r2) { st.r2 = st.r1; st.r1 = off; return st; } /* swap 1↔2 */
    if (off == st.r3) { Lz2OptRepState n = {off, st.r1, st.r2}; return n; }
    Lz2OptRepState n = {off, st.r1, st.r2};
    return n;
}

/* Extend a rep match at position pos with back-reference pos-off.
 * Returns match length (0 if offset is out of range or < LZ_MIN_MATCH). */
static inline uint32_t lz_opt_rep_extend(const uint8_t *src, uint32_t src_size,
                                          uint32_t pos, uint32_t off) {
    if (off == 0u || off > pos) return 0u;
    uint32_t cand = pos - off;
    uint32_t max_remain = src_size - pos;
    /* Cap to LZ_OPT_COMMIT_LEN. On periodic inputs (smooth f64, residuals
     * with a recurring offset), an uncapped rep extend walks for thousands
     * of bytes; called up to 6× per position × 3 priced passes this drives
     * the parser to ~0.4 MB/s on 16 MiB f64.
     *
     * Why COMMIT_LEN and not EXTEND_CAP: at lengths >= COMMIT_LEN, the
     * DP commit-and-skip path fires (post_match update + skip_until), so
     * the parser fast-forwards past the match without further per-byte
     * DP work. Capping at COMMIT_LEN means we recognize "this is a long
     * match, commit it" without walking arbitrarily further; the actual
     * match length is reconstructed at decode from the emitted token
     * (we may under-report by truncating, accepting a small ratio cost
     * on inputs whose typical rep extends past 256). */
    uint32_t cap = (max_remain < LZ_OPT_COMMIT_LEN) ? max_remain : LZ_OPT_COMMIT_LEN;
    return lz_opt_extend(src, pos, cand, cap);
}

/* Emit a DP transition: if candidate cost < current best at `end`,
 * update post_match, backtrack, and rep state. */
static inline void lz_opt_streams_emit(
    int64_t cand_cost, uint32_t end,
    uint32_t prev_p, uint32_t r_start, uint32_t L, uint32_t off,
    Lz2OptRepState new_rep,
    int64_t *post_match, Lz2OptBacktrack *bt, Lz2OptRepState *rep_at)
{
    if (cand_cost < post_match[end]) {
        post_match[end] = cand_cost;
        bt[end].prev_p  = prev_p;
        bt[end].r_start = r_start;
        bt[end].L       = L;
        bt[end].off     = off;
        rep_at[end]     = new_rep;
    }
}

tdc_status tdc_lz_parse_optimal_streams(const uint8_t *src, uint32_t src_size,
                                         tdc_buffer *dst,
                                         LZSeq **out_seqs,
                                         uint32_t *out_seq_count)
{
    *out_seqs = NULL;
    *out_seq_count = 0u;

    if (src_size < LZ_MIN_MATCH + 1u) return TDC_OK;

    const int timing = lz_opt_timing_enabled();
    const double t_start = timing ? tdc_now_secs() : 0.0;

    uint32_t N = src_size;

    int64_t *post_match = (int64_t *)lz_opt_alloc(dst, (size_t)(N + 1u) * sizeof(int64_t));
    Lz2OptBacktrack *bt = (Lz2OptBacktrack *)lz_opt_alloc(dst,
        (size_t)(N + 1u) * sizeof(Lz2OptBacktrack));
    Lz2OptRepState *rep_at = (Lz2OptRepState *)lz_opt_alloc(dst,
        (size_t)(N + 1u) * sizeof(Lz2OptRepState));

    const tdc_lz_mf_vt *mf = tdc_lz_mf_select(src_size, /*prefer_quality=*/1);
    tdc_lz_mf_params mf_params = {
        .chain_depth = LZ_OPT_MAX_CHAIN - 1u,
        .hash_bits   = LZ_OPT_HASH_BITS,
    };
    tdc_lz_mf_ctx *mf_ctx = mf->create(src, src_size, &mf_params, dst);

    if (!post_match || !bt || !rep_at || !mf_ctx) {
        lz_opt_free(dst, post_match);
        lz_opt_free(dst, bt);
        lz_opt_free(dst, rep_at);
        if (mf_ctx) mf->destroy(mf_ctx, dst);
        return TDC_E_NOMEM;
    }

    const int64_t INF64 = (int64_t)1 << 62;
    for (uint32_t i = 0u; i <= N; i++) post_match[i] = INF64;
    post_match[0] = 0;
    memset(bt, 0, (size_t)(N + 1u) * sizeof(Lz2OptBacktrack));

    const double t_init = timing ? tdc_now_secs() : 0.0;

    /* Branch counters (zero cost when timing is off — compiler folds). */
    uint64_t cnt_skip = 0, cnt_inf = 0, cnt_rep_skip = 0;
    uint64_t cnt_chain = 0, cnt_chain_skip = 0;
    uint64_t rep_bytes = 0, rep_calls = 0;

    /* Initialize rep state at position 0. */
    Lz2OptRepState rep_init = {LZ_OPT_REP_INIT_1, LZ_OPT_REP_INIT_2, LZ_OPT_REP_INIT_3};
    rep_at[0] = rep_init;

    const int64_t C_LIT   = LZ_OPT_STREAMS_LIT_COST;
    const int64_t C_REP   = LZ_OPT_STREAMS_REP_COST;

    /* 2-entry prefix-minimum of (post_match[p] - p*C_LIT).
     * Primary: global best g(p).
     * Secondary: best g(p) whose rep1 differs from primary's rep1.
     * This lets the DP discover rep matches via a different rep state
     * that the single-entry prefix-min would miss entirely. */
    int64_t  best_g = INF64;
    uint32_t best_g_p = 0u;
    Lz2OptRepState best_g_rep = rep_init;

    int64_t  best_g2 = INF64;
    uint32_t best_g2_p = 0u;
    Lz2OptRepState best_g2_rep = rep_init;

    uint32_t skip_until = 0u;

    for (uint32_t r = 0u; r < N; r++) {
        /* Absorb p = r into the 2-entry prefix-min. */
        if (post_match[r] < INF64) {
            int64_t g = post_match[r] - (int64_t)r * C_LIT;
            if (g < best_g) {
                /* New global best. Push old primary to secondary if it
                 * has a different rep1 and is better than current secondary. */
                if (best_g < INF64 && best_g_rep.r1 != rep_at[r].r1
                    && best_g < best_g2) {
                    best_g2     = best_g;
                    best_g2_p   = best_g_p;
                    best_g2_rep = best_g_rep;
                }
                best_g     = g;
                best_g_p   = r;
                best_g_rep = rep_at[r];
            } else if (rep_at[r].r1 != best_g_rep.r1 && g < best_g2) {
                /* Different rep1 from primary and cheaper than secondary. */
                best_g2     = g;
                best_g2_p   = r;
                best_g2_rep = rep_at[r];
            }
        }

        int64_t  best_start = (best_g < INF64)
                               ? (best_g + (int64_t)r * C_LIT)
                               : INF64;
        uint32_t best_start_prev_p = best_g_p;
        Lz2OptRepState cur_rep = best_g_rep;

        int64_t  best_start2 = (best_g2 < INF64)
                                ? (best_g2 + (int64_t)r * C_LIT)
                                : INF64;
        uint32_t best_start2_prev_p = best_g2_p;
        Lz2OptRepState cur_rep2 = best_g2_rep;

        /* Fast-forward inside a committed long match — chain insert only. */
        if (r < skip_until) {
            cnt_skip++;
            mf->insert(mf_ctx, r);
            continue;
        }

        if (best_start >= INF64) {
            cnt_inf++;
            mf->insert(mf_ctx, r);
            continue;
        }

        /* ----- Rep-match candidates ------------------------------------ */
        /* Check matches at rep1, rep2, rep3 offsets from both prefix-min
         * entries. The secondary entry has a different rep1 and may
         * discover rep matches that the primary misses entirely. */
        uint32_t max_rep_rlen = 0u;
        {
            uint32_t reps[3] = {cur_rep.r1, cur_rep.r2, cur_rep.r3};
            for (int ri = 0; ri < 3; ri++) {
                uint32_t rlen = lz_opt_rep_extend(src, src_size, r, reps[ri]);
                rep_calls++; rep_bytes += rlen;
                if (rlen < LZ_MIN_MATCH) continue;
                if (rlen > max_rep_rlen) max_rep_rlen = rlen;
                int64_t cand_cost = best_start + C_REP;
                Lz2OptRepState new_rep = lz_opt_rep_update(cur_rep, reps[ri]);
                lz_opt_streams_emit(cand_cost, r + rlen,
                    best_start_prev_p, r, rlen, reps[ri], new_rep,
                    post_match, bt, rep_at);
            }
        }

        /* Secondary entry rep matches. Only try offsets that differ from
         * the primary's rep set — identical offsets can't beat primary. */
        if (best_start2 < INF64) {
            uint32_t reps2[3] = {cur_rep2.r1, cur_rep2.r2, cur_rep2.r3};
            for (int ri = 0; ri < 3; ri++) {
                if (reps2[ri] == cur_rep.r1 || reps2[ri] == cur_rep.r2 ||
                    reps2[ri] == cur_rep.r3) continue;
                uint32_t rlen = lz_opt_rep_extend(src, src_size, r, reps2[ri]);
                rep_calls++; rep_bytes += rlen;
                if (rlen < LZ_MIN_MATCH) continue;
                if (rlen > max_rep_rlen) max_rep_rlen = rlen;
                int64_t cand_cost = best_start2 + C_REP;
                Lz2OptRepState new_rep = lz_opt_rep_update(cur_rep2, reps2[ri]);
                lz_opt_streams_emit(cand_cost, r + rlen,
                    best_start2_prev_p, r, rlen, reps2[ri], new_rep,
                    post_match, bt, rep_at);
            }
        }

        /* Commit-and-skip on long rep matches. The rep_extend is capped at
         * LZ_OPT_COMMIT_LEN, so a rlen at the cap means the match continues
         * past the cap; fast-forward the parser past it (the next position
         * can re-discover the rest as another rep). Without this, periodic
         * inputs (smooth f64 at stride 8, residual planes with a recurring
         * offset) re-extend the full 256-byte cap at every position, making
         * L0 encode run at <1 MB/s on 16 MiB inputs. */
        if (max_rep_rlen >= LZ_OPT_COMMIT_LEN) {
            cnt_rep_skip++;
            mf->insert(mf_ctx, r);
            skip_until = r + max_rep_rlen;
            continue;
        }

        /* ----- Hash-chain match ---------------------------------------- */
        cnt_chain++;
        uint32_t match_off = 0u;
        uint32_t match_len = mf->find_best(mf_ctx, r, LZ_OPT_EXTEND_CAP,
                                            &match_off);

        mf->insert(mf_ctx, r);

        if (match_len >= LZ_MIN_MATCH) {
            /* Is this hash-chain match at a rep offset? */
            int is_rep = (match_off == cur_rep.r1 ||
                          match_off == cur_rep.r2 ||
                          match_off == cur_rep.r3);
            int64_t c_match = is_rep ? C_REP : lz_opt_novel_cost(match_off);
            Lz2OptRepState new_rep = lz_opt_rep_update(cur_rep, match_off);

            /* Commit-and-skip for long matches. */
            if (match_len >= LZ_OPT_COMMIT_LEN) {
                cnt_chain_skip++;
                lz_opt_streams_emit(best_start + c_match, r + match_len,
                    best_start_prev_p, r, match_len, match_off, new_rep,
                    post_match, bt, rep_at);
                skip_until = r + match_len;
                continue;
            }

            /* Longest-match is dominant for same-cost matches. */
            lz_opt_streams_emit(best_start + c_match, r + match_len,
                best_start_prev_p, r, match_len, match_off, new_rep,
                post_match, bt, rep_at);
        }
    }

    const double t_loop = timing ? tdc_now_secs() : 0.0;

    /* Final: trailing literals at cost C_LIT per byte. */
    uint32_t best_final_p = 0u;
    int64_t  best_final_cost = (int64_t)N * C_LIT;  /* all-literal path */
    for (uint32_t p = 1u; p <= N; p++) {
        if (post_match[p] >= INF64) continue;
        int64_t c = post_match[p] + (int64_t)(N - p) * C_LIT;
        if (c < best_final_cost) {
            best_final_cost = c;
            best_final_p = p;
        }
    }

    uint32_t seq_count = 0u;
    {
        uint32_t cur = best_final_p;
        while (cur > 0u) {
            seq_count++;
            cur = bt[cur].prev_p;
        }
    }

    LZSeq *seqs = NULL;
    if (seq_count > 0u) {
        seqs = (LZSeq *)lz_opt_alloc(dst, (size_t)seq_count * sizeof(LZSeq));
        if (!seqs) {
            lz_opt_free(dst, post_match);
            lz_opt_free(dst, bt);
            lz_opt_free(dst, rep_at);
            mf->destroy(mf_ctx, dst);
            return TDC_E_NOMEM;
        }
        uint32_t cur = best_final_p;
        uint32_t idx = seq_count;
        while (cur > 0u) {
            idx--;
            uint32_t prev_p = bt[cur].prev_p;
            seqs[idx].lit_len   = bt[cur].r_start - prev_p;
            seqs[idx].match_len = bt[cur].L;
            seqs[idx].match_off = bt[cur].off;
            cur = prev_p;
        }
    }

    lz_opt_free(dst, post_match);
    lz_opt_free(dst, bt);
    lz_opt_free(dst, rep_at);
    mf->destroy(mf_ctx, dst);

    if (timing) {
        double t_end = tdc_now_secs();
        uint64_t cnt_total = (uint64_t)N;
        uint64_t cnt_work = cnt_total - cnt_skip - cnt_inf;
        TDC_LOG(
            "[lz_opt-time] N=%u init=%.1fms loop=%.1fms post=%.1fms\n"
            "              pos: total=%llu  skip=%llu  inf=%llu  work=%llu\n"
            "              work-branches: rep_skip=%llu  chain=%llu  chain_skip=%llu\n"
            "              rep_extend: calls=%llu  bytes=%llu  avg=%.1f/call %.2f/pos\n",
            N,
            (t_init - t_start) * 1000.0,
            (t_loop - t_init) * 1000.0,
            (t_end  - t_loop) * 1000.0,
            (unsigned long long)cnt_total,
            (unsigned long long)cnt_skip,
            (unsigned long long)cnt_inf,
            (unsigned long long)cnt_work,
            (unsigned long long)cnt_rep_skip,
            (unsigned long long)cnt_chain,
            (unsigned long long)cnt_chain_skip,
            (unsigned long long)rep_calls,
            (unsigned long long)rep_bytes,
            rep_calls ? (double)rep_bytes / (double)rep_calls : 0.0,
            cnt_total ? (double)rep_bytes / (double)cnt_total : 0.0);
    }

    *out_seqs = seqs;
    *out_seq_count = seq_count;
    return TDC_OK;
}

/* --- Symbol index helpers for the priced parser -------------------------- */

static inline uint8_t lz_opt_ml_sym(uint32_t match_len_m3) {
    if (match_len_m3 <= 1u) return (uint8_t)match_len_m3;
    uint32_t lg = 0, t = match_len_m3;
    while (t > 1u) { t >>= 1; lg++; }
    return (uint8_t)(lg + 1u);
}

static inline uint8_t lz_opt_off_sym_novel(uint32_t offset) {
    uint32_t lg = 0, t = offset;
    while (t > 1u) { t >>= 1; lg++; }
    return (uint8_t)(lg + 3u);
}

/* --- Priced STREAMS parser (second pass) -------------------------------- */
/*
 * Same structure as tdc_lz_parse_optimal_streams but uses per-symbol costs
 * from a first-pass frequency analysis. The prefix-min trick uses prefix
 * sums of per-byte literal costs instead of position × C_LIT.
 */
tdc_status tdc_lz_parse_optimal_streams_priced(
    const uint8_t *src, uint32_t src_size,
    tdc_buffer *dst,
    const LzsStreamsPrices *prices,
    LZSeq **out_seqs, uint32_t *out_seq_count)
{
    *out_seqs = NULL;
    *out_seq_count = 0u;

    if (src_size < LZ_MIN_MATCH + 1u) return TDC_OK;

    uint32_t N = src_size;

    /* Precompute prefix sums of per-byte literal costs. */
    int64_t *lit_prefix = (int64_t *)lz_opt_alloc(dst,
        (size_t)(N + 1u) * sizeof(int64_t));
    int64_t *post_match = (int64_t *)lz_opt_alloc(dst,
        (size_t)(N + 1u) * sizeof(int64_t));
    Lz2OptBacktrack *bt = (Lz2OptBacktrack *)lz_opt_alloc(dst,
        (size_t)(N + 1u) * sizeof(Lz2OptBacktrack));
    Lz2OptRepState *rep_at = (Lz2OptRepState *)lz_opt_alloc(dst,
        (size_t)(N + 1u) * sizeof(Lz2OptRepState));

    const tdc_lz_mf_vt *mf = tdc_lz_mf_select(src_size, /*prefer_quality=*/1);
    tdc_lz_mf_params mf_params = {
        .chain_depth = LZ_OPT_MAX_CHAIN - 1u,
        .hash_bits   = LZ_OPT_HASH_BITS,
    };
    tdc_lz_mf_ctx *mf_ctx = mf->create(src, src_size, &mf_params, dst);

    if (!lit_prefix || !post_match || !bt || !rep_at || !mf_ctx) {
        lz_opt_free(dst, lit_prefix);
        lz_opt_free(dst, post_match);
        lz_opt_free(dst, bt);
        lz_opt_free(dst, rep_at);
        if (mf_ctx) mf->destroy(mf_ctx, dst);
        return TDC_E_NOMEM;
    }

    /* Build literal cost prefix sums. */
    lit_prefix[0] = 0;
    for (uint32_t i = 0; i < N; i++)
        lit_prefix[i + 1] = lit_prefix[i] + prices->lit[src[i]];

    const int64_t INF64 = (int64_t)1 << 62;
    for (uint32_t i = 0u; i <= N; i++) post_match[i] = INF64;
    post_match[0] = 0;
    memset(bt, 0, (size_t)(N + 1u) * sizeof(Lz2OptBacktrack));

    Lz2OptRepState rep_init = {LZ_OPT_REP_INIT_1, LZ_OPT_REP_INIT_2, LZ_OPT_REP_INIT_3};
    rep_at[0] = rep_init;

    /* 2-entry prefix-minimum of (post_match[p] - lit_prefix[p]).
     * Primary: global best. Secondary: best with different rep1. */
    int64_t  best_g = INF64;
    uint32_t best_g_p = 0u;
    Lz2OptRepState best_g_rep = rep_init;

    int64_t  best_g2 = INF64;
    uint32_t best_g2_p = 0u;
    Lz2OptRepState best_g2_rep = rep_init;

    uint32_t skip_until = 0u;

    for (uint32_t r = 0u; r < N; r++) {
        if (post_match[r] < INF64) {
            int64_t g = post_match[r] - lit_prefix[r];
            if (g < best_g) {
                if (best_g < INF64 && best_g_rep.r1 != rep_at[r].r1
                    && best_g < best_g2) {
                    best_g2     = best_g;
                    best_g2_p   = best_g_p;
                    best_g2_rep = best_g_rep;
                }
                best_g     = g;
                best_g_p   = r;
                best_g_rep = rep_at[r];
            } else if (rep_at[r].r1 != best_g_rep.r1 && g < best_g2) {
                best_g2     = g;
                best_g2_p   = r;
                best_g2_rep = rep_at[r];
            }
        }

        int64_t  best_start = (best_g < INF64)
                               ? (best_g + lit_prefix[r])
                               : INF64;
        uint32_t best_start_prev_p = best_g_p;
        Lz2OptRepState cur_rep = best_g_rep;

        int64_t  best_start2 = (best_g2 < INF64)
                                ? (best_g2 + lit_prefix[r])
                                : INF64;
        uint32_t best_start2_prev_p = best_g2_p;
        Lz2OptRepState cur_rep2 = best_g2_rep;

        if (r < skip_until) {
            mf->insert(mf_ctx, r);
            continue;
        }

        if (best_start >= INF64) {
            mf->insert(mf_ctx, r);
            continue;
        }

        /* Rep-match candidates from primary entry. */
        uint32_t max_rep_rlen = 0u;
        {
            uint32_t reps[3] = {cur_rep.r1, cur_rep.r2, cur_rep.r3};
            for (int ri = 0; ri < 3; ri++) {
                uint32_t rlen = lz_opt_rep_extend(src, src_size, r, reps[ri]);
                if (rlen < LZ_MIN_MATCH) continue;
                if (rlen > max_rep_rlen) max_rep_rlen = rlen;
                int64_t ml_c = (int64_t)prices->ml[lz_opt_ml_sym(rlen - LZ_MIN_MATCH)];
                int64_t off_c = (int64_t)prices->off[ri];
                int64_t cand_cost = best_start + prices->ll_avg + ml_c + off_c;
                Lz2OptRepState new_rep = lz_opt_rep_update(cur_rep, reps[ri]);
                lz_opt_streams_emit(cand_cost, r + rlen,
                    best_start_prev_p, r, rlen, reps[ri], new_rep,
                    post_match, bt, rep_at);
            }
        }

        /* Rep-match candidates from secondary entry (different rep1).
         * Only try offsets not already in the primary's rep set. */
        if (best_start2 < INF64) {
            uint32_t reps2[3] = {cur_rep2.r1, cur_rep2.r2, cur_rep2.r3};
            for (int ri = 0; ri < 3; ri++) {
                if (reps2[ri] == cur_rep.r1 || reps2[ri] == cur_rep.r2 ||
                    reps2[ri] == cur_rep.r3) continue;
                uint32_t rlen = lz_opt_rep_extend(src, src_size, r, reps2[ri]);
                if (rlen < LZ_MIN_MATCH) continue;
                if (rlen > max_rep_rlen) max_rep_rlen = rlen;
                int64_t ml_c = (int64_t)prices->ml[lz_opt_ml_sym(rlen - LZ_MIN_MATCH)];
                int64_t off_c = (int64_t)prices->off[ri];
                int64_t cand_cost = best_start2 + prices->ll_avg + ml_c + off_c;
                Lz2OptRepState new_rep = lz_opt_rep_update(cur_rep2, reps2[ri]);
                lz_opt_streams_emit(cand_cost, r + rlen,
                    best_start2_prev_p, r, rlen, reps2[ri], new_rep,
                    post_match, bt, rep_at);
            }
        }

        /* Commit-and-skip on long rep matches — see initial parser above. */
        if (max_rep_rlen >= LZ_OPT_COMMIT_LEN) {
            mf->insert(mf_ctx, r);
            skip_until = r + max_rep_rlen;
            continue;
        }

        /* Hash-chain matches: find up to LZ_OPT_MAX_MATCHES candidates
         * at different offsets, then emit transitions at multiple match
         * lengths along ml-symbol boundaries for each. This lets the DP
         * choose close-offset matches (fewer off extra bits) over long-
         * offset matches, and shorter matches that enable better
         * downstream transitions. */
        LzOptMatch matches[LZ_OPT_MAX_MATCHES];
        uint32_t n_matches = mf->find_multi(mf_ctx, r, LZ_OPT_EXTEND_CAP,
                                             matches, LZ_OPT_MAX_MATCHES);

        mf->insert(mf_ctx, r);

        for (uint32_t mi = 0u; mi < n_matches; mi++) {
            uint32_t m_len = matches[mi].len;
            uint32_t m_off = matches[mi].off;

            int rep_idx = -1;
            if (m_off == cur_rep.r1) rep_idx = 0;
            else if (m_off == cur_rep.r2) rep_idx = 1;
            else if (m_off == cur_rep.r3) rep_idx = 2;

            int64_t off_c = (rep_idx >= 0)
                ? (int64_t)prices->off[rep_idx]
                : (int64_t)prices->off[lz_opt_off_sym_novel(
                      m_off >> prices->offset_shift)];
            Lz2OptRepState new_rep = lz_opt_rep_update(cur_rep, m_off);

            /* Commit-and-skip for long matches at the longest offset. */
            if (mi == n_matches - 1u && m_len >= LZ_OPT_COMMIT_LEN) {
                int64_t ml_c = (int64_t)prices->ml[lz_opt_ml_sym(m_len - LZ_MIN_MATCH)];
                lz_opt_streams_emit(best_start + prices->ll_avg + ml_c + off_c,
                    r + m_len, best_start_prev_p, r, m_len, m_off, new_rep,
                    post_match, bt, rep_at);
                skip_until = r + m_len;
                break;
            }

            /* Emit transitions at ml-symbol boundaries: within each
             * symbol range, the longest length dominates (same cost,
             * more bytes covered). Boundaries are at match_len_m3 =
             * 0, 1, 2, 4, 8, 16, 32, ... (sym = 0, 1, 2, 3, 4, ...).
             * We iterate through boundaries that fit within m_len. */
            /* Always emit at the full match length. */
            {
                int64_t ml_c = (int64_t)prices->ml[lz_opt_ml_sym(m_len - LZ_MIN_MATCH)];
                lz_opt_streams_emit(best_start + prices->ll_avg + ml_c + off_c,
                    r + m_len, best_start_prev_p, r, m_len, m_off, new_rep,
                    post_match, bt, rep_at);
            }
            /* Emit at shorter symbol-boundary lengths (different ml cost). */
            static const uint32_t ml_bounds[] = {
                3, 4, 6, 10, 18, 34, 66, 130, 258
            };
            for (int bi = 0; bi < 9; bi++) {
                uint32_t try_len = ml_bounds[bi];
                if (try_len >= m_len) break;  /* already emitted m_len */
                if (try_len < LZ_MIN_MATCH) continue;
                int64_t ml_c = (int64_t)prices->ml[lz_opt_ml_sym(try_len - LZ_MIN_MATCH)];
                lz_opt_streams_emit(best_start + prices->ll_avg + ml_c + off_c,
                    r + try_len, best_start_prev_p, r, try_len, m_off, new_rep,
                    post_match, bt, rep_at);
            }
        }
    }

    /* Final: trailing literals using prefix sums. */
    uint32_t best_final_p = 0u;
    int64_t  best_final_cost = lit_prefix[N];  /* all-literal path */
    for (uint32_t p = 1u; p <= N; p++) {
        if (post_match[p] >= INF64) continue;
        int64_t c = post_match[p] + (lit_prefix[N] - lit_prefix[p]);
        if (c < best_final_cost) {
            best_final_cost = c;
            best_final_p = p;
        }
    }

    uint32_t seq_count = 0u;
    {
        uint32_t cur = best_final_p;
        while (cur > 0u) { seq_count++; cur = bt[cur].prev_p; }
    }

    LZSeq *seqs = NULL;
    if (seq_count > 0u) {
        seqs = (LZSeq *)lz_opt_alloc(dst, (size_t)seq_count * sizeof(LZSeq));
        if (!seqs) {
            lz_opt_free(dst, lit_prefix);
            lz_opt_free(dst, post_match);
            lz_opt_free(dst, bt);
            lz_opt_free(dst, rep_at);
            mf->destroy(mf_ctx, dst);
            return TDC_E_NOMEM;
        }
        uint32_t cur = best_final_p;
        uint32_t idx = seq_count;
        while (cur > 0u) {
            idx--;
            uint32_t prev_p = bt[cur].prev_p;
            seqs[idx].lit_len   = bt[cur].r_start - prev_p;
            seqs[idx].match_len = bt[cur].L;
            seqs[idx].match_off = bt[cur].off;
            cur = prev_p;
        }
    }

    lz_opt_free(dst, lit_prefix);
    lz_opt_free(dst, post_match);
    lz_opt_free(dst, bt);
    lz_opt_free(dst, rep_at);
    mf->destroy(mf_ctx, dst);

    *out_seqs = seqs;
    *out_seq_count = seq_count;
    return TDC_OK;
}

/* Convenience wrapper for the single-stream TDC_ENTROPY_LZ_OPT path:
 * parse optimally, then serialize through the shared writer in lz.c.
 * TDC_ENTROPY_LZ_STREAMS does not use this — it calls the parser
 * directly so it can apply its own multi-stream pipeline to the
 * resulting LZSeq array. */
static tdc_status lz_opt_parse_and_serialize(
    const uint8_t *src, uint32_t src_size, tdc_buffer *dst)
{
    LZSeq *seqs = NULL;
    uint32_t seq_count = 0u;
    tdc_status st = tdc_lz_parse_optimal(src, src_size, dst,
                                          &seqs, &seq_count);
    if (st != TDC_OK) return st;

    st = tdc_lz_serialize_sequences(src, src_size, seqs, seq_count, dst);
    if (seqs) lz_opt_free(dst, seqs);
    return st;
}

/* ----- vtable wiring ----------------------------------------------------- */

static size_t lz_opt_encode_bound(size_t src_size) {
    /* Worst case is the literal-only fallback stream. */
    return src_size + LZ_HEADER_SIZE;
}

static tdc_status lz_opt_encode(const uint8_t *src, size_t src_size,
                                 const void *params, tdc_buffer *dst) {
    (void)params;
    if (!dst || !dst->realloc_fn) return TDC_E_INVAL;
    if (src_size > UINT32_MAX) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;
    return lz_opt_parse_and_serialize(src, (uint32_t)src_size, dst);
}

/* Decode is shared with TDC_ENTROPY_LZ — lz.c's lz_decode_core handles
 * both greedy- and optimal-parsed streams identically (same on-disk
 * format). We re-use the greedy vtable's decode function pointer via
 * registry.c's dispatch, so we forward to the same symbol here. */
extern const tdc_entropy_vt tdc_entropy_lz_vt;

static tdc_status lz_opt_decode(const uint8_t *src, size_t src_size,
                                 uint8_t *dst, size_t dst_size) {
    return tdc_entropy_lz_vt.decode(src, src_size, dst, dst_size);
}

const tdc_entropy_vt tdc_entropy_lz_opt_vt = {
    .id           = TDC_ENTROPY_LZ_OPT,
    .name         = "lz_opt",
    .encode_bound = lz_opt_encode_bound,
    .encode       = lz_opt_encode,
    .decode       = lz_opt_decode,
};

/* ===================================================================== *
 * TDC_ENTROPY_LZ_SPLIT — optimal parser + split descriptor/literal      *
 *                         Huffman encoding.                              *
 *                                                                        *
 * On-disk format:                                                        *
 *                                                                        *
 *   offset  size  field                                                  *
 *     0      4    n_seqs            (0 => passthrough: lit_enc IS src)   *
 *     4      4    uncompressed_size (== dst_size on decode)              *
 *     8      4    total_lit_size    (uncompressed literal bytes)         *
 *    12      4    trailing_lit_len  (literals after last match)          *
 *    16      4    desc_raw_size     (uncompressed descriptor bytes)      *
 *    20      4    desc_enc_size     (Huffman-encoded descriptor bytes)   *
 *    24      4    lit_enc_size      (Huffman-encoded literal bytes)      *
 *    28      ...  [desc_enc_size bytes] — Huffman-encoded descriptors    *
 *    28+D    ...  [lit_enc_size bytes]  — Huffman-encoded literals       *
 *                                                                        *
 * Descriptors are the same byte format as the single-stream LZ:          *
 *   [LLLLMMMM tag] [lit_ext*] [match_ext*] [offset 2+ bytes]            *
 * but WITHOUT the literal bytes interleaved. The decoder walks           *
 * descriptors exactly like lz_decode_core but pulls literal bytes        *
 * from the separate literal stream.                                      *
 * ===================================================================== */

#define LZ_SPLIT_HEADER_SIZE 28u

/* ----- Encode -------------------------------------------------------- */

static void lz_split_build_streams(
    const uint8_t *src, uint32_t src_size,
    const LZSeq *seqs, uint32_t seq_count,
    uint8_t *lit_buf, uint32_t *lit_pos_out,
    uint8_t *desc_buf, uint32_t *desc_pos_out,
    uint32_t *trailing_out)
{
    uint32_t src_pos = 0, lit_pos = 0, desc_pos = 0;
    uint64_t consumed = 0;

    for (uint32_t i = 0; i < seq_count; i++) {
        uint32_t ll = seqs[i].lit_len;
        uint32_t ml = seqs[i].match_len;
        uint32_t ml_m3 = ml - LZ_MIN_MATCH;
        uint32_t off = seqs[i].match_off;

        /* Literals go to lit_buf. */
        if (ll > 0) {
            memcpy(lit_buf + lit_pos, src + src_pos, ll);
            lit_pos += ll;
        }
        src_pos += ll + ml;
        consumed += (uint64_t)ll + ml;

        /* Descriptor: tag + lit_ext + match_ext + offset. */
        uint8_t ll_nib = (ll >= 15u) ? 15u : (uint8_t)ll;
        uint8_t ml_nib = (ml_m3 >= 15u) ? 15u : (uint8_t)ml_m3;
        desc_buf[desc_pos++] = (uint8_t)((ll_nib << 4) | ml_nib);

        if (ll >= 15u) {
            uint32_t extra = ll - 15u;
            while (extra >= 255u) {
                desc_buf[desc_pos++] = 255u;
                extra -= 255u;
            }
            desc_buf[desc_pos++] = (uint8_t)extra;
        }
        if (ml_m3 >= 15u) {
            uint8_t *dp = lz_leb128_write(desc_buf + desc_pos, ml_m3 - 15u);
            desc_pos = (uint32_t)(dp - desc_buf);
        }
        {
            uint8_t *dp = lz_offset_write(desc_buf + desc_pos, off);
            desc_pos = (uint32_t)(dp - desc_buf);
        }
    }

    /* Trailing literals. */
    uint32_t trailing = src_size - (uint32_t)consumed;
    if (trailing > 0) {
        memcpy(lit_buf + lit_pos, src + src_pos, trailing);
        lit_pos += trailing;
    }

    *lit_pos_out = lit_pos;
    *desc_pos_out = desc_pos;
    *trailing_out = trailing;
}

/* Encode a sub-stream with Huffman, falling back to NONE passthrough
 * if Huffman expands. Returns an allocated buffer the caller frees. */
static tdc_status lz_split_encode_sub(const uint8_t *raw, uint32_t raw_size,
                                       tdc_buffer *owner,
                                       uint8_t **out, uint32_t *out_size)
{
    if (raw_size == 0) {
        *out = NULL;
        *out_size = 0;
        return TDC_OK;
    }

    tdc_buffer scratch = {0};
    scratch.realloc_fn = owner->realloc_fn;
    scratch.user = owner->user;

    tdc_status st = tdc_entropy_huffman_vt.encode(raw, raw_size, NULL, &scratch);
    if (st == TDC_OK && scratch.size < raw_size) {
        /* Huffman won — transfer ownership. */
        *out = scratch.data;
        *out_size = (uint32_t)scratch.size;
        return TDC_OK;
    }

    /* Huffman didn't help — passthrough (copy). */
    if (scratch.data)
        (void)owner->realloc_fn(owner->user, scratch.data, 0);

    uint8_t *buf = (uint8_t *)owner->realloc_fn(owner->user, NULL, raw_size);
    if (!buf) return TDC_E_NOMEM;
    memcpy(buf, raw, raw_size);
    *out = buf;
    *out_size = raw_size;
    return TDC_OK;
}

static size_t lz_split_encode_bound(size_t src_size) {
    return LZ_SPLIT_HEADER_SIZE + src_size + src_size;  /* generous */
}

static tdc_status lz_split_encode(const uint8_t *src, size_t src_size,
                                   const void *params, tdc_buffer *dst)
{
    (void)params;
    if (!dst || !dst->realloc_fn) return TDC_E_INVAL;
    if (src_size > UINT32_MAX) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;

    uint32_t N = (uint32_t)src_size;

    /* Passthrough for tiny inputs. */
    if (N < LZ_MIN_MATCH + 1u) {
        tdc_status st = tdc_buf_reserve(dst, LZ_SPLIT_HEADER_SIZE + N);
        if (st != TDC_OK) return st;
        uint32_t zero = 0;
        memcpy(dst->data + 0,  &zero, 4);  /* n_seqs */
        memcpy(dst->data + 4,  &N,    4);  /* uncompressed_size */
        memcpy(dst->data + 8,  &N,    4);  /* total_lit_size */
        memcpy(dst->data + 12, &N,    4);  /* trailing_lit_len */
        memcpy(dst->data + 16, &zero, 4);  /* desc_raw_size */
        memcpy(dst->data + 20, &zero, 4);  /* desc_enc_size */
        memcpy(dst->data + 24, &N,    4);  /* lit_enc_size */
        if (N > 0) memcpy(dst->data + LZ_SPLIT_HEADER_SIZE, src, N);
        dst->size = LZ_SPLIT_HEADER_SIZE + N;
        return TDC_OK;
    }

    /* Parse. */
    LZSeq *seqs = NULL;
    uint32_t seq_count = 0;
    tdc_status st = tdc_lz_parse_optimal(src, N, dst, &seqs, &seq_count);
    if (st != TDC_OK) return st;

    if (seq_count == 0) {
        /* No matches — passthrough. */
        if (seqs) lz_opt_free(dst, seqs);
        tdc_status st2 = tdc_buf_reserve(dst, LZ_SPLIT_HEADER_SIZE + N);
        if (st2 != TDC_OK) return st2;
        uint32_t zero = 0;
        memcpy(dst->data + 0,  &zero, 4);
        memcpy(dst->data + 4,  &N,    4);
        memcpy(dst->data + 8,  &N,    4);
        memcpy(dst->data + 12, &N,    4);
        memcpy(dst->data + 16, &zero, 4);  /* desc_raw_size */
        memcpy(dst->data + 20, &zero, 4);  /* desc_enc_size */
        memcpy(dst->data + 24, &N,    4);  /* lit_enc_size */
        if (N > 0) memcpy(dst->data + LZ_SPLIT_HEADER_SIZE, src, N);
        dst->size = LZ_SPLIT_HEADER_SIZE + N;
        return TDC_OK;
    }

    /* Compute descriptor size upper bound. Repcode sentinels use 2 bytes
     * (same as regular offsets), so the old lz_seq_encoded_size bound is
     * still valid (it assumes 2-byte minimum offset). */
    uint64_t desc_bound = 0;
    for (uint32_t i = 0; i < seq_count; i++) {
        uint32_t ml_m3 = seqs[i].match_len - LZ_MIN_MATCH;
        desc_bound += lz_seq_encoded_size(seqs[i].lit_len, ml_m3,
                                          seqs[i].match_off);
    }

    uint8_t *lit_buf = (uint8_t *)lz_opt_alloc(dst, (size_t)N + 1);
    uint8_t *desc_buf = (uint8_t *)lz_opt_alloc(dst, (size_t)desc_bound + 1);
    if (!lit_buf || !desc_buf) {
        lz_opt_free(dst, lit_buf);
        lz_opt_free(dst, desc_buf);
        lz_opt_free(dst, seqs);
        return TDC_E_NOMEM;
    }

    uint32_t lit_pos = 0, desc_pos = 0, trailing = 0;
    lz_split_build_streams(src, N, seqs, seq_count,
                           lit_buf, &lit_pos, desc_buf, &desc_pos, &trailing);
    lz_opt_free(dst, seqs);
    seqs = NULL;

    /* Huffman each sub-stream. */
    uint8_t *desc_enc = NULL, *lit_enc = NULL;
    uint32_t desc_enc_size = 0, lit_enc_size = 0;

    st = lz_split_encode_sub(desc_buf, desc_pos, dst, &desc_enc, &desc_enc_size);
    lz_opt_free(dst, desc_buf);
    if (st != TDC_OK) { lz_opt_free(dst, lit_buf); return st; }

    st = lz_split_encode_sub(lit_buf, lit_pos, dst, &lit_enc, &lit_enc_size);
    lz_opt_free(dst, lit_buf);
    if (st != TDC_OK) { lz_opt_free(dst, desc_enc); return st; }

    /* Check if the split format actually helps vs passthrough. */
    uint32_t total = LZ_SPLIT_HEADER_SIZE + desc_enc_size + lit_enc_size;
    if (total >= N) {
        /* Passthrough is smaller. */
        lz_opt_free(dst, desc_enc);
        lz_opt_free(dst, lit_enc);
        st = tdc_buf_reserve(dst, LZ_SPLIT_HEADER_SIZE + N);
        if (st != TDC_OK) return st;
        uint32_t zero = 0;
        memcpy(dst->data + 0,  &zero, 4);
        memcpy(dst->data + 4,  &N,    4);
        memcpy(dst->data + 8,  &N,    4);
        memcpy(dst->data + 12, &N,    4);
        memcpy(dst->data + 16, &zero, 4);  /* desc_raw_size */
        memcpy(dst->data + 20, &zero, 4);  /* desc_enc_size */
        memcpy(dst->data + 24, &N,    4);  /* lit_enc_size */
        memcpy(dst->data + LZ_SPLIT_HEADER_SIZE, src, N);
        dst->size = LZ_SPLIT_HEADER_SIZE + N;
        return TDC_OK;
    }

    /* Assemble final output. */
    st = tdc_buf_reserve(dst, total);
    if (st != TDC_OK) {
        lz_opt_free(dst, desc_enc);
        lz_opt_free(dst, lit_enc);
        return st;
    }

    memcpy(dst->data + 0,  &seq_count,     4);
    memcpy(dst->data + 4,  &N,             4);
    memcpy(dst->data + 8,  &lit_pos,       4);  /* total_lit_size */
    memcpy(dst->data + 12, &trailing,      4);
    memcpy(dst->data + 16, &desc_pos,      4);  /* desc_raw_size */
    memcpy(dst->data + 20, &desc_enc_size, 4);
    memcpy(dst->data + 24, &lit_enc_size,  4);

    if (desc_enc_size > 0)
        memcpy(dst->data + LZ_SPLIT_HEADER_SIZE, desc_enc, desc_enc_size);
    if (lit_enc_size > 0)
        memcpy(dst->data + LZ_SPLIT_HEADER_SIZE + desc_enc_size,
               lit_enc, lit_enc_size);

    dst->size = total;

    lz_opt_free(dst, desc_enc);
    lz_opt_free(dst, lit_enc);
    return TDC_OK;
}

/* ----- Decode -------------------------------------------------------- */

static tdc_status lz_split_decode(const uint8_t *src, size_t src_size,
                                   uint8_t *dst, size_t dst_size)
{
    if (src_size < LZ_SPLIT_HEADER_SIZE) return TDC_E_CORRUPT;

    uint32_t n_seqs, uncompressed_size, total_lit_size, trailing_lit_len;
    uint32_t desc_raw_size, desc_enc_size, lit_enc_size;
    memcpy(&n_seqs,             src + 0,  4);
    memcpy(&uncompressed_size,  src + 4,  4);
    memcpy(&total_lit_size,     src + 8,  4);
    memcpy(&trailing_lit_len,   src + 12, 4);
    memcpy(&desc_raw_size,      src + 16, 4);
    memcpy(&desc_enc_size,      src + 20, 4);
    memcpy(&lit_enc_size,       src + 24, 4);

    if ((size_t)uncompressed_size != dst_size) return TDC_E_CORRUPT;

    /* Passthrough case. */
    if (n_seqs == 0) {
        if (lit_enc_size != uncompressed_size) return TDC_E_CORRUPT;
        if (src_size < LZ_SPLIT_HEADER_SIZE + lit_enc_size) return TDC_E_CORRUPT;
        memcpy(dst, src + LZ_SPLIT_HEADER_SIZE, uncompressed_size);
        return TDC_OK;
    }

    if (src_size < LZ_SPLIT_HEADER_SIZE + (size_t)desc_enc_size + lit_enc_size)
        return TDC_E_CORRUPT;

    const uint8_t *desc_enc = src + LZ_SPLIT_HEADER_SIZE;
    const uint8_t *lit_enc  = desc_enc + desc_enc_size;

    /* Decode descriptor sub-stream. Huffman if enc_size < raw_size,
     * passthrough if enc_size == raw_size. */
    uint8_t *desc_raw = (uint8_t *)malloc(desc_raw_size);
    if (!desc_raw) return TDC_E_NOMEM;

    if (desc_enc_size < desc_raw_size) {
        tdc_status st = tdc_entropy_huffman_vt.decode(
            desc_enc, desc_enc_size, desc_raw, desc_raw_size);
        if (st != TDC_OK) { free(desc_raw); return st; }
    } else {
        if (desc_enc_size != desc_raw_size) { free(desc_raw); return TDC_E_CORRUPT; }
        memcpy(desc_raw, desc_enc, desc_raw_size);
    }

    /* Decode literal sub-stream. */
    uint8_t *lit_raw = (uint8_t *)malloc(total_lit_size + 1);
    if (!lit_raw) { free(desc_raw); return TDC_E_NOMEM; }

    if (lit_enc_size < total_lit_size) {
        tdc_status st = tdc_entropy_huffman_vt.decode(
            lit_enc, lit_enc_size, lit_raw, total_lit_size);
        if (st != TDC_OK) { free(desc_raw); free(lit_raw); return st; }
    } else {
        if (lit_enc_size != total_lit_size) {
            free(desc_raw); free(lit_raw); return TDC_E_CORRUPT;
        }
        memcpy(lit_raw, lit_enc, total_lit_size);
    }
    uint32_t lit_raw_size = total_lit_size;

    /* Walk descriptors + literals to reconstruct output, same as
     * lz_decode_core but pulling from separate streams and decoding
     * repcode offsets. */
    uint32_t dst_pos = 0;
    uint32_t lit_pos = 0;
    const uint8_t *dp = desc_raw;

    for (uint32_t s = 0; s < n_seqs; s++) {
        uint8_t tag = *dp++;
        uint32_t ll = (uint32_t)(tag >> 4);
        uint32_t ml_m3 = (uint32_t)(tag & 0x0F);

        /* Literal-length extension (chained-255 varint). */
        if (ll == 15u) {
            uint8_t b;
            do {
                b = *dp++;
                ll += b;
            } while (b == 255u);
        }

        /* Match-length extension (LEB128). */
        if (ml_m3 == 15u) {
            uint32_t ext;
            dp = lz_leb128_read(dp, &ext);
            ml_m3 += ext;
        }
        uint32_t ml = ml_m3 + LZ_MIN_MATCH;

        /* Offset. */
        uint32_t off;
        dp = lz_offset_read(dp, &off);

        /* Copy literals from literal stream. */
        if (ll > 0) {
            if (lit_pos + ll > lit_raw_size || dst_pos + ll > dst_size) {
                free(desc_raw); free(lit_raw); return TDC_E_CORRUPT;
            }
            memcpy(dst + dst_pos, lit_raw + lit_pos, ll);
            dst_pos += ll;
            lit_pos += ll;
        }

        /* Copy match from output history. */
        if (off == 0 || off > dst_pos || dst_pos + ml > dst_size) {
            free(desc_raw); free(lit_raw); return TDC_E_CORRUPT;
        }
        {
            uint8_t *match_dst = dst + dst_pos;
            const uint8_t *match_src = dst + dst_pos - off;
            for (uint32_t j = 0; j < ml; j++)
                match_dst[j] = match_src[j];
            dst_pos += ml;
        }
    }

    /* Trailing literals. */
    if (trailing_lit_len > 0) {
        if (lit_pos + trailing_lit_len > lit_raw_size ||
            dst_pos + trailing_lit_len > dst_size) {
            free(desc_raw); free(lit_raw); return TDC_E_CORRUPT;
        }
        memcpy(dst + dst_pos, lit_raw + lit_pos, trailing_lit_len);
        dst_pos += trailing_lit_len;
    }

    free(desc_raw);
    free(lit_raw);

    if (dst_pos != dst_size) return TDC_E_CORRUPT;
    return TDC_OK;
}

const tdc_entropy_vt tdc_entropy_lz_split_vt = {
    .id           = TDC_ENTROPY_LZ_SPLIT,
    .name         = "lz_split",
    .encode_bound = lz_split_encode_bound,
    .encode       = lz_split_encode,
    .decode       = lz_split_decode,
};
