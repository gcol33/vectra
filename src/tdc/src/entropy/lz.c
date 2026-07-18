/*
 * src/entropy/lz.c
 *
 * TDC_ENTROPY_LZ — native LZ77 with separated streams.
 *
 * Architecture (matches the design notes in vectra/CLAUDE.md):
 *   - separated sequence-descriptor and literal streams (zstd-style)
 *   - 4 MiB window, greedy hash matcher
 *   - packed variable-length [LLLLMMMM] tag byte; 3 bytes/sequence
 *     overhead in the common case
 *   - decode hot path: fast-path / safe-path split with 16-byte
 *     unconditional wildcopy for matches
 *
 * IMPORTANT: this file owns the entropy stage only. Byte shuffle is a
 * separate concern in transform/shuffle.c. The vectra naming
 * "SHUFFLE_LZ" combined the two; tdc keeps them orthogonal.
 *
 * Source: extracted verbatim from vectra/src/vtr_codec.c
 *         (vtr_lz_compress / vtr_lz_decompress_into, lines 411-916).
 *         The inner encode/decode loops MUST stay byte-identical to
 *         vectra so existing .vtr files round-trip without churn. Do not
 *         "improve" the loops without benchmarks: see the design note in
 *         vectra/CLAUDE.md ("No batched parse-then-copy in LZ fast
 *         path") and the comment above lz_decode_fast() below.
 *
 * Differences vs vectra:
 *   - All allocation goes through tdc_buffer::realloc_fn (POSIX-style:
 *     (NULL, n) allocates, (p, 0) frees). No bare malloc/free.
 *   - vectra returns NULL when input is incompressible or shorter than
 *     LZ_MIN_MATCH+1 bytes; tdc instead emits a literal-only stream
 *     (n_seq=0, all bytes as trailing literals). The on-disk format is
 *     unchanged — vectra's decoder happens to handle that shape too via
 *     the safe-path trailing-literal block.
 *   - lz_decode_safe returns TDC_E_CORRUPT on invalid back-references
 *     instead of calling vectra_error (which longjmps to R).
 */

#include "tdc/entropy.h"
#include "entropy_internal.h"
#include "lz_internal.h"
#include "match_finder.h"
#include "../core/buffer.h"
#include "../core/simd.h"

#include <string.h>
#include <stdint.h>
#include <stddef.h>

#if defined(_MSC_VER)
#  include <intrin.h>
#endif

/* ----- Greedy-matcher constants ------------------------------------------ */
/* LZ_MIN_MATCH, LZ_MAX_OFFSET, LZ_HEADER_SIZE, and the LZSeq type live
 * in lz_internal.h — they're shared with the optimal parser in lz_opt.c.
 * The constants below are specific to this file's greedy matcher. */

#define LZ_HASH_BITS  18
#define LZ_HASH_SIZE  (1 << LZ_HASH_BITS)
/* Match length is encoded as 4 bits in the tag + a varint extension
 * (chained 255-byte chunks, same shape as the literal-length encoding).
 * No hard ceiling — long zero-run residuals (PLANE2D, RAW on flat data)
 * compress to a handful of bytes when one big match can span millions of
 * positions. The previous 130-byte cap (vectra inheritance) chopped a
 * 4 MiB zero stream into ~32k three-byte sequences = ~100 KiB payload,
 * which made the model stages look bad on synthetic-flat inputs and was
 * the root cause documented in SPEEDUP-TODO P0.1. */
#define LZ_MAX_MATCH  UINT32_MAX

/* ----- Branch hints (zstd-style) ----------------------------------------- */

#if defined(__GNUC__) || defined(__clang__)
#  define LZ_LIKELY(x)   __builtin_expect(!!(x), 1)
#  define LZ_UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#  define LZ_LIKELY(x)   (x)
#  define LZ_UNLIKELY(x) (x)
#endif

/* ----- Allocation helpers ------------------------------------------------- */
/*
 * tdc_buffer::realloc_fn is the only allocation path inside tdc. We use it
 * with POSIX realloc semantics:
 *   realloc_fn(user, NULL, n)  -> allocate n bytes
 *   realloc_fn(user, p,    0)  -> free p
 *   realloc_fn(user, p,    n)  -> grow p to n bytes (may move)
 *
 * Callers (vectra, the test harness) must supply a realloc_fn that honors
 * these conventions. The standard C realloc() does, modulo the C11 quirk
 * that realloc(p, 0) is implementation-defined; tdc requires the
 * "free and return NULL" interpretation, which is what every mainstream
 * libc actually ships.
 */

static void *lz_alloc(tdc_buffer *buf, size_t n) {
    return buf->realloc_fn(buf->user, NULL, n);
}

static void lz_free(tdc_buffer *buf, void *p) {
    if (p) (void)buf->realloc_fn(buf->user, p, 0);
}

/* Output-buffer growth uses the shared tdc_buf_reserve helper from
 * src/core/buffer.h. The previous local copy lz_buf_reserve was lifted
 * along with shuffle_buf_reserve and quantize_buf_reserve into a single
 * source of truth — see src/core/buffer.h. */

/* ----- Match finder primitives ------------------------------------------- */
/*
 * lz_hash4 (the 4-byte Fibonacci hash) lives in mf_hashchain.c now, along
 * with the chain walk, prefetch, and per-position chain insert. The
 * fast-path below keeps a local copy because it bypasses the vtable for
 * speed (single-probe, accelerating step, no chain_prev allocation).
 * Both copies use the same constant (2654435761u) and the same number of
 * hash bits (LZ_HASH_BITS) so bucket distribution is identical.
 */

static inline uint32_t lz_hash4(const uint8_t *p) {
    uint32_t h = ((uint32_t)p[0]) |
                 ((uint32_t)p[1] << 8) |
                 ((uint32_t)p[2] << 16) |
                 ((uint32_t)p[3] << 24);
    /* Fibonacci hash for good distribution */
    return (h * 2654435761u) >> (32 - LZ_HASH_BITS);
}

/* Wildcopy and match copy now live in src/core/simd.h as tdc_wildcopy16
 * and tdc_match_copy.  SSE2/NEON where available, scalar fallback. */

/* ----- Sequence layout note ---------------------------------------------- *
 *
 * The packed sequence header format is:
 *
 *   Byte 0: [LLLLMMMM] tag byte
 *     L (0-14):  literal byte count before this match
 *     L (15):    extended — chained 255-byte varint (add 255 per byte until
 *                a byte < 255 terminates; total = 15 + sum)
 *     M (0-14):  match_length - 3
 *     M (15):    extended — LEB128 unsigned varint (7 bits of payload + 1
 *                continuation bit per byte)
 *   uint16_le offset (2 bytes base, with 0xFFFF overflow sentinel)
 *     offset 1-65535:   2 bytes (uint16 of offset-1)
 *     offset 65536+:    2 + LEB128 bytes (sentinel + LEB128 of offset-65536)
 *
 * Common case: 3 bytes (tag + 2-byte offset), same as the original format.
 *
 * Why LEB128 on match length but chained-255 on literal length:
 * match length can legitimately hit millions of bytes (a 4 MiB run of
 * identical bytes produces one match spanning the whole run). The old
 * chained-255 encoding cost `extra / 255 + 1` bytes, i.e. ~16 KiB for a
 * 4 MiB match — the payload term alone dominated every other cost in the
 * block and capped PLANE2D at ~213x on inputs whose information content
 * was closer to ~700x. LEB128 encodes the same 4 MiB match in 4 bytes
 * (`ceil(log2(extra) / 7)`). Literal runs, by contrast, never reach that
 * regime: trailing literals after the last match are written to the
 * literal buffer directly (no sequence header), so per-sequence lit_len
 * is bounded by real literal density between matches and never blows up
 * the same way. See SPEEDUP-TODO P0.3 for the full diagnosis.
 *
 * lz_seq_encoded_size() and the LZSeq type are defined in lz_internal.h
 * so the optimal parser in lz_opt.c can use them without duplicating.
 */

/* ----- Literal-only fallback --------------------------------------------- */
/*
 * Used when input is shorter than LZ_MIN_MATCH+1 OR when a parse (greedy or
 * optimal) produced no compression gain. Output is the same on-disk format:
 * 8-byte header with n_seq=0, literals_size=src_size, followed by raw bytes.
 * The decoder picks them up in the safe path's trailing-literal block.
 */
static tdc_status lz_encode_literal_only(const uint8_t *src, uint32_t src_size,
                                          tdc_buffer *dst) {
    size_t total = LZ_HEADER_SIZE + (size_t)src_size;
    tdc_status st = tdc_buf_reserve(dst, total);
    if (st != TDC_OK) return st;
    uint8_t *p = dst->data;
    uint32_t n_seq = 0;
    uint32_t literals_size = src_size;
    memcpy(p,     &n_seq,         4);
    memcpy(p + 4, &literals_size, 4);
    if (src_size > 0) memcpy(p + LZ_HEADER_SIZE, src, src_size);
    dst->size = total;
    return TDC_OK;
}

/* ----- Sequence serializer (shared with lz_opt) ------------------------- */
/*
 * Takes a parsed LZSeq array and writes the on-disk LZ stream. The greedy
 * encoder (lz_encode_core) and the optimal parser (lz_opt.c) both produce
 * LZSeq arrays and call this function to serialize them. Both emit the
 * exact same on-disk format, decoded by lz_decode_core — optimal parsing
 * is a pure encode-side optimization.
 *
 * Literals for sequence i live at src[src_pos .. src_pos + seqs[i].lit_len),
 * where src_pos is the running sum of (lit_len + match_len) over sequences
 * 0..i-1. Trailing literals (after the last match) live at
 * src[consumed .. src_size). No separate literal buffer is needed.
 *
 * If the encoded size would meet or exceed src_size, falls back to the
 * literal-only stream so the entropy stage always produces a valid LZ
 * record regardless of parse quality.
 */
tdc_status tdc_lz_serialize_sequences(const uint8_t *src, uint32_t src_size,
                                       const LZSeq *seqs, uint32_t seq_count,
                                       tdc_buffer *dst) {
    /* Walk seqs once to compute total literal length and header size. */
    uint32_t total_lit = 0;
    uint32_t seq_hdr_size = 0;
    uint32_t consumed = 0;
    for (uint32_t i = 0; i < seq_count; i++) {
        if (seqs[i].match_len < LZ_MIN_MATCH) return TDC_E_CORRUPT;
        if (seqs[i].match_off == 0 || seqs[i].match_off > LZ_MAX_OFFSET) return TDC_E_CORRUPT;
        uint32_t ll = seqs[i].lit_len;
        uint32_t ml_m3 = seqs[i].match_len - LZ_MIN_MATCH;
        seq_hdr_size += lz_seq_encoded_size(ll, ml_m3, seqs[i].match_off);
        total_lit += ll;
        consumed += ll + seqs[i].match_len;
    }
    if (consumed > src_size) return TDC_E_CORRUPT;
    uint32_t trailing = src_size - consumed;
    total_lit += trailing;

    uint32_t total = LZ_HEADER_SIZE + seq_hdr_size + total_lit;
    if (total >= src_size) {
        return lz_encode_literal_only(src, src_size, dst);
    }

    tdc_status st = tdc_buf_reserve(dst, total);
    if (st != TDC_OK) return st;

    uint8_t *p = dst->data;
    memcpy(p, &seq_count, 4); p += 4;
    memcpy(p, &total_lit, 4); p += 4;

    /* Write packed sequence headers. */
    for (uint32_t i = 0; i < seq_count; i++) {
        uint32_t ll = seqs[i].lit_len;
        uint32_t ml_m3 = seqs[i].match_len - LZ_MIN_MATCH;
        uint32_t L = ll < 15 ? ll : 15;
        uint32_t M = ml_m3 < 15 ? ml_m3 : 15;

        *p++ = (uint8_t)((L << 4) | M);

        /* Extended literal length — chained 255-byte varint. */
        if (ll >= 15) {
            uint32_t extra = ll - 15;
            while (extra >= 255) {
                *p++ = 255;
                extra -= 255;
            }
            *p++ = (uint8_t)extra;
        }

        /* Extended match length — LEB128 unsigned varint. See the note
         * above lz_seq_encoded_size for why match length uses LEB128 while
         * literal length uses chained-255. */
        if (ml_m3 >= 15) {
            uint32_t extra = ml_m3 - 15;
            while (extra >= 128) {
                *p++ = (uint8_t)((extra & 0x7Fu) | 0x80u);
                extra >>= 7;
            }
            *p++ = (uint8_t)extra;
        }

        /* Offset — LEB128(offset - 1) */
        p = lz_offset_write(p, seqs[i].match_off);
    }

    /* Write literals — contiguous regions in src between match ends. */
    uint32_t src_pos = 0;
    for (uint32_t i = 0; i < seq_count; i++) {
        uint32_t ll = seqs[i].lit_len;
        if (ll > 0) {
            memcpy(p, src + src_pos, ll);
            p += ll;
        }
        src_pos += ll + seqs[i].match_len;
    }
    if (trailing > 0) {
        memcpy(p, src + src_pos, trailing);
    }

    dst->size = total;
    return TDC_OK;
}

/* ----- Greedy match-finder (shared) -------------------------------------- */
/*
 * Hash-chain greedy match finder with optional lazy matching. Produces an
 * LZSeq array via dst->realloc_fn.
 *
 * chain_depth controls how many hash-chain candidates are probed per
 * position. 0 = flat hash (one candidate, fastest), 4-8 = good balance,
 * 32+ = deep search. The chain is maintained via a chain_prev[] array
 * that links positions sharing the same hash bucket.
 *
 * lazy_depth controls lazy matching. 0 = greedy (take first match),
 * 1 = single lazy (if position+1 has a strictly longer match, emit
 * current position as a literal and take the better match instead).
 */

/* lz_find_best_match and lz_chain_insert previously lived here. Both have
 * been replaced by the match-finder vtable in match_finder.h — the
 * default backend (mf_hashchain.c) carries the equivalent chain walk and
 * insert, with extras (per-candidate prefetch, optional extension cap)
 * that subsume the old optimal-parser variants in one implementation. */

/* Rep-at-literal probe. Tries each of the three remembered offsets
 * (rep[0..2]) and returns the longest rep-hit at `pos` along with its
 * offset. 0 means no hit >= LZ_MIN_MATCH. Much cheaper than walking the
 * hash chain: 3 short prefix memcmps + a single extend on hit.
 *
 * Periodic data (e.g. daily/seasonal time series) hammers a few dominant
 * strides. Once rep[0] locks onto the true period, repeated matches at
 * that stride bypass the hash search entirely and encode in 1 offset
 * symbol with 0 extra bits — vs ~20 bits for a novel offset in the same
 * log2 bucket. Preferred over hash on hit because a rep match of length N
 * beats a hash match of length N (same length, strictly cheaper offset).
 * Only falls back to hash when no rep slot matches the first 3 bytes. */
static inline uint32_t lz_rep_probe(const uint8_t *src, uint32_t src_size,
                                     uint32_t pos, const uint32_t rep[3],
                                     uint32_t *out_off) {
    if (pos + LZ_MIN_MATCH > src_size) return 0;
    uint32_t max_len = src_size - pos;
    uint32_t best_len = 0;
    uint32_t best_off = 0;
    for (int i = 0; i < 3; i++) {
        uint32_t ro = rep[i];
        /* Skip unfilled slots. Rep state starts at zero; each slot becomes
         * live once lz_rep_update has promoted a real match offset into it.
         *
         * Why not init with (1,4,8) like zstd's rep defaults: the
         * LZ_STREAMS serializer runs lzs_detect_offset_shift (bitwise OR
         * across all offsets) to detect the data's natural stride — a
         * multiple of 8 for aligned f64. A single rep-hit at a synthetic
         * init (1 or 4) contaminates the OR and drops the shift to 0,
         * costing ~3 bits per offset on every sequence in the block
         * (~13% ratio loss on USGS). Starting from zero means rep only
         * fires at offsets that already appeared as real matches, so the
         * stride alignment is inherited from the data. */
        if (ro == 0u) continue;
        if (pos < ro) continue;
        if (memcmp(src + pos, src + pos - ro, LZ_MIN_MATCH) != 0) continue;
        uint32_t len = LZ_MIN_MATCH;
        while (len + 8 <= max_len) {
            uint64_t a, b;
            memcpy(&a, src + pos + len, 8);
            memcpy(&b, src + pos - ro + len, 8);
            if (a != b) {
#if defined(__GNUC__) || defined(__clang__)
                len += (uint32_t)(__builtin_ctzll(a ^ b) >> 3);
#elif defined(_MSC_VER)
                unsigned long idx;
                _BitScanForward64(&idx, a ^ b);
                len += (uint32_t)(idx >> 3);
#else
                uint64_t diff = a ^ b;
                while (!(diff & 0xFFu)) { diff >>= 8; len++; }
#endif
                goto rep_cand_done;
            }
            len += 8;
        }
        while (len < max_len && src[pos + len] == src[pos - ro + len]) len++;
    rep_cand_done:
        if (len > best_len) {
            best_len = len;
            best_off = ro;
        }
    }
    if (best_len >= LZ_MIN_MATCH) {
        *out_off = best_off;
        return best_len;
    }
    return 0;
}

/* Update the MRU rep state after emitting a match with offset `off`.
 * Must match lz_streams.c's lzs_repcode_encode transitions exactly so the
 * serializer's re-computation of the rep state lands on the same slot
 * assignments the parser saw. */
static inline void lz_rep_update(uint32_t rep[3], uint32_t off) {
    if (off == rep[0]) return;
    if (off == rep[1]) {
        uint32_t t = rep[0]; rep[0] = rep[1]; rep[1] = t;
        return;
    }
    if (off == rep[2]) {
        uint32_t t = rep[2]; rep[2] = rep[1]; rep[1] = rep[0]; rep[0] = t;
        return;
    }
    rep[2] = rep[1]; rep[1] = rep[0]; rep[0] = off;
}

/* Specialized fast path: flat hash, no lazy, accelerating step on misses.
 * zstd-style: after consecutive misses, step grows so we skip positions.
 * On low-compressibility data this is 3-5x faster than hashing every byte.
 * On high-compressibility data, step stays ~1 because matches reset it. */
static tdc_status lz_parse_fast(const uint8_t *src, uint32_t src_size,
                                 tdc_buffer *dst,
                                 LZSeq **out_seqs, uint32_t *out_seq_count) {
    *out_seqs = NULL;
    *out_seq_count = 0;

    if (src_size < LZ_MIN_MATCH + 1) return TDC_OK;

    uint32_t *htab = (uint32_t *)lz_alloc(dst, LZ_HASH_SIZE * sizeof(uint32_t));
    if (!htab) return TDC_E_NOMEM;
    memset(htab, 0xFF, LZ_HASH_SIZE * sizeof(uint32_t));

    uint32_t seq_cap = 4096;
    uint32_t seq_count = 0;
    LZSeq *seqs = (LZSeq *)lz_alloc(dst, seq_cap * sizeof(LZSeq));
    if (!seqs) { lz_free(dst, htab); return TDC_E_NOMEM; }

    uint32_t sp = 0;
    uint32_t lit_start = 0;
    uint32_t step = 1;

    while (sp + LZ_MIN_MATCH + 1 <= src_size) {
        uint32_t h = lz_hash4(src + sp);
        uint32_t cand = htab[h];
        htab[h] = sp;

        /* Quick single-probe match attempt (no chain). */
        uint32_t match_len = 0;
        uint32_t match_off = 0;
        if (cand != 0xFFFFFFFF && cand < sp) {
            uint32_t off = sp - cand;
            if (off <= LZ_MAX_OFFSET && src[cand] == src[sp]) {
                uint32_t max_len = src_size - sp;
                uint32_t len = 0;
                while (len + 8 <= max_len) {
                    uint64_t a, b;
                    memcpy(&a, src + sp + len, 8);
                    memcpy(&b, src + cand + len, 8);
                    if (a != b) {
#if defined(__GNUC__) || defined(__clang__)
                        len += (uint32_t)(__builtin_ctzll(a ^ b) >> 3);
#elif defined(_MSC_VER)
                        unsigned long idx;
                        _BitScanForward64(&idx, a ^ b);
                        len += (uint32_t)(idx >> 3);
#else
                        uint64_t diff = a ^ b;
                        while (!(diff & 0xFF)) { diff >>= 8; len++; }
#endif
                        goto fast_cand_done;
                    }
                    len += 8;
                }
                while (len < max_len && src[sp + len] == src[cand + len])
                    len++;
            fast_cand_done:
                if (len >= LZ_MIN_MATCH) {
                    match_len = len;
                    match_off = off;
                }
            }
        }

        if (match_len >= LZ_MIN_MATCH) {
            uint32_t pending_lit = sp - lit_start;

            if (seq_count >= seq_cap) {
                uint32_t new_cap = seq_cap * 2;
                LZSeq *new_seqs = (LZSeq *)dst->realloc_fn(
                    dst->user, seqs, new_cap * sizeof(LZSeq));
                if (!new_seqs) {
                    lz_free(dst, htab); lz_free(dst, seqs);
                    return TDC_E_NOMEM;
                }
                seqs = new_seqs;
                seq_cap = new_cap;
            }
            seqs[seq_count].lit_len = pending_lit;
            seqs[seq_count].match_len = match_len;
            seqs[seq_count].match_off = match_off;
            seq_count++;

            /* Insert every 4th position within match for future references. */
            for (uint32_t i = 1; i < match_len && sp + i + LZ_MIN_MATCH + 1 <= src_size; i += 4) {
                uint32_t mh = lz_hash4(src + sp + i);
                htab[mh] = sp + i;
            }
            sp += match_len;
            lit_start = sp;
            step = 1; /* reset acceleration */
        } else {
            sp += step;
            /* Accelerate: after consecutive misses, grow step linearly.
             * Capped at 8 to limit ratio loss on semi-compressible data.
             * Reset to 1 on each match hit. On data with abundant matches
             * (real f64 time series, 4x+ ratio) the step rarely grows past
             * 2-3 because matches keep resetting it. The cap only matters
             * for low-compressibility data where speed > ratio. */
            if (step < 8) step++;
        }
    }

    lz_free(dst, htab);
    *out_seqs = seqs;
    *out_seq_count = seq_count;
    return TDC_OK;
}

tdc_status tdc_lz_parse_greedy(const uint8_t *src, uint32_t src_size,
                                tdc_buffer *dst,
                                uint32_t chain_depth, uint32_t lazy_depth,
                                LZSeq **out_seqs, uint32_t *out_seq_count) {
    /* Fast path: flat hash + no lazy + accelerating step. */
    if (chain_depth == 0 && lazy_depth == 0) {
        return lz_parse_fast(src, src_size, dst, out_seqs, out_seq_count);
    }

    *out_seqs = NULL;
    *out_seq_count = 0;

    if (src_size < LZ_MIN_MATCH + 1) {
        return TDC_OK;
    }

    /* Match finder owns htab + chain_prev allocation. chain_depth here is
     * "links past htab" — chain_depth == 0 means flat hash (no chain_prev). */
    const tdc_lz_mf_vt *mf = &tdc_lz_mf_hashchain_vt;
    tdc_lz_mf_params mf_params = {
        .chain_depth = chain_depth,
        .hash_bits   = LZ_HASH_BITS,
    };
    tdc_lz_mf_ctx *mf_ctx = mf->create(src, src_size, &mf_params, dst);
    if (!mf_ctx) return TDC_E_NOMEM;

    uint32_t seq_cap = 4096;
    uint32_t seq_count = 0;
    LZSeq *seqs = (LZSeq *)lz_alloc(dst, seq_cap * sizeof(LZSeq));
    if (!seqs) {
        mf->destroy(mf_ctx, dst);
        return TDC_E_NOMEM;
    }

    uint32_t sp = 0;
    uint32_t lit_start = 0;
    /* Start rep state empty (all zeros). lz_rep_probe treats 0 slots as
     * unfilled and skips them; each slot becomes live only after a real
     * match has been emitted and promoted in via lz_rep_update. See the
     * long comment in lz_rep_probe for why synthetic inits like (1,4,8)
     * break LZ_STREAMS's stride-shift detection on aligned f64 inputs. */
    uint32_t rep[3] = { 0u, 0u, 0u };

    while (sp < src_size) {
        uint32_t match_off = 0;
        uint32_t match_len = 0;

        /* Probe both rep-at-literal and the hash chain; pick the better.
         *
         * Rep matches encode with a 1-symbol offset (0 extra bits) vs
         * ~10-20 extra bits for a novel offset in the common log2 buckets.
         * For rep and hash at the same match length, rep is strictly
         * cheaper. Conservative rule: take rep only when it matches or
         * beats the hash length. This keeps ratio monotone vs the pre-rep
         * baseline on non-periodic data (hash wins every strict comparison)
         * while letting rep dominate on periodic data (the hash usually
         * finds the same length at a noisier offset).
         *
         * Rep is also preferred when it ties a hash match of LZ_MIN_MATCH —
         * the ultra-cheap rep offset makes the match worth keeping where
         * the equivalent novel-offset match might not break even. */
        uint32_t rep_off = 0;
        uint32_t rep_len = lz_rep_probe(src, src_size, sp, rep, &rep_off);
        uint32_t hash_off = 0;
        uint32_t hash_len = mf->find_best(mf_ctx, sp, 0u, &hash_off);

        if (rep_len >= LZ_MIN_MATCH && rep_len >= hash_len) {
            match_len = rep_len;
            match_off = rep_off;
        } else {
            match_len = hash_len;
            match_off = hash_off;
        }

        /* Lazy matching: if enabled, check whether position sp+1 yields a
         * strictly longer match. If so, emit sp as a literal and take the
         * better match at sp+1. This repeats up to lazy_depth times.
         *
         * Runs even on rep hits. A rep-at-sp that wins the length race
         * against a short hash match at sp is still vulnerable to a much
         * longer hash match at sp+1 — losing that lazy win on noisy data
         * (temperature/streamflow) costs ratio. When the rep is truly
         * dominant (length matches the period), next_len at sp+1 won't
         * strictly exceed cur_len and the loop breaks on the first probe. */
        if (match_len >= LZ_MIN_MATCH && lazy_depth > 0) {
            uint32_t cur_pos = sp;
            uint32_t cur_len = match_len;
            uint32_t cur_off = match_off;

            for (uint32_t lazy = 0; lazy < lazy_depth; lazy++) {
                /* Insert current position before probing the next one,
                 * so that the chain is up to date. */
                mf->insert(mf_ctx, cur_pos);

                uint32_t next_off = 0;
                uint32_t next_len = mf->find_best(mf_ctx, cur_pos + 1, 0u, &next_off);

                if (next_len > cur_len) {
                    /* Better match at next position — skip current as literal. */
                    cur_pos++;
                    cur_len = next_len;
                    cur_off = next_off;
                } else {
                    break; /* current match is already the best */
                }
            }

            /* We may have advanced past sp; insert any skipped positions
             * that weren't inserted during the lazy probes. Position sp
             * was not yet inserted (the loop inserts cur_pos before probing
             * cur_pos+1, so sp was inserted if cur_pos > sp). But we need
             * to insert cur_pos itself now since we're about to consume it. */
            sp = cur_pos;
            match_len = cur_len;
            match_off = cur_off;
        }

        mf->insert(mf_ctx, sp);

        if (match_len >= LZ_MIN_MATCH) {
            uint32_t pending_lit = sp - lit_start;

            if (seq_count >= seq_cap) {
                uint32_t new_cap = seq_cap * 2;
                LZSeq *new_seqs = (LZSeq *)dst->realloc_fn(
                    dst->user, seqs, new_cap * sizeof(LZSeq));
                if (!new_seqs) {
                    mf->destroy(mf_ctx, dst);
                    lz_free(dst, seqs);
                    return TDC_E_NOMEM;
                }
                seqs = new_seqs;
                seq_cap = new_cap;
            }
            seqs[seq_count].lit_len = pending_lit;
            seqs[seq_count].match_len = match_len;
            seqs[seq_count].match_off = match_off;
            seq_count++;

            /* Update MRU rep state. Kept in sync with the serializer's
             * re-computation (lz_streams.c lzs_repcode_encode) so both
             * ends assign the same code to each match_off. */
            lz_rep_update(rep, match_off);

            /* Insert positions within the match into the hash chain so
             * future matches can reference them. Every 4th position is a
             * balance between chain quality and insert cost. */
            for (uint32_t i = 1; i < match_len && sp + i + LZ_MIN_MATCH + 1 <= src_size; i += 4) {
                mf->insert(mf_ctx, sp + i);
            }
            sp += match_len;
            lit_start = sp;
        } else {
            sp++;
        }
    }

    mf->destroy(mf_ctx, dst);

    *out_seqs = seqs;
    *out_seq_count = seq_count;
    return TDC_OK;
}

/* ----- Encoder (greedy single-stream) ------------------------------------ */
/*
 * Top-level LZ encoder. Calls the shared greedy parser, then hands the
 * resulting sequence array to the shared single-stream serializer.
 * TDC_ENTROPY_LZ_STREAMS shares the parser via tdc_lz_parse_greedy and
 * uses its own multi-stream serializer instead.
 */
/* Map tdc_entropy_level::level to (chain_depth, lazy_depth).
 *
 * Level 0 (default): chain_depth=4, lazy=1.  Good balance of speed and
 *   ratio — roughly equivalent to zstd's default greedy mode.
 * Level 1: chain_depth=0, lazy=0.  Flat hash, no lazy — maximum speed,
 *   lowest ratio. Use when encode throughput matters more than ratio.
 * Level 2: chain_depth=4, lazy=0.  Hash chains but no lazy — faster
 *   than default, better ratio than level 1.
 * Level 3: chain_depth=4, lazy=1.  Same as default (alias).
 * Level 4: chain_depth=8, lazy=1.  Deeper search.
 * Level 5: chain_depth=16, lazy=1.
 * Level 6: chain_depth=32, lazy=2.  Double lazy — slow but thorough.
 * Level 7+: chain_depth=64, lazy=2.
 */
static void lz_level_to_params(int level,
                                uint32_t *chain_depth,
                                uint32_t *lazy_depth) {
    switch (level) {
    case 1:  *chain_depth = 0;  *lazy_depth = 0; break;
    case 2:  *chain_depth = 4;  *lazy_depth = 0; break;
    case 4:  *chain_depth = 8;  *lazy_depth = 1; break;
    case 5:  *chain_depth = 16; *lazy_depth = 1; break;
    case 6:  *chain_depth = 32; *lazy_depth = 2; break;
    default:
        if (level >= 7) {
            *chain_depth = 64; *lazy_depth = 2;
        } else {
            /* level 0 (default) and level 3 */
            *chain_depth = 4; *lazy_depth = 1;
        }
        break;
    }
}

static tdc_status lz_encode_core(const uint8_t *src, uint32_t src_size,
                                  const void *params, tdc_buffer *dst) {
    int level = 0;
    if (params) {
        const tdc_entropy_level *lp = (const tdc_entropy_level *)params;
        level = lp->level;
    }

    uint32_t chain_depth, lazy_depth;
    lz_level_to_params(level, &chain_depth, &lazy_depth);

    LZSeq *seqs = NULL;
    uint32_t seq_count = 0;
    tdc_status st = tdc_lz_parse_greedy(src, src_size, dst,
                                         chain_depth, lazy_depth,
                                         &seqs, &seq_count);
    if (st != TDC_OK) return st;

    if (seq_count == 0) {
        /* Empty parse (input too short or trivial) — fall back. */
        return lz_encode_literal_only(src, src_size, dst);
    }

    st = tdc_lz_serialize_sequences(src, src_size, seqs, seq_count, dst);
    lz_free(dst, seqs);
    return st;
}

/* ----- Decoder ----------------------------------------------------------- */
/*
 * Note on batched parse-then-copy: this loop interleaves sequence parsing
 * with literal/match copies on purpose. A batched variant (parse 4-16
 * sequences ahead into a small stack array, then run a pure copy loop
 * over the parsed descriptors) was tried in vectra and measured ~6-7%
 * slower at every batch size. The reason is structural: LZ's parse is
 * just a tag byte + occasional varint extension + varint offset — so
 * cheap that out-of-order execution already overlaps the next sequence's
 * parse with the current sequence's wildcopy. Forcing a phase split
 * serializes parse and copy, costing the OoO overlap without recovering
 * anything because the parse phase has nothing expensive enough to
 * benefit from a dedicated decode pipeline.
 *
 * Revisit only once a real entropy stage (FSE/Huffman) lives in front
 * of LZ: at that point parse becomes expensive enough to benefit from
 * being decoupled from copy.
 */
static inline void lz_decode_fast(
    uint8_t *dst, const uint8_t *lit_data,
    const uint8_t *seq_ptr, const uint8_t *seq_end, uint32_t n_seq,
    uint32_t uncompressed_size, uint32_t literals_size,
    const uint8_t **seq_ptr_out, uint32_t *dp_out, uint32_t *lp_out,
    uint32_t *si_out)
{
    uint32_t dp = *dp_out, lp = *lp_out, si = *si_out;
    const uint8_t *sp = seq_ptr;

    while (LZ_LIKELY(si < n_seq)) {
        /* Save position before parsing (for rewind on bail). */
        const uint8_t *sp_save = sp;

        /* Every sequence-byte read is bounded by seq_end so a crafted length or
         * varint cannot walk `sp` past the sequence region; on any shortfall we
         * rewind and hand off to the safe path, which reports corruption. */
        if (LZ_UNLIKELY(sp >= seq_end)) break;

        /* Parse packed [LLLLMMMM] tag byte: high nibble = literal-run length
         * (or 15 = extended via chained-255 varint), low nibble =
         * match_len - 3 (or 15 = extended via LEB128). */
        uint8_t tag = *sp++;
        uint32_t lit_len = tag >> 4;
        uint32_t match_len_m3 = tag & 0x0F;

        if (lit_len == 15) {
            uint8_t extra;
            do {
                if (LZ_UNLIKELY(sp >= seq_end)) { sp = sp_save; goto fast_done; }
                extra = *sp++; lit_len += extra;
            } while (extra == 255);
        }
        if (match_len_m3 == 15) {
            uint32_t extra = 0;
            uint32_t shift = 0;
            uint8_t b;
            do {
                if (LZ_UNLIKELY(sp >= seq_end || shift >= 32u)) { sp = sp_save; goto fast_done; }
                b = *sp++;
                extra |= ((uint32_t)(b & 0x7Fu)) << shift;
                shift += 7;
            } while (b & 0x80u);
            match_len_m3 += extra;
        }
        uint32_t mlen = match_len_m3 + LZ_MIN_MATCH;

        uint32_t off;
        const uint8_t *sp_next = lz_offset_read_bounded(sp, seq_end, &off);
        if (LZ_UNLIKELY(!sp_next)) { sp = sp_save; goto fast_done; }
        sp = sp_next;

        /* Bail to safe path if this sequence would exceed bounds. The
         * TDC_WILDCOPY_SLACK margin covers the worst-case overshoot of
         * tdc_match_copy (31 bytes on AVX2 builds, 15 otherwise). Long
         * matches (now possible since the varint extension lifted the
         * old 130-byte cap) frequently trip this and finish in the safe
         * tail — that path uses memcpy and handles overlap correctly. */
        if (LZ_UNLIKELY(dp + lit_len + mlen + TDC_WILDCOPY_SLACK > uncompressed_size)) {
            sp = sp_save; /* rewind — safe path will re-parse */
            break;
        }
        if (LZ_UNLIKELY(lp + lit_len > literals_size)) {
            sp = sp_save;
            break;
        }
        /* Invalid back-reference: the match would read before the start of the
         * decoded output (off past the current position dp + lit_len). Bail to
         * the safe path, which returns TDC_E_CORRUPT. */
        if (LZ_UNLIKELY(off > dp + lit_len)) {
            sp = sp_save;
            break;
        }

        /* Prefetch match address before literal copy. */
        TDC_PREFETCH_L1(dst + dp - off + lit_len);

        /* Copy literals. The 16-byte SIMD path overreads up to 16 bytes
         * from lit_data, which lives inside the *input* buffer (no
         * caller-provided trailing slack). Gate the SIMD path on
         * lp + 16 <= literals_size; fall back to exact memcpy near the
         * literals tail. */
        if (LZ_LIKELY(lit_len > 0)) {
            if (lit_len <= 16 && lp + 16 <= literals_size) {
                tdc_copy16(dst + dp, lit_data + lp);
            } else {
                memcpy(dst + dp, lit_data + lp, lit_len);
            }
            lp += lit_len;
            dp += lit_len;
        }

        /* Copy match — wildcopy is safe because the bounds check above
         * reserved 15 bytes of slack at dst. */
        tdc_match_copy(dst + dp, off, mlen);
        dp += mlen;

        si++;
    }

fast_done:
    *seq_ptr_out = sp;
    *dp_out = dp;
    *lp_out = lp;
    *si_out = si;
}

/* Safe decode: full bounds checking, byte-accurate copies. Picks up where
 * the fast path bailed (or runs the whole decode if the fast path declined
 * the input). Returns TDC_E_CORRUPT on invalid back-references. */
static tdc_status lz_decode_safe(
    uint8_t *dst, const uint8_t *lit_data,
    const uint8_t *seq_ptr, const uint8_t *seq_end, uint32_t n_seq,
    uint32_t uncompressed_size, uint32_t literals_size,
    uint32_t *dp_out, uint32_t *lp_out, uint32_t *si_out)
{
    uint32_t dp = *dp_out, lp = *lp_out, si = *si_out;
    const uint8_t *sp = seq_ptr;

    while (si < n_seq && dp < uncompressed_size) {
        /* Bounds-check every sequence-byte read against seq_end. */
        if (sp >= seq_end) return TDC_E_CORRUPT;
        uint8_t tag = *sp++;
        uint32_t lit_len = tag >> 4;
        uint32_t match_len_m3 = tag & 0x0F;

        if (lit_len == 15) {
            uint8_t extra;
            do {
                if (sp >= seq_end) return TDC_E_CORRUPT;
                extra = *sp++; lit_len += extra;
            } while (extra == 255);
        }
        if (match_len_m3 == 15) {
            uint32_t extra = 0;
            uint32_t shift = 0;
            uint8_t b;
            do {
                if (sp >= seq_end || shift >= 32u) return TDC_E_CORRUPT;
                b = *sp++;
                extra |= ((uint32_t)(b & 0x7Fu)) << shift;
                shift += 7;
            } while (b & 0x80u);
            match_len_m3 += extra;
        }
        uint32_t mlen = match_len_m3 + LZ_MIN_MATCH;

        uint32_t off;
        sp = lz_offset_read_bounded(sp, seq_end, &off);
        if (!sp) return TDC_E_CORRUPT;

        /* Copy literals (clamped) */
        if (lit_len > 0) {
            uint32_t ll = lit_len;
            if (ll > uncompressed_size - dp) ll = uncompressed_size - dp;
            if (lp + ll > literals_size) ll = literals_size - lp;
            memcpy(dst + dp, lit_data + lp, ll);
            lp += ll;
            dp += ll;
        }

        /* Copy match — overlap-correct, with the same doubling pattern the
         * fast path uses so long offset==1 matches (e.g. PLANE2D's
         * all-zero residual stream now that the LZ max-match cap is
         * removed) decode at memcpy speed instead of byte-by-byte.
         *
         * Invariant: after seeding `off` bytes at op[0..off), each
         * iteration copies a non-overlapping chunk of `filled` bytes
         * from op[0..filled) to op[filled..2*filled). chunk doubles per
         * iter, so the loop runs in O(log mlen) memcpys. */
        if (dp < uncompressed_size) {
            if (dp < off) return TDC_E_CORRUPT; /* invalid back-reference */
            if (mlen > uncompressed_size - dp) mlen = uncompressed_size - dp;
            if (off >= mlen) {
                memcpy(dst + dp, dst + dp - off, mlen);
            } else {
                uint8_t *op = dst + dp;
                memcpy(op, op - off, off);          /* seed */
                uint32_t filled = off;
                while (filled < mlen) {
                    uint32_t chunk = filled;
                    if (filled + chunk > mlen) chunk = mlen - filled;
                    memcpy(op + filled, op, chunk); /* op[0..chunk) and
                                                       op[filled..filled+chunk)
                                                       are disjoint because
                                                       chunk <= filled */
                    filled += chunk;
                }
            }
            dp += mlen;
        }

        si++;
    }

    /* Trailing literals (after last match) */
    if (lp < literals_size && dp < uncompressed_size) {
        uint32_t trail = literals_size - lp;
        if (trail > uncompressed_size - dp) trail = uncompressed_size - dp;
        memcpy(dst + dp, lit_data + lp, trail);
        lp += trail;
        dp += trail;
    }

    *dp_out = dp;
    *lp_out = lp;
    *si_out = si;
    return TDC_OK;
}

static tdc_status lz_decode_core(uint8_t *dst, uint32_t uncompressed_size,
                                  const uint8_t *src, uint32_t src_size) {
    if (src_size < LZ_HEADER_SIZE) return TDC_E_CORRUPT;

    uint32_t n_seq, literals_size;
    memcpy(&n_seq,         src,     4);
    memcpy(&literals_size, src + 4, 4);

    /* Literals follow sequence headers; we don't know seq_hdr_size from the
     * header alone, so we derive it: total - 8 - literals = seq_hdr_size. */
    if ((size_t)LZ_HEADER_SIZE + literals_size > src_size) return TDC_E_CORRUPT;
    uint32_t seq_hdr_size = src_size - LZ_HEADER_SIZE - literals_size;

    const uint8_t *seq_start = src + LZ_HEADER_SIZE;
    const uint8_t *lit_data  = seq_start + seq_hdr_size;

    uint32_t si = 0, dp = 0, lp = 0;
    const uint8_t *seq_ptr = seq_start;

    /* The sequence region is [seq_start, lit_data); pass lit_data as seq_end so
     * both decoders bound every sequence-byte read. */
    const uint8_t *seq_end = lit_data;

    /* Fast path for the bulk of the data. */
    if (n_seq > 0) {
        lz_decode_fast(dst, lit_data, seq_ptr, seq_end, n_seq,
                        uncompressed_size, literals_size,
                        &seq_ptr, &dp, &lp, &si);
    }

    /* Safe tail: bounds-checked, handles remaining sequences + trailing
     * literals. Also runs in full when n_seq == 0 (literal-only fallback). */
    tdc_status st = lz_decode_safe(dst, lit_data, seq_ptr, seq_end, n_seq,
                                    uncompressed_size, literals_size,
                                    &dp, &lp, &si);
    if (st != TDC_OK) return st;

    if (dp != uncompressed_size) return TDC_E_CORRUPT;
    return TDC_OK;
}

/* ----- vtable wiring ----------------------------------------------------- */

static size_t lz_encode_bound(size_t src_size) {
    /* Worst case: literal-only stream = 8-byte header + raw bytes. */
    return src_size + LZ_HEADER_SIZE;
}

static tdc_status lz_encode(const uint8_t *src, size_t src_size,
                             const void *params, tdc_buffer *dst) {
    if (!dst || !dst->realloc_fn) return TDC_E_INVAL;
    if (src_size > UINT32_MAX) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;
    return lz_encode_core(src, (uint32_t)src_size, params, dst);
}

static tdc_status lz_decode(const uint8_t *src, size_t src_size,
                             uint8_t *dst, size_t dst_size) {
    if (src_size > UINT32_MAX || dst_size > UINT32_MAX) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;
    if (dst_size > 0 && !dst) return TDC_E_INVAL;
    return lz_decode_core(dst, (uint32_t)dst_size,
                           src, (uint32_t)src_size);
}

const tdc_entropy_vt tdc_entropy_lz_vt = {
    .id           = TDC_ENTROPY_LZ,
    .name         = "lz",
    .encode_bound = lz_encode_bound,
    .encode       = lz_encode,
    .decode       = lz_decode,
};
