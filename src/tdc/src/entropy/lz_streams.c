/*
 * src/entropy/lz_streams.c
 *
 * TDC_ENTROPY_LZ_STREAMS — LZ parse with four separated, entropy-coded
 * streams.
 *
 * Shares the greedy parser (tdc_lz_parse_greedy) with TDC_ENTROPY_LZ.
 * Where the single-stream serializer interleaves sequence descriptors and
 * literals into one packed byte stream, this backend splits the parser
 * output into four streams and applies an independent entropy coder to
 * each:
 *
 *   literal_stream   — all literal bytes concatenated (includes trailing)
 *   lit_len_stream   — n_seqs × u32 lit_len, byte-shuffled
 *   match_len_stream — n_seqs × u32 match_len, byte-shuffled
 *   offset_sym_stream — n_seqs × u8 log2-prefix symbol (v2)
 *   offset_extra      — packed extra bits for novel offsets (v2, raw)
 *
 * Format v2 (current) uses log2-prefix offset encoding: after repcodes,
 * each offset is split into a symbol (0-23, entropy-coded) and extra
 * bits (packed raw). Symbols 0-2 are repcodes (0 extra bits), symbols
 * 3-23 encode floor(log2(offset))+3 with the remainder as extra bits.
 * This compresses the offset stream far better than v1's byte-shuffled
 * u32, especially for structured data with dominant strides.
 *
 * Each stream picks between NONE / HUFFMAN / FSE by trying all coders
 * and keeping the smallest. A whole-block fallback also exists: if the
 * full encoded size meets or exceeds src_size, the header is written
 * with n_seqs=0 and the lit stream carries src as passthrough NONE.
 *
 * On-disk header v2 (44 bytes, fixed):
 *
 *   offset  size  field
 *     0      1    format_version         (2)
 *     1      1    flags                  (bit 0: LZS_FLAG_REPCODES)
 *     2      2    reserved               (zero)
 *     4      4    n_seqs                 (0 => passthrough fallback)
 *     8      4    total_lit_size         (bytes in literal_stream)
 *    12      4    trailing_lit_len       (literals after last match)
 *    16      4    uncompressed_size      (== dst_size on decode)
 *    20      4    entropy_id[4]          (lit, lit_len, match_len, off_sym)
 *    24      4    stream_size_lit
 *    28      4    stream_size_lit_len
 *    32      4    stream_size_match_len
 *    36      4    stream_size_off_sym
 *    40      4    stream_size_off_extra
 *
 * followed by the five streams: lit, ll, ml, off_sym, off_extra.
 *
 * Repcode transform (when LZS_FLAG_REPCODES is set):
 *
 * Instead of the raw match_off (1..65536), the match_off stream carries an
 * offset_code that exploits offset-reuse locality. Three recent offsets
 * (rep1, rep2, rep3, initialised to 1, 4, 8 — the zstd defaults) are
 * maintained as an MRU list during parsing. On each sequence:
 *
 *   match_off == rep1  ->  code = 1           (state unchanged)
 *   match_off == rep2  ->  code = 2           (rep1,rep2 swap)
 *   match_off == rep3  ->  code = 3           (rep3 promoted, others shift)
 *   otherwise          ->  code = off + 3     (new offset becomes rep1)
 *
 * Structured tabular data hammers a few strides (8 for f64 columns, 1 for
 * RLE, the row width for per-row repetition). Under repcodes the match_off
 * stream collapses to a histogram dominated by the single symbol "1",
 * which the downstream entropy coder (Huffman or FSE) compresses to
 * near-zero bits/symbol. The decoder reverses the transform with the same
 * rep-state walk before reconstructing the output.
 */

#include "tdc/entropy.h"
#include "tdc/codec.h"
#include "entropy_internal.h"
#include "fse_internal.h"
#include "lz_internal.h"
#include "../format/metadata_internal.h"
#include "../core/buffer.h"
#include "../core/log.h"
#include "../core/simd.h"
#include "../core/decode_profile.h"
#include "../core/timer.h"

#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* Diagnostic dump (enabled at runtime via TDC_LZS_DUMP=1). Prints, per
 * encode call: src_size, n_seqs, per-stream raw and compressed sizes, and
 * the coder id each stream ended up with. Use this to see where the bytes
 * are going on a given workload before chasing micro-optimizations. */
static int lzs_dump_enabled(void) {
    static int checked = 0;
    static int enabled = 0;
    if (!checked) {
        const char *v = getenv("TDC_LZS_DUMP");
        enabled = (v && v[0] && v[0] != '0');
        checked = 1;
    }
    return enabled;
}

/* Per-phase encode wall-time breakdown (enabled via TDC_LZS_TIMING=1).
 * Prints one line per encode call with cumulative seconds in each phase:
 * initial parse, priced re-parses, stream build, entropy, lit-context.
 * Use to identify the dominant phase before chasing micro-optimizations. */
static int lzs_timing_enabled(void) {
    static int checked = 0;
    static int enabled = 0;
    if (!checked) {
        const char *v = getenv("TDC_LZS_TIMING");
        enabled = (v && v[0] && v[0] != '0');
        checked = 1;
    }
    return enabled;
}

#define LZS_HEADER_SIZE_V1   40u
#define LZS_HEADER_SIZE_V2   44u
#define LZS_FORMAT_VERSION   2u
#define LZS_FLAG_REPCODES    0x01u
#define LZS_FLAG_OFFSET_SHIFT 0x02u  /* offsets right-shifted by header[2] bits */
#define LZS_FLAG_LIT_CONTEXT  0x04u  /* literals split by order-1 context */
#define LZS_FLAG_DELTA_OFFSETS 0x10u /* novel offsets: zigzag(off - rep1) */

/* Order-1 literal context: 16 buckets keyed on high nibble of previous
 * literal byte. When LZS_FLAG_LIT_CONTEXT is set, the literal section
 * in the body carries a mini-header (ctx_ids, ctx_counts, ctx_sizes)
 * followed by 16 independently entropy-coded sub-streams. The main
 * header's sz_lit field holds the total context section size; id_lit
 * is unused (per-bucket ids in the context header are authoritative).
 *
 * Context section layout:
 *   u8[16]   ctx_ids     — per-bucket entropy coder ID
 *   u32[16]  ctx_counts  — per-bucket uncompressed byte count
 *   u32[16]  ctx_sizes   — per-bucket compressed stream size
 *   [stream_0] .. [stream_15]
 */
#define LZS_LIT_N_CTX         16u
#define LZS_LIT_CTX_HDR_SIZE  (LZS_LIT_N_CTX + 2u * LZS_LIT_N_CTX * 4u)  /* 144 bytes */

/* Repcode init values live in lz_internal.h (shared with lz.c greedy
 * parser and lz_opt.c optimal parser). Alias the old names locally so
 * the existing static helpers read naturally. */
#define LZS_REP_INIT_1       LZ_REP_INIT_1
#define LZS_REP_INIT_2       LZ_REP_INIT_2
#define LZS_REP_INIT_3       LZ_REP_INIT_3

/* ----- Log2-prefix offset encoding (format v2) --------------------------- */
/*
 * After repcode transform, offset codes are:
 *   1-3:  repcodes  → symbol 0-2, 0 extra bits
 *   >= 4: novel     → actual_off = code-3, symbol = floor_log2(off)+3,
 *                      extra = off - (1 << floor_log2(off)), nbits extra bits
 *
 * Symbol alphabet: 0..LZS_MAX_OFFSET_SYMBOL (26 symbols for 22-bit window).
 * The entropy coder sees only this small alphabet; extra bits are packed
 * separately as a raw bitstream.
 */
#define LZS_MAX_OFFSET_SYMBOL 25u  /* floor_log2(LZ_MAX_OFFSET) + 3 */
#define LZS_MAX_OFFSET_DELTA_SYMBOL 27u  /* delta: zigzag up to ~2×LZ_MAX_OFFSET */

static inline uint32_t lzs_floor_log2(uint32_t v) {
    uint32_t r = 0;
    while (v > 1u) { v >>= 1; r++; }
    return r;
}

/* --- Generic log2-prefix for non-negative integers ----------------------- *
 *
 * Encodes v >= 0 as (symbol, extra_bits):
 *   v = 0  → sym 0, 0 extra bits
 *   v = 1  → sym 1, 0 extra bits
 *   v >= 2 → sym = floor_log2(v) + 1, nbits = floor_log2(v),
 *             extra = v - (1 << nbits)
 *
 * Used for lit_len (v = lit_len) and match_len (v = match_len - 3).
 */
static inline void lzs_uint_to_symbol(uint32_t v,
                                        uint8_t *sym,
                                        uint32_t *extra,
                                        uint32_t *nbits) {
    if (v <= 1u) {
        *sym = (uint8_t)v;
        *extra = 0;
        *nbits = 0;
    } else {
        uint32_t lg = lzs_floor_log2(v);
        *sym   = (uint8_t)(lg + 1u);
        *extra = v - (1u << lg);
        *nbits = lg;
    }
}

static inline uint32_t lzs_symbol_to_uint(uint8_t sym, uint32_t extra) {
    if (sym <= 1u) return (uint32_t)sym;
    uint32_t lg = (uint32_t)sym - 1u;
    return (1u << lg) + extra;
}

/* Max symbol for lit_len / match_len_m3. Covers src blocks up to
 * (1<<25) = 32 MiB (sym 25 encodes [2^24, 2^25-1]). The original cap of
 * 21 assumed ~1 MiB blocks, but nothing in the pipeline enforced that,
 * so larger inputs (e.g. DICT_NUMERIC u32 indices from a 1M-elem f64
 * block → ~4 MiB post-shuffle) silently produced sym 22+ which the
 * decoder's range check rejected as corrupt. 25 matches
 * LZS_MAX_OFFSET_SYMBOL and stays within the 25-bit single-read budget
 * of lzs_br_read. */
#define LZS_MAX_LL_SYMBOL  25u
#define LZS_MAX_ML_SYMBOL  25u

/* --- Offset log2-prefix with repcode symbols ----------------------------- */

static inline void lzs_offset_to_symbol(uint32_t code,
                                          uint8_t *sym,
                                          uint32_t *extra,
                                          uint32_t *nbits) {
    if (code <= 3u) {
        *sym = (uint8_t)(code - 1u);
        *extra = 0;
        *nbits = 0;
    } else {
        uint32_t off = code - 3u;
        uint32_t lg  = lzs_floor_log2(off);
        *sym   = (uint8_t)(lg + 3u);
        *extra = off - (1u << lg);
        *nbits = lg;
    }
}

static inline uint32_t lzs_symbol_to_code(uint8_t sym, uint32_t extra) {
    if (sym <= 2u) return (uint32_t)sym + 1u;
    uint32_t lg = (uint32_t)sym - 3u;
    return (1u << lg) + extra + 3u;
}

/* Branchless reconstruct via lookup. (base, nbits) per symbol; reconstruct
 * is `base + read(nbits)`. Replaces the per-sequence sym<=1 / sym>=3 branches
 * in the fused decode hot loop. Tables are tiny (cache-resident). */
typedef struct { uint32_t base; uint8_t nbits; } lzs_recon_t;

/* LL/ML uint reconstruct: sym 0→(0,0), 1→(1,0), k≥2→(1<<(k-1), k-1). */
static const lzs_recon_t LZS_UINT_RECON[LZS_MAX_LL_SYMBOL + 1u] = {
    {0u, 0}, {1u, 0},
    {1u << 1, 1},  {1u << 2, 2},  {1u << 3, 3},  {1u << 4, 4},
    {1u << 5, 5},  {1u << 6, 6},  {1u << 7, 7},  {1u << 8, 8},
    {1u << 9, 9},  {1u <<10,10},  {1u <<11,11},  {1u <<12,12},
    {1u <<13,13},  {1u <<14,14},  {1u <<15,15},  {1u <<16,16},
    {1u <<17,17},  {1u <<18,18},  {1u <<19,19},  {1u <<20,20},
    {1u <<21,21},  {1u <<22,22},  {1u <<23,23},  {1u <<24,24},
};

/* Non-delta offset reconstruct: sym 0..2→(1..3, 0), k≥3→((1<<(k-3))+3, k-3). */
static const lzs_recon_t LZS_OFF_RECON[LZS_MAX_OFFSET_SYMBOL + 1u] = {
    {1u, 0}, {2u, 0}, {3u, 0},
    {(1u << 0) + 3u, 0},
    {(1u << 1) + 3u, 1},
    {(1u << 2) + 3u, 2},
    {(1u << 3) + 3u, 3},
    {(1u << 4) + 3u, 4},
    {(1u << 5) + 3u, 5},
    {(1u << 6) + 3u, 6},
    {(1u << 7) + 3u, 7},
    {(1u << 8) + 3u, 8},
    {(1u << 9) + 3u, 9},
    {(1u <<10) + 3u,10},
    {(1u <<11) + 3u,11},
    {(1u <<12) + 3u,12},
    {(1u <<13) + 3u,13},
    {(1u <<14) + 3u,14},
    {(1u <<15) + 3u,15},
    {(1u <<16) + 3u,16},
    {(1u <<17) + 3u,17},
    {(1u <<18) + 3u,18},
    {(1u <<19) + 3u,19},
    {(1u <<20) + 3u,20},
    {(1u <<21) + 3u,21},
    {(1u <<22) + 3u,22},
};

/* Zigzag encoding for signed→unsigned mapping of offset deltas.
 * Branchless: standard protobuf-style zigzag via arithmetic shift. */
static inline uint32_t lzs_zigzag_encode(int32_t v) {
    return ((uint32_t)v << 1) ^ (uint32_t)(v >> 31);
}
static inline int32_t lzs_zigzag_decode(uint32_t z) {
    return (int32_t)(z >> 1) ^ -(int32_t)(z & 1u);
}

/* Rep-state update from a repcode-encoded offset code. */
static inline void lzs_rep_update(uint32_t code,
                                    uint32_t *r1, uint32_t *r2, uint32_t *r3) {
    if (code == 2u) { uint32_t t = *r1; *r1 = *r2; *r2 = t; }
    else if (code == 3u) { uint32_t t = *r3; *r3 = *r2; *r2 = *r1; *r1 = t; }
    else if (code >= 4u) { *r3 = *r2; *r2 = *r1; *r1 = code - 3u; }
    /* code == 1: no change */
}

/* Bit packing — caller must memset buffer to zero before writing.
 * Word-at-a-time: OR the value (up to 25 bits, fitting in 32 bits even
 * with a 7-bit misalignment) into a 32-bit window at the current bit
 * position. The caller's buffer must have at least 3 bytes of slack
 * past the last written bit to absorb the partial-word overwrite. */
static inline void lzs_bits_write(uint8_t *buf, uint32_t *bit_pos,
                                    uint32_t val, uint32_t nbits) {
    if (nbits == 0) return;
    uint32_t pos = *bit_pos;
    uint32_t byte_idx = pos >> 3;
    uint32_t bit_off  = pos & 7u;
    /* Read existing partial byte, OR in new bits, write back.
     * val is at most 25 bits (max offset symbol extra); shifted by up to
     * 7 bits = 32 bits total, fits in a uint32_t write. We do an
     * unaligned 4-byte read-modify-write which is safe on x86/ARM. */
    uint32_t window;
    memcpy(&window, buf + byte_idx, 4);
    window |= (val << bit_off);
    memcpy(buf + byte_idx, &window, 4);
    *bit_pos = pos + nbits;
}

/* Fast bits reader: uses a 4-byte unaligned load when there are at least
 * 4 bytes remaining from byte_idx. Falls back to byte-by-byte for the
 * last 3 bytes. buf_size is the total byte count of buf. */
static inline uint32_t lzs_bits_read_safe(const uint8_t *buf, uint32_t buf_size,
                                            uint32_t *bit_pos, uint32_t nbits) {
    if (nbits == 0) return 0;
    uint32_t pos = *bit_pos;
    uint32_t byte_idx = pos >> 3;
    uint32_t bit_off  = pos & 7u;
    *bit_pos = pos + nbits;

    /* Fast path: 4-byte unaligned load. Covers nbits + bit_off <= 32
     * (max nbits ~25, max bit_off 7 → 32). Safe on LE x86/ARM. */
    if (byte_idx + 4 <= buf_size) {
        uint32_t window;
        memcpy(&window, buf + byte_idx, 4);
        return (window >> bit_off) & ((1u << nbits) - 1u);
    }

    /* Slow tail: byte-by-byte for last 3 bytes of buffer. */
    uint32_t val = 0;
    uint32_t bits_in_byte = 8u - bit_off;
    uint32_t byte_val = (uint32_t)buf[byte_idx] >> bit_off;
    if (nbits <= bits_in_byte) {
        val = byte_val & ((1u << nbits) - 1u);
    } else {
        val = byte_val;
        uint32_t remaining = nbits - bits_in_byte;
        uint32_t shift = bits_in_byte;
        byte_idx++;
        while (remaining >= 8u) {
            val |= (uint32_t)buf[byte_idx] << shift;
            byte_idx++;
            shift += 8u;
            remaining -= 8u;
        }
        if (remaining > 0) {
            val |= ((uint32_t)buf[byte_idx] & ((1u << remaining) - 1u)) << shift;
        }
    }
    return val;
}

/* Backwards-compatible wrapper when buffer size is not available. */
static inline uint32_t lzs_bits_read(const uint8_t *buf, uint32_t *bit_pos,
                                       uint32_t nbits) {
    /* Assume 4-byte safety — only the encode path calls this, and the
     * encode path's extras buffer always has 4 bytes of slack. */
    if (nbits == 0) return 0;
    uint32_t pos = *bit_pos;
    uint32_t byte_idx = pos >> 3;
    uint32_t bit_off  = pos & 7u;
    *bit_pos = pos + nbits;
    uint32_t window;
    memcpy(&window, buf + byte_idx, 4);
    return (window >> bit_off) & ((1u << nbits) - 1u);
}

/* ----- Allocation helpers ------------------------------------------------- */

static void *lzs_alloc(tdc_buffer *buf, size_t n) {
    return buf->realloc_fn(buf->user, NULL, n);
}

static void lzs_free(tdc_buffer *buf, void *p) {
    if (p) (void)buf->realloc_fn(buf->user, p, 0);
}

/* ----- Sub-coder vtable lookup ------------------------------------------- */

static const tdc_entropy_vt *lzs_sub_vt(tdc_entropy_id id) {
    switch (id) {
        case TDC_ENTROPY_NONE:    return &tdc_entropy_none_vt;
        case TDC_ENTROPY_HUFFMAN: return &tdc_entropy_huffman_vt;
        case TDC_ENTROPY_HUFFMAN4:return &tdc_entropy_huffman4_vt;
        case TDC_ENTROPY_FSE:     return &tdc_entropy_fse_vt;
        default:                  return NULL;
    }
}

/* Stream coder selection: try all three (NONE / HUFFMAN / FSE) and keep
 * the smallest output. The previous Shannon-entropy heuristic misclassified
 * the match-offset stream after repcodes — Shannon put it in the "medium
 * entropy, use FSE" bucket, but tdc's FSE compresses a near-constant
 * histogram ~5× worse than Huffman does. Measuring both directly removes
 * the heuristic error at the cost of 2-3× encode time, which is explicitly
 * acceptable in SMALL mode.
 *
 * NONE is included as a floor so the "coder expanded the stream" case is
 * handled in the same comparison instead of as a separate fallback. */

/* ----- Repcode transform -------------------------------------------------- */
/*
 * Encode: walk the LZSeq array, replacing each .match_off with an
 * offset_code in [1, LZ_MAX_OFFSET + 3]. Codes 1/2/3 encode rep1/rep2/rep3;
 * any other code is (actual_offset + 3). Rep state is maintained MRU.
 *
 * Decode: walk the unshuffled match_off stream, replacing each offset_code
 * with its actual_offset. Same rep state walk as encode — the two ends
 * stay in lockstep by construction.
 */
/* Compute the largest power-of-2 that divides all match offsets.
 * Returns the shift (trailing zeros), clamped to 0-7. */
static uint32_t lzs_detect_offset_shift(const LZSeq *seqs, uint32_t n_seqs) {
    if (n_seqs == 0) return 0;
    uint32_t all_or = 0;
    for (uint32_t i = 0; i < n_seqs; i++) {
        all_or |= seqs[i].match_off;
    }
    if (all_or == 0) return 0;
    uint32_t shift = 0;
    while (shift < 7u && (all_or & (1u << shift)) == 0) shift++;
    return shift;
}

/* Post-parse filter: drop every sequence whose match is shorter than
 * min_match, folding the dropped (literal + match) bytes into the next
 * sequence's literal run. If the last sequence has a short match, we drop
 * it entirely — the trailing-bytes path in lzs_encode handles the uncovered
 * suffix. Called BEFORE lzs_repcode_encode, so repcode state is built over
 * the filtered list only.
 *
 * The on-disk format is unchanged: any valid (ll, ml>=3, mo) sequence list
 * decodes correctly. Raising min_match just produces fewer, longer
 * sequences, which raises decode throughput proportionally. */
static void lzs_filter_short_matches(LZSeq *seqs, uint32_t *n_seqs,
                                      uint32_t min_match) {
    if (min_match <= LZ_MIN_MATCH) return;
    uint32_t n = *n_seqs;
    if (n == 0) return;

    uint32_t w = 0;
    uint32_t carry_ll = 0;   /* bytes absorbed from dropped seqs */
    for (uint32_t r = 0; r < n; r++) {
        if (seqs[r].match_len < min_match) {
            /* Fold (lit + match) bytes of this seq into the next seq's
             * literal run via the carry. */
            carry_ll += seqs[r].lit_len + seqs[r].match_len;
            continue;
        }
        seqs[w] = seqs[r];
        seqs[w].lit_len += carry_ll;
        carry_ll = 0;
        w++;
    }
    /* Any trailing carry_ll is uncovered suffix; lzs_encode's trailing path
     * picks it up automatically via (src_size - sum(ll+ml)). */
    *n_seqs = w;
}

static void lzs_repcode_encode(LZSeq *seqs, uint32_t n_seqs) {
    uint32_t r1 = LZS_REP_INIT_1;
    uint32_t r2 = LZS_REP_INIT_2;
    uint32_t r3 = LZS_REP_INIT_3;
    for (uint32_t i = 0; i < n_seqs; i++) {
        uint32_t off = seqs[i].match_off;
        uint32_t code;
        if (off == r1) {
            code = 1u;
            /* state unchanged */
        } else if (off == r2) {
            code = 2u;
            uint32_t t = r1; r1 = r2; r2 = t;
        } else if (off == r3) {
            code = 3u;
            uint32_t t = r3; r3 = r2; r2 = r1; r1 = t;
        } else {
            code = off + 3u;
            r3 = r2; r2 = r1; r1 = off;
        }
        seqs[i].match_off = code;
    }
}

static tdc_status lzs_repcode_decode(uint32_t *match_offs, uint32_t n_seqs) {
    uint32_t r1 = LZS_REP_INIT_1;
    uint32_t r2 = LZS_REP_INIT_2;
    uint32_t r3 = LZS_REP_INIT_3;
    for (uint32_t i = 0; i < n_seqs; i++) {
        uint32_t code = match_offs[i];
        uint32_t off;
        if (code == 0u) return TDC_E_CORRUPT;
        if (code == 1u) {
            off = r1;
        } else if (code == 2u) {
            off = r2;
            uint32_t t = r1; r1 = r2; r2 = t;
        } else if (code == 3u) {
            off = r3;
            uint32_t t = r3; r3 = r2; r2 = r1; r1 = t;
        } else {
            off = code - 3u;
            if (off == 0u || off > LZ_MAX_OFFSET) return TDC_E_CORRUPT;
            r3 = r2; r2 = r1; r1 = off;
        }
        match_offs[i] = off;
    }
    return TDC_OK;
}

/* ----- Byte-shuffle for u32 arrays --------------------------------------- */
/*
 * Transpose n u32 values into 4 contiguous byte lanes of n bytes each:
 *   lane0[i] = v[i]      & 0xFF
 *   lane1[i] = v[i] >> 8 & 0xFF
 *   lane2[i] = v[i] >>16 & 0xFF
 *   lane3[i] = v[i] >>24 & 0xFF
 *
 * Separating the lanes exposes per-lane structure: lane 3 (high byte) is
 * almost always zero for lit/match lengths and for most match offsets,
 * which is exactly the shape HUFFMAN and FSE compress well.
 */
static void lzs_shuffle_u32(const uint32_t *src, uint32_t n, uint8_t *dst) {
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t v = src[i];
        dst[i]                = (uint8_t)(v);
        dst[(size_t)n + i]    = (uint8_t)(v >> 8);
        dst[2u * (size_t)n + i] = (uint8_t)(v >> 16);
        dst[3u * (size_t)n + i] = (uint8_t)(v >> 24);
    }
}

static void lzs_unshuffle_u32(const uint8_t *src, uint32_t n, uint32_t *dst) {
    for (uint32_t i = 0; i < n; ++i) {
        dst[i] = (uint32_t)src[i]
               | ((uint32_t)src[(size_t)n + i]          << 8)
               | ((uint32_t)src[2u * (size_t)n + i]     << 16)
               | ((uint32_t)src[3u * (size_t)n + i]     << 24);
    }
}

/* ----- Encode one stream, try all coders, keep the smallest -------------- */
/*
 * Runs HUFFMAN and FSE against the input and compares their encoded sizes
 * against the NONE floor (n bytes). Whichever is smallest is written into
 * `final` (a per-stream destination buffer owned by the caller) and its
 * coder id is returned via *out_id. `scratch` is a tdc_buffer reused
 * across candidate coders; `final` receives a copy of the winning output.
 *
 * `final` is allocated here with dst->realloc_fn and transferred to the
 * caller, who is responsible for freeing via lzs_free(dst, *final).
 */
static tdc_status lzs_encode_stream(const uint8_t *src, size_t n,
                                     tdc_buffer    *scratch,
                                     tdc_buffer    *owner,   /* for realloc_fn */
                                     uint8_t       **final,
                                     uint32_t       *final_size,
                                     tdc_entropy_id *out_id) {
    *final = NULL;
    *final_size = 0;
    *out_id = TDC_ENTROPY_NONE;

    if (n == 0) {
        return TDC_OK;
    }

    /* Best-so-far is "NONE passthrough" at exactly n bytes. Only a coder
     * that strictly beats n wins. */
    size_t         best_size = n;
    tdc_entropy_id best_id   = TDC_ENTROPY_NONE;
    int            best_is_encoded = 0;

    /* Decode-speed bias: for streams >= LZS_HUF4_PREFER_BYTES, HUFFMAN4
     * decodes ~1.5-2x faster than single-stream HUFFMAN but produces a
     * payload ~20 bytes larger (3x u32 stream header + extra bit-flush
     * padding on 3 sub-streams with the same tree). Bias the selector
     * so HUFFMAN4 wins as long as it costs < 0.05% of the stream size. */
#define LZS_HUF4_PREFER_BYTES  (64u * 1024u)
    const size_t huf4_budget = (n >= LZS_HUF4_PREFER_BYTES)
                               ? (n / 2000u + 32u) : 0;

    /* Keep a stash of the winning encoded bytes across candidate coders.
     * Each candidate writes into scratch; on a win we snapshot into stash
     * so the next candidate's encode doesn't clobber it. */
    uint8_t *stash = NULL;
    size_t   stash_size = 0;

    /* HUFFMAN4 splits into 4 independently-decoded streams — adds ~12B of
     * header overhead vs single-stream HUFFMAN, but roughly doubles decode
     * throughput on big (Huffman-dominated) streams. HUFFMAN4 itself
     * delegates to single-stream for n < 256, so including both here is
     * only a win on the literal stream for non-tiny blocks; trial-and-
     * compare picks whichever lands smaller. */
    static const tdc_entropy_id candidates[] = {
        TDC_ENTROPY_HUFFMAN,
        TDC_ENTROPY_HUFFMAN4,
        TDC_ENTROPY_FSE,
    };
    for (size_t c = 0; c < sizeof(candidates) / sizeof(candidates[0]); c++) {
        tdc_entropy_id id = candidates[c];
        const tdc_entropy_vt *sub = lzs_sub_vt(id);
        if (!sub) continue;

        scratch->size = 0;
        tdc_status st = sub->encode(src, n, NULL, scratch);
        if (st != TDC_OK) {
            /* Coder refused this input — skip, don't abort the whole block. */
            continue;
        }
        /* HUFFMAN4 gets a size budget over the current best: its payload
         * is bit-identical to HUFFMAN but ~20B larger due to stream
         * headers. For large streams we pay those ~20B for ~2x decode. */
        size_t effective_size = scratch->size;
        if (id == TDC_ENTROPY_HUFFMAN4 && huf4_budget > 0 &&
            scratch->size <= best_size + huf4_budget) {
            effective_size = 0;  /* force win */
        }
        if (effective_size >= best_size) continue;

        /* New champion — snapshot into stash. */
        uint8_t *new_stash = (uint8_t *)owner->realloc_fn(
            owner->user, stash, scratch->size);
        if (!new_stash) {
            if (stash) (void)owner->realloc_fn(owner->user, stash, 0);
            return TDC_E_NOMEM;
        }
        stash = new_stash;
        stash_size = scratch->size;
        memcpy(stash, scratch->data, stash_size);
        best_size = stash_size;
        best_id   = id;
        best_is_encoded = 1;
    }

    if (best_is_encoded) {
        *final      = stash;
        *final_size = (uint32_t)best_size;
        *out_id     = best_id;
        return TDC_OK;
    }

    /* No coder beat NONE. Write NONE passthrough into a fresh buffer.
     * (We can't reuse stash because it's NULL in this branch.) */
    uint8_t *none_buf = (uint8_t *)owner->realloc_fn(owner->user, NULL, n);
    if (!none_buf) return TDC_E_NOMEM;
    memcpy(none_buf, src, n);
    *final      = none_buf;
    *final_size = (uint32_t)n;
    *out_id     = TDC_ENTROPY_NONE;
    return TDC_OK;
}

/* ----- Fast stream encode (Shannon heuristic, no trial) ------------------ */
/*
 * For level >= 1: pick a coder based on Shannon entropy of the input
 * stream, then encode once. No trial-and-compare, no context splitting.
 * This is ~3x faster than lzs_encode_stream because it avoids 2 full
 * encode passes + potential context sub-stream work.
 *
 * Heuristic (same thresholds as lane.c):
 *   h >= 7.5 bits/byte → near-random, store raw (NONE)
 *   h <  7.5           → Huffman (always competitive, fast)
 *
 * FSE is skipped entirely — its fractional-bit advantage over Huffman
 * is <1% on typical distributions and doesn't justify the decode cost.
 */
static tdc_status lzs_encode_stream_fast(const uint8_t *src, size_t n,
                                          tdc_buffer    *scratch,
                                          tdc_buffer    *owner,
                                          uint8_t       **final,
                                          uint32_t       *final_size,
                                          tdc_entropy_id *out_id) {
    *final = NULL;
    *final_size = 0;
    *out_id = TDC_ENTROPY_NONE;

    if (n == 0) return TDC_OK;

    /* Histogram in one pass — reused by both Shannon check and Huffman. */
    uint32_t hist[256] = {0};
    for (size_t i = 0; i < n; i++) hist[src[i]]++;

    /* Quick Shannon entropy estimate (integer approximation).
     * Uses the identity: H = log2(n) - (1/n) * sum(freq * log2(freq)).
     * We approximate with leading-zero count instead of fp log2. */
    int n_used = 0;
    for (int s = 0; s < 256; s++) {
        if (hist[s] != 0) n_used++;
    }

    /* Single-symbol or near-random fast reject. */
    if (n_used <= 1) {
        /* Single symbol: Huffman always wins (1 bit/sym + tiny header). */
    } else if (n_used >= 240 && n >= 64) {
        /* Nearly full alphabet on non-tiny input: likely near-random.
         * Do the full Shannon check only in this case. */
        double inv_n = 1.0 / (double)n;
        double entropy = 0.0;
        for (int s = 0; s < 256; s++) {
            if (hist[s] == 0) continue;
            double p = (double)hist[s] * inv_n;
            entropy -= p * log2(p);
        }
        if (entropy >= 7.5) {
            uint8_t *buf = (uint8_t *)owner->realloc_fn(owner->user, NULL, n);
            if (!buf) return TDC_E_NOMEM;
            memcpy(buf, src, n);
            *final = buf;
            *final_size = (uint32_t)n;
            *out_id = TDC_ENTROPY_NONE;
            return TDC_OK;
        }
    }

    /* Try Huffman with pre-computed frequencies (no double histogram). */
    scratch->size = 0;
    tdc_status st = tdc_huffman_encode_prefreq(src, n, hist, scratch);
    if (st == TDC_OK && scratch->size < n) {
        uint8_t *buf = (uint8_t *)owner->realloc_fn(owner->user, NULL, scratch->size);
        if (!buf) return TDC_E_NOMEM;
        memcpy(buf, scratch->data, scratch->size);
        *final = buf;
        *final_size = (uint32_t)scratch->size;
        *out_id = TDC_ENTROPY_HUFFMAN;
        return TDC_OK;
    }

    /* Huffman didn't help — store raw. */
    uint8_t *buf = (uint8_t *)owner->realloc_fn(owner->user, NULL, n);
    if (!buf) return TDC_E_NOMEM;
    memcpy(buf, src, n);
    *final = buf;
    *final_size = (uint32_t)n;
    *out_id = TDC_ENTROPY_NONE;
    return TDC_OK;
}

/* ----- Order-1 literal context encode ------------------------------------ */
/*
 * Split the literal stream into 16 sub-streams by context = prev_byte >> 4.
 * The first literal byte uses context 0 (no predecessor).  Each sub-stream
 * is encoded independently with the best-of {NONE, Huffman, FSE} selection.
 *
 * Returns the complete context section (144-byte header + encoded streams)
 * in *out_section.  Caller frees via lzs_free(owner, *out_section).
 */
static tdc_status lzs_encode_lit_context(
    const uint8_t *lit_raw, uint32_t total_lit,
    tdc_buffer *scratch, tdc_buffer *owner,
    uint8_t **out_section, uint32_t *out_section_size)
{
    *out_section = NULL;
    *out_section_size = 0;
    if (total_lit == 0) return TDC_OK;

    /* Pass 1: count bytes per context bucket. */
    uint32_t bkt_count[LZS_LIT_N_CTX];
    memset(bkt_count, 0, sizeof(bkt_count));
    {
        uint8_t ctx = 0;
        for (uint32_t i = 0; i < total_lit; i++) {
            bkt_count[ctx]++;
            ctx = lit_raw[i] >> 4;
        }
    }

    /* Pass 2: scatter literals into per-context buffers. */
    uint8_t  *bkt_data[LZS_LIT_N_CTX];
    uint32_t  bkt_pos[LZS_LIT_N_CTX];
    memset(bkt_data, 0, sizeof(bkt_data));
    memset(bkt_pos,  0, sizeof(bkt_pos));

    tdc_status st = TDC_OK;
    for (uint32_t c = 0; c < LZS_LIT_N_CTX; c++) {
        if (bkt_count[c] == 0) continue;
        bkt_data[c] = (uint8_t *)lzs_alloc(owner, bkt_count[c]);
        if (!bkt_data[c]) { st = TDC_E_NOMEM; goto ctx_enc_done; }
    }
    {
        uint8_t ctx = 0;
        for (uint32_t i = 0; i < total_lit; i++) {
            bkt_data[ctx][bkt_pos[ctx]++] = lit_raw[i];
            ctx = lit_raw[i] >> 4;
        }
    }

    /* Pass 3: encode each bucket independently. */
    uint8_t       *enc_buf[LZS_LIT_N_CTX];
    uint32_t       enc_sz[LZS_LIT_N_CTX];
    tdc_entropy_id enc_id[LZS_LIT_N_CTX];
    memset(enc_buf, 0, sizeof(enc_buf));
    memset(enc_sz,  0, sizeof(enc_sz));

    uint32_t total_enc = 0;
    for (uint32_t c = 0; c < LZS_LIT_N_CTX; c++) {
        enc_id[c] = TDC_ENTROPY_NONE;
        if (bkt_count[c] == 0) continue;
        st = lzs_encode_stream(bkt_data[c], bkt_count[c], scratch, owner,
                               &enc_buf[c], &enc_sz[c], &enc_id[c]);
        if (st != TDC_OK) goto ctx_enc_done;
        total_enc += enc_sz[c];
    }

    /* Assemble context section: [ids(16)][counts(64)][sizes(64)][streams] */
    {
        uint32_t section_size = (uint32_t)LZS_LIT_CTX_HDR_SIZE + total_enc;
        uint8_t *section = (uint8_t *)lzs_alloc(owner, section_size);
        if (!section) { st = TDC_E_NOMEM; goto ctx_enc_done; }

        for (uint32_t c = 0; c < LZS_LIT_N_CTX; c++)
            section[c] = (uint8_t)enc_id[c];
        for (uint32_t c = 0; c < LZS_LIT_N_CTX; c++)
            memcpy(section + LZS_LIT_N_CTX + c * 4u, &bkt_count[c], 4);
        for (uint32_t c = 0; c < LZS_LIT_N_CTX; c++)
            memcpy(section + LZS_LIT_N_CTX + LZS_LIT_N_CTX * 4u + c * 4u,
                   &enc_sz[c], 4);

        uint8_t *wp = section + LZS_LIT_CTX_HDR_SIZE;
        for (uint32_t c = 0; c < LZS_LIT_N_CTX; c++) {
            if (enc_sz[c] > 0) {
                memcpy(wp, enc_buf[c], enc_sz[c]);
                wp += enc_sz[c];
            }
        }

        *out_section      = section;
        *out_section_size  = section_size;
    }

ctx_enc_done:
    for (uint32_t c = 0; c < LZS_LIT_N_CTX; c++) {
        lzs_free(owner, bkt_data[c]);
        lzs_free(owner, enc_buf[c]);
    }
    return st;
}

/* ----- Order-1 literal context decode ------------------------------------ */
/*
 * Decode the context section (144-byte header + 16 encoded sub-streams)
 * and reconstruct the flat literal stream by walking the same context
 * assignment the encoder used.
 */
static tdc_status lzs_decode_lit_context(
    const uint8_t *section, uint32_t section_size,
    uint8_t *lit_raw, uint32_t total_lit)
{
    if (total_lit == 0) return TDC_OK;
    if (section_size < LZS_LIT_CTX_HDR_SIZE) return TDC_E_CORRUPT;

    tdc_entropy_id ctx_ids[LZS_LIT_N_CTX];
    uint32_t       ctx_counts[LZS_LIT_N_CTX];
    uint32_t       ctx_sizes[LZS_LIT_N_CTX];

    for (uint32_t c = 0; c < LZS_LIT_N_CTX; c++)
        ctx_ids[c] = (tdc_entropy_id)section[c];
    for (uint32_t c = 0; c < LZS_LIT_N_CTX; c++)
        memcpy(&ctx_counts[c], section + LZS_LIT_N_CTX + c * 4u, 4);
    for (uint32_t c = 0; c < LZS_LIT_N_CTX; c++)
        memcpy(&ctx_sizes[c],
               section + LZS_LIT_N_CTX + LZS_LIT_N_CTX * 4u + c * 4u, 4);

    /* Validate totals. */
    uint64_t count_sum = 0, size_sum = 0;
    for (uint32_t c = 0; c < LZS_LIT_N_CTX; c++) {
        count_sum += ctx_counts[c];
        size_sum  += ctx_sizes[c];
    }
    if (count_sum != total_lit) return TDC_E_CORRUPT;
    if (LZS_LIT_CTX_HDR_SIZE + size_sum > section_size) return TDC_E_CORRUPT;

    /* Decode each sub-stream into a temporary buffer. */
    uint8_t *buckets[LZS_LIT_N_CTX];
    memset(buckets, 0, sizeof(buckets));
    tdc_status st = TDC_OK;

    const uint8_t *sp = section + LZS_LIT_CTX_HDR_SIZE;
    for (uint32_t c = 0; c < LZS_LIT_N_CTX; c++) {
        if (ctx_counts[c] == 0) { sp += ctx_sizes[c]; continue; }
        buckets[c] = (uint8_t *)malloc(ctx_counts[c]);
        if (!buckets[c]) { st = TDC_E_NOMEM; goto ctx_dec_done; }

        const tdc_entropy_vt *sub = lzs_sub_vt(ctx_ids[c]);
        if (!sub) { st = TDC_E_CORRUPT; goto ctx_dec_done; }
        st = sub->decode(sp, ctx_sizes[c], buckets[c], ctx_counts[c]);
        if (st != TDC_OK) goto ctx_dec_done;
        sp += ctx_sizes[c];
    }

    /* Reconstruct flat literal stream by walking context assignment. */
    {
        uint32_t bkt_pos[LZS_LIT_N_CTX];
        memset(bkt_pos, 0, sizeof(bkt_pos));
        uint8_t ctx = 0;
        for (uint32_t i = 0; i < total_lit; i++) {
            if (bkt_pos[ctx] >= ctx_counts[ctx]) {
                st = TDC_E_CORRUPT; goto ctx_dec_done;
            }
            lit_raw[i] = buckets[ctx][bkt_pos[ctx]++];
            ctx = lit_raw[i] >> 4;
        }
    }

ctx_dec_done:
    for (uint32_t c = 0; c < LZS_LIT_N_CTX; c++)
        free(buckets[c]);
    return st;
}

/* ----- Encode bound ------------------------------------------------------ */

static size_t lzs_encode_bound(size_t src_size) {
    /* Worst case: literal stream = src_size, two u32 streams (ll, ml)
     * at most 4 * src_size each, symbol stream at most src_size, extra
     * bits stream at most 20 * src_size / 8. Each sub-coder adds at
     * most ~1 KiB of headers. Plus the 44-byte v2 header. */
    /* Context-coded literals add up to 144-byte context header + 16
     * sub-stream Huffman headers (~260 each) ≈ 4.3 KiB overhead. */
    return LZS_HEADER_SIZE_V2 + src_size + 2u * src_size
         + src_size + 3u * src_size + 10u * 1024u;
}

/* ----- Passthrough fallback --------------------------------------------- */
/*
 * Writes an LZ_STREAMS record that carries src byte-for-byte in the
 * literal stream with NONE coding. Used when the full encoded form
 * (parse + four entropy-coded streams) would match or exceed src_size.
 */
static tdc_status lzs_encode_passthrough(const uint8_t *src, uint32_t src_size,
                                          tdc_buffer *dst) {
    size_t need = (size_t)LZS_HEADER_SIZE_V2 + src_size;
    tdc_status st = tdc_buf_reserve(dst, need);
    if (st != TDC_OK) return st;

    uint8_t *p = dst->data;
    uint32_t zero = 0;
    p[0] = (uint8_t)LZS_FORMAT_VERSION;
    p[1] = 0;                              /* flags — no repcodes in passthrough */
    p[2] = 0;
    p[3] = 0;
    memcpy(p +  4, &zero,     4); /* n_seqs */
    memcpy(p +  8, &src_size, 4); /* total_lit_size */
    memcpy(p + 12, &src_size, 4); /* trailing_lit_len */
    memcpy(p + 16, &src_size, 4); /* uncompressed_size */
    p[20] = (uint8_t)TDC_ENTROPY_NONE; /* id_lit */
    p[21] = (uint8_t)TDC_ENTROPY_NONE; /* id_ll  */
    p[22] = (uint8_t)TDC_ENTROPY_NONE; /* id_ml  */
    p[23] = (uint8_t)TDC_ENTROPY_NONE; /* id_mo_sym */
    memcpy(p + 24, &src_size, 4);      /* sz_lit */
    memset(p + 28, 0, 16);             /* sz_ll, sz_ml, sz_mo_sym, sz_extra = 0 */

    if (src_size > 0) memcpy(p + LZS_HEADER_SIZE_V2, src, src_size);
    dst->size = need;
    return TDC_OK;
}

/* ----- Price computation from first-pass frequencies ---------------------- *
 *
 * After the first (flat-cost) parse, we have the three symbol streams and
 * the literal stream. From these we compute per-symbol costs via Shannon
 * entropy: cost(s) = ceil(-log2(freq/total)) in 1/8-bit fixed-point.
 * Extra-bit costs are added on top: symbols with wider extra-bit fields
 * carry those bits at 1 bit each (= 8 in 1/8-bit units).
 *
 * These costs feed the second (priced) parse, which can make better
 * decisions about match vs. literal trade-offs because it knows the
 * actual entropy of each symbol rather than using flat estimates.
 */

/* Fixed-point scale: costs are in 1/8-bit units (multiply real bits × 8). */
#define LZS_PRICE_SCALE  8

/* Shannon cost for a symbol with frequency `freq` in a stream of `total`
 * symbols. Returns cost in 1/8-bit units. Clamps to [8, 120] (1-15 bits). */
static inline int32_t lzs_shannon_cost(uint32_t freq, uint32_t total) {
    if (freq == 0 || total == 0) return 15 * LZS_PRICE_SCALE; /* penalty */
    double p = (double)freq / (double)total;
    double bits = -log2(p);
    int32_t cost = (int32_t)(bits * LZS_PRICE_SCALE + 0.5);
    if (cost < LZS_PRICE_SCALE) cost = LZS_PRICE_SCALE;       /* min 1 bit */
    if (cost > 15 * LZS_PRICE_SCALE) cost = 15 * LZS_PRICE_SCALE;
    return cost;
}

/* Build per-symbol price tables from first-pass symbol frequencies. */
static void lzs_compute_prices(const uint8_t *lit_raw, uint32_t total_lit,
                                const uint8_t *ll_sym, const uint8_t *ml_sym,
                                const uint8_t *off_sym, uint32_t n_seqs,
                                LzsStreamsPrices *prices) {
    /* --- Literal byte frequencies --- */
    uint32_t lit_freq[256];
    memset(lit_freq, 0, sizeof(lit_freq));
    for (uint32_t i = 0; i < total_lit; i++)
        lit_freq[lit_raw[i]]++;
    for (int i = 0; i < 256; i++)
        prices->lit[i] = lzs_shannon_cost(lit_freq[i], total_lit);

    /* --- Lit-len symbol frequencies --- */
    uint32_t ll_freq[LZS_PRICE_MAX_SYM];
    memset(ll_freq, 0, sizeof(ll_freq));
    for (uint32_t i = 0; i < n_seqs; i++) {
        uint8_t s = ll_sym[i];
        if (s < LZS_PRICE_MAX_SYM) ll_freq[s]++;
    }

    /* ll_avg: average ll symbol cost + average extra bits.
     * The ll cost enters once per sequence in the DP; we use a flat
     * average rather than per-symbol because the DP decides lit_len
     * implicitly via the prefix-min trick — it controls where matches
     * start, and the literal run between matches falls out. */
    {
        int64_t total_ll_cost = 0;
        for (uint32_t s = 0; s < LZS_PRICE_MAX_SYM; s++) {
            if (ll_freq[s] == 0) continue;
            int32_t sym_cost = lzs_shannon_cost(ll_freq[s], n_seqs);
            uint32_t extra = (s >= 2u) ? (s - 1u) : 0u;
            total_ll_cost += (int64_t)(sym_cost + (int32_t)(extra * LZS_PRICE_SCALE))
                           * ll_freq[s];
        }
        prices->ll_avg = (n_seqs > 0)
            ? (int32_t)(total_ll_cost / (int64_t)n_seqs)
            : 10 * LZS_PRICE_SCALE;
    }

    /* --- Match-len symbol frequencies + extra bits --- */
    uint32_t ml_freq[LZS_PRICE_MAX_SYM];
    memset(ml_freq, 0, sizeof(ml_freq));
    for (uint32_t i = 0; i < n_seqs; i++) {
        uint8_t s = ml_sym[i];
        if (s < LZS_PRICE_MAX_SYM) ml_freq[s]++;
    }
    for (uint32_t s = 0; s < LZS_PRICE_MAX_SYM; s++) {
        int32_t sym_cost = lzs_shannon_cost(ml_freq[s], n_seqs);
        uint32_t extra = (s >= 2u) ? (s - 1u) : 0u;
        prices->ml[s] = sym_cost + (int32_t)(extra * LZS_PRICE_SCALE);
    }

    /* --- Offset symbol frequencies + extra bits --- */
    uint32_t off_freq[LZS_PRICE_MAX_SYM];
    memset(off_freq, 0, sizeof(off_freq));
    for (uint32_t i = 0; i < n_seqs; i++) {
        uint8_t s = off_sym[i];
        if (s < LZS_PRICE_MAX_SYM) off_freq[s]++;
    }
    for (uint32_t s = 0; s < LZS_PRICE_MAX_SYM; s++) {
        int32_t sym_cost = lzs_shannon_cost(off_freq[s], n_seqs);
        uint32_t extra = (s >= 3u) ? (s - 3u) : 0u;
        prices->off[s] = sym_cost + (int32_t)(extra * LZS_PRICE_SCALE);
    }
}

/* ----- Encode ------------------------------------------------------------- */

/* LZ_STREAMS level mapping.
 *
 * The level controls the parse strategy. The serialization (repcodes,
 * log2-prefix symbols, per-stream entropy coding) is always the same —
 * only the match-finding phase changes.
 *
 * level <= 0 (default): 3-pass optimal parsing. Best ratio, slowest.
 * level 1:  flat hash (chain_depth=0), no lazy. Maximum encode speed.
 * level 2:  greedy HC4 + lazy. Good balance.
 * level 3:  greedy HC8 + lazy.
 * level 4+: greedy HC16 + double lazy.
 */
static tdc_status lzs_encode(const uint8_t *src, size_t src_size,
                              const void    *params,
                              tdc_buffer    *dst) {
    if (!dst || !dst->realloc_fn) return TDC_E_INVAL;
    if (src_size > 0 && !src)     return TDC_E_INVAL;
    if (src_size > UINT32_MAX)    return TDC_E_INVAL;

    /* Level semantics:
     *   level == 0   : default → L2 (HC4 + lazy1). Best measured Pareto.
     *   level <  0   : optimal DP parser. Highest ratio, very slow encode
     *                  (~0.3 MB/s on 16 MiB f64 smooth). Explicit opt-in.
     *   level >= 1   : greedy hash-chain, level N. */
    int level = 0;
    uint32_t min_match = LZ_MIN_MATCH;
    if (params) {
        /* LZ_STREAMS's params slot is tdc_lz_streams_params. `level` is the
         * leading field so an old tdc_entropy_level initializer still sets
         * level correctly — the extra min_match field on zeroed memory
         * yields min_match=0 → baseline (3). Callers that want the new knob
         * must declare tdc_lz_streams_params explicitly. */
        const tdc_lz_streams_params *sp = (const tdc_lz_streams_params *)params;
        level = sp->level;
        if (sp->min_match > LZ_MIN_MATCH) min_match = sp->min_match;
    }
    if (level == 0) level = 2;  /* remap default → L2 */

    /* Empty input: emit an empty passthrough record. */
    if (src_size == 0) {
        return lzs_encode_passthrough(src, 0u, dst);
    }

    LZSeq *seqs = NULL;
    uint32_t seq_count = 0;
    tdc_status st;

    const int timing = lzs_timing_enabled();
    double t_phase_start = timing ? tdc_now_secs() : 0.0;
    double t_initial = 0.0, t_priced = 0.0, t_build = 0.0,
           t_entropy = 0.0, t_lit_ctx = 0.0;

    if (level >= 1) {
        /* Fast path: greedy parse with hash chains + lazy matching.
         * Skips the multi-pass optimal refinement loop entirely. */
        uint32_t chain_depth, lazy_depth;
        switch (level) {
        case 1:  chain_depth = 0;  lazy_depth = 0; break;
        case 2:  chain_depth = 4;  lazy_depth = 1; break;
        case 3:  chain_depth = 8;  lazy_depth = 1; break;
        default: chain_depth = 16; lazy_depth = 2; break;
        }
        st = tdc_lz_parse_greedy(src, (uint32_t)src_size, dst,
                                  chain_depth, lazy_depth,
                                  &seqs, &seq_count);
        if (st != TDC_OK) return st;
    } else {
        /* level < 0: explicit opt-in to multi-pass optimal parsing. Best
         * ratio, slow encode.
         * Pass 1: approximate cost model (offset-aware flat costs)
         * Pass 2..N: extract per-symbol prices from previous parse, re-parse
         *
         * N_PASSES=2 (1 initial + 1 priced reparse). Profiling with
         * TDC_LZS_TIMING on vec1d f64 2M smooth at L0 showed the
         * three-pass regime spent 99% of encode time inside the parsers
         * (initial 33%, each priced pass 33%). Dropping from 3 to 2
         * saves the second priced pass — ~33% encode speedup on L0 for
         * a negligible ratio delta on typical data. */
#define LZS_N_PASSES 2

        st = tdc_lz_parse_optimal_streams(src, (uint32_t)src_size, dst,
                                           &seqs, &seq_count);
        if (st != TDC_OK) return st;
    }

    /* Post-parse min_match filter (fold sub-threshold matches into
     * literals). Runs on every parser output — initial greedy, initial
     * optimal, and the priced re-parses below. */
    lzs_filter_short_matches(seqs, &seq_count, min_match);

    if (timing) {
        double n = tdc_now_secs();
        t_initial = n - t_phase_start;
        t_phase_start = n;
    }

    if (seq_count == 0) {
        return lzs_encode_passthrough(src, (uint32_t)src_size, dst);
    }

    /* Detect offset stride from the first pass. The parser emits raw byte
     * offsets; for typed data these are always multiples of the element
     * width (e.g. 8 for f64). Shifting offsets right by this factor
     * before repcode/symbol encoding removes redundant low bits. */
    uint32_t off_shift = lzs_detect_offset_shift(seqs, seq_count);

    /* Multi-pass refinement: only for the optimal parser (level <= 0).
     * Greedy parse (level >= 1) skips refinement entirely — the parse
     * is not cost-model-driven so repricing has no effect. */
    for (int pass = (level >= 1) ? LZS_N_PASSES : 1; pass < LZS_N_PASSES; pass++) {
        /* Shift offsets (parser produces unshifted; we shift for encoding). */
        if (off_shift > 0) {
            for (uint32_t i = 0; i < seq_count; i++)
                seqs[i].match_off >>= off_shift;
        }

        /* Apply repcodes, build symbol streams, compute prices. */
        lzs_repcode_encode(seqs, seq_count);

        uint64_t tl64 = 0, con64 = 0;
        for (uint32_t i = 0; i < seq_count; i++) {
            tl64  += seqs[i].lit_len;
            con64 += (uint64_t)seqs[i].lit_len + seqs[i].match_len;
        }
        uint32_t trail_p = (uint32_t)((uint64_t)src_size - con64);
        uint32_t tlit_p  = (uint32_t)(tl64 + trail_p);

        uint8_t *tlit_raw = NULL;
        uint8_t *tll  = NULL;
        uint8_t *tml  = NULL;
        uint8_t *toff = NULL;

        if (tlit_p > 0) {
            tlit_raw = (uint8_t *)lzs_alloc(dst, tlit_p);
            if (!tlit_raw) { lzs_free(dst, seqs); return TDC_E_NOMEM; }
            uint32_t sp = 0, lp = 0;
            for (uint32_t i = 0; i < seq_count; i++) {
                uint32_t ll = seqs[i].lit_len;
                if (ll > 0) { memcpy(tlit_raw + lp, src + sp, ll); lp += ll; }
                sp += ll + seqs[i].match_len;
            }
            if (trail_p > 0) memcpy(tlit_raw + lp, src + sp, trail_p);
        }

        tll  = (uint8_t *)lzs_alloc(dst, seq_count);
        tml  = (uint8_t *)lzs_alloc(dst, seq_count);
        toff = (uint8_t *)lzs_alloc(dst, seq_count);
        if (!tll || !tml || !toff) {
            lzs_free(dst, tlit_raw); lzs_free(dst, tll);
            lzs_free(dst, tml); lzs_free(dst, toff);
            lzs_free(dst, seqs);
            return TDC_E_NOMEM;
        }

        for (uint32_t i = 0; i < seq_count; i++) {
            uint8_t s; uint32_t ex, nb;
            lzs_uint_to_symbol(seqs[i].lit_len, &s, &ex, &nb);
            tll[i] = s;
            lzs_uint_to_symbol(seqs[i].match_len - LZ_MIN_MATCH, &s, &ex, &nb);
            tml[i] = s;
            lzs_offset_to_symbol(seqs[i].match_off, &s, &ex, &nb);
            toff[i] = s;
        }

        LzsStreamsPrices prices;
        lzs_compute_prices(tlit_raw, tlit_p, tll, tml, toff, seq_count, &prices);
        prices.offset_shift = off_shift;

        if (lzs_dump_enabled()) {
            uint32_t p_extra = 0;
            for (uint32_t i = 0; i < seq_count; i++) {
                uint8_t s; uint32_t ex, nb;
                lzs_uint_to_symbol(seqs[i].lit_len, &s, &ex, &nb); p_extra += nb;
                lzs_uint_to_symbol(seqs[i].match_len-LZ_MIN_MATCH, &s, &ex, &nb); p_extra += nb;
                lzs_offset_to_symbol(seqs[i].match_off, &s, &ex, &nb); p_extra += nb;
            }
            TDC_LOG("[lzs-p%d] src=%zu n_seqs=%u  lit=%u  extra=%u bytes\n",
                    pass, src_size, seq_count, tlit_p, (p_extra + 7u) / 8u);
        }

        lzs_free(dst, tlit_raw);
        lzs_free(dst, tll);
        lzs_free(dst, tml);
        lzs_free(dst, toff);
        lzs_free(dst, seqs);
        seqs = NULL;
        seq_count = 0;

        st = tdc_lz_parse_optimal_streams_priced(
            src, (uint32_t)src_size, dst, &prices, &seqs, &seq_count);
        if (st != TDC_OK) return st;

        lzs_filter_short_matches(seqs, &seq_count, min_match);

        if (seq_count == 0) {
            return lzs_encode_passthrough(src, (uint32_t)src_size, dst);
        }
    }

    if (timing) {
        double n = tdc_now_secs();
        t_priced = n - t_phase_start;
        t_phase_start = n;
    }

    /* Fused pass: detect offset stride + compute stream sizes.
     * Both need a full scan of seqs, so we do them together. */
    uint32_t offset_shift;
    uint32_t trailing;
    uint32_t total_lit;
    {
        uint32_t all_or = 0;
        uint64_t total_lit64 = 0;
        uint64_t consumed64 = 0;
        for (uint32_t i = 0; i < seq_count; i++) {
            all_or      |= seqs[i].match_off;
            total_lit64 += seqs[i].lit_len;
            consumed64  += (uint64_t)seqs[i].lit_len + seqs[i].match_len;
        }
        trailing = (uint32_t)((uint64_t)src_size - consumed64);
        total_lit = (uint32_t)(total_lit64 + trailing);

        /* Offset stride: largest power-of-2 dividing all offsets (0-7). */
        offset_shift = 0;
        if (all_or != 0) {
            while (offset_shift < 7u && (all_or & (1u << offset_shift)) == 0)
                offset_shift++;
        }
    }

    /* Apply offset shift. */
    if (offset_shift > 0) {
        for (uint32_t i = 0; i < seq_count; i++)
            seqs[i].match_off >>= offset_shift;
    }

    if (lzs_dump_enabled() && offset_shift > 0) {
        TDC_LOG("  [lzs-shift] offset_shift=%u (stride=%u)\n",
                offset_shift, 1u << offset_shift);
    }

    /* Repcode transform on the shifted offsets. */
    lzs_repcode_encode(seqs, seq_count);

    tdc_buffer scratch = {0};
    scratch.realloc_fn = dst->realloc_fn;
    scratch.user       = dst->user;

    uint8_t  *lit_raw    = NULL;
    uint8_t  *ll_sym    = NULL;   /* lit_len symbol stream (1 byte/seq) */
    uint8_t  *ml_sym    = NULL;   /* match_len symbol stream (1 byte/seq) */
    uint8_t  *off_sym   = NULL;   /* offset symbol stream (1 byte/seq) */
    uint8_t  *extra_raw = NULL;   /* combined packed extra bits (interleaved) */
    uint32_t  extra_bytes = 0;

    int       use_delta_offsets = 0;

    uint8_t *enc_lit = NULL, *enc_ll = NULL, *enc_ml = NULL, *enc_off = NULL;
    uint8_t *enc_lit_ctx = NULL;
    uint32_t sz_lit = 0, sz_ll = 0, sz_ml = 0, sz_off = 0;
    uint32_t sz_lit_ctx = 0;
    int      use_lit_ctx = 0;
    tdc_entropy_id id_lit = TDC_ENTROPY_NONE;
    tdc_entropy_id id_ll  = TDC_ENTROPY_NONE;
    tdc_entropy_id id_ml  = TDC_ENTROPY_NONE;
    tdc_entropy_id id_off = TDC_ENTROPY_NONE;

    /* Fused: build literal stream + symbol streams + count extra bits.
     * One pass over seqs instead of two. */
    {
        ll_sym  = (uint8_t *)lzs_alloc(dst, seq_count);
        ml_sym  = (uint8_t *)lzs_alloc(dst, seq_count);
        off_sym = (uint8_t *)lzs_alloc(dst, seq_count);
        if (!ll_sym || !ml_sym || !off_sym) {
            st = TDC_E_NOMEM; goto cleanup;
        }
        if (total_lit > 0) {
            lit_raw = (uint8_t *)lzs_alloc(dst, total_lit);
            if (!lit_raw) { st = TDC_E_NOMEM; goto cleanup; }
        }

        uint32_t total_extra_bits = 0;
        uint32_t ll_extra_bits = 0, ml_extra_bits = 0, off_extra_bits = 0;
        uint32_t n_repcodes = 0;
        uint32_t src_pos = 0;
        uint32_t lit_pos = 0;
        for (uint32_t i = 0; i < seq_count; i++) {
            uint8_t s; uint32_t ex, nb;

            /* Copy literals for this sequence. */
            uint32_t ll = seqs[i].lit_len;
            if (ll > 0) {
                memcpy(lit_raw + lit_pos, src + src_pos, ll);
                lit_pos += ll;
            }
            src_pos += ll + seqs[i].match_len;

            /* Build symbols. */
            lzs_uint_to_symbol(seqs[i].lit_len, &s, &ex, &nb);
            ll_sym[i] = s;
            total_extra_bits += nb;
            ll_extra_bits += nb;

            lzs_uint_to_symbol(seqs[i].match_len - LZ_MIN_MATCH, &s, &ex, &nb);
            ml_sym[i] = s;
            total_extra_bits += nb;
            ml_extra_bits += nb;

            lzs_offset_to_symbol(seqs[i].match_off, &s, &ex, &nb);
            off_sym[i] = s;
            total_extra_bits += nb;
            off_extra_bits += nb;
            if (seqs[i].match_off <= 3u) n_repcodes++;
        }
        if (trailing > 0) memcpy(lit_raw + lit_pos, src + src_pos, trailing);

        /* Trial delta offset encoding: novel offsets as zigzag(off - rep1).
         * Saves bits when consecutive offsets are similar (e.g., seasonal
         * strides alternating 365/366 for leap years).
         * Skipped for level >= 1 (fast path) — the trial cost is significant
         * and the savings are typically < 5% of off_extra bits. */
        if (level <= 0 && off_extra_bits >= 16u && seq_count > 1u) {
            uint32_t delta_off_bits = 0;
            uint32_t dr1 = LZS_REP_INIT_1, dr2 = LZS_REP_INIT_2,
                     dr3 = LZS_REP_INIT_3;
            for (uint32_t i = 0; i < seq_count; i++) {
                uint32_t code = seqs[i].match_off;
                if (code <= 3u) {
                    lzs_rep_update(code, &dr1, &dr2, &dr3);
                } else {
                    uint32_t actual_off = code - 3u;
                    uint32_t zz = lzs_zigzag_encode(
                        (int32_t)actual_off - (int32_t)dr1);
                    uint8_t zs; uint32_t zex, znb;
                    lzs_uint_to_symbol(zz, &zs, &zex, &znb);
                    delta_off_bits += znb;
                    dr3 = dr2; dr2 = dr1; dr1 = actual_off;
                }
            }
            if (delta_off_bits < off_extra_bits) {
                use_delta_offsets = 1;
                /* Rebuild off_sym with delta symbols. */
                dr1 = LZS_REP_INIT_1; dr2 = LZS_REP_INIT_2;
                dr3 = LZS_REP_INIT_3;
                for (uint32_t i = 0; i < seq_count; i++) {
                    uint32_t code = seqs[i].match_off;
                    if (code <= 3u) {
                        lzs_rep_update(code, &dr1, &dr2, &dr3);
                    } else {
                        uint32_t actual_off = code - 3u;
                        uint32_t zz = lzs_zigzag_encode(
                            (int32_t)actual_off - (int32_t)dr1);
                        uint8_t zs; uint32_t zex, znb;
                        lzs_uint_to_symbol(zz, &zs, &zex, &znb);
                        off_sym[i] = (uint8_t)(zs + 3u);
                        dr3 = dr2; dr2 = dr1; dr1 = actual_off;
                    }
                }
                if (lzs_dump_enabled()) {
                    TDC_LOG("  [lzs-delta] off_extra: %u -> %u bits (saved %u)\n",
                            off_extra_bits, delta_off_bits,
                            off_extra_bits - delta_off_bits);
                }
                total_extra_bits = total_extra_bits - off_extra_bits
                                 + delta_off_bits;
                off_extra_bits = delta_off_bits;
            }
        }

        if (lzs_dump_enabled()) {
            uint32_t off_hist[LZS_MAX_OFFSET_DELTA_SYMBOL + 1] = {0};
            for (uint32_t i = 0; i < seq_count; i++) {
                if (off_sym[i] <= LZS_MAX_OFFSET_DELTA_SYMBOL) off_hist[off_sym[i]]++;
            }
            TDC_LOG("  [lzs-extra] ll=%u ml=%u off=%u total=%u bits "
                    "(%u bytes) repcodes=%u/%u (%.0f%%)%s\n",
                    ll_extra_bits, ml_extra_bits, off_extra_bits,
                    total_extra_bits, (total_extra_bits + 7u) / 8u,
                    n_repcodes, seq_count,
                    seq_count > 0 ? 100.0 * n_repcodes / seq_count : 0.0,
                    use_delta_offsets ? " [DELTA]" : "");
            TDC_LOG("  [lzs-off-hist]");
            for (uint32_t j = 0; j <= LZS_MAX_OFFSET_DELTA_SYMBOL; j++) {
                if (off_hist[j] > 0) TDC_LOG(" s%d=%u", j, off_hist[j]);
            }
            TDC_LOG("\n");
        }

        /* Pack extra bits into combined (interleaved) buffer. */
        extra_bytes = (total_extra_bits + 7u) / 8u;
        if (extra_bytes > 0) {
            /* +4 bytes slack for the 4-byte window in lzs_bits_write. */
            extra_raw = (uint8_t *)lzs_alloc(dst, extra_bytes + 4u);
            if (!extra_raw) { st = TDC_E_NOMEM; goto cleanup; }
            memset(extra_raw, 0, extra_bytes + 4u);

            uint32_t bit_pos = 0;
            uint32_t pr1 = LZS_REP_INIT_1, pr2 = LZS_REP_INIT_2,
                     pr3 = LZS_REP_INIT_3;
            for (uint32_t i = 0; i < seq_count; i++) {
                uint8_t s; uint32_t ex, nb;

                lzs_uint_to_symbol(seqs[i].lit_len, &s, &ex, &nb);
                if (nb > 0) lzs_bits_write(extra_raw, &bit_pos, ex, nb);

                lzs_uint_to_symbol(seqs[i].match_len - LZ_MIN_MATCH, &s, &ex, &nb);
                if (nb > 0) lzs_bits_write(extra_raw, &bit_pos, ex, nb);

                if (use_delta_offsets) {
                    uint32_t code = seqs[i].match_off;
                    if (code >= 4u) {
                        uint32_t actual_off = code - 3u;
                        uint32_t zz = lzs_zigzag_encode(
                            (int32_t)actual_off - (int32_t)pr1);
                        lzs_uint_to_symbol(zz, &s, &ex, &nb);
                        if (nb > 0) lzs_bits_write(extra_raw, &bit_pos, ex, nb);
                    }
                    lzs_rep_update(code, &pr1, &pr2, &pr3);
                } else {
                    lzs_offset_to_symbol(seqs[i].match_off, &s, &ex, &nb);
                    if (nb > 0) lzs_bits_write(extra_raw, &bit_pos, ex, nb);
                }
            }
        }

        /* P2: split-extras removed — all levels emit merged interleaved extras.
         * Trades ~3% compression ratio for ~40% decode speedup by routing L0
         * through the single-bit-reader fused decoder. */
    }

    if (timing) {
        double n = tdc_now_secs();
        t_build = n - t_phase_start;
        t_phase_start = n;
    }

    /* Encode each stream.  For level >= 1 (fast), use Shannon heuristic
     * to pick one coder per stream (no trial-and-compare, no context
     * coding).  For level <= 0 (default), try all coders + context. */
    if (level >= 1) {
        st = lzs_encode_stream_fast(lit_raw, total_lit, &scratch, dst,
                                     &enc_lit, &sz_lit, &id_lit);
        if (st != TDC_OK) goto cleanup;
        /* If literal stream selected NONE, enc_lit is a redundant copy of
         * lit_raw. Drop it and write directly from lit_raw in output assembly. */
        if (id_lit == TDC_ENTROPY_NONE && enc_lit) {
            lzs_free(dst, enc_lit);
            enc_lit = NULL;
        }
        st = lzs_encode_stream_fast(ll_sym, seq_count, &scratch, dst,
                                     &enc_ll, &sz_ll, &id_ll);
        if (st != TDC_OK) goto cleanup;
        st = lzs_encode_stream_fast(ml_sym, seq_count, &scratch, dst,
                                     &enc_ml, &sz_ml, &id_ml);
        if (st != TDC_OK) goto cleanup;
        st = lzs_encode_stream_fast(off_sym, seq_count, &scratch, dst,
                                     &enc_off, &sz_off, &id_off);
        if (st != TDC_OK) goto cleanup;
    } else {
        st = lzs_encode_stream(lit_raw, total_lit, &scratch, dst,
                                &enc_lit, &sz_lit, &id_lit);
        if (st != TDC_OK) goto cleanup;

        if (total_lit >= 512u) {
            double t_ctx0 = timing ? tdc_now_secs() : 0.0;
            st = lzs_encode_lit_context(lit_raw, total_lit, &scratch, dst,
                                         &enc_lit_ctx, &sz_lit_ctx);
            if (timing) t_lit_ctx += tdc_now_secs() - t_ctx0;
            if (st != TDC_OK) goto cleanup;

            if (enc_lit_ctx && sz_lit_ctx < sz_lit) {
                use_lit_ctx = 1;
                lzs_free(dst, enc_lit);
                enc_lit = enc_lit_ctx;
                sz_lit  = sz_lit_ctx;
                enc_lit_ctx = NULL;
                id_lit = TDC_ENTROPY_NONE;
            } else {
                lzs_free(dst, enc_lit_ctx);
                enc_lit_ctx = NULL;
            }
        }

        st = lzs_encode_stream(ll_sym, seq_count, &scratch, dst,
                                &enc_ll, &sz_ll, &id_ll);
        if (st != TDC_OK) goto cleanup;
        st = lzs_encode_stream(ml_sym, seq_count, &scratch, dst,
                                &enc_ml, &sz_ml, &id_ml);
        if (st != TDC_OK) goto cleanup;
        st = lzs_encode_stream(off_sym, seq_count, &scratch, dst,
                                &enc_off, &sz_off, &id_off);
        if (st != TDC_OK) goto cleanup;
    }

    if (timing) {
        double n = tdc_now_secs();
        t_entropy = n - t_phase_start;
        t_phase_start = n;
    }

    if (timing) {
        double t_other = t_entropy - t_lit_ctx;
        double t_total = t_initial + t_priced + t_build + t_entropy;
        TDC_LOG("[lzs-time] src=%zu L%d total=%.3fms  "
                "parse0=%.3f priced=%.3f build=%.3f "
                "entropy=%.3f (ctx=%.3f nctx=%.3f) ms\n",
                src_size, level, t_total * 1e3,
                t_initial * 1e3, t_priced * 1e3, t_build * 1e3,
                t_entropy * 1e3, t_lit_ctx * 1e3, t_other * 1e3);
    }

    if (lzs_dump_enabled()) {
        static const char *id_name[] = {
            [TDC_ENTROPY_NONE]    = "NONE",
            [TDC_ENTROPY_HUFFMAN] = "HUF",
            [TDC_ENTROPY_HUFFMAN4]= "HUF4",
            [TDC_ENTROPY_FSE]     = "FSE",
        };
        uint32_t total_enc = (uint32_t)LZS_HEADER_SIZE_V2
                           + sz_lit + sz_ll + sz_ml + sz_off + extra_bytes;
        const char *lit_mode = use_lit_ctx ? "CTX" : id_name[id_lit];
        TDC_LOG("[lzs-p2] src=%zu n_seqs=%u  "
                "lit=%u(%u/%s) ll=%u/%s ml=%u/%s off=%u/%s extra=%u  "
                "total=%u (%.2fx)\n",
                src_size, seq_count,
                total_lit, sz_lit, lit_mode,
                sz_ll, id_name[id_ll],
                sz_ml, id_name[id_ml],
                sz_off, id_name[id_off],
                extra_bytes, total_enc,
                total_enc > 0 ? (double)src_size / total_enc : 0.0);
    }

    /* Whole-block fallback: if we didn't beat passthrough, write one. */
    size_t final_size = (size_t)LZS_HEADER_SIZE_V2
                      + sz_lit + sz_ll + sz_ml + sz_off + extra_bytes;
    if (final_size >= src_size) {
        st = lzs_encode_passthrough(src, (uint32_t)src_size, dst);
        goto cleanup;
    }

    /* Write final output (v2 header, 44 bytes). */
    st = tdc_buf_reserve(dst, final_size);
    if (st != TDC_OK) goto cleanup;

    {
        uint8_t *p = dst->data;
        uint32_t us = (uint32_t)src_size;
        p[0] = (uint8_t)LZS_FORMAT_VERSION;
        p[1] = (uint8_t)(LZS_FLAG_REPCODES
                         | (offset_shift > 0 ? LZS_FLAG_OFFSET_SHIFT : 0)
                         | (use_lit_ctx ? LZS_FLAG_LIT_CONTEXT : 0)
                         | (use_delta_offsets ? LZS_FLAG_DELTA_OFFSETS : 0));
        p[2] = (uint8_t)offset_shift;
        p[3] = 0;
        memcpy(p +  4, &seq_count,   4);
        memcpy(p +  8, &total_lit,   4);
        memcpy(p + 12, &trailing,    4);
        memcpy(p + 16, &us,          4);
        p[20] = (uint8_t)id_lit;
        p[21] = (uint8_t)id_ll;
        p[22] = (uint8_t)id_ml;
        p[23] = (uint8_t)id_off;
        memcpy(p + 24, &sz_lit,       4);
        memcpy(p + 28, &sz_ll,        4);
        memcpy(p + 32, &sz_ml,        4);
        memcpy(p + 36, &sz_off,       4);
        memcpy(p + 40, &extra_bytes,  4);

        uint8_t *body = p + LZS_HEADER_SIZE_V2;
        if (sz_lit) {
            /* enc_lit is NULL when NONE was selected and we dropped the
             * redundant copy (L1 fast path). Write directly from lit_raw. */
            const uint8_t *lit_src = enc_lit ? enc_lit : lit_raw;
            memcpy(body, lit_src, sz_lit);
            body += sz_lit;
        }
        if (sz_ll)       { memcpy(body, enc_ll,    sz_ll);       body += sz_ll;       }
        if (sz_ml)       { memcpy(body, enc_ml,    sz_ml);       body += sz_ml;       }
        if (sz_off)      { memcpy(body, enc_off,   sz_off);      body += sz_off;      }
        if (extra_bytes) memcpy(body, extra_raw, extra_bytes);
    }
    dst->size = final_size;

cleanup:
    if (scratch.data) scratch.realloc_fn(scratch.user, scratch.data, 0);
    lzs_free(dst, enc_lit);
    lzs_free(dst, enc_lit_ctx);
    lzs_free(dst, enc_ll);
    lzs_free(dst, enc_ml);
    lzs_free(dst, enc_off);
    lzs_free(dst, lit_raw);
    lzs_free(dst, ll_sym);
    lzs_free(dst, ml_sym);
    lzs_free(dst, off_sym);
    lzs_free(dst, extra_raw);
    lzs_free(dst, seqs);
    return st;
}

/* ----- Decode ------------------------------------------------------------- */
/*
 * Scratch strategy: a single libc malloc for all decode-side buffers.
 * Using libc malloc keeps the entropy vtable's decode signature unchanged.
 *
 * Supports format v1 (byte-shuffled u32 offsets, header 40) and v2
 * (log2-prefix symbols + packed extra bits, header 44).
 */

/* Wildcopy and match copy now live in src/core/simd.h as tdc_wildcopy16
 * and tdc_match_copy.  SSE2/NEON where available, scalar fallback. */

/* 64-bit refillable bit buffer for the fused decode hot loop.
 * Amortizes byte loads across multiple bit reads. After refill the
 * buffer holds 56-64 bits; each read consumes a few bits and shifts
 * left, so we refill roughly every 8-12 reads instead of loading
 * per call. */
typedef struct {
    uint64_t bits;      /* left-aligned: MSB first */
    int      nbits;     /* valid bits in `bits` */
    const uint8_t *ptr; /* next unread byte */
    const uint8_t *end; /* one past last byte */
    const uint8_t *safe; /* end - 7: safe for 8-byte loads */
} lzs_br;

static inline void lzs_br_init(lzs_br *br, const uint8_t *buf, uint32_t size) {
    br->bits  = 0;
    br->nbits = 0;
    br->ptr   = buf;
    br->end   = buf + size;
    br->safe  = (size >= 8) ? buf + size - 7 : buf;
}

static inline void lzs_br_refill(lzs_br *br) {
    /* No-op when buffer is already nearly full. Also avoids UB from
     * `next << 64` when callers pre-refill unconditionally. */
    if (br->nbits >= 56) return;
    if (br->ptr < br->safe) {
        /* Fast: 8-byte unaligned LE load. The extras buffer is
         * LSB-first, so `next` in LE native order places byte[0]
         * at bits 0-7 etc. Shift up past existing valid bits and
         * OR in.  Bits from bytes we don't advance past are harmless:
         * they're correct values that will be OR'd again idempotently
         * on the next refill (same technique as zstd/lz4). */
        uint64_t next;
        memcpy(&next, br->ptr, 8);
        br->bits  |= next << br->nbits;
        unsigned fresh = (unsigned)(64 - br->nbits) >> 3;
        br->ptr   += fresh;
        br->nbits += (int)(fresh << 3);
    } else {
        while (br->nbits <= 56 && br->ptr < br->end) {
            br->bits |= (uint64_t)(*br->ptr++) << br->nbits;
            br->nbits += 8;
        }
    }
}

/* Read `nb` bits from the buffer (LSB-first, matching the extras layout).
 * Caller must ensure nbits <= 25 (max extra bits for any symbol).
 * Branchless on nb==0: mask `(1<<0)-1 == 0`, shifts by 0 are no-ops. */
static inline uint32_t lzs_br_read(lzs_br *br, uint32_t nb) {
    if (br->nbits < (int)nb) lzs_br_refill(br);
    uint32_t val = (uint32_t)br->bits & ((1u << nb) - 1u);
    br->bits >>= nb;
    br->nbits -= (int)nb;
    return val;
}

/* Fast-path read: assumes caller has pre-refilled the buffer with enough
 * bits for all reads in the sequence. Skips the refill check entirely.
 * Used in fused hot loops that refill once per sequence. */
static inline uint32_t lzs_br_read_fast(lzs_br *br, uint32_t nb) {
    uint32_t val = (uint32_t)br->bits & ((1u << nb) - 1u);
    br->bits >>= nb;
    br->nbits -= (int)nb;
    return val;
}

/* lzs_match_copy removed — callers now use tdc_match_copy (simd.h). */

/* Fused single-pass decode for v2 blocks (non-split-extras).
 *
 * Merges symbol reconstruction, repcode decode, offset shift, and sequence
 * execution into one loop. Eliminates the 3 × n_seqs uint32 intermediate
 * arrays and reduces total memory traffic by ~40% vs the multi-pass path.
 * The symbol streams (ll_dec, ml_dec, off_dec) and literals (lit_raw) are
 * already decoded by the Huffman/FSE sub-coders; this function walks them
 * once and produces the final output. */
static tdc_status lzs_decode_fused(
    const uint8_t *ll_dec, const uint8_t *ml_dec, const uint8_t *off_dec,
    const uint8_t *extra_ptr, uint32_t sz_extra,
    const uint8_t *lit_raw, uint32_t total_lit, uint32_t trailing,
    uint32_t n_seqs, int delta_offsets, int has_repcodes,
    uint32_t offset_shift,
    uint8_t *dst, size_t dst_size)
{
    /* 64-bit refillable bit buffer — replaces per-call lzs_bits_read_safe
     * to amortize loads across 8-12 reads per refill. */
    lzs_br br;
    lzs_br_init(&br, extra_ptr, sz_extra);
    /* Delta-offset rep state (only used when delta_offsets is set). */
    uint32_t dr1 = LZS_REP_INIT_1, dr2 = LZS_REP_INIT_2,
             dr3 = LZS_REP_INIT_3;
    /* Repcode state (only used when has_repcodes is set). */
    uint32_t r1 = LZS_REP_INIT_1, r2 = LZS_REP_INIT_2,
             r3 = LZS_REP_INIT_3;

    size_t dp = 0, lp = 0;
    size_t safe_end = (dst_size >= TDC_WILDCOPY_SLACK)
                          ? dst_size - TDC_WILDCOPY_SLACK : 0;

    /* --- Upfront symbol-range validation (items 4.2) ---
     * Move the 3 per-sequence symbol-range checks out of the hot loop.
     * After this pass, the main loop can skip those branches entirely. */
    {
        uint8_t off_max = delta_offsets ? (uint8_t)LZS_MAX_OFFSET_DELTA_SYMBOL
                                        : (uint8_t)LZS_MAX_OFFSET_SYMBOL;
        for (uint32_t i = 0; i < n_seqs; i++) {
            if (ll_dec[i]  > LZS_MAX_LL_SYMBOL) return TDC_E_CORRUPT;
            if (ml_dec[i]  > LZS_MAX_ML_SYMBOL) return TDC_E_CORRUPT;
            if (off_dec[i] > off_max)            return TDC_E_CORRUPT;
        }
    }

    tdc_dp_count_seqs(n_seqs);
    for (uint32_t i = 0; i < n_seqs; i++) {
        uint8_t s;
        uint64_t t_sym = tdc_dp_rdtsc();

        /* --- Reconstruct lit_len (table-driven, branchless) --- */
        s = ll_dec[i];
        lzs_recon_t r = LZS_UINT_RECON[s];
        uint32_t ll = r.base + lzs_br_read(&br, r.nbits);

        /* --- Reconstruct match_len --- */
        s = ml_dec[i];
        r = LZS_UINT_RECON[s];
        uint32_t ml = r.base + lzs_br_read(&br, r.nbits) + LZ_MIN_MATCH;

        /* --- Reconstruct offset code --- */
        uint32_t mo;
        s = off_dec[i];
        if (delta_offsets) {
            if (s <= 2u) {
                mo = (uint32_t)s + 1u;
                lzs_rep_update(mo, &dr1, &dr2, &dr3);
            } else {
                uint8_t usym = (uint8_t)(s - 3u);
                r = LZS_UINT_RECON[usym];
                uint32_t zigzag = r.base + lzs_br_read(&br, r.nbits);
                int32_t delta = lzs_zigzag_decode(zigzag);
                int64_t off64 = (int64_t)dr1 + delta;
                if (off64 <= 0) return TDC_E_CORRUPT;
                mo = (uint32_t)off64 + 3u;
                lzs_rep_update(mo, &dr1, &dr2, &dr3);
            }
        } else {
            r = LZS_OFF_RECON[s];
            mo = r.base + lzs_br_read(&br, r.nbits);
        }

        /* --- Repcode decode (inline, flattened) ---
         * Four cases (code=1,2,3, code>=4), but the r2/r3 update rule
         * unifies across them: r3 takes r2 when code>=3, r2 takes r1
         * when code>=2. Only new_r1 needs a real select: reps[code] for
         * code<=3, else (code-3). Replaces the 3-if-else chain with two
         * cmov-friendly ternaries. */
        if (has_repcodes) {
            uint32_t code = mo;
            if (code == 0u) return TDC_E_CORRUPT;
            uint32_t reps[4] = { 0u, r1, r2, r3 };
            uint32_t v = code - 3u;
            if (code > 3u && v > LZ_MAX_OFFSET) return TDC_E_CORRUPT;
            uint32_t new_r1 = (code <= 3u) ? reps[code] : v;
            uint32_t new_r2 = (code >= 2u) ? r1 : r2;
            uint32_t new_r3 = (code >= 3u) ? r2 : r3;
            mo = new_r1;
            r1 = new_r1; r2 = new_r2; r3 = new_r3;
        }

        /* --- Offset stride shift --- */
        if (offset_shift > 0) mo <<= offset_shift;
        tdc_dp_count_offset(mo);
        tdc_dp_add(TDC_DP_SYMBOL, t_sym);

        /* --- Execute: prefetch match address (item 1.5) ---
         * mo >= 1 always by construction: non-repcode LZS_OFF_RECON[0].base
         * is 1; delta-offset path produces mo = s+1 or off64+3; repcode
         * path rejects code==0 and uses r1/r2/r3 (init nonzero) or code-3
         * (explicit mo==0 check). So mo != 0 is dead here. */
        uint64_t t_other = tdc_dp_rdtsc();
        if ((uint64_t)lp + ll > total_lit) return TDC_E_CORRUPT;
        if ((uint64_t)dp + ll > dst_size)  return TDC_E_CORRUPT;
        if (mo <= dp)
            TDC_PREFETCH_L1(dst + dp - mo + ll);
        tdc_dp_add(TDC_DP_OTHER, t_other);

        /* --- Execute: copy literals (item 4.3) ---
         * Gate the 16-byte SIMD path on lp + 16 <= total_lit too: the
         * last sequences can sit within 15 bytes of the literals tail,
         * and the SIMD load overreads up to 16 bytes from lit_raw. */
        uint64_t t_lit = tdc_dp_rdtsc();
        if (ll > 0) {
            if (ll <= 16 && dp + ll <= safe_end && lp + 16 <= total_lit) {
                tdc_copy16(dst + dp, lit_raw + lp);
            } else {
                memcpy(dst + dp, lit_raw + lp, ll);
            }
            dp += ll;
            lp += ll;
        }
        tdc_dp_add(TDC_DP_LIT, t_lit);

        /* --- Execute: match copy ---
         * ml >= LZ_MIN_MATCH always: ml = base + extras + LZ_MIN_MATCH
         * with base >= 0, extras >= 0. Upfront symbol-range pass also
         * bounds base + extras. Only mo > dp (out-of-range backref) and
         * dp + ml > dst_size (oob write) are load-dependent. */
        uint64_t t_mch0 = tdc_dp_rdtsc();
        if (mo > dp)                       return TDC_E_CORRUPT;
        if ((uint64_t)dp + ml > dst_size)  return TDC_E_CORRUPT;
        tdc_dp_add(TDC_DP_OTHER, t_mch0);

        uint64_t t_mch = tdc_dp_rdtsc();
        if (dp + ml <= safe_end) {
            tdc_match_copy(dst + dp, mo, ml);
        } else {
            size_t from = dp - mo;
            for (uint32_t k = 0; k < ml; k++)
                dst[dp + k] = dst[from + k];
        }
        dp += ml;
        tdc_dp_add(TDC_DP_MATCH, t_mch);
    }

    if ((uint64_t)lp + trailing != total_lit) return TDC_E_CORRUPT;
    if (dp + trailing != dst_size)            return TDC_E_CORRUPT;
    if (trailing > 0) memcpy(dst + dp, lit_raw + lp, trailing);
    return TDC_OK;
}

/* Shared output reconstruction — used by both v1 and v2 decoders.
 *
 * Match copy uses wildcopy (up to TDC_WILDCOPY_SLACK bytes overshoot:
 * 31 on AVX2 builds for wildcopy32, 15 otherwise for wildcopy16). The
 * fast path only runs while dp + ml + TDC_WILDCOPY_SLACK <= dst_size;
 * the last few sequences fall back to byte-by-byte copy. */
#define LZS_PREFETCH_DIST 8u
static tdc_status lzs_reconstruct(uint8_t *dst, size_t dst_size,
                                    const uint8_t *lit_raw, uint32_t total_lit,
                                    uint32_t trailing,
                                    const uint32_t *lit_lens,
                                    const uint32_t *match_lens,
                                    const uint32_t *match_offs,
                                    uint32_t n_seqs) {
    size_t dp = 0, lp = 0;
    /* Fast path: wildcopy-safe region. */
    size_t safe_end = (dst_size >= TDC_WILDCOPY_SLACK)
                          ? dst_size - TDC_WILDCOPY_SLACK : 0;

    /* Upfront validation: sum lit_lens and match_lens, check against totals.
     * If this passes, every per-sequence buffer-bounds check in the hot loop
     * is provably redundant (except mo <= dp, which is load-dependent).
     * ml >= LZ_MIN_MATCH is guaranteed by symbol reconstruction. */
    uint64_t sum_ll = 0, sum_ml = 0;
    for (uint32_t k = 0; k < n_seqs; k++) {
        sum_ll += lit_lens[k];
        sum_ml += match_lens[k];
    }
    if (sum_ll + trailing != total_lit)        return TDC_E_CORRUPT;
    if (sum_ll + sum_ml + trailing != dst_size) return TDC_E_CORRUPT;

    /* 8-sequence match-address prefetch pipeline.
     *
     * Maintains dp_ahead running LZS_PREFETCH_DIST sequences ahead of the
     * execute cursor. Each step computes the match address for seq[i+D]
     * and issues an L1 prefetch, so by the time execute reaches seq[i+D]
     * the match cacheline is warm. Covers ~200-300 ns of main-memory
     * latency on long-offset matches (common on f64 scientific data
     * with periodic patterns, per the offset histogram).
     *
     * Correctness notes:
     *   - dp_ahead is an address predictor, not an authoritative cursor;
     *     actual bounds/validity checks stay in the execute loop.
     *   - If the prefetched address is one that a prior sequence is about
     *     to write, the prefetch just warms a stale cacheline and loses
     *     nothing (the later write still wins).
     *   - If dp_ahead + ll + ml runs past dst_size mid-prime, we clamp
     *     the prefetch address so we never issue a prefetch for OOB
     *     memory; the execute loop will catch the actual corruption. */

    /* Prime: walk dp_ahead forward D sequences, prefetching each match. */
    tdc_dp_count_seqs(n_seqs);
    for (uint32_t k = 0; k < n_seqs; k++) tdc_dp_count_offset(match_offs[k]);
    size_t dp_ahead = 0;
    uint32_t primed = (n_seqs < LZS_PREFETCH_DIST) ? n_seqs : LZS_PREFETCH_DIST;
    for (uint32_t k = 0; k < primed; k++) {
        uint32_t ll_k = lit_lens[k];
        uint32_t ml_k = match_lens[k];
        uint32_t mo_k = match_offs[k];
        size_t   addr = dp_ahead + ll_k;
        if (mo_k != 0 && mo_k <= addr && addr < dst_size)
            TDC_PREFETCH_L1(dst + addr - mo_k);
        dp_ahead = addr + ml_k;
        if (dp_ahead > dst_size) dp_ahead = dst_size;
    }

    uint32_t i = 0;
    /* Main loop: execute seq[i], prefetch seq[i+D]. Runs while there are
     * D-ahead sequences to prefetch (i + D < n_seqs). */
    uint32_t main_end = (n_seqs > LZS_PREFETCH_DIST) ? (n_seqs - LZS_PREFETCH_DIST) : 0;
    for (; i < main_end; i++) {
        uint32_t ll = lit_lens[i];
        uint32_t ml = match_lens[i];
        uint32_t mo = match_offs[i];

        /* Prefetch seq[i+D]. dp_ahead tracks the D-ahead position; mo_a
         * uses that same ahead position. Counted as SYMBOL since the
         * address-walk is the reconstruct-side cost. */
        uint64_t t_sym = tdc_dp_rdtsc();
        uint32_t ll_a = lit_lens[i + LZS_PREFETCH_DIST];
        uint32_t ml_a = match_lens[i + LZS_PREFETCH_DIST];
        uint32_t mo_a = match_offs[i + LZS_PREFETCH_DIST];
        size_t   addr_a = dp_ahead + ll_a;
        if (mo_a != 0 && mo_a <= addr_a && addr_a < dst_size)
            TDC_PREFETCH_L1(dst + addr_a - mo_a);
        dp_ahead = addr_a + ml_a;
        if (dp_ahead > dst_size) dp_ahead = dst_size;
        tdc_dp_add(TDC_DP_SYMBOL, t_sym);

        uint64_t t_lit = tdc_dp_rdtsc();
        if (ll > 0) {
            if (ll <= 16 && dp + ll <= safe_end && lp + 16 <= total_lit) {
                tdc_copy16(dst + dp, lit_raw + lp);
            } else {
                memcpy(dst + dp, lit_raw + lp, ll);
            }
            dp += ll;
            lp += ll;
        }
        tdc_dp_add(TDC_DP_LIT, t_lit);

        uint64_t t_other = tdc_dp_rdtsc();
        if (mo == 0 || mo > dp) return TDC_E_CORRUPT;
        tdc_dp_add(TDC_DP_OTHER, t_other);

        uint64_t t_mch = tdc_dp_rdtsc();
        if (dp + ml <= safe_end) {
            tdc_match_copy(dst + dp, mo, ml);
        } else {
            size_t from = dp - mo;
            for (uint32_t k = 0; k < ml; k++)
                dst[dp + k] = dst[from + k];
        }
        dp += ml;
        tdc_dp_add(TDC_DP_MATCH, t_mch);
    }

    /* Drain: execute the last D sequences without further prefetch. */
    for (; i < n_seqs; i++) {
        uint32_t ll = lit_lens[i];
        uint32_t ml = match_lens[i];
        uint32_t mo = match_offs[i];

        if (ll > 0) {
            if (ll <= 16 && dp + ll <= safe_end && lp + 16 <= total_lit) {
                tdc_copy16(dst + dp, lit_raw + lp);
            } else {
                memcpy(dst + dp, lit_raw + lp, ll);
            }
            dp += ll;
            lp += ll;
        }

        if (mo == 0 || mo > dp) return TDC_E_CORRUPT;

        if (dp + ml <= safe_end) {
            tdc_match_copy(dst + dp, mo, ml);
        } else {
            size_t from = dp - mo;
            for (uint32_t k = 0; k < ml; k++)
                dst[dp + k] = dst[from + k];
        }
        dp += ml;
    }

    if ((uint64_t)lp + trailing != total_lit) return TDC_E_CORRUPT;
    if (dp + trailing != dst_size)            return TDC_E_CORRUPT;
    if (trailing > 0) memcpy(dst + dp, lit_raw + lp, trailing);
    return TDC_OK;
}

/* Shared symbol→value reconstruction for v2 and v3 decoders. Reads
 * symbols from ll_dec/ml_dec/off_dec (v2 separate, or de-interleaved
 * from v3 combined) and extra bits from extra_ptr. Fills lit_lens,
 * match_lens, match_offs. */
static tdc_status lzs_reconstruct_symbols(const uint8_t *ll_dec,
                                            const uint8_t *ml_dec,
                                            const uint8_t *off_dec,
                                            const uint8_t *extra_ptr,
                                            uint32_t sz_extra,
                                            uint32_t n_seqs,
                                            int delta_offsets,
                                            uint32_t *lit_lens,
                                            uint32_t *match_lens,
                                            uint32_t *match_offs) {
    uint32_t bit_pos = 0;
    uint32_t sz_extra_bits = sz_extra << 3;  /* precompute once */
    uint32_t dr1 = LZS_REP_INIT_1, dr2 = LZS_REP_INIT_2,
             dr3 = LZS_REP_INIT_3;
    for (uint32_t i = 0; i < n_seqs; i++) {
        uint8_t s; uint32_t ex, nb;

        /* Reconstruct lit_len. */
        s = ll_dec[i];
        if (s > LZS_MAX_LL_SYMBOL) return TDC_E_CORRUPT;
        ex = 0;
        if (s >= 2u) {
            nb = (uint32_t)s - 1u;
            if (bit_pos + nb > sz_extra_bits) return TDC_E_CORRUPT;
            ex = lzs_bits_read_safe(extra_ptr, sz_extra, &bit_pos, nb);
        }
        lit_lens[i] = lzs_symbol_to_uint(s, ex);

        /* Reconstruct match_len. */
        s = ml_dec[i];
        if (s > LZS_MAX_ML_SYMBOL) return TDC_E_CORRUPT;
        ex = 0;
        if (s >= 2u) {
            nb = (uint32_t)s - 1u;
            if (bit_pos + nb > sz_extra_bits) return TDC_E_CORRUPT;
            ex = lzs_bits_read_safe(extra_ptr, sz_extra, &bit_pos, nb);
        }
        match_lens[i] = lzs_symbol_to_uint(s, ex) + LZ_MIN_MATCH;

        /* Reconstruct offset code. */
        s = off_dec[i];
        if (delta_offsets) {
            if (s > LZS_MAX_OFFSET_DELTA_SYMBOL) return TDC_E_CORRUPT;
            if (s <= 2u) {
                match_offs[i] = (uint32_t)s + 1u;
                lzs_rep_update(match_offs[i], &dr1, &dr2, &dr3);
            } else {
                uint8_t usym = (uint8_t)(s - 3u);
                nb = (usym >= 2u) ? (uint32_t)usym - 1u : 0u;
                ex = 0;
                if (nb > 0) {
                    if (bit_pos + nb > sz_extra_bits) return TDC_E_CORRUPT;
                    ex = lzs_bits_read_safe(extra_ptr, sz_extra, &bit_pos, nb);
                }
                uint32_t zigzag = lzs_symbol_to_uint(usym, ex);
                int32_t delta = lzs_zigzag_decode(zigzag);
                int64_t off64 = (int64_t)dr1 + delta;
                if (off64 <= 0) return TDC_E_CORRUPT;
                uint32_t actual_off = (uint32_t)off64;
                match_offs[i] = actual_off + 3u;
                lzs_rep_update(match_offs[i], &dr1, &dr2, &dr3);
            }
        } else {
            if (s > LZS_MAX_OFFSET_SYMBOL) return TDC_E_CORRUPT;
            ex = 0;
            if (s >= 3u) {
                nb = (uint32_t)s - 3u;
                if (nb > 0) {
                    if (bit_pos + nb > sz_extra_bits) return TDC_E_CORRUPT;
                    ex = lzs_bits_read_safe(extra_ptr, sz_extra, &bit_pos, nb);
                }
            }
            match_offs[i] = lzs_symbol_to_code(s, ex);
        }
    }
    return TDC_OK;
}


static tdc_status lzs_decode(const uint8_t *src, size_t src_size,
                              uint8_t       *dst, size_t dst_size) {
    if (src_size < LZS_HEADER_SIZE_V1) return TDC_E_CORRUPT;
    if (dst_size > 0 && !dst)           return TDC_E_INVAL;

    uint8_t version = src[0];
    uint8_t flags   = src[1];
    if (version != 1u && version != 2u) return TDC_E_CORRUPT;
    if (flags & ~(uint8_t)(LZS_FLAG_REPCODES | LZS_FLAG_OFFSET_SHIFT
                           | LZS_FLAG_LIT_CONTEXT
                           | LZS_FLAG_DELTA_OFFSETS))
        return TDC_E_CORRUPT;
    int has_repcodes      = (flags & LZS_FLAG_REPCODES)      != 0;
    int has_lit_ctx       = (flags & LZS_FLAG_LIT_CONTEXT)   != 0;
    int has_delta_offsets = (flags & LZS_FLAG_DELTA_OFFSETS)  != 0;
    uint32_t offset_shift = (flags & LZS_FLAG_OFFSET_SHIFT) ? src[2] : 0;
    if (offset_shift > 7u) return TDC_E_CORRUPT;

    uint32_t hdr_size = (version == 1u) ? LZS_HEADER_SIZE_V1
                                        : LZS_HEADER_SIZE_V2;
    if (src_size < hdr_size) return TDC_E_CORRUPT;

    uint32_t n_seqs, total_lit, trailing, uncompressed;
    memcpy(&n_seqs,       src +  4, 4);
    memcpy(&total_lit,    src +  8, 4);
    memcpy(&trailing,     src + 12, 4);
    memcpy(&uncompressed, src + 16, 4);
    if ((size_t)uncompressed != dst_size) return TDC_E_CORRUPT;
    if (trailing > total_lit)             return TDC_E_CORRUPT;

    tdc_entropy_id id_lit = (tdc_entropy_id)src[20];
    tdc_entropy_id id_ll  = (tdc_entropy_id)src[21];
    tdc_entropy_id id_ml  = (tdc_entropy_id)src[22];
    tdc_entropy_id id_mo  = (tdc_entropy_id)src[23];

    uint32_t sz_lit, sz_ll, sz_ml, sz_mo;
    memcpy(&sz_lit, src + 24, 4);
    memcpy(&sz_ll,  src + 28, 4);
    memcpy(&sz_ml,  src + 32, 4);
    memcpy(&sz_mo,  src + 36, 4);

    uint32_t sz_extra = 0;
    if (version == 2u) memcpy(&sz_extra, src + 40, 4);

    /* Bounds check the body. */
    {
        uint64_t need = (uint64_t)hdr_size
                      + sz_lit + sz_ll + sz_ml + sz_mo + sz_extra;
        if (need > src_size) return TDC_E_CORRUPT;
    }

    /* Empty record. */
    if (dst_size == 0) {
        if (n_seqs != 0 || total_lit != 0) return TDC_E_CORRUPT;
        return TDC_OK;
    }

    size_t stream_bytes = (size_t)n_seqs * 4u;

    /* v2: three symbol streams (1 byte each) + u32 output arrays.
     * v1: three byte-shuffled streams (4 bytes each) + u32 output arrays.
     * Fused v2 path (non-split-extras): no u32 output arrays needed. */
    size_t v2_sym_size  = 3u * (size_t)n_seqs;
    size_t v1_raw_size  = 3u * stream_bytes;
    size_t per_version  = (version == 2u) ? v2_sym_size : v1_raw_size;
    int use_fused = (version == 2u);  /* both split and non-split paths are fused */
    size_t u32_arrays   = use_fused ? 0 : 3u * (size_t)n_seqs * sizeof(uint32_t);
    size_t scratch_bytes = (size_t)total_lit + per_version + u32_arrays;
    uint8_t *scratch = scratch_bytes ? (uint8_t *)malloc(scratch_bytes) : NULL;
    if (scratch_bytes > 0 && !scratch) return TDC_E_NOMEM;

    uint8_t  *lit_raw    = scratch;
    uint32_t *lit_lens = NULL, *match_lens = NULL, *match_offs = NULL;

    uint8_t *region2 = lit_raw + total_lit;

    if (!use_fused) {
        if (version == 1u) {
            lit_lens   = (uint32_t *)(region2 + 3u * stream_bytes);
        } else {
            lit_lens   = (uint32_t *)(region2 + 3u * (size_t)n_seqs);
        }
        match_lens = lit_lens   + n_seqs;
        match_offs = match_lens + n_seqs;
    }

    tdc_status st = TDC_OK;
    const uint8_t *p = src + hdr_size;

    /* Decode literal stream — flat or order-1 context. */
    if (total_lit > 0) {
        if (has_lit_ctx) {
            st = lzs_decode_lit_context(p, sz_lit, lit_raw, total_lit);
        } else {
            const tdc_entropy_vt *sub = lzs_sub_vt(id_lit);
            if (!sub) { st = TDC_E_CORRUPT; goto done; }
            st = sub->decode(p, sz_lit, lit_raw, total_lit);
        }
        if (st != TDC_OK) goto done;
    }
    p += sz_lit;

    if (n_seqs > 0) {
        if (version == 1u) {
            /* v1: three byte-shuffled u32 streams. */
            uint8_t *ll_raw = region2;
            uint8_t *ml_raw = ll_raw + stream_bytes;
            uint8_t *mo_raw = ml_raw + stream_bytes;

            const tdc_entropy_vt *sub_ll = lzs_sub_vt(id_ll);
            const tdc_entropy_vt *sub_ml = lzs_sub_vt(id_ml);
            const tdc_entropy_vt *sub_mo = lzs_sub_vt(id_mo);
            if (!sub_ll || !sub_ml || !sub_mo) { st = TDC_E_CORRUPT; goto done; }

            st = sub_ll->decode(p, sz_ll, ll_raw, stream_bytes);
            if (st != TDC_OK) goto done;
            p += sz_ll;
            st = sub_ml->decode(p, sz_ml, ml_raw, stream_bytes);
            if (st != TDC_OK) goto done;
            p += sz_ml;
            st = sub_mo->decode(p, sz_mo, mo_raw, stream_bytes);
            if (st != TDC_OK) goto done;

            lzs_unshuffle_u32(ll_raw, n_seqs, lit_lens);
            lzs_unshuffle_u32(ml_raw, n_seqs, match_lens);
            lzs_unshuffle_u32(mo_raw, n_seqs, match_offs);
        } else {
            /* v2: three symbol streams + combined extra bits. */
            uint8_t *ll_dec  = region2;
            uint8_t *ml_dec  = ll_dec + n_seqs;
            uint8_t *off_dec = ml_dec + n_seqs;

            const tdc_entropy_vt *sub_ll  = lzs_sub_vt(id_ll);
            const tdc_entropy_vt *sub_ml  = lzs_sub_vt(id_ml);
            const tdc_entropy_vt *sub_off = lzs_sub_vt(id_mo);
            if (!sub_ll || !sub_ml || !sub_off) { st = TDC_E_CORRUPT; goto done; }

            uint64_t t_sd = tdc_dp_rdtsc();
            st = sub_ll->decode(p, sz_ll, ll_dec, n_seqs);
            if (st != TDC_OK) goto done;
            st = sub_ml->decode(p + sz_ll, sz_ml, ml_dec, n_seqs);
            if (st != TDC_OK) goto done;
            st = sub_off->decode(p + sz_ll + sz_ml, sz_mo, off_dec, n_seqs);
            if (st != TDC_OK) goto done;
            p += sz_ll + sz_ml + sz_mo;
            tdc_dp_add(TDC_DP_STREAMDEC, t_sd);

            /* Fused single-pass decode: symbol reconstruct + repcode +
             * offset shift + sequence execution in one loop. Skips the
             * intermediate uint32 arrays entirely. */
            st = lzs_decode_fused(ll_dec, ml_dec, off_dec,
                                   p, sz_extra,
                                   lit_raw, total_lit, trailing,
                                   n_seqs, has_delta_offsets,
                                   has_repcodes, offset_shift,
                                   dst, dst_size);
            goto done;
        }

        if (has_repcodes) {
            st = lzs_repcode_decode(match_offs, n_seqs);
            if (st != TDC_OK) goto done;
        }

        /* Undo offset stride shift. */
        if (offset_shift > 0) {
            for (uint32_t i = 0; i < n_seqs; i++)
                match_offs[i] <<= offset_shift;
        }
    }

    st = lzs_reconstruct(dst, dst_size, lit_raw, total_lit, trailing,
                           lit_lens, match_lens, match_offs, n_seqs);

done:
    free(scratch);
    return st;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_entropy_vt tdc_entropy_lz_streams_vt = {
    .id           = TDC_ENTROPY_LZ_STREAMS,
    .name         = "lz_streams",
    .encode_bound = lzs_encode_bound,
    .encode       = lzs_encode,
    .decode       = lzs_decode,
};
