/*
 * src/entropy/lz_internal.h
 *
 * Private header shared between lz.c (greedy encoder + decoder) and
 * lz_opt.c (optimal parser). Neither end is part of the public ABI.
 *
 * The two encoders emit the SAME on-disk LZ stream — optimal parsing is
 * a pure encode-side optimization. Decoding is handled by lz.c's
 * lz_decode_core regardless of which parser produced the stream, so
 * existing round-trip tests gate both.
 */

#ifndef TDC_ENTROPY_LZ_INTERNAL_H
#define TDC_ENTROPY_LZ_INTERNAL_H

#include "tdc/entropy.h"

#include <stdint.h>
#include <stddef.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/* On-disk constants. */
#define LZ_MIN_MATCH   3u
#define LZ_MAX_OFFSET  (1u << 22)  /* 4 MiB window — 22-bit offset range */
#define LZ_HEADER_SIZE 8u          /* uint32 n_sequences + uint32 literals_size */

/* Initial repcode offsets (zstd defaults). Shared by the greedy parser
 * (rep-at-literal probing in lz.c), the optimal parser (repcode-aware DP
 * in lz_opt.c), and the streams serializer (MRU transform in lz_streams.c)
 * so all three start from the same state. */
#define LZ_REP_INIT_1  1u
#define LZ_REP_INIT_2  4u
#define LZ_REP_INIT_3  8u

/* Encoder-side sequence descriptor.
 *   lit_len   — literal bytes emitted before this match (0 .. N)
 *   match_len — actual match length, >= LZ_MIN_MATCH
 *   match_off — back-reference offset, 1 .. LZ_MAX_OFFSET
 */
typedef struct {
    uint32_t lit_len;
    uint32_t match_len;
    uint32_t match_off;
} LZSeq;

/* ----- LEB128 varint helpers --------------------------------------------- *
 *
 * LEB128 (Little-Endian Base 128) is the variable-width integer encoding
 * used for match lengths, literal-run extensions, AND match offsets. Each
 * byte carries 7 data bits + 1 continuation bit (MSB). The same three
 * helpers — size / write / read — are used by the serializer, the
 * decoder, and the cost model so the encoding lives in one place.
 */
static inline uint32_t lz_leb128_size(uint32_t v) {
    uint32_t n = 1u;
    while (v >= 128u) { v >>= 7; n++; }
    return n;
}

static inline uint8_t *lz_leb128_write(uint8_t *p, uint32_t v) {
    while (v >= 128u) {
        *p++ = (uint8_t)((v & 0x7Fu) | 0x80u);
        v >>= 7;
    }
    *p++ = (uint8_t)v;
    return p;
}

static inline const uint8_t *lz_leb128_read(const uint8_t *p, uint32_t *out) {
    uint32_t val = 0u;
    uint32_t shift = 0u;
    uint8_t b;
    do {
        b = *p++;
        val |= ((uint32_t)(b & 0x7Fu)) << shift;
        shift += 7u;
    } while (b & 0x80u);
    *out = val;
    return p;
}

/* ----- Offset encoding --------------------------------------------------- *
 *
 * Match offsets use a 2-byte base with overflow extension:
 *
 *   uint16_le off_m1      always written (2 bytes)
 *   [LEB128 extension]    present only when off_m1 == 0xFFFF
 *
 * Decoding:
 *   Read uint16_le val.
 *   If val < 0xFFFF:  offset = val + 1            (range 1 – 65535)
 *   If val == 0xFFFF: read LEB128 ext,
 *                     offset = ext + 65536         (range 65536 – ~2 GiB)
 *
 * This preserves the 2-byte cost for the common case (offsets ≤ 65535)
 * while extending the window to 1 MiB+ for periodic signals whose
 * seasonal stride exceeds 64K bytes.
 */
static inline uint32_t lz_offset_size(uint32_t off) {
    if (off <= 65535u) return 2u;
    return 2u + lz_leb128_size(off - 65536u);
}

static inline uint8_t *lz_offset_write(uint8_t *p, uint32_t off) {
    if (off <= 65535u) {
        uint16_t v = (uint16_t)(off - 1u);
        memcpy(p, &v, 2);
        return p + 2;
    }
    uint16_t sentinel = 0xFFFFu;
    memcpy(p, &sentinel, 2);
    return lz_leb128_write(p + 2, off - 65536u);
}

static inline const uint8_t *lz_offset_read(const uint8_t *p, uint32_t *off) {
    uint16_t v;
    memcpy(&v, p, 2);
    p += 2;
    if (v < 0xFFFFu) {
        *off = (uint32_t)v + 1u;
    } else {
        uint32_t ext;
        p = lz_leb128_read(p, &ext);
        *off = ext + 65536u;
    }
    return p;
}

/* ---- Bounded reads for decoding untrusted input --------------------------
 * The readers above trust the stream. When decoding a possibly-corrupt block
 * the decoder passes `end` (the end of the sequence region) so a crafted
 * length/varint cannot walk the cursor past the buffer. Each returns NULL on
 * overrun; the shift guard also caps a malicious multi-byte LEB128. */
static inline const uint8_t *lz_leb128_read_bounded(const uint8_t *p,
                                                    const uint8_t *end,
                                                    uint32_t *out) {
    uint32_t val = 0u, shift = 0u;
    uint8_t b;
    do {
        if (p >= end || shift >= 32u) return NULL;
        b = *p++;
        val |= ((uint32_t)(b & 0x7Fu)) << shift;
        shift += 7u;
    } while (b & 0x80u);
    *out = val;
    return p;
}

static inline const uint8_t *lz_offset_read_bounded(const uint8_t *p,
                                                    const uint8_t *end,
                                                    uint32_t *off) {
    if (end - p < 2) return NULL;
    uint16_t v;
    memcpy(&v, p, 2);
    p += 2;
    if (v < 0xFFFFu) {
        *off = (uint32_t)v + 1u;
        return p;
    }
    uint32_t ext;
    p = lz_leb128_read_bounded(p, end, &ext);
    if (!p) return NULL;
    *off = ext + 65536u;
    return p;
}

/* ----- Sequence size computation ----------------------------------------- *
 *
 * Bytes charged to the packed sequence header for a given literal-run
 * length, match_len_m3, and match offset. Used by both the greedy
 * encoder (serializer size check) and the optimal parser (cost model).
 *
 * Layout per sequence:
 *   [LLLLMMMM tag]  1 byte
 *   [lit ext]       0+ bytes (chained-255 varint if lit_len >= 15)
 *   [match ext]     0+ bytes (LEB128 if match_len_m3 >= 15)
 *   [offset]        2+ bytes (uint16 base + optional LEB128 extension)
 */
static inline uint32_t lz_seq_encoded_size(uint32_t lit_len,
                                            uint32_t match_len_m3,
                                            uint32_t match_off) {
    uint32_t size = 1u + lz_offset_size(match_off); /* tag + offset */
    if (lit_len >= 15u) {
        uint32_t extra = lit_len - 15u;
        size += extra / 255u + 1u;
    }
    if (match_len_m3 >= 15u) {
        size += lz_leb128_size(match_len_m3 - 15u);
    }
    return size;
}

/* Shared on-disk serializer (defined in lz.c). Takes a pre-built LZSeq
 * array (from greedy or optimal parsing) and writes the on-disk LZ
 * stream into dst. Literals are read directly from src between match
 * ends. Falls back to a literal-only stream if the parse would not
 * compress. */
tdc_status tdc_lz_serialize_sequences(const uint8_t *src, uint32_t src_size,
                                       const LZSeq *seqs, uint32_t seq_count,
                                       tdc_buffer *dst);

/* Shared greedy match-finder (defined in lz.c). Walks src once with a
 * hash-chain match finder (4 MiB window) and emits an LZSeq array. The
 * caller frees *out_seqs via dst->realloc_fn (allocated through it).
 *
 * chain_depth: how many hash-chain positions to probe per lookup.
 *   0 = flat hash (one candidate per bucket, fastest, worst ratio).
 *   4-8 = good balance (zstd-like greedy).
 *   32-64 = deep search (slower, better ratio on structured data).
 *
 * lazy_depth: lazy matching.
 *   0 = no lazy (emit first match found).
 *   1 = single lazy (check position+1; if longer match, emit literal at P).
 *   2 = double lazy (check position+2 as well). */
tdc_status tdc_lz_parse_greedy(const uint8_t *src, uint32_t src_size,
                                tdc_buffer *dst,
                                uint32_t chain_depth, uint32_t lazy_depth,
                                LZSeq **out_seqs, uint32_t *out_seq_count);

/* Shared optimal-parser match-finder (defined in lz_opt.c). Walks src
 * with a hash-chain match finder (depth ≤ 128) and runs a forward DP
 * over the single-stream cost model (literals = 8 bits, matches = 8*(1 +
 * offset_size) bits + match_ext + bracket penalty for long literal runs).
 * Emits the minimum-cost LZSeq array. Same output contract as
 * tdc_lz_parse_greedy: caller frees *out_seqs via dst->realloc_fn.
 * Used by TDC_ENTROPY_LZ_OPT for single-stream encoding. */
tdc_status tdc_lz_parse_optimal(const uint8_t *src, uint32_t src_size,
                                 tdc_buffer *dst,
                                 LZSeq **out_seqs, uint32_t *out_seq_count);

/* STREAMS-cost optimal parser (defined in lz_opt.c). Same match-finder
 * as tdc_lz_parse_optimal but runs the DP with the multi-stream cost
 * model: literals ≈ 6 bits (entropy-coded), match header ≈ 31 bits
 * (ll + ml + mo combined), no match_ext, no lit-run bracket penalty.
 * The resulting parse is optimal for the LZ_STREAMS on-disk format, not
 * the single-stream format. Same output contract as the legacy variant.
 * Used by TDC_ENTROPY_LZ_STREAMS (first pass). */
tdc_status tdc_lz_parse_optimal_streams(const uint8_t *src, uint32_t src_size,
                                         tdc_buffer *dst,
                                         LZSeq **out_seqs,
                                         uint32_t *out_seq_count);

/* Per-symbol cost table for the priced STREAMS parser. All costs are in
 * 1/8-bit units (matching the LZ_OPT_STREAMS_*_COST scale). Computed
 * from first-pass frequency tables via Shannon entropy. */
#define LZS_PRICE_MAX_SYM  32u  /* comfortably covers all sub-alphabets */

typedef struct {
    int32_t lit[256];            /* per literal byte value */
    int32_t ll_avg;              /* flat ll overhead (avg symbol + extra) */
    int32_t ml[LZS_PRICE_MAX_SYM];  /* per ml symbol (Huf code + extra bits) */
    int32_t off[LZS_PRICE_MAX_SYM]; /* per off symbol (Huf code + extra bits) */
    uint32_t offset_shift;       /* right-shift to apply before offset symbol lookup */
} LzsStreamsPrices;

/* Priced variant of the STREAMS parser (second pass). Uses per-symbol
 * costs from the first pass to make more accurate parse decisions.
 * - Literal costs: per-byte, from prices->lit[byte_value]
 * - Match costs: per-symbol for ml and off, flat average for ll
 * Same output contract as tdc_lz_parse_optimal_streams. */
tdc_status tdc_lz_parse_optimal_streams_priced(
    const uint8_t *src, uint32_t src_size,
    tdc_buffer *dst,
    const LzsStreamsPrices *prices,
    LZSeq **out_seqs, uint32_t *out_seq_count);

#ifdef __cplusplus
}
#endif

#endif /* TDC_ENTROPY_LZ_INTERNAL_H */
