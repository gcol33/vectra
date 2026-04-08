/*
 * src/entropy/lz2.c
 *
 * TDC_ENTROPY_LZ2 — native LZ77 with separated streams.
 *
 * Architecture (matches the design notes in vectra/CLAUDE.md):
 *   - separated sequence-descriptor and literal streams (zstd-style)
 *   - 64K window, greedy hash matcher
 *   - packed variable-length [LLLLMMMM] tag byte; 3 bytes/sequence
 *     overhead in the common case
 *   - decode hot path: fast-path / safe-path split with 16-byte
 *     unconditional wildcopy for matches
 *
 * IMPORTANT: this file owns the entropy stage only. Byte shuffle is a
 * separate concern in transform/shuffle.c. The vectra naming
 * "SHUFFLE_LZ2" combined the two; tdc keeps them orthogonal.
 *
 * Source: extracted verbatim from vectra/src/vtr_codec.c
 *         (vtr_lz2_compress / vtr_lz2_decompress_into, lines 411-916).
 *         The inner encode/decode loops MUST stay byte-identical to
 *         vectra so existing .vtr files round-trip without churn. Do not
 *         "improve" the loops without benchmarks: see the design note in
 *         vectra/CLAUDE.md ("No batched parse-then-copy in LZ2 fast
 *         path") and the comment above lz2_decode_fast() below.
 *
 * Differences vs vectra:
 *   - All allocation goes through tdc_buffer::realloc_fn (POSIX-style:
 *     (NULL, n) allocates, (p, 0) frees). No bare malloc/free.
 *   - vectra returns NULL when input is incompressible or shorter than
 *     LZ2_MIN_MATCH+1 bytes; tdc instead emits a literal-only stream
 *     (n_seq=0, all bytes as trailing literals). The on-disk format is
 *     unchanged — vectra's decoder happens to handle that shape too via
 *     the safe-path trailing-literal block.
 *   - lz2_decode_safe returns TDC_E_CORRUPT on invalid back-references
 *     instead of calling vectra_error (which longjmps to R).
 */

#include "tdc/entropy.h"
#include "entropy_internal.h"
#include "../core/buffer.h"

#include <string.h>
#include <stdint.h>
#include <stddef.h>

#if defined(_MSC_VER)
#  include <intrin.h>
#endif

/* ----- Constants (frozen on-disk format) --------------------------------- */

#define LZ2_HASH_BITS  16
#define LZ2_HASH_SIZE  (1 << LZ2_HASH_BITS)
#define LZ2_MIN_MATCH  3
#define LZ2_MAX_MATCH  130   /* 7 bits + 3 */
#define LZ2_MAX_OFFSET 65536 /* 16 bits + 1 */

/* On-disk header is 8 bytes: uint32 n_sequences + uint32 literals_size. */
#define LZ2_HEADER_SIZE 8

/* ----- Branch hints (zstd-style) ----------------------------------------- */

#if defined(__GNUC__) || defined(__clang__)
#  define LZ2_LIKELY(x)   __builtin_expect(!!(x), 1)
#  define LZ2_UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#  define LZ2_LIKELY(x)   (x)
#  define LZ2_UNLIKELY(x) (x)
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

static void *lz2_alloc(tdc_buffer *buf, size_t n) {
    return buf->realloc_fn(buf->user, NULL, n);
}

static void lz2_free(tdc_buffer *buf, void *p) {
    if (p) (void)buf->realloc_fn(buf->user, p, 0);
}

/* Output-buffer growth uses the shared tdc_buf_reserve helper from
 * src/core/buffer.h. The previous local copy lz2_buf_reserve was lifted
 * along with shuffle_buf_reserve and quantize_buf_reserve into a single
 * source of truth — see src/core/buffer.h. */

/* ----- Match finder primitives ------------------------------------------- */

static inline uint32_t lz2_hash4(const uint8_t *p) {
    uint32_t h = ((uint32_t)p[0]) |
                 ((uint32_t)p[1] << 8) |
                 ((uint32_t)p[2] << 16) |
                 ((uint32_t)p[3] << 24);
    /* Fibonacci hash for good distribution */
    return (h * 2654435761u) >> (32 - LZ2_HASH_BITS);
}

/* Wildcopy: copy in 16-byte chunks, may overwrite up to 15 bytes past len.
 * Caller must ensure dst has room for len + 15 bytes. */
static inline void lz2_wildcopy16(uint8_t *dst, const uint8_t *src, uint32_t len) {
    const uint8_t *end = dst + len;
    do {
        memcpy(dst, src, 16);
        dst += 16;
        src += 16;
    } while (dst < end);
}

/* ----- Sequence descriptor (encoder-side temporary) ---------------------- */

typedef struct {
    uint32_t lit_len;
    uint32_t match_len;     /* 3-130 for matches */
    uint32_t match_off;     /* 1-65536 */
} LZ2Seq;

/* Encoded byte size of one packed sequence header.
 *
 * Layout:
 *   Byte 0: [LLLLMMMM] tag byte
 *     L (0-14):  literal byte count before this match
 *     L (15):    extended — read additional bytes (while byte==255: add 255;
 *                then add final byte < 255; total = 15 + sum)
 *     M (0-14):  match_length - 3
 *     M (15):    extended — read 1 extra byte; total = 18 + extra
 *   uint16_t offset_m1 (little-endian)
 *
 * Common case: 3 bytes (tag + 2-byte offset).
 */
static inline uint32_t lz2_seq_encoded_size(uint32_t lit_len, uint32_t match_len_m3) {
    uint32_t size = 1 + 2; /* tag byte + uint16 offset */
    if (lit_len >= 15) {
        uint32_t extra = lit_len - 15;
        size += extra / 255 + 1; /* 255-byte chunks + final byte */
    }
    if (match_len_m3 >= 15) {
        size += 1; /* single extra byte */
    }
    return size;
}

/* ----- Literal-only fallback --------------------------------------------- */
/*
 * Used when input is shorter than LZ2_MIN_MATCH+1 OR when the greedy match
 * finder produced no compression gain. Output is the same on-disk format:
 * 8-byte header with n_seq=0, literals_size=src_size, followed by raw bytes.
 * The decoder picks them up in the safe path's trailing-literal block.
 */
static tdc_status lz2_encode_literal_only(const uint8_t *src, uint32_t src_size,
                                          tdc_buffer *dst) {
    size_t total = LZ2_HEADER_SIZE + (size_t)src_size;
    tdc_status st = tdc_buf_reserve(dst, total);
    if (st != TDC_OK) return st;
    uint8_t *p = dst->data;
    uint32_t n_seq = 0;
    uint32_t literals_size = src_size;
    memcpy(p,     &n_seq,         4);
    memcpy(p + 4, &literals_size, 4);
    if (src_size > 0) memcpy(p + LZ2_HEADER_SIZE, src, src_size);
    dst->size = total;
    return TDC_OK;
}

/* ----- Encoder ----------------------------------------------------------- */
/*
 * Greedy hash-table match finder; outputs sequences into a temporary array
 * and literals into a temporary buffer, then concatenates them with packed
 * variable-length headers (zstd-style [LLLLMMMM] tag bytes).
 *
 * If the encoded size would meet or exceed the input size, falls back to
 * the literal-only stream so the entropy stage always produces a valid
 * LZ2 record. Vectra's variant returns NULL in that case and the caller
 * picks a different compression tag; tdc keeps the entropy stage closed
 * over its own input/output.
 */
static tdc_status lz2_encode_core(const uint8_t *src, uint32_t src_size,
                                  tdc_buffer *dst) {
    if (src_size < LZ2_MIN_MATCH + 1) {
        return lz2_encode_literal_only(src, src_size, dst);
    }

    /* Flat hash table: one position per slot. 0xFFFFFFFF = empty. */
    uint32_t *htab = (uint32_t *)lz2_alloc(dst, LZ2_HASH_SIZE * sizeof(uint32_t));
    if (!htab) return TDC_E_NOMEM;
    memset(htab, 0xFF, LZ2_HASH_SIZE * sizeof(uint32_t));

    /* Collect sequences into a growable array. */
    uint32_t seq_cap = 4096;
    uint32_t seq_count = 0;
    LZ2Seq *seqs = (LZ2Seq *)lz2_alloc(dst, seq_cap * sizeof(LZ2Seq));
    if (!seqs) {
        lz2_free(dst, htab);
        return TDC_E_NOMEM;
    }

    /* Collect literals into a separate buffer. */
    uint32_t lit_total = 0;
    uint8_t *lit_buf = (uint8_t *)lz2_alloc(dst, src_size);
    if (!lit_buf) {
        lz2_free(dst, htab);
        lz2_free(dst, seqs);
        return TDC_E_NOMEM;
    }

    uint32_t sp = 0;
    uint32_t lit_start = 0;

    while (sp < src_size) {
        uint32_t match_len = 0, match_off = 0;

        if (sp + LZ2_MIN_MATCH + 1 <= src_size) {
            uint32_t h = lz2_hash4(src + sp);
            uint32_t cand = htab[h];
            htab[h] = sp;

            if (cand != 0xFFFFFFFF && cand < sp) {
                uint32_t off = sp - cand;
                if (off <= LZ2_MAX_OFFSET && src[cand] == src[sp]) {
                    uint32_t max_len = src_size - sp;
                    if (max_len > LZ2_MAX_MATCH) max_len = LZ2_MAX_MATCH;

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
                            goto lz2_match_done;
                        }
                        len += 8;
                    }
                    while (len < max_len && src[sp + len] == src[cand + len])
                        len++;
                    lz2_match_done:;
                    if (len >= LZ2_MIN_MATCH) {
                        match_len = len;
                        match_off = off;
                    }
                }
            }
        }

        if (match_len >= LZ2_MIN_MATCH) {
            uint32_t pending_lit = sp - lit_start;

            if (seq_count >= seq_cap) {
                uint32_t new_cap = seq_cap * 2;
                LZ2Seq *new_seqs = (LZ2Seq *)dst->realloc_fn(
                    dst->user, seqs, new_cap * sizeof(LZ2Seq));
                if (!new_seqs) {
                    lz2_free(dst, htab);
                    lz2_free(dst, seqs);
                    lz2_free(dst, lit_buf);
                    return TDC_E_NOMEM;
                }
                seqs = new_seqs;
                seq_cap = new_cap;
            }
            seqs[seq_count].lit_len = pending_lit;
            seqs[seq_count].match_len = match_len;
            seqs[seq_count].match_off = match_off;
            seq_count++;

            if (pending_lit > 0) {
                memcpy(lit_buf + lit_total, src + lit_start, pending_lit);
                lit_total += pending_lit;
            }

            for (uint32_t i = 1; i < match_len && sp + i + LZ2_MIN_MATCH + 1 <= src_size; i += 4) {
                htab[lz2_hash4(src + sp + i)] = sp + i;
            }
            sp += match_len;
            lit_start = sp;
        } else {
            sp++;
        }
    }

    /* Trailing literals go into the literal buffer only (no sequence). */
    uint32_t trailing_lit = src_size - lit_start;
    if (trailing_lit > 0) {
        memcpy(lit_buf + lit_total, src + lit_start, trailing_lit);
        lit_total += trailing_lit;
    }

    lz2_free(dst, htab);

    /* Compute total encoded size of sequence headers. */
    uint32_t seq_hdr_size = 0;
    for (uint32_t i = 0; i < seq_count; i++) {
        seq_hdr_size += lz2_seq_encoded_size(seqs[i].lit_len,
                                              seqs[i].match_len - LZ2_MIN_MATCH);
    }

    uint32_t total = LZ2_HEADER_SIZE + seq_hdr_size + lit_total;

    if (total >= src_size) {
        /* Incompressible — fall back to literal-only stream. */
        lz2_free(dst, seqs);
        lz2_free(dst, lit_buf);
        return lz2_encode_literal_only(src, src_size, dst);
    }

    tdc_status st = tdc_buf_reserve(dst, total);
    if (st != TDC_OK) {
        lz2_free(dst, seqs);
        lz2_free(dst, lit_buf);
        return st;
    }

    uint8_t *p = dst->data;
    memcpy(p, &seq_count, 4); p += 4;
    memcpy(p, &lit_total, 4); p += 4;

    /* Write packed sequence headers. */
    for (uint32_t i = 0; i < seq_count; i++) {
        uint32_t ll = seqs[i].lit_len;
        uint32_t ml_m3 = seqs[i].match_len - LZ2_MIN_MATCH;
        uint32_t L = ll < 15 ? ll : 15;
        uint32_t M = ml_m3 < 15 ? ml_m3 : 15;

        *p++ = (uint8_t)((L << 4) | M);

        /* Extended literal length */
        if (ll >= 15) {
            uint32_t extra = ll - 15;
            while (extra >= 255) {
                *p++ = 255;
                extra -= 255;
            }
            *p++ = (uint8_t)extra;
        }

        /* Extended match length */
        if (ml_m3 >= 15) {
            *p++ = (uint8_t)(ml_m3 - 15);
        }

        /* Offset */
        uint16_t off_m1 = (uint16_t)(seqs[i].match_off - 1);
        memcpy(p, &off_m1, 2); p += 2;
    }

    /* Literal data */
    memcpy(p, lit_buf, lit_total);

    lz2_free(dst, seqs);
    lz2_free(dst, lit_buf);

    dst->size = total;
    return TDC_OK;
}

/* ----- Decoder ----------------------------------------------------------- */
/*
 * Note on batched parse-then-copy: this loop interleaves sequence parsing
 * with literal/match copies on purpose. A batched variant (parse 4-16
 * sequences ahead into a small stack array, then run a pure copy loop
 * over the parsed descriptors) was tried in vectra and measured ~6-7%
 * slower at every batch size. The reason is structural: LZ2's parse is
 * just a tag byte + occasional varint extension + 2-byte offset — so
 * cheap that out-of-order execution already overlaps the next sequence's
 * parse with the current sequence's wildcopy. Forcing a phase split
 * serializes parse and copy, costing the OoO overlap without recovering
 * anything because the parse phase has nothing expensive enough to
 * benefit from a dedicated decode pipeline.
 *
 * Revisit only once a real entropy stage (FSE/Huffman) lives in front
 * of LZ2: at that point parse becomes expensive enough to benefit from
 * being decoupled from copy.
 */
static inline void lz2_decode_fast(
    uint8_t *dst, const uint8_t *lit_data,
    const uint8_t *seq_ptr, uint32_t n_seq,
    uint32_t uncompressed_size, uint32_t literals_size,
    const uint8_t **seq_ptr_out, uint32_t *dp_out, uint32_t *lp_out,
    uint32_t *si_out)
{
    uint32_t dp = *dp_out, lp = *lp_out, si = *si_out;
    const uint8_t *sp = seq_ptr;

    while (LZ2_LIKELY(si < n_seq)) {
        /* Save position before parsing (for rewind on bail). */
        const uint8_t *sp_save = sp;

        /* Parse packed [LLLLMMMM] tag byte: high nibble = literal-run length
         * (or 15 = extended via varint), low nibble = match_len - 3 (or 15
         * = extended via single extra byte). */
        uint8_t tag = *sp++;
        uint32_t lit_len = tag >> 4;
        uint32_t match_len_m3 = tag & 0x0F;

        if (lit_len == 15) {
            uint8_t extra;
            do { extra = *sp++; lit_len += extra; } while (extra == 255);
        }
        if (match_len_m3 == 15) {
            match_len_m3 += *sp++;
        }
        uint32_t mlen = match_len_m3 + LZ2_MIN_MATCH;

        uint16_t off_m1;
        memcpy(&off_m1, sp, 2); sp += 2;
        uint32_t off = (uint32_t)off_m1 + 1;

        /* Bail to safe path if this sequence would exceed bounds. The +15
         * margin accounts for the unconditional 16-byte wildcopy below. */
        if (LZ2_UNLIKELY(dp + lit_len + mlen + 15 > uncompressed_size)) {
            sp = sp_save; /* rewind — safe path will re-parse */
            break;
        }
        if (LZ2_UNLIKELY(lp + lit_len > literals_size)) {
            sp = sp_save;
            break;
        }

        /* Copy literals — exact memcpy (no source overread). */
        if (LZ2_LIKELY(lit_len > 0)) {
            memcpy(dst + dp, lit_data + lp, lit_len);
            lp += lit_len;
            dp += lit_len;
        }

        /* Copy match — wildcopy is safe because the bounds check above
         * reserved 15 bytes of slack at dst. */
        {
            uint8_t *op = dst + dp;
            const uint8_t *match = op - off;

            if (LZ2_LIKELY(off >= 16)) {
                lz2_wildcopy16(op, match, mlen);
            } else if (off >= 8) {
                uint8_t *oend = op + mlen;
                do {
                    memcpy(op, match, 8);
                    op += 8;
                    match += 8;
                } while (op < oend);
            } else {
                /* Overlapping copy: fill the first `off` bytes byte-wise,
                 * then double the live region until we have at least 16
                 * bytes, after which we can use 16-byte unconditional
                 * copies. */
                for (uint32_t k = 0; k < off; k++)
                    op[k] = match[k];
                uint8_t *fill = op + off;
                uint8_t *fend = op + mlen;
                uint32_t filled = off;
                while (filled < 16 && fill < fend) {
                    uint32_t chunk = filled;
                    if (fill + chunk > fend) chunk = (uint32_t)(fend - fill);
                    memcpy(fill, op, chunk);
                    fill += chunk;
                    filled += chunk;
                }
                while (fill < fend) {
                    memcpy(fill, fill - filled, 16);
                    fill += 16;
                }
            }
            dp += mlen;
        }

        si++;
    }

    *seq_ptr_out = sp;
    *dp_out = dp;
    *lp_out = lp;
    *si_out = si;
}

/* Safe decode: full bounds checking, byte-accurate copies. Picks up where
 * the fast path bailed (or runs the whole decode if the fast path declined
 * the input). Returns TDC_E_CORRUPT on invalid back-references. */
static tdc_status lz2_decode_safe(
    uint8_t *dst, const uint8_t *lit_data,
    const uint8_t *seq_ptr, uint32_t n_seq,
    uint32_t uncompressed_size, uint32_t literals_size,
    uint32_t *dp_out, uint32_t *lp_out, uint32_t *si_out)
{
    uint32_t dp = *dp_out, lp = *lp_out, si = *si_out;
    const uint8_t *sp = seq_ptr;

    while (si < n_seq && dp < uncompressed_size) {
        uint8_t tag = *sp++;
        uint32_t lit_len = tag >> 4;
        uint32_t match_len_m3 = tag & 0x0F;

        if (lit_len == 15) {
            uint8_t extra;
            do { extra = *sp++; lit_len += extra; } while (extra == 255);
        }
        if (match_len_m3 == 15) {
            match_len_m3 += *sp++;
        }
        uint32_t mlen = match_len_m3 + LZ2_MIN_MATCH;

        uint16_t off_m1;
        memcpy(&off_m1, sp, 2); sp += 2;
        uint32_t off = (uint32_t)off_m1 + 1;

        /* Copy literals (clamped) */
        if (lit_len > 0) {
            uint32_t ll = lit_len;
            if (ll > uncompressed_size - dp) ll = uncompressed_size - dp;
            if (lp + ll > literals_size) ll = literals_size - lp;
            memcpy(dst + dp, lit_data + lp, ll);
            lp += ll;
            dp += ll;
        }

        /* Copy match */
        if (dp < uncompressed_size) {
            if (dp < off) return TDC_E_CORRUPT; /* invalid back-reference */
            if (mlen > uncompressed_size - dp) mlen = uncompressed_size - dp;
            if (off >= mlen) {
                memcpy(dst + dp, dst + dp - off, mlen);
            } else {
                uint32_t remaining = mlen;
                uint32_t wdp = dp;
                while (remaining > 0) {
                    uint32_t chunk = remaining < off ? remaining : off;
                    memcpy(dst + wdp, dst + wdp - off, chunk);
                    wdp += chunk;
                    remaining -= chunk;
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

static tdc_status lz2_decode_core(uint8_t *dst, uint32_t uncompressed_size,
                                  const uint8_t *src, uint32_t src_size) {
    if (src_size < LZ2_HEADER_SIZE) return TDC_E_CORRUPT;

    uint32_t n_seq, literals_size;
    memcpy(&n_seq,         src,     4);
    memcpy(&literals_size, src + 4, 4);

    /* Literals follow sequence headers; we don't know seq_hdr_size from the
     * header alone, so we derive it: total - 8 - literals = seq_hdr_size. */
    if ((size_t)LZ2_HEADER_SIZE + literals_size > src_size) return TDC_E_CORRUPT;
    uint32_t seq_hdr_size = src_size - LZ2_HEADER_SIZE - literals_size;

    const uint8_t *seq_start = src + LZ2_HEADER_SIZE;
    const uint8_t *lit_data  = seq_start + seq_hdr_size;

    uint32_t si = 0, dp = 0, lp = 0;
    const uint8_t *seq_ptr = seq_start;

    /* Fast path for the bulk of the data. */
    if (n_seq > 0) {
        lz2_decode_fast(dst, lit_data, seq_ptr, n_seq,
                        uncompressed_size, literals_size,
                        &seq_ptr, &dp, &lp, &si);
    }

    /* Safe tail: bounds-checked, handles remaining sequences + trailing
     * literals. Also runs in full when n_seq == 0 (literal-only fallback). */
    tdc_status st = lz2_decode_safe(dst, lit_data, seq_ptr, n_seq,
                                    uncompressed_size, literals_size,
                                    &dp, &lp, &si);
    if (st != TDC_OK) return st;

    if (dp != uncompressed_size) return TDC_E_CORRUPT;
    return TDC_OK;
}

/* ----- vtable wiring ----------------------------------------------------- */

static size_t lz2_encode_bound(size_t src_size) {
    /* Worst case: literal-only stream = 8-byte header + raw bytes. */
    return src_size + LZ2_HEADER_SIZE;
}

static tdc_status lz2_encode(const uint8_t *src, size_t src_size,
                             const void *params, tdc_buffer *dst) {
    (void)params; /* level is currently unused; greedy matcher only */
    if (!dst || !dst->realloc_fn) return TDC_E_INVAL;
    if (src_size > UINT32_MAX) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;
    return lz2_encode_core(src, (uint32_t)src_size, dst);
}

static tdc_status lz2_decode(const uint8_t *src, size_t src_size,
                             uint8_t *dst, size_t dst_size) {
    if (src_size > UINT32_MAX || dst_size > UINT32_MAX) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;
    if (dst_size > 0 && !dst) return TDC_E_INVAL;
    return lz2_decode_core(dst, (uint32_t)dst_size,
                           src, (uint32_t)src_size);
}

const tdc_entropy_vt tdc_entropy_lz2_vt = {
    .id           = TDC_ENTROPY_LZ2,
    .name         = "lz2",
    .encode_bound = lz2_encode_bound,
    .encode       = lz2_encode,
    .decode       = lz2_decode,
};
