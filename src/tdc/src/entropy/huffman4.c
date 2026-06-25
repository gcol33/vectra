/*
 * src/entropy/huffman4.c
 *
 * TDC_ENTROPY_HUFFMAN4 — 4-stream interleaved canonical Huffman coder.
 * Shares the canonical-Huffman tree builder, histogram, bit writer, and
 * bit reader with the single-stream coder via huffman_internal.h. The
 * small-input path (src_size < HUF4_SMALL_THRESH) delegates to the
 * single-stream huffman_encode/decode entry points.
 */

#include "huffman_internal.h"
#include "entropy_internal.h"
#include "../format/metadata_internal.h"

/* ======================================================================== */
/* 4-stream Huffman (TDC_ENTROPY_HUFFMAN4)                                  */
/*                                                                          */
/* Same canonical Huffman tree but the payload is split into 4 independent  */
/* bitstreams.  The decoder interleaves 4 huf_br readers, giving the CPU   */
/* 4 independent decode chains to schedule in parallel — the same trick    */
/* that makes zstd's Huffman decoder fast.                                  */
/*                                                                          */
/* On-disk layout (after the shared code-lengths header):                   */
/*   u32 stream_bytes[3]   compressed size of streams 0-2 (stream 3 is     */
/*                         the remainder of the payload)                    */
/*   u8[] stream0 .. stream3   MSB-first bitstreams                        */
/*                                                                          */
/* For src_size < 256 the encoder/decoder delegate to single-stream         */
/* HUFFMAN (overhead of 4 streams not worth it on tiny blocks).            */
/* ======================================================================== */

#define HUF4_STREAM_HDR 12u   /* 3 x u32 stream sizes */
#define HUF4_SMALL_THRESH 256u

static size_t huffman4_encode_bound(size_t src_size) {
    return (size_t)HUFFMAN_HDR_MAX + HUF4_STREAM_HDR +
           src_size + (src_size / 2u) + 32u;
}

static tdc_status huffman4_encode(const uint8_t *src, size_t src_size,
                                  const void    *params,
                                  tdc_buffer    *dst) {
    (void)params;
    if (!dst || !dst->realloc_fn) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;
    if (src_size > 0xFFFFFFFFu) return TDC_E_INVAL;

    /* Small input: delegate to single-stream. */
    if (src_size < HUF4_SMALL_THRESH)
        return huffman_encode(src, src_size, params, dst);

    size_t bound = huffman4_encode_bound(src_size);
    tdc_status st = tdc_buf_reserve(dst, bound);
    if (st != TDC_OK) return st;

    /* Shared Huffman tree across all 4 streams. */
    uint32_t freq[HUFFMAN_NSYMS];
    huffman_count_freq(src, src_size, freq);

    uint8_t  lens[HUFFMAN_NSYMS];
    uint16_t codes[HUFFMAN_NSYMS];
    (void)huffman_compute_lengths_limited(freq, lens);
    huffman_build_canonical(lens, codes);

    uint16_t n_lengths = 0;
    for (int s = HUFFMAN_NSYMS - 1; s >= 0; --s) {
        if (lens[s] != 0) { n_lengths = (uint16_t)(s + 1); break; }
    }
    uint32_t hdr_size = HUFFMAN_HDR_PREFIX + (uint32_t)n_lengths;

    uint8_t *out = dst->data;
    tdc_le_store_u32(out + 0, (uint32_t)src_size);
    tdc_le_store_u32(out + 4, 0u);  /* unused for 4-stream */
    tdc_le_store_u16(out + 8, n_lengths);
    if (n_lengths > 0) memcpy(out + HUFFMAN_HDR_PREFIX, lens, n_lengths);

    uint32_t enc[HUFFMAN_NSYMS];
    for (int s = 0; s < HUFFMAN_NSYMS; ++s)
        enc[s] = ((uint32_t)codes[s] << 8) | (uint32_t)lens[s];

    /* Split source into 4 chunks. */
    size_t chunk = (src_size + 3u) / 4u;
    size_t off[5];
    off[0] = 0;
    off[1] = chunk;
    off[2] = 2u * chunk;
    off[3] = 3u * chunk;
    off[4] = src_size;
    for (int j = 1; j < 4; ++j)
        if (off[j] > src_size) off[j] = src_size;

    uint8_t *ss_ptr      = out + hdr_size;
    uint8_t *payload_ptr = ss_ptr + HUF4_STREAM_HDR;

    /* Encode each chunk with its own bit_writer. */
    uint32_t stream_bytes[4];
    size_t write_pos = 0;
    for (int si = 0; si < 4; ++si) {
        size_t begin = off[si], end_off = off[si + 1];
        size_t count = end_off - begin;
        if (count == 0) { stream_bytes[si] = 0; continue; }

        bit_writer bw;
        bw_init(&bw, payload_ptr + write_pos);

        size_t k = begin;
        size_t n4 = begin + (count & ~(size_t)3);
        for (; k < n4; k += 4) {
            uint32_t e0 = enc[src[k + 0]];
            uint32_t e1 = enc[src[k + 1]];
            uint32_t e2 = enc[src[k + 2]];
            uint32_t e3 = enc[src[k + 3]];
            bw_write_bits(&bw, (uint16_t)(e0 >> 8), (int)(e0 & 0xFF));
            bw_write_bits(&bw, (uint16_t)(e1 >> 8), (int)(e1 & 0xFF));
            bw_write_bits(&bw, (uint16_t)(e2 >> 8), (int)(e2 & 0xFF));
            bw_write_bits(&bw, (uint16_t)(e3 >> 8), (int)(e3 & 0xFF));
        }
        for (; k < end_off; ++k) {
            uint32_t e = enc[src[k]];
            bw_write_bits(&bw, (uint16_t)(e >> 8), (int)(e & 0xFF));
        }
        bw_flush(&bw);
        stream_bytes[si] = (uint32_t)bw.used;
        write_pos += bw.used;
    }

    tdc_le_store_u32(ss_ptr + 0, stream_bytes[0]);
    tdc_le_store_u32(ss_ptr + 4, stream_bytes[1]);
    tdc_le_store_u32(ss_ptr + 8, stream_bytes[2]);

    dst->size = (size_t)(payload_ptr - out) + write_pos;
    return TDC_OK;
}

/* ----- 4-stream decode helper: build fast table into caller's array ------ */

static void huf4_build_fast_table(const uint8_t  *lens,
                                  const uint16_t *bl_count,
                                  const uint16_t *firstsym,
                                  const uint8_t  *sorted_syms,
                                  uint16_t        fast[HUFFMAN_FAST_SIZE]) {
    memset(fast, 0, HUFFMAN_FAST_SIZE * sizeof(uint16_t));
    uint16_t code = 0;
    for (int L = 1; L <= HUFFMAN_FAST_BITS; ++L) {
        uint32_t shift = (uint32_t)(HUFFMAN_FAST_BITS - L);
        uint32_t span  = 1u << shift;
        for (uint16_t i = 0; i < bl_count[L]; ++i) {
            uint8_t  sym   = sorted_syms[firstsym[L] + i];
            uint16_t entry = (uint16_t)(((uint16_t)sym << 8) | (uint16_t)L);
            uint32_t start = (uint32_t)code << shift;
            uint32_t end   = start + span;
            for (uint32_t j = start; j < end; ++j) fast[j] = entry;
            code++;
        }
        code = (uint16_t)(code << 1);
    }
    (void)lens;
}

static tdc_status huffman4_decode(const uint8_t *src, size_t src_size,
                                  uint8_t       *dst, size_t dst_size) {
    if (src_size < HUFFMAN_HDR_PREFIX) return TDC_E_CORRUPT;
    if (dst_size > 0 && !dst) return TDC_E_INVAL;

    uint32_t hdr_src_size = tdc_le_load_u32(src + 0);
    uint16_t n_lengths    = tdc_le_load_u16(src + 8);
    if ((size_t)hdr_src_size != dst_size) return TDC_E_CORRUPT;
    if (n_lengths > HUFFMAN_NSYMS) return TDC_E_CORRUPT;

    /* Small input: single-stream fallback (encoder wrote single-stream). */
    if (dst_size < HUF4_SMALL_THRESH)
        return huffman_decode(src, src_size, dst, dst_size);

    uint32_t hdr_size = HUFFMAN_HDR_PREFIX + (uint32_t)n_lengths;
    if (src_size < (size_t)hdr_size + HUF4_STREAM_HDR) return TDC_E_CORRUPT;

    uint8_t lens_buf[HUFFMAN_NSYMS];
    memset(lens_buf, 0, sizeof lens_buf);
    if (n_lengths > 0) memcpy(lens_buf, src + HUFFMAN_HDR_PREFIX, n_lengths);

    if (dst_size == 0u) return TDC_OK;

    /* Build canonical decode tables. */
    uint16_t bl_count[HUFFMAN_MAX_LEN + 2] = {0};
    for (int s = 0; s < HUFFMAN_NSYMS; ++s) {
        if (lens_buf[s] > HUFFMAN_MAX_LEN) return TDC_E_CORRUPT;
        if (lens_buf[s] != 0) bl_count[lens_buf[s]]++;
    }

    /* Kraft inequality: reject over-full length distributions before
     * huf4_build_fast_table would otherwise write past the 2^FAST_BITS
     * fast[] bound on corrupt lens[]. See huffman.c for the full
     * explanation. */
    {
        uint32_t kraft = 0u;
        for (int L = 1; L <= HUFFMAN_MAX_LEN; ++L) {
            kraft += (uint32_t)bl_count[L] << (HUFFMAN_MAX_LEN - L);
        }
        if (kraft > (1u << HUFFMAN_MAX_LEN)) return TDC_E_CORRUPT;
    }

    uint16_t firstcode[HUFFMAN_MAX_LEN + 2] = {0};
    uint16_t firstsym [HUFFMAN_MAX_LEN + 2] = {0};
    {
        uint16_t code = 0, idx = 0;
        for (int bits = 1; bits <= HUFFMAN_MAX_LEN; ++bits) {
            code = (uint16_t)((code + bl_count[bits - 1]) << 1);
            firstcode[bits] = code;
            firstsym[bits]  = idx;
            idx = (uint16_t)(idx + bl_count[bits]);
        }
    }

    uint8_t sorted_syms[HUFFMAN_NSYMS];
    {
        uint16_t cursor[HUFFMAN_MAX_LEN + 2];
        memcpy(cursor, firstsym, sizeof cursor);
        for (int s = 0; s < HUFFMAN_NSYMS; ++s) {
            uint8_t L = lens_buf[s];
            if (L != 0) sorted_syms[cursor[L]++] = (uint8_t)s;
        }
    }

    int n_used = 0;
    for (int bits = 1; bits <= HUFFMAN_MAX_LEN; ++bits) n_used += bl_count[bits];
    if (n_used == 0) return TDC_E_CORRUPT;

    uint16_t fast[HUFFMAN_FAST_SIZE];
    huf4_build_fast_table(lens_buf, bl_count, firstsym, sorted_syms, fast);

    /* Build 2-symbol fast table: (s0 << 24) | (s1 << 16) | (total << 8) | L1.
     * total = pre-computed L0+L1 (or L0 when L1=0). Saves an add per symbol. */
    uint32_t fast2[HUFFMAN_FAST_SIZE];
    {
        for (uint32_t idx = 0; idx < HUFFMAN_FAST_SIZE; ++idx) {
            uint16_t e0 = fast[idx];
            int L0 = e0 & 0xFF;
            if (L0 == 0) { fast2[idx] = 0; continue; }
            uint8_t s0 = (uint8_t)(e0 >> 8);
            int rem = HUFFMAN_FAST_BITS - L0;
            if (rem > 0) {
                uint32_t idx2 = (idx << L0) & (HUFFMAN_FAST_SIZE - 1);
                uint16_t e1 = fast[idx2];
                int L1 = e1 & 0xFF;
                if (L1 != 0 && L1 <= rem) {
                    uint8_t s1 = (uint8_t)(e1 >> 8);
                    fast2[idx] = ((uint32_t)s0 << 24) | ((uint32_t)s1 << 16) |
                                 ((uint32_t)(L0 + L1) << 8) | (uint32_t)L1;
                    continue;
                }
            }
            fast2[idx] = ((uint32_t)s0 << 24) | ((uint32_t)L0 << 8);
        }
    }

    /* Read stream sizes. */
    const uint8_t *ss_ptr = src + hdr_size;
    uint32_t ss0 = tdc_le_load_u32(ss_ptr + 0);
    uint32_t ss1 = tdc_le_load_u32(ss_ptr + 4);
    uint32_t ss2 = tdc_le_load_u32(ss_ptr + 8);

    const uint8_t *payload = ss_ptr + HUF4_STREAM_HDR;
    size_t payload_avail = src_size - (size_t)(payload - src);
    if ((size_t)ss0 + ss1 + ss2 > payload_avail) return TDC_E_CORRUPT;
    uint32_t ss3 = (uint32_t)(payload_avail - ss0 - ss1 - ss2);

    /* Output chunk boundaries. */
    size_t chunk = (dst_size + 3u) / 4u;
    size_t n0 = (chunk < dst_size) ? chunk : dst_size;
    size_t n1 = (dst_size > chunk)   ? ((2*chunk < dst_size) ? chunk : dst_size - chunk) : 0;
    size_t n2 = (dst_size > 2*chunk) ? ((3*chunk < dst_size) ? chunk : dst_size - 2*chunk) : 0;
    size_t n3 = (dst_size > 3*chunk) ? dst_size - 3*chunk : 0;

    uint8_t *d0 = dst;
    uint8_t *d1 = dst + n0;
    uint8_t *d2 = dst + n0 + n1;
    uint8_t *d3 = dst + n0 + n1 + n2;

    /* Initialize 4 bit readers. */
    huf_br br0, br1, br2, br3;
    huf_br_init(&br0, payload,             ss0);
    huf_br_init(&br1, payload + ss0,       ss1);
    huf_br_init(&br2, payload + ss0 + ss1, ss2);
    huf_br_init(&br3, payload + ss0 + ss1 + ss2, ss3);

    /* Interleaved 4-stream decode — registers-only hot loop.
     *
     * All four huf_br states (bits/nbits/ptr) are pulled into plain local
     * variables at loop entry and written back on exit, so MSVC /O2 keeps
     * them in registers across iterations instead of reloading from the
     * struct each round. Refill is inlined to avoid struct aliasing.
     *
     * Three rounds between refills: 2x fast2 (up to 2 syms, ≤22 bits) +
     * 1x fast (1 sym, ≤11 bits) = up to 55 bits total, within the 56-bit
     * post-refill guarantee. Up to 5 symbols/stream/iteration. */
    size_t j0 = 0, j1 = 0, j2 = 0, j3 = 0;
    {
        /* Safe margin: each round writes up to 2 syms from the j cursor,
         * so j + 2 <= n must hold for each round. Use j + 5 <= n as the
         * loop guard covering all 3 rounds. */
        size_t safe0 = (n0 > 5) ? n0 - 5 : 0;
        size_t safe1 = (n1 > 5) ? n1 - 5 : 0;
        size_t safe2 = (n2 > 5) ? n2 - 5 : 0;
        size_t safe3 = (n3 > 5) ? n3 - 5 : 0;

        /* Register-promoted bitstream state for all 4 streams.
         * MSVC /O2 keeps these 12 values (bits/nbits/ptr × 4) in registers
         * across the loop instead of reloading from the huf_br struct each
         * round. `safe`/`end` are loop-invariant and stay in the struct. */
        uint64_t       bits0 = br0.bits, bits1 = br1.bits, bits2 = br2.bits, bits3 = br3.bits;
        int            nb0   = br0.nbits, nb1 = br1.nbits, nb2 = br2.nbits, nb3 = br3.nbits;
        const uint8_t *p0 = br0.ptr, *p1 = br1.ptr, *p2 = br2.ptr, *p3 = br3.ptr;
        const uint8_t *s0 = br0.safe, *s1 = br1.safe, *s2 = br2.safe, *s3 = br3.safe;

        /* Refill macro: fast-path only (ptr < safe guarded by the while
         * condition). Operates on locals — no struct aliasing, so MSVC
         * keeps bits/nbits/ptr in registers. */
#define HUF_REFILL_LOCAL(BITS, NB, PTR) do {                               \
            uint64_t _next;                                                \
            memcpy(&_next, (PTR), 8);                                      \
            _next = huf_bswap64(_next);                                    \
            (BITS) |= _next >> (NB);                                       \
            unsigned _fresh = (unsigned)(64 - (NB)) >> 3;                  \
            (PTR) += _fresh;                                               \
            (NB)  += (int)(_fresh << 3);                                   \
        } while (0)

        while (j0 < safe0 && j1 < safe1 && j2 < safe2 && j3 < safe3 &&
               p0 < s0 && p1 < s1 && p2 < s2 && p3 < s3) {
            HUF_REFILL_LOCAL(bits0, nb0, p0);
            HUF_REFILL_LOCAL(bits1, nb1, p1);
            HUF_REFILL_LOCAL(bits2, nb2, p2);
            HUF_REFILL_LOCAL(bits3, nb3, p3);

            /* Round 1: fast2 lookup, 1 or 2 symbols per stream. */
            {
                uint32_t pk0 = (uint32_t)(bits0 >> (64 - HUFFMAN_FAST_BITS));
                uint32_t pk1 = (uint32_t)(bits1 >> (64 - HUFFMAN_FAST_BITS));
                uint32_t pk2 = (uint32_t)(bits2 >> (64 - HUFFMAN_FAST_BITS));
                uint32_t pk3 = (uint32_t)(bits3 >> (64 - HUFFMAN_FAST_BITS));
                uint32_t e0 = fast2[pk0], e1 = fast2[pk1];
                uint32_t e2 = fast2[pk2], e3 = fast2[pk3];
                int tb0 = (int)((e0 >> 8) & 0xFF);
                int tb1 = (int)((e1 >> 8) & 0xFF);
                int tb2 = (int)((e2 >> 8) & 0xFF);
                int tb3 = (int)((e3 >> 8) & 0xFF);
                if (tb0 == 0 || tb1 == 0 || tb2 == 0 || tb3 == 0) break;
                d0[j0] = (uint8_t)(e0 >> 24); d0[j0 + 1] = (uint8_t)(e0 >> 16);
                j0 += 1u + ((e0 & 0xFFu) != 0);
                bits0 <<= tb0; nb0 -= tb0;
                d1[j1] = (uint8_t)(e1 >> 24); d1[j1 + 1] = (uint8_t)(e1 >> 16);
                j1 += 1u + ((e1 & 0xFFu) != 0);
                bits1 <<= tb1; nb1 -= tb1;
                d2[j2] = (uint8_t)(e2 >> 24); d2[j2 + 1] = (uint8_t)(e2 >> 16);
                j2 += 1u + ((e2 & 0xFFu) != 0);
                bits2 <<= tb2; nb2 -= tb2;
                d3[j3] = (uint8_t)(e3 >> 24); d3[j3 + 1] = (uint8_t)(e3 >> 16);
                j3 += 1u + ((e3 & 0xFFu) != 0);
                bits3 <<= tb3; nb3 -= tb3;
            }
            /* Round 2: fast2 again. */
            {
                uint32_t pk0 = (uint32_t)(bits0 >> (64 - HUFFMAN_FAST_BITS));
                uint32_t pk1 = (uint32_t)(bits1 >> (64 - HUFFMAN_FAST_BITS));
                uint32_t pk2 = (uint32_t)(bits2 >> (64 - HUFFMAN_FAST_BITS));
                uint32_t pk3 = (uint32_t)(bits3 >> (64 - HUFFMAN_FAST_BITS));
                uint32_t e0 = fast2[pk0], e1 = fast2[pk1];
                uint32_t e2 = fast2[pk2], e3 = fast2[pk3];
                int tb0 = (int)((e0 >> 8) & 0xFF);
                int tb1 = (int)((e1 >> 8) & 0xFF);
                int tb2 = (int)((e2 >> 8) & 0xFF);
                int tb3 = (int)((e3 >> 8) & 0xFF);
                if (tb0 == 0 || tb1 == 0 || tb2 == 0 || tb3 == 0) break;
                d0[j0] = (uint8_t)(e0 >> 24); d0[j0 + 1] = (uint8_t)(e0 >> 16);
                j0 += 1u + ((e0 & 0xFFu) != 0);
                bits0 <<= tb0; nb0 -= tb0;
                d1[j1] = (uint8_t)(e1 >> 24); d1[j1 + 1] = (uint8_t)(e1 >> 16);
                j1 += 1u + ((e1 & 0xFFu) != 0);
                bits1 <<= tb1; nb1 -= tb1;
                d2[j2] = (uint8_t)(e2 >> 24); d2[j2 + 1] = (uint8_t)(e2 >> 16);
                j2 += 1u + ((e2 & 0xFFu) != 0);
                bits2 <<= tb2; nb2 -= tb2;
                d3[j3] = (uint8_t)(e3 >> 24); d3[j3 + 1] = (uint8_t)(e3 >> 16);
                j3 += 1u + ((e3 & 0xFFu) != 0);
                bits3 <<= tb3; nb3 -= tb3;
            }
            /* Round 3: single-sym fast. */
            {
                uint32_t pk0 = (uint32_t)(bits0 >> (64 - HUFFMAN_FAST_BITS));
                uint32_t pk1 = (uint32_t)(bits1 >> (64 - HUFFMAN_FAST_BITS));
                uint32_t pk2 = (uint32_t)(bits2 >> (64 - HUFFMAN_FAST_BITS));
                uint32_t pk3 = (uint32_t)(bits3 >> (64 - HUFFMAN_FAST_BITS));
                uint16_t fe0 = fast[pk0], fe1 = fast[pk1];
                uint16_t fe2 = fast[pk2], fe3 = fast[pk3];
                int fl0 = fe0 & 0xFF, fl1 = fe1 & 0xFF;
                int fl2 = fe2 & 0xFF, fl3 = fe3 & 0xFF;
                if (fl0 == 0 || fl1 == 0 || fl2 == 0 || fl3 == 0) break;
                d0[j0++] = (uint8_t)(fe0 >> 8); bits0 <<= fl0; nb0 -= fl0;
                d1[j1++] = (uint8_t)(fe1 >> 8); bits1 <<= fl1; nb1 -= fl1;
                d2[j2++] = (uint8_t)(fe2 >> 8); bits2 <<= fl2; nb2 -= fl2;
                d3[j3++] = (uint8_t)(fe3 >> 8); bits3 <<= fl3; nb3 -= fl3;
            }
        }

#undef HUF_REFILL_LOCAL

        /* Write locals back into br0..br3 so the sequential-tail block
         * picks up from the exact post-hot-loop bit position. */
        br0.bits = bits0; br0.nbits = nb0; br0.ptr = p0;
        br1.bits = bits1; br1.nbits = nb1; br1.ptr = p1;
        br2.bits = bits2; br2.nbits = nb2; br2.ptr = p2;
        br3.bits = bits3; br3.nbits = nb3; br3.ptr = p3;
    }

    /* Per-stream sequential tail: finish remaining symbols from each
     * stream's current position (j0..j3).  Uses fast2 + slow path. */
    {
        huf_br  *brs[4] = { &br0, &br1, &br2, &br3 };
        uint8_t *ds[4]  = { d0, d1, d2, d3 };
        size_t   ns[4]  = { n0, n1, n2, n3 };
        size_t   js[4]  = { j0, j1, j2, j3 };

        for (int si = 0; si < 4; ++si) {
            huf_br  *b = brs[si];
            uint8_t *d = ds[si];
            size_t   n = ns[si];
            size_t   j = js[si];
            size_t   safe_end = (n > 0) ? n - 1 : 0;
            for (; j < safe_end; ) {
                if (b->nbits < HUFFMAN_FAST_BITS) huf_br_refill(b);
                if (b->nbits >= HUFFMAN_FAST_BITS) {
                    uint32_t idx   = (uint32_t)(b->bits >> (64 - HUFFMAN_FAST_BITS));
                    uint32_t entry = fast2[idx];
                    int      tb    = (int)((entry >> 8) & 0xFF);
                    if (entry & 0xFF) {
                        d[j]     = (uint8_t)(entry >> 24);
                        d[j + 1] = (uint8_t)(entry >> 16);
                        b->bits <<= tb;
                        b->nbits -= tb;
                        j += 2;
                        continue;
                    }
                    if (tb) {
                        d[j]     = (uint8_t)(entry >> 24);
                        b->bits <<= tb;
                        b->nbits -= tb;
                        j += 1;
                        continue;
                    }
                }
                {
                    uint16_t code = 0;
                    int L;
                    for (L = 1; L <= HUFFMAN_MAX_LEN; ++L) {
                        if (b->nbits < 1) huf_br_refill(b);
                        code = (uint16_t)((code << 1) | (uint16_t)(b->bits >> 63));
                        b->bits <<= 1;
                        b->nbits -= 1;
                        if (bl_count[L] != 0u &&
                            code < (uint16_t)(firstcode[L] + bl_count[L])) {
                            d[j] = sorted_syms[firstsym[L] + (code - firstcode[L])];
                            break;
                        }
                    }
                    if (L > HUFFMAN_MAX_LEN) return TDC_E_CORRUPT;
                    j += 1;
                }
            }
            for (; j < n; ++j) {
                if (b->nbits < HUFFMAN_FAST_BITS) huf_br_refill(b);
                if (b->nbits >= HUFFMAN_FAST_BITS) {
                    uint32_t idx   = (uint32_t)(b->bits >> (64 - HUFFMAN_FAST_BITS));
                    uint16_t entry = fast[idx];
                    int      L     = entry & 0xFF;
                    if (L != 0) {
                        d[j]      = (uint8_t)(entry >> 8);
                        b->bits <<= L;
                        b->nbits -= L;
                        continue;
                    }
                }
                uint16_t code = 0;
                int L;
                for (L = 1; L <= HUFFMAN_MAX_LEN; ++L) {
                    if (b->nbits < 1) huf_br_refill(b);
                    code = (uint16_t)((code << 1) | (uint16_t)(b->bits >> 63));
                    b->bits <<= 1;
                    b->nbits -= 1;
                    if (bl_count[L] != 0u &&
                        code < (uint16_t)(firstcode[L] + bl_count[L])) {
                        d[j] = sorted_syms[firstsym[L] + (code - firstcode[L])];
                        break;
                    }
                }
                if (L > HUFFMAN_MAX_LEN) return TDC_E_CORRUPT;
            }
        }
    }

    return TDC_OK;
}

const tdc_entropy_vt tdc_entropy_huffman4_vt = {
    .id           = TDC_ENTROPY_HUFFMAN4,
    .name         = "huffman4",
    .encode_bound = huffman4_encode_bound,
    .encode       = huffman4_encode,
    .decode       = huffman4_decode,
};
