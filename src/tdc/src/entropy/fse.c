/*
 * src/entropy/fse.c
 *
 * TDC_ENTROPY_FSE — native rANS entropy coder with adaptive precision.
 *
 * "FSE" in tdc names the family, not the specific table-coded variant
 * Yann Collet popularized. The coder shipped here is rANS (range
 * Asymmetric Numeral Systems), the range variant of Jarek Duda's ANS
 * family that tANS is the table variant of. Both target the same
 * compression ratio (within 0.1% of Shannon entropy on iid sources)
 * and are typically used in the same downstream slot. rANS was chosen
 * for v0 because:
 *
 *   - the encode/decode loops are short and easy to verify against the
 *     algorithm definition (one mod, one div, one shift per symbol),
 *   - there is no spread-table construction or per-symbol state-bit
 *     bookkeeping (which is the part of tANS that historically hides
 *     bugs),
 *   - the runtime ratio is identical to tANS for the residual streams
 *     tdc cares about.
 *
 * Adaptive precision: TABLE_LOG is chosen per-block based on the number
 * of active symbols. Small alphabets (e.g. 24-symbol LZ offset codes)
 * use TABLE_LOG=7 (128 states); full byte alphabets use TABLE_LOG=10
 * (1024 states). This keeps quantization loss < 0.5% of Shannon entropy
 * while minimizing header and decode-table overhead.
 *
 * Algorithm:
 *
 *   Constants (per block)
 *     TABLE_LOG  = 5..12        (chosen at encode time, stored in header)
 *     TABLE_SIZE = 1 << TABLE_LOG
 *     L_MIN      = 1 << 16         = 65536  (renorm boundary; state stays
 *                                            in [L_MIN, L_MIN << 8))
 *
 *   Per-symbol state update (encode, run on input from end to start):
 *     while state >= ((L_MIN >> TABLE_LOG) << 8) * norm[s]:
 *         emit  state & 0xFF
 *         state >>= 8
 *     state = (state / norm[s]) << TABLE_LOG
 *           + (state % norm[s])
 *           + cum[s]
 *
 *   Per-symbol state update (decode, run on output from start to end):
 *     slot   = state & (TABLE_SIZE - 1)
 *     symbol = slot_to_sym[slot]
 *     state  = norm[s] * (state >> TABLE_LOG) + slot - cum[s]
 *     while state < L_MIN:
 *         state = (state << 8) | next_byte
 *
 *   The encoder pushes bytes "low byte first" while reducing state, so
 *   the on-disk byte stream IS reversed before being written. The
 *   decoder then reads the stream forward and pulls bytes high-byte-
 *   first via the standard rANS refill loop.
 *
 * On-disk payload (compact self-contained block):
 *
 *     u32   src_size            (uncompressed bytes; cross-check vs caller)
 *     u32   payload_size        (number of bytes in the byte stream)
 *     u32   final_state         (encoder state at end of input)
 *     u8    table_log           (TABLE_LOG used for this block, 5..12)
 *     u8    reserved            (zero)
 *     u16   max_symbol          (highest symbol index + 1; 0 for empty)
 *     u16   norm[max_symbol]    (normalized PMF; sum exactly TABLE_SIZE)
 *     u8[]  payload             payload_size bytes, in decode (read) order
 *
 *   Header size: 16 + 2 * max_symbol bytes.
 *   For 24-symbol alphabet: 64 bytes. For 256-symbol: 528 bytes.
 *
 * Edge cases:
 *   - Empty input: src_size == 0, payload_size == 0, final_state == L_MIN,
 *     max_symbol == 0, table_log == FSE_TABLE_LOG_MIN. No norm[] entries.
 *   - Single distinct symbol s: norm[s] == TABLE_SIZE, all others zero.
 *     The encode update is the identity (state unchanged), no bytes are
 *     emitted, payload_size == 0, final_state == L_MIN. The decoder
 *     reads slot = state & (TABLE_SIZE - 1) -> s every iteration.
 */

#include "tdc/entropy.h"
#include "entropy_internal.h"
#include "fse_internal.h"
#include "../core/buffer.h"
#include "../format/metadata_internal.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define FSE_NSYMS          TDC_FSE_NSYMS
#define FSE_TABLE_LOG_MIN  TDC_FSE_TABLE_LOG_MIN
#define FSE_TABLE_LOG_MAX  TDC_FSE_TABLE_LOG_MAX
#define FSE_TABLE_SIZE_MAX TDC_FSE_TABLE_SIZE_MAX
#define FSE_L_MIN          TDC_FSE_L_MIN
#define FSE_HDR_PREFIX     TDC_FSE_HDR_PREFIX

typedef tdc_fse_fat_decode_entry fse_fat_decode_entry;
typedef tdc_fse_fat_decode_table fse_fat_decode_table;

/* ----- Adaptive table_log ------------------------------------------------- */
/*
 * Pick TABLE_LOG based on the number of distinct symbols in the input.
 * Goal: enough precision to represent the PMF accurately (quantization
 * loss < 0.5% of Shannon entropy) while keeping the decode table and
 * header small. TABLE_SIZE should be at least 4x the active alphabet
 * to give even rare symbols a reasonable count.
 */
static uint32_t fse_pick_table_log(uint32_t n_active) {
    if (n_active <= 2)   return 5;   /*   32 states */
    if (n_active <= 8)   return 6;   /*   64 states */
    if (n_active <= 32)  return 7;   /*  128 states */
    if (n_active <= 64)  return 8;   /*  256 states */
    if (n_active <= 128) return 9;   /*  512 states */
    if (n_active <= 200) return 10;  /* 1024 states */
    return 11;                        /* 2048 states — 8 counts/symbol for
                                       256-symbol alphabets. Fat decode
                                       table is 16 KiB (fits in L1d)
                                       instead of 32 KiB at TABLE_LOG=12.
                                       Quantization loss is ~1% of Shannon
                                       entropy, acceptable trade for
                                       decode speed. */
}

/* ----- Frequency normalization -------------------------------------------- */
/*
 * Quantize a histogram of total = sum(freq) into a normalized PMF whose
 * entries sum to exactly table_size. Used symbols receive at least one
 * count; unused symbols receive zero. The "Hamilton apportionment"
 * fix-up at the end never reduces a count below 1 (so the inverse
 * mapping stays well-defined for every symbol that ever appears in the
 * input).
 *
 * Edge case: total == 0 returns an all-zero PMF (caller treats this as
 * an empty input and short-circuits).
 */
static void fse_normalize(const uint32_t *freq, uint32_t n_syms,
                          uint32_t table_size,
                          uint16_t *norm) {
    uint32_t total = 0;
    for (uint32_t i = 0; i < n_syms; ++i) total += freq[i];

    for (uint32_t i = 0; i < n_syms; ++i) norm[i] = 0;
    if (total == 0u) return;

    int distributed = 0;
    for (uint32_t i = 0; i < n_syms; ++i) {
        if (freq[i] == 0u) continue;
        uint64_t prop = ((uint64_t)freq[i] * (uint64_t)table_size) / (uint64_t)total;
        if (prop == 0u) prop = 1u;
        if (prop > (uint64_t)table_size) prop = table_size;
        norm[i] = (uint16_t)prop;
        distributed += (int)prop;
    }

    int diff = (int)table_size - distributed;
    if (diff > 0) {
        /* Hand the slack to the largest-count symbol. */
        uint32_t idx = 0;
        uint32_t best = 0;
        for (uint32_t i = 0; i < n_syms; ++i) {
            if (freq[i] > best) { best = freq[i]; idx = i; }
        }
        norm[idx] = (uint16_t)((int)norm[idx] + diff);
    } else {
        /* Pull from symbols with norm > 1, biggest first, until balanced. */
        while (diff < 0) {
            int idx = -1;
            uint16_t best = 1;
            for (uint32_t i = 0; i < n_syms; ++i) {
                if (norm[i] > best) { best = norm[i]; idx = (int)i; }
            }
            if (idx < 0) break;
            norm[idx]--;
            diff++;
        }
    }
}

/* ----- Cumulative table + slot->symbol map -------------------------------- */

typedef struct {
    uint16_t cum[FSE_NSYMS + 1];
    uint16_t slot_to_sym[FSE_TABLE_SIZE_MAX];
} fse_decode_tables;

/* Fat decode entry definitions live in fse_internal.h so lz_streams.c
 * can drive the rANS state machine directly for its fused decode path. */

static tdc_status fse_build_tables(const uint16_t *norm, uint32_t n_syms,
                                   uint32_t table_size,
                                   fse_decode_tables *t) {
    uint32_t acc = 0;
    for (uint32_t s = 0; s < n_syms; ++s) {
        t->cum[s] = (uint16_t)acc;
        acc += norm[s];
    }
    /* Fill remaining cum entries up to FSE_NSYMS with the final acc
     * so that any symbol lookup beyond n_syms sees zero range. */
    for (uint32_t s = n_syms; s <= FSE_NSYMS; ++s) {
        t->cum[s] = (uint16_t)acc;
    }
    if (acc != table_size) return TDC_E_CORRUPT;

    uint32_t p = 0;
    for (uint32_t s = 0; s < n_syms; ++s) {
        for (uint32_t j = 0; j < norm[s]; ++j) {
            t->slot_to_sym[p++] = (uint16_t)s;
        }
    }
    return TDC_OK;
}

tdc_status tdc_fse_build_fat_table(const uint16_t *norm, uint32_t n_syms,
                                    uint32_t table_size,
                                    tdc_fse_fat_decode_table *ft) {
    uint16_t cum[FSE_NSYMS + 1];
    uint32_t acc = 0;
    for (uint32_t s = 0; s < n_syms; ++s) {
        cum[s] = (uint16_t)acc;
        acc += norm[s];
    }
    for (uint32_t s = n_syms; s <= FSE_NSYMS; ++s)
        cum[s] = (uint16_t)acc;
    if (acc != table_size) return TDC_E_CORRUPT;

    uint32_t p = 0;
    for (uint32_t s = 0; s < n_syms; ++s) {
        for (uint32_t j = 0; j < norm[s]; ++j) {
            ft->entries[p].symbol = (uint8_t)s;
            ft->entries[p].freq   = norm[s];
            ft->entries[p].cum    = cum[s];
            ft->entries[p].pad[0] = 0;
            ft->entries[p].pad[1] = 0;
            ft->entries[p].pad[2] = 0;
            p++;
        }
    }
    return TDC_OK;
}

/* ----- Encode ------------------------------------------------------------- */

static size_t fse_encode_bound(size_t src_size) {
    /* Header (max 526 for 256 symbols) + worst-case ~1 byte/symbol + slack. */
    return (size_t)FSE_HDR_PREFIX + 2u * FSE_NSYMS + src_size + 16u;
}

static tdc_status fse_encode_stream(const uint8_t *src, size_t src_size,
                                    const uint16_t *norm,
                                    const uint16_t *cum,
                                    uint32_t table_log,
                                    tdc_buffer *scratch,
                                    uint32_t *out_state) {
    uint32_t state     = FSE_L_MIN;
    uint32_t table_size = 1u << table_log;

    /* x_max factor: ((L_MIN >> TABLE_LOG) << 8) */
    const uint32_t x_max_factor = (FSE_L_MIN >> table_log) << 8;

    for (size_t k = src_size; k-- > 0; ) {
        uint8_t  s    = src[k];
        uint32_t freq = norm[s];
        if (freq == 0u) return TDC_E_INVAL;

        uint32_t x_max = x_max_factor * freq;
        while (state >= x_max) {
            tdc_status st = tdc_meta_write_u8(scratch, (uint8_t)(state & 0xFFu));
            if (st != TDC_OK) return st;
            state >>= 8;
        }
        state = (state / freq) * table_size + (state % freq) + (uint32_t)cum[s];
    }

    *out_state = state;
    return TDC_OK;
}

static tdc_status fse_encode(const uint8_t *src, size_t src_size,
                             const void    *params,
                             tdc_buffer    *dst) {
    (void)params;
    if (!dst || !dst->realloc_fn) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;
    if (src_size > 0xFFFFFFFFu) return TDC_E_INVAL;

    /* Count frequencies and find max active symbol. */
    uint32_t freq[FSE_NSYMS] = {0};
    for (size_t i = 0; i < src_size; ++i) freq[src[i]]++;

    uint32_t max_symbol = 0;  /* max_symbol = highest active symbol + 1 */
    for (int s = FSE_NSYMS - 1; s >= 0; --s) {
        if (freq[s] != 0u) { max_symbol = (uint32_t)(s + 1); break; }
    }

    /* Count distinct active symbols for table_log selection. */
    uint32_t n_active = 0;
    for (uint32_t i = 0; i < max_symbol; ++i) {
        if (freq[i] != 0u) n_active++;
    }

    uint32_t table_log  = (src_size == 0) ? FSE_TABLE_LOG_MIN : fse_pick_table_log(n_active);
    uint32_t table_size = 1u << table_log;

    /* Normalize frequencies. */
    uint16_t norm[FSE_NSYMS];
    fse_normalize(freq, max_symbol, table_size, norm);

    /* Build cumulative table (encode side needs cum[]). */
    fse_decode_tables t;
    tdc_status st;
    if (src_size > 0u) {
        st = fse_build_tables(norm, max_symbol, table_size, &t);
        if (st != TDC_OK) return st;
    }

    /* Encode bytes into a scratch buffer in emit order. */
    tdc_buffer scratch = {0};
    scratch.realloc_fn = dst->realloc_fn;
    scratch.user       = dst->user;

    uint32_t final_state = FSE_L_MIN;
    if (src_size > 0) {
        st = fse_encode_stream(src, src_size, norm, t.cum, table_log,
                               &scratch, &final_state);
        if (st != TDC_OK) {
            if (scratch.data) scratch.realloc_fn(scratch.user, scratch.data, 0);
            return st;
        }
    }

    /* Reserve dst and emit header + reversed payload. */
    uint32_t hdr_size = FSE_HDR_PREFIX + 2u * max_symbol;
    size_t total = (size_t)hdr_size + scratch.size;
    st = tdc_buf_reserve(dst, total);
    if (st != TDC_OK) {
        if (scratch.data) scratch.realloc_fn(scratch.user, scratch.data, 0);
        return st;
    }

    uint8_t *out = dst->data;
    tdc_le_store_u32(out + 0, (uint32_t)src_size);
    tdc_le_store_u32(out + 4, (uint32_t)scratch.size);
    tdc_le_store_u32(out + 8, final_state);
    out[12] = (uint8_t)table_log;
    out[13] = 0;  /* reserved/pad */
    tdc_le_store_u16(out + 14, (uint16_t)max_symbol);
    if (max_symbol > 0) {
        memcpy(out + FSE_HDR_PREFIX, norm, 2u * max_symbol);
    }

    /* Reverse the scratch payload into the on-disk slot. */
    uint8_t *payload_dst = out + hdr_size;
    for (size_t i = 0; i < scratch.size; ++i) {
        payload_dst[i] = scratch.data[scratch.size - 1 - i];
    }

    if (scratch.data) scratch.realloc_fn(scratch.user, scratch.data, 0);
    dst->size = total;
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status fse_decode(const uint8_t *src, size_t src_size,
                             uint8_t       *dst, size_t dst_size) {
    if (src_size < FSE_HDR_PREFIX) return TDC_E_CORRUPT;
    if (dst_size > 0 && !dst) return TDC_E_INVAL;

    uint32_t hdr_src_size     = tdc_le_load_u32(src + 0);
    uint32_t hdr_payload_size = tdc_le_load_u32(src + 4);
    uint32_t hdr_final_state  = tdc_le_load_u32(src + 8);
    uint32_t table_log        = src[12];
    /* src[13] is reserved/pad */
    uint32_t max_symbol       = tdc_le_load_u16(src + 14);

    if ((size_t)hdr_src_size != dst_size) return TDC_E_CORRUPT;
    if (table_log < FSE_TABLE_LOG_MIN || table_log > FSE_TABLE_LOG_MAX) return TDC_E_CORRUPT;
    if (max_symbol > FSE_NSYMS) return TDC_E_CORRUPT;

    uint32_t hdr_size = FSE_HDR_PREFIX + 2u * max_symbol;
    if (src_size < hdr_size) return TDC_E_CORRUPT;
    if ((size_t)hdr_payload_size + (size_t)hdr_size != src_size) return TDC_E_CORRUPT;

    uint32_t table_size = 1u << table_log;
    uint32_t table_mask = table_size - 1u;

    /* Read norm entries: only max_symbol are stored, rest are zero. */
    uint16_t norm[FSE_NSYMS];
    memset(norm, 0, sizeof norm);
    if (max_symbol > 0) {
        memcpy(norm, src + FSE_HDR_PREFIX, 2u * max_symbol);
    }

    if (dst_size == 0u) {
        if (hdr_payload_size != 0u) return TDC_E_CORRUPT;
        if (max_symbol != 0u) return TDC_E_CORRUPT;
        if (hdr_final_state != FSE_L_MIN) return TDC_E_CORRUPT;
        return TDC_OK;
    }

    /* Fat decode table: one cache line per slot instead of three separate
     * array lookups. On TABLE_LOG=12 this is 32 KiB on the stack — larger
     * than the split tables (9 KiB) but the per-symbol decode loop does
     * one load instead of three, which eliminates two L1d miss slots. */
    fse_fat_decode_table ft;
    tdc_status st = tdc_fse_build_fat_table(norm, max_symbol, table_size, &ft);
    if (st != TDC_OK) return st;

    if (hdr_final_state < FSE_L_MIN || hdr_final_state >= (FSE_L_MIN << 8)) {
        return TDC_E_CORRUPT;
    }

    const uint8_t *payload     = src + hdr_size;
    size_t         payload_off = 0;
    uint32_t       state       = hdr_final_state;

    for (size_t i = 0; i < dst_size; ++i) {
        uint32_t slot = state & table_mask;
        const fse_fat_decode_entry *e = &ft.entries[slot];
        dst[i] = e->symbol;

        state = (uint32_t)e->freq * (state >> table_log) + slot - (uint32_t)e->cum;

        while (state < FSE_L_MIN) {
            if (payload_off >= hdr_payload_size) return TDC_E_CORRUPT;
            state = (state << 8) | (uint32_t)payload[payload_off++];
        }
    }

    if (payload_off != hdr_payload_size) return TDC_E_CORRUPT;
    return TDC_OK;
}

const tdc_entropy_vt tdc_entropy_fse_vt = {
    .id           = TDC_ENTROPY_FSE,
    .name         = "fse",
    .encode_bound = fse_encode_bound,
    .encode       = fse_encode,
    .decode       = fse_decode,
};
