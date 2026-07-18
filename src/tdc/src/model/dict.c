/*
 * src/model/dict.c
 *
 * TDC_MODEL_DICT_1D — string dictionary model.
 *
 * Encodes a vector of strings as
 *   - a side-meta dictionary holding the unique strings, and
 *   - a u32 residual stream of indices into that dictionary, one entry
 *     per row.
 *
 * Accepted dtype/layout: TDC_DT_STRING + TDC_LAYOUT_VECTOR_1D.
 *
 * Side metadata layout (little-endian):
 *
 *     u32 dict_count
 *     u32 dict_total_bytes
 *     u32 dict_offsets[dict_count + 1]   (byte offsets into dict_data;
 *                                         offsets[count] == dict_total_bytes)
 *     u8  dict_data[dict_total_bytes]    (concatenated unique strings)
 *
 * Residual stream:
 *
 *     u32 indices[n_elems]               (one dictionary index per input row)
 *
 * The model ALWAYS encodes — there is no cardinality cap and no fallback
 * to a "plain" path. With every input string distinct, the dictionary
 * holds n_elems entries and the residual is the monotone sequence
 * 0,1,...,n-1; that is the worst case but still a valid round trip.
 * Downstream BYTE_SHUFFLE + LZ collapses the high zero bytes of the
 * u32 indices for the common low-cardinality case.
 *
 * Residual dtype is fixed at TDC_DT_U32 regardless of dictionary size.
 * The decoder side of the chain walk hardcodes the same value (see
 * driver_internal.h's driver_model_residual_dtype). u32 covers any
 * sane n_unique; data-dependent index width would force per-block
 * model state on disk and the savings versus a downstream entropy stage
 * are negligible.
 *
 * Validity bitmap:
 *   Treated as opaque pass-through, same convention as every other v0
 *   model. Every row contributes its string to the dictionary regardless
 *   of validity. The driver writes the bitmap to disk verbatim.
 *
 * Hash table:
 *   FNV-1a + open-addressing, 70% load factor, double on resize. Same
 *   policy as vectra/src/vtr_codec.c. NOT Robin Hood — the do-not-retry
 *   notes in tdc/CLAUDE.md document the 12-29% regression measured in
 *   the vectra benchmarks.
 *
 * Source today: vectra/src/vtr_codec.c try_dict_encode/dict_decode
 * (search VTR_ENC_DICTIONARY). The hash table loop and FNV-1a constants
 * are preserved one-to-one. Allocation, error handling, side-meta
 * layout, and the RLE wrapper are rewritten for tdc:
 *   - realloc_fn allocation everywhere (no malloc, no longjmp)
 *   - residual is plain u32 indices (vectra RLE'd because no entropy
 *     stage followed; tdc lets LZ handle runs)
 *   - cardinality cap removed (no fallback path in tdc)
 *   - side meta carries an explicit dict_total_bytes field for cheaper
 *     bounds checking on decode
 */

#include "tdc/model.h"
#include "model_internal.h"
#include "model_load_store.h"
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Acceptance bitmasks ------------------------------------------------ */

#define DICT1D_ACCEPTED_DTYPES  TDC_DT_BIT(TDC_DT_STRING)
#define DICT1D_ACCEPTED_LAYOUTS TDC_LAYOUT_BIT(TDC_LAYOUT_VECTOR_1D)

/* ----- Hash table -------------------------------------------------------- */
/*
 * Open-addressing slot. hash == 0 means empty; FNV-1a is forced non-zero
 * by ORing the low bit (see dict_fnv1a). The string pointer is owned by
 * the input block and is stable for the duration of the encode call.
 */
typedef struct {
    uint32_t hash;          /* FNV-1a; 0 = empty sentinel */
    uint32_t idx;           /* dictionary index for this slot */
    const uint8_t *str;     /* pointer into in->data */
    uint32_t len;           /* string length in bytes */
} dict_slot;

static inline uint32_t dict_fnv1a(const uint8_t *str, uint32_t len) {
    uint32_t h = 2166136261u;
    for (uint32_t i = 0; i < len; ++i) {
        h ^= str[i];
        h *= 16777619u;
    }
    return h | 1u;          /* never zero — that's the empty sentinel */
}

/* Wrapper around realloc_fn so internal scratch growth shares one
 * code path. Returns NULL and frees `old` on failure (semi-realloc
 * semantics — caller's old pointer is stale either way). */
static void *dict_realloc(tdc_buffer *parent, void *old, size_t new_size) {
    void *p = parent->realloc_fn(parent->user, old, new_size);
    if (!p && new_size > 0) {
        if (old) parent->realloc_fn(parent->user, old, 0);
    }
    return p;
}

/* ----- Encode ------------------------------------------------------------- */

static tdc_status dict1d_encode(const tdc_block *in,
                                const void      *params,
                                tdc_buffer      *residual_out,
                                tdc_dtype       *residual_dtype,
                                tdc_buffer      *side_out) {
    (void)params;
    if (!in || !residual_out || !residual_dtype || !side_out) return TDC_E_INVAL;
    if (in->dtype  != TDC_DT_STRING)        return TDC_E_DTYPE;
    if (in->layout != TDC_LAYOUT_VECTOR_1D) return TDC_E_LAYOUT;
    if (in->shape.rank != 1)                return TDC_E_SHAPE;

    *residual_dtype = TDC_DT_U32;

    int64_t n = in->shape.dim[0];
    if (n < 0) return TDC_E_SHAPE;

    /* Empty input: emit an empty dictionary (count=0, total=0) plus
     * the mandatory offsets[count+1] entry — same on-disk shape the
     * non-empty path produces, just with no entries and no heap. The
     * decoder's want_side computation expects (8 + (count+1)*4 + total)
     * bytes uniformly. */
    if (n == 0) {
        tdc_status st = tdc_buf_reserve(side_out, 12u);
        if (st != TDC_OK) return st;
        uint32_t zero = 0u;
        memcpy(side_out->data + 0, &zero, 4u);
        memcpy(side_out->data + 4, &zero, 4u);
        memcpy(side_out->data + 8, &zero, 4u);
        side_out->size = 12u;
        residual_out->size = 0u;
        return TDC_OK;
    }

    if (in->offsets == NULL) return TDC_E_INVAL;

    /* The input must use a contiguous offsets[] starting at 0; that is
     * the standing tdc_block convention for STRING. We do not enforce
     * offsets[0] == 0 here (block_validate already passed) but we do
     * read it as the start of the first string below. */
    const uint8_t *heap = (const uint8_t *)in->data;

    /* ----- Hash table + dict scratch ------------------------------------ */
    /*
     * Three scratch buffers, all freed before return:
     *   ht         : open-addressing hash table (dict_slot[ht_cap])
     *   dict_strs  : per-unique-entry pointer/len pairs (used for the
     *                final dictionary serialization)
     *   indices    : per-row u32 dictionary index
     *
     * Hot path requirement: the hash table doubles when load >= 70%, so
     * the worst-case capacity is the next power of two above n / 0.7.
     */
    uint32_t  ht_cap  = 256u;
    uint32_t  ht_used = 0u;
    uint32_t  ht_max  = (uint32_t)((double)ht_cap * 0.7);
    dict_slot *ht = NULL;

    typedef struct { const uint8_t *str; uint32_t len; } dict_entry;
    dict_entry *dict   = NULL;
    uint32_t    dict_n   = 0u;
    uint32_t    dict_cap = 256u;
    uint32_t   *indices  = NULL;

    tdc_status st = TDC_OK;

    ht = (dict_slot *)dict_realloc(side_out, NULL, (size_t)ht_cap * sizeof(dict_slot));
    if (!ht) { st = TDC_E_NOMEM; goto cleanup; }
    memset(ht, 0, (size_t)ht_cap * sizeof(dict_slot));

    dict = (dict_entry *)dict_realloc(side_out, NULL, (size_t)dict_cap * sizeof(dict_entry));
    if (!dict) { st = TDC_E_NOMEM; goto cleanup; }

    indices = (uint32_t *)dict_realloc(side_out, NULL, (size_t)n * sizeof(uint32_t));
    if (!indices) { st = TDC_E_NOMEM; goto cleanup; }

    /* ----- Pass 1: build dict + indices --------------------------------- */
    for (int64_t i = 0; i < n; ++i) {
        uint32_t off  = in->offsets[i];
        uint32_t next = in->offsets[i + 1];
        if (next < off) { st = TDC_E_CORRUPT; goto cleanup; }
        uint32_t slen = next - off;
        const uint8_t *s = heap + off;

        uint32_t h    = dict_fnv1a(s, slen);
        uint32_t mask = ht_cap - 1u;
        uint32_t slot = h & mask;

        for (;;) {
            if (ht[slot].hash == 0u) {
                /* Empty slot — new unique string. Insert. */
                if (dict_n == dict_cap) {
                    uint32_t new_cap = dict_cap * 2u;
                    dict_entry *grown = (dict_entry *)dict_realloc(
                        side_out, dict, (size_t)new_cap * sizeof(dict_entry));
                    if (!grown) { dict = NULL; st = TDC_E_NOMEM; goto cleanup; }
                    dict     = grown;
                    dict_cap = new_cap;
                }
                ht[slot].hash = h;
                ht[slot].idx  = dict_n;
                ht[slot].str  = s;
                ht[slot].len  = slen;
                dict[dict_n].str = s;
                dict[dict_n].len = slen;
                indices[i] = dict_n;
                dict_n++;
                ht_used++;

                /* Resize at 70% load. Rehash everything into a fresh
                 * table; same probe sequence, just with the new mask. */
                if (ht_used >= ht_max) {
                    uint32_t new_cap = ht_cap * 2u;
                    dict_slot *new_ht = (dict_slot *)dict_realloc(
                        side_out, NULL, (size_t)new_cap * sizeof(dict_slot));
                    if (!new_ht) { st = TDC_E_NOMEM; goto cleanup; }
                    memset(new_ht, 0, (size_t)new_cap * sizeof(dict_slot));
                    uint32_t new_mask = new_cap - 1u;
                    for (uint32_t j = 0; j < ht_cap; ++j) {
                        if (ht[j].hash == 0u) continue;
                        uint32_t ns = ht[j].hash & new_mask;
                        while (new_ht[ns].hash != 0u) ns = (ns + 1u) & new_mask;
                        new_ht[ns] = ht[j];
                    }
                    side_out->realloc_fn(side_out->user, ht, 0);
                    ht     = new_ht;
                    ht_cap = new_cap;
                    ht_max = (uint32_t)((double)ht_cap * 0.7);
                }
                break;
            }
            if (ht[slot].hash == h &&
                ht[slot].len  == slen &&
                (slen == 0 || memcmp(ht[slot].str, s, (size_t)slen) == 0)) {
                /* Cache hit — reuse the existing dictionary index. */
                indices[i] = ht[slot].idx;
                break;
            }
            slot = (slot + 1u) & mask;
        }
    }

    /* ----- Pass 2: serialize side_meta and residual --------------------- */
    /*
     * dict_total_bytes = sum of all unique string lengths. Bounded by
     * the input data heap size, which fits in u32 by construction
     * (offsets are u32). uncompressed_size in the block record is u64
     * so the total side_meta + residual byte count remains bounded.
     */
    uint64_t dict_total = 0u;
    for (uint32_t d = 0; d < dict_n; ++d) dict_total += dict[d].len;
    if (dict_total > UINT32_MAX) { st = TDC_E_INVAL; goto cleanup; }

    size_t side_size = (size_t)8u                      /* header  */
                     + (size_t)(dict_n + 1u) * 4u       /* offsets */
                     + (size_t)dict_total;              /* heap    */
    st = tdc_buf_reserve(side_out, side_size);
    if (st != TDC_OK) goto cleanup;

    uint8_t *sp = side_out->data;
    uint32_t dict_count_u32   = dict_n;
    uint32_t dict_total_u32   = (uint32_t)dict_total;
    memcpy(sp + 0, &dict_count_u32, 4u);
    memcpy(sp + 4, &dict_total_u32, 4u);
    sp += 8;

    uint32_t running = 0u;
    for (uint32_t d = 0; d < dict_n; ++d) {
        memcpy(sp, &running, 4u);
        sp += 4;
        running += dict[d].len;
    }
    memcpy(sp, &running, 4u);
    sp += 4;

    for (uint32_t d = 0; d < dict_n; ++d) {
        if (dict[d].len > 0u) memcpy(sp, dict[d].str, dict[d].len);
        sp += dict[d].len;
    }
    side_out->size = side_size;

    size_t residual_size = (size_t)n * 4u;
    st = tdc_buf_reserve(residual_out, residual_size);
    if (st != TDC_OK) goto cleanup;
    memcpy(residual_out->data, indices, residual_size);
    residual_out->size = residual_size;

cleanup:
    if (ht)      side_out->realloc_fn(side_out->user, ht, 0);
    if (dict)    side_out->realloc_fn(side_out->user, dict, 0);
    if (indices) side_out->realloc_fn(side_out->user, indices, 0);
    return st;
}

/* ----- Decode-side helpers ----------------------------------------------- */
/*
 * dict1d_validate_side_header parses the 8-byte side-meta header and the
 * (count+1) offsets table, validating internal consistency. Returns the
 * dict_count, dict_total, and a pointer to the offsets sub-table on TDC_OK.
 *
 * dict1d_compute_output_size walks the residual stream to compute the
 * exact byte count the output heap will occupy. Exposed (non-static) so
 * the variable-width public decode entry point (tdc_decode_block_varlen)
 * can size dst->data without running the model decode twice.
 *
 * Both helpers are pure: they do not allocate and do not touch the
 * caller's output buffers.
 */

static tdc_status dict1d_validate_side_header(const uint8_t *side_meta,
                                              size_t         side_size,
                                              uint32_t      *out_dict_count,
                                              uint32_t      *out_dict_total,
                                              const uint8_t **out_off_p) {
    if (side_size < 8u || side_meta == NULL) return TDC_E_CORRUPT;

    uint32_t dict_count, dict_total;
    memcpy(&dict_count, side_meta + 0, 4u);
    memcpy(&dict_total, side_meta + 4, 4u);

    /* 64-bit so (dict_count + 1) * 4 cannot wrap to 0 when dict_count is
     * UINT32_MAX; an oversized count then fails this equality instead of
     * zeroing the table size and leaving the loop below unterminated. */
    uint64_t want_side = (uint64_t)8u
                       + ((uint64_t)dict_count + 1u) * 4u
                       + (uint64_t)dict_total;
    if ((uint64_t)side_size != want_side) return TDC_E_CORRUPT;

    const uint8_t *off_p = side_meta + 8u;

    /* Verify the offsets table is internally consistent and matches the
     * declared total. Catches torn/corrupt records before we dereference
     * out-of-range offsets in the inner loop. The counter is 64-bit so
     * d <= dict_count cannot wrap near UINT32_MAX. */
    uint32_t prev_off = 0u;
    for (uint64_t d = 0; d <= (uint64_t)dict_count; ++d) {
        uint32_t cur;
        memcpy(&cur, off_p + (size_t)d * 4u, 4u);
        if (cur < prev_off)   return TDC_E_CORRUPT;
        if (cur > dict_total) return TDC_E_CORRUPT;
        prev_off = cur;
    }
    {
        uint32_t last;
        memcpy(&last, off_p + (size_t)dict_count * 4u, 4u);
        if (last != dict_total) return TDC_E_CORRUPT;
    }

    *out_dict_count = dict_count;
    *out_dict_total = dict_total;
    *out_off_p      = off_p;
    return TDC_OK;
}

tdc_status dict1d_compute_output_size(const uint8_t *residuals,
                                      size_t         residual_size,
                                      const uint8_t *side_meta,
                                      size_t         side_size,
                                      int64_t        n_elems,
                                      size_t        *out_heap_bytes) {
    if (!out_heap_bytes) return TDC_E_INVAL;
    *out_heap_bytes = 0;
    if (n_elems < 0) return TDC_E_SHAPE;

    uint32_t dict_count = 0, dict_total = 0;
    const uint8_t *off_p = NULL;
    tdc_status st = dict1d_validate_side_header(side_meta, side_size,
                                                &dict_count, &dict_total,
                                                &off_p);
    if (st != TDC_OK) return st;

    if (n_elems == 0) return TDC_OK;

    if ((size_t)n_elems * 4u != residual_size) return TDC_E_CORRUPT;
    if (residuals == NULL) return TDC_E_INVAL;

    uint64_t total_out = 0u;
    for (int64_t i = 0; i < n_elems; ++i) {
        uint32_t idx;
        memcpy(&idx, residuals + (size_t)i * 4u, 4u);
        if (idx >= dict_count) return TDC_E_CORRUPT;
        uint32_t s_off, s_next;
        memcpy(&s_off,  off_p + (size_t)idx       * 4u, 4u);
        memcpy(&s_next, off_p + (size_t)(idx + 1u) * 4u, 4u);
        total_out += (uint64_t)(s_next - s_off);
        if (total_out > UINT32_MAX) return TDC_E_INVAL;
    }

    *out_heap_bytes = (size_t)total_out;
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status dict1d_decode(tdc_block      *out,
                                const void     *params,
                                tdc_dtype       residual_dtype,
                                const uint8_t  *residuals, size_t residual_size,
                                const uint8_t  *side_meta, size_t side_size) {
    (void)params;
    if (!out) return TDC_E_INVAL;
    if (out->dtype  != TDC_DT_STRING)        return TDC_E_DTYPE;
    if (out->layout != TDC_LAYOUT_VECTOR_1D) return TDC_E_LAYOUT;
    if (out->shape.rank != 1)                return TDC_E_SHAPE;
    if (residual_dtype != TDC_DT_U32)        return TDC_E_DTYPE;

    int64_t n = out->shape.dim[0];
    if (n < 0) return TDC_E_SHAPE;

    uint32_t dict_count = 0, dict_total = 0;
    const uint8_t *off_p = NULL;
    tdc_status st = dict1d_validate_side_header(side_meta, side_size,
                                                &dict_count, &dict_total,
                                                &off_p);
    if (st != TDC_OK) return st;

    /* Empty case: nothing to write. tdc_block_validate allows
     * offsets == NULL when n == 0, so we tolerate that here too. */
    if (n == 0) {
        if (out->offsets) out->offsets[0] = 0u;
        return TDC_OK;
    }

    if (out->offsets == NULL) return TDC_E_INVAL;
    /* out->data may be NULL when the residual indices select only
     * zero-length dictionary entries (total heap is 0 bytes). The write
     * loop is gated by `slen > 0u`, so a NULL dst is safe in that case;
     * tdc_decode_block_varlen's hook intentionally leaves dst.data NULL
     * for an empty heap. */
    if ((size_t)n * 4u != residual_size) return TDC_E_CORRUPT;
    if (residuals == NULL) return TDC_E_INVAL;

    const uint8_t *heap_p = off_p + (size_t)(dict_count + 1u) * 4u;

    /* Pass 1: validate every index. The total output byte count is
     * available via dict1d_compute_output_size for callers that need
     * to size out->data; this routine only re-validates the indices
     * because tdc_decode_block_varlen has already used the helper
     * upstream. */
    for (int64_t i = 0; i < n; ++i) {
        uint32_t idx;
        memcpy(&idx, residuals + (size_t)i * 4u, 4u);
        if (idx >= dict_count) return TDC_E_CORRUPT;
    }

    /* Pass 2: write out->offsets and out->data. The caller is
     * responsible for sizing out->data; the variable-width entry point
     * (tdc_decode_block_varlen) does so via dict1d_compute_output_size.
     * The model vtable cannot allocate caller-owned memory itself. */
    uint32_t pos = 0u;
    uint8_t *dst = (uint8_t *)out->data;
    for (int64_t i = 0; i < n; ++i) {
        uint32_t idx;
        memcpy(&idx, residuals + (size_t)i * 4u, 4u);
        uint32_t s_off, s_next;
        memcpy(&s_off,  off_p + (size_t)idx       * 4u, 4u);
        memcpy(&s_next, off_p + (size_t)(idx + 1u) * 4u, 4u);
        uint32_t slen = s_next - s_off;
        out->offsets[i] = pos;
        if (slen > 0u) memcpy(dst + pos, heap_p + s_off, slen);
        pos += slen;
    }
    out->offsets[n] = pos;

    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_model_vt tdc_model_dict1d_vt = {
    .id               = TDC_MODEL_DICT_1D,
    .name             = "dict1d",
    .accepted_dtypes  = DICT1D_ACCEPTED_DTYPES,
    .accepted_layouts = DICT1D_ACCEPTED_LAYOUTS,
    .encode           = dict1d_encode,
    .decode           = dict1d_decode,
};
