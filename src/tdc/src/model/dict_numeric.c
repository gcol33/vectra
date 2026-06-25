/*
 * src/model/dict_numeric.c
 *
 * TDC_MODEL_DICT_NUMERIC_1D — numeric value dictionary.
 *
 * Encodes a 1D vector of fixed-width numeric values as
 *   - a side-meta value table holding each unique value once, and
 *   - a u32 residual stream of indices into that table, one per input row.
 *
 * The motivating case is real-world low-precision float data (e.g. NASA
 * POWER daily air temperature rounded to 0.1 degC: ~5 400 unique f64s
 * across a million samples). Byte-level predictor residuals on that
 * data have *higher* entropy than the raw bytes because the compressible
 * structure is 8-byte repetition at f64 alignment, not bit-level
 * predictability. A numeric dictionary exposes that structure directly.
 *
 * Accepted dtype/layout:
 *   TDC_LAYOUT_VECTOR_1D, dtype in { i16, u16, i32, u32, f32, i64, u64, f64 }.
 * i8/u8 are rejected: the universe is already <= 256 values, so a
 * dictionary adds overhead with no gain.
 *
 * Side metadata layout (little-endian, unaligned; all reads via memcpy):
 *
 *     u8  value_dtype                (tdc_dtype of the stored values)
 *     u32 dict_count                 (number of unique values)
 *     u8  values[dict_count * elem_size]   (raw little-endian numeric
 *                                          values in insertion order)
 *
 * Residual stream:
 *
 *     u32 indices[n_elems]           (one dictionary index per input row)
 *
 * Residual dtype is fixed at TDC_DT_U32 regardless of dictionary size.
 * See driver_internal.h's driver_model_residual_dtype; the same rule
 * covers DICT_1D (string) and this model. Data-dependent index width
 * would force per-block encoder state on disk and the savings versus a
 * downstream BSHUF+HUF/LZ stage are negligible (high-significance index
 * bytes are nearly all zero for any plausible dictionary size).
 *
 * Hash table:
 *   FNV-1a over the raw value bytes, open-addressing, 70% load factor,
 *   double on resize. Each slot stores the value bytes in-place (8 bytes,
 *   whatever the elem_size actually uses — we never inspect unused
 *   tail bytes). Same "no Robin Hood" policy as DICT_1D and the vectra
 *   hash tables documented in tdc/CLAUDE.md.
 *
 * Float semantics:
 *   Values are compared and hashed by byte identity, NOT by numeric
 *   equality. +0.0 and -0.0 therefore sit in different slots, and every
 *   distinct NaN payload is a distinct dictionary entry. This preserves
 *   byte-exact round-trip, which is the only correctness contract for
 *   tdc models.
 *
 * NA / validity:
 *   Opaque pass-through, same convention as every other v0 model. Every
 *   row contributes its value to the dictionary regardless of validity;
 *   the driver writes the bitmap verbatim.
 */

#include "tdc/model.h"
#include "model_internal.h"
#include "model_load_store.h"  /* TDC_DT_BIT / TDC_LAYOUT_BIT */
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Acceptance bitmasks ------------------------------------------------ */

#define DNUM_ACCEPTED_DTYPES  ( \
    TDC_DT_BIT(TDC_DT_I16) | TDC_DT_BIT(TDC_DT_U16) | \
    TDC_DT_BIT(TDC_DT_I32) | TDC_DT_BIT(TDC_DT_U32) | TDC_DT_BIT(TDC_DT_F32) | \
    TDC_DT_BIT(TDC_DT_I64) | TDC_DT_BIT(TDC_DT_U64) | TDC_DT_BIT(TDC_DT_F64))

#define DNUM_ACCEPTED_LAYOUTS TDC_LAYOUT_BIT(TDC_LAYOUT_VECTOR_1D)

/* Fixed side-meta header size: u8 value_dtype + u32 dict_count. */
#define DNUM_HEADER_SIZE 5u

/* Maximum element width; keeps the in-slot value storage POD. */
#define DNUM_MAX_ELEM 8u

/* ----- Hash table -------------------------------------------------------- */

typedef struct {
    uint32_t hash;          /* FNV-1a; 0 = empty sentinel */
    uint32_t idx;           /* dictionary index for this slot */
    uint8_t  value[DNUM_MAX_ELEM];  /* raw bytes; only elem_size are live */
} dnum_slot;

static inline uint32_t dnum_fnv1a(const uint8_t *p, size_t n) {
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < n; ++i) {
        h ^= p[i];
        h *= 16777619u;
    }
    return h | 1u;          /* never zero — that's the empty sentinel */
}

/* Wrapper around realloc_fn: on failure, frees the old pointer and
 * returns NULL. Same semi-realloc semantics the string dict uses. */
static void *dnum_realloc(tdc_buffer *parent, void *old, size_t new_size) {
    void *p = parent->realloc_fn(parent->user, old, new_size);
    if (!p && new_size > 0) {
        if (old) parent->realloc_fn(parent->user, old, 0);
    }
    return p;
}

/* ----- Encode ------------------------------------------------------------- */

static tdc_status dnum_encode(const tdc_block *in,
                              const void      *params,
                              tdc_buffer      *residual_out,
                              tdc_dtype       *residual_dtype,
                              tdc_buffer      *side_out) {
    (void)params;
    if (!in || !residual_out || !residual_dtype || !side_out) return TDC_E_INVAL;
    if (in->layout != TDC_LAYOUT_VECTOR_1D) return TDC_E_LAYOUT;
    if (in->shape.rank != 1)                return TDC_E_SHAPE;

    size_t elem_size = tdc_dtype_size(in->dtype);
    if (elem_size < 2u || elem_size > DNUM_MAX_ELEM) return TDC_E_DTYPE;

    *residual_dtype = TDC_DT_U32;

    int64_t n = in->shape.dim[0];
    if (n < 0) return TDC_E_SHAPE;

    /* Empty input: emit a header with count=0 and no values. */
    if (n == 0) {
        tdc_status st = tdc_buf_reserve(side_out, (size_t)DNUM_HEADER_SIZE);
        if (st != TDC_OK) return st;
        uint8_t  dt8   = (uint8_t)in->dtype;
        uint32_t zero  = 0u;
        memcpy(side_out->data + 0, &dt8,  1u);
        memcpy(side_out->data + 1, &zero, 4u);
        side_out->size = DNUM_HEADER_SIZE;
        residual_out->size = 0u;
        return TDC_OK;
    }

    if (in->data == NULL) return TDC_E_INVAL;

    const uint8_t *src = (const uint8_t *)in->data;

    uint32_t   ht_cap  = 256u;
    uint32_t   ht_used = 0u;
    uint32_t   ht_max  = (uint32_t)((double)ht_cap * 0.7);
    dnum_slot *ht      = NULL;

    /* Dictionary values are written directly into a growable byte array
     * sized in multiples of elem_size. The final side_out buffer is
     * assembled once we know dict_count. */
    uint8_t  *dict_vals = NULL;
    uint32_t  dict_n    = 0u;
    uint32_t  dict_cap  = 256u;

    uint32_t *indices = NULL;

    tdc_status st = TDC_OK;

    ht = (dnum_slot *)dnum_realloc(side_out, NULL, (size_t)ht_cap * sizeof(dnum_slot));
    if (!ht) { st = TDC_E_NOMEM; goto cleanup; }
    memset(ht, 0, (size_t)ht_cap * sizeof(dnum_slot));

    dict_vals = (uint8_t *)dnum_realloc(side_out, NULL, (size_t)dict_cap * elem_size);
    if (!dict_vals) { st = TDC_E_NOMEM; goto cleanup; }

    indices = (uint32_t *)dnum_realloc(side_out, NULL, (size_t)n * sizeof(uint32_t));
    if (!indices) { st = TDC_E_NOMEM; goto cleanup; }

    /* ----- Pass 1: build dict + indices --------------------------------- */
    for (int64_t i = 0; i < n; ++i) {
        const uint8_t *v = src + (size_t)i * elem_size;
        uint32_t h    = dnum_fnv1a(v, elem_size);
        uint32_t mask = ht_cap - 1u;
        uint32_t slot = h & mask;

        for (;;) {
            if (ht[slot].hash == 0u) {
                /* Empty slot — insert new unique value. */
                if (dict_n == dict_cap) {
                    uint32_t new_cap = dict_cap * 2u;
                    uint8_t *grown = (uint8_t *)dnum_realloc(
                        side_out, dict_vals, (size_t)new_cap * elem_size);
                    if (!grown) { dict_vals = NULL; st = TDC_E_NOMEM; goto cleanup; }
                    dict_vals = grown;
                    dict_cap  = new_cap;
                }
                ht[slot].hash = h;
                ht[slot].idx  = dict_n;
                memcpy(ht[slot].value, v, elem_size);
                memcpy(dict_vals + (size_t)dict_n * elem_size, v, elem_size);
                indices[i] = dict_n;
                dict_n++;
                ht_used++;

                if (ht_used >= ht_max) {
                    uint32_t new_cap = ht_cap * 2u;
                    dnum_slot *new_ht = (dnum_slot *)dnum_realloc(
                        side_out, NULL, (size_t)new_cap * sizeof(dnum_slot));
                    if (!new_ht) { st = TDC_E_NOMEM; goto cleanup; }
                    memset(new_ht, 0, (size_t)new_cap * sizeof(dnum_slot));
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
                memcmp(ht[slot].value, v, elem_size) == 0) {
                indices[i] = ht[slot].idx;
                break;
            }
            slot = (slot + 1u) & mask;
        }
    }

    /* ----- Pass 2: serialize side_meta and residual --------------------- */
    /*
     * Overflow guard: dict_n is bounded by n (u32 here; n fits in u32
     * because shape.dim[0] uses int64_t but the indices residual is u32
     * and will be rejected for n > UINT32_MAX upstream). dict_n *
     * elem_size therefore fits in size_t on any 32-bit-size platform
     * we care about (x86_64, aarch64 — 64-bit size_t).
     */
    size_t values_bytes = (size_t)dict_n * elem_size;
    size_t side_size    = (size_t)DNUM_HEADER_SIZE + values_bytes;
    st = tdc_buf_reserve(side_out, side_size);
    if (st != TDC_OK) goto cleanup;

    uint8_t *sp   = side_out->data;
    uint8_t  dt8  = (uint8_t)in->dtype;
    uint32_t cnt  = dict_n;
    memcpy(sp + 0, &dt8, 1u);
    memcpy(sp + 1, &cnt, 4u);
    if (values_bytes > 0) {
        memcpy(sp + DNUM_HEADER_SIZE, dict_vals, values_bytes);
    }
    side_out->size = side_size;

    size_t residual_size = (size_t)n * 4u;
    st = tdc_buf_reserve(residual_out, residual_size);
    if (st != TDC_OK) goto cleanup;
    memcpy(residual_out->data, indices, residual_size);
    residual_out->size = residual_size;

cleanup:
    if (ht)        side_out->realloc_fn(side_out->user, ht, 0);
    if (dict_vals) side_out->realloc_fn(side_out->user, dict_vals, 0);
    if (indices)   side_out->realloc_fn(side_out->user, indices, 0);
    return st;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status dnum_decode(tdc_block      *out,
                              const void     *params,
                              tdc_dtype       residual_dtype,
                              const uint8_t  *residuals, size_t residual_size,
                              const uint8_t  *side_meta, size_t side_size) {
    (void)params;
    if (!out) return TDC_E_INVAL;
    if (out->layout != TDC_LAYOUT_VECTOR_1D) return TDC_E_LAYOUT;
    if (out->shape.rank != 1)                return TDC_E_SHAPE;
    if (residual_dtype != TDC_DT_U32)        return TDC_E_DTYPE;

    size_t elem_size = tdc_dtype_size(out->dtype);
    if (elem_size < 2u || elem_size > DNUM_MAX_ELEM) return TDC_E_DTYPE;

    int64_t n = out->shape.dim[0];
    if (n < 0) return TDC_E_SHAPE;

    if (side_size < DNUM_HEADER_SIZE || side_meta == NULL) return TDC_E_CORRUPT;

    uint8_t  stored_dtype;
    uint32_t dict_count;
    memcpy(&stored_dtype, side_meta + 0, 1u);
    memcpy(&dict_count,   side_meta + 1, 4u);

    if ((tdc_dtype)stored_dtype != out->dtype) return TDC_E_DTYPE;

    size_t values_bytes = (size_t)dict_count * elem_size;
    if (side_size != (size_t)DNUM_HEADER_SIZE + values_bytes) return TDC_E_CORRUPT;

    if (n == 0) return TDC_OK;

    if (out->data == NULL) return TDC_E_INVAL;
    if ((size_t)n * 4u != residual_size) return TDC_E_CORRUPT;
    if (residuals == NULL) return TDC_E_INVAL;

    const uint8_t *values = side_meta + DNUM_HEADER_SIZE;
    uint8_t       *dst    = (uint8_t *)out->data;

    /* Single pass: validate index, copy value. The compiler specializes
     * memcpy on constant elem_size within each iteration (LTO at least),
     * but elem_size is loop-invariant so even without LTO the common
     * 8-byte f64 case collapses to one mov. */
    for (int64_t i = 0; i < n; ++i) {
        uint32_t idx;
        memcpy(&idx, residuals + (size_t)i * 4u, 4u);
        if (idx >= dict_count) return TDC_E_CORRUPT;
        memcpy(dst + (size_t)i * elem_size,
               values + (size_t)idx * elem_size,
               elem_size);
    }

    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_model_vt tdc_model_dict_numeric_1d_vt = {
    .id               = TDC_MODEL_DICT_NUMERIC_1D,
    .name             = "dict_numeric_1d",
    .accepted_dtypes  = DNUM_ACCEPTED_DTYPES,
    .accepted_layouts = DNUM_ACCEPTED_LAYOUTS,
    .encode           = dnum_encode,
    .decode           = dnum_decode,
};
