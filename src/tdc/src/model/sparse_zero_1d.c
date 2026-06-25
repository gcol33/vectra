/*
 * src/model/sparse_zero_1d.c
 *
 * TDC_MODEL_SPARSE_ZERO_1D — sparse-zero numeric vector.
 *
 * Encodes a 1D vector where "most" entries are zero as
 *   - a side-meta run of the non-zero values in order of appearance, and
 *   - a u32 residual stream of positions where those values live.
 *
 * Zero is defined as all-zero bytes (the same byte-identity convention
 * DICT_NUMERIC_1D uses). For floats that means +0.0 is zero and -0.0
 * is not; for signed ints 0 is zero and INT_MIN is not. The test is a
 * plain memcmp against a static zero pattern, so any storage format
 * whose zero is represented as all-zero bytes decodes correctly.
 *
 * Accepted dtype/layout:
 *   TDC_LAYOUT_VECTOR_1D, dtype in { i16, u16, i32, u32, f32, i64, u64, f64 }.
 * i8/u8 are rejected: a 1-byte zero test gains nothing over RAW once the
 * entropy stage runs.
 *
 * Side metadata layout (little-endian, unaligned; all reads via memcpy):
 *
 *     u8  value_dtype                (tdc_dtype of the stored values)
 *     u32 n_nonzero                  (number of non-zero entries)
 *     u8  values[n_nonzero * elem_size]   (non-zero values in order of
 *                                          appearance in the input)
 *
 * Residual stream:
 *
 *     u32 positions[n_nonzero]       (position of each non-zero in the
 *                                     original vector; strictly ascending)
 *
 * Residual dtype is fixed at TDC_DT_U32. See driver_model_residual_dtype
 * in src/api/driver_internal.h; the same rule covers DICT_1D (string) and
 * DICT_NUMERIC_1D. Same rationale: data-dependent position width would
 * force per-block encoder state on disk and a downstream BSHUF+LZ collapses
 * the high zero bytes of u32 positions for the sparse common case.
 *
 * Validity:
 *   Opaque pass-through, same convention as every other v0 model. Every
 *   row's value participates in the zero test regardless of validity; the
 *   driver writes the bitmap verbatim.
 *
 * Worst case:
 *   Dense input (no zeros) encodes to 5 + n*elem_size side bytes plus
 *   n*4 residual bytes — ~50% larger than RAW for i64/f64. No fallback:
 *   the caller is expected to pick this model only when the zero
 *   density earns the overhead back. Same no-fallback stance as DICT_1D.
 */

#include "tdc/model.h"
#include "model_internal.h"
#include "model_load_store.h"  /* TDC_DT_BIT / TDC_LAYOUT_BIT */
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Acceptance bitmasks ------------------------------------------------ */

#define SPZ_ACCEPTED_DTYPES  ( \
    TDC_DT_BIT(TDC_DT_I16) | TDC_DT_BIT(TDC_DT_U16) | \
    TDC_DT_BIT(TDC_DT_I32) | TDC_DT_BIT(TDC_DT_U32) | TDC_DT_BIT(TDC_DT_F32) | \
    TDC_DT_BIT(TDC_DT_I64) | TDC_DT_BIT(TDC_DT_U64) | TDC_DT_BIT(TDC_DT_F64))

#define SPZ_ACCEPTED_LAYOUTS TDC_LAYOUT_BIT(TDC_LAYOUT_VECTOR_1D)

/* Fixed side-meta header size: u8 value_dtype + u32 n_nonzero. */
#define SPZ_HEADER_SIZE 5u

/* Maximum element width; keeps the static zero pattern bounded. */
#define SPZ_MAX_ELEM 8u

/* Static all-zero pattern used as the zero reference. Sized to the widest
 * accepted element; the memcmp below only reads elem_size bytes. */
static const uint8_t spz_zero_pattern[SPZ_MAX_ELEM] = {0, 0, 0, 0, 0, 0, 0, 0};

/* Is the elem_size-byte value at p all-zero? */
static inline int spz_is_zero(const uint8_t *p, size_t elem_size) {
    return memcmp(p, spz_zero_pattern, elem_size) == 0;
}

/* ----- Encode ------------------------------------------------------------- */

static tdc_status spz_encode(const tdc_block *in,
                             const void      *params,
                             tdc_buffer      *residual_out,
                             tdc_dtype       *residual_dtype,
                             tdc_buffer      *side_out) {
    (void)params;
    if (!in || !residual_out || !residual_dtype || !side_out) return TDC_E_INVAL;
    if (in->layout != TDC_LAYOUT_VECTOR_1D) return TDC_E_LAYOUT;
    if (in->shape.rank != 1)                return TDC_E_SHAPE;

    size_t elem_size = tdc_dtype_size(in->dtype);
    if (elem_size < 2u || elem_size > SPZ_MAX_ELEM) return TDC_E_DTYPE;

    *residual_dtype = TDC_DT_U32;

    int64_t n = in->shape.dim[0];
    if (n < 0) return TDC_E_SHAPE;
    if ((uint64_t)n > (uint64_t)UINT32_MAX) return TDC_E_INVAL;

    /* Empty input: header with n_nonzero=0 and no values. Same shape the
     * non-empty path produces, just with no entries. */
    if (n == 0) {
        tdc_status st = tdc_buf_reserve(side_out, (size_t)SPZ_HEADER_SIZE);
        if (st != TDC_OK) return st;
        uint8_t  dt8  = (uint8_t)in->dtype;
        uint32_t zero = 0u;
        memcpy(side_out->data + 0, &dt8,  1u);
        memcpy(side_out->data + 1, &zero, 4u);
        side_out->size = SPZ_HEADER_SIZE;
        residual_out->size = 0u;
        return TDC_OK;
    }

    if (in->data == NULL) return TDC_E_INVAL;

    const uint8_t *src = (const uint8_t *)in->data;

    /* Pass 1: count non-zeros. Single predictable linear scan; cheaper
     * than growing two buffers incrementally and lets us size the
     * residual and side-meta allocations exactly once. */
    uint32_t n_nonzero = 0u;
    for (int64_t i = 0; i < n; ++i) {
        if (!spz_is_zero(src + (size_t)i * elem_size, elem_size)) {
            ++n_nonzero;
        }
    }

    /* Size the output buffers exactly. Overflow guard: n_nonzero <= n <=
     * UINT32_MAX and elem_size <= 8, so n_nonzero * elem_size fits in
     * size_t on any 64-bit-size_t platform we care about (x86_64,
     * aarch64). The +5 header fits trivially. */
    size_t values_bytes = (size_t)n_nonzero * elem_size;
    size_t side_size    = (size_t)SPZ_HEADER_SIZE + values_bytes;
    size_t residual_size = (size_t)n_nonzero * 4u;

    tdc_status st = tdc_buf_reserve(side_out, side_size);
    if (st != TDC_OK) return st;
    st = tdc_buf_reserve(residual_out, residual_size);
    if (st != TDC_OK) return st;

    /* Write side-meta header. */
    uint8_t *sp = side_out->data;
    uint8_t  dt8 = (uint8_t)in->dtype;
    memcpy(sp + 0, &dt8,       1u);
    memcpy(sp + 1, &n_nonzero, 4u);

    /* Pass 2: emit positions (into residual) and values (into side-meta).
     * Two write cursors; single read pass over the input. */
    uint8_t *values_p = sp + SPZ_HEADER_SIZE;
    uint8_t *pos_p    = residual_out->data;
    uint32_t emitted  = 0u;
    for (int64_t i = 0; i < n; ++i) {
        const uint8_t *v = src + (size_t)i * elem_size;
        if (spz_is_zero(v, elem_size)) continue;
        uint32_t pos = (uint32_t)i;
        memcpy(pos_p + (size_t)emitted * 4u, &pos, 4u);
        memcpy(values_p + (size_t)emitted * elem_size, v, elem_size);
        ++emitted;
    }
    /* emitted == n_nonzero by construction (both passes read the same
     * bytes and apply the same predicate). Assert via a corruption
     * return rather than abort: if a racing mutator ever defeats this
     * we want a reportable error, not UB. */
    if (emitted != n_nonzero) return TDC_E_CORRUPT;

    side_out->size     = side_size;
    residual_out->size = residual_size;
    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status spz_decode(tdc_block      *out,
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
    if (elem_size < 2u || elem_size > SPZ_MAX_ELEM) return TDC_E_DTYPE;

    int64_t n = out->shape.dim[0];
    if (n < 0) return TDC_E_SHAPE;
    if ((uint64_t)n > (uint64_t)UINT32_MAX) return TDC_E_INVAL;

    if (side_size < SPZ_HEADER_SIZE || side_meta == NULL) return TDC_E_CORRUPT;

    uint8_t  stored_dtype;
    uint32_t n_nonzero;
    memcpy(&stored_dtype, side_meta + 0, 1u);
    memcpy(&n_nonzero,    side_meta + 1, 4u);

    if ((tdc_dtype)stored_dtype != out->dtype)         return TDC_E_DTYPE;
    if ((uint64_t)n_nonzero > (uint64_t)n)             return TDC_E_CORRUPT;

    size_t values_bytes = (size_t)n_nonzero * elem_size;
    if (side_size != (size_t)SPZ_HEADER_SIZE + values_bytes) return TDC_E_CORRUPT;
    if (residual_size != (size_t)n_nonzero * 4u)             return TDC_E_CORRUPT;

    if (n == 0) return TDC_OK;
    if (out->data == NULL) return TDC_E_INVAL;

    /* Zero-fill the destination, then scatter the non-zero values into
     * their recorded positions. memset is the right primitive: the zero
     * reference for every accepted dtype is an all-zero bit pattern. */
    uint8_t *dst = (uint8_t *)out->data;
    memset(dst, 0, (size_t)n * elem_size);

    if (n_nonzero == 0u) return TDC_OK;
    if (residuals == NULL) return TDC_E_INVAL;

    const uint8_t *values = side_meta + SPZ_HEADER_SIZE;

    /* Positions are validated to be in-range and strictly ascending.
     * Strict monotonicity catches duplicate-position corruption that
     * a range-only check would miss (two non-zero writes to the same
     * slot, with one silently discarded). Ascending order is an
     * encoder invariant — the encoder walks i = 0..n-1. */
    uint32_t prev = 0u;
    int have_prev = 0;
    for (uint32_t k = 0; k < n_nonzero; ++k) {
        uint32_t pos;
        memcpy(&pos, residuals + (size_t)k * 4u, 4u);
        if ((int64_t)pos >= n) return TDC_E_CORRUPT;
        if (have_prev && pos <= prev) return TDC_E_CORRUPT;
        prev = pos;
        have_prev = 1;

        memcpy(dst + (size_t)pos * elem_size,
               values + (size_t)k * elem_size,
               elem_size);
    }

    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_model_vt tdc_model_sparse_zero_1d_vt = {
    .id               = TDC_MODEL_SPARSE_ZERO_1D,
    .name             = "sparse_zero_1d",
    .accepted_dtypes  = SPZ_ACCEPTED_DTYPES,
    .accepted_layouts = SPZ_ACCEPTED_LAYOUTS,
    .encode           = spz_encode,
    .decode           = spz_decode,
};
