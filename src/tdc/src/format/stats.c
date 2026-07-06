/*
 * src/format/stats.c — column statistics: compute, serialize, parse
 *
 * Computes min/max over a tdc_block's elements for zone-map filtering
 * and sorted-column detection. All comparisons use the natural ordering
 * of the dtype; floats use ordered-integer mapping so that -0 < +0 and
 * NaN sorts consistently (though NaN min/max semantics are not
 * meaningful, the code won't crash).
 *
 * For TDC_DT_STRING: stores the first 8 bytes of the lexicographically
 * smallest/largest string, zero-padded.
 *
 * No allocation: all buffers are caller-supplied.
 */

#include "stats_internal.h"
#include "../core/float_order.h"

#include <string.h>

/* ---- typed min/max scanners --------------------------------------------- */

/*
 * Generic scanner for fixed-width types. Uses a compare callback that
 * returns negative/zero/positive like memcmp. The min/max element pointers
 * are tracked as byte offsets into the data array; at the end the winning
 * elements are stored via tdc_stats_store_value.
 *
 * For small element counts this is not worth abstracting further — the
 * per-dtype switch in tdc_stats_compute keeps each case tight and avoids
 * function-pointer overhead in what may be a hot path.
 */

/* Scan signed integers, comparing as their native C type. */
#define DEFINE_SIGNED_SCANNER(bits)                                         \
static void scan_i##bits(const void *data, int64_t n,                       \
                         const void **out_min, const void **out_max)         \
{                                                                           \
    const int##bits##_t *p = (const int##bits##_t *)data;                   \
    int##bits##_t cur_min, cur_max;                                         \
    memcpy(&cur_min, p, sizeof(cur_min));                                   \
    cur_max = cur_min;                                                      \
    const void *ptr_min = p;                                                \
    const void *ptr_max = p;                                                \
    for (int64_t i = 1; i < n; ++i) {                                       \
        int##bits##_t v;                                                    \
        memcpy(&v, p + i, sizeof(v));                                       \
        if (v < cur_min) { cur_min = v; ptr_min = p + i; }                  \
        if (v > cur_max) { cur_max = v; ptr_max = p + i; }                  \
    }                                                                       \
    *out_min = ptr_min;                                                     \
    *out_max = ptr_max;                                                     \
}

DEFINE_SIGNED_SCANNER(8)
DEFINE_SIGNED_SCANNER(16)
DEFINE_SIGNED_SCANNER(32)
DEFINE_SIGNED_SCANNER(64)

/* Scan unsigned integers. */
#define DEFINE_UNSIGNED_SCANNER(bits)                                        \
static void scan_u##bits(const void *data, int64_t n,                       \
                         const void **out_min, const void **out_max)         \
{                                                                           \
    const uint##bits##_t *p = (const uint##bits##_t *)data;                 \
    uint##bits##_t cur_min, cur_max;                                        \
    memcpy(&cur_min, p, sizeof(cur_min));                                   \
    cur_max = cur_min;                                                      \
    const void *ptr_min = p;                                                \
    const void *ptr_max = p;                                                \
    for (int64_t i = 1; i < n; ++i) {                                       \
        uint##bits##_t v;                                                   \
        memcpy(&v, p + i, sizeof(v));                                       \
        if (v < cur_min) { cur_min = v; ptr_min = p + i; }                  \
        if (v > cur_max) { cur_max = v; ptr_max = p + i; }                  \
    }                                                                       \
    *out_min = ptr_min;                                                     \
    *out_max = ptr_max;                                                     \
}

DEFINE_UNSIGNED_SCANNER(8)
DEFINE_UNSIGNED_SCANNER(16)
DEFINE_UNSIGNED_SCANNER(32)
DEFINE_UNSIGNED_SCANNER(64)

/*
 * Float scanners: compare via ordered-integer mapping so the result is
 * a total order that agrees with numerical < for non-NaN values.
 */
static void scan_f16(const void *data, int64_t n,
                     const void **out_min, const void **out_max)
{
    const uint8_t *bytes = (const uint8_t *)data;
    uint16_t best_min_bits = tdc_load_f16_bits(bytes);
    uint16_t best_max_bits = best_min_bits;
    uint16_t best_min_ord  = tdc_f16_to_ordered(best_min_bits);
    uint16_t best_max_ord  = best_min_ord;
    const void *ptr_min = bytes;
    const void *ptr_max = bytes;
    for (int64_t i = 1; i < n; ++i) {
        const uint8_t *elem = bytes + i * 2;
        uint16_t bits = tdc_load_f16_bits(elem);
        uint16_t ord  = tdc_f16_to_ordered(bits);
        if (ord < best_min_ord) { best_min_ord = ord; best_min_bits = bits; ptr_min = elem; }
        if (ord > best_max_ord) { best_max_ord = ord; best_max_bits = bits; ptr_max = elem; }
    }
    (void)best_min_bits;
    (void)best_max_bits;
    *out_min = ptr_min;
    *out_max = ptr_max;
}

#define DEFINE_FLOAT_SCANNER(SUFFIX, UT, W, TO_ORD, LOAD_BITS)            \
static void scan_##SUFFIX(const void *data, int64_t n,                    \
                          const void **out_min, const void **out_max) {    \
    const uint8_t *bytes = (const uint8_t *)data;                         \
    UT best_min_ord = TO_ORD(LOAD_BITS(bytes));                           \
    UT best_max_ord = best_min_ord;                                       \
    const void *ptr_min = bytes;                                          \
    const void *ptr_max = bytes;                                          \
    for (int64_t i = 1; i < n; ++i) {                                    \
        const uint8_t *elem = bytes + i * (W);                           \
        UT ord = TO_ORD(LOAD_BITS(elem));                                \
        if (ord < best_min_ord) { best_min_ord = ord; ptr_min = elem; }  \
        if (ord > best_max_ord) { best_max_ord = ord; ptr_max = elem; }  \
    }                                                                    \
    *out_min = ptr_min;                                                  \
    *out_max = ptr_max;                                                  \
}

DEFINE_FLOAT_SCANNER(f32, uint32_t, 4, tdc_f32_to_ordered, tdc_load_f32_bits)
DEFINE_FLOAT_SCANNER(f64, uint64_t, 8, tdc_f64_to_ordered, tdc_load_f64_bits)

#undef DEFINE_FLOAT_SCANNER

/* ---- string min/max ----------------------------------------------------- */

/*
 * For strings: lexicographic comparison on raw bytes, store first 8 bytes
 * of min/max string zero-padded into the 16-byte stats slot.
 */
#define TDC_STATS_STRING_PREFIX 8

static void stats_store_string_prefix(uint8_t dst[TDC_STATS_VALUE_SIZE],
                                      const uint8_t *str, uint32_t len)
{
    memset(dst, 0, TDC_STATS_VALUE_SIZE);
    uint32_t copy = len < TDC_STATS_STRING_PREFIX ? len : TDC_STATS_STRING_PREFIX;
    if (copy > 0) memcpy(dst, str, copy);
}

/* Compare two byte strings lexicographically. Returns <0, 0, >0. */
static int string_cmp(const uint8_t *a, uint32_t a_len,
                      const uint8_t *b, uint32_t b_len)
{
    uint32_t min_len = a_len < b_len ? a_len : b_len;
    int cmp = min_len > 0 ? memcmp(a, b, min_len) : 0;
    if (cmp != 0) return cmp;
    if (a_len < b_len) return -1;
    if (a_len > b_len) return  1;
    return 0;
}

static tdc_status compute_string_stats(const tdc_block *blk,
                                       tdc_column_stats *out)
{
    int64_t n = tdc_shape_n_elems(&blk->shape);
    if (n <= 0) {
        memset(out, 0, sizeof(*out));
        return TDC_OK;
    }

    const uint8_t  *heap    = (const uint8_t *)blk->data;
    const uint32_t *offsets = blk->offsets;

    /* First element as initial min and max. */
    uint32_t min_off = offsets[0];
    uint32_t min_len = offsets[1] - offsets[0];
    uint32_t max_off = min_off;
    uint32_t max_len = min_len;

    for (int64_t i = 1; i < n; ++i) {
        uint32_t off = offsets[i];
        uint32_t len = offsets[i + 1] - offsets[i];
        if (string_cmp(heap + off, len, heap + min_off, min_len) < 0) {
            min_off = off;
            min_len = len;
        }
        if (string_cmp(heap + off, len, heap + max_off, max_len) > 0) {
            max_off = off;
            max_len = len;
        }
    }

    out->has_stats = 1;
    stats_store_string_prefix(out->min, heap + min_off, min_len);
    stats_store_string_prefix(out->max, heap + max_off, max_len);
    return TDC_OK;
}

/* ---- public API --------------------------------------------------------- */

void tdc_stats_store_value(uint8_t dst[TDC_STATS_VALUE_SIZE],
                           const void *value, tdc_dtype dtype)
{
    size_t sz = tdc_dtype_size(dtype);
    memset(dst, 0, TDC_STATS_VALUE_SIZE);
    if (sz > 0 && sz <= TDC_STATS_VALUE_SIZE) {
        memcpy(dst, value, sz);
    }
}

void tdc_stats_load_value(const uint8_t src[TDC_STATS_VALUE_SIZE],
                          void *dst, tdc_dtype dtype)
{
    size_t sz = tdc_dtype_size(dtype);
    if (sz > 0 && sz <= TDC_STATS_VALUE_SIZE) {
        memcpy(dst, src, sz);
    }
}

tdc_status tdc_stats_compute(const tdc_block *blk, tdc_column_stats *out)
{
    if (!blk || !out) return TDC_E_INVAL;

    memset(out, 0, sizeof(*out));

    int64_t n = tdc_shape_n_elems(&blk->shape);
    if (n <= 0) {
        /* Empty block: no stats. */
        return TDC_OK;
    }

    if (!blk->data) return TDC_E_INVAL;

    /* String dtype has its own path. */
    if (blk->dtype == TDC_DT_STRING) {
        if (!blk->offsets) return TDC_E_INVAL;
        return compute_string_stats(blk, out);
    }

    /* Fixed-width numeric dtypes: dispatch scanner, then store results. */
    const void *ptr_min = NULL;
    const void *ptr_max = NULL;

    switch (blk->dtype) {
        case TDC_DT_I8:  scan_i8 (blk->data, n, &ptr_min, &ptr_max); break;
        case TDC_DT_I16: scan_i16(blk->data, n, &ptr_min, &ptr_max); break;
        case TDC_DT_I32: scan_i32(blk->data, n, &ptr_min, &ptr_max); break;
        case TDC_DT_I64: scan_i64(blk->data, n, &ptr_min, &ptr_max); break;
        case TDC_DT_U8:  scan_u8 (blk->data, n, &ptr_min, &ptr_max); break;
        case TDC_DT_U16: scan_u16(blk->data, n, &ptr_min, &ptr_max); break;
        case TDC_DT_U32: scan_u32(blk->data, n, &ptr_min, &ptr_max); break;
        case TDC_DT_U64: scan_u64(blk->data, n, &ptr_min, &ptr_max); break;
        case TDC_DT_F16: scan_f16(blk->data, n, &ptr_min, &ptr_max); break;
        case TDC_DT_F32: scan_f32(blk->data, n, &ptr_min, &ptr_max); break;
        case TDC_DT_F64: scan_f64(blk->data, n, &ptr_min, &ptr_max); break;
        default:
            /* Unknown dtype: no stats. */
            return TDC_OK;
    }

    out->has_stats = 1;
    tdc_stats_store_value(out->min, ptr_min, blk->dtype);
    tdc_stats_store_value(out->max, ptr_max, blk->dtype);
    return TDC_OK;
}

size_t tdc_stats_serialize(const tdc_column_stats *stats, uint16_t n_cols,
                           uint8_t *buf)
{
    if (!stats || !buf) return 0;

    for (uint16_t i = 0; i < n_cols; ++i) {
        uint8_t *dst = buf + (size_t)i * TDC_STATS_ENTRY_SIZE;
        dst[0] = stats[i].has_stats;
        memcpy(dst + 1,  stats[i].min, TDC_STATS_VALUE_SIZE);
        memcpy(dst + 1 + TDC_STATS_VALUE_SIZE, stats[i].max, TDC_STATS_VALUE_SIZE);
        memcpy(dst + 1 + 2 * TDC_STATS_VALUE_SIZE, &stats[i].null_count,
               sizeof(uint64_t));
    }

    return (size_t)n_cols * TDC_STATS_ENTRY_SIZE;
}

tdc_status tdc_stats_parse(const uint8_t *buf, size_t buf_size,
                           uint16_t n_cols, tdc_column_stats *out)
{
    if (!buf || !out) return TDC_E_INVAL;

    size_t required = (size_t)n_cols * TDC_STATS_ENTRY_SIZE;
    if (buf_size < required) return TDC_E_BUF_TOO_SMALL;

    for (uint16_t i = 0; i < n_cols; ++i) {
        const uint8_t *src = buf + (size_t)i * TDC_STATS_ENTRY_SIZE;
        out[i].has_stats = src[0];
        memcpy(out[i].min, src + 1, TDC_STATS_VALUE_SIZE);
        memcpy(out[i].max, src + 1 + TDC_STATS_VALUE_SIZE, TDC_STATS_VALUE_SIZE);
        memcpy(&out[i].null_count, src + 1 + 2 * TDC_STATS_VALUE_SIZE,
               sizeof(uint64_t));
    }

    return TDC_OK;
}
