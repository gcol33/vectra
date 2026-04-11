#include "vtr_codec_internal.h"
#include "array.h"
#include "error.h"

#include <stdlib.h>
#include <string.h>
#include <math.h>

/* ================================================================
 * RLE helpers — for dictionary indices
 *
 * Encodes runs of repeated uint32 values:
 *   uint32_t n_runs
 *   [uint32_t value, uint32_t run_length] * n_runs
 *
 * Decodes back to a flat uint32 array.
 * ================================================================ */

/* Count the number of runs in indices[0..n-1]. */
static uint32_t rle_count_runs(const uint32_t *indices, int64_t n) {
    if (n == 0) return 0;
    uint32_t runs = 1;
    for (int64_t i = 1; i < n; i++) {
        if (indices[i] != indices[i - 1])
            runs++;
    }
    return runs;
}

/* RLE-encode indices[0..n-1].
 * Returns malloc'd buffer; sets *out_size.
 * Layout: n_runs(u32) + [value(u32) + run_length(u32)] * n_runs */
static uint8_t *rle_encode_u32(const uint32_t *indices, int64_t n,
                               uint32_t *out_size) {
    uint32_t n_runs = rle_count_runs(indices, n);
    /* 4 bytes for n_runs + 8 bytes per run */
    uint32_t buf_size = 4 + n_runs * 8;
    uint8_t *buf = (uint8_t *)malloc(buf_size);
    if (!buf) vectra_error("alloc failed in rle_encode_u32");

    uint8_t *p = buf;
    memcpy(p, &n_runs, 4); p += 4;

    if (n == 0) {
        *out_size = buf_size;
        return buf;
    }

    uint32_t cur_val = indices[0];
    uint32_t cur_len = 1;
    for (int64_t i = 1; i < n; i++) {
        if (indices[i] == cur_val) {
            cur_len++;
        } else {
            memcpy(p, &cur_val, 4); p += 4;
            memcpy(p, &cur_len, 4); p += 4;
            cur_val = indices[i];
            cur_len = 1;
        }
    }
    /* Flush last run */
    memcpy(p, &cur_val, 4); p += 4;
    memcpy(p, &cur_len, 4); p += 4;

    *out_size = buf_size;
    return buf;
}

/* ================================================================
 * PLAIN encoding — just serialize the raw column data to a byte buffer
 * ================================================================ */

uint8_t *plain_encode(const VecArray *col, int64_t n_rows,
                      uint32_t *out_size) {
    uint8_t *buf = NULL;
    uint32_t size = 0;

    switch (col->type) {
    case VEC_INT64:
        size = (uint32_t)(n_rows * 8);
        buf = (uint8_t *)malloc(size);
        if (!buf) vectra_error("alloc failed");
        memcpy(buf, col->buf.i64, size);
        break;
    case VEC_INT32:
        size = (uint32_t)(n_rows * 4);
        buf = (uint8_t *)malloc(size);
        if (!buf) vectra_error("alloc failed");
        memcpy(buf, col->buf.i32, size);
        break;
    case VEC_INT16:
        size = (uint32_t)(n_rows * 2);
        buf = (uint8_t *)malloc(size);
        if (!buf) vectra_error("alloc failed");
        memcpy(buf, col->buf.i16, size);
        break;
    case VEC_INT8:
        size = (uint32_t)n_rows;
        buf = (uint8_t *)malloc(size > 0 ? size : 1);
        if (!buf) vectra_error("alloc failed");
        if (size > 0) memcpy(buf, col->buf.i8, size);
        break;
    case VEC_DOUBLE:
        size = (uint32_t)(n_rows * 8);
        buf = (uint8_t *)malloc(size);
        if (!buf) vectra_error("alloc failed");
        memcpy(buf, col->buf.dbl, size);
        break;
    case VEC_BOOL:
        size = (uint32_t)n_rows;
        buf = (uint8_t *)malloc(size > 0 ? size : 1);
        if (!buf) vectra_error("alloc failed");
        if (size > 0) memcpy(buf, col->buf.bln, size);
        break;
    case VEC_STRING: {
        /* offsets (n_rows+1 * 8) + data_len (8) + data */
        uint32_t off_size = (uint32_t)((n_rows + 1) * 8);
        uint64_t dl = (uint64_t)col->buf.str.data_len;
        size = off_size + 8 + (uint32_t)col->buf.str.data_len;
        buf = (uint8_t *)malloc(size > 0 ? size : 1);
        if (!buf) vectra_error("alloc failed");
        memcpy(buf, col->buf.str.offsets, off_size);
        memcpy(buf + off_size, &dl, 8);
        if (col->buf.str.data_len > 0)
            memcpy(buf + off_size + 8, col->buf.str.data,
                   (size_t)col->buf.str.data_len);
        break;
    }
    }

    *out_size = size;
    return buf;
}

void plain_decode(VecArray *col, int64_t n_rows,
                  const uint8_t *data, uint32_t data_size) {
    switch (col->type) {
    case VEC_INT64:
        col->buf.i64 = (int64_t *)malloc((size_t)(n_rows * 8));
        if (!col->buf.i64) vectra_error("alloc failed");
        memcpy(col->buf.i64, data, (size_t)(n_rows * 8));
        break;
    case VEC_INT32:
        col->buf.i32 = (int32_t *)malloc((size_t)(n_rows * 4));
        if (!col->buf.i32) vectra_error("alloc failed");
        memcpy(col->buf.i32, data, (size_t)(n_rows * 4));
        break;
    case VEC_INT16:
        col->buf.i16 = (int16_t *)malloc((size_t)(n_rows * 2));
        if (!col->buf.i16) vectra_error("alloc failed");
        memcpy(col->buf.i16, data, (size_t)(n_rows * 2));
        break;
    case VEC_INT8:
        col->buf.i8 = (int8_t *)malloc(n_rows > 0 ? (size_t)n_rows : 1);
        if (!col->buf.i8) vectra_error("alloc failed");
        if (n_rows > 0) memcpy(col->buf.i8, data, (size_t)n_rows);
        break;
    case VEC_DOUBLE:
        col->buf.dbl = (double *)malloc((size_t)(n_rows * 8));
        if (!col->buf.dbl) vectra_error("alloc failed");
        memcpy(col->buf.dbl, data, (size_t)(n_rows * 8));
        break;
    case VEC_BOOL:
        col->buf.bln = (uint8_t *)malloc(n_rows > 0 ? (size_t)n_rows : 1);
        if (!col->buf.bln) vectra_error("alloc failed");
        if (n_rows > 0) memcpy(col->buf.bln, data, (size_t)n_rows);
        break;
    case VEC_STRING: {
        uint32_t off_size = (uint32_t)((n_rows + 1) * 8);
        col->buf.str.offsets = (int64_t *)malloc(off_size);
        if (!col->buf.str.offsets) vectra_error("alloc failed");
        memcpy(col->buf.str.offsets, data, off_size);

        uint64_t dl;
        memcpy(&dl, data + off_size, 8);
        col->buf.str.data_len = (int64_t)dl;
        col->buf.str.data = (char *)malloc(dl > 0 ? (size_t)dl : 1);
        if (!col->buf.str.data) vectra_error("alloc failed");
        if (dl > 0)
            memcpy(col->buf.str.data, data + off_size + 8, (size_t)dl);
        break;
    }
    }
}

/* ================================================================
 * DICTIONARY encoding — for string columns
 *
 * On-disk layout (before compression):
 *   uint32_t dict_count          — number of unique strings
 *   int64_t  dict_offsets[dict_count+1] — byte offsets into dict_data
 *   char     dict_data[...]      — concatenated unique strings
 *   RLE-encoded indices:
 *     uint32_t n_runs
 *     [uint32_t value, uint32_t run_length] * n_runs
 *
 * Uses open-addressing hash table with dynamic resizing for O(1) amortized
 * lookups. Single pass: builds dictionary + index array simultaneously.
 * Returns NULL if n_unique/n_rows >= 0.5 (not worth encoding).
 * ================================================================ */

/* Open-addressing dict hash table slot */
typedef struct {
    uint32_t hash;       /* stored hash (0 = empty slot) */
    uint32_t idx;        /* dictionary index */
    const char *str;     /* pointer into column string data */
    int64_t len;         /* string length */
} DictSlot;

static inline uint32_t dict_fnv1a(const char *str, int64_t len) {
    uint32_t h = 2166136261u;
    for (int64_t i = 0; i < len; i++) {
        h ^= (uint8_t)str[i];
        h *= 16777619u;
    }
    return h | 1u; /* ensure non-zero (0 = empty sentinel) */
}

/* Try dictionary encoding in a single pass. Returns encoded buffer on
 * success, NULL if not worthwhile (too many uniques). */
uint8_t *try_dict_encode(const VecArray *col, int64_t n_rows,
                         uint32_t *out_size) {
    if (col->type != VEC_STRING || n_rows < 2) return NULL;

    /* Open-addressing hash table, starts at 256, resizes at 70% load */
    uint32_t ht_cap = 256;
    while (ht_cap < 4) ht_cap *= 2; /* safety */
    DictSlot *ht = (DictSlot *)calloc((size_t)ht_cap, sizeof(DictSlot));
    if (!ht) return NULL;
    uint32_t ht_used = 0;
    uint32_t ht_max = (uint32_t)(ht_cap * 0.7);

    /* Dict entries (grow as needed) */
    uint32_t dict_cap = 256;
    const char **dict_strs = (const char **)malloc(dict_cap * sizeof(char *));
    int64_t *dict_lens = (int64_t *)malloc(dict_cap * sizeof(int64_t));

    /* Index array — one entry per row */
    uint32_t *indices = (uint32_t *)malloc((size_t)n_rows * sizeof(uint32_t));
    if (!dict_strs || !dict_lens || !indices) {
        free(ht); free(dict_strs); free(dict_lens); free(indices);
        return NULL;
    }

    uint32_t n_unique = 0;
    uint32_t threshold = (uint32_t)(n_rows / 2);

    for (int64_t i = 0; i < n_rows; i++) {
        if (!vec_array_is_valid(col, i)) {
            indices[i] = 0; /* placeholder */
            continue;
        }

        int64_t off = col->buf.str.offsets[i];
        int64_t slen = col->buf.str.offsets[i + 1] - off;
        const char *s = col->buf.str.data + off;
        uint32_t h = dict_fnv1a(s, slen);
        uint32_t mask = ht_cap - 1;
        uint32_t slot = h & mask;

        /* Linear probe */
        for (;;) {
            if (ht[slot].hash == 0) {
                /* Empty slot — new unique string */
                if (n_unique >= threshold) {
                    /* Too many uniques — abort */
                    free(ht); free(dict_strs); free(dict_lens); free(indices);
                    return NULL;
                }
                /* Grow dict arrays if needed */
                if (n_unique >= dict_cap) {
                    dict_cap *= 2;
                    dict_strs = (const char **)realloc(dict_strs, dict_cap * sizeof(char *));
                    dict_lens = (int64_t *)realloc(dict_lens, dict_cap * sizeof(int64_t));
                    if (!dict_strs || !dict_lens) vectra_error("alloc failed");
                }
                ht[slot].hash = h;
                ht[slot].idx = n_unique;
                ht[slot].str = s;
                ht[slot].len = slen;
                dict_strs[n_unique] = s;
                dict_lens[n_unique] = slen;
                indices[i] = n_unique;
                n_unique++;
                ht_used++;

                /* Resize hash table at 70% load */
                if (ht_used >= ht_max) {
                    uint32_t new_cap = ht_cap * 2;
                    DictSlot *new_ht = (DictSlot *)calloc((size_t)new_cap, sizeof(DictSlot));
                    if (!new_ht) vectra_error("alloc failed");
                    uint32_t new_mask = new_cap - 1;
                    for (uint32_t j = 0; j < ht_cap; j++) {
                        if (ht[j].hash == 0) continue;
                        uint32_t ns = ht[j].hash & new_mask;
                        while (new_ht[ns].hash != 0) ns = (ns + 1) & new_mask;
                        new_ht[ns] = ht[j];
                    }
                    free(ht);
                    ht = new_ht;
                    ht_cap = new_cap;
                    ht_max = (uint32_t)(ht_cap * 0.7);
                }
                break;
            }
            if (ht[slot].hash == h &&
                ht[slot].len == slen &&
                memcmp(ht[slot].str, s, (size_t)slen) == 0) {
                /* Found existing entry */
                indices[i] = ht[slot].idx;
                break;
            }
            slot = (slot + 1) & mask;
        }
    }

    free(ht);

    /* RLE-encode the indices */
    uint32_t rle_size = 0;
    uint8_t *rle_buf = rle_encode_u32(indices, n_rows, &rle_size);
    free(indices);

    /* Compute total dictionary data size */
    int64_t total_dict_data = 0;
    for (uint32_t d = 0; d < n_unique; d++)
        total_dict_data += dict_lens[d];

    /* Layout:
     *   4 bytes: dict_count (uint32)
     *   (dict_count+1)*8 bytes: dict offsets (int64)
     *   total_dict_data bytes: dict string data
     *   rle_size bytes: RLE-encoded indices
     */
    uint32_t dict_header_size = 4 + (uint32_t)((n_unique + 1) * 8) +
                                (uint32_t)total_dict_data;
    uint32_t buf_size = dict_header_size + rle_size;
    uint8_t *buf = (uint8_t *)malloc(buf_size);
    if (!buf) vectra_error("alloc failed");

    uint8_t *p = buf;
    memcpy(p, &n_unique, 4); p += 4;

    int64_t running = 0;
    for (uint32_t d = 0; d < n_unique; d++) {
        memcpy(p + d * 8, &running, 8);
        running += dict_lens[d];
    }
    memcpy(p + n_unique * 8, &running, 8);
    p += (n_unique + 1) * 8;

    for (uint32_t d = 0; d < n_unique; d++) {
        memcpy(p, dict_strs[d], (size_t)dict_lens[d]);
        p += dict_lens[d];
    }
    memcpy(p, rle_buf, rle_size);

    free(rle_buf);
    free(dict_strs);
    free(dict_lens);

    *out_size = buf_size;
    return buf;
}

void dict_decode(VecArray *col, int64_t n_rows,
                 const uint8_t *data, uint32_t data_size) {
    const uint8_t *p = data;

    /* dict_count */
    uint32_t dict_count;
    memcpy(&dict_count, p, 4); p += 4;

    /* dict offsets — copy into aligned buffer before use */
    int64_t *dict_offsets = (int64_t *)malloc((dict_count + 1) * 8);
    if (!dict_offsets) vectra_error("alloc failed in dict_decode");
    memcpy(dict_offsets, p, (dict_count + 1) * 8);
    p += (dict_count + 1) * 8;

    /* Precompute per-dictionary-entry string lengths */
    int64_t *dict_lens = (int64_t *)malloc((size_t)dict_count * sizeof(int64_t));
    if (!dict_lens) vectra_error("alloc failed in dict_decode");
    for (uint32_t d = 0; d < dict_count; d++)
        dict_lens[d] = dict_offsets[d + 1] - dict_offsets[d];

    /* dict data */
    int64_t total_dict_data = dict_offsets[dict_count];
    const char *dict_data = (const char *)p;
    p += total_dict_data;

    /* Process RLE runs directly — avoid expanding to flat index array.
       Two passes: first compute total string data size, then fill. */
    uint32_t n_runs;
    memcpy(&n_runs, p, 4); p += 4;

    /* Pass 1: compute total output string data */
    int64_t total_str_data = 0;
    {
        const uint8_t *rp = p;
        int64_t row = 0;
        for (uint32_t r = 0; r < n_runs && row < n_rows; r++) {
            uint32_t val, len;
            memcpy(&val, rp, 4); rp += 4;
            memcpy(&len, rp, 4); rp += 4;
            int64_t slen = dict_lens[val];
            for (uint32_t k = 0; k < len && row < n_rows; k++, row++) {
                if (vec_array_is_valid(col, row))
                    total_str_data += slen;
            }
        }
    }

    col->buf.str.offsets = (int64_t *)malloc((size_t)((n_rows + 1) * 8));
    col->buf.str.data = (char *)malloc(total_str_data > 0 ? (size_t)total_str_data : 1);
    if (!col->buf.str.offsets || !col->buf.str.data)
        vectra_error("alloc failed in dict_decode");
    col->buf.str.data_len = total_str_data;

    /* Pass 2: fill offsets and string data, processing per-run */
    int64_t pos = 0;
    int64_t row = 0;
    for (uint32_t r = 0; r < n_runs && row < n_rows; r++) {
        uint32_t val, len;
        memcpy(&val, p, 4); p += 4;
        memcpy(&len, p, 4); p += 4;
        int64_t slen = dict_lens[val];
        const char *sptr = dict_data + dict_offsets[val];

        for (uint32_t k = 0; k < len && row < n_rows; k++, row++) {
            col->buf.str.offsets[row] = pos;
            if (vec_array_is_valid(col, row)) {
                if (slen > 0)
                    memcpy(col->buf.str.data + pos, sptr, (size_t)slen);
                pos += slen;
            }
        }
    }
    col->buf.str.offsets[n_rows] = pos;

    free(dict_lens);
    free(dict_offsets);
}

/* ================================================================
 * DICT_NUM encoding — numeric dictionary for int64 / double columns
 *
 * On-disk layout (before compression):
 *   uint32_t dict_count
 *   uint8_t  idx_width     (1 = u8, 2 = u16)
 *   uint8_t  value_bytes   (8 for int64/double)
 *   uint16_t reserved      (0)
 *   dict_count × value_bytes bytes — unique values in insertion order
 *   n_rows × idx_width bytes — per-row indices
 *
 * Single-pass open-addressing hash on the raw 8-byte bits (so bitwise-
 * identical doubles dedupe; NaN patterns keyed on their exact bits).
 * Aborts if n_unique ≥ n_rows/4 or > 65536.
 *
 * NA rows store index 0 as a placeholder — the validity bitmap is the
 * authoritative NA signal so the garbage-value slot is never read by
 * downstream code. If ALL rows are NA (n_unique == 0), the encoder
 * returns NULL so the caller falls through to PLAIN.
 * ================================================================ */

#define DICT_NUM_MAX_UNIQUE 65536u
#define DICT_NUM_HEADER_SIZE 8u  /* u32 dict_count + u8 idx_w + u8 vb + u16 rsv */

typedef struct {
    uint64_t key;  /* raw bits of the 8-byte value (0 = empty slot) */
    uint32_t idx;  /* dictionary index */
    uint32_t used; /* 1 if slot occupied (distinguishes key==0 from empty) */
} DictNumSlot;

static inline uint64_t dict_num_mix(uint64_t x) {
    /* splitmix64 — fast, high-quality scalar hash */
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    x = x ^ (x >> 31);
    return x;
}

uint8_t *try_dict_num_encode(const VecArray *col, int64_t n_rows,
                             uint32_t *out_size) {
    if (col->type != VEC_INT64 && col->type != VEC_DOUBLE) return NULL;
    if (n_rows < 2) return NULL;

    const uint64_t *src64;
    if (col->type == VEC_INT64)
        src64 = (const uint64_t *)col->buf.i64;
    else
        src64 = (const uint64_t *)col->buf.dbl;

    /* Cardinality cutoff: abort once we exceed min(n_rows/4, 65536). */
    uint32_t max_unique = (uint32_t)(n_rows / 4);
    if (max_unique > DICT_NUM_MAX_UNIQUE) max_unique = DICT_NUM_MAX_UNIQUE;
    if (max_unique < 2) return NULL; /* column too small for dict to pay */

    /* Open-addressing hash table, capacity power-of-two, 70% load. */
    uint32_t ht_cap = 256u;
    while (ht_cap < max_unique * 2u && ht_cap < DICT_NUM_MAX_UNIQUE * 2u)
        ht_cap *= 2u;
    DictNumSlot *ht = (DictNumSlot *)calloc(ht_cap, sizeof(DictNumSlot));
    if (!ht) return NULL;

    uint64_t *dict_vals = (uint64_t *)malloc(max_unique * sizeof(uint64_t));
    if (!dict_vals) { free(ht); return NULL; }

    uint32_t n_unique = 0u;
    uint32_t *idx32 = (uint32_t *)malloc((size_t)n_rows * sizeof(uint32_t));
    if (!idx32) { free(ht); free(dict_vals); return NULL; }

    uint32_t mask = ht_cap - 1u;
    for (int64_t i = 0; i < n_rows; i++) {
        if (!vec_array_is_valid(col, i)) {
            idx32[i] = 0u; /* placeholder */
            continue;
        }
        uint64_t v = src64[i];
        uint32_t slot = (uint32_t)(dict_num_mix(v) & mask);
        for (;;) {
            if (!ht[slot].used) {
                if (n_unique >= max_unique) {
                    free(ht); free(dict_vals); free(idx32);
                    return NULL;
                }
                ht[slot].key = v;
                ht[slot].idx = n_unique;
                ht[slot].used = 1u;
                dict_vals[n_unique] = v;
                idx32[i] = n_unique;
                n_unique++;
                break;
            }
            if (ht[slot].key == v) {
                idx32[i] = ht[slot].idx;
                break;
            }
            slot = (slot + 1u) & mask;
        }
    }

    free(ht);

    if (n_unique == 0u || n_unique > DICT_NUM_MAX_UNIQUE) {
        free(dict_vals); free(idx32);
        return NULL;
    }

    /* Pick idx_width: 1 byte if cardinality fits, else 2. */
    uint8_t idx_width = (n_unique <= 256u) ? 1u : 2u;
    uint8_t value_bytes = 8u;

    /* Output size check — must actually shrink vs PLAIN. PLAIN for an
     * 8-byte numeric column is n_rows * 8 bytes. We spend:
     *   header (8) + n_unique*8 + n_rows*idx_width
     * For this to be smaller we need idx_width < 8, which is always true,
     * but for very tiny columns the dict header can dominate — require at
     * least 8× size reduction versus PLAIN, otherwise bail and let LZ on
     * the PLAIN stream do its thing. */
    uint32_t raw_size = DICT_NUM_HEADER_SIZE +
                        n_unique * value_bytes +
                        (uint32_t)n_rows * idx_width;
    uint32_t plain_size = (uint32_t)n_rows * 8u;
    if (raw_size * 4u > plain_size * 3u) {
        /* Dict is >= 75% of PLAIN; not worth the downstream LZ penalty
         * on the index stream. */
        free(dict_vals); free(idx32);
        return NULL;
    }

    uint8_t *buf = (uint8_t *)malloc(raw_size);
    if (!buf) { free(dict_vals); free(idx32); return NULL; }

    uint8_t *p = buf;
    memcpy(p, &n_unique, 4); p += 4;
    p[0] = idx_width;
    p[1] = value_bytes;
    p[2] = 0; p[3] = 0;
    p += 4;

    memcpy(p, dict_vals, (size_t)n_unique * 8u);
    p += (size_t)n_unique * 8u;

    if (idx_width == 1u) {
        for (int64_t i = 0; i < n_rows; i++) p[i] = (uint8_t)idx32[i];
    } else {
        uint16_t *p16 = (uint16_t *)p;
        for (int64_t i = 0; i < n_rows; i++) p16[i] = (uint16_t)idx32[i];
    }

    free(dict_vals);
    free(idx32);

    *out_size = raw_size;
    return buf;
}

/* Parse a DICT_NUM header, returning pointers/counts and the start of the
 * index stream. Returns 0 on malformed input, 1 on success. */
int dict_num_parse_header(const uint8_t *data, uint32_t data_size,
                          uint32_t *out_dict_count,
                          uint8_t *out_idx_width,
                          uint8_t *out_value_bytes,
                          const uint8_t **out_dict_vals,
                          const uint8_t **out_indices) {
    if (data_size < DICT_NUM_HEADER_SIZE) return 0;
    uint32_t dict_count;
    memcpy(&dict_count, data, 4);
    uint8_t idx_width = data[4];
    uint8_t value_bytes = data[5];
    if ((idx_width != 1u && idx_width != 2u) || value_bytes != 8u) return 0;
    size_t dict_bytes = (size_t)dict_count * value_bytes;
    if (data_size < DICT_NUM_HEADER_SIZE + dict_bytes) return 0;
    *out_dict_count = dict_count;
    *out_idx_width = idx_width;
    *out_value_bytes = value_bytes;
    *out_dict_vals = data + DICT_NUM_HEADER_SIZE;
    *out_indices = data + DICT_NUM_HEADER_SIZE + dict_bytes;
    return 1;
}

/* Fan out dict indices into a fixed-width destination buffer. Works for
 * any 8-byte numeric type since dict values are stored as raw 8-byte
 * blobs and the destination is treated as uint64_t[]. */
void dict_num_fanout_u64(uint64_t *dst, int64_t n_rows,
                         const uint8_t *dict_vals, uint32_t dict_count,
                         const uint8_t *indices, uint8_t idx_width) {
    const uint64_t *dv = (const uint64_t *)dict_vals;
    (void)dict_count; /* indices are validated by encoder; trust on decode */

    if (idx_width == 1u) {
        for (int64_t i = 0; i < n_rows; i++) dst[i] = dv[indices[i]];
    } else {
        const uint16_t *p16 = (const uint16_t *)indices;
        for (int64_t i = 0; i < n_rows; i++) dst[i] = dv[p16[i]];
    }
}

void dict_num_decode(VecArray *col, int64_t n_rows,
                     const uint8_t *data, uint32_t data_size) {
    uint32_t dict_count;
    uint8_t idx_width, value_bytes;
    const uint8_t *dict_vals;
    const uint8_t *indices;
    if (!dict_num_parse_header(data, data_size, &dict_count, &idx_width,
                               &value_bytes, &dict_vals, &indices))
        vectra_error("dict_num_decode: malformed header");

    uint64_t *dst = NULL;
    if (col->type == VEC_INT64) {
        col->buf.i64 = (int64_t *)malloc((size_t)n_rows * 8u);
        if (!col->buf.i64) vectra_error("alloc failed in dict_num_decode");
        dst = (uint64_t *)col->buf.i64;
    } else if (col->type == VEC_DOUBLE) {
        col->buf.dbl = (double *)malloc((size_t)n_rows * 8u);
        if (!col->buf.dbl) vectra_error("alloc failed in dict_num_decode");
        dst = (uint64_t *)col->buf.dbl;
    } else {
        vectra_error("dict_num_decode: unsupported column type");
    }
    dict_num_fanout_u64(dst, n_rows, dict_vals, dict_count, indices, idx_width);
}

/* ================================================================
 * SPARSE_ZERO encoding — for int64 / double columns where most rows are
 * exactly zero (0 / 0.0). Stores a list of non-zero (position, value)
 * pairs as two separate byte-shuffled + internally-LZ-compressed
 * streams. Positions are stored as int64 gaps (deltas between
 * consecutive non-zero row indices), which are small positive integers
 * so 6-7 of the 8 byte-lanes are all zero after shuffling. Values are
 * stored byte-shuffled so the IEEE754 sign+exponent lane clusters
 * under entropy coding.
 *
 * Compression is performed internally per stream so the two halves do
 * not share entropy tables with each other (mixing gaps + values in one
 * LZ block badly hurts the low-entropy gaps stream). The outer
 * compression layer sees a self-contained pre-compressed blob and
 * stores it verbatim with compression = VTR_COMP_NONE.
 *
 * On-disk layout (the whole column data):
 *   uint32_t n_nonzero
 *   uint8_t  value_bytes   (8 for int64/double)
 *   uint8_t  gaps_comp_tag (VTR_COMP_* for gaps stream; NONE means raw)
 *   uint8_t  vals_comp_tag (VTR_COMP_* for values stream)
 *   uint8_t  reserved      (0)
 *   uint32_t gaps_stream_size
 *   uint32_t vals_stream_size
 *   gaps_stream_size bytes — shuffled int64 gaps (raw or LZ-compressed)
 *   vals_stream_size bytes — shuffled int64 values (raw or LZ-compressed)
 *
 * NA rows are treated as "zero" (they are not emitted). The validity
 * bitmap overrides the decoded value anyway.
 * ================================================================ */

#define SPARSE_ZERO_HEADER_SIZE 16u
#define SPARSE_ZERO_MIN_RATIO 3u /* at least 75% zeros */

/* Per-stream candidate menus used by try_sparse_zero_encode. Gaps are
 * tiny skewed integers (usually runs of 1 with occasional larger jumps);
 * after byte-shuffle they have a narrow byte alphabet that FSE / Huffman
 * direct can pack very tightly, often beating any LZ variant. Values are
 * 8-byte doubles or int64s whose high-byte lanes are effectively random,
 * so direct entropy is dead weight — we stick to the LZ family. */
static const VtrCandidate sparse_fast_menu[] = {
    { VTR_COMP_SHUFFLE_LZ, lz_vtr_compress },
};
static const VtrCandidate sparse_gaps_small_menu[] = {
    { VTR_COMP_SHUFFLE_LZ,         lz_vtr_compress },
    { VTR_COMP_SHUFFLE_LZ_STREAMS, lz_streams_vtr_compress },
    { VTR_COMP_SHUFFLE_FSE,        fse_vtr_compress },
    { VTR_COMP_SHUFFLE_HUFF,       huffman_vtr_compress },
};
static const VtrCandidate sparse_vals_small_menu[] = {
    { VTR_COMP_SHUFFLE_LZ,         lz_vtr_compress },
    { VTR_COMP_SHUFFLE_LZ_STREAMS, lz_streams_vtr_compress },
};

/* Compress a shuffled sparse stream against the given candidate menu.
 * Returns a malloc'd compressed buffer and sets *out_comp_tag. If no
 * candidate shrinks the input, returns NULL and the caller stores the
 * stream raw (with out_comp_tag = VTR_COMP_NONE). */
static uint8_t *sparse_stream_compress(const uint8_t *shuffled, uint32_t size,
                                       const VtrCandidate *cands, size_t n_cands,
                                       uint32_t *out_size,
                                       uint8_t *out_comp_tag) {
    *out_size = 0;
    *out_comp_tag = VTR_COMP_NONE;

    if (size <= COMPRESS_THRESHOLD) return NULL;

    return vtr_try_candidates(shuffled, size, cands, n_cands,
                              out_size, out_comp_tag);
}

static void sparse_stream_decompress(uint8_t *dst, uint32_t uncomp_size,
                                     const uint8_t *src, uint32_t src_size,
                                     uint8_t comp_tag) {
    if (comp_tag == VTR_COMP_NONE) {
        if (src_size != uncomp_size)
            vectra_error("sparse stream raw size mismatch");
        memcpy(dst, src, uncomp_size);
        return;
    }
    if (comp_tag == VTR_COMP_SHUFFLE_LZ ||
        comp_tag == VTR_COMP_SHUFFLE_LZ_HUFF ||
        comp_tag == VTR_COMP_SHUFFLE_LZ_STREAMS ||
        comp_tag == VTR_COMP_SHUFFLE_FSE ||
        comp_tag == VTR_COMP_SHUFFLE_HUFF) {
        vtr_decompress_into(dst, uncomp_size, src, src_size, comp_tag);
        return;
    }
    vectra_error("sparse stream: unknown comp tag 0x%02x", comp_tag);
}

uint8_t *try_sparse_zero_encode(const VecArray *col, int64_t n_rows,
                                int comp_level, uint32_t *out_size) {
    if (col->type != VEC_INT64 && col->type != VEC_DOUBLE) return NULL;
    if (n_rows < 16) return NULL;
    if ((uint64_t)n_rows > UINT32_MAX) return NULL;

    const uint64_t *src64 = (col->type == VEC_INT64)
        ? (const uint64_t *)col->buf.i64
        : (const uint64_t *)col->buf.dbl;

    /* First pass: count non-zero (valid) rows. +0.0 and -0.0 have
     * different bit patterns (0x0 vs 0x80..0); we only collapse +0.
     * Negative zero is rare in practice and treating it as non-zero is
     * lossless — the decode will reproduce the exact bits. */
    int64_t n_nonzero = 0;
    for (int64_t i = 0; i < n_rows; i++) {
        if (!vec_array_is_valid(col, i)) continue;
        if (src64[i] != 0u) n_nonzero++;
    }

    /* Require at least SPARSE_ZERO_MIN_RATIO:1 zero-to-nonzero ratio.
     * Uncompressed gap+value pair cost is 16 bytes/nonzero; the
     * stricter 75% cutoff keeps us from invoking the sparse path on
     * moderately-dense columns where dict or plain will do better. */
    if ((uint64_t)n_nonzero * (uint64_t)(SPARSE_ZERO_MIN_RATIO + 1u) >
        (uint64_t)n_rows)
        return NULL;

    uint32_t stream_bytes = (uint32_t)n_nonzero * 8u;

    /* Gather gaps + values into scratch arrays. */
    int64_t  *gaps_scratch = NULL;
    uint64_t *vals_scratch = NULL;
    if (n_nonzero > 0) {
        gaps_scratch = (int64_t *)malloc((size_t)n_nonzero * 8u);
        vals_scratch = (uint64_t *)malloc((size_t)n_nonzero * 8u);
        if (!gaps_scratch || !vals_scratch) {
            free(gaps_scratch); free(vals_scratch);
            return NULL;
        }
    }

    int64_t vpos = 0;
    int64_t prev_pos = 0;
    for (int64_t i = 0; i < n_rows; i++) {
        if (!vec_array_is_valid(col, i)) continue;
        uint64_t v = src64[i];
        if (v != 0u) {
            gaps_scratch[vpos] = (vpos == 0) ? i : (i - prev_pos);
            vals_scratch[vpos] = v;
            prev_pos = i;
            vpos++;
        }
    }

    /* Shuffle both streams into temp buffers. */
    uint8_t *gaps_shuf = NULL, *vals_shuf = NULL;
    if (n_nonzero > 0) {
        gaps_shuf = (uint8_t *)malloc(stream_bytes);
        vals_shuf = (uint8_t *)malloc(stream_bytes);
        if (!gaps_shuf || !vals_shuf) {
            free(gaps_scratch); free(vals_scratch);
            free(gaps_shuf); free(vals_shuf);
            return NULL;
        }
        byte_shuffle(gaps_shuf, (const uint8_t *)gaps_scratch,
                     (uint32_t)n_nonzero, 8u);
        byte_shuffle(vals_shuf, (const uint8_t *)vals_scratch,
                     (uint32_t)n_nonzero, 8u);
    }
    free(gaps_scratch);
    free(vals_scratch);

    /* Compress each shuffled stream independently. */
    uint32_t gaps_comp_size = 0, vals_comp_size = 0;
    uint8_t  gaps_comp_tag  = VTR_COMP_NONE;
    uint8_t  vals_comp_tag  = VTR_COMP_NONE;
    uint8_t *gaps_comp = NULL, *vals_comp = NULL;

    if (n_nonzero > 0) {
        const VtrCandidate *gaps_menu;
        const VtrCandidate *vals_menu;
        size_t gaps_n, vals_n;
        if (comp_level == VTR_COMPRESS_FAST) {
            gaps_menu = sparse_fast_menu;
            gaps_n    = sizeof(sparse_fast_menu)/sizeof(sparse_fast_menu[0]);
            vals_menu = sparse_fast_menu;
            vals_n    = gaps_n;
        } else {
            gaps_menu = sparse_gaps_small_menu;
            gaps_n    = sizeof(sparse_gaps_small_menu)/sizeof(sparse_gaps_small_menu[0]);
            vals_menu = sparse_vals_small_menu;
            vals_n    = sizeof(sparse_vals_small_menu)/sizeof(sparse_vals_small_menu[0]);
        }
        gaps_comp = sparse_stream_compress(gaps_shuf, stream_bytes,
                                           gaps_menu, gaps_n,
                                           &gaps_comp_size, &gaps_comp_tag);
        vals_comp = sparse_stream_compress(vals_shuf, stream_bytes,
                                           vals_menu, vals_n,
                                           &vals_comp_size, &vals_comp_tag);
    }

    /* Pick final emitted bytes per stream (compressed if it shrank,
     * otherwise raw shuffled). */
    const uint8_t *gaps_emit;
    uint32_t       gaps_emit_size;
    if (gaps_comp) {
        gaps_emit = gaps_comp;
        gaps_emit_size = gaps_comp_size;
    } else {
        gaps_emit = gaps_shuf;
        gaps_emit_size = stream_bytes;
        gaps_comp_tag = VTR_COMP_NONE;
    }

    const uint8_t *vals_emit;
    uint32_t       vals_emit_size;
    if (vals_comp) {
        vals_emit = vals_comp;
        vals_emit_size = vals_comp_size;
    } else {
        vals_emit = vals_shuf;
        vals_emit_size = stream_bytes;
        vals_comp_tag = VTR_COMP_NONE;
    }

    uint32_t total_size = SPARSE_ZERO_HEADER_SIZE + gaps_emit_size + vals_emit_size;

    /* Only emit if total is smaller than PLAIN. */
    if ((uint64_t)total_size >= (uint64_t)n_rows * 8u) {
        free(gaps_comp); free(vals_comp);
        free(gaps_shuf); free(vals_shuf);
        return NULL;
    }

    uint8_t *buf = (uint8_t *)malloc(total_size);
    if (!buf) {
        free(gaps_comp); free(vals_comp);
        free(gaps_shuf); free(vals_shuf);
        return NULL;
    }

    uint32_t nz32 = (uint32_t)n_nonzero;
    memcpy(buf, &nz32, 4);
    buf[4] = 8u;              /* value_bytes */
    buf[5] = gaps_comp_tag;
    buf[6] = vals_comp_tag;
    buf[7] = 0;               /* reserved */
    memcpy(buf + 8,  &gaps_emit_size, 4);
    memcpy(buf + 12, &vals_emit_size, 4);
    memcpy(buf + SPARSE_ZERO_HEADER_SIZE, gaps_emit, gaps_emit_size);
    memcpy(buf + SPARSE_ZERO_HEADER_SIZE + gaps_emit_size,
           vals_emit, vals_emit_size);

    free(gaps_comp); free(vals_comp);
    free(gaps_shuf); free(vals_shuf);

    *out_size = total_size;
    return buf;
}

int sparse_zero_parse_header(const uint8_t *data, uint32_t data_size,
                             int64_t n_rows, SparseZeroView *v) {
    (void)n_rows;
    if (data_size < SPARSE_ZERO_HEADER_SIZE) return 0;
    memcpy(&v->n_nonzero, data, 4);
    uint8_t value_bytes = data[4];
    if (value_bytes != 8u) return 0;
    v->gaps_comp_tag = data[5];
    v->vals_comp_tag = data[6];
    memcpy(&v->gaps_stream_size, data + 8,  4);
    memcpy(&v->vals_stream_size, data + 12, 4);
    uint64_t need = (uint64_t)SPARSE_ZERO_HEADER_SIZE +
                    (uint64_t)v->gaps_stream_size +
                    (uint64_t)v->vals_stream_size;
    if ((uint64_t)data_size < need) return 0;
    v->gaps_bytes = data + SPARSE_ZERO_HEADER_SIZE;
    v->vals_bytes = data + SPARSE_ZERO_HEADER_SIZE + v->gaps_stream_size;
    return 1;
}

void sparse_zero_fanout_u64(uint64_t *dst, int64_t n_rows,
                            const SparseZeroView *v) {
    memset(dst, 0, (size_t)n_rows * 8u);
    if (v->n_nonzero == 0) return;

    uint32_t uncomp_bytes = v->n_nonzero * 8u;
    uint8_t  *gaps_shuf  = (uint8_t  *)malloc(uncomp_bytes);
    uint8_t  *vals_shuf  = (uint8_t  *)malloc(uncomp_bytes);
    int64_t  *gaps_plain = (int64_t  *)malloc(uncomp_bytes);
    uint64_t *vals_plain = (uint64_t *)malloc(uncomp_bytes);
    if (!gaps_shuf || !vals_shuf || !gaps_plain || !vals_plain) {
        free(gaps_shuf); free(vals_shuf);
        free(gaps_plain); free(vals_plain);
        vectra_error("alloc failed in sparse_zero_fanout");
    }

    sparse_stream_decompress(gaps_shuf, uncomp_bytes,
                             v->gaps_bytes, v->gaps_stream_size,
                             v->gaps_comp_tag);
    sparse_stream_decompress(vals_shuf, uncomp_bytes,
                             v->vals_bytes, v->vals_stream_size,
                             v->vals_comp_tag);

    byte_unshuffle((uint8_t *)gaps_plain, gaps_shuf, v->n_nonzero, 8u);
    byte_unshuffle((uint8_t *)vals_plain, vals_shuf, v->n_nonzero, 8u);

    free(gaps_shuf);
    free(vals_shuf);

    int64_t pos = 0;
    for (uint32_t k = 0; k < v->n_nonzero; k++) {
        pos += gaps_plain[k];
        if (pos < 0 || pos >= n_rows) {
            free(gaps_plain); free(vals_plain);
            vectra_error("sparse_zero_fanout: position out of range");
        }
        dst[pos] = vals_plain[k];
    }
    free(gaps_plain);
    free(vals_plain);
}

void sparse_zero_decode(VecArray *col, int64_t n_rows,
                        const uint8_t *data, uint32_t data_size) {
    SparseZeroView v;
    if (!sparse_zero_parse_header(data, data_size, n_rows, &v))
        vectra_error("sparse_zero_decode: malformed header");

    uint64_t *dst = NULL;
    if (col->type == VEC_INT64) {
        col->buf.i64 = (int64_t *)malloc((size_t)n_rows * 8u);
        if (!col->buf.i64) vectra_error("alloc failed in sparse_zero_decode");
        dst = (uint64_t *)col->buf.i64;
    } else if (col->type == VEC_DOUBLE) {
        col->buf.dbl = (double *)malloc((size_t)n_rows * 8u);
        if (!col->buf.dbl) vectra_error("alloc failed in sparse_zero_decode");
        dst = (uint64_t *)col->buf.dbl;
    } else {
        vectra_error("sparse_zero_decode: unsupported column type");
    }
    sparse_zero_fanout_u64(dst, n_rows, &v);
}

/* Parse a DICTIONARY chunk into an owned VtrDictBlob — the fast-path
 * alternative to dict_decode. Skips materializing the flat string buffer;
 * instead returns the dictionary table + (deinterleaved) RLE runs so the
 * caller can build an R STRSXP by interning each unique entry once and
 * dispatching per row via SET_STRING_ELT.
 *
 * All fields are owned heap allocations copied out of `src`, so the blob
 * survives the death of whatever scratch buffer produced `src`. */
VtrDictBlob *vtr_dict_parse_to_blob(const uint8_t *src, uint32_t src_size) {
    if (!src || src_size < 4) return NULL;

    VtrDictBlob *b = (VtrDictBlob *)calloc(1, sizeof(VtrDictBlob));
    if (!b) return NULL;

    const uint8_t *p = src;
    const uint8_t *end = src + src_size;

    /* dict_count */
    if (p + 4 > end) { vtr_dict_blob_free(b); return NULL; }
    memcpy(&b->dict_count, p, 4);
    p += 4;

    /* dict offsets — aligned copy */
    size_t off_bytes = (size_t)(b->dict_count + 1) * 8;
    if ((size_t)(end - p) < off_bytes) { vtr_dict_blob_free(b); return NULL; }
    b->dict_offsets = (int64_t *)malloc(off_bytes > 0 ? off_bytes : 1);
    if (!b->dict_offsets) { vtr_dict_blob_free(b); return NULL; }
    memcpy(b->dict_offsets, p, off_bytes);
    p += off_bytes;

    /* dict data — owned copy */
    int64_t total_dict_data = b->dict_offsets[b->dict_count];
    if (total_dict_data < 0 || (size_t)(end - p) < (size_t)total_dict_data) {
        vtr_dict_blob_free(b); return NULL;
    }
    b->dict_data = (char *)malloc(total_dict_data > 0 ? (size_t)total_dict_data : 1);
    if (!b->dict_data) { vtr_dict_blob_free(b); return NULL; }
    if (total_dict_data > 0) memcpy(b->dict_data, p, (size_t)total_dict_data);
    p += total_dict_data;

    /* n_runs */
    if (p + 4 > end) { vtr_dict_blob_free(b); return NULL; }
    memcpy(&b->n_runs, p, 4);
    p += 4;

    /* RLE runs — deinterleave (val, len) pairs into two flat arrays so the
     * consumer can iterate without per-step pointer arithmetic. */
    size_t runs_bytes_disk = (size_t)b->n_runs * 8;
    if ((size_t)(end - p) < runs_bytes_disk) { vtr_dict_blob_free(b); return NULL; }
    size_t runs_bytes = (size_t)b->n_runs * sizeof(uint32_t);
    b->run_vals = (uint32_t *)malloc(runs_bytes > 0 ? runs_bytes : 1);
    b->run_lens = (uint32_t *)malloc(runs_bytes > 0 ? runs_bytes : 1);
    if (!b->run_vals || !b->run_lens) { vtr_dict_blob_free(b); return NULL; }
    for (uint32_t r = 0; r < b->n_runs; r++) {
        memcpy(&b->run_vals[r], p, 4); p += 4;
        memcpy(&b->run_lens[r], p, 4); p += 4;
    }

    return b;
}

void vtr_dict_blob_free(VtrDictBlob *b) {
    if (!b) return;
    free(b->dict_offsets);
    free(b->dict_data);
    free(b->run_vals);
    free(b->run_lens);
    free(b);
}

/* ================================================================
 * DELTA encoding — for int64 columns
 *
 * On-disk layout (before compression):
 *   int64_t first_value
 *   int64_t deltas[n_rows - 1]
 * ================================================================ */

int should_delta_encode(const VecArray *col, int64_t n_rows) {
    if (col->type != VEC_INT64 || n_rows < 2) return 0;

    /* Check monotonically increasing (allows gaps, but all deltas >= 0) */
    int64_t prev = 0;
    int started = 0;
    for (int64_t i = 0; i < n_rows; i++) {
        if (!vec_array_is_valid(col, i)) continue;
        if (!started) {
            prev = col->buf.i64[i];
            started = 1;
            continue;
        }
        if (col->buf.i64[i] < prev) return 0;
        prev = col->buf.i64[i];
    }
    return started;
}

uint8_t *delta_encode(const VecArray *col, int64_t n_rows,
                      uint32_t *out_size) {
    /* first_value (8) + deltas (n_rows-1)*8 = n_rows*8 total */
    uint32_t size = (uint32_t)(n_rows * 8);
    uint8_t *buf = (uint8_t *)malloc(size);
    if (!buf) vectra_error("alloc failed");

    int64_t *out = (int64_t *)buf;
    out[0] = col->buf.i64[0];
    for (int64_t i = 1; i < n_rows; i++)
        out[i] = col->buf.i64[i] - col->buf.i64[i - 1];

    *out_size = size;
    return buf;
}

void delta_decode(VecArray *col, int64_t n_rows,
                  const uint8_t *data, uint32_t data_size) {
    col->buf.i64 = (int64_t *)malloc((size_t)(n_rows * 8));
    if (!col->buf.i64) vectra_error("alloc failed");

    int64_t val;
    memcpy(&val, data, 8);
    col->buf.i64[0] = val;
    for (int64_t i = 1; i < n_rows; i++) {
        memcpy(&val, data + i * 8, 8);
        col->buf.i64[i] = col->buf.i64[i - 1] + val;
    }
}

/* ================================================================
 * DIFF encoding — signed differencing for any fixed-width type
 *
 * On-disk layout (before compression):
 *   first_value (elem_size bytes)
 *   signed differences[n_rows - 1] (elem_size bytes each)
 *
 * Works on all integer types and float64. For slowly varying data,
 * differences are small values near zero. After byte-shuffle, the high
 * bytes are mostly 0x00/0xFF which LZ crushes.
 * ================================================================ */

/* Heuristic: should we diff-encode this column? Check that consecutive
   differences have low variance (mean abs diff < range/4). */
int should_diff_encode(const VecArray *col, int64_t n_rows) {
    if (n_rows < 4) return 0;
    /* Must be a fixed-width numeric type */
    if (col->type == VEC_STRING || col->type == VEC_BOOL) return 0;
    /* DIFF on doubles is lossy: (a - b) + b != a in general floating-point,
     * so a round-trip loses 1 ULP per value. Integer types are safe. */
    if (col->type == VEC_DOUBLE) return 0;
    /* Don't diff-encode if already monotonic (DELTA is better) */
    if (col->type == VEC_INT64 && should_delta_encode(col, n_rows)) return 0;

    /* Sample up to 1000 consecutive diffs */
    int64_t sample_n = n_rows < 1000 ? n_rows : 1000;
    double sum_abs_diff = 0;
    double vmin = HUGE_VAL, vmax = -HUGE_VAL;
    int64_t count = 0;

    for (int64_t i = 1; i < sample_n; i++) {
        if (!vec_array_is_valid(col, i) || !vec_array_is_valid(col, i - 1))
            continue;
        double cur, prev;
        switch (col->type) {
        case VEC_INT64:  cur = (double)col->buf.i64[i]; prev = (double)col->buf.i64[i-1]; break;
        case VEC_INT32:  cur = (double)col->buf.i32[i]; prev = (double)col->buf.i32[i-1]; break;
        case VEC_INT16:  cur = (double)col->buf.i16[i]; prev = (double)col->buf.i16[i-1]; break;
        case VEC_INT8:   cur = (double)col->buf.i8[i];  prev = (double)col->buf.i8[i-1];  break;
        case VEC_DOUBLE: cur = col->buf.dbl[i];         prev = col->buf.dbl[i-1];         break;
        default: return 0;
        }
        double d = cur - prev;
        sum_abs_diff += (d < 0) ? -d : d;
        if (cur < vmin) vmin = cur;
        if (cur > vmax) vmax = cur;
        count++;
    }
    if (count < 2) return 0;

    double range = vmax - vmin;
    if (range <= 0) return 1; /* constant column — diff is perfect */
    double mean_abs_diff = sum_abs_diff / count;

    /* Diff helps when average step is small relative to range */
    return mean_abs_diff < range / 4.0;
}

/* Encode: store first value + signed differences, all at elem_size width. */
uint8_t *diff_encode(const VecArray *col, int64_t n_rows,
                     uint32_t *out_size) {
    uint8_t es = vec_type_elem_size(col->type);
    if (es == 0) return NULL; /* variable-length types */
    uint32_t size = (uint32_t)((uint32_t)n_rows * es);
    uint8_t *buf = (uint8_t *)malloc(size);
    if (!buf) vectra_error("alloc failed in diff_encode");

    switch (col->type) {
    case VEC_INT64: {
        int64_t *out = (int64_t *)buf;
        out[0] = col->buf.i64[0];
        for (int64_t i = 1; i < n_rows; i++)
            out[i] = col->buf.i64[i] - col->buf.i64[i - 1];
        break;
    }
    case VEC_INT32: {
        int32_t *out = (int32_t *)buf;
        out[0] = col->buf.i32[0];
        for (int64_t i = 1; i < n_rows; i++)
            out[i] = col->buf.i32[i] - col->buf.i32[i - 1];
        break;
    }
    case VEC_INT16: {
        int16_t *out = (int16_t *)buf;
        out[0] = col->buf.i16[0];
        for (int64_t i = 1; i < n_rows; i++)
            out[i] = (int16_t)(col->buf.i16[i] - col->buf.i16[i - 1]);
        break;
    }
    case VEC_INT8: {
        int8_t *out = (int8_t *)buf;
        out[0] = col->buf.i8[0];
        for (int64_t i = 1; i < n_rows; i++)
            out[i] = (int8_t)(col->buf.i8[i] - col->buf.i8[i - 1]);
        break;
    }
    case VEC_DOUBLE: {
        double *out = (double *)buf;
        out[0] = col->buf.dbl[0];
        for (int64_t i = 1; i < n_rows; i++)
            out[i] = col->buf.dbl[i] - col->buf.dbl[i - 1];
        break;
    }
    default:
        free(buf);
        return NULL;
    }

    *out_size = size;
    return buf;
}

void diff_decode(VecArray *col, int64_t n_rows,
                 const uint8_t *data, uint32_t data_size) {
    switch (col->type) {
    case VEC_INT64: {
        col->buf.i64 = (int64_t *)malloc((size_t)(n_rows * 8));
        if (!col->buf.i64) vectra_error("alloc failed");
        const int64_t *in = (const int64_t *)data;
        col->buf.i64[0] = in[0];
        for (int64_t i = 1; i < n_rows; i++)
            col->buf.i64[i] = col->buf.i64[i - 1] + in[i];
        break;
    }
    case VEC_INT32: {
        col->buf.i32 = (int32_t *)malloc((size_t)(n_rows * 4));
        if (!col->buf.i32) vectra_error("alloc failed");
        const int32_t *in = (const int32_t *)data;
        col->buf.i32[0] = in[0];
        for (int64_t i = 1; i < n_rows; i++)
            col->buf.i32[i] = col->buf.i32[i - 1] + in[i];
        break;
    }
    case VEC_INT16: {
        col->buf.i16 = (int16_t *)malloc((size_t)(n_rows * 2));
        if (!col->buf.i16) vectra_error("alloc failed");
        const int16_t *in = (const int16_t *)data;
        col->buf.i16[0] = in[0];
        for (int64_t i = 1; i < n_rows; i++)
            col->buf.i16[i] = (int16_t)(col->buf.i16[i - 1] + in[i]);
        break;
    }
    case VEC_INT8: {
        col->buf.i8 = (int8_t *)malloc(n_rows > 0 ? (size_t)n_rows : 1);
        if (!col->buf.i8) vectra_error("alloc failed");
        const int8_t *in = (const int8_t *)data;
        col->buf.i8[0] = in[0];
        for (int64_t i = 1; i < n_rows; i++)
            col->buf.i8[i] = (int8_t)(col->buf.i8[i - 1] + in[i]);
        break;
    }
    case VEC_DOUBLE: {
        col->buf.dbl = (double *)malloc((size_t)(n_rows * 8));
        if (!col->buf.dbl) vectra_error("alloc failed");
        const double *in = (const double *)data;
        col->buf.dbl[0] = in[0];
        for (int64_t i = 1; i < n_rows; i++)
            col->buf.dbl[i] = col->buf.dbl[i - 1] + in[i];
        break;
    }
    default:
        break;
    }
}

/* ================================================================
 * SPATIAL encoding — 2D predictor + residuals
 *
 * Applies a spatial predictor to a column of values arranged in a 2D
 * grid (nx columns × ny rows). The predictor removes smooth trends,
 * leaving small residuals that compress much better.
 *
 * Predictor tags:
 *   0 = Left:    pred = val[row][col-1]
 *   1 = Up:      pred = val[row-1][col]
 *   2 = Average: pred = (left + up) / 2
 *   3 = Paeth:   PNG-style Paeth predictor
 *   4 = Plane:   per-tile least-squares plane fit
 *
 * On-disk layout (before compression):
 *   For predictors 0-3: residual array (same size as input)
 *   For predictor 4:    3*n_tiles int32 coefficients + residual array
 * ================================================================ */

/* Paeth predictor: given left, above, upper-left, pick the one closest
   to the linear predictor p = a + b - c. */
static inline int64_t paeth_predict(int64_t a, int64_t b, int64_t c) {
    int64_t p = a + b - c;
    int64_t pa = p > a ? p - a : a - p;
    int64_t pb = p > b ? p - b : b - p;
    int64_t pc = p > c ? p - c : c - p;
    if (pa <= pb && pa <= pc) return a;
    if (pb <= pc) return b;
    return c;
}

/* Get int64 value from a column by index (type-generic). */
static inline int64_t get_val_i64(const void *data, VecType type, int64_t idx) {
    switch (type) {
    case VEC_INT64:  return ((const int64_t *)data)[idx];
    case VEC_INT32:  return (int64_t)((const int32_t *)data)[idx];
    case VEC_INT16:  return (int64_t)((const int16_t *)data)[idx];
    case VEC_INT8:   return (int64_t)((const int8_t *)data)[idx];
    default: return 0;
    }
}

static inline double get_val_dbl(const void *data, VecType type, int64_t idx) {
    if (type == VEC_DOUBLE) return ((const double *)data)[idx];
    return (double)get_val_i64(data, type, idx);
}

/* Apply spatial predictor to int data, store residuals as int64. */
void spatial_encode_int(const void *src, VecType src_type,
                        int64_t *residuals, int64_t n,
                        uint32_t nx, uint32_t ny, int predictor) {
    for (int64_t i = 0; i < n; i++) {
        int64_t val = get_val_i64(src, src_type, i);
        uint32_t col = (uint32_t)(i % nx);
        uint32_t row = (uint32_t)(i / nx);
        int64_t left  = (col > 0) ? get_val_i64(src, src_type, i - 1) : 0;
        int64_t up    = (row > 0) ? get_val_i64(src, src_type, i - (int64_t)nx) : 0;
        int64_t upleft = (col > 0 && row > 0) ? get_val_i64(src, src_type, i - (int64_t)nx - 1) : 0;

        int64_t pred;
        switch (predictor) {
        case VTR_PRED_LEFT:    pred = left; break;
        case VTR_PRED_UP:      pred = up; break;
        case VTR_PRED_AVERAGE: pred = (left + up) / 2; break;
        case VTR_PRED_PAETH:   pred = paeth_predict(left, up, upleft); break;
        default:               pred = 0; break;
        }
        residuals[i] = val - pred;
    }
}

/* Inverse: reconstruct from residuals. */
void spatial_decode_int(int64_t *dst, const int64_t *residuals,
                        int64_t n, uint32_t nx, uint32_t ny,
                        int predictor) {
    for (int64_t i = 0; i < n; i++) {
        uint32_t col = (uint32_t)(i % nx);
        uint32_t row = (uint32_t)(i / nx);
        int64_t left  = (col > 0) ? dst[i - 1] : 0;
        int64_t up    = (row > 0) ? dst[i - (int64_t)nx] : 0;
        int64_t upleft = (col > 0 && row > 0) ? dst[i - (int64_t)nx - 1] : 0;

        int64_t pred;
        switch (predictor) {
        case VTR_PRED_LEFT:    pred = left; break;
        case VTR_PRED_UP:      pred = up; break;
        case VTR_PRED_AVERAGE: pred = (left + up) / 2; break;
        case VTR_PRED_PAETH:   pred = paeth_predict(left, up, upleft); break;
        default:               pred = 0; break;
        }
        dst[i] = residuals[i] + pred;
    }
}

/* Plane predictor: per-tile least-squares plane fit.
   For each tile, fit pred(x,y) = a + b*x + c*y via closed-form 3x3 system.
   Coefficients stored as int32 (fixed-point with implied scale = 1).
   Returns malloc'd coefficient array (3 per tile). */
int32_t *plane_encode(const void *src, VecType src_type,
                      int64_t *residuals, int64_t n,
                      uint32_t nx, uint32_t ny, uint16_t tile_size,
                      uint32_t *out_n_tiles) {
    uint32_t tiles_x = (nx + tile_size - 1) / tile_size;
    uint32_t tiles_y = (ny + tile_size - 1) / tile_size;
    uint32_t n_tiles = tiles_x * tiles_y;
    *out_n_tiles = n_tiles;

    int32_t *coeffs = (int32_t *)calloc((size_t)n_tiles * 3, sizeof(int32_t));
    if (!coeffs) vectra_error("alloc failed in plane_encode");

    for (uint32_t ty = 0; ty < tiles_y; ty++) {
        for (uint32_t tx = 0; tx < tiles_x; tx++) {
            uint32_t x0 = tx * tile_size;
            uint32_t y0 = ty * tile_size;
            uint32_t x1 = x0 + tile_size; if (x1 > nx) x1 = nx;
            uint32_t y1 = y0 + tile_size; if (y1 > ny) y1 = ny;

            /* Accumulate sums for least-squares: val = a + b*lx + c*ly
               where lx = x - x0, ly = y - y0 (local coords) */
            double s_1 = 0, s_x = 0, s_y = 0;
            double s_xx = 0, s_xy = 0, s_yy = 0;
            double s_v = 0, s_vx = 0, s_vy = 0;
            uint32_t count = 0;

            for (uint32_t py = y0; py < y1; py++) {
                for (uint32_t px = x0; px < x1; px++) {
                    int64_t idx = (int64_t)py * nx + px;
                    if (idx >= n) continue;
                    double v = get_val_dbl(src, src_type, idx);
                    double lx = (double)(px - x0);
                    double ly = (double)(py - y0);
                    s_1  += 1;
                    s_x  += lx;
                    s_y  += ly;
                    s_xx += lx * lx;
                    s_xy += lx * ly;
                    s_yy += ly * ly;
                    s_v  += v;
                    s_vx += v * lx;
                    s_vy += v * ly;
                    count++;
                }
            }

            double a = 0, b = 0, c = 0;
            if (count >= 3) {
                /* Solve 3x3 normal equations:
                   [s_1  s_x  s_y ] [a]   [s_v ]
                   [s_x  s_xx s_xy] [b] = [s_vx]
                   [s_y  s_xy s_yy] [c]   [s_vy] */
                double det = s_1 * (s_xx * s_yy - s_xy * s_xy)
                           - s_x * (s_x * s_yy - s_xy * s_y)
                           + s_y * (s_x * s_xy - s_xx * s_y);
                if (det != 0.0 && det == det) { /* det != NaN */
                    double inv_det = 1.0 / det;
                    a = (s_v * (s_xx * s_yy - s_xy * s_xy)
                       - s_vx * (s_x * s_yy - s_xy * s_y)
                       + s_vy * (s_x * s_xy - s_xx * s_y)) * inv_det;
                    b = (s_1 * (s_vx * s_yy - s_vy * s_xy)
                       - s_x * (s_v * s_yy - s_vy * s_y)
                       + s_y * (s_v * s_xy - s_vx * s_y)) * inv_det;
                    c = (s_1 * (s_xx * s_vy - s_xy * s_vx)
                       - s_x * (s_x * s_vy - s_xy * s_v)
                       + s_y * (s_x * s_vx - s_xx * s_v)) * inv_det;
                }
            } else if (count > 0) {
                a = s_v / s_1; /* just use mean */
            }

            uint32_t tidx = ty * tiles_x + tx;
            /* Store as int32 (rounding) */
            coeffs[tidx * 3 + 0] = (int32_t)round(a * 256.0); /* 8-bit fractional */
            coeffs[tidx * 3 + 1] = (int32_t)round(b * 256.0);
            coeffs[tidx * 3 + 2] = (int32_t)round(c * 256.0);

            /* Compute residuals for this tile */
            for (uint32_t py = y0; py < y1; py++) {
                for (uint32_t px = x0; px < x1; px++) {
                    int64_t idx = (int64_t)py * nx + px;
                    if (idx >= n) continue;
                    double lx = (double)(px - x0);
                    double ly = (double)(py - y0);
                    double pred = (coeffs[tidx * 3 + 0]
                                 + coeffs[tidx * 3 + 1] * lx
                                 + coeffs[tidx * 3 + 2] * ly) / 256.0;
                    int64_t val = get_val_i64(src, src_type, idx);
                    residuals[idx] = val - (int64_t)round(pred);
                }
            }
        }
    }

    return coeffs;
}

void plane_decode(int64_t *dst, const int64_t *residuals, int64_t n,
                  uint32_t nx, uint32_t ny, uint16_t tile_size,
                  const int32_t *coeffs) {
    uint32_t tiles_x = (nx + tile_size - 1) / tile_size;

    for (int64_t i = 0; i < n; i++) {
        uint32_t px = (uint32_t)(i % nx);
        uint32_t py = (uint32_t)(i / nx);
        uint32_t tx = px / tile_size;
        uint32_t ty = py / tile_size;
        uint32_t tidx = ty * tiles_x + tx;

        double lx = (double)(px - tx * tile_size);
        double ly = (double)(py - ty * tile_size);
        double pred = (coeffs[tidx * 3 + 0]
                     + coeffs[tidx * 3 + 1] * lx
                     + coeffs[tidx * 3 + 2] * ly) / 256.0;
        dst[i] = residuals[i] + (int64_t)round(pred);
    }
}

/* Auto-select best predictor (0-3) by testing on a sample. */
int auto_select_predictor(const void *src, VecType src_type,
                          int64_t n, uint32_t nx, uint32_t ny) {
    int64_t sample_n = n < 10000 ? n : 10000;
    int64_t *tmp = (int64_t *)malloc((size_t)sample_n * sizeof(int64_t));
    if (!tmp) return VTR_PRED_AVERAGE; /* fallback */

    double best_score = HUGE_VAL;
    int best_pred = VTR_PRED_AVERAGE;

    for (int p = VTR_PRED_LEFT; p <= VTR_PRED_PAETH; p++) {
        spatial_encode_int(src, src_type, tmp, sample_n, nx, ny, p);
        double sum = 0;
        for (int64_t i = 0; i < sample_n; i++) {
            int64_t v = tmp[i];
            sum += (double)(v < 0 ? -v : v);
        }
        if (sum < best_score) {
            best_score = sum;
            best_pred = p;
        }
    }

    free(tmp);
    return best_pred;
}

/* ================================================================
 * QUANTIZE encoding — lossy float64 → scaled narrow int
 *
 * Encoding:  stored = round((value - offset) * scale)
 * Decoding:  value  = (stored / scale) + offset
 *
 * On disk: the quantized column is stored as a PLAIN narrow int buffer.
 * The chunk header carries encoding = VTR_ENC_QUANTIZE plus 17 bytes of
 * metadata (scale:f64, offset:f64, target_type:u8).
 * ================================================================ */

void quantize_float_to_int(const double *src, int64_t n_rows,
                           const uint8_t *validity,
                           double scale, double offset,
                           VecType target_type,
                           uint8_t *dst, int *overflow_count) {
    *overflow_count = 0;
    int64_t tmin, tmax;
    switch (target_type) {
    case VEC_INT8:  tmin = -128;       tmax = 127;       break;
    case VEC_INT16: tmin = -32768;     tmax = 32767;     break;
    case VEC_INT32: tmin = INT32_MIN;  tmax = INT32_MAX; break;
    default: return;
    }

    for (int64_t i = 0; i < n_rows; i++) {
        int valid = (validity[i / 8] >> (i % 8)) & 1;
        if (!valid) continue;
        double v = round((src[i] - offset) * scale);
        int64_t iv = (int64_t)v;
        if (iv < tmin || iv > tmax) {
            (*overflow_count)++;
            iv = iv < tmin ? tmin : tmax;
        }
        switch (target_type) {
        case VEC_INT8:  ((int8_t *)dst)[i]  = (int8_t)iv;  break;
        case VEC_INT16: ((int16_t *)dst)[i] = (int16_t)iv; break;
        case VEC_INT32: ((int32_t *)dst)[i] = (int32_t)iv; break;
        default: break;
        }
    }
}

void vtr_dequantize(double *dst, const uint8_t *src, int64_t n_rows,
                    const uint8_t *validity,
                    double scale, double offset, VecType target_type) {
    for (int64_t i = 0; i < n_rows; i++) {
        int valid = (validity[i / 8] >> (i % 8)) & 1;
        if (!valid) { dst[i] = 0.0; continue; }
        int64_t iv;
        switch (target_type) {
        case VEC_INT8:  iv = (int64_t)((const int8_t *)src)[i];  break;
        case VEC_INT16: iv = (int64_t)((const int16_t *)src)[i]; break;
        case VEC_INT32: iv = (int64_t)((const int32_t *)src)[i]; break;
        default: iv = 0; break;
        }
        dst[i] = ((double)iv / scale) + offset;
    }
}

/* ================================================================
 * Determine shuffle element size from column type + encoding.
 * Returns 0 if shuffle is not applicable (variable-length or too small).
 * ================================================================ */

uint8_t vtr_shuffle_elem_size(VecType type, uint8_t encoding) {
    if (encoding == VTR_ENC_DICTIONARY) return 0; /* RLE: variable layout */
    if (encoding == VTR_ENC_DICT_NUM)   return 0; /* header + dict + idx blob */
    if (encoding == VTR_ENC_SPARSE_ZERO) return 0; /* bitmap + values blob */
    /* DIFF and SPATIAL store same-width elements as PLAIN, so shuffle applies */
    switch (type) {
    case VEC_INT64:  return 8;
    case VEC_INT32:  return 4;
    case VEC_INT16:  return 2;
    case VEC_INT8:   return 0; /* 1-byte: shuffle is identity */
    case VEC_DOUBLE: return 8;
    case VEC_BOOL:   return 0; /* 1-byte: shuffle is identity */
    case VEC_STRING: return 0; /* variable-length */
    }
    return 0;
}

/* ================================================================
 * Public spatial decode
 * ================================================================ */

void vtr_spatial_decode(int64_t *dst, const int64_t *residuals, int64_t n_rows,
                        uint32_t nx, uint32_t ny, uint8_t predictor,
                        uint16_t tile_size, const int32_t *coeffs) {
    if (predictor == VTR_PRED_PLANE && coeffs) {
        plane_decode(dst, residuals, n_rows, nx, ny, tile_size, coeffs);
    } else {
        spatial_decode_int(dst, residuals, n_rows, nx, ny, predictor);
    }
}

void vtr_spatial_dequantize(double *dst, const int64_t *values, int64_t n_rows,
                            const uint8_t *validity,
                            double scale, double offset) {
    for (int64_t i = 0; i < n_rows; i++) {
        int valid = (validity[i / 8] >> (i % 8)) & 1;
        if (!valid) { dst[i] = 0.0; continue; }
        dst[i] = ((double)values[i] / scale) + offset;
    }
}
