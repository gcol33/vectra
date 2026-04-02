#include "vtr_codec.h"
#include "array.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/* ================================================================
 * LZ-VTR: minimal LZ77 byte compressor — zero external dependencies
 *
 * Format: sequence of chunks, each is either:
 *   [0xxxxxxx]          — literal run: next (x+1) bytes are literal (1-128)
 *   [1xxxxxxx yyyyyyyy] — match: length (x+3), offset (y+1)
 *                          length 3-130, offset 1-256
 *
 * Simple hash chain for match finding, greedy parsing. Designed for
 * post-encoding residual where dict+RLE already removed most entropy.
 * Decompression is branchless-friendly and fast (~800 MB/s).
 * ================================================================ */

#define LZ_HASH_BITS  14
#define LZ_HASH_SIZE  (1 << LZ_HASH_BITS)
#define LZ_MIN_MATCH  3
#define LZ_MAX_MATCH  130  /* 7 bits + 3 */
#define LZ_MAX_OFFSET 256  /* 8 bits + 1 */
#define LZ_MAX_LIT    128  /* 7 bits + 1 */

static inline uint32_t lz_hash3(const uint8_t *p) {
    return ((uint32_t)p[0] ^ ((uint32_t)p[1] << 5) ^
            ((uint32_t)p[2] << 10)) & (LZ_HASH_SIZE - 1);
}

/* Compress src[0..src_size-1].
 * Returns malloc'd buffer; sets *out_size.
 * Returns NULL if compression doesn't shrink. */
static uint8_t *lz_vtr_compress(const uint8_t *src, uint32_t src_size,
                                uint32_t *out_size) {
    if (src_size < LZ_MIN_MATCH) return NULL;

    /* Worst case: all literals → src_size + src_size/128 + 1 headers */
    uint32_t bound = src_size + (src_size / LZ_MAX_LIT) + 2;
    uint8_t *dst = (uint8_t *)malloc(bound);
    if (!dst) return NULL;

    int32_t htable[LZ_HASH_SIZE];
    memset(htable, -1, sizeof(htable));

    uint32_t sp = 0;          /* source position */
    uint32_t dp = 0;          /* dest position */
    uint32_t lit_start = 0;   /* start of pending literal run */
    uint32_t lit_len = 0;

    #define FLUSH_LITERALS() do { \
        while (lit_len > 0) { \
            uint32_t run = lit_len < LZ_MAX_LIT ? lit_len : LZ_MAX_LIT; \
            dst[dp++] = (uint8_t)(run - 1); /* 0xxxxxxx = literal */ \
            memcpy(dst + dp, src + lit_start, run); \
            dp += run; \
            lit_start += run; \
            lit_len -= run; \
        } \
    } while (0)

    while (sp < src_size) {
        uint32_t best_len = 0, best_off = 0;

        if (sp + LZ_MIN_MATCH <= src_size) {
            uint32_t h = lz_hash3(src + sp);
            int32_t candidate = htable[h];
            htable[h] = (int32_t)sp;

            if (candidate >= 0) {
                uint32_t off = sp - (uint32_t)candidate;
                if (off >= 1 && off <= LZ_MAX_OFFSET) {
                    /* Extend match */
                    uint32_t max_len = src_size - sp;
                    if (max_len > LZ_MAX_MATCH) max_len = LZ_MAX_MATCH;
                    uint32_t len = 0;
                    while (len < max_len &&
                           src[sp + len] == src[(uint32_t)candidate + len])
                        len++;
                    if (len >= LZ_MIN_MATCH) {
                        best_len = len;
                        best_off = off;
                    }
                }
            }
        }

        if (best_len >= LZ_MIN_MATCH) {
            FLUSH_LITERALS();
            /* Emit match: [1xxxxxxx yyyyyyyy] */
            dst[dp++] = (uint8_t)(0x80 | (best_len - LZ_MIN_MATCH));
            dst[dp++] = (uint8_t)(best_off - 1);
            /* Update hash for skipped positions */
            for (uint32_t i = 1; i < best_len && sp + i + LZ_MIN_MATCH <= src_size; i++) {
                uint32_t h = lz_hash3(src + sp + i);
                htable[h] = (int32_t)(sp + i);
            }
            sp += best_len;
            lit_start = sp;
        } else {
            lit_len++;
            sp++;
        }
    }
    FLUSH_LITERALS();
    #undef FLUSH_LITERALS

    if (dp >= src_size) {
        free(dst);
        return NULL;  /* compression didn't help */
    }

    *out_size = dp;
    return dst;
}

/* Decompress into a buffer of known uncompressed_size. */
static uint8_t *lz_vtr_decompress(const uint8_t *src, uint32_t src_size,
                                  uint32_t uncompressed_size) {
    uint8_t *dst = (uint8_t *)malloc((size_t)uncompressed_size);
    if (!dst) vectra_error("alloc failed in lz_vtr_decompress");

    uint32_t sp = 0, dp = 0;
    while (sp < src_size && dp < uncompressed_size) {
        uint8_t tag = src[sp++];
        if (tag & 0x80) {
            /* Match */
            uint32_t len = (tag & 0x7F) + LZ_MIN_MATCH;
            uint32_t off = (uint32_t)src[sp++] + 1;
            if (dp < off) vectra_error("lz_vtr: invalid back-reference");
            for (uint32_t i = 0; i < len && dp < uncompressed_size; i++) {
                dst[dp] = dst[dp - off];
                dp++;
            }
        } else {
            /* Literal run */
            uint32_t len = (tag & 0x7F) + 1;
            if (len > uncompressed_size - dp) len = uncompressed_size - dp;
            memcpy(dst + dp, src + sp, len);
            sp += len;
            dp += len;
        }
    }
    return dst;
}

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

/* RLE-decode into a flat uint32 array of exactly n_rows elements.
 * The caller must know n_rows from the row group header. */
static uint32_t *rle_decode_u32(const uint8_t *data, int64_t n_rows,
                                uint32_t *bytes_consumed) {
    const uint8_t *p = data;
    uint32_t n_runs;
    memcpy(&n_runs, p, 4); p += 4;

    uint32_t *out = (uint32_t *)malloc((size_t)n_rows * sizeof(uint32_t));
    if (!out) vectra_error("alloc failed in rle_decode_u32");

    int64_t pos = 0;
    for (uint32_t r = 0; r < n_runs; r++) {
        uint32_t val, len;
        memcpy(&val, p, 4); p += 4;
        memcpy(&len, p, 4); p += 4;
        for (uint32_t k = 0; k < len && pos < n_rows; k++)
            out[pos++] = val;
    }

    if (bytes_consumed)
        *bytes_consumed = 4 + n_runs * 8;
    return out;
}

/* ================================================================
 * PLAIN encoding — just serialize the raw column data to a byte buffer
 * ================================================================ */

static uint8_t *plain_encode(const VecArray *col, int64_t n_rows,
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

static void plain_decode(VecArray *col, int64_t n_rows,
                         const uint8_t *data, uint32_t data_size) {
    switch (col->type) {
    case VEC_INT64:
        col->buf.i64 = (int64_t *)malloc((size_t)(n_rows * 8));
        if (!col->buf.i64) vectra_error("alloc failed");
        memcpy(col->buf.i64, data, (size_t)(n_rows * 8));
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
static uint8_t *try_dict_encode(const VecArray *col, int64_t n_rows,
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

static void dict_decode(VecArray *col, int64_t n_rows,
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

    /* dict data */
    int64_t total_dict_data = dict_offsets[dict_count];
    const char *dict_data = (const char *)p;
    p += total_dict_data;

    /* RLE-decode indices */
    uint32_t rle_consumed = 0;
    uint32_t *indices = rle_decode_u32(p, n_rows, &rle_consumed);

    /* Rebuild string column: compute total data size */
    int64_t total_str_data = 0;
    for (int64_t i = 0; i < n_rows; i++) {
        if (!vec_array_is_valid(col, i)) continue;
        uint32_t idx = indices[i];
        total_str_data += dict_offsets[idx + 1] - dict_offsets[idx];
    }

    col->buf.str.offsets = (int64_t *)malloc((size_t)((n_rows + 1) * 8));
    col->buf.str.data = (char *)malloc(total_str_data > 0 ? (size_t)total_str_data : 1);
    if (!col->buf.str.offsets || !col->buf.str.data)
        vectra_error("alloc failed in dict_decode");
    col->buf.str.data_len = total_str_data;

    int64_t pos = 0;
    for (int64_t i = 0; i < n_rows; i++) {
        col->buf.str.offsets[i] = pos;
        if (!vec_array_is_valid(col, i)) continue;
        uint32_t idx = indices[i];
        int64_t slen = dict_offsets[idx + 1] - dict_offsets[idx];
        memcpy(col->buf.str.data + pos, dict_data + dict_offsets[idx],
               (size_t)slen);
        pos += slen;
    }
    col->buf.str.offsets[n_rows] = pos;

    free(indices);
    free(dict_offsets);
}

/* ================================================================
 * DELTA encoding — for int64 columns
 *
 * On-disk layout (before compression):
 *   int64_t first_value
 *   int64_t deltas[n_rows - 1]
 * ================================================================ */

static int should_delta_encode(const VecArray *col, int64_t n_rows) {
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

static uint8_t *delta_encode(const VecArray *col, int64_t n_rows,
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

static void delta_decode(VecArray *col, int64_t n_rows,
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
 * Top-level encode/decode
 * ================================================================ */

/* Minimum size to bother with compression */
#define COMPRESS_THRESHOLD 64

VtrEncodedCol vtr_encode_column(const VecArray *col, int64_t n_rows) {
    VtrEncodedCol result;
    memset(&result, 0, sizeof(result));

    if (n_rows == 0) {
        /* Empty column: PLAIN + NONE, zero-length buffer */
        result.encoding = VTR_ENC_PLAIN;
        result.compression = VTR_COMP_NONE;
        result.data = (uint8_t *)malloc(1);
        result.data_size = 0;
        result.uncompressed_size = 0;
        return result;
    }

    /* Choose encoding */
    uint8_t *raw = NULL;
    uint32_t raw_size = 0;

    if (col->type == VEC_STRING) {
        raw = try_dict_encode(col, n_rows, &raw_size);
        if (raw) result.encoding = VTR_ENC_DICTIONARY;
    }
    if (!raw && col->type == VEC_INT64 && should_delta_encode(col, n_rows)) {
        raw = delta_encode(col, n_rows, &raw_size);
        result.encoding = VTR_ENC_DELTA;
    }
    if (!raw) {
        raw = plain_encode(col, n_rows, &raw_size);
        result.encoding = VTR_ENC_PLAIN;
    }

    result.uncompressed_size = raw_size;

    /* Try LZ-VTR compression */
    if (raw_size > COMPRESS_THRESHOLD) {
        uint32_t comp_size = 0;
        uint8_t *comp = lz_vtr_compress(raw, raw_size, &comp_size);
        if (comp) {
            free(raw);
            result.data = comp;
            result.data_size = comp_size;
            result.compression = VTR_COMP_LZ_VTR;
            return result;
        }
    }

    /* No compression */
    result.data = raw;
    result.data_size = raw_size;
    result.compression = VTR_COMP_NONE;
    return result;
}

void vtr_decode_column(VecArray *col, int64_t n_rows,
                       uint8_t encoding, uint8_t compression,
                       const uint8_t *data, uint32_t data_size,
                       uint32_t uncompressed_size) {
    if (n_rows == 0) return;

    /* Decompress if needed */
    const uint8_t *decoded_data = data;
    uint8_t *decompressed = NULL;

    if (compression == VTR_COMP_LZ_VTR) {
        decompressed = lz_vtr_decompress(data, data_size, uncompressed_size);
        decoded_data = decompressed;
        data_size = uncompressed_size;
    } else if (compression != VTR_COMP_NONE) {
        vectra_error("unknown compression tag: 0x%02x", compression);
    }

    /* Decode */
    switch (encoding) {
    case VTR_ENC_PLAIN:
        plain_decode(col, n_rows, decoded_data, data_size);
        break;
    case VTR_ENC_DICTIONARY:
        dict_decode(col, n_rows, decoded_data, data_size);
        break;
    case VTR_ENC_DELTA:
        delta_decode(col, n_rows, decoded_data, data_size);
        break;
    default:
        if (decompressed) free(decompressed);
        vectra_error("unknown encoding tag: 0x%02x", encoding);
    }

    free(decompressed);
}
