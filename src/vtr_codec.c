#include "vtr_codec.h"
#include "array.h"
#include "error.h"
#include "tdc.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* ---------- decode profiling counters (nanoseconds) ----------
 *
 * The counters and the public reset/get API are always compiled so
 * R-side bridge code (init.c) links unconditionally. The actual
 * clock_gettime() calls and increments in the hot decode path are
 * gated behind VTR_PROFILE — when undefined, the macros below expand
 * to ((void)0) and the ~50 ns/call timer overhead disappears.
 *
 * Enable for profiling builds with: PKG_CFLAGS="-DVTR_PROFILE"
 */
static uint64_t g_prof_decompress_ns = 0;
static uint64_t g_prof_unshuffle_ns  = 0;
static uint64_t g_prof_decode_ns     = 0;
static uint64_t g_prof_calls         = 0;
uint64_t g_prof_sse2_unshuffle_calls = 0;  /* SIMD path hit counter */

#ifdef VTR_PROFILE
#include <time.h>
static inline uint64_t prof_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}
#  define PROF_TIME_START(t)         uint64_t t = prof_now_ns()
#  define PROF_TIME_MARK(t)          uint64_t t = prof_now_ns()
#  define PROF_TIME_ACC(counter, t)  do { (counter) += prof_now_ns() - (t); } while (0)
#  define PROF_DIFF_ACC(counter, a, b) do { (counter) += (a) - (b); } while (0)
#  define PROF_INC(counter)          do { (counter)++; } while (0)
#else
#  define PROF_TIME_START(t)         ((void)0)
#  define PROF_TIME_MARK(t)          ((void)0)
#  define PROF_TIME_ACC(counter, t)  ((void)0)
#  define PROF_DIFF_ACC(counter, a, b) ((void)0)
#  define PROF_INC(counter)          ((void)0)
#endif

void vtr_codec_profile_reset(void) {
    g_prof_decompress_ns = 0;
    g_prof_unshuffle_ns  = 0;
    g_prof_decode_ns     = 0;
    g_prof_calls         = 0;
    g_prof_sse2_unshuffle_calls = 0;
}
void vtr_codec_profile_get(uint64_t *decompress_ns, uint64_t *unshuffle_ns,
                           uint64_t *decode_ns, uint64_t *calls) {
    if (decompress_ns) *decompress_ns = g_prof_decompress_ns;
    if (unshuffle_ns)  *unshuffle_ns  = g_prof_unshuffle_ns;
    if (decode_ns)     *decode_ns     = g_prof_decode_ns;
    if (calls)         *calls         = g_prof_calls;
}

/* ================================================================
 * tdc bridge — LZ2 + byte-shuffle now live in src/tdc/.
 *
 * The native LZ77 ("LZ-VTR"), LZ2 implementation, and SIMD byte-shuffle
 * kernels that used to live here have moved into the vendored tdc tree
 * (src/tdc/src/entropy/lz2.c and src/tdc/src/transform/shuffle.c). The
 * inner loops are byte-identical to the originals; only the outer
 * wrapping (allocation, error handling) is rewritten for tdc's
 * conventions.
 *
 * This bridge provides the small set of file-local helpers and public
 * vtr_* symbols that the rest of vtr_codec.c and vtr1.c still call.
 * Once vectra fully retires the .vtr v4 read path, these can disappear
 * entirely and call sites can talk to tdc directly.
 * ================================================================ */

/* stdlib-backed realloc shim that satisfies tdc_buffer's contract:
 *   realloc_fn(user, NULL, n) -> allocate
 *   realloc_fn(user, p,    0) -> free
 *   realloc_fn(user, p,    n) -> grow (may move) */
static void *vtr_stdlib_realloc(void *user, void *ptr, size_t new_size) {
    (void)user;
    if (new_size == 0) { free(ptr); return NULL; }
    return realloc(ptr, new_size);
}

/* Map a vtr byte-shuffle elem_size (1/2/4/8) to a tdc unsigned dtype.
 * tdc shuffle uses tdc_dtype_size(in_dtype) to derive the lane width. */
static tdc_dtype vtr_elem_size_to_dtype(uint8_t elem_size) {
    switch (elem_size) {
        case 1:  return TDC_DT_U8;
        case 2:  return TDC_DT_U16;
        case 4:  return TDC_DT_U32;
        case 8:  return TDC_DT_U64;
        default: return TDC_DT_U8;  /* caller already gates on es > 0 */
    }
}

/* ----- byte-shuffle bridge ------------------------------------------------ */

/* Forward shuffle (encode side). The encoder call sites pass non-overlapping
 * src/dst buffers; tdc shuffle handles the SIMD/scalar dispatch internally. */
static void byte_shuffle(uint8_t *dst, const uint8_t *src,
                         uint32_t n_elems, uint8_t elem_size) {
    if (n_elems == 0 || elem_size == 0) return;

    const tdc_xform_vt *vt = tdc_xform_get(TDC_XFORM_BYTE_SHUFFLE);
    if (!vt) vectra_error("tdc shuffle vtable missing");

    size_t total = (size_t)n_elems * elem_size;
    tdc_buffer out = {0};
    out.realloc_fn = vtr_stdlib_realloc;

    tdc_dtype out_dtype = 0;
    tdc_status st = vt->encode(src, total,
                               vtr_elem_size_to_dtype(elem_size),
                               NULL, &out, &out_dtype);
    if (st != TDC_OK) {
        free(out.data);
        vectra_error("tdc shuffle encode failed: %d", (int)st);
    }
    if (out.size != total) {
        free(out.data);
        vectra_error("tdc shuffle encode size mismatch: %zu vs %zu",
                     out.size, total);
    }
    memcpy(dst, out.data, total);
    free(out.data);
}

/* Inverse shuffle (decode side). Direct write into caller buffer. */
static void byte_unshuffle(uint8_t *dst, const uint8_t *src,
                           uint32_t n_elems, uint8_t elem_size) {
    if (n_elems == 0 || elem_size == 0) return;

    const tdc_xform_vt *vt = tdc_xform_get(TDC_XFORM_BYTE_SHUFFLE);
    if (!vt) vectra_error("tdc shuffle vtable missing");

    size_t total = (size_t)n_elems * elem_size;
    tdc_dtype out_dtype = 0;
    tdc_status st = vt->decode(src, total,
                               vtr_elem_size_to_dtype(elem_size),
                               NULL, dst, total, &out_dtype);
    if (st != TDC_OK) vectra_error("tdc shuffle decode failed: %d", (int)st);
}

/* Public: in-place byte-unshuffle via temp scratch buffer. */
void vtr_byte_unshuffle(uint8_t *data, uint32_t n_elems, uint8_t elem_size) {
    uint32_t total = (uint32_t)n_elems * elem_size;
    if (total == 0) return;
    uint8_t *tmp = (uint8_t *)malloc(total);
    if (!tmp) vectra_error("alloc failed in vtr_byte_unshuffle");
    memcpy(tmp, data, total);
    byte_unshuffle(data, tmp, n_elems, elem_size);
    free(tmp);
}

/* Public: byte-unshuffle from src to dst (no temp alloc). */
void vtr_byte_unshuffle_to(uint8_t *dst, const uint8_t *src,
                           uint32_t n_elems, uint8_t elem_size) {
    PROF_TIME_START(t0);
    byte_unshuffle(dst, src, n_elems, elem_size);
    PROF_TIME_ACC(g_prof_unshuffle_ns, t0);
}

/* ----- LZ2 bridge --------------------------------------------------------- */

/* Drop-in replacement for the old static lz2_vtr_compress: returns a
 * malloc'd buffer of size *out_size on success, or NULL if compression
 * does not shrink the input (caller falls back to uncompressed). */
static uint8_t *lz2_vtr_compress(const uint8_t *src, uint32_t src_size,
                                 uint32_t *out_size) {
    *out_size = 0;
    if (src_size == 0) return NULL;

    const tdc_entropy_vt *vt = tdc_entropy_get(TDC_ENTROPY_LZ2);
    if (!vt) vectra_error("tdc lz2 vtable missing");

    tdc_buffer out = {0};
    out.realloc_fn = vtr_stdlib_realloc;

    tdc_status st = vt->encode(src, src_size, NULL, &out);
    if (st != TDC_OK) {
        free(out.data);
        vectra_error("tdc lz2 encode failed: %d", (int)st);
    }

    /* Match vectra's "did it shrink?" semantics. */
    if (out.size >= src_size) {
        free(out.data);
        return NULL;
    }

    *out_size = (uint32_t)out.size;
    return out.data;  /* ownership transferred to caller */
}

/* Public: decompress LZ2 into caller-provided buffer. */
void vtr_lz2_decompress_into(uint8_t *dst, uint32_t uncompressed_size,
                             const uint8_t *src, uint32_t src_size) {
    PROF_TIME_START(t0);
    const tdc_entropy_vt *vt = tdc_entropy_get(TDC_ENTROPY_LZ2);
    if (!vt) vectra_error("tdc lz2 vtable missing");
    tdc_status st = vt->decode(src, src_size, dst, uncompressed_size);
    if (st != TDC_OK) vectra_error("tdc lz2 decode failed: %d", (int)st);
    PROF_TIME_ACC(g_prof_decompress_ns, t0);
    PROF_INC(g_prof_calls);
}

/* Internal: alloc + decompress, used by vtr_decode_column below. */
static uint8_t *lz2_vtr_decompress(const uint8_t *src, uint32_t src_size,
                                   uint32_t uncompressed_size) {
    uint8_t *dst = (uint8_t *)malloc((size_t)uncompressed_size);
    if (!dst) vectra_error("alloc failed in lz2_vtr_decompress");
    const tdc_entropy_vt *vt = tdc_entropy_get(TDC_ENTROPY_LZ2);
    if (!vt) vectra_error("tdc lz2 vtable missing");
    tdc_status st = vt->decode(src, src_size, dst, uncompressed_size);
    if (st != TDC_OK) vectra_error("tdc lz2 decode failed: %d", (int)st);
    return dst;
}

/* ----- Huffman bridge ------------------------------------------------------ */

static uint8_t *huffman_vtr_compress(const uint8_t *src, uint32_t src_size,
                                     uint32_t *out_size) {
    *out_size = 0;
    if (src_size == 0) return NULL;

    const tdc_entropy_vt *vt = tdc_entropy_get(TDC_ENTROPY_HUFFMAN);
    if (!vt) vectra_error("tdc huffman vtable missing");

    tdc_buffer out = {0};
    out.realloc_fn = vtr_stdlib_realloc;

    tdc_status st = vt->encode(src, src_size, NULL, &out);
    if (st != TDC_OK) {
        free(out.data);
        vectra_error("tdc huffman encode failed: %d", (int)st);
    }

    if (out.size >= src_size) {
        free(out.data);
        return NULL;
    }

    *out_size = (uint32_t)out.size;
    return out.data;
}

/* ----- Shared compress/decompress helpers ---------------------------------- */

/* Compress pre-shuffled bytes at the requested level.
 * Returns malloc'd buffer on success (caller owns), NULL if compression
 * did not shrink the input. Sets *out_size and *out_tag. */
static uint8_t *vtr_compress_shuffled(const uint8_t *shuffled, uint32_t size,
                                      int comp_level,
                                      uint32_t *out_size, uint8_t *out_tag) {
    *out_size = 0;
    *out_tag = VTR_COMP_NONE;

    /* LZ2 first (both FAST and RATIO need it) */
    uint32_t lz2_size = 0;
    uint8_t *lz2 = lz2_vtr_compress(shuffled, size, &lz2_size);
    if (!lz2) return NULL;  /* didn't shrink */

    if (comp_level == VTR_COMPRESS_FAST) {
        *out_size = lz2_size;
        *out_tag = VTR_COMP_SHUFFLE_LZ2;
        return lz2;
    }

    /* RATIO: try Huffman on top of LZ2 */
    uint32_t huff_size = 0;
    uint8_t *huff = huffman_vtr_compress(lz2, lz2_size, &huff_size);
    if (huff) {
        free(lz2);
        *out_size = huff_size;
        *out_tag = VTR_COMP_SHUFFLE_LZ2_HUFF;
        return huff;
    }
    /* Huffman didn't help — fall back to LZ2-only */
    *out_size = lz2_size;
    *out_tag = VTR_COMP_SHUFFLE_LZ2;
    return lz2;
}

/* Decompress (LZ2 or LZ2+Huffman) into caller-provided buffer.
 * Does NOT unshuffle. */
void vtr_decompress_into(uint8_t *dst, uint32_t uncompressed_size,
                         const uint8_t *src, uint32_t src_size,
                         uint8_t compression) {
    if (compression == VTR_COMP_SHUFFLE_LZ2) {
        vtr_lz2_decompress_into(dst, uncompressed_size, src, src_size);
    } else if (compression == VTR_COMP_SHUFFLE_LZ2_HUFF) {
        /* Huffman header: first 4 bytes = u32 src_size (the LZ2 blob size) */
        if (src_size < 4) vectra_error("truncated huffman header");
        uint32_t lz2_size = (uint32_t)src[0] |
                            ((uint32_t)src[1] << 8) |
                            ((uint32_t)src[2] << 16) |
                            ((uint32_t)src[3] << 24);

        uint8_t *lz2_buf = (uint8_t *)malloc((size_t)lz2_size);
        if (!lz2_buf) vectra_error("alloc failed in vtr_decompress_into");

        const tdc_entropy_vt *hvt = tdc_entropy_get(TDC_ENTROPY_HUFFMAN);
        if (!hvt) vectra_error("tdc huffman vtable missing");
        tdc_status st = hvt->decode(src, src_size, lz2_buf, lz2_size);
        if (st != TDC_OK) {
            free(lz2_buf);
            vectra_error("tdc huffman decode failed: %d", (int)st);
        }

        vtr_lz2_decompress_into(dst, uncompressed_size, lz2_buf, lz2_size);
        free(lz2_buf);
    } else {
        vectra_error("unknown compression tag: 0x%02x", compression);
    }
}

/* Decompress + unshuffle into caller-provided buffer. */
void vtr_decompress_unshuffle_into(uint8_t *dst, uint32_t uncompressed_size,
                                   const uint8_t *src, uint32_t src_size,
                                   uint8_t compression, uint8_t elem_size) {
    vtr_decompress_into(dst, uncompressed_size, src, src_size, compression);
    if (elem_size > 0)
        vtr_byte_unshuffle(dst, uncompressed_size / elem_size, elem_size);
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

static void plain_decode(VecArray *col, int64_t n_rows,
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
 * DIFF encoding — signed differencing for any fixed-width type
 *
 * On-disk layout (before compression):
 *   first_value (elem_size bytes)
 *   signed differences[n_rows - 1] (elem_size bytes each)
 *
 * Works on all integer types and float64. For slowly varying data,
 * differences are small values near zero. After byte-shuffle, the high
 * bytes are mostly 0x00/0xFF which LZ2 crushes.
 * ================================================================ */

/* Heuristic: should we diff-encode this column? Check that consecutive
   differences have low variance (mean abs diff < range/4). */
static int should_diff_encode(const VecArray *col, int64_t n_rows) {
    if (n_rows < 4) return 0;
    /* Must be a fixed-width numeric type */
    if (col->type == VEC_STRING || col->type == VEC_BOOL) return 0;
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
static uint8_t *diff_encode(const VecArray *col, int64_t n_rows,
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

static void diff_decode(VecArray *col, int64_t n_rows,
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
static void spatial_encode_int(const void *src, VecType src_type,
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
static void spatial_decode_int(int64_t *dst, const int64_t *residuals,
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
static int32_t *plane_encode(const void *src, VecType src_type,
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

static void plane_decode(int64_t *dst, const int64_t *residuals, int64_t n,
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
static int auto_select_predictor(const void *src, VecType src_type,
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

static void quantize_float_to_int(const double *src, int64_t n_rows,
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
 * Top-level encode/decode
 * ================================================================ */

/* Minimum size to bother with compression */
#define COMPRESS_THRESHOLD 64

/* Determine shuffle element size from column type + encoding.
 * Returns 0 if shuffle is not applicable (variable-length or too small). */
uint8_t vtr_shuffle_elem_size(VecType type, uint8_t encoding) {
    if (encoding == VTR_ENC_DICTIONARY) return 0; /* RLE: variable layout */
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

VtrEncodedCol vtr_encode_column_ex(const VecArray *col, int64_t n_rows,
                                   int comp_level) {
    VtrEncodedCol result;
    memset(&result, 0, sizeof(result));

    if (n_rows == 0) {
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
    /* Skip DIFF encoding when no compression is requested: DIFF only
       reduces entropy for downstream compression and introduces
       floating-point precision loss for doubles.  Skipping also enables
       zero-copy direct reads in the collect fast path. */
    if (!raw && comp_level != VTR_COMPRESS_NONE &&
        should_diff_encode(col, n_rows)) {
        raw = diff_encode(col, n_rows, &raw_size);
        if (raw) result.encoding = VTR_ENC_DIFF;
    }
    if (!raw) {
        raw = plain_encode(col, n_rows, &raw_size);
        result.encoding = VTR_ENC_PLAIN;
    }

    result.uncompressed_size = raw_size;

    if (comp_level == VTR_COMPRESS_NONE || raw_size <= COMPRESS_THRESHOLD) {
        result.data = raw;
        result.data_size = raw_size;
        result.compression = VTR_COMP_NONE;
        return result;
    }

    /* Determine if shuffle is applicable */
    uint8_t es = vtr_shuffle_elem_size(col->type, result.encoding);
    uint32_t n_elems = (es > 0) ? raw_size / es : 0;

    /* For PLAIN fixed-width encoding, shuffle directly from column buffer
       and reuse raw as the shuffle destination (avoid extra alloc+copy). */
    uint8_t *work = NULL;
    int shuffled_in_raw = 0;
    if (es > 0 && n_elems > 0 && result.encoding == VTR_ENC_PLAIN &&
        col->type != VEC_STRING && col->type != VEC_BOOL) {
        /* raw is a malloc'd copy of the column data. Shuffle from the
           original column buffer directly into raw, repurposing it. */
        const uint8_t *src_ptr = NULL;
        switch (col->type) {
        case VEC_INT64:  src_ptr = (const uint8_t *)col->buf.i64; break;
        case VEC_INT32:  src_ptr = (const uint8_t *)col->buf.i32; break;
        case VEC_INT16:  src_ptr = (const uint8_t *)col->buf.i16; break;
        case VEC_DOUBLE: src_ptr = (const uint8_t *)col->buf.dbl; break;
        default: break;
        }
        if (src_ptr) {
            byte_shuffle(raw, src_ptr, n_elems, es);
            shuffled_in_raw = 1;
        }
    }
    if (es > 0 && n_elems > 0 && !shuffled_in_raw) {
        work = (uint8_t *)malloc(raw_size);
        if (!work) vectra_error("alloc failed in vtr_encode_column_ex");
        byte_shuffle(work, raw, n_elems, es);
    }

    const uint8_t *to_compress = shuffled_in_raw ? raw : (work ? work : raw);
    uint32_t comp_size = 0;
    uint8_t *comp = NULL;
    uint8_t comp_tag = VTR_COMP_NONE;

    comp = vtr_compress_shuffled(to_compress, raw_size, comp_level,
                                 &comp_size, &comp_tag);

    if (comp) {
        free(raw);
        free(work);
        result.data = comp;
        result.data_size = comp_size;
        result.compression = comp_tag;
        return result;
    }

    /* Compression didn't help — return uncompressed.
       If we shuffled in-place into raw, we need to restore original data. */
    free(work);
    if (shuffled_in_raw) {
        /* Re-encode from source since raw was overwritten with shuffled data */
        free(raw);
        raw = plain_encode(col, n_rows, &raw_size);
    }
    result.data = raw;
    result.data_size = raw_size;
    result.compression = VTR_COMP_NONE;
    return result;
}

VtrEncodedCol vtr_encode_column_q(const VecArray *col, int64_t n_rows,
                                  int comp_level,
                                  const VtrQuantizeSpec *qspec) {
    if (!qspec || !qspec->enabled || col->type != VEC_DOUBLE)
        return vtr_encode_column_ex(col, n_rows, comp_level);

    VtrEncodedCol result;
    memset(&result, 0, sizeof(result));

    if (n_rows == 0) {
        result.encoding = VTR_ENC_QUANTIZE;
        result.compression = VTR_COMP_NONE;
        result.data = (uint8_t *)malloc(1);
        result.data_size = 0;
        result.uncompressed_size = 0;
        result.quantize_scale = qspec->scale;
        result.quantize_offset = qspec->offset;
        result.quantize_target_type = (uint8_t)qspec->target_type;
        return result;
    }

    /* Quantize float64 → narrow int */
    VecType tt = qspec->target_type;
    uint8_t es = vec_type_elem_size(tt);
    uint32_t raw_size = (uint32_t)((uint32_t)n_rows * es);
    uint8_t *raw = (uint8_t *)calloc(raw_size > 0 ? raw_size : 1, 1);
    if (!raw) vectra_error("alloc failed in vtr_encode_column_q");

    int overflow = 0;
    quantize_float_to_int(col->buf.dbl, n_rows, col->validity,
                          qspec->scale, qspec->offset, tt, raw, &overflow);

    result.encoding = VTR_ENC_QUANTIZE;
    result.quantize_scale = qspec->scale;
    result.quantize_offset = qspec->offset;
    result.quantize_target_type = (uint8_t)tt;
    result.quantize_overflow = overflow;
    result.uncompressed_size = raw_size;

    if (comp_level == VTR_COMPRESS_NONE || raw_size <= COMPRESS_THRESHOLD) {
        result.data = raw;
        result.data_size = raw_size;
        result.compression = VTR_COMP_NONE;
        return result;
    }

    /* Byte-shuffle if elem_size > 1 */
    uint8_t *work = NULL;
    uint32_t n_elems = raw_size / es;
    if (es > 1 && n_elems > 0) {
        work = (uint8_t *)malloc(raw_size);
        if (!work) vectra_error("alloc failed in vtr_encode_column_q");
        byte_shuffle(work, raw, n_elems, es);
    }

    const uint8_t *to_compress = work ? work : raw;
    uint32_t comp_size = 0;
    uint8_t *comp = NULL;
    uint8_t comp_tag = VTR_COMP_NONE;

    comp = vtr_compress_shuffled(to_compress, raw_size, comp_level,
                                 &comp_size, &comp_tag);

    if (comp) {
        free(raw);
        free(work);
        result.data = comp;
        result.data_size = comp_size;
        result.compression = comp_tag;
        return result;
    }

    free(work);
    result.data = raw;
    result.data_size = raw_size;
    result.compression = VTR_COMP_NONE;
    return result;
}

/* Legacy wrapper: uses LZ_VTR (backward compat for old callers) */
VtrEncodedCol vtr_encode_column(const VecArray *col, int64_t n_rows) {
    return vtr_encode_column_ex(col, n_rows, VTR_COMPRESS_FAST);
}

void vtr_decode_column(VecArray *col, int64_t n_rows,
                       uint8_t encoding, uint8_t compression,
                       const uint8_t *data, uint32_t data_size,
                       uint32_t uncompressed_size) {
    if (n_rows == 0) return;

    /* Decompress + unshuffle if needed */
    const uint8_t *decoded_data = data;
    uint8_t *decompressed = NULL;

    if (compression == VTR_COMP_SHUFFLE_LZ2 ||
        compression == VTR_COMP_SHUFFLE_LZ2_HUFF) {
        PROF_TIME_START(t0);
        decompressed = (uint8_t *)malloc((size_t)uncompressed_size);
        if (!decompressed) vectra_error("alloc failed in vtr_decode_column");
        uint8_t es = vtr_shuffle_elem_size(col->type, encoding);
        vtr_decompress_unshuffle_into(decompressed, uncompressed_size,
                                      data, data_size, compression, es);
        PROF_TIME_ACC(g_prof_decompress_ns, t0);
        PROF_INC(g_prof_calls);
        decoded_data = decompressed;
        data_size = uncompressed_size;
    } else if (compression != VTR_COMP_NONE) {
        vectra_error("unknown compression tag: 0x%02x", compression);
    }

    /* Decode */
    PROF_TIME_START(td0);
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
    case VTR_ENC_DIFF:
        diff_decode(col, n_rows, decoded_data, data_size);
        break;
    default:
        if (decompressed) free(decompressed);
        vectra_error("unknown encoding tag: 0x%02x", encoding);
    }
    PROF_TIME_ACC(g_prof_decode_ns, td0);

    free(decompressed);
}

void vtr_decode_column_raw(VecArray *col, int64_t n_rows,
                           uint8_t encoding,
                           const uint8_t *data, uint32_t data_size) {
    if (n_rows == 0) return;
    switch (encoding) {
    case VTR_ENC_PLAIN:
        plain_decode(col, n_rows, data, data_size);
        break;
    case VTR_ENC_DICTIONARY:
        dict_decode(col, n_rows, data, data_size);
        break;
    case VTR_ENC_DELTA:
        delta_decode(col, n_rows, data, data_size);
        break;
    case VTR_ENC_DIFF:
        diff_decode(col, n_rows, data, data_size);
        break;
    default:
        vectra_error("unknown encoding tag: 0x%02x", encoding);
    }
}

int vtr_decode_column_raw_into(VecType type, int64_t n_rows,
                               uint8_t encoding,
                               const uint8_t *src, uint32_t src_size,
                               void *dst) {
    (void)src_size;
    if (n_rows == 0) return 1;
    if (!dst || !src) return 0;

    switch (encoding) {
    case VTR_ENC_PLAIN: {
        uint8_t es = vec_type_elem_size(type);
        if (es == 0) return 0; /* variable-length / unsupported */
        memcpy(dst, src, (size_t)n_rows * es);
        return 1;
    }
    case VTR_ENC_DELTA: {
        if (type != VEC_INT64) return 0;
        int64_t *out = (int64_t *)dst;
        int64_t val;
        memcpy(&val, src, 8);
        out[0] = val;
        for (int64_t i = 1; i < n_rows; i++) {
            memcpy(&val, src + i * 8, 8);
            out[i] = out[i - 1] + val;
        }
        return 1;
    }
    case VTR_ENC_DIFF: {
        if (type == VEC_INT64) {
            int64_t *out = (int64_t *)dst;
            const int64_t *in = (const int64_t *)src;
            out[0] = in[0];
            for (int64_t i = 1; i < n_rows; i++)
                out[i] = out[i - 1] + in[i];
            return 1;
        }
        if (type == VEC_DOUBLE) {
            double *out = (double *)dst;
            const double *in = (const double *)src;
            out[0] = in[0];
            for (int64_t i = 1; i < n_rows; i++)
                out[i] = out[i - 1] + in[i];
            return 1;
        }
        return 0; /* narrow ints not in the direct-write contract today */
    }
    default:
        return 0; /* DICTIONARY / QUANTIZE / SPATIAL handled by their own paths */
    }
}

/* ================================================================
 * Spatial-aware encode entry point
 *
 * If sspec is enabled, applies spatial prediction to the column
 * (after optional quantization), converting values to int64 residuals.
 * The residual column is then encoded with the standard pipeline
 * (DIFF auto-selection, compression, shuffle).
 * ================================================================ */

VtrEncodedCol vtr_encode_column_qs(const VecArray *col, int64_t n_rows,
                                   int comp_level,
                                   const VtrQuantizeSpec *qspec,
                                   const VtrSpatialSpec *sspec) {
    /* If no spatial spec, fall through to quantize-only path */
    if (!sspec || !sspec->enabled)
        return vtr_encode_column_q(col, n_rows, comp_level, qspec);

    /* Validate grid dimensions */
    uint32_t nx = sspec->nx;
    uint32_t ny = sspec->ny;
    if ((int64_t)nx * ny != n_rows)
        vectra_error("spatial: nx*ny (%u*%u=%llu) != n_rows (%lld)",
                     nx, ny, (unsigned long long)nx * ny, (long long)n_rows);

    /* First, apply quantization if requested (produces narrow int column) */
    const VecArray *src_col = col;
    VecArray q_col;
    int q_allocated = 0;

    if (qspec && qspec->enabled && col->type == VEC_DOUBLE) {
        /* Quantize to narrow int in a temp VecArray */
        memset(&q_col, 0, sizeof(q_col));
        q_col.type = qspec->target_type;
        q_col.length = n_rows;
        q_col.owns_data = 1;
        uint8_t es = vec_type_elem_size(qspec->target_type);
        uint32_t raw_size = (uint32_t)((uint32_t)n_rows * es);

        switch (qspec->target_type) {
        case VEC_INT8:
            q_col.buf.i8 = (int8_t *)calloc(raw_size > 0 ? raw_size : 1, 1);
            break;
        case VEC_INT16:
            q_col.buf.i16 = (int16_t *)calloc(raw_size > 0 ? raw_size : 1, 1);
            break;
        case VEC_INT32:
            q_col.buf.i32 = (int32_t *)calloc(raw_size > 0 ? raw_size : 1, 1);
            break;
        default:
            break;
        }
        q_col.validity = (uint8_t *)malloc((size_t)vec_validity_bytes(n_rows));
        if (!q_col.validity) vectra_error("alloc failed");
        memcpy(q_col.validity, col->validity, (size_t)vec_validity_bytes(n_rows));

        int overflow = 0;
        uint8_t *dst_ptr = NULL;
        switch (qspec->target_type) {
        case VEC_INT8:  dst_ptr = (uint8_t *)q_col.buf.i8;  break;
        case VEC_INT16: dst_ptr = (uint8_t *)q_col.buf.i16; break;
        case VEC_INT32: dst_ptr = (uint8_t *)q_col.buf.i32; break;
        default: break;
        }
        quantize_float_to_int(col->buf.dbl, n_rows, col->validity,
                              qspec->scale, qspec->offset, qspec->target_type,
                              dst_ptr, &overflow);
        src_col = &q_col;
        q_allocated = 1;
    }

    /* Get source data pointer */
    const void *src_data = NULL;
    VecType src_type = src_col->type;
    switch (src_type) {
    case VEC_INT64:  src_data = src_col->buf.i64; break;
    case VEC_INT32:  src_data = src_col->buf.i32; break;
    case VEC_INT16:  src_data = src_col->buf.i16; break;
    case VEC_INT8:   src_data = src_col->buf.i8;  break;
    case VEC_DOUBLE: src_data = src_col->buf.dbl; break;
    default:
        if (q_allocated) { free(q_col.validity); }
        vectra_error("spatial encoding requires numeric column");
    }

    /* Choose predictor */
    int predictor = sspec->predictor;
    if (predictor < 0)
        predictor = auto_select_predictor(src_data, src_type, n_rows, nx, ny);

    /* Compute residuals as int64 */
    int64_t *residuals = (int64_t *)malloc((size_t)n_rows * sizeof(int64_t));
    if (!residuals) vectra_error("alloc failed in spatial encode");

    int32_t *coeffs = NULL;
    uint32_t n_tiles = 0;
    uint16_t tile_size = sspec->tile_size > 0 ? sspec->tile_size : 32;

    if (predictor == VTR_PRED_PLANE) {
        coeffs = plane_encode(src_data, src_type, residuals, n_rows,
                              nx, ny, tile_size, &n_tiles);
    } else {
        spatial_encode_int(src_data, src_type, residuals, n_rows,
                           nx, ny, predictor);
    }

    if (q_allocated) {
        /* Free temp quantized column */
        switch (q_col.type) {
        case VEC_INT8:  free(q_col.buf.i8);  break;
        case VEC_INT16: free(q_col.buf.i16); break;
        case VEC_INT32: free(q_col.buf.i32); break;
        default: break;
        }
        free(q_col.validity);
    }

    /* Build a temp VecArray from the int64 residuals for encoding */
    VecArray res_col;
    memset(&res_col, 0, sizeof(res_col));
    res_col.type = VEC_INT64;
    res_col.length = n_rows;
    res_col.buf.i64 = residuals;
    res_col.owns_data = 0; /* we'll free residuals ourselves */
    res_col.validity = col->validity; /* borrow original validity */

    /* Encode residuals as PLAIN int64 (no DIFF/DELTA auto-selection — we don't
       store the inner encoding tag, so the reader expects raw int64).
       Apply shuffle + compression directly. */
    uint32_t raw_size = (uint32_t)((uint32_t)n_rows * 8);
    uint8_t *raw = (uint8_t *)malloc(raw_size);
    if (!raw) { free(residuals); vectra_error("alloc failed"); }
    memcpy(raw, residuals, raw_size);
    free(residuals);

    VtrEncodedCol result;
    memset(&result, 0, sizeof(result));
    result.encoding = VTR_ENC_SPATIAL;
    result.uncompressed_size = raw_size;

    if (comp_level == VTR_COMPRESS_NONE || raw_size <= COMPRESS_THRESHOLD) {
        result.data = raw;
        result.data_size = raw_size;
        result.compression = VTR_COMP_NONE;
    } else {
        /* Byte-shuffle int64 (elem_size=8) then compress */
        uint8_t *work = (uint8_t *)malloc(raw_size);
        if (!work) { free(raw); vectra_error("alloc failed"); }
        byte_shuffle(work, raw, (uint32_t)n_rows, 8);

        uint32_t comp_size = 0;
        uint8_t comp_tag = VTR_COMP_NONE;
        uint8_t *comp = vtr_compress_shuffled(work, raw_size, comp_level,
                                              &comp_size, &comp_tag);

        if (comp) {
            free(raw); free(work);
            result.data = comp;
            result.data_size = comp_size;
            result.compression = comp_tag;
        } else {
            free(work);
            result.data = raw;
            result.data_size = raw_size;
            result.compression = VTR_COMP_NONE;
        }
    }

    result.encoding = VTR_ENC_SPATIAL;
    result.spatial_predictor = (uint8_t)predictor;
    result.spatial_nx = nx;
    result.spatial_ny = ny;
    result.spatial_tile_size = tile_size;
    result.spatial_n_tiles = n_tiles;
    result.spatial_coeffs = coeffs; /* caller frees after writing */

    /* Carry quantize metadata if quantization was applied */
    if (qspec && qspec->enabled && col->type == VEC_DOUBLE) {
        result.quantize_scale = qspec->scale;
        result.quantize_offset = qspec->offset;
        result.quantize_target_type = (uint8_t)qspec->target_type;
    }

    return result;
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
