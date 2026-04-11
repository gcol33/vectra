#ifndef VECTRA_VTR_CODEC_H
#define VECTRA_VTR_CODEC_H

/*
 * vtr_codec.h — Columnar encoding and compression for .vtr format v4+
 *
 * Encoding layer (applied per column per row group before compression):
 *   PLAIN      — raw bytes, no transformation
 *   DICTIONARY — for string columns with < 50% unique values
 *   DELTA      — for int64 columns with monotonically increasing values
 *
 * Compression layer (applied after encoding):
 *   NONE        — no compression
 *   SHUFFLE_LZ — byte-shuffle + LZ (separated-stream LZ77, 64K window)
 *
 * The byte-shuffle + LZ implementation lives in the vendored tdc tree
 * (src/tdc/). vtr_codec.c contains thin bridge wrappers that delegate to
 * tdc_xform_byte_shuffle_vt and tdc_entropy_lz_vt. The legacy LZ_VTR
 * (256-byte window) and zlib SHUFFLE_DEFLATE codecs were removed when
 * vectra adopted tdc — there is no read-side back-compat for old .vtr
 * files written with those tags.
 */

#include "types.h"
#include <stdint.h>
#include <stddef.h>

/* Encoding tags (1 byte on disk) */
#define VTR_ENC_PLAIN      0x00
#define VTR_ENC_DICTIONARY 0x01
#define VTR_ENC_DELTA      0x02
#define VTR_ENC_QUANTIZE   0x03  /* lossy: float64 → scaled narrow int */
#define VTR_ENC_DIFF       0x04  /* signed differencing (any int/float type) */
#define VTR_ENC_SPATIAL    0x05  /* 2D spatial predictor + residuals */
#define VTR_ENC_DICT_NUM   0x06  /* numeric dictionary (int64 / double,
                                  * < 65536 unique values) */
#define VTR_ENC_SPARSE_ZERO 0x07 /* zero-sparse (int64 / double, bitmap +
                                  * dense non-zero values) */

/* Compression tags (1 byte on disk) */
#define VTR_COMP_NONE                0x00
#define VTR_COMP_SHUFFLE_LZ          0x04
#define VTR_COMP_SHUFFLE_LZ_HUFF     0x05  /* byte-shuffle + LZ + Huffman */
#define VTR_COMP_SHUFFLE_LZ_STREAMS  0x06  /* byte-shuffle + LZ parse split
                                            * into 4 entropy-coded streams */
#define VTR_COMP_SHUFFLE_FSE         0x07  /* byte-shuffle + FSE direct
                                            * (tabled-ANS, no LZ stage) */
#define VTR_COMP_SHUFFLE_HUFF        0x08  /* byte-shuffle + Huffman direct
                                            * (canonical static, no LZ stage) */

/* Compression levels (passed to encoder).
 *
 * FAST runs greedy LZ only.
 *
 * SMALL runs the full candidate menu — greedy LZ, separated-streams LZ,
 * and LZ + Huffman — and writes whichever shrank the block the most. The
 * read side dispatches on the tag each candidate emits
 * (VTR_COMP_SHUFFLE_LZ / _STREAMS / _HUFF), so SMALL is per-block adaptive
 * and is never worse than FAST on any single block. */
#define VTR_COMPRESS_NONE   0
#define VTR_COMPRESS_FAST   1
#define VTR_COMPRESS_SMALL  2

/* Spatial predictor tags (1 byte on disk) */
#define VTR_PRED_LEFT    0
#define VTR_PRED_UP      1
#define VTR_PRED_AVERAGE 2
#define VTR_PRED_PAETH   3
#define VTR_PRED_PLANE   4

/* Per-column spatial specification (write-time) */
typedef struct {
    int      enabled;     /* 0 = no spatial encoding */
    uint32_t nx;          /* raster width */
    uint32_t ny;          /* raster height */
    int      predictor;   /* -1 = auto, 0-4 = forced predictor tag */
    uint16_t tile_size;   /* for plane predictor (default 32) */
} VtrSpatialSpec;

/* Per-column quantization specification (write-time) */
typedef struct {
    int     enabled;       /* 0 = no quantization */
    double  scale;         /* multiplier: stored = round((value - offset) * scale) */
    double  offset;        /* centering offset */
    VecType target_type;   /* VEC_INT8/16/32 */
} VtrQuantizeSpec;

/* Encoded column buffer (intermediate representation between encode and compress) */
typedef struct {
    uint8_t  encoding;        /* VTR_ENC_* */
    uint8_t  compression;     /* VTR_COMP_* */
    uint8_t *data;            /* encoded (and possibly compressed) bytes */
    uint32_t data_size;       /* size of data[] */
    uint32_t uncompressed_size; /* size before compression (== data_size if NONE) */
    /* Quantize metadata (valid when encoding == VTR_ENC_QUANTIZE) */
    double   quantize_scale;
    double   quantize_offset;
    uint8_t  quantize_target_type; /* VecType tag of the narrow int */
    int      quantize_overflow;    /* count of clamped values */
    /* Spatial metadata (valid when encoding == VTR_ENC_SPATIAL) */
    uint8_t  spatial_predictor;    /* VTR_PRED_* tag */
    uint32_t spatial_nx;           /* raster width */
    uint32_t spatial_ny;           /* raster height */
    uint16_t spatial_tile_size;    /* tile size (plane predictor only) */
    uint32_t spatial_n_tiles;      /* number of tiles (plane predictor only) */
    int32_t *spatial_coeffs;       /* 3 per tile (plane predictor only), caller frees */
} VtrEncodedCol;

/*
 * Encode + compress a single column's data payload (not validity bitmap).
 * The caller writes the validity bitmap separately.
 *
 * The function chooses the best encoding automatically:
 *   - VEC_STRING: DICTIONARY if n_unique / n_rows < 0.5, else PLAIN
 *   - VEC_INT64:  DELTA if monotonically increasing, else PLAIN
 *   - VEC_DOUBLE/VEC_BOOL: always PLAIN
 *
 * Then compresses with a built-in LZ77 compressor if the encoded size
 * > 64 bytes (no point compressing tiny buffers). Zero external deps.
 *
 * Returns a VtrEncodedCol. Caller must free .data with free().
 */
VtrEncodedCol vtr_encode_column(const VecArray *col, int64_t n_rows);

/*
 * Encode + compress with explicit compression level.
 * level: VTR_COMPRESS_NONE / VTR_COMPRESS_FAST
 */
VtrEncodedCol vtr_encode_column_ex(const VecArray *col, int64_t n_rows,
                                   int comp_level);

/*
 * Encode + compress with optional lossy quantization.
 * If qspec is non-NULL and enabled, float64 data is quantized to a narrow int
 * before encoding. The VtrEncodedCol output includes quantize metadata.
 */
VtrEncodedCol vtr_encode_column_q(const VecArray *col, int64_t n_rows,
                                  int comp_level,
                                  const VtrQuantizeSpec *qspec);

/*
 * Encode + compress with optional quantization AND spatial encoding.
 * If sspec is non-NULL and enabled, spatial prediction is applied after
 * optional quantization, producing small residuals that compress well.
 */
VtrEncodedCol vtr_encode_column_qs(const VecArray *col, int64_t n_rows,
                                   int comp_level,
                                   const VtrQuantizeSpec *qspec,
                                   const VtrSpatialSpec *sspec);

/*
 * Decode + decompress a column chunk read from a v4 file.
 *
 * Reads the encoded bytes and populates the VecArray's data fields
 * (buf.i64, buf.dbl, buf.bln, or buf.str.*).
 *
 * The VecArray must already have:
 *   - type set
 *   - length set to n_rows
 *   - validity bitmap already read (handled by caller)
 *
 * For PLAIN encoding, this allocates and fills the data buffers.
 * For DICTIONARY/DELTA, this decodes back to the original representation.
 */
void vtr_decode_column(VecArray *col, int64_t n_rows,
                       uint8_t encoding, uint8_t compression,
                       const uint8_t *data, uint32_t data_size,
                       uint32_t uncompressed_size);

/* Profiling: per-call accumulators for the LZ decode path. */
void vtr_codec_profile_reset(void);
void vtr_codec_profile_get(uint64_t *decompress_ns, uint64_t *unshuffle_ns,
                           uint64_t *decode_ns, uint64_t *calls);

/*
 * Decompress LZ (separated-stream) data into a caller-provided buffer.
 * dst must be at least uncompressed_size bytes.
 */
void vtr_lz_decompress_into(uint8_t *dst, uint32_t uncompressed_size,
                             const uint8_t *src, uint32_t src_size);

/*
 * Decompress (LZ or LZ+Huffman) into a caller-provided buffer.
 * Handles VTR_COMP_SHUFFLE_LZ and VTR_COMP_SHUFFLE_LZ_HUFF.
 * Does NOT unshuffle — caller handles that (for fused paths that
 * unshuffle directly into the final destination).
 */
void vtr_decompress_into(uint8_t *dst, uint32_t uncompressed_size,
                         const uint8_t *src, uint32_t src_size,
                         uint8_t compression);

/*
 * Decompress + unshuffle into a caller-provided buffer.
 * Combines vtr_decompress_into + vtr_byte_unshuffle.
 * If elem_size == 0, unshuffle is skipped (variable-length types).
 */
void vtr_decompress_unshuffle_into(uint8_t *dst, uint32_t uncompressed_size,
                                   const uint8_t *src, uint32_t src_size,
                                   uint8_t compression, uint8_t elem_size);

/*
 * Byte-unshuffle in place (via internal temp buffer).
 * n_elems elements of elem_size bytes each.
 * Used by fast-path reader after decompression.
 */
void vtr_byte_unshuffle(uint8_t *data, uint32_t n_elems, uint8_t elem_size);

/*
 * Byte-unshuffle from src to dst (no temp alloc).
 * dst and src must not overlap.
 */
void vtr_byte_unshuffle_to(uint8_t *dst, const uint8_t *src,
                           uint32_t n_elems, uint8_t elem_size);

/*
 * Determine shuffle element size from column type + encoding.
 * Returns 0 if shuffle is not applicable (variable-length, bool, etc.).
 */
uint8_t vtr_shuffle_elem_size(VecType type, uint8_t encoding);

/*
 * Decode a column chunk using pre-decompressed data.
 * Like vtr_decode_column but skips decompression — caller already handled it.
 */
void vtr_decode_column_raw(VecArray *col, int64_t n_rows,
                           uint8_t encoding,
                           const uint8_t *data, uint32_t data_size);

/*
 * Decode a column chunk into a caller-provided fixed-width destination
 * buffer. Used by the collect fast path when an R vector has already been
 * pre-allocated and we want to skip the malloc + memcpy round-trip.
 *
 * Supported (encoding, type) pairs:
 *   PLAIN  + any fixed-width type   — memcpy
 *   DELTA  + VEC_INT64              — cumulative-sum decode
 *   DIFF   + VEC_INT64 / VEC_DOUBLE — cumulative-sum decode
 *
 * The function does NOT touch the validity bitmap. The caller is
 * responsible for patching NA sentinels into dst after this call (the
 * collect path already does this via patch_na_into_direct_real).
 *
 * Returns 1 on success (dst was filled). Returns 0 if the (encoding, type)
 * pair is not handled here — the caller must fall back to
 * vtr_decode_column_raw, which allocates its own buffer.
 *
 * This is the single source of truth for "what direct-write supports". If
 * you add a new encoding and want it to participate in the zero-copy path,
 * extend this function rather than open-coding the dispatch in vtr1.c.
 */
int vtr_decode_column_raw_into(VecType type, int64_t n_rows,
                               uint8_t encoding,
                               const uint8_t *src, uint32_t src_size,
                               void *dst);

/*
 * Parsed view of a DICTIONARY-encoded string column, decoded from a .vtr
 * chunk without materializing a flat string buffer. Used by the direct-read
 * fast path in collect.c to build an R STRSXP by interning each unique
 * dictionary entry once as a CHARSXP and then walking the RLE runs with
 * SET_STRING_ELT, skipping the per-row Rf_mkCharLenCE round-trip.
 *
 * Owned: all pointer fields are heap allocations freed by
 * vtr_dict_blob_free. The blob does NOT alias the source buffer, so it can
 * outlive the scratch buffers used to decompress the chunk (this matters for
 * the parallel reader, where per-thread scratch is freed before collect.c
 * processes the batches).
 *
 * On-disk chunk layout (see dict_decode() in vtr_codec.c):
 *   uint32_t dict_count
 *   int64_t  dict_offsets[dict_count + 1]   (byte offsets into dict_data)
 *   char     dict_data[dict_offsets[dict_count]]
 *   uint32_t n_runs
 *   { uint32_t val; uint32_t len; } runs[n_runs]
 */
typedef struct {
    uint32_t  dict_count;
    int64_t  *dict_offsets;   /* dict_count + 1 entries (aligned copy) */
    char     *dict_data;      /* total_dict_data bytes (not NUL-terminated) */
    uint32_t  n_runs;
    uint32_t *run_vals;       /* n_runs entries (deinterleaved from disk) */
    uint32_t *run_lens;       /* n_runs entries */
} VtrDictBlob;

/*
 * Parse a DICTIONARY-encoded chunk payload into an owned VtrDictBlob. The
 * source buffer is the post-decompression chunk bytes (same buffer that
 * dict_decode consumes). Returns NULL on allocation failure.
 *
 * Caller frees via vtr_dict_blob_free. After parsing, the blob is
 * self-contained and does not reference `src`.
 */
VtrDictBlob *vtr_dict_parse_to_blob(const uint8_t *src, uint32_t src_size);

/* Free a VtrDictBlob and all its owned buffers. Safe to pass NULL. */
void vtr_dict_blob_free(VtrDictBlob *b);

/*
 * Sentinel direct_bufs value for VEC_STRING columns: "don't materialize the
 * flat string buffer; if the on-disk encoding is DICTIONARY, parse into a
 * VtrDictBlob and attach it to the VecArray via arr.str_dict." Non-NULL so
 * `if (direct_bufs && direct_bufs[i])` still signals "caller wants the
 * fast path", but distinct from any real buffer pointer so the decode
 * paths can recognize it. Callers must always pass this exact value (never
 * a computed offset) for string columns.
 */
#define VTR_STRING_DICT_DEFER ((void *)(uintptr_t)1)

/*
 * Dequantize a narrow integer buffer back to float64.
 * dst must be allocated for n_rows doubles.
 * validity is the column's validity bitmap (for skipping NA values).
 */
void vtr_dequantize(double *dst, const uint8_t *src, int64_t n_rows,
                    const uint8_t *validity,
                    double scale, double offset, VecType target_type);

/*
 * Decode spatial residuals back to original values.
 * dst must be allocated for n_rows int64 values.
 * For plane predictor, coeffs points to 3*n_tiles int32 values.
 */
void vtr_spatial_decode(int64_t *dst, const int64_t *residuals, int64_t n_rows,
                        uint32_t nx, uint32_t ny, uint8_t predictor,
                        uint16_t tile_size, const int32_t *coeffs);

/*
 * Dequantize spatial-decoded int64 values back to float64.
 * Combines inverse quantization with spatial decode result.
 */
void vtr_spatial_dequantize(double *dst, const int64_t *values, int64_t n_rows,
                            const uint8_t *validity,
                            double scale, double offset);

#endif /* VECTRA_VTR_CODEC_H */


