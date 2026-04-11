#ifndef VECTRA_VTR_CODEC_INTERNAL_H
#define VECTRA_VTR_CODEC_INTERNAL_H

/*
 * vtr_codec_internal.h — private cross-unit header for the codec split.
 *
 * vtr_codec.c was split into three translation units:
 *   - vtr_compress.c   — tdc/shuffle/LZ/huffman bridge, compress candidate
 *                        menu, decompress dispatch
 *   - vtr_encodings.c  — per-column encodings (PLAIN/DICT/DICT_NUM/
 *                        SPARSE_ZERO/DELTA/DIFF/SPATIAL/QUANTIZE) + RLE
 *                        helpers
 *   - vtr_codec.c      — top-level encode/decode dispatch + profile API
 *
 * This header exposes the small set of symbols that the three units need
 * to share with each other (formerly file-local statics) plus the
 * PROF_* macros and counters used on the hot decode path.
 */

#include "vtr_codec.h"
#include "types.h"
#include "tdc.h"

#include <stdint.h>
#include <stddef.h>

/* ---------------- profile counters ---------------- */

/* Definitions live in vtr_compress.c. The public reset/get wrappers live
 * in vtr_codec.c. Everything is always compiled so init.c links
 * unconditionally; the macros below gate the hot-path increments. */
extern uint64_t g_prof_decompress_ns;
extern uint64_t g_prof_unshuffle_ns;
extern uint64_t g_prof_decode_ns;
extern uint64_t g_prof_calls;
extern uint64_t g_prof_sse2_unshuffle_calls;

#ifdef VTR_PROFILE
#include <time.h>
static inline uint64_t vtr_prof_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}
#  define PROF_TIME_START(t)         uint64_t t = vtr_prof_now_ns()
#  define PROF_TIME_MARK(t)          uint64_t t = vtr_prof_now_ns()
#  define PROF_TIME_ACC(counter, t)  do { (counter) += vtr_prof_now_ns() - (t); } while (0)
#  define PROF_DIFF_ACC(counter, a, b) do { (counter) += (a) - (b); } while (0)
#  define PROF_INC(counter)          do { (counter)++; } while (0)
#else
#  define PROF_TIME_START(t)         ((void)0)
#  define PROF_TIME_MARK(t)          ((void)0)
#  define PROF_TIME_ACC(counter, t)  ((void)0)
#  define PROF_DIFF_ACC(counter, a, b) ((void)0)
#  define PROF_INC(counter)          ((void)0)
#endif

/* ---------------- compress/decompress (vtr_compress.c) ---------------- */

/* Forward byte-shuffle (encoder side). Non-overlapping src/dst. */
void byte_shuffle(uint8_t *dst, const uint8_t *src,
                  uint32_t n_elems, uint8_t elem_size);

/* Inverse byte-shuffle (decoder side). Non-overlapping src/dst.
 * Unprofiled — the profiled wrapper is vtr_byte_unshuffle_to in
 * the public header. This raw version exists for call sites in
 * encodings that do not participate in the main decode-path timer. */
void byte_unshuffle(uint8_t *dst, const uint8_t *src,
                    uint32_t n_elems, uint8_t elem_size);

/* Greedy LZ compress via tdc. Returns malloc'd buffer of size *out_size on
 * success, NULL if compression did not shrink the input. */
uint8_t *lz_vtr_compress(const uint8_t *src, uint32_t src_size,
                         uint32_t *out_size);

/* Separated-streams LZ compress (SMALL mode). Emits the
 * VTR_COMP_SHUFFLE_LZ_STREAMS on-disk format. Same NULL semantics as
 * lz_vtr_compress. */
uint8_t *lz_streams_vtr_compress(const uint8_t *src, uint32_t src_size,
                                 uint32_t *out_size);

/* Direct Huffman compress (no LZ stage). Emits a self-describing tdc
 * huffman blob. Same NULL semantics as lz_vtr_compress. */
uint8_t *huffman_vtr_compress(const uint8_t *src, uint32_t src_size,
                              uint32_t *out_size);

/* Direct FSE compress (no LZ stage). Emits a self-describing tdc FSE
 * blob. Same NULL semantics as lz_vtr_compress. */
uint8_t *fse_vtr_compress(const uint8_t *src, uint32_t src_size,
                          uint32_t *out_size);

/* Alloc + decompress LZ stream into a new malloc'd buffer. */
uint8_t *lz_vtr_decompress(const uint8_t *src, uint32_t src_size,
                           uint32_t uncompressed_size);

/* ---- candidate menu ------------------------------------------------- *
 *
 * A candidate is a (on-disk-tag, encoder) pair. vtr_try_candidates runs
 * each encoder on the same input, keeps whichever produced the smallest
 * output, frees the rest, and writes the winning tag into *out_tag.
 * Earlier candidates win ties (strict `<` comparison), so order the list
 * from "cheapest / most likely to win" to "last resort".
 *
 * An encoder that returns NULL is skipped (candidate did not shrink the
 * input, or the input was too small). If every candidate returns NULL,
 * vtr_try_candidates returns NULL and the caller stores the block raw. */
typedef struct {
    uint8_t   tag;  /* VTR_COMP_SHUFFLE_* emitted on disk */
    uint8_t *(*encode)(const uint8_t *src, uint32_t src_size,
                       uint32_t *out_size);
} VtrCandidate;

uint8_t *vtr_try_candidates(const uint8_t *src, uint32_t src_size,
                            const VtrCandidate *cands, size_t n_cands,
                            uint32_t *out_size, uint8_t *out_tag);

/* Compress a pre-shuffled byte buffer at the requested level.
 * Returns malloc'd buffer on success (sets *out_size and *out_tag),
 * NULL if none of the candidates shrank the input. */
uint8_t *vtr_compress_shuffled(const uint8_t *shuffled, uint32_t size,
                               int comp_level,
                               uint32_t *out_size, uint8_t *out_tag);

/* ---------------- encodings (vtr_encodings.c) ---------------- */

/* PLAIN — raw bytes. */
uint8_t *plain_encode(const VecArray *col, int64_t n_rows, uint32_t *out_size);
void     plain_decode(VecArray *col, int64_t n_rows,
                      const uint8_t *data, uint32_t data_size);

/* DICTIONARY — string columns. */
uint8_t *try_dict_encode(const VecArray *col, int64_t n_rows, uint32_t *out_size);
void     dict_decode(VecArray *col, int64_t n_rows,
                     const uint8_t *data, uint32_t data_size);

/* DICT_NUM — numeric dictionary. */
uint8_t *try_dict_num_encode(const VecArray *col, int64_t n_rows,
                             uint32_t *out_size);
void     dict_num_decode(VecArray *col, int64_t n_rows,
                         const uint8_t *data, uint32_t data_size);

/* SPARSE_ZERO — zero-sparse numeric columns. */
uint8_t *try_sparse_zero_encode(const VecArray *col, int64_t n_rows,
                                int comp_level, uint32_t *out_size);
void     sparse_zero_decode(VecArray *col, int64_t n_rows,
                            const uint8_t *data, uint32_t data_size);

/* DELTA — monotone int64 columns. */
int      should_delta_encode(const VecArray *col, int64_t n_rows);
uint8_t *delta_encode(const VecArray *col, int64_t n_rows, uint32_t *out_size);
void     delta_decode(VecArray *col, int64_t n_rows,
                      const uint8_t *data, uint32_t data_size);

/* DIFF — signed differencing. */
int      should_diff_encode(const VecArray *col, int64_t n_rows);
uint8_t *diff_encode(const VecArray *col, int64_t n_rows, uint32_t *out_size);
void     diff_decode(VecArray *col, int64_t n_rows,
                     const uint8_t *data, uint32_t data_size);

/* QUANTIZE — lossy float64 → narrow int. */
void quantize_float_to_int(const double *src, int64_t n_rows,
                           const uint8_t *validity,
                           double scale, double offset,
                           VecType target_type,
                           uint8_t *dst, int *overflow_count);

/* SPATIAL — 2D predictor + residuals. */
void     spatial_encode_int(const void *src, VecType src_type,
                            int64_t *residuals, int64_t n,
                            uint32_t nx, uint32_t ny, int predictor);
void     spatial_decode_int(int64_t *dst, const int64_t *residuals,
                            int64_t n, uint32_t nx, uint32_t ny,
                            int predictor);
int32_t *plane_encode(const void *src, VecType src_type,
                      int64_t *residuals, int64_t n,
                      uint32_t nx, uint32_t ny, uint16_t tile_size,
                      uint32_t *out_n_tiles);
void     plane_decode(int64_t *dst, const int64_t *residuals, int64_t n,
                      uint32_t nx, uint32_t ny, uint16_t tile_size,
                      const int32_t *coeffs);
int      auto_select_predictor(const void *src, VecType src_type,
                               int64_t n, uint32_t nx, uint32_t ny);

/* SPARSE_ZERO view shared between encodings.c and codec.c direct-write path. */
typedef struct {
    uint32_t n_nonzero;
    uint8_t  gaps_comp_tag;
    uint8_t  vals_comp_tag;
    uint32_t gaps_stream_size;
    uint32_t vals_stream_size;
    const uint8_t *gaps_bytes;
    const uint8_t *vals_bytes;
} SparseZeroView;

int  sparse_zero_parse_header(const uint8_t *data, uint32_t data_size,
                              int64_t n_rows, SparseZeroView *v);
void sparse_zero_fanout_u64(uint64_t *dst, int64_t n_rows,
                            const SparseZeroView *v);

/* DICT_NUM header parsing shared with direct-write fast path. */
int  dict_num_parse_header(const uint8_t *data, uint32_t data_size,
                           uint32_t *out_dict_count,
                           uint8_t *out_idx_width,
                           uint8_t *out_value_bytes,
                           const uint8_t **out_dict_vals,
                           const uint8_t **out_indices);
void dict_num_fanout_u64(uint64_t *dst, int64_t n_rows,
                         const uint8_t *dict_vals, uint32_t dict_count,
                         const uint8_t *indices, uint8_t idx_width);

/* Minimum size to bother with compression. Used by the top-level
 * encode functions in vtr_codec.c. */
#define COMPRESS_THRESHOLD 64

#endif /* VECTRA_VTR_CODEC_INTERNAL_H */
