/*
 * fse.h — Native Finite-State Entropy (tANS) coder for vectra.
 *
 * This is a clean-room, from-scratch C11 implementation of the tANS variant
 * popularised by Yann Collet's FSE / zstd. It is intended as the entropy
 * stage that sits underneath vectra's existing LZ (LZ77) layer and on top
 * of the byte-shuffle / dictionary / RLE encoding layer.
 *
 * Design goals (in order of priority):
 *   1. Correctness, including round-trip on degenerate alphabets (single
 *      symbol, two symbols, all 256 symbols).
 *   2. Self-contained: depends only on stdint.h, stddef.h, string.h, no libc
 *      math, no external libraries. Compiles cleanly under
 *      `gcc -O2 -Wall -Wextra -Wpedantic -std=c11`.
 *   3. Honest single-stream layout, easy to reason about. Performance is
 *      explicitly *not* a goal of this prototype — the existing decode loop
 *      is portable C with no SIMD and no manual unrolling. The reference
 *      target is "in the same order of magnitude as zstd's FSE",
 *      ~50–200 MB/s on a modern x86 core; everything beyond that is for a
 *      later optimisation pass.
 *
 * What this is *not*:
 *   - Not a drop-in replacement for libfse: the bitstream layout, header
 *     format, and table-log defaults are deliberately our own and are not
 *     wire-compatible with zstd.
 *   - Not yet integrated with vtr_codec.c. The integration plan lives in
 *     `src/codec/entropy/README.md` and the higher-level architecture lives
 *     in `docs/codec_architecture.md`.
 *
 * Block format (single FSE block):
 *
 *   [u8  flags]                          1 byte
 *       bit 0: 0 = compressed FSE block, 1 = raw uncompressed
 *       bit 1: 0 = multi-symbol, 1 = single-symbol RLE
 *       bits 2-7: reserved (must be 0)
 *
 *   if flags.raw:
 *       [u32 raw_size]                   4 bytes
 *       [raw bytes ...]                  raw_size bytes
 *
 *   else if flags.rle:
 *       [u8  symbol]                     1 byte
 *       (the original length is supplied out-of-band by the caller via
 *        the FSE block's known uncompressed size)
 *
 *   else:
 *       [u8  table_log]                  1 byte, 5..12
 *       [u16 n_symbols_used]             2 bytes (1..256)
 *       [normalised count table ...]     variable length, see below
 *       [u32 bitstream_size]             4 bytes
 *       [u8  bitstream ...]              bitstream_size bytes (little-endian
 *                                         words, sentinel-terminated)
 *
 * Normalised count table encoding (simple, not zstd-compatible):
 *   For each of the 256 possible symbols, write a single signed byte:
 *       0          → symbol unused
 *       -1         → "less than probable", normalised count of 1
 *       1..127     → normalised count = value
 *   If a normalised count would exceed 127 we fall back to a 2-byte
 *   form: 0x80 followed by a u16 little-endian count. This costs us
 *   ~256 bytes per block in the worst case, which is acceptable for
 *   a prototype.
 *
 * The caller is responsible for choosing block sizes. The recommended
 * block size for vectra integration is the LZ literal stream of a single
 * row group (~10 KB to ~1 MB depending on row group size and column type).
 *
 * Error codes (returned negated from encode/decode):
 *    0          OK
 *   -1          dst buffer too small
 *   -2          malformed input (decoder)
 *   -3          input too large for current block format (> 2^31 bytes)
 *   -4          invalid table log
 *   -5          internal sanity check failed (should never happen)
 */

#ifndef VECTRA_CODEC_FSE_H
#define VECTRA_CODEC_FSE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FSE_MAX_TABLE_LOG    12
#define FSE_DEFAULT_TABLE_LOG 11
#define FSE_MIN_TABLE_LOG     5
#define FSE_MAX_SYMBOLS      256

/*
 * Encode `src_size` bytes from `src` into `dst`, writing at most `dst_cap`
 * bytes. On success returns 0 and stores the number of bytes written in
 * `*out_size`. On failure returns a negative error code (see header
 * comment) and `*out_size` is left unchanged.
 *
 * `table_log` controls the size of the FSE table (2^table_log states).
 * Pass 0 to use the default (FSE_DEFAULT_TABLE_LOG).
 */
int fse_encode(const uint8_t *src, size_t src_size,
               uint8_t *dst, size_t dst_cap, size_t *out_size,
               unsigned table_log);

/*
 * Decode an FSE block. `dst_cap` MUST equal the original uncompressed
 * size of the block (the caller is expected to know this from an outer
 * container). On success returns 0 and stores `dst_cap` in `*out_size`.
 * On failure returns a negative error code.
 */
int fse_decode(const uint8_t *src, size_t src_size,
               uint8_t *dst, size_t dst_cap, size_t *out_size);

/* ---------- helpers exposed for testing ---------- */

/*
 * Build a histogram of byte frequencies from `src`. `counts` must hold
 * at least FSE_MAX_SYMBOLS entries. Returns the number of distinct
 * symbols (1..256). The total of `counts` equals `src_size`.
 */
unsigned fse_histogram(const uint8_t *src, size_t src_size,
                       uint32_t counts[FSE_MAX_SYMBOLS]);

/*
 * Normalise an FSE count table so that the sum equals 2^table_log.
 * On entry `counts` holds raw frequencies. On exit `norm` holds signed
 * normalised counts in the range [-1, 2^table_log] where -1 means
 * "less-than-one symbol" (will be assigned a single state).
 *
 * Returns 0 on success, negative on error.
 */
int fse_normalize_counts(const uint32_t counts[FSE_MAX_SYMBOLS],
                         size_t total,
                         int16_t norm[FSE_MAX_SYMBOLS],
                         unsigned table_log);

#ifdef __cplusplus
}
#endif

#endif /* VECTRA_CODEC_FSE_H */
