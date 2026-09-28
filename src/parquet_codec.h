#ifndef VECTRA_PARQUET_CODEC_H
#define VECTRA_PARQUET_CODEC_H

/*
 * Page decompression for the Parquet reader.
 *
 * Every codec decodes one page into a caller buffer of exactly the page's
 * uncompressed size (known from the page header), and fails rather than
 * writing past it or returning short. SNAPPY and LZ4 are implemented here;
 * GZIP rides the vendored miniz, ZSTD the vendored zstd decoder (src/zstd/).
 */

#include <stdint.h>
#include <stddef.h>

/* Per-thread decompression state (a reusable zstd context). */
typedef struct {
    void *zstd_dctx;
} PqCodecCtx;

void pq_codec_ctx_free(PqCodecCtx *ctx);

/* 1 if the reader can decode this codec. */
int pq_codec_supported(int codec);

const char *pq_codec_name(int codec);

/* Decompress src into dst (dst_len = expected uncompressed size).
   Returns 0 on success, -1 on corrupt input or a size mismatch. */
int pq_decompress(PqCodecCtx *ctx, int codec,
                  const uint8_t *src, size_t src_len,
                  uint8_t *dst, size_t dst_len);

#endif /* VECTRA_PARQUET_CODEC_H */
