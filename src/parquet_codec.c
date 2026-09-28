#include "parquet_codec.h"
#include "parquet_meta.h"
#include "miniz/miniz.h"
#include "zstd/zstd.h"
#include <string.h>

void pq_codec_ctx_free(PqCodecCtx *ctx) {
    if (!ctx) return;
    if (ctx->zstd_dctx) ZSTD_freeDCtx((ZSTD_DCtx *)ctx->zstd_dctx);
    ctx->zstd_dctx = NULL;
}

int pq_codec_supported(int codec) {
    switch (codec) {
    case PQ_CODEC_UNCOMPRESSED:
    case PQ_CODEC_SNAPPY:
    case PQ_CODEC_GZIP:
    case PQ_CODEC_LZ4:
    case PQ_CODEC_ZSTD:
    case PQ_CODEC_LZ4_RAW:
        return 1;
    default:
        return 0;
    }
}

const char *pq_codec_name(int codec) {
    switch (codec) {
    case PQ_CODEC_UNCOMPRESSED: return "UNCOMPRESSED";
    case PQ_CODEC_SNAPPY:       return "SNAPPY";
    case PQ_CODEC_GZIP:         return "GZIP";
    case PQ_CODEC_LZO:          return "LZO";
    case PQ_CODEC_BROTLI:       return "BROTLI";
    case PQ_CODEC_LZ4:          return "LZ4";
    case PQ_CODEC_ZSTD:         return "ZSTD";
    case PQ_CODEC_LZ4_RAW:      return "LZ4_RAW";
    default:                    return "unknown";
    }
}

/* Copy a back-reference that may overlap its own output (offset < length),
   which both Snappy and LZ4 use to encode runs. */
static inline void copy_match(uint8_t *op, size_t offset, size_t len) {
    const uint8_t *m = op - offset;
    if (offset >= len) {
        memcpy(op, m, len);
    } else {
        for (size_t i = 0; i < len; i++) op[i] = m[i];
    }
}

/* ---- Snappy (raw block format) ---- */

static int snappy_decode(const uint8_t *src, size_t src_len,
                         uint8_t *dst, size_t dst_len) {
    const uint8_t *ip = src, *iend = src + src_len;
    uint64_t ulen = 0;
    int shift = 0;
    for (;;) {
        if (ip >= iend || shift > 35) return -1;
        uint8_t b = *ip++;
        ulen |= (uint64_t)(b & 0x7f) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    if (ulen != dst_len) return -1;

    size_t op = 0;
    while (ip < iend) {
        uint8_t tag = *ip++;
        size_t len, off;
        switch (tag & 3) {
        case 0: {
            len = (size_t)(tag >> 2) + 1;
            if (len > 60) {
                size_t nb = len - 60;
                if ((size_t)(iend - ip) < nb) return -1;
                size_t v = 0;
                for (size_t k = 0; k < nb; k++) v |= (size_t)ip[k] << (8 * k);
                ip += nb;
                len = v + 1;
            }
            if ((size_t)(iend - ip) < len || dst_len - op < len) return -1;
            memcpy(dst + op, ip, len);
            ip += len;
            op += len;
            continue;
        }
        case 1:
            if (ip >= iend) return -1;
            len = (size_t)((tag >> 2) & 7) + 4;
            off = ((size_t)(tag >> 5) << 8) | *ip++;
            break;
        case 2:
            if (iend - ip < 2) return -1;
            len = (size_t)(tag >> 2) + 1;
            off = (size_t)ip[0] | ((size_t)ip[1] << 8);
            ip += 2;
            break;
        default:
            if (iend - ip < 4) return -1;
            len = (size_t)(tag >> 2) + 1;
            off = (size_t)ip[0] | ((size_t)ip[1] << 8) |
                  ((size_t)ip[2] << 16) | ((size_t)ip[3] << 24);
            ip += 4;
            break;
        }
        if (off == 0 || off > op || dst_len - op < len) return -1;
        copy_match(dst + op, off, len);
        op += len;
    }
    return op == dst_len ? 0 : -1;
}

/* ---- LZ4 block format ---- */

/* Decodes one LZ4 block into dst[0..dst_cap), returning the bytes written or
   -1. */
static int64_t lz4_block_decode(const uint8_t *src, size_t src_len,
                                uint8_t *dst, size_t dst_cap) {
    const uint8_t *ip = src, *iend = src + src_len;
    size_t op = 0;
    while (ip < iend) {
        uint8_t token = *ip++;
        size_t lit = token >> 4;
        if (lit == 15) {
            uint8_t b;
            do {
                if (ip >= iend) return -1;
                b = *ip++;
                lit += b;
            } while (b == 255);
        }
        if ((size_t)(iend - ip) < lit || dst_cap - op < lit) return -1;
        memcpy(dst + op, ip, lit);
        ip += lit;
        op += lit;
        if (ip == iend) break;          /* last sequence: literals only */
        if (iend - ip < 2) return -1;
        size_t off = (size_t)ip[0] | ((size_t)ip[1] << 8);
        ip += 2;
        size_t mlen = token & 15;
        if (mlen == 15) {
            uint8_t b;
            do {
                if (ip >= iend) return -1;
                b = *ip++;
                mlen += b;
            } while (b == 255);
        }
        mlen += 4;
        if (off == 0 || off > op || dst_cap - op < mlen) return -1;
        copy_match(dst + op, off, mlen);
        op += mlen;
    }
    return (int64_t)op;
}

static uint32_t be32(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

/* The deprecated LZ4 codec is the Hadoop framing: a sequence of
   [u32 BE uncompressed length][u32 BE compressed length][LZ4 block]. Some
   writers emitted a bare LZ4 block under the same codec id, which is what the
   frames fail to parse as. */
static int lz4_hadoop_decode(const uint8_t *src, size_t src_len,
                             uint8_t *dst, size_t dst_len) {
    const uint8_t *ip = src, *iend = src + src_len;
    size_t op = 0;
    int framed = 1;
    while (ip < iend) {
        if (iend - ip < 8) { framed = 0; break; }
        uint32_t ulen = be32(ip), clen = be32(ip + 4);
        ip += 8;
        if ((size_t)(iend - ip) < clen || dst_len - op < ulen) { framed = 0; break; }
        int64_t got = lz4_block_decode(ip, clen, dst + op, ulen);
        if (got != (int64_t)ulen) { framed = 0; break; }
        ip += clen;
        op += ulen;
    }
    if (framed && op == dst_len) return 0;
    return lz4_block_decode(src, src_len, dst, dst_len) == (int64_t)dst_len ? 0 : -1;
}

/* ---- GZIP (RFC 1952 member around a raw deflate stream) ---- */

static int gzip_decode(const uint8_t *src, size_t n, uint8_t *dst, size_t dst_len) {
    size_t pos = 0;
    int flags = 0;
    if (n >= 10 && src[0] == 0x1f && src[1] == 0x8b) {
        if (src[2] != 8) return -1;
        uint8_t flg = src[3];
        pos = 10;
        if (flg & 4) {                                 /* FEXTRA */
            if (n - pos < 2) return -1;
            size_t xlen = (size_t)src[pos] | ((size_t)src[pos + 1] << 8);
            pos += 2;
            if (n - pos < xlen) return -1;
            pos += xlen;
        }
        if (flg & 8) {                                 /* FNAME */
            while (pos < n && src[pos]) pos++;
            if (pos++ >= n) return -1;
        }
        if (flg & 16) {                                /* FCOMMENT */
            while (pos < n && src[pos]) pos++;
            if (pos++ >= n) return -1;
        }
        if (flg & 2) pos += 2;                         /* FHCRC */
        if (pos > n) return -1;
    } else if (n >= 2 && (src[0] & 0x0f) == 8 &&
               (((unsigned)src[0] << 8) | src[1]) % 31 == 0) {
        flags = TINFL_FLAG_PARSE_ZLIB_HEADER;          /* zlib-wrapped */
    }
    size_t got = tinfl_decompress_mem_to_mem(dst, dst_len, src + pos, n - pos,
                                             flags);
    return got == dst_len ? 0 : -1;
}

int pq_decompress(PqCodecCtx *ctx, int codec,
                  const uint8_t *src, size_t src_len,
                  uint8_t *dst, size_t dst_len) {
    switch (codec) {
    case PQ_CODEC_UNCOMPRESSED:
        if (src_len != dst_len) return -1;
        if (dst_len) memcpy(dst, src, dst_len);
        return 0;
    case PQ_CODEC_SNAPPY:
        return snappy_decode(src, src_len, dst, dst_len);
    case PQ_CODEC_GZIP:
        return gzip_decode(src, src_len, dst, dst_len);
    case PQ_CODEC_LZ4_RAW:
        return lz4_block_decode(src, src_len, dst, dst_len) == (int64_t)dst_len
               ? 0 : -1;
    case PQ_CODEC_LZ4:
        return lz4_hadoop_decode(src, src_len, dst, dst_len);
    case PQ_CODEC_ZSTD: {
        if (!ctx->zstd_dctx) {
            ctx->zstd_dctx = ZSTD_createDCtx();
            if (!ctx->zstd_dctx) return -1;
        }
        size_t r = ZSTD_decompressDCtx((ZSTD_DCtx *)ctx->zstd_dctx,
                                       dst, dst_len, src, src_len);
        return (!ZSTD_isError(r) && r == dst_len) ? 0 : -1;
    }
    default:
        return -1;
    }
}
