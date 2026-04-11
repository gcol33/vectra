#include "vtr_codec_internal.h"
#include "error.h"

#include <stdlib.h>
#include <string.h>

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
uint64_t g_prof_decompress_ns = 0;
uint64_t g_prof_unshuffle_ns  = 0;
uint64_t g_prof_decode_ns     = 0;
uint64_t g_prof_calls         = 0;
uint64_t g_prof_sse2_unshuffle_calls = 0;  /* SIMD path hit counter */

/* ================================================================
 * tdc bridge — LZ + byte-shuffle now live in src/tdc/.
 *
 * The native LZ77 ("LZ-VTR"), LZ implementation, and SIMD byte-shuffle
 * kernels that used to live here have moved into the vendored tdc tree
 * (src/tdc/src/entropy/lz.c and src/tdc/src/transform/shuffle.c). The
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
void byte_shuffle(uint8_t *dst, const uint8_t *src,
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
void byte_unshuffle(uint8_t *dst, const uint8_t *src,
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

/* ----- LZ bridge --------------------------------------------------------- */

/* Compress with the requested LZ parser. `entropy_id` selects greedy
 * (TDC_ENTROPY_LZ) or optimal (TDC_ENTROPY_LZ_OPT) — both emit the same
 * on-disk LZ stream, so the decoder is shared and the on-disk tag is
 * the same VTR_COMP_SHUFFLE_LZ in both cases.
 *
 * Returns a malloc'd buffer of size *out_size on success, or NULL if
 * compression does not shrink the input (caller falls back to uncompressed). */
static uint8_t *lz_vtr_compress_with(const uint8_t *src, uint32_t src_size,
                                      tdc_entropy_id entropy_id,
                                      uint32_t *out_size) {
    *out_size = 0;
    if (src_size == 0) return NULL;

    const tdc_entropy_vt *vt = tdc_entropy_get(entropy_id);
    if (!vt) vectra_error("tdc lz vtable missing");

    tdc_buffer out = {0};
    out.realloc_fn = vtr_stdlib_realloc;

    tdc_status st = vt->encode(src, src_size, NULL, &out);
    if (st != TDC_OK) {
        free(out.data);
        vectra_error("tdc lz encode failed: %d", (int)st);
    }

    /* Match vectra's "did it shrink?" semantics. */
    if (out.size >= src_size) {
        free(out.data);
        return NULL;
    }

    *out_size = (uint32_t)out.size;
    return out.data;  /* ownership transferred to caller */
}

/* Greedy LZ (FAST/RATIO modes). */
uint8_t *lz_vtr_compress(const uint8_t *src, uint32_t src_size,
                         uint32_t *out_size) {
    return lz_vtr_compress_with(src, src_size, TDC_ENTROPY_LZ, out_size);
}

/* Optimal-parser LZ (single-stream format). Slower encode, smaller output.
 * Same on-disk format as greedy — decoder is unchanged. Kept around for
 * benchmarking and potential future use; the shipped SMALL mode now uses
 * the separated-streams serializer below. */
#if 0  /* currently unused — kept for future benchmarking / tuning */
static uint8_t *lz_opt_vtr_compress(const uint8_t *src, uint32_t src_size,
                                     uint32_t *out_size) {
    return lz_vtr_compress_with(src, src_size, TDC_ENTROPY_LZ_OPT, out_size);
}
#endif

/* Separated-streams LZ (SMALL mode). Different on-disk format (tag
 * VTR_COMP_SHUFFLE_LZ_STREAMS) — the parser output is split into four
 * entropy-coded streams. Decoder is distinct from single-stream LZ. */
uint8_t *lz_streams_vtr_compress(const uint8_t *src, uint32_t src_size,
                                 uint32_t *out_size) {
    return lz_vtr_compress_with(src, src_size, TDC_ENTROPY_LZ_STREAMS, out_size);
}

/* Public: decompress LZ into caller-provided buffer. */
void vtr_lz_decompress_into(uint8_t *dst, uint32_t uncompressed_size,
                             const uint8_t *src, uint32_t src_size) {
    PROF_TIME_START(t0);
    const tdc_entropy_vt *vt = tdc_entropy_get(TDC_ENTROPY_LZ);
    if (!vt) vectra_error("tdc lz vtable missing");
    tdc_status st = vt->decode(src, src_size, dst, uncompressed_size);
    if (st != TDC_OK) vectra_error("tdc lz decode failed: %d", (int)st);
    PROF_TIME_ACC(g_prof_decompress_ns, t0);
    PROF_INC(g_prof_calls);
}

/* Internal: alloc + decompress, used by vtr_decode_column below. */
uint8_t *lz_vtr_decompress(const uint8_t *src, uint32_t src_size,
                           uint32_t uncompressed_size) {
    uint8_t *dst = (uint8_t *)malloc((size_t)uncompressed_size);
    if (!dst) vectra_error("alloc failed in lz_vtr_decompress");
    const tdc_entropy_vt *vt = tdc_entropy_get(TDC_ENTROPY_LZ);
    if (!vt) vectra_error("tdc lz vtable missing");
    tdc_status st = vt->decode(src, src_size, dst, uncompressed_size);
    if (st != TDC_OK) vectra_error("tdc lz decode failed: %d", (int)st);
    return dst;
}

/* ----- Direct entropy bridges (Huffman / FSE) ------------------------------ */

/* Shared helper: invoke a tdc entropy codec by id and wrap the tdc_buffer
 * result in vectra's "malloc'd buffer or NULL if it didn't shrink" contract.
 * The shipped tdc huffman and FSE encoders both emit self-describing blobs
 * (the decoder reads the uncompressed size out of the first header bytes),
 * so the on-disk bytes need no extra framing. */
static uint8_t *tdc_entropy_direct_compress(const uint8_t *src, uint32_t src_size,
                                            tdc_entropy_id entropy_id,
                                            const char *name,
                                            uint32_t *out_size) {
    *out_size = 0;
    if (src_size == 0) return NULL;

    const tdc_entropy_vt *vt = tdc_entropy_get(entropy_id);
    if (!vt) vectra_error("tdc %s vtable missing", name);

    tdc_buffer out = {0};
    out.realloc_fn = vtr_stdlib_realloc;

    tdc_status st = vt->encode(src, src_size, NULL, &out);
    if (st != TDC_OK) {
        free(out.data);
        vectra_error("tdc %s encode failed: %d", name, (int)st);
    }

    if (out.size >= src_size) {
        free(out.data);
        return NULL;
    }

    *out_size = (uint32_t)out.size;
    return out.data;
}

uint8_t *huffman_vtr_compress(const uint8_t *src, uint32_t src_size,
                              uint32_t *out_size) {
    return tdc_entropy_direct_compress(src, src_size,
                                       TDC_ENTROPY_HUFFMAN, "huffman",
                                       out_size);
}

uint8_t *fse_vtr_compress(const uint8_t *src, uint32_t src_size,
                          uint32_t *out_size) {
    return tdc_entropy_direct_compress(src, src_size,
                                       TDC_ENTROPY_FSE, "fse",
                                       out_size);
}

/* Composite: greedy LZ, then Huffman on the LZ output. Emits the combined
 * huffman blob (which is what VTR_COMP_SHUFFLE_LZ_HUFF stores on disk).
 * Returns NULL if LZ didn't shrink the input or if Huffman didn't further
 * shrink the LZ stream — in that case the caller's candidate menu will
 * fall back to the standalone LZ candidate. */
static uint8_t *lz_huff_vtr_compress(const uint8_t *src, uint32_t src_size,
                                     uint32_t *out_size) {
    *out_size = 0;
    uint32_t lz_size = 0;
    uint8_t *lz = lz_vtr_compress(src, src_size, &lz_size);
    if (!lz) return NULL;

    uint32_t hu_size = 0;
    uint8_t *hu = huffman_vtr_compress(lz, lz_size, &hu_size);
    free(lz);
    if (!hu) return NULL;

    *out_size = hu_size;
    return hu;
}

/* ----- Shared compress/decompress helpers ---------------------------------- */

/* Try every candidate in `cands` against the same input, keep whichever
 * returned the smallest buffer, free the rest. Earlier candidates win ties
 * (strict `<`), so list them from cheapest-encode first. */
uint8_t *vtr_try_candidates(const uint8_t *src, uint32_t src_size,
                            const VtrCandidate *cands, size_t n_cands,
                            uint32_t *out_size, uint8_t *out_tag) {
    *out_size = 0;
    *out_tag = VTR_COMP_NONE;

    uint8_t *best = NULL;
    uint32_t best_size = UINT32_MAX;
    uint8_t  best_tag = VTR_COMP_NONE;

    for (size_t i = 0; i < n_cands; ++i) {
        uint32_t sz = 0;
        uint8_t *buf = cands[i].encode(src, src_size, &sz);
        if (!buf) continue;
        if (sz < best_size) {
            if (best) free(best);
            best = buf;
            best_size = sz;
            best_tag = cands[i].tag;
        } else {
            free(buf);
        }
    }

    if (!best) return NULL;
    *out_size = best_size;
    *out_tag = best_tag;
    return best;
}

/* Compress pre-shuffled bytes at the requested level.
 * Returns malloc'd buffer on success (caller owns), NULL if compression
 * did not shrink the input. Sets *out_size and *out_tag.
 *
 * FAST runs greedy LZ only — the hot read/write path for bulk tabular
 * data, optimized for encode speed.
 *
 * SMALL runs a per-block adaptive candidate menu and writes whichever
 * shrank the block the most:
 *
 *   - greedy LZ          — fastest, wins on short / already-compact blocks
 *   - separated-streams  — wins on tabular blocks where LZ tokens have
 *                          enough structure to amortize the 4-stream
 *                          header overhead
 *   - LZ + Huffman       — wins when the LZ sequence bytes carry residual
 *                          redundancy an entropy stage can squeeze out
 *   - direct FSE         — wins on small-alphabet / low-entropy blocks
 *                          where the LZ parser has nothing to find (e.g.
 *                          shuffled zero-rich sparse gap streams)
 *   - direct Huffman     — same regime as FSE, but cheaper decode; wins
 *                          when Huffman's symbol-length limit is not the
 *                          bottleneck
 *
 * SMALL is guaranteed never worse than FAST on any single block: the
 * candidate list contains plain LZ, so FAST's output is always available
 * as a fallback winner. */
uint8_t *vtr_compress_shuffled(const uint8_t *shuffled, uint32_t size,
                               int comp_level,
                               uint32_t *out_size, uint8_t *out_tag) {
    *out_size = 0;
    *out_tag = VTR_COMP_NONE;

    if (comp_level == VTR_COMPRESS_FAST) {
        static const VtrCandidate fast_menu[] = {
            { VTR_COMP_SHUFFLE_LZ, lz_vtr_compress },
        };
        return vtr_try_candidates(shuffled, size,
                                  fast_menu,
                                  sizeof(fast_menu)/sizeof(fast_menu[0]),
                                  out_size, out_tag);
    }

    static const VtrCandidate small_menu[] = {
        { VTR_COMP_SHUFFLE_LZ,         lz_vtr_compress },
        { VTR_COMP_SHUFFLE_LZ_STREAMS, lz_streams_vtr_compress },
        { VTR_COMP_SHUFFLE_LZ_HUFF,    lz_huff_vtr_compress },
        { VTR_COMP_SHUFFLE_FSE,        fse_vtr_compress },
        { VTR_COMP_SHUFFLE_HUFF,       huffman_vtr_compress },
    };
    return vtr_try_candidates(shuffled, size,
                              small_menu,
                              sizeof(small_menu)/sizeof(small_menu[0]),
                              out_size, out_tag);
}

/* Decompress (LZ or LZ+Huffman) into caller-provided buffer.
 * Does NOT unshuffle. */
void vtr_decompress_into(uint8_t *dst, uint32_t uncompressed_size,
                         const uint8_t *src, uint32_t src_size,
                         uint8_t compression) {
    if (compression == VTR_COMP_SHUFFLE_LZ) {
        vtr_lz_decompress_into(dst, uncompressed_size, src, src_size);
    } else if (compression == VTR_COMP_SHUFFLE_LZ_STREAMS) {
        PROF_TIME_START(t0);
        const tdc_entropy_vt *vt = tdc_entropy_get(TDC_ENTROPY_LZ_STREAMS);
        if (!vt) vectra_error("tdc lz_streams vtable missing");
        tdc_status st = vt->decode(src, src_size, dst, uncompressed_size);
        if (st != TDC_OK)
            vectra_error("tdc lz_streams decode failed: %d", (int)st);
        PROF_TIME_ACC(g_prof_decompress_ns, t0);
        PROF_INC(g_prof_calls);
    } else if (compression == VTR_COMP_SHUFFLE_LZ_HUFF) {
        /* Huffman header: first 4 bytes = u32 src_size (the LZ blob size) */
        if (src_size < 4) vectra_error("truncated huffman header");
        uint32_t lz_size = (uint32_t)src[0] |
                            ((uint32_t)src[1] << 8) |
                            ((uint32_t)src[2] << 16) |
                            ((uint32_t)src[3] << 24);

        uint8_t *lz_buf = (uint8_t *)malloc((size_t)lz_size);
        if (!lz_buf) vectra_error("alloc failed in vtr_decompress_into");

        const tdc_entropy_vt *hvt = tdc_entropy_get(TDC_ENTROPY_HUFFMAN);
        if (!hvt) vectra_error("tdc huffman vtable missing");
        tdc_status st = hvt->decode(src, src_size, lz_buf, lz_size);
        if (st != TDC_OK) {
            free(lz_buf);
            vectra_error("tdc huffman decode failed: %d", (int)st);
        }

        vtr_lz_decompress_into(dst, uncompressed_size, lz_buf, lz_size);
        free(lz_buf);
    } else if (compression == VTR_COMP_SHUFFLE_FSE) {
        PROF_TIME_START(t0);
        const tdc_entropy_vt *vt = tdc_entropy_get(TDC_ENTROPY_FSE);
        if (!vt) vectra_error("tdc fse vtable missing");
        tdc_status st = vt->decode(src, src_size, dst, uncompressed_size);
        if (st != TDC_OK) vectra_error("tdc fse decode failed: %d", (int)st);
        PROF_TIME_ACC(g_prof_decompress_ns, t0);
        PROF_INC(g_prof_calls);
    } else if (compression == VTR_COMP_SHUFFLE_HUFF) {
        PROF_TIME_START(t0);
        const tdc_entropy_vt *vt = tdc_entropy_get(TDC_ENTROPY_HUFFMAN);
        if (!vt) vectra_error("tdc huffman vtable missing");
        tdc_status st = vt->decode(src, src_size, dst, uncompressed_size);
        if (st != TDC_OK) vectra_error("tdc huffman decode failed: %d", (int)st);
        PROF_TIME_ACC(g_prof_decompress_ns, t0);
        PROF_INC(g_prof_calls);
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
