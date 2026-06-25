/*
 * src/entropy/lane.c
 *
 * TDC_ENTROPY_LANE — per-lane entropy coding.
 *
 * After BYTE_SHUFFLE transposes by byte lane, different lanes have very
 * different byte distributions. This backend splits the input into
 * n_lanes equal-sized lanes, applies an independent sub-coder per lane,
 * and packs the results into a single output buffer.
 *
 * AUTO heuristic: computes order-0 Shannon entropy on each lane's byte
 * histogram and selects:
 *   - NONE (memcpy)  if entropy > 7.5 bits/byte (near-random)
 *   - HUFFMAN        if entropy < 4.0 bits/byte (very peaked)
 *   - FSE            otherwise (moderate structure)
 *
 * On-disk header (self-describing — no params needed for decode):
 *   u8   n_lanes
 *   u8   sub_entropy_id[n_lanes]
 *   u32  lane_uncompressed_size          (all lanes same = src_size / n_lanes)
 *   u32  lane_compressed_size[n_lanes]
 *   [lane 0 compressed bytes]
 *   [lane 1 compressed bytes]
 *   ...
 *
 * The sub-coders are called directly via their vtables (extern from
 * entropy_internal.h), not through tdc_entropy_get(). Same pattern as
 * registry.c.
 */

#include "tdc/entropy.h"
#include "tdc/codec.h"
#include "entropy_internal.h"
#include "../core/buffer.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <math.h>

/* ----- Sub-coder vtable lookup ------------------------------------------- */

static const tdc_entropy_vt *lane_sub_vt(tdc_entropy_id id) {
    switch (id) {
        case TDC_ENTROPY_NONE:    return &tdc_entropy_none_vt;
        case TDC_ENTROPY_HUFFMAN: return &tdc_entropy_huffman_vt;
        case TDC_ENTROPY_FSE:     return &tdc_entropy_fse_vt;
        default:                  return NULL;
    }
}

/* ----- Shannon entropy heuristic ----------------------------------------- */
/*
 * Compute order-0 Shannon entropy in bits/byte for a byte buffer.
 * Returns a value in [0.0, 8.0].
 */
static double lane_shannon_entropy(const uint8_t *data, size_t n) {
    if (n == 0) return 0.0;
    uint32_t hist[256] = {0};
    for (size_t i = 0; i < n; ++i)
        hist[data[i]]++;

    double entropy = 0.0;
    double inv_n = 1.0 / (double)n;
    for (int s = 0; s < 256; ++s) {
        if (hist[s] == 0) continue;
        double p = (double)hist[s] * inv_n;
        entropy -= p * log2(p);
    }
    return entropy;
}

static tdc_entropy_id lane_auto_select(const uint8_t *data, size_t n) {
    double h = lane_shannon_entropy(data, n);
    if (h > 7.5) return TDC_ENTROPY_NONE;
    if (h < 4.0) return TDC_ENTROPY_HUFFMAN;
    return TDC_ENTROPY_FSE;
}

/* ----- Header layout ----------------------------------------------------- */
/*
 * Header size = 1 + n_lanes + 4 + 4*n_lanes
 *             = 5 + 5*n_lanes
 */
static size_t lane_header_size(uint8_t n_lanes) {
    return 5u + 5u * (size_t)n_lanes;
}

/* ----- Encode bound ------------------------------------------------------ */

static size_t lane_encode_bound(size_t src_size) {
    /* Worst case: 8 lanes, each at Huffman's bound (~src/8 + overhead),
     * plus the header. Over-estimate conservatively. */
    size_t max_lanes = TDC_MAX_LANES;
    size_t hdr = lane_header_size((uint8_t)max_lanes);
    /* Each sub-coder's bound on (src_size / 1 lane) is at most
     * src_size + some overhead. Huffman bound is src_size + 320ish.
     * Be generous: src_size + 1024 per lane. */
    return hdr + src_size + max_lanes * 1024u;
}

/* ----- Encode ------------------------------------------------------------- */

static tdc_status lane_encode(const uint8_t *src, size_t src_size,
                              const void    *params,
                              tdc_buffer    *dst) {
    if (!dst || !dst->realloc_fn) return TDC_E_INVAL;
    if (src_size > 0 && !src) return TDC_E_INVAL;

    /* Extract params. */
    uint8_t n_lanes = 0;
    tdc_entropy_id lane_ids[TDC_MAX_LANES] = {0};

    if (params) {
        const tdc_lane_entropy_params *lp =
            (const tdc_lane_entropy_params *)params;
        n_lanes = lp->n_lanes;
        for (uint8_t i = 0; i < n_lanes && i < TDC_MAX_LANES; ++i)
            lane_ids[i] = lp->lane_entropy[i];
    }

    if (n_lanes == 0 || n_lanes > TDC_MAX_LANES) return TDC_E_INVAL;
    if (src_size % (size_t)n_lanes != 0) return TDC_E_INVAL;

    size_t lane_size = src_size / (size_t)n_lanes;

    /* Resolve AUTO entries. */
    tdc_entropy_id resolved[TDC_MAX_LANES];
    for (uint8_t i = 0; i < n_lanes; ++i) {
        if (lane_ids[i] == TDC_ENTROPY_NONE) {
            /* AUTO: pick based on Shannon entropy. But TDC_ENTROPY_NONE
             * also means "passthrough" in the enum. For LANE params,
             * NONE means AUTO. We resolve to a real coder. */
            resolved[i] = lane_auto_select(src + (size_t)i * lane_size,
                                           lane_size);
        } else {
            resolved[i] = lane_ids[i];
        }
    }

    /* Allocate output buffer. */
    size_t bound = lane_encode_bound(src_size);
    tdc_status st = tdc_buf_reserve(dst, bound);
    if (st != TDC_OK) return st;

    /* Write header. */
    size_t hdr_size = lane_header_size(n_lanes);
    uint8_t *out = dst->data;
    out[0] = n_lanes;
    for (uint8_t i = 0; i < n_lanes; ++i)
        out[1 + i] = (uint8_t)resolved[i];

    /* lane_uncompressed_size (u32 LE) */
    {
        uint32_t lus = (uint32_t)lane_size;
        memcpy(out + 1u + n_lanes, &lus, 4u);
    }

    /* Compress each lane. */
    size_t payload_offset = hdr_size;
    uint32_t compressed_sizes[TDC_MAX_LANES];

    /* Scratch buffer for sub-coder output. */
    tdc_buffer scratch = {0};
    scratch.realloc_fn = dst->realloc_fn;
    scratch.user       = dst->user;

    for (uint8_t i = 0; i < n_lanes; ++i) {
        const tdc_entropy_vt *sub = lane_sub_vt(resolved[i]);
        if (!sub) {
            if (scratch.data) scratch.realloc_fn(scratch.user, scratch.data, 0);
            return TDC_E_INVAL;
        }

        /* Reset scratch. */
        scratch.size = 0;

        const uint8_t *lane_data = src + (size_t)i * lane_size;
        st = sub->encode(lane_data, lane_size, NULL, &scratch);
        if (st != TDC_OK) {
            if (scratch.data) scratch.realloc_fn(scratch.user, scratch.data, 0);
            return st;
        }

        /* If sub-coder expanded the data, fall back to NONE. */
        if (scratch.size >= lane_size) {
            resolved[i] = TDC_ENTROPY_NONE;
            out[1 + i] = (uint8_t)TDC_ENTROPY_NONE;
            /* Re-encode as passthrough. */
            scratch.size = 0;
            st = tdc_entropy_none_vt.encode(lane_data, lane_size, NULL, &scratch);
            if (st != TDC_OK) {
                if (scratch.data) scratch.realloc_fn(scratch.user, scratch.data, 0);
                return st;
            }
        }

        compressed_sizes[i] = (uint32_t)scratch.size;

        /* Ensure dst has room for the payload. */
        size_t needed = payload_offset + scratch.size;
        if (needed > dst->capacity) {
            st = tdc_buf_reserve(dst, needed);
            if (st != TDC_OK) {
                if (scratch.data) scratch.realloc_fn(scratch.user, scratch.data, 0);
                return st;
            }
            out = dst->data; /* pointer may have moved */
        }

        memcpy(out + payload_offset, scratch.data, scratch.size);
        payload_offset += scratch.size;
    }

    /* Write compressed sizes into header. */
    for (uint8_t i = 0; i < n_lanes; ++i) {
        memcpy(out + 1u + n_lanes + 4u + (size_t)i * 4u,
               &compressed_sizes[i], 4u);
    }

    dst->size = payload_offset;

    /* Free scratch. */
    if (scratch.data) scratch.realloc_fn(scratch.user, scratch.data, 0);

    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status lane_decode(const uint8_t *src, size_t src_size,
                              uint8_t       *dst, size_t dst_size) {
    if (src_size == 0 || !src) return TDC_E_CORRUPT;
    if (dst_size > 0 && !dst) return TDC_E_INVAL;

    /* Read header. */
    uint8_t n_lanes = src[0];
    if (n_lanes == 0 || n_lanes > TDC_MAX_LANES) return TDC_E_CORRUPT;

    size_t hdr_size = lane_header_size(n_lanes);
    if (src_size < hdr_size) return TDC_E_CORRUPT;

    tdc_entropy_id resolved[TDC_MAX_LANES];
    for (uint8_t i = 0; i < n_lanes; ++i)
        resolved[i] = (tdc_entropy_id)src[1 + i];

    uint32_t lane_uncompressed_size;
    memcpy(&lane_uncompressed_size, src + 1u + n_lanes, 4u);

    uint32_t compressed_sizes[TDC_MAX_LANES];
    for (uint8_t i = 0; i < n_lanes; ++i)
        memcpy(&compressed_sizes[i],
               src + 1u + n_lanes + 4u + (size_t)i * 4u, 4u);

    /* Validate: total uncompressed = n_lanes * lane_uncompressed_size == dst_size. */
    size_t total_uncompressed = (size_t)n_lanes * (size_t)lane_uncompressed_size;
    if (total_uncompressed != dst_size) return TDC_E_CORRUPT;

    /* Validate: total compressed fits in src. */
    size_t payload_offset = hdr_size;
    for (uint8_t i = 0; i < n_lanes; ++i) {
        if (payload_offset + compressed_sizes[i] > src_size)
            return TDC_E_CORRUPT;
        payload_offset += compressed_sizes[i];
    }

    /* Decode each lane. */
    payload_offset = hdr_size;
    for (uint8_t i = 0; i < n_lanes; ++i) {
        const tdc_entropy_vt *sub = lane_sub_vt(resolved[i]);
        if (!sub) return TDC_E_CORRUPT;

        uint8_t *lane_dst = dst + (size_t)i * (size_t)lane_uncompressed_size;
        tdc_status st = sub->decode(src + payload_offset,
                                    compressed_sizes[i],
                                    lane_dst,
                                    (size_t)lane_uncompressed_size);
        if (st != TDC_OK) return st;

        payload_offset += compressed_sizes[i];
    }

    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_entropy_vt tdc_entropy_lane_vt = {
    .id           = TDC_ENTROPY_LANE,
    .name         = "lane",
    .encode_bound = lane_encode_bound,
    .encode       = lane_encode,
    .decode       = lane_decode,
};
