/*
 * src/format/header.c
 *
 * Container header validate. Read/write of full container files lives
 * outside tdc in v0: the only consumer (vectra) brings its own row-group
 * container, so this TU exists only to provide the cheap structural
 * checker that the public header promises.
 *
 * Container header is fixed at 64 bytes (TDC_CONTAINER_HEADER_SIZE).
 * tdc is a prototype with no on-disk consumers — change the layout in
 * place; do not introduce parallel format versions.
 *
 * Validator scope mirrors tdc_block_record_validate: magic, version,
 * reserved bytes, flag/field coherence, dim sanity, no n_elems overflow.
 * It does NOT touch any bytes outside the 64-byte struct.
 */

#include "tdc/format.h"

#include <stddef.h>
#include <stdint.h>

static int hdr_dtype_known(uint8_t dt) {
    return dt >= TDC_DT_I8 && dt <= TDC_DT_STRING;
}

static int hdr_layout_known(uint8_t lo) {
    return lo >= TDC_LAYOUT_VECTOR_1D && lo <= TDC_LAYOUT_VOLUME_3D;
}

static int hdr_layout_rank(uint8_t lo) {
    switch (lo) {
        case TDC_LAYOUT_VECTOR_1D: return 1;
        case TDC_LAYOUT_RASTER_2D: return 2;
        case TDC_LAYOUT_STACK_2D:  return 3;
        case TDC_LAYOUT_VOLUME_3D: return 3;
        default:                   return -1;
    }
}

tdc_status tdc_container_header_validate(const tdc_container_header *h) {
    if (!h) return TDC_E_INVAL;

    if (h->magic   != TDC_CONTAINER_MAGIC)   return TDC_E_CORRUPT;
    if (h->version != TDC_CONTAINER_VERSION) return TDC_E_VERSION;
    if (h->_reserved0 != 0u)                 return TDC_E_CORRUPT;
    if (h->_reserved1 != 0u)                 return TDC_E_CORRUPT;

    /* Heterogeneous containers leave global dtype/layout/rank/dim zeroed
     * because each block carries its own. Homogeneous containers fill
     * them in. The flag and the zero-ness must agree, or callers reading
     * either path get inconsistent answers. */
    int heterogeneous = (h->flags & TDC_CONTAINER_FLAG_HETEROGENEOUS) != 0;

    if (heterogeneous) {
        if (h->global_dtype  != 0u) return TDC_E_CORRUPT;
        if (h->global_layout != 0u) return TDC_E_CORRUPT;
        if (h->global_rank   != 0u) return TDC_E_CORRUPT;
        for (uint8_t i = 0; i < TDC_MAX_RANK; ++i) {
            if (h->global_dim[i] != 0) return TDC_E_CORRUPT;
        }
    } else {
        if (!hdr_dtype_known(h->global_dtype))   return TDC_E_CORRUPT;
        if (!hdr_layout_known(h->global_layout)) return TDC_E_CORRUPT;
        int expected = hdr_layout_rank(h->global_layout);
        if (expected < 0) return TDC_E_CORRUPT;
        if ((int)h->global_rank != expected) return TDC_E_CORRUPT;
        if (h->global_rank == 0 || h->global_rank > TDC_MAX_RANK) return TDC_E_CORRUPT;

        /* Dim sanity: non-negative, no overflow under the n_elems product. */
        int64_t n = 1;
        for (uint8_t i = 0; i < h->global_rank; ++i) {
            int64_t d = h->global_dim[i];
            if (d < 0) return TDC_E_CORRUPT;
            if (d != 0 && n > INT64_MAX / d) return TDC_E_CORRUPT;
            n *= d;
        }
        /* Trailing dim slots beyond rank must be 0 per the format docs. */
        for (uint8_t i = h->global_rank; i < TDC_MAX_RANK; ++i) {
            if (h->global_dim[i] != 0) return TDC_E_CORRUPT;
        }
    }

    /* Index section consistency: a non-empty container needs a non-zero
     * index_offset and index_size. An empty container (n_blocks == 0)
     * may legitimately have both zero. We do NOT range-check
     * index_offset against any container length here — the validator
     * never sees the file, only the 64-byte struct. */
    if (h->n_blocks == 0u) {
        if (h->index_offset != 0u || h->index_size != 0u) return TDC_E_CORRUPT;
    } else {
        if (h->index_size == 0u) return TDC_E_CORRUPT;
        /* index_size must be a whole number of TDC_INDEX_ENTRY_SIZE
         * entries and exactly match n_blocks worth. */
        if (h->index_size % (uint64_t)TDC_INDEX_ENTRY_SIZE != 0u) return TDC_E_CORRUPT;
        if (h->index_size / (uint64_t)TDC_INDEX_ENTRY_SIZE != h->n_blocks) return TDC_E_CORRUPT;
    }

    return TDC_OK;
}
