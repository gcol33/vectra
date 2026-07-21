/*
 * src/format/header.c
 *
 * Container header validate. Read/write of full container files lives
 * outside tdc in v0: the only consumer (vectra) brings its own row-group
 * container, so this TU exists only to provide the cheap structural
 * checker that the public header promises.
 *
 * Container header is fixed at 64 bytes (TDC_CONTAINER_HEADER_SIZE).
 *
 * Two container versions exist: TDC_CONTAINER_VERSION, and
 * TDC_CONTAINER_VERSION_WIDENED for a container whose schema has been
 * relocated to the tail so that columns could be appended without
 * rewriting the body. They differ only in where the schema and the first
 * block live, which the header records explicitly in the widened case.
 *
 * Validator scope mirrors tdc_block_record_validate: magic, version,
 * reserved bytes, flag/field coherence, dim sanity, no n_elems overflow.
 * It does NOT touch any bytes outside the 64-byte struct, so it cannot
 * range-check any offset against the actual file length.
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

    if (h->magic != TDC_CONTAINER_MAGIC) return TDC_E_CORRUPT;
    if (h->version != TDC_CONTAINER_VERSION &&
        h->version != TDC_CONTAINER_VERSION_WIDENED) return TDC_E_VERSION;
    if (h->_reserved0 != 0u) return TDC_E_CORRUPT;

    const int widened = (h->version == TDC_CONTAINER_VERSION_WIDENED);

    /* Heterogeneous containers leave global dtype/layout/rank/dim zeroed
     * because each block carries its own. Homogeneous containers fill
     * them in. The flag and the zero-ness must agree, or callers reading
     * either path get inconsistent answers. */
    int heterogeneous = (h->flags & TDC_CONTAINER_FLAG_HETEROGENEOUS) != 0;

    if (heterogeneous) {
        if (h->global_dtype  != 0u) return TDC_E_CORRUPT;
        if (h->global_layout != 0u) return TDC_E_CORRUPT;
        if (h->global_rank   != 0u) return TDC_E_CORRUPT;

        /* The shape slots are the widened-schema pointers for this kind of
         * container: zero throughout at v1, both set at v2. */
        if (h->u.het._reserved1 != 0u) return TDC_E_CORRUPT;
        if (widened) {
            if (h->u.het.schema_offset == 0u) return TDC_E_CORRUPT;
            if (h->u.het.blocks_start  == 0u) return TDC_E_CORRUPT;
            /* Both sections sit past the header, and the schema is written
             * after the blocks it supersedes. */
            if (h->u.het.blocks_start < TDC_CONTAINER_HEADER_SIZE)
                return TDC_E_CORRUPT;
            if (h->u.het.schema_offset <= h->u.het.blocks_start)
                return TDC_E_CORRUPT;
            if (h->schema_size == 0u) return TDC_E_CORRUPT;
        } else {
            if (h->u.het.schema_offset != 0u) return TDC_E_CORRUPT;
            if (h->u.het.blocks_start  != 0u) return TDC_E_CORRUPT;
        }
    } else {
        /* Only a heterogeneous container can be widened: the relocated
         * schema pointers share the global-shape slots. */
        if (widened) return TDC_E_CORRUPT;

        if (!hdr_dtype_known(h->global_dtype))   return TDC_E_CORRUPT;
        if (!hdr_layout_known(h->global_layout)) return TDC_E_CORRUPT;
        int expected = hdr_layout_rank(h->global_layout);
        if (expected < 0) return TDC_E_CORRUPT;
        if ((int)h->global_rank != expected) return TDC_E_CORRUPT;
        if (h->global_rank == 0 || h->global_rank > TDC_MAX_RANK) return TDC_E_CORRUPT;

        /* Dim sanity: non-negative, no overflow under the n_elems product. */
        int64_t n = 1;
        for (uint8_t i = 0; i < h->global_rank; ++i) {
            int64_t d = h->u.global_dim[i];
            if (d < 0) return TDC_E_CORRUPT;
            if (d != 0 && n > INT64_MAX / d) return TDC_E_CORRUPT;
            n *= d;
        }
        /* Trailing dim slots beyond rank must be 0 per the format docs. */
        for (uint8_t i = h->global_rank; i < TDC_MAX_RANK; ++i) {
            if (h->u.global_dim[i] != 0) return TDC_E_CORRUPT;
        }
    }

    /* Index section consistency: a non-empty container needs a non-zero
     * index_offset and index_size. An empty container (n_blocks == 0)
     * may legitimately have both zero.
     *
     * The trailing index is a row-group index whose size depends on the
     * per-row-group column counts and on whether stats are attached, so
     * it is NOT derivable from n_blocks. Only the presence relationship
     * can be checked from the header alone. */
    if (h->n_blocks == 0u) {
        if (h->index_offset != 0u || h->index_size != 0u) return TDC_E_CORRUPT;
    } else {
        if (h->index_offset == 0u) return TDC_E_CORRUPT;
        if (h->index_size   == 0u) return TDC_E_CORRUPT;
    }

    return TDC_OK;
}
