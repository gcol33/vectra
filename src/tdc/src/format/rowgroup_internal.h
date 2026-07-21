/*
 * src/format/rowgroup_internal.h
 *
 * Internal header for the trailing row-group index of a TDC1 container.
 *
 * This is the SINGLE definition of the index wire format. The stream
 * encoder, the stream decoder, and the widening encoder all go through
 * these entry points; none of them may hand-roll the layout.
 *
 * Wire format (all little-endian):
 *
 *   u64 n_rowgroups
 *   per row group:
 *       u64 offset          (file offset of first block in this group)
 *       u64 n_rows          (rows in this group)
 *       u16 n_cols          (columns; must match schema)
 *       u16 _pad            (alignment padding, must be 0)
 *       u32 stats_size      (bytes of the trailing stats block; 0 = none)
 *       per column:
 *           u64 block_offset
 *           u64 block_total
 *       [stats block of stats_size bytes, when non-zero]
 *
 * Row group fixed header: 8 + 8 + 2 + 2 + 4 = 24 bytes.
 * Column entry: 16 bytes each.
 * Stats block, when present, is exactly n_cols * TDC_STATS_ENTRY_SIZE bytes
 * and may only appear when the container header carries
 * TDC_CONTAINER_FLAG_HAS_STATS.
 *
 * The per-column entries of a row group are ordered by schema column and
 * carry no column id -- position is identity. A column APPENDED to an
 * existing container (see tdc_stream_encoder_open_widen) is therefore
 * appended at the tail of every row group's column array, matching the
 * tail-append of the widened schema.
 */

#ifndef TDC_FORMAT_ROWGROUP_INTERNAL_H
#define TDC_FORMAT_ROWGROUP_INTERNAL_H

#include "tdc/types.h"
#include "tdc/stream.h"   /* tdc_rg_col_entry, tdc_rowgroup_entry,
                             tdc_column_stats */
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* One row group: the publicly-visible entry plus its optional per-column
 * stats block. Kept as an embedded struct (not a parallel array) so the
 * decoder can hand out `&groups[i].entry` without a second lookup. */
typedef struct {
    tdc_rowgroup_entry entry;      /* offset, n_rows, n_cols, columns */
    uint8_t            has_stats;  /* 1 when stats is populated */
    tdc_column_stats  *stats;      /* entry.n_cols entries; owned */
} tdc_rg_group;

/* Full row-group index. */
typedef struct {
    uint64_t      n_rowgroups;
    tdc_rg_group *groups;          /* array of n_rowgroups; owned */
} tdc_rowgroup_index;

/* Wire sizes. */
#define TDC_RG_HEADER_SIZE   24   /* per-rowgroup fixed header on disk */
#define TDC_RG_COL_SIZE      16   /* per-column entry on disk */

/* Compute serialized size of the full index section (bytes). */
size_t tdc_rowgroup_index_serialized_size(const tdc_rowgroup_index *idx);

/* Serialize the index into buf (must be at least serialized_size bytes).
 * Returns bytes written. */
size_t tdc_rowgroup_index_serialize(const tdc_rowgroup_index *idx,
                                    uint8_t *buf);

/* Parse from buf. `has_stats_flag` is the container header's
 * TDC_CONTAINER_FLAG_HAS_STATS bit: a row group carrying a stats block
 * while the flag is clear is rejected as corrupt. Allocates via
 * realloc_fn; caller frees with tdc_rowgroup_index_free. On error, *out
 * is zeroed and nothing remains allocated. */
tdc_status tdc_rowgroup_index_parse(
    const uint8_t *buf, size_t buf_size,
    int has_stats_flag,
    void *(*realloc_fn)(void *user, void *ptr, size_t sz),
    void *alloc_user,
    tdc_rowgroup_index *out);

/* Free all memory in a parsed index (groups array + each column and
 * stats array). */
void tdc_rowgroup_index_free(
    tdc_rowgroup_index *idx,
    void *(*realloc_fn)(void *user, void *ptr, size_t sz),
    void *alloc_user);

#ifdef __cplusplus
}
#endif
#endif /* TDC_FORMAT_ROWGROUP_INTERNAL_H */
