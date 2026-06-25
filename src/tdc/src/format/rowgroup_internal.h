/*
 * src/format/rowgroup_internal.h
 *
 * Internal header for the row-group index (TDC1 v2 container format).
 *
 * The trailing index becomes a structured array of row groups, each
 * containing one block entry per column. Wire format:
 *
 *   u64 n_rowgroups
 *   per row group:
 *       u64 offset          (file offset of first block in this group)
 *       u64 n_rows          (rows in this group)
 *       u16 n_cols          (columns; must match schema)
 *       u16 _pad            (alignment padding, must be 0)
 *       u32 _reserved       (future use, must be 0)
 *       per column:
 *           u64 block_offset
 *           u64 block_total
 *
 * Row group fixed header: 8 + 8 + 2 + 2 + 4 = 24 bytes.
 * Column entry: 16 bytes each.
 *
 * All values little-endian on disk (memcpy on LE targets).
 */

#ifndef TDC_FORMAT_ROWGROUP_INTERNAL_H
#define TDC_FORMAT_ROWGROUP_INTERNAL_H

#include "tdc/types.h"
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Per-column entry within a row group (same layout as tdc_index_entry_v1). */
typedef struct {
    uint64_t block_offset;
    uint64_t block_total;
} tdc_rg_col_entry;

/* One row group in the index. */
typedef struct {
    uint64_t          offset;    /* file offset of first block */
    uint64_t          n_rows;
    uint16_t          n_cols;
    tdc_rg_col_entry *columns;   /* array of n_cols entries; owned */
} tdc_rowgroup_entry;

/* Full row-group index. */
typedef struct {
    uint64_t             n_rowgroups;
    tdc_rowgroup_entry  *entries;     /* array of n_rowgroups; owned */
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

/* Parse from buf. Allocates via realloc_fn. Caller must free with
 * tdc_rowgroup_index_free. On error, *out is zeroed. */
tdc_status tdc_rowgroup_index_parse(
    const uint8_t *buf, size_t buf_size,
    void *(*realloc_fn)(void *user, void *ptr, size_t sz),
    void *alloc_user,
    tdc_rowgroup_index *out);

/* Free all memory in a parsed index (entries array + each column array). */
void tdc_rowgroup_index_free(
    tdc_rowgroup_index *idx,
    void *(*realloc_fn)(void *user, void *ptr, size_t sz),
    void *alloc_user);

#ifdef __cplusplus
}
#endif
#endif /* TDC_FORMAT_ROWGROUP_INTERNAL_H */
