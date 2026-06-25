/*
 * src/format/stats_internal.h — column statistics: compute + wire codec
 *
 * The `tdc_column_stats` struct and the TDC_STATS_VALUE_SIZE constant
 * are defined in the public `tdc/stream.h` header (the streaming API
 * exposes stats to callers via set_rowgroup_stats / get_stats).
 *
 * This internal header adds:
 *   - The on-disk entry size (41 bytes: 1 has_stats + 16 min + 16 max
 *     + 8 null_count), used by the encoder/decoder to size the per-rg
 *     stats block in the trailing row-group index.
 *   - Compute / serialize / parse entry points not meant to be public.
 */

#ifndef TDC_FORMAT_STATS_INTERNAL_H
#define TDC_FORMAT_STATS_INTERNAL_H

#include "tdc/types.h"
#include "tdc/stream.h"   /* tdc_column_stats, TDC_STATS_VALUE_SIZE */
#include <stdint.h>
#include <stddef.h>

#define TDC_STATS_ENTRY_SIZE 41   /* 1 has_stats + 16 min + 16 max + 8 null_count */

/*
 * Compute min/max statistics from a tdc_block. Writes into `out`.
 * Sets has_stats = 1 on success, 0 if dtype is unsupported for stats
 * or the block is empty (n_elems == 0). null_count is always zeroed by
 * compute; callers that track nulls write their own count before
 * handing the struct to set_rowgroup_stats.
 */
tdc_status tdc_stats_compute(const tdc_block *blk, tdc_column_stats *out);

/*
 * Serialize n_cols stats entries into buf (must be n_cols * 41 bytes).
 * Returns bytes written.
 */
size_t tdc_stats_serialize(const tdc_column_stats *stats, uint16_t n_cols,
                           uint8_t *buf);

/*
 * Parse n_cols stats entries from buf. out must point to an array of
 * at least n_cols tdc_column_stats.
 */
tdc_status tdc_stats_parse(const uint8_t *buf, size_t buf_size,
                           uint16_t n_cols, tdc_column_stats *out);

/*
 * Helper: store a typed value into a 16-byte stats buffer.
 * value points to a value of the appropriate dtype. Zero-pads remaining bytes.
 */
void tdc_stats_store_value(uint8_t dst[TDC_STATS_VALUE_SIZE],
                           const void *value, tdc_dtype dtype);

/*
 * Helper: load a typed value from a 16-byte stats buffer into dst.
 * dst must be large enough for the dtype.
 */
void tdc_stats_load_value(const uint8_t src[TDC_STATS_VALUE_SIZE],
                          void *dst, tdc_dtype dtype);

#endif /* TDC_FORMAT_STATS_INTERNAL_H */
