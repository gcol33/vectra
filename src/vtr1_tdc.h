#ifndef VECTRA_VTR1_TDC_H
#define VECTRA_VTR1_TDC_H

/*
 * vtr1_tdc.h — tdc-backed row-group container writer/reader.
 *
 * Writes a tdc heterogeneous container (TDC1 magic, per-block
 * dtype/layout, attached schema, trailing row-group index). Each row
 * group becomes one tdc row group; each column becomes one
 * self-describing tdc_block_record.
 *
 * Encode pipeline per column: vtr_codec_tdc_prepare_request +
 * tdc_stream_encoder_write_block. Decode pipeline per column:
 * fseek/fread the raw block bytes (recorded in the row-group index)
 * + vtr_decode_column_tdc, which surfaces the validity bitmap that
 * tdc v0 leaves opaque.
 *
 * Per-column min/max/null_count statistics ride through
 * tdc_stream_encoder_set_rowgroup_stats / _decoder_get_stats.
 * VecSchema.col_annotations are propagated through the schema-section
 * annotation slot with layout:
 *
 *   [1 byte: vt_name_len] [vt_name_len bytes: vec_type_name]
 *   [remaining ann_len-1-vt_name_len bytes: user annotation]
 *
 * The leading length-prefix carries the VecType discriminator so the
 * reader can distinguish e.g. VEC_BOOL from a future u8 mapping;
 * the rest is the verbatim user annotation (factor levels, quantize
 * spec, etc.).
 *
 * VEC_STRING round-trips end-to-end through the tdc varlen decode hook;
 * stats currently surface only null_count (min/max remain zero, mapped
 * to NA on the R side) since packed-prefix string min/max isn't wired.
 */

#include "types.h"
#include "vtr1.h"        /* Vtr1ColStat reused by the reader's stats accessor */
#include "vtr_codec.h"
#include <stdint.h>

/* ---------- writer -------------------------------------------------------- */

typedef struct Vtr1TdcWriter Vtr1TdcWriter;

/* Open a new container for writing. The schema is serialized into the
 * container header section immediately and frozen for the file's
 * lifetime. Aborts via vectra_error on I/O or alloc failure. */
Vtr1TdcWriter *vtr1_open_tdc_writer(const char *path, const VecSchema *schema);

/* Append one row group. batch->n_cols and column types must match the
 * schema passed to open. comp_level is VTR_COMPRESS_NONE / _FAST /
 * _SMALL. qspecs / sspecs may be NULL. */
void vtr1_write_rowgroup_tdc(Vtr1TdcWriter        *w,
                             const VecBatch        *batch,
                             int                    comp_level,
                             const VtrQuantizeSpec *qspecs,
                             const VtrSpatialSpec  *sspecs);

/* Finalize the container (writes the trailing index, patches the
 * header n_blocks/index_offset/index_size) and free w. */
void vtr1_close_tdc_writer(Vtr1TdcWriter *w);

/* ---------- reader -------------------------------------------------------- */

typedef struct Vtr1TdcFile Vtr1TdcFile;

/* Open an existing container: validates the header, reads the schema
 * and the row-group index. Returns NULL on bad magic / version. */
Vtr1TdcFile *vtr1_open_tdc(const char *path);

const VecSchema *vtr1_tdc_schema(const Vtr1TdcFile *file);
uint32_t         vtr1_tdc_n_rowgroups(const Vtr1TdcFile *file);
int64_t          vtr1_tdc_rowgroup_n_rows(const Vtr1TdcFile *file,
                                          uint32_t rg_idx);

/* Read one row group. col_mask is a length-n_cols array; columns with
 * mask[c]==0 are skipped. The returned VecBatch is freshly allocated
 * (vec_batch_free to release). Aborts via vectra_error on corruption. */
VecBatch *vtr1_read_rowgroup_tdc(Vtr1TdcFile *file, uint32_t rg_idx,
                                 const int *col_mask);

/* Direct-write decoder: per output column the caller may supply a
 * pre-allocated, dtype-correct destination buffer in direct_bufs[out_col].
 * Indexed by output column position (post col_mask). When honored, the
 * returned VecArray has owns_data=0 and data_borrowed=1, and its buffer
 * pointer aliases direct_bufs[out_col]; the caller retains ownership of
 * that buffer. Pass direct_bufs == NULL for behavior identical to
 * vtr1_read_rowgroup_tdc.
 *
 * Element-size contract: the buffer must have room for n_rows elements at
 * the on-disk dtype size (8B for double/int64, 4B for int32, 2B for int16,
 * 1B for int8/bool). This matches REAL/INTEGER SEXP storage for
 * VEC_DOUBLE / VEC_INT32. VEC_BOOL is honored too but the caller must
 * supply a uint8-wide buffer (LGLSXP int storage is NOT byte-compatible).
 *
 * Validity bitmap is always owned by the returned VecArray (never
 * borrowed) so callers can free it uniformly via vec_batch_free. */
VecBatch *vtr1_read_rowgroup_tdc_ex(Vtr1TdcFile *file, uint32_t rg_idx,
                                    const int *col_mask,
                                    void **direct_bufs);

/* Per-rowgroup column statistics, indexed by schema column. Returns
 * NULL when stats were not encoded for the row group (e.g. zero-row
 * group) or rg_idx is out of range. The returned array has n_cols
 * entries and is owned by the file. */
const Vtr1ColStat *vtr1_tdc_rowgroup_col_stats(const Vtr1TdcFile *file,
                                               uint32_t rg_idx);

/* Per-column "is sorted across row groups" flag, length n_schema_cols.
 * Bit c is 1 iff every consecutive rowgroup pair has stats[c] with
 * sa.max <= sb.min for the column's dtype. Used by scan.c to narrow
 * the rowgroup range under range/equality predicates via binary search.
 * Returns NULL when the file has <2 row groups (in which case the
 * concept is vacuous); the array is owned by the file. */
const uint8_t *vtr1_tdc_col_sorted(const Vtr1TdcFile *file);

/* Parallel readers: each rowgroup is decoded by an OpenMP worker that
 * holds its own FILE* (no shared seek state) and per-thread scratch.
 * Returns a malloc'd array of length *out_count = n_rowgroups; entries
 * must be freed by the caller via vec_batch_free.
 *
 * The _into variant accepts per-output-column base buffers; thread `t`
 * decoding rowgroup `rg` writes into col_bases[i] + cum_rows[rg] *
 * col_elem_sizes[i] using the direct-write contract from
 * vtr1_read_rowgroup_tdc_ex (8B for double/int64, 4B int32, etc.).
 *
 * IMPORTANT: when col_bases entries reference R SEXP storage (REAL /
 * INTEGER), no R API call may happen on any thread inside the call —
 * any R-side allocation could move the underlying buffer. The decoder
 * itself never touches R; callers must respect the same rule. */
VecBatch **vtr1_read_parallel_tdc(Vtr1TdcFile *file, const int *col_mask,
                                  const char *path, uint32_t *out_count);

VecBatch **vtr1_read_parallel_tdc_into(Vtr1TdcFile *file, const int *col_mask,
                                       const char *path,
                                       void **col_bases,
                                       const size_t *col_elem_sizes,
                                       int n_out_cols,
                                       uint32_t *out_count);

void vtr1_close_tdc(Vtr1TdcFile *file);

#endif /* VECTRA_VTR1_TDC_H */
