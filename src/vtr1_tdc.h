#ifndef VECTRA_VTR1_TDC_H
#define VECTRA_VTR1_TDC_H

/*
 * vtr1_tdc.h — tdc-backed row-group container writer/reader (P3+P4a).
 *
 * Side-by-side companion to vtr1.c. Writes a tdc heterogeneous
 * container (TDC1 magic, per-block dtype/layout, attached schema,
 * trailing row-group index). Each row group becomes one tdc row
 * group; each column becomes one self-describing tdc_block_record.
 *
 * Encode pipeline per column: vtr_codec_tdc_prepare_request +
 * tdc_stream_encoder_write_block. Decode pipeline per column:
 * fseek/fread the raw block bytes (recorded in the row-group index)
 * + vtr_decode_column_tdc, which surfaces the validity bitmap that
 * tdc v0 leaves opaque.
 *
 * P4a wires per-column min/max/null_count statistics through
 * tdc_stream_encoder_set_rowgroup_stats / _decoder_get_stats and
 * propagates VecSchema.col_annotations through the schema-section
 * annotation slot. Annotation slot layout:
 *
 *   [1 byte: vt_name_len] [vt_name_len bytes: vec_type_name]
 *   [remaining ann_len-1-vt_name_len bytes: user annotation]
 *
 * The leading length-prefix carries the VecType discriminator so the
 * reader can distinguish e.g. VEC_BOOL from a future u8 mapping;
 * the rest is the verbatim user annotation (factor levels, quantize
 * spec, etc.).
 *
 * VEC_STRING is rejected at write time (TDC_E_UNSUPPORTED via R-side
 * Rf_error), since vtr_decode_column_tdc cannot round-trip strings
 * until tdc grows a public size query for variable-width payloads.
 *
 * Production read/write entry points (C_write_vtr / C_scan_node) are
 * NOT yet routed through this code — that swap is P4e.
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

/* Per-rowgroup column statistics, indexed by schema column. Returns
 * NULL when stats were not encoded for the row group (e.g. zero-row
 * group) or rg_idx is out of range. The returned array has n_cols
 * entries and is owned by the file. */
const Vtr1ColStat *vtr1_tdc_rowgroup_col_stats(const Vtr1TdcFile *file,
                                               uint32_t rg_idx);

void vtr1_close_tdc(Vtr1TdcFile *file);

#endif /* VECTRA_VTR1_TDC_H */
