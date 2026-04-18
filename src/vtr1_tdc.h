#ifndef VECTRA_VTR1_TDC_H
#define VECTRA_VTR1_TDC_H

/*
 * vtr1_tdc.h — tdc-backed row-group container writer/reader (P3).
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
 * No per-column statistics in P3. tdc supports them via
 * tdc_stream_encoder_set_rowgroup_stats; vtr1_tdc skips them so the
 * gate is purely byte-exact data round-trip. P4 may wire up stats.
 *
 * VEC_STRING is rejected at write time (TDC_E_UNSUPPORTED via R-side
 * Rf_error), since vtr_decode_column_tdc cannot round-trip strings
 * until tdc grows a public size query for variable-width payloads.
 *
 * Production read/write entry points (C_write_vtr / C_scan_node) are
 * NOT yet routed through this code — that swap is P4.
 */

#include "types.h"
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

void vtr1_close_tdc(Vtr1TdcFile *file);

#endif /* VECTRA_VTR1_TDC_H */
