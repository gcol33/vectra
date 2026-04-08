#ifndef VECTRA_VTR_WRITE_H
#define VECTRA_VTR_WRITE_H

#include "types.h"
#include "vtr_codec.h"

/* Stream batches from a plan node into a .vtr file.
   Writes to a temp file, then atomically renames to `path`.
   Each batch becomes one row group.
   comp_level: VTR_COMPRESS_NONE / VTR_COMPRESS_FAST */
void vtr_write_node(VecNode *node, const char *path, int comp_level);

/* Same as vtr_write_node but controls row group size.
   If batch_size > 0, accumulates rows and flushes when buffer >= batch_size.
   If batch_size <= 0, each upstream batch becomes one row group (same as vtr_write_node). */
void vtr_write_node_batched(VecNode *node, const char *path, int64_t batch_size,
                            int comp_level);

/* Quantize-aware variants. qspecs is array of n_cols entries (or NULL). */
void vtr_write_node_q(VecNode *node, const char *path, int comp_level,
                      const VtrQuantizeSpec *qspecs);
void vtr_write_node_batched_q(VecNode *node, const char *path, int64_t batch_size,
                              int comp_level, const VtrQuantizeSpec *qspecs);

/* Quantize + spatial-aware variants. sspecs is array of n_cols entries (or NULL). */
void vtr_write_node_qs(VecNode *node, const char *path, int comp_level,
                       const VtrQuantizeSpec *qspecs, const VtrSpatialSpec *sspecs);
void vtr_write_node_batched_qs(VecNode *node, const char *path, int64_t batch_size,
                               int comp_level, const VtrQuantizeSpec *qspecs,
                               const VtrSpatialSpec *sspecs);

#endif /* VECTRA_VTR_WRITE_H */
