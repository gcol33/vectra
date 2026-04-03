#ifndef VECTRA_VTR_WRITE_H
#define VECTRA_VTR_WRITE_H

#include "types.h"

/* Stream batches from a plan node into a .vtr file.
   Writes to a temp file, then atomically renames to `path`.
   Each batch becomes one row group. */
void vtr_write_node(VecNode *node, const char *path);

/* Same as vtr_write_node but controls row group size.
   If batch_size > 0, accumulates rows and flushes when buffer >= batch_size.
   If batch_size <= 0, each upstream batch becomes one row group (same as vtr_write_node). */
void vtr_write_node_batched(VecNode *node, const char *path, int64_t batch_size);

#endif /* VECTRA_VTR_WRITE_H */
