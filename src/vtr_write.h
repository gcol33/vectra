#ifndef VECTRA_VTR_WRITE_H
#define VECTRA_VTR_WRITE_H

#include "types.h"

/* Stream batches from a plan node into a .vtr file.
   Writes to a temp file, then atomically renames to `path`.
   Each batch becomes one row group. */
void vtr_write_node(VecNode *node, const char *path);

#endif /* VECTRA_VTR_WRITE_H */
