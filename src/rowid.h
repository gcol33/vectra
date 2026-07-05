#ifndef VECTRA_ROWID_H
#define VECTRA_ROWID_H

#include "types.h"
#include <stdint.h>

/* Appends a monotonic int64 column (0, 1, 2, ...) to each child batch. The
   counter runs over physical rows and persists across batches, so every row
   the child produces gets a globally unique, arrival-ordered id. Streaming,
   O(1) state. Used by the window node to preserve original row order across a
   group-key sort (sort by the id restores arrival order). */
typedef struct {
    VecNode   base;
    VecNode  *child;
    char     *name;
    int64_t   counter;
} RowIdNode;

RowIdNode *rowid_node_create(VecNode *child, const char *name);

#endif /* VECTRA_ROWID_H */
