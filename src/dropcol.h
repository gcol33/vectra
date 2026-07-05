#ifndef VECTRA_DROPCOL_H
#define VECTRA_DROPCOL_H

#include "types.h"

/* Drops a single column, identified by its position, from each child batch.
   Selecting by index (not name) preserves every remaining column verbatim,
   including duplicate names, so it is a faithful "remove exactly this column"
   rather than a by-name projection. Used to strip the internal row-id column
   the window pipeline carries for order restoration. */
typedef struct {
    VecNode   base;
    VecNode  *child;
    int       drop_idx;
} DropColNode;

DropColNode *dropcol_node_create(VecNode *child, int drop_idx);

#endif /* VECTRA_DROPCOL_H */
