#ifndef VECTRA_CONCAT_H
#define VECTRA_CONCAT_H

#include "types.h"

typedef struct {
    VecNode    base;
    int        n_children;
    VecNode  **children;
    int        current;     /* index of current child being drained */
} ConcatNode;

ConcatNode *concat_node_create(int n_children, VecNode **children);

#endif /* VECTRA_CONCAT_H */
