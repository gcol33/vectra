#ifndef VECTRA_SORT_H
#define VECTRA_SORT_H

#include "types.h"

typedef struct {
    int   col_index;
    int   descending;   /* 1 = DESC, 0 = ASC */
} SortKey;

typedef struct {
    VecNode   base;
    VecNode  *child;
    int       n_keys;
    SortKey  *keys;
    int       done;
} SortNode;

SortNode *sort_node_create(VecNode *child, int n_keys, SortKey *keys);

#endif /* VECTRA_SORT_H */
