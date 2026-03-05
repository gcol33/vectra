#ifndef VECTRA_TOPN_H
#define VECTRA_TOPN_H

#include "types.h"
#include "sort.h"  /* SortKey */

typedef struct {
    VecNode   base;
    VecNode  *child;
    int       n_keys;
    SortKey  *keys;
    int64_t   limit;
    int       done;
} TopNNode;

TopNNode *topn_node_create(VecNode *child, int n_keys, SortKey *keys,
                            int64_t limit);

#endif /* VECTRA_TOPN_H */
