#ifndef VECTRA_LIMIT_H
#define VECTRA_LIMIT_H

#include "types.h"

typedef struct {
    VecNode   base;
    VecNode  *child;
    int64_t   max_rows;
    int64_t   rows_emitted;
} LimitNode;

LimitNode *limit_node_create(VecNode *child, int64_t max_rows);

#endif /* VECTRA_LIMIT_H */
