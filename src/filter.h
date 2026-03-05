#ifndef VECTRA_FILTER_H
#define VECTRA_FILTER_H

#include "types.h"
#include "expr.h"

typedef struct {
    VecNode   base;
    VecNode  *child;
    VecExpr  *predicate;   /* must evaluate to VEC_BOOL */
} FilterNode;

/* Create a filter node. Takes ownership of child and predicate. */
FilterNode *filter_node_create(VecNode *child, VecExpr *predicate);

#endif /* VECTRA_FILTER_H */
