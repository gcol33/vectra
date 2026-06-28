#ifndef VECTRA_GROUP_TOPN_H
#define VECTRA_GROUP_TOPN_H

#include "types.h"

typedef struct {
    VecNode   base;
    VecNode  *child;
    int       n_keys;
    int      *key_idx;     /* indices of group-key columns in the child schema */
    int       order_idx;   /* index of the order column in the child schema */
    int       descending;  /* 0 = keep the minimum, 1 = keep the maximum */
    int       done;
} GroupTopNNode;

/* Streaming grouped argmin/argmax: one row per group, the row whose order
   value is smallest (descending = 0) or largest (descending = 1) within the
   group. Holds only the running champion per group, so memory is O(#groups)
   in the output, not O(#rows) in the input. NA order values sort last, so a
   known value always wins; a group stays NA only when every row in it is NA.
   Ties keep the first row seen. */
GroupTopNNode *group_topn_node_create(VecNode *child, int n_keys,
                                      const int *key_idx, int order_idx,
                                      int descending);

#endif /* VECTRA_GROUP_TOPN_H */
