#ifndef VECTRA_GROUP_TOPN_H
#define VECTRA_GROUP_TOPN_H

#include "types.h"
#include "key_snap.h"

typedef struct {
    VecNode   base;
    VecNode  *child;       /* sort( rowid( original child ) ), keyed by group */
    int       n_keys;
    int      *key_idx;     /* group-key column indices (stable in the wider schema) */
    int       order_idx;   /* index of the order column in the child schema */
    int       descending;  /* 0 = keep the minimum, 1 = keep the maximum */
    int       n_cols;      /* number of original (output) columns, excl. row-id */

    /* Streaming state. The child is sorted by (keys, row-id), so each group's
       rows arrive contiguously in input order. One running champion is kept for
       the open group; completed champions accumulate in a bounded store and are
       emitted in row batches, so resident memory is O(emit batch), not
       O(#groups). */
    int       initialized;
    int       done;
    int       input_done;
    VecBatch *cur_batch;   /* child batch currently being scanned */
    int64_t   cur_row;     /* next row to process in cur_batch */
    KeySnap   snap;        /* open group's key values */
    void     *champ;       /* ChampCol[n_cols] bounded champion store */
    int64_t   fill;        /* committed champions waiting to be emitted */
    int       has_group;   /* a group is open (champion at slot `fill`) */
} GroupTopNNode;

/* Streaming grouped argmin/argmax: one row per group, the row whose order
   value is smallest (descending = 0) or largest (descending = 1) within the
   group. Routes through the bounded external sort (keyed by the group columns
   plus a row-id tiebreak), then keeps only the running champion for the open
   group, so memory is O(emit batch), not O(#rows) or O(#groups). NA order
   values sort last, so a known value always wins; a group stays NA only when
   every row in it is NA. Ties keep the first row in input order (the row-id
   tiebreak makes the sorted order stable). temp_dir is borrowed by the inserted
   sort; mem_budget <= 0 selects the sort default. */
GroupTopNNode *group_topn_node_create(VecNode *child, int n_keys,
                                      const int *key_idx, int order_idx,
                                      int descending,
                                      int64_t mem_budget, const char *temp_dir);

#endif /* VECTRA_GROUP_TOPN_H */
