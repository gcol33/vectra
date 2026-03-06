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
    int       phase;        /* internal: init / memory / merging / done */

    int64_t   mem_budget;   /* spill threshold in bytes; 0 = unlimited */
    char     *temp_dir;     /* directory for spill files; NULL = no spill */

    int       n_runs;       /* number of spilled runs */
    int       runs_cap;
    char    **run_paths;    /* temp file paths (for cleanup) */

    VecBatch *mem_result;   /* in-memory sorted result (single-run path) */
    void     *merge;        /* opaque MergeState* for multi-run merge */
} SortNode;

/* Create a sort node.
   temp_dir: directory for spill files (NULL = in-memory only).
   Takes ownership of the keys array. */
SortNode *sort_node_create(VecNode *child, int n_keys, SortKey *keys,
                           const char *temp_dir);

#endif /* VECTRA_SORT_H */
