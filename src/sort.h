#ifndef VECTRA_SORT_H
#define VECTRA_SORT_H

#include "types.h"

typedef struct {
    int   col_index;
    int   descending;   /* 1 = DESC, 0 = ASC */
    int   na_last;      /* 1 = NA sorts positionally last regardless of desc
                           (dplyr arrange, na.last = TRUE);
                           0 = NA behaves as the maximum value and flips with
                           desc (window value sorts: cume_dist treats NA as the
                           largest value). Both keep the radix run generation
                           and the merge comparator consistent. */
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

    int64_t   total_rows;   /* exact row count, set once input is consumed;
                               -1 until the build phase completes */
} SortNode;

/* Default sort spill threshold, and the floor vectra_mem() enforces. */
#define VECTRA_SORT_MEM_DEFAULT (1024LL * 1024 * 1024)

/* Total-order comparison of one column's value at row ra of `a` versus row rb
   of `b`, matching the sort/merge ordering exactly. `desc` flips the result;
   `na_last` controls NA placement (see SortKey). Exposed so streaming consumers
   that merge over external SortNodes (e.g. the primary-key diff) use the same
   ordering the sort produced, rather than re-deriving it. */
int sort_compare_value(const VecArray *a, int64_t ra,
                       const VecArray *b, int64_t rb, int desc, int na_last);

/* Create a sort node.
   temp_dir: directory for spill files (NULL = in-memory only).
   mem_budget: spill threshold in bytes (0 = unlimited, never spills).
   Takes ownership of the keys array. */
SortNode *sort_node_create(VecNode *child, int n_keys, SortKey *keys,
                           const char *temp_dir, int64_t mem_budget);

/* Exact number of rows the sort will emit. Valid only after the sort has
   consumed its input (i.e. after the first next_batch call); returns -1
   before that. Used by streaming consumers that need the partition size up
   front (window ntile/percent_rank/cume_dist). */
int64_t sort_node_total_rows(const SortNode *sn);

#endif /* VECTRA_SORT_H */
