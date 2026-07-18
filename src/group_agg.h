#ifndef VECTRA_GROUP_AGG_H
#define VECTRA_GROUP_AGG_H

#include "types.h"
#include "agg_ops.h"

typedef struct {
    char    *output_name;
    AggKind  kind;
    char    *input_col;   /* NULL for count_star */
    int      na_rm;
} AggSpec;

typedef struct {
    VecNode     base;
    VecNode    *child;
    int         n_keys;
    char      **key_names;
    int         n_aggs;
    AggSpec    *agg_specs;
    int         done;        /* 1 after result emitted (hash path) */
    int         use_sorted;  /* 1 = sort-based agg (spill-safe) */
    int64_t     mem_budget;  /* spill threshold shared by the sort + holistic aggs */
    char       *temp_dir;    /* owned copy; run-file dir for holistic spill */
    void       *sagg;        /* SortedAggState* for the streaming sorted path */
} GroupAggNode;

/* Create a group-by + aggregate node.
   Takes ownership of child, key_names, and agg_specs.
   temp_dir: if non-NULL, enables sort-based aggregation for spill safety and is
   the run-file directory for spill-safe median/n_distinct.
   mem_budget: spill threshold in bytes (from vectra_mem()); 0 selects a
   default. */
GroupAggNode *group_agg_node_create(VecNode *child,
                                    int n_keys, char **key_names,
                                    int n_aggs, AggSpec *agg_specs,
                                    const char *temp_dir, int64_t mem_budget);

#endif /* VECTRA_GROUP_AGG_H */
