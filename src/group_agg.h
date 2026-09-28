#ifndef VECTRA_GROUP_AGG_H
#define VECTRA_GROUP_AGG_H

#include "types.h"
#include "agg_ops.h"
#include "plan_budget.h"

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
    int         use_sorted;  /* 1 = sort-based agg (median/n_distinct) */
    int64_t     mem_budget;  /* budget requested at creation */
    VecMemAcct  mem;         /* reservation on the plan's memory pool: the hash
                                tables + partition buffers, or the holistic
                                stores on the sorted path. Internal sorts (the
                                sorted path's input sort, the result sort)
                                reserve through the same grant. */
    char       *temp_dir;    /* owned copy; run-file dir for every spill */
    void       *sagg;        /* SortedAggState* for the streaming sorted path */
    VecNode    *out;         /* hash path: HashAggNode, or a SortNode over it
                                once the table overflowed; created on first pull */
} GroupAggNode;

/* Create a group-by + aggregate node.
   Takes ownership of child, key_names, and agg_specs.
   Groups are aggregated in a hash table while it fits the budget; past it the
   remaining groups are hash-partitioned to run files under temp_dir and
   aggregated one partition at a time (see group_agg.c). Output is ordered by
   the key columns. median()/n_distinct() take the sort-based path.
   temp_dir: run-file directory; NULL disables spilling (unbounded table).
   mem_budget: node budget in bytes (from vectra_mem()); 0 selects a default. */
GroupAggNode *group_agg_node_create(VecNode *child,
                                    int n_keys, char **key_names,
                                    int n_aggs, AggSpec *agg_specs,
                                    const char *temp_dir, int64_t mem_budget);

#endif /* VECTRA_GROUP_AGG_H */
