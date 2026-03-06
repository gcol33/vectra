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
    int         done;       /* 1 after result emitted */
    int         use_sorted; /* 1 = sort-based agg (spill-safe) */
} GroupAggNode;

/* Create a group-by + aggregate node.
   Takes ownership of child, key_names, and agg_specs.
   temp_dir: if non-NULL, enables sort-based aggregation for spill safety. */
GroupAggNode *group_agg_node_create(VecNode *child,
                                    int n_keys, char **key_names,
                                    int n_aggs, AggSpec *agg_specs,
                                    const char *temp_dir);

#endif /* VECTRA_GROUP_AGG_H */
