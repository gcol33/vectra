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
} GroupAggNode;

/* Create a group-by + aggregate node.
   Takes ownership of child, key_names, and agg_specs. */
GroupAggNode *group_agg_node_create(VecNode *child,
                                    int n_keys, char **key_names,
                                    int n_aggs, AggSpec *agg_specs);

#endif /* VECTRA_GROUP_AGG_H */
