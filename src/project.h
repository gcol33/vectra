#ifndef VECTRA_PROJECT_H
#define VECTRA_PROJECT_H

#include "types.h"
#include "expr.h"

/* A single projection entry: either a pass-through column or a new expression */
typedef struct {
    char    *output_name;
    VecExpr *expr;        /* if NULL, pass through column named output_name */
} ProjEntry;

typedef struct {
    VecNode     base;
    VecNode    *child;
    int         n_entries;
    ProjEntry  *entries;
} ProjectNode;

/* Create a project node.
   entries: array of n ProjEntry (takes ownership of expr pointers and name strings).
   If an entry has expr==NULL, it selects an existing column. */
ProjectNode *project_node_create(VecNode *child, int n_entries, ProjEntry *entries);

#endif /* VECTRA_PROJECT_H */
