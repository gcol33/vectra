#ifndef VECTRA_OPTIMIZE_H
#define VECTRA_OPTIMIZE_H

#include "types.h"

/* Run optimization passes on the plan tree before execution.
   - Column pruning: propagates required columns to scan nodes
   - Predicate pushdown: moves filter predicates into scan nodes */
void vec_optimize(VecNode *root);

#endif /* VECTRA_OPTIMIZE_H */
