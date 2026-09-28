#ifndef VECTRA_PLAN_BUDGET_H
#define VECTRA_PLAN_BUDGET_H

#include "types.h"

/* Defines `static int fn(VecNode *self, VecNode **out, int cap)`, a
   ChildrenFn for a node struct T whose single input is the field `field`. */
#define VEC_ONE_CHILD_FN(fn, T, field)                              \
    static int fn(VecNode *self, VecNode **out, int cap) {          \
        if (cap > 0) out[0] = ((T *)self)->field;                   \
        return 1;                                                   \
    }

/* Same for a node with two inputs. */
#define VEC_TWO_CHILDREN_FN(fn, T, first, second)                   \
    static int fn(VecNode *self, VecNode **out, int cap) {          \
        if (cap > 0) out[0] = ((T *)self)->first;                   \
        if (cap > 1) out[1] = ((T *)self)->second;                  \
        return 2;                                                   \
    }

/* Register node as budgeted: *slot is the budget it spills on, currently
   holding the budget it was created with. */
void vec_node_set_budgeted(VecNode *node, int64_t *slot);

/* Divide vectra_mem() among the plan's budgeted nodes. Every budgeted node in
   the tree is live for the whole pull (none releases its state until the plan
   is freed), so their buffers can peak together; each gets an equal share of
   the budget it was created with. Idempotent: shares are recomputed from
   mem_request, so running it again (explain() then collect()) does not shrink
   them further. Nodes created during execution (grace-hash sub-joins) inherit
   their parent's share. */
void vec_plan_assign_budgets(VecNode *root);

#endif /* VECTRA_PLAN_BUDGET_H */
