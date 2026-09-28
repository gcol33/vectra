#ifndef VECTRA_PLAN_BUDGET_H
#define VECTRA_PLAN_BUDGET_H

#include "types.h"

/*
 * Plan-level memory budget.
 *
 * vectra_mem() bounds a whole plan, not each node. Before execution
 * vec_plan_assign_budgets() creates one VecMemPool of `total` bytes for the
 * plan and hands each budgeted node a VecMemGrant on it. A node reserves the
 * bytes it is about to allocate through a VecMemAcct and spills when the
 * reservation is refused; it releases bytes as it frees them. So one node can
 * use most of the budget while the others need little, and the plan's
 * reserved total never exceeds `total`.
 *
 * No node can starve another: each grant owns a floor of total / (4N) that it
 * can always reserve, and only the other three quarters of the pool are
 * shared, first come first served. A grant's bytes up to its floor come from its own floor;
 * bytes past it come from the shared half.
 *
 * Reservations are made on the master thread between batches (never inside an
 * OpenMP region): the pull model runs one node at a time, so the counters are
 * plain integers.
 */

typedef struct VecMemPool VecMemPool;

/* One budgeted node's claim on a pool. Reference-counted: a node and the
   internal or run-time nodes it creates (a grace-hash sub-join, an output
   sort) share their parent's grant. */
struct VecMemGrant {
    VecMemPool *pool;
    int64_t     floor;   /* bytes this grant can always reserve */
    int64_t     held;    /* bytes reserved through this grant */
    int         refs;
};

/* One component's reservation. With no grant (a plan that was never
   optimized) it behaves as a private budget of `cap` bytes; cap <= 0 means
   unbounded. */
typedef struct {
    VecMemGrant *grant;
    int64_t      cap;
    int64_t      held;
} VecMemAcct;

VecMemAcct vec_mem_acct(VecMemGrant *grant, int64_t cap);

/* Reserve so this account holds `want` bytes. Shrinking always succeeds;
   growing succeeds only if the grant's floor or the shared pool covers it.
   Returns 1 when granted (held = want), 0 when refused (held unchanged). */
int vec_mem_try(VecMemAcct *a, int64_t want);

/* Set the reservation to `bytes` unconditionally: for bytes already
   allocated (a batch a node must take to make progress) and for releases. */
void vec_mem_set(VecMemAcct *a, int64_t bytes);

/* Bytes this account could hold right now: what it holds plus what the grant
   would still give it. INT64_MAX when unbounded. */
int64_t vec_mem_allowance(const VecMemAcct *a);

/* Point the account at another grant (NULL = private cap), carrying its
   reservation across. The account takes a reference on the new grant. */
void vec_mem_acct_rebind(VecMemAcct *a, VecMemGrant *grant);

/* Release the reservation and the grant reference. */
void vec_mem_acct_free(VecMemAcct *a);

VecMemGrant *vec_mem_grant_ref(VecMemGrant *g);
void vec_mem_grant_unref(VecMemGrant *g);

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

/* Defines a SetBudgetFn for node struct T whose reservation is the
   VecMemAcct field `field`. */
#define VEC_BUDGET_ACCT_FN(fn, T, field)                            \
    static void fn(VecNode *self, VecMemGrant *grant) {             \
        vec_mem_acct_rebind(&((T *)self)->field, grant);            \
    }

/* Register node as budgeted: `request` is the budget it was created with
   (vectra_mem() or an explicit override), set_grant receives its grant. A
   composite node whose internal nodes share its grant registers once and
   clears their registration (vec_node_clear_budgeted), so the plan counts it
   once. */
void vec_node_set_budgeted(VecNode *node, int64_t request,
                           SetBudgetFn set_grant);
void vec_node_clear_budgeted(VecNode *node);

/* Create the plan's pool (total = the largest request among its budgeted
   nodes) and give every budgeted node a grant on it. Running it again
   (explain() then collect()) replaces the pool; nodes carry their
   reservations across. */
void vec_plan_assign_budgets(VecNode *root);

#endif /* VECTRA_PLAN_BUDGET_H */
