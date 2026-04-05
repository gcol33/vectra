#ifndef VECTRA_FILTER_H
#define VECTRA_FILTER_H

#include "types.h"
#include "expr.h"

/* Maximum number of AND conjuncts to flatten for reordering */
#define FILTER_MAX_CONJUNCTS 16

typedef struct {
    VecNode   base;
    VecNode  *child;
    VecExpr  *predicate;   /* must evaluate to VEC_BOOL */

    /* Flattened AND conjuncts for selectivity-based reordering.
       When predicate is an AND-chain, conjuncts[] holds pointers into
       the expression tree (not owned — predicate still owns the memory).
       n_conjuncts == 0 means no flattening (single predicate). */
    int        n_conjuncts;
    VecExpr   *conjuncts[FILTER_MAX_CONJUNCTS];
    int        conj_order[FILTER_MAX_CONJUNCTS];  /* evaluation order */
    double     conj_selectivity[FILTER_MAX_CONJUNCTS]; /* fraction passing (0..1) */
    int64_t    conj_total_rows[FILTER_MAX_CONJUNCTS];  /* rows evaluated */
} FilterNode;

/* Create a filter node. Takes ownership of child and predicate. */
FilterNode *filter_node_create(VecNode *child, VecExpr *predicate);

#endif /* VECTRA_FILTER_H */
