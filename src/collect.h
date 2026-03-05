#ifndef VECTRA_COLLECT_H
#define VECTRA_COLLECT_H

#include "types.h"
#include <R.h>
#include <Rinternals.h>

/* Execute the plan rooted at node, return an R data.frame */
SEXP vec_collect(VecNode *root);

#endif /* VECTRA_COLLECT_H */
