#ifndef VECTRA_COLLECT_H
#define VECTRA_COLLECT_H

#include "types.h"
#include <R.h>
#include <Rinternals.h>

/* Execute the plan rooted at node, return an R data.frame */
SEXP vec_collect(VecNode *root);

/* Pull one batch from an already-optimized plan as an R data.frame, or
   R_NilValue at end of stream. Empty batches are skipped. The plan must be
   optimized (vec_optimize) once before the first call; each call then advances
   the pull cursor by one batch. Backs the chunked/streaming R verbs. */
SEXP vec_collect_next(VecNode *root);

#endif /* VECTRA_COLLECT_H */
