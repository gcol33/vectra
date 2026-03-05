#ifndef VECTRA_BATCH_H
#define VECTRA_BATCH_H

#include "types.h"

/* Allocate a batch with n_cols columns (arrays uninitialized) */
VecBatch *vec_batch_alloc(int n_cols, int64_t n_rows);

/* Free batch and all its arrays */
void vec_batch_free(VecBatch *batch);

/* Deep-copy column names into batch */
void vec_batch_set_names(VecBatch *batch, char **names);

/* Materialize a batch with selection vector into a flat batch.
   If batch has no sel, returns the batch unchanged.
   If batch has sel, returns a new compacted batch and frees the original. */
VecBatch *vec_batch_compact(VecBatch *batch);

#endif /* VECTRA_BATCH_H */
