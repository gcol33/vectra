#ifndef VECTRA_FEATURE_KNN_H
#define VECTRA_FEATURE_KNN_H

#include <R.h>
#include <Rinternals.h>

/* Feature-space (attribute-space) k-nearest-neighbour over a resident cloud.
 * Build the resident reference cloud once (C_feature_knn_build), then query it
 * one streamed batch at a time (C_feature_knn_query). See feature_knn.c. */
SEXP C_feature_knn_build(SEXP ref_sexp, SEXP transform_sexp);
SEXP C_feature_knn_query(SEXP idx_ptr, SEXP query_sexp, SEXP keff_sexp,
                         SEXP nthreads_sexp);

#endif /* VECTRA_FEATURE_KNN_H */
