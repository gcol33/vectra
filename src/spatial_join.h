#ifndef VECTRA_SPATIAL_JOIN_H
#define VECTRA_SPATIAL_JOIN_H

#include <R.h>
#include <Rinternals.h>

/* Lazy streamed spatial join of a left node against a resident layer. See
 * spatial_join.c. */
SEXP C_spatial_join_node(SEXP node_xptr, SEXP wkb_list, SEXP right_df,
                         SEXP geom_sexp, SEXP coords_sexp, SEXP pred_sexp,
                         SEXP dist_sexp, SEXP left_sexp, SEXP out_side_sexp,
                         SEXP out_src_sexp, SEXP out_names_sexp,
                         SEXP nthreads_sexp);

#endif
