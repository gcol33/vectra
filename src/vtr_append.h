#ifndef VECTRA_VTR_APPEND_H
#define VECTRA_VTR_APPEND_H

#include "types.h"
#include <R.h>
#include <Rinternals.h>

/* Append new rows (from node) as one or more new row groups to an existing
   .vtr file. Existing row groups are untouched; only the n_rowgroups field
   in the header is updated. Schema must match exactly. */
void vtr_append_node(VecNode *node, const char *path);

/* .Call entry point: C_append_vtr(node_xptr, path) */
SEXP C_append_vtr(SEXP node_xptr, SEXP path);

#endif /* VECTRA_VTR_APPEND_H */
