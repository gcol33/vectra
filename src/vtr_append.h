#ifndef VECTRA_VTR_APPEND_H
#define VECTRA_VTR_APPEND_H

#include "types.h"
#include <R.h>
#include <Rinternals.h>

/* Append the node's rows as new row groups of an existing .vtr file. The
   existing row groups are neither read nor rewritten, so the call costs the
   rows being appended rather than the size of the store. Schema must match
   exactly. comp_level is VTR_COMPRESS_NONE / _FAST / _SMALL. */
void vtr_append_node(VecNode *node, const char *path, int comp_level);

/* .Call entry point: C_append_vtr(node_xptr, path, compress) */
SEXP C_append_vtr(SEXP node_xptr, SEXP path, SEXP compress);

#endif /* VECTRA_VTR_APPEND_H */
