#ifndef VECTRA_VTR_APPEND_COLS_H
#define VECTRA_VTR_APPEND_COLS_H

#include "types.h"
#include <R.h>
#include <Rinternals.h>

/*
 * Append the node's columns to an existing .vtr store, in place.
 *
 * The store's existing columns are never read or rewritten: the new columns
 * are encoded into blocks appended past the container's trailing index, and
 * a replacement schema plus a rebuilt index follow (see vtr1_tdc.h). The
 * peak memory is one row group's worth of the incoming columns, so a table
 * too wide to hold in memory can be built one block of columns at a time.
 *
 * The node must produce exactly as many rows as the store holds, and its
 * column names must not collide with the store's. Row counts can only be
 * checked as the rows arrive, so a mismatch surfaces at the end -- in which
 * case the widen is aborted and the store is left untouched.
 */
void vtr_append_cols_node(VecNode *node, const char *path, int comp_level);

/* .Call entry point: C_append_cols_vtr(node_xptr, path, compress) */
SEXP C_append_cols_vtr(SEXP node_xptr, SEXP path, SEXP compress);

#endif /* VECTRA_VTR_APPEND_COLS_H */
