#ifndef VECTRA_VTR_DIFF_H
#define VECTRA_VTR_DIFF_H

#include <R.h>
#include <Rinternals.h>

/* C_diff_vtr: streaming key-set diff between two .vtr files.
 *
 * Arguments:
 *   path_a   - STRSXP(1): path to the "old" .vtr file
 *   path_b   - STRSXP(1): path to the "new" .vtr file
 *   key_col  - STRSXP(1): name of the key column (must exist in both files)
 *
 * Returns a named list:
 *   $added_keys   - vector of key values present in B but not A
 *   $deleted_keys - vector of key values present in A but not B
 *
 * Memory: O(n_unique_keys_in_A), never materialises full rows.
 */
SEXP C_diff_vtr(SEXP path_a, SEXP path_b, SEXP key_col);

#endif /* VECTRA_VTR_DIFF_H */
