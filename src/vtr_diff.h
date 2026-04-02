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
 *   $added_path   - STRSXP(1): path to a temp .vtr file containing all rows
 *                   from B whose key was not in A (full row data, all columns).
 *                   The caller (R wrapper) is responsible for deleting this file.
 *   $deleted_keys - vector of key values present in A but not B
 *
 * Algorithm: two passes.
 *   Pass 1: stream A key column only -> build hash set of all A keys.
 *   Pass 2: stream ALL columns of B; for each row, if key not in A, write
 *           the row to the temp .vtr file directly. O(n_unique_keys) memory.
 */
SEXP C_diff_vtr(SEXP path_a, SEXP path_b, SEXP key_col);

#endif /* VECTRA_VTR_DIFF_H */
