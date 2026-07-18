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
 *   mem      - REALSXP(1): memory budget in bytes (spill threshold for the
 *              external sorts), from vectra_mem()
 *
 * Returns a named list:
 *   $added_path   - STRSXP(1): path to a temp .vtr file containing all rows
 *                   from B whose key was not in A (full row data, all columns).
 *                   The caller (R wrapper) is responsible for deleting this file.
 *   $deleted_keys - vector of key values present in A but not B
 *
 * Algorithm: bounded sweep-merge. Both files are streamed through the external
 * sort (keyed by the key column) and merged in one forward pass, so no hash set
 * of A's keys is held resident. Peak state is the two sorts' own bounded spill
 * plus one output chunk of added rows plus the returned deleted-key vector.
 */
SEXP C_diff_vtr(SEXP path_a, SEXP path_b, SEXP key_col, SEXP mem);

#endif /* VECTRA_VTR_DIFF_H */
