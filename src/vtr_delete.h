#ifndef VECTRA_VTR_DELETE_H
#define VECTRA_VTR_DELETE_H

#include <stdint.h>
#include <stddef.h>
#include <R.h>
#include <Rinternals.h>

/*
 * Tombstone file format (.del):
 *   Line-delimited list of 0-based physical row indices to exclude.
 *   Example:
 *     0
 *     5
 *     12
 *
 * The scan node checks for <path>.del on open; if it exists, deleted rows
 * are skipped when reading.
 *
 * A tombstone set is represented as a sorted array of int64_t row indices.
 */

typedef struct {
    int64_t *rows;   /* sorted 0-based physical row indices */
    int64_t  n;
} TombstoneSet;

/* Load a .del file into a TombstoneSet.  Returns NULL if the file doesn't
   exist or is empty (no deletion needed).  Caller must free with
   tombstone_free(). */
TombstoneSet *tombstone_load(const char *del_path);

/* Free a TombstoneSet */
void tombstone_free(TombstoneSet *ts);

/* Return 1 if physical row `row` is deleted according to ts, else 0.
   Safe to call with ts == NULL (always returns 0). */
int tombstone_is_deleted(const TombstoneSet *ts, int64_t row);

/*
 * Write (or merge into) a .del file.
 *
 * Appends the given 0-based row indices to the tombstone file, deduplicating
 * on write. If the file already exists, the new indices are merged with the
 * existing ones (union, deduplicated, sorted).
 *
 * del_path : path to the .del file (typically "<vtr_path>.del")
 * rows     : 0-based physical row indices to mark as deleted
 * n_rows   : number of entries in `rows`
 */
void tombstone_write(const char *del_path, const int64_t *rows, int64_t n_rows);

/* .Call entry points */
SEXP C_delete_vtr(SEXP path, SEXP row_indices);   /* row_indices: 0-based integer/double vector */

#endif /* VECTRA_VTR_DELETE_H */
