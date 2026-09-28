#ifndef VECTRA_PARQUET_SCAN_H
#define VECTRA_PARQUET_SCAN_H

/*
 * Streaming Parquet scan node over one or more files.
 *
 * The files form one table: the first file's schema names the columns and
 * every later file must carry them with the same vectra types (its own
 * nullability and encodings may differ). Row groups are visited file by file;
 * each is read one column chunk per needed column and decoded page by page
 * into batches of at most batch_size rows, one OpenMP thread per column.
 *
 * The optimizer prunes the columns read (parquet_scan_prune) and attaches a
 * filter predicate (parquet_scan_predicate_slot), which skips row groups whose
 * footer statistics prove no row can match, via the same zone-map check the
 * .vtr scan uses.
 */

#include "types.h"

typedef struct VecExpr VecExpr;

VecNode *parquet_scan_node_create(int n_paths, const char **paths,
                                  int64_t batch_size, const char *list_sep);

/* Keep only the output columns flagged in needed[0..n_needed) (at least one
   column is always kept). */
void parquet_scan_prune(VecNode *node, const uint8_t *needed, int n_needed);

/* The scan's owned row-group pruning predicate, for the optimizer to AND a
   pruning copy into; NULL once the scan has started emitting. The scan frees
   whatever the slot holds. */
VecExpr **parquet_scan_predicate_slot(VecNode *node);

/* One-line description for explain(). */
int parquet_scan_describe(const VecNode *node, char *buf, int bufsize);

#endif /* VECTRA_PARQUET_SCAN_H */
