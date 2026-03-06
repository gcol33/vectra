#ifndef VECTRA_SCAN_H
#define VECTRA_SCAN_H

#include "types.h"
#include "vtr1.h"

typedef struct VecExpr VecExpr;  /* forward decl */

typedef struct {
    VecNode    base;
    Vtr1File  *file;
    int       *col_mask;     /* which columns to read */
    uint32_t   next_rg;      /* next row group to read */
    VecExpr   *predicate;    /* pushed-down filter predicate (NULL = none) */
    int        pred_borrowed; /* 1 = don't free predicate (owned by filter node) */
} ScanNode;

/* Create a scan node over a .vtr file.
   col_indices: NULL = all columns, otherwise array of col indices to read
   n_selected: number of entries in col_indices (ignored if col_indices is NULL) */
ScanNode *scan_node_create(const char *path, int *col_indices, int n_selected);

#endif /* VECTRA_SCAN_H */
