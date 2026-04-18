#ifndef VECTRA_SCAN_H
#define VECTRA_SCAN_H

#include "types.h"
#include "vtr1.h"           /* Vtr1ColStat, vec_type_is_int helpers reused */
#include "vtr1_tdc.h"
#include "vtr_delete.h"

typedef struct VecExpr VecExpr;  /* forward decl */

typedef struct VtrIndex VtrIndex;  /* forward decl */

typedef struct {
    VecNode        base;
    Vtr1TdcFile   *file;
    int           *col_mask;       /* which columns to read */
    uint32_t       next_rg;        /* next row group to read */
    uint32_t       last_rg;        /* exclusive upper bound (0 = use n_rowgroups) */
    int            rg_range_set;   /* 1 = binary search narrowed the range */
    VecExpr       *predicate;      /* pushed-down filter predicate (NULL = none) */
    int            pred_borrowed;  /* 1 = don't free predicate (owned by filter node) */
    TombstoneSet  *tombstone;      /* deleted rows (NULL = no deletions) */
    int64_t        rg_row_base;    /* physical row index of first row in current rg */
    VtrIndex      *index;          /* persistent hash index (NULL = none) */
    uint8_t       *rg_bitmap;      /* from hash index probe: 1 = visit this rg */
    char          *vtr_path;       /* path to .vtr file (for deferred index lookups) */
} ScanNode;

/* Create a scan node over a .vtr file.
   col_indices: NULL = all columns, otherwise array of col indices to read
   n_selected: number of entries in col_indices (ignored if col_indices is NULL) */
ScanNode *scan_node_create(const char *path, int *col_indices, int n_selected);

/* Parallel I/O eligibility: returns 1 if this is a plain scan with no
   predicate, tombstone, index bitmap, or binary-search range narrowing. */
int scan_node_is_parallel_safe(const VecNode *node);

/* Accessors for parallel I/O integration */
const char   *scan_node_get_path(const VecNode *node);
Vtr1TdcFile  *scan_node_get_file(const VecNode *node);
const int    *scan_node_get_col_mask(const VecNode *node);

#endif /* VECTRA_SCAN_H */
