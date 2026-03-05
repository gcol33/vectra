#ifndef VECTRA_CSV_SCAN_H
#define VECTRA_CSV_SCAN_H

#include "types.h"

typedef struct {
    VecNode    base;
    FILE      *fp;
    long       data_start;    /* file offset after header line */
    int        n_file_cols;   /* total columns in the CSV */
    VecType   *col_types;     /* inferred type per column */
    int64_t    batch_size;    /* rows per batch */
    int        exhausted;
} CsvScanNode;

/* Create a CSV scan node.
   path:       path to CSV file
   batch_size: rows per batch (default 65536) */
CsvScanNode *csv_scan_node_create(const char *path, int64_t batch_size);

#endif /* VECTRA_CSV_SCAN_H */
