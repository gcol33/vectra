#ifndef VECTRA_CSV_SCAN_H
#define VECTRA_CSV_SCAN_H

#include "types.h"
#include "byte_reader.h"

typedef struct {
    VecNode     base;
    ByteReader *reader;
    int64_t     data_start;    /* byte offset after header line */
    int         n_file_cols;   /* total columns in the CSV */
    VecType    *col_types;     /* inferred type per column */
    int64_t     batch_size;    /* rows per batch */
    char        delim;         /* field separator byte */
    int         exhausted;
} CsvScanNode;

/* Create a CSV scan node.
   path:       path to CSV file
   batch_size: rows per batch (default 65536)
   delim:      field separator byte (e.g. ',' or '\t') */
CsvScanNode *csv_scan_node_create(const char *path, int64_t batch_size,
                                  char delim);

#endif /* VECTRA_CSV_SCAN_H */
