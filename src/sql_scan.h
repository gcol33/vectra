#ifndef VECTRA_SQL_SCAN_H
#define VECTRA_SQL_SCAN_H

#include "types.h"
#include "sqlite_format.h"

typedef struct {
    VecNode        base;
    SqlfmtReader  *reader;
    VecType       *col_types;
    int            n_cols;
    int64_t        batch_size;
    int            exhausted;
} SqlScanNode;

/* Create a SQL scan node.
   path:       path to SQLite database file
   table:      table name to scan
   batch_size: rows per batch (default 65536) */
SqlScanNode *sql_scan_node_create(const char *path, const char *table,
                                   int64_t batch_size);

#endif /* VECTRA_SQL_SCAN_H */
