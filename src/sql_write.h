#ifndef VECTRA_SQL_WRITE_H
#define VECTRA_SQL_WRITE_H

#include "types.h"

/* Write a node's output to a SQLite table.
   Creates the table if it doesn't exist. */
void sql_write_node(VecNode *node, const char *path, const char *table_name);

#endif /* VECTRA_SQL_WRITE_H */
