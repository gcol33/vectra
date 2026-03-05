#ifndef VECTRA_CSV_WRITE_H
#define VECTRA_CSV_WRITE_H

#include "types.h"
#include <stdio.h>

/* Write a node's output to a CSV file, streaming batch by batch. */
void csv_write_node(VecNode *node, const char *path);

#endif /* VECTRA_CSV_WRITE_H */
