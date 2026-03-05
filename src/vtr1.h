#ifndef VECTRA_VTR1_H
#define VECTRA_VTR1_H

#include "types.h"
#include <stdio.h>

/* File header info */
typedef struct {
    uint16_t  version;
    VecSchema schema;
    uint32_t  n_rowgroups;
} Vtr1Header;

/* Row group metadata (for seeking) */
typedef struct {
    int64_t file_offset;  /* byte offset where row group data starts */
    int64_t n_rows;
} Vtr1RowGroup;

/* File handle for reading */
typedef struct {
    FILE        *fp;
    Vtr1Header   header;
    Vtr1RowGroup *rowgroups;  /* array of n_rowgroups entries */
} Vtr1File;

/* Open a .vtr file for reading, parse header and row group index */
Vtr1File *vtr1_open(const char *path);

/* Read a specific row group, only loading selected columns.
   col_mask: bit array of length n_cols, 1 = load this column */
VecBatch *vtr1_read_rowgroup(Vtr1File *file, uint32_t rg_idx,
                             const int *col_mask);

/* Close and free */
void vtr1_close(Vtr1File *file);

/* Write a VecBatch to a new .vtr file (single row group) */
void vtr1_write(const char *path, const VecBatch *batch);

/* Write: append a row group to an open file, used for multi-rowgroup writes */
void vtr1_write_header(FILE *fp, const VecSchema *schema, uint32_t n_rowgroups);
void vtr1_write_rowgroup(FILE *fp, const VecBatch *batch);

#endif /* VECTRA_VTR1_H */
