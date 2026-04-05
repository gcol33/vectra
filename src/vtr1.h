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

/* Per-column per-rowgroup statistics (v3+) */
typedef struct {
    uint8_t has_stats;    /* 0 for string cols or no data */
    union {
        struct { int64_t min, max; } i64;
        struct { double min, max; } dbl;
        struct { uint8_t min, max; } bln;
    };
} Vtr1ColStat;

/* Row group metadata (for seeking) */
typedef struct {
    int64_t file_offset;  /* byte offset where row group data starts */
    int64_t n_rows;
    Vtr1ColStat *col_stats;  /* array of n_cols entries (NULL for v1/v2) */
} Vtr1RowGroup;

/* Reusable scratch buffer — grows but never shrinks */
typedef struct {
    uint8_t *data;
    size_t   capacity;
} Vtr1Scratch;

/* File handle for reading */
typedef struct {
    FILE        *fp;
    Vtr1Header   header;
    Vtr1RowGroup *rowgroups;  /* array of n_rowgroups entries */
    uint8_t     *col_sorted;  /* [n_cols] — 1 if row groups are sorted by this col */
    Vtr1Scratch  scratch_enc; /* reusable buffer for encoded data reads */
    Vtr1Scratch  scratch_dec; /* reusable buffer for decompression output */
} Vtr1File;

/* Open a .vtr file for reading, parse header and row group index */
Vtr1File *vtr1_open(const char *path);

/* Read a specific row group, only loading selected columns.
   col_mask: bit array of length n_cols, 1 = load this column */
VecBatch *vtr1_read_rowgroup(Vtr1File *file, uint32_t rg_idx,
                             const int *col_mask);

/* Read all row groups in parallel using thread-local FILE handles.
   Returns malloc'd array of VecBatch* (caller frees each with vec_batch_free).
   path: needed to open per-thread file handles.
   out_count: set to number of batches returned. */
VecBatch **vtr1_read_parallel(Vtr1File *file, const int *col_mask,
                              const char *path, uint32_t *out_count);

/* Close and free */
void vtr1_close(Vtr1File *file);

/* Write a VecBatch to a new .vtr file (single row group) */
void vtr1_write(const char *path, const VecBatch *batch);

/* Write: append a row group to an open file, used for multi-rowgroup writes */
void vtr1_write_header(FILE *fp, const VecSchema *schema, uint32_t n_rowgroups);
void vtr1_write_rowgroup(FILE *fp, const VecBatch *batch);

#endif /* VECTRA_VTR1_H */
