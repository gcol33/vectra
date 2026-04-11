#ifndef VECTRA_VTR1_H
#define VECTRA_VTR1_H

#include "types.h"
#include "vtr_codec.h"
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

/* Direct-write decoder API (sequential).
 *
 * Like vtr1_read_rowgroup, but per output column the caller can hand the
 * decoder a buffer to materialize into, eliminating an intermediate
 * malloc + memcpy. Indexed by *output* column position (post col_mask).
 *
 * Honored encodings: PLAIN+NONE+fixed and PLAIN+SHUFFLE_LZ+fixed only.
 * For every other encoding (DICT, DELTA, DIFF, QUANTIZE, SPATIAL, strings)
 * the decoder ignores the supplied buffer and allocates its own.
 *
 * After return, the caller MUST inspect arr->data_borrowed (NOT owns_data,
 * which is also 0 for string-arena borrows) to know which path was taken:
 *   data_borrowed == 1: decoder wrote into direct_bufs[out_col]; the caller
 *                       still needs to handle NA-patching from validity.
 *   data_borrowed == 0: decoder allocated its own buffer; caller must copy.
 *
 * Pass direct_bufs == NULL for behavior identical to vtr1_read_rowgroup. */
VecBatch *vtr1_read_rowgroup_ex(Vtr1File *file, uint32_t rg_idx,
                                const int *col_mask, void **direct_bufs);

/* Read all row groups in parallel using thread-local FILE handles.
   Returns malloc'd array of VecBatch* (caller frees each with vec_batch_free).
   path: needed to open per-thread file handles.
   out_count: set to number of batches returned. */
VecBatch **vtr1_read_parallel(Vtr1File *file, const int *col_mask,
                              const char *path, uint32_t *out_count);

/* Parallel direct-write reader.
 *
 * col_bases     [length n_out_cols]: base pointer of each output column's
 *                                    destination (NULL = no direct write,
 *                                    decoder mallocs).
 * col_elem_sizes[length n_out_cols]: element size in bytes for each column.
 *                                    Each thread computes its row-group
 *                                    slice as col_bases[i] + offset *
 *                                    col_elem_sizes[i].
 * n_out_cols                       : number of output (post-mask) columns.
 *
 * Same per-encoding rules as vtr1_read_rowgroup_ex: only PLAIN+fixed paths
 * honor the direct buffer. For unsupported encodings the returned VecArray
 * has data_borrowed == 0 and the caller must copy.
 *
 * IMPORTANT: col_bases entries must remain valid for the lifetime of the
 * call. When passing R vector storage (REAL/INTEGER), this means: NO call
 * into the R API may happen on any thread inside the parallel region —
 * the pointers would be invalidated by an R-side allocation. The decoder
 * itself never touches R. Callers must respect the same rule.
 *
 * Pass col_bases == NULL for behavior identical to vtr1_read_parallel. */
VecBatch **vtr1_read_parallel_into(Vtr1File *file, const int *col_mask,
                                   const char *path,
                                   void **col_bases,
                                   const size_t *col_elem_sizes,
                                   int n_out_cols,
                                   uint32_t *out_count);

/* Close and free */
void vtr1_close(Vtr1File *file);

/* Write a VecBatch to a new .vtr file (single row group) */
void vtr1_write(const char *path, const VecBatch *batch);

/* Write: append a row group to an open file, used for multi-rowgroup writes.
   comp_level: VTR_COMPRESS_NONE / VTR_COMPRESS_FAST */
void vtr1_write_header(FILE *fp, const VecSchema *schema, uint32_t n_rowgroups);
void vtr1_write_rowgroup(FILE *fp, const VecBatch *batch, int comp_level);

/* Write row group with per-column quantize specs.
   qspecs: array of n_cols entries (or NULL for no quantization).
   Columns with qspecs[c].enabled are quantized during encoding. */
void vtr1_write_rowgroup_q(FILE *fp, const VecBatch *batch, int comp_level,
                           const VtrQuantizeSpec *qspecs);

/* Write row group with per-column quantize + spatial specs.
   sspecs: array of n_cols entries (or NULL for no spatial encoding). */
void vtr1_write_rowgroup_qs(FILE *fp, const VecBatch *batch, int comp_level,
                            const VtrQuantizeSpec *qspecs,
                            const VtrSpatialSpec *sspecs);

#endif /* VECTRA_VTR1_H */
