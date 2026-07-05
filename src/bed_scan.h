#ifndef VECTRA_BED_SCAN_H
#define VECTRA_BED_SCAN_H

#include "types.h"
#include "byte_reader.h"

/* Streaming BED (Browser Extensible Data) scan node.
   Feature lines become rows with the standard BED columns
   (chrom, start, end, name, score, strand, thickStart, thickEnd, itemRgb,
   blockCount, blockSizes, blockStarts; extra fields V13, V14, ...). The
   column count is fixed by the first data line; every later line must match.

   Coordinates are read faithfully: BED start is 0-based, end is half-open
   (exclusive). No coordinate is rewritten on read. One batch per batch_size
   features, so a genome-scale interval set never materializes. Gzip (.gz)
   input rides the shared byte_reader path. */
typedef struct {
    VecNode     base;
    ByteReader *reader;
    int         n_cols;          /* fixed by first data line, >= 3 */
    int64_t     batch_size;      /* features per batch */
    int         quiet;           /* suppress the end-of-scan record-count log */
    char       *path;            /* file path, for error/log messages */
    int64_t     records_emitted; /* cumulative, for the log */
    int         exhausted;
    int         logged;          /* end-of-scan log emitted once */
} BedScanNode;

/* Create a BED scan node.
   path:       path to a .bed file (optionally .gz)
   batch_size: features per batch (default 65536)
   quiet:      1 = suppress the end-of-scan record-count log */
BedScanNode *bed_scan_node_create(const char *path, int64_t batch_size,
                                  int quiet);

#endif /* VECTRA_BED_SCAN_H */
