#ifndef VECTRA_TIFF_SCAN_H
#define VECTRA_TIFF_SCAN_H

#include "types.h"
#include "tiff_format.h"

typedef struct {
    VecNode      base;
    TiffReader  *reader;
    int          n_bands;
    int64_t      batch_size;   /* rows of the raster per batch */
    int64_t      next_row;     /* next raster row to read */
    int          exhausted;
} TiffScanNode;

/* Create a TIFF scan node.
   path:       path to GeoTIFF file
   batch_size: raster rows per batch (default 256) */
TiffScanNode *tiff_scan_node_create(const char *path, int64_t batch_size);

#endif /* VECTRA_TIFF_SCAN_H */
