#ifndef VECTRA_TIFF_WRITE_H
#define VECTRA_TIFF_WRITE_H

#include "types.h"

/* Write a node's output to a GeoTIFF file.
   The node must have x, y columns and one or more band columns (double).
   Grid dimensions are inferred from x/y values.
   use_deflate: 1 to compress strips with DEFLATE. */
void tiff_write_node(VecNode *node, const char *path, int use_deflate);

/* Write with explicit pixel type (TIFF_PIXEL_*) and optional GDAL_METADATA.
   metadata_xml may be NULL. */
void tiff_write_node_typed(VecNode *node, const char *path, int use_deflate,
                           int pixel_type, const char *metadata_xml);

#endif /* VECTRA_TIFF_WRITE_H */
