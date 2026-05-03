#ifndef VECTRA_TIFF_WRITE_H
#define VECTRA_TIFF_WRITE_H

#include "types.h"

/* Write a node's output to a GeoTIFF file.
   The node must have x, y columns and one or more band columns (double).
   Grid dimensions are inferred from x/y values.
   use_deflate: 1 to compress strips with DEFLATE. */
void tiff_write_node(VecNode *node, const char *path, int use_deflate);

/* Write with explicit pixel type (TIFF_PIXEL_*) and optional GDAL_METADATA.
   metadata_xml may be NULL. epsg_geographic / epsg_projected are passed
   to tiff_writer_set_crs (pass 0 for either to omit). citation may be NULL.
   tile_width, tile_height: positive values produce a tiled TIFF (tags
   322/323/324/325). Zero/negative produces a strip TIFF.
   BigTIFF dispatch defaults to AUTO. */
void tiff_write_node_typed(VecNode *node, const char *path, int use_deflate,
                           int pixel_type, const char *metadata_xml,
                           int epsg_geographic, int epsg_projected,
                           const char *crs_citation,
                           int tile_width, int tile_height);

/* Like tiff_write_node_typed but with explicit BigTIFF dispatch
   (TIFF_BIGTIFF_AUTO / TIFF_BIGTIFF_OFF / TIFF_BIGTIFF_FORCE). */
void tiff_write_node_typed_ex(VecNode *node, const char *path, int use_deflate,
                              int pixel_type, const char *metadata_xml,
                              int epsg_geographic, int epsg_projected,
                              const char *crs_citation,
                              int tile_width, int tile_height,
                              int bigtiff_mode);

#endif /* VECTRA_TIFF_WRITE_H */
