#ifndef VECTRA_TIFF_FORMAT_H
#define VECTRA_TIFF_FORMAT_H

#include <stdint.h>

/* ------------------------------------------------------------------ */
/*  Minimal GeoTIFF reader/writer for climate rasters.                  */
/*  Reads/writes strip-based GeoTIFF with optional DEFLATE compression. */
/*  Data model: pixels as (x, y, band1, band2, ...) rows.              */
/* ------------------------------------------------------------------ */

#define TIFF_MAX_BANDS 64

/* ---- Reader ---- */

typedef struct TiffReader TiffReader;

/* Open a GeoTIFF file. Returns 0 on success, -1 on error. */
int tiff_reader_open(const char *path, TiffReader **out);

/* Raster metadata */
int64_t tiff_reader_width(TiffReader *r);
int64_t tiff_reader_height(TiffReader *r);
int     tiff_reader_nbands(TiffReader *r);

/* Affine transform: x = gt[0] + (col+0.5)*gt[1], y = gt[3] + (row+0.5)*gt[5]
   (gt[2] and gt[4] are rotation, usually 0) */
const double *tiff_reader_geotransform(TiffReader *r);

/* NoData value (NaN if not set) */
double tiff_reader_nodata(TiffReader *r);
int    tiff_reader_has_nodata(TiffReader *r);

/* Read a block of rows [row_start, row_start+n_rows).
   out_x, out_y: coordinate arrays (n_pixels each)
   out_bands[band]: value arrays (n_pixels each)
   n_pixels = width * n_rows.
   Caller allocates all arrays. Returns 0 on success. */
int tiff_reader_read_rows(TiffReader *r, int64_t row_start, int64_t n_rows,
                           double *out_x, double *out_y,
                           double **out_bands);

const char *tiff_reader_errmsg(TiffReader *r);
void tiff_reader_close(TiffReader *r);

/* ---- Writer ---- */

typedef struct TiffWriter TiffWriter;

/* Create a GeoTIFF writer.
   width, height: raster dimensions
   n_bands: samples per pixel
   gt: 6-element affine transform (NULL for default identity)
   nodata: nodata value (NaN = no GDAL_NODATA tag)
   use_deflate: 1 to DEFLATE-compress strips */
int tiff_writer_open(const char *path, TiffWriter **out,
                     int64_t width, int64_t height, int n_bands,
                     const double *gt, double nodata, int use_deflate);

/* Write a block of rows [row_start, row_start+n_rows).
   bands[b] = array of width * n_rows doubles, row-major.
   NaN values are written as-is (mapped to nodata on read). */
int tiff_writer_write_rows(TiffWriter *w, int64_t row_start, int64_t n_rows,
                           const double *const *bands);

/* Finalize: writes IFD, patches header. Must be called before close. */
int tiff_writer_finish(TiffWriter *w);

const char *tiff_writer_errmsg(TiffWriter *w);
void tiff_writer_close(TiffWriter *w);

#endif /* VECTRA_TIFF_FORMAT_H */
