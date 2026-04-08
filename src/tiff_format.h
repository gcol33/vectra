#ifndef VECTRA_TIFF_FORMAT_H
#define VECTRA_TIFF_FORMAT_H

#include <stdint.h>

/* ------------------------------------------------------------------ */
/*  Minimal GeoTIFF reader/writer for climate rasters.                  */
/*  Reads/writes strip-based GeoTIFF with optional DEFLATE compression. */
/*  Data model: pixels as (x, y, band1, band2, ...) rows.              */
/* ------------------------------------------------------------------ */

#define TIFF_MAX_BANDS 64

/* Pixel storage type for writer */
#define TIFF_PIXEL_FLOAT64  0
#define TIFF_PIXEL_FLOAT32  1
#define TIFF_PIXEL_INT16    2
#define TIFF_PIXEL_INT32    3
#define TIFF_PIXEL_UINT8    4
#define TIFF_PIXEL_UINT16   5

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

/* Extract band values at specific (x, y) coordinates.
   n_points: number of query points
   xs, ys: coordinate arrays (n_points each)
   out_bands[band]: output arrays (n_points each), caller-allocated
   Points outside the raster extent get NaN.
   Returns 0 on success, -1 on error. */
int tiff_reader_extract_points(TiffReader *r, int64_t n_points,
                                const double *xs, const double *ys,
                                double **out_bands);

/* GDAL_METADATA (tag 42112) XML string, or NULL if not present.
   Returned pointer is owned by the reader — do not free. */
const char *tiff_reader_metadata(TiffReader *r);

const char *tiff_reader_errmsg(TiffReader *r);
void tiff_reader_close(TiffReader *r);

/* ---- Writer ---- */

typedef struct TiffWriter TiffWriter;

/* Create a GeoTIFF writer.
   width, height: raster dimensions
   n_bands: samples per pixel
   gt: 6-element affine transform (NULL for default identity)
   nodata: nodata value (NaN = no GDAL_NODATA tag)
   use_deflate: 1 to DEFLATE-compress strips
   pixel_type: one of TIFF_PIXEL_* constants */
int tiff_writer_open(const char *path, TiffWriter **out,
                           int64_t width, int64_t height, int n_bands,
                           const double *gt, double nodata,
                           int use_deflate, int pixel_type);

/* Attach GDAL_METADATA XML to be written into tag 42112.
   Must be called before tiff_writer_finish(). */
void tiff_writer_set_metadata(TiffWriter *w, const char *xml);

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
