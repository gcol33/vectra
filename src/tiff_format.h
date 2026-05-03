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

/* Compression codes accepted by tiff_writer_open(). Internal to the writer
   API; the on-disk TIFF Compression tag (259) values are 1/8/5 respectively. */
#define TIFF_COMPRESS_NONE     0
#define TIFF_COMPRESS_DEFLATE  1
#define TIFF_COMPRESS_LZW      2

/* BigTIFF dispatch for the writer.
     AUTO  — pick automatically based on expected raw size (~3.9 GB cutoff).
     OFF   — always emit classic TIFF (will silently corrupt past 4 GB).
     FORCE — always emit BigTIFF (useful for round-trip tests on small data). */
#define TIFF_BIGTIFF_AUTO   0
#define TIFF_BIGTIFF_OFF    1
#define TIFF_BIGTIFF_FORCE  2

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

/* CRS metadata from the GeoKey directory (TIFF tags 34735/34736/34737).
   tiff_reader_epsg() returns the projected CRS EPSG (key 3072) if present,
   else the geographic CRS EPSG (key 2048), else 0 if no GeoKey directory.
   tiff_reader_crs_citation() returns the PCS / GT / Geog citation string
   (keys 3073 / 1026 / 2049, in that priority), or NULL if absent.
   Returned pointer is owned by the reader — do not free. */
int32_t     tiff_reader_epsg(TiffReader *r);
const char *tiff_reader_crs_citation(TiffReader *r);

const char *tiff_reader_errmsg(TiffReader *r);
void tiff_reader_close(TiffReader *r);

/* ---- Writer ---- */

typedef struct TiffWriter TiffWriter;

/* Create a GeoTIFF writer.
   width, height: raster dimensions
   n_bands: samples per pixel
   gt: 6-element affine transform (NULL for default identity)
   nodata: nodata value (NaN = no GDAL_NODATA tag)
   compression: one of TIFF_COMPRESS_* constants
   pixel_type: one of TIFF_PIXEL_* constants

   When compression is LZW the writer also applies horizontal differencing
   (Predictor 2) before encoding for integer pixel types and emits the
   Predictor tag (317 = 2) in the IFD. Float pixel types skip the predictor
   (Predictor = 1) because byte-wise subtraction of float bit patterns is
   not meaningful — that matches GDAL's behaviour.

   Auto-promotes to BigTIFF when the expected raw payload exceeds the
   classic-TIFF 4 GB ceiling (minus a small header budget). Use
   tiff_writer_open_ex to force a particular mode. */
int tiff_writer_open(const char *path, TiffWriter **out,
                           int64_t width, int64_t height, int n_bands,
                           const double *gt, double nodata,
                           int compression, int pixel_type);

/* Create a tiled GeoTIFF writer.
   compression: one of TIFF_COMPRESS_* constants (composes with tiling).
   tile_width, tile_height: positive multiples of 16 (TIFF spec).
     Pass 0 for both to use strip layout (equivalent to tiff_writer_open).
   Edge tiles at the right/bottom of the image are padded with NoData/NaN
   to full tile size, as required by the TIFF spec. */
int tiff_writer_open_tiled(const char *path, TiffWriter **out,
                           int64_t width, int64_t height, int n_bands,
                           const double *gt, double nodata,
                           int compression, int pixel_type,
                           int tile_width, int tile_height);

/* Like tiff_writer_open, but with explicit control over BigTIFF dispatch.
   compression: one of TIFF_COMPRESS_* constants.
   bigtiff_mode: TIFF_BIGTIFF_AUTO (default), TIFF_BIGTIFF_OFF, or
                 TIFF_BIGTIFF_FORCE. */
int tiff_writer_open_ex(const char *path, TiffWriter **out,
                        int64_t width, int64_t height, int n_bands,
                        const double *gt, double nodata,
                        int compression, int pixel_type,
                        int bigtiff_mode);

/* Attach GDAL_METADATA XML to be written into tag 42112.
   Must be called before tiff_writer_finish(). */
void tiff_writer_set_metadata(TiffWriter *w, const char *xml);

/* Embed CRS metadata as a GeoKey directory (TIFF tag 34735).
   Pass exactly one positive EPSG (the other should be 0):
     epsg_geographic > 0 → GeographicTypeGeoKey  (geographic CRS)
     epsg_projected  > 0 → ProjectedCSTypeGeoKey (projected CRS)
   When both are 0 the writer emits no GeoKey directory.
   citation may be NULL; when non-NULL it is written into GeoAsciiParams
   (tag 34737) and referenced by PCSCitationGeoKey or GeogCitationGeoKey.
   Must be called before tiff_writer_finish(). */
void tiff_writer_set_crs(TiffWriter *w, int32_t epsg_geographic,
                         int32_t epsg_projected, const char *citation);

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
