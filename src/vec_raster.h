#ifndef VECTRA_VEC_RASTER_H
#define VECTRA_VEC_RASTER_H

/*
 * vec_raster.h — VECR raster file format (Phase 1).
 *
 * Tile-per-row schema as described in raster-plan.md. Each tile is a
 * self-describing tdc_block_record encoded with PRED_2D + BYTE_SHUFFLE + LZ
 * (the chain is recorded in the block header so newer readers can decode
 * older files transparently). The on-disk file is:
 *
 *     [ VECR header (160 bytes) ]
 *     [ band names section      ]   nul-separated UTF-8, optional
 *     [ tdc block 1             ]   tile (level=L, band=B, ty=Y, tx=X)
 *     [ tdc block 2             ]
 *     ...
 *     [ tile index              ]   n_tiles_total fixed-size entries
 *
 * The index is contiguous at the file tail so a reader can mmap+parse it
 * with a single read. Each entry carries enough info (level/band/coords +
 * offset/size + min/max/n_valid) for spatial-statistical predicate
 * pushdown without decoding any pixels.
 *
 * Phase 1 supports a single overview level (n_levels=1). Higher levels
 * are reserved by the schema (level field exists) but are populated by
 * vec_build_overviews in Phase 3.
 */

#include <stdint.h>
#include <stddef.h>

#define VECR_MAGIC          0x52434556u   /* 'VECR' little-endian */
#define VECR_VERSION        1
#define VECR_HEADER_SIZE    160

/* Sample dtypes — share numeric values with tdc_dtype so the tdc bridge
 * is a memcpy. Listed redundantly here so callers don't have to pull in
 * tdc/types.h just to pick a dtype. */
#define VECR_DT_I8    1
#define VECR_DT_I16   2
#define VECR_DT_I32   3
#define VECR_DT_I64   4
#define VECR_DT_U8    5
#define VECR_DT_U16   6
#define VECR_DT_U32   7
#define VECR_DT_U64   8
#define VECR_DT_F32   9
#define VECR_DT_F64   10

/* Header flags */
#define VECR_FLAG_HAS_NODATA      0x0001u
#define VECR_FLAG_HAS_CRS         0x0002u
#define VECR_FLAG_HAS_BAND_NAMES  0x0004u

/* Tile layout (Phase 6b). Image-major is the default and matches every
 * file written before Phase 6b — the reserved byte that holds the layout
 * code was previously zero-filled, so old files read as VECR_LAYOUT_IMAGE
 * without a format-version bump.
 *
 *   IMAGE  one tile per (level, band, time, ty, tx). Optimal for "give
 *          me one full image at time T" queries.
 *   PIXEL  one tile per (level, band, ty, tx); each tile holds the full
 *          time stack for that spatial block, stored as
 *          [tw*th, n_time] row-major (the time axis is the inner dim so
 *          a single pixel's time series is one contiguous run). Optimal
 *          for "give me the time series at pixel (x, y)" queries.
 */
#define VECR_LAYOUT_IMAGE  0
#define VECR_LAYOUT_PIXEL  1

/* Compression knobs.
 *
 *   FAST     One spec, no probing. PRED_2D + ZIGZAG/BYTE_SHUFFLE + LZ.
 *            Matches Phase 1's behavior. Predictable encode cost.
 *   BALANCED Probe two entropy coders (LZ, LZ_SPLIT) and keep the smaller.
 *            ~2x slower encode than FAST; usually 5-15% smaller files on
 *            mixed-distribution rasters.
 *   MAX      Probe a six-way candidate set (predictor variants + entropy
 *            variants + RAW fallback). Slowest encode; smallest file.
 *            Decode cost is unchanged — every spec produces a self-
 *            describing block the existing reader already handles. */
#define VECR_COMPRESS_FAST     0
#define VECR_COMPRESS_BALANCED 1
#define VECR_COMPRESS_MAX      2

/* Tile index entry — exactly 64 bytes for cache-line alignment.
 *
 *     offset  size  field
 *     ------  ----  -----
 *          0     1  level
 *          1     1  _pad0
 *          2     2  band
 *          4     4  tile_x
 *          8     4  tile_y
 *         12     4  _pad1
 *         16     8  offset           file offset of tdc block record
 *         24     8  size             tdc block total bytes
 *         32     8  min              little-endian dtype-native bytes (zero if unset)
 *         40     8  max
 *         48     8  n_valid
 *         56     8  _pad2
 */
#define VECR_INDEX_ENTRY_SIZE 64

typedef struct {
    uint8_t  level;
    uint8_t  _pad0;
    uint16_t band;
    int32_t  tile_x;
    int32_t  tile_y;
    int32_t  _pad1;
    int64_t  offset;
    int64_t  size;
    uint64_t min_bits;     /* dtype-native min as raw bytes */
    uint64_t max_bits;     /* dtype-native max as raw bytes */
    int64_t  n_valid;
    int64_t  time;          /* epoch ms or step index; 0 = unspecified.
                             * V1 wrote zero in this slot (under the name
                             * _pad2); existing files therefore read as
                             * time=0 without a format version bump. */
} VecrIndexEntry;

/* On-disk header — 160 bytes total.
 *
 *     offset  size  field
 *     ------  ----  -----
 *          0     4  magic = VECR_MAGIC
 *          4     2  version
 *          6     2  flags
 *          8     8  width
 *         16     8  height
 *         24     4  n_bands
 *         28     2  tile_size
 *         30     1  sample_dtype     (one of VECR_DT_*)
 *         31     1  n_levels         (1 = no overviews)
 *         32    48  geotransform[6]  (doubles)
 *         80     8  nodata           (double; NaN if HAS_NODATA unset)
 *         88     4  epsg
 *         92     4  band_names_size
 *         96     8  index_offset
 *        104     8  index_size
 *        112     8  n_tiles_total
 *        120     1  layout           (Phase 6b; VECR_LAYOUT_IMAGE/PIXEL)
 *        121     3  _pad_layout
 *        124     4  n_time           (Phase 6b; 0 unless layout=PIXEL)
 *        128     8  times_offset     (Phase 6b; file offset of the times[]
 *                                     section when layout=PIXEL, else 0)
 *        136    24  _reserved
 */
typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t flags;
    int64_t  width;
    int64_t  height;
    int32_t  n_bands;
    uint16_t tile_size;
    uint8_t  sample_dtype;
    uint8_t  n_levels;
    double   geotransform[6];
    double   nodata;
    int32_t  epsg;
    uint32_t band_names_size;
    int64_t  index_offset;
    int64_t  index_size;
    int64_t  n_tiles_total;
    uint8_t  layout;             /* VECR_LAYOUT_IMAGE (0) or VECR_LAYOUT_PIXEL (1) */
    uint8_t  _pad_layout[3];
    uint32_t n_time;             /* >0 only for layout=PIXEL */
    int64_t  times_offset;       /* file offset of int64 times[] (PIXEL only) */
    uint8_t  _reserved[24];
} VecrHeader;

/* Returns the size in bytes of one sample of the given VECR_DT_* dtype.
 * 0 for unknown ids. */
size_t vecr_dtype_size(uint8_t dtype);

/* dtype string ("f32"/"i16"/...) <-> VECR_DT_* code. from_string returns 0
 * for an unrecognised name; to_string returns "unknown". */
uint8_t     vecr_dtype_from_string(const char *s);
const char *vecr_dtype_to_string(uint8_t dt);

/* Cast a row-major double buffer into the target dtype's buffer, and back.
 * On the doubles -> dtype path NaN/NA maps to 0 for integer dtypes (the
 * caller records a matching nodata value); on the reverse path the raw
 * sample is widened to double with NaN passthrough for float dtypes. `dst`
 * must hold `n` samples of the respective representation. */
void vecr_cast_doubles_to_dtype(const double *src, int64_t n,
                                uint8_t dt, void *dst);
void vecr_cast_dtype_to_doubles(const void *src, int64_t n,
                                uint8_t dt, double *dst);

/* ---------- Writer ------------------------------------------------------ */

typedef struct VecrWriter VecrWriter;

/* Open a writer.
 *
 *   path:        output file path
 *   width,height: raster dimensions in pixels
 *   n_bands:     number of bands (>= 1)
 *   tile_size:   square tile edge in pixels (default 512 if 0)
 *   sample_dtype: VECR_DT_* code (must match the sample buffers given
 *                 to vecr_writer_write_band)
 *   gt:          6-element affine transform; NULL = identity
 *   epsg:        EPSG code; 0 = no CRS recorded
 *   nodata:      NaN to skip recording a nodata value
 *   band_names:  array of n_bands C strings; NULL or any element NULL =
 *                no band names recorded
 *
 * Returns 0 on success, -1 on error. *out is set to a writer handle that
 * must be closed via vecr_writer_close even on error (so the error
 * message can be retrieved via vecr_writer_errmsg). */
int vecr_writer_open(const char *path,
                     int64_t width, int64_t height,
                     int n_bands, uint16_t tile_size,
                     uint8_t sample_dtype,
                     const double *gt,
                     int32_t epsg,
                     double nodata,
                     const char *const *band_names,
                     VecrWriter **out);

/* Set the compression level. Must be called between vecr_writer_open and
 * the first vecr_writer_write_band call. Default is VECR_COMPRESS_FAST.
 * Unknown values are silently clamped to FAST. */
void vecr_writer_set_compression(VecrWriter *w, int level);

/* The writer's sample dtype (VECR_DT_* code), as given to vecr_writer_open. */
uint8_t vecr_writer_dtype(VecrWriter *w);

/* ---------- Overviews --------------------------------------------------- */

/* Resampling kernels for vecr_build_overviews. Nearest = top-left of the
 * source 2x2 (fast, no smoothing). Average = mean of valid pixels (skips
 * nodata). Bilinear = 3x3 [1,2,1;2,4,2;1,2,1]/16 followed by 2x decimation.
 * Mode = most-frequent value over the 2x2 (categorical). Gauss = 5x5
 * separable [1,4,6,4,1]/16 then decimate; alias of Bilinear when no Gauss
 * implementation is built (Phase 3 ships Bilinear). */
#define VECR_RESAMPLE_NEAREST  0
#define VECR_RESAMPLE_AVERAGE  1
#define VECR_RESAMPLE_BILINEAR 2
#define VECR_RESAMPLE_MODE     3
#define VECR_RESAMPLE_GAUSS    4

/* Append n_levels-1 overview levels to an existing .vec raster (so the
 * file ends up holding levels 0..n_levels-1 inclusive). The caller must
 * have closed any open writer/reader on the file. compression is the
 * VECR_COMPRESS_* constant used for the new tiles; pass -1 to inherit
 * VECR_COMPRESS_FAST.
 *
 * Returns 0 on success. On failure the file may be left without an index
 * (corrupted) — callers that care about atomicity should write a temp
 * copy first. */
int vecr_build_overviews(const char *path,
                         int n_levels,
                         int resampling,
                         int compression,
                         char *errbuf, size_t errbuf_size);

/* Write a full band.
 *
 *   band_index:  0-based band index (must be < n_bands)
 *   pixels:      row-major buffer of width*height samples in sample_dtype
 *
 * The writer tiles the buffer internally. Each tile becomes one tdc block
 * appended to the file. Index entries are accumulated and flushed by
 * vecr_writer_finish.
 *
 * Returns 0 on success, -1 on error. */
int vecr_writer_write_band(VecrWriter *w,
                           int band_index,
                           const void *pixels);

/* Write a single tile-row: the `ty`-th row of tiles of `band_index` from a
 * row-major strip of `strip_h` x width samples in the file's sample dtype.
 * `strip_h` must equal the tile height for `ty` (tile_size, or the remainder
 * for the last row). Tiles are appended in (band, ty) call order but the
 * index records their grid position, so callers may stream tile-rows without
 * holding the whole band. Returns 0 on success, -1 on error. */
int vecr_writer_write_tile_row(VecrWriter *w,
                               int band_index, int64_t ty,
                               const void *strip_pixels, int64_t strip_h);

/* Set the time stamp recorded on every tile written by subsequent
 * vecr_writer_write_band calls. Pass 0 to unset. Used by time-cube
 * writers that emit multiple band-shaped slices into the same file. */
void vecr_writer_set_time(VecrWriter *w, int64_t time);

/* Finalize: write the index and patch the header. Must be called before
 * close. Returns 0 on success, -1 on error. */
int vecr_writer_finish(VecrWriter *w);

const char *vecr_writer_errmsg(VecrWriter *w);
void        vecr_writer_close(VecrWriter *w);

/* ---------- Reader ------------------------------------------------------ */

typedef struct VecrReader VecrReader;

int vecr_reader_open(const char *path, VecrReader **out);

int64_t       vecr_reader_width(VecrReader *r);
int64_t       vecr_reader_height(VecrReader *r);
int           vecr_reader_nbands(VecrReader *r);
uint16_t      vecr_reader_tile_size(VecrReader *r);
uint8_t       vecr_reader_dtype(VecrReader *r);
const double *vecr_reader_geotransform(VecrReader *r);
int32_t       vecr_reader_epsg(VecrReader *r);
double        vecr_reader_nodata(VecrReader *r);
int           vecr_reader_has_nodata(VecrReader *r);
int           vecr_reader_n_levels(VecrReader *r);
/* Returns the band-name string for `band` (NUL-terminated), or NULL if
 * the file did not record band names. Pointer is owned by the reader. */
const char   *vecr_reader_band_name(VecrReader *r, int band);

/* Read a (col_min, row_min)-(col_max, row_max) inclusive window of band b
 * at the requested overview level.
 *
 *   out:  pre-allocated buffer of (col_max-col_min+1) * (row_max-row_min+1)
 *         samples in the file's sample dtype, row-major
 *
 * Pixels outside the window's intersection with the raster extent are
 * filled with the nodata value (or NaN for float dtypes when no nodata
 * was set). Tiles fully outside the window are skipped without decoding.
 *
 * Returns 0 on success, -1 on error. */
int vecr_reader_read_window(VecrReader *r,
                            int band, uint8_t level,
                            int64_t col_min, int64_t row_min,
                            int64_t col_max, int64_t row_max,
                            void *out);

/* Read a window at a specific time stamp. The reader iterates the index
 * looking for tiles whose .time field equals `time`. Pass 0 for the
 * "untimed" default (matches files that never set a time). Returns -1 if
 * no tiles match. */
int vecr_reader_read_window_t(VecrReader *r,
                              int band, uint8_t level,
                              int64_t time,
                              int64_t col_min, int64_t row_min,
                              int64_t col_max, int64_t row_max,
                              void *out);

/* Returns 1 if the file has any tile with time != 0. */
int vecr_reader_has_time(VecrReader *r);

/* ---------- Pixel-major time cube (Phase 6b) ---------------------------- */

/* Returns the file's layout code (VECR_LAYOUT_IMAGE / VECR_LAYOUT_PIXEL). */
uint8_t vecr_reader_layout(VecrReader *r);

/* Returns the number of time steps stored in a pixel-major file (>0 only
 * when layout=VECR_LAYOUT_PIXEL). Image-major files return 0. */
uint32_t vecr_reader_n_time(VecrReader *r);

/* Returns a pointer to the int64 times[] table (length = n_time) for
 * pixel-major files; NULL for image-major. Pointer is owned by the
 * reader. */
const int64_t *vecr_reader_times(VecrReader *r);

/* Count distinct .time stamps in the index for the given (band, level).
 * Used by image-major callers to size the output of read_pixel_series.
 * Returns 0 if no tiles match. Each output slot in `out_times` (length
 * n_max) gets the next ascending stamp; pass NULL for `out_times` to
 * just count. */
int vecr_reader_distinct_times(VecrReader *r, int band, uint8_t level,
                               int64_t *out_times, int n_max);

/* Read the full time series at a single pixel (col, row) for one band at
 * the requested overview level. Output buffer must hold n_time samples
 * in the file's sample dtype.
 *
 *   - Layout=PIXEL: decodes the single tile that contains the pixel and
 *     copies n_time consecutive samples — the optimal path.
 *   - Layout=IMAGE: scans the index for tiles matching the band+level
 *     across all distinct time stamps, decodes one tile per time step,
 *     extracts the pixel — slow but correct fall-through.
 *
 * Pixels outside the raster extent get nodata-filled (NaN for floats).
 * Returns 0 on success, -1 on error. */
int vecr_reader_read_pixel_series(VecrReader *r,
                                  int64_t col, int64_t row,
                                  int band, uint8_t level,
                                  void *out);

/* Write a 4D time cube using the pixel-major layout. Same arguments as
 * vecr_writer_open + a times[] vector and the full (rows*cols*bands*time)
 * data array (band-major then row-major within each band, time outer).
 * The writer reorganises the data into one tile per spatial block holding
 * [tw*th, n_time] samples row-major.
 *
 *   path,width,height,n_bands,tile_size,sample_dtype,gt,epsg,nodata,
 *   band_names — same as vecr_writer_open.
 *   times    : int64[n_time] time stamps (caller-supplied; not interpreted
 *              other than 0 being remapped to 1 for parity with image-major).
 *   n_time   : number of time steps.
 *   data     : (n_bands * n_time * height * width) samples in sample_dtype,
 *              ordered [time][band][row][col] — i.e. the same memory
 *              layout the image-major time-cube writer expects.
 *   compression : VECR_COMPRESS_* code.
 *
 * Returns 0 on success, -1 on error. */
int vecr_write_pixel_cube(const char *path,
                          int64_t width, int64_t height,
                          int n_bands, uint16_t tile_size,
                          uint8_t sample_dtype,
                          const double *gt, int32_t epsg, double nodata,
                          const char *const *band_names,
                          const int64_t *times, int n_time,
                          const void *data,
                          int compression,
                          char *errbuf, size_t errbuf_size);

/* Extract values at n_points (xs, ys) from band b at level 0. Coordinates
 * are in CRS units (consumed via the geotransform). Output is doubles for
 * convenience (matching tiff_reader_extract_points). Points outside the
 * raster get NaN. Returns 0 on success. */
int vecr_reader_extract_points(VecrReader *r, int band,
                               int64_t n_points,
                               const double *xs, const double *ys,
                               double *out);

const char *vecr_reader_errmsg(VecrReader *r);
void        vecr_reader_close(VecrReader *r);

#endif /* VECTRA_VEC_RASTER_H */
