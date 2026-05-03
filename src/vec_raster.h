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
    int64_t  _pad2;
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
 *        120    40  _reserved
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
    uint8_t  _reserved[40];
} VecrHeader;

/* Returns the size in bytes of one sample of the given VECR_DT_* dtype.
 * 0 for unknown ids. */
size_t vecr_dtype_size(uint8_t dtype);

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
