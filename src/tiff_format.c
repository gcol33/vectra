#include "tiff_format.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include "miniz/miniz.h"

/* ================================================================== */
/*  TIFF tag IDs and constants                                         */
/* ================================================================== */

#define TAG_IMAGE_WIDTH          256
#define TAG_IMAGE_LENGTH         257
#define TAG_BITS_PER_SAMPLE      258
#define TAG_COMPRESSION          259
#define TAG_PHOTOMETRIC          262
#define TAG_STRIP_OFFSETS        273
#define TAG_SAMPLES_PER_PIXEL    277
#define TAG_ROWS_PER_STRIP       278
#define TAG_STRIP_BYTE_COUNTS    279
#define TAG_PLANAR_CONFIG        284
#define TAG_TILE_WIDTH           322
#define TAG_TILE_LENGTH          323
#define TAG_TILE_OFFSETS         324
#define TAG_TILE_BYTE_COUNTS     325
#define TAG_SAMPLE_FORMAT        339
#define TAG_PREDICTOR            317
#define TAG_MODEL_TIEPOINT       33922
#define TAG_MODEL_PIXEL_SCALE    33550
#define TAG_GEO_KEY_DIRECTORY    34735
#define TAG_GEO_DOUBLE_PARAMS    34736
#define TAG_GEO_ASCII_PARAMS     34737
#define TAG_GDAL_METADATA        42112
#define TAG_GDAL_NODATA          42113

/* GeoKey IDs we care about (GeoTIFF spec section 6.2). */
#define GEO_KEY_GT_CITATION       1026  /* full WKT-ish citation */
#define GEO_KEY_GEOGRAPHIC_TYPE   2048  /* EPSG of geographic CRS */
#define GEO_KEY_GEOG_CITATION     2049
#define GEO_KEY_PROJECTED_CS_TYPE 3072  /* EPSG of projected CRS */
#define GEO_KEY_PCS_CITATION      3073

#define COMPRESS_NONE     1
#define COMPRESS_LZW      5
#define COMPRESS_DEFLATE  8
#define COMPRESS_ADOBE_DEFLATE 32946

#define SAMPLE_UINT     1
#define SAMPLE_INT      2
#define SAMPLE_FLOAT    3

/* TIFF data types */
#define TIFF_BYTE    1
#define TIFF_ASCII   2
#define TIFF_SHORT   3
#define TIFF_LONG    4
#define TIFF_RATIONAL 5
#define TIFF_SBYTE   6
#define TIFF_SSHORT  8
#define TIFF_SLONG   9
#define TIFF_FLOAT   11
#define TIFF_DOUBLE  12
#define TIFF_LONG8   16
#define TIFF_SLONG8  17

/* ================================================================== */
/*  Endian-aware reading                                               */
/* ================================================================== */

typedef struct {
    FILE    *fp;
    int      big_endian;
    int      bigtiff;      /* 0 = classic TIFF, 1 = BigTIFF */
} TiffIO;

static uint16_t tio_read16(TiffIO *io, const uint8_t *p) {
    if (io->big_endian) return (uint16_t)((p[0] << 8) | p[1]);
    return (uint16_t)((p[1] << 8) | p[0]);
}

static uint32_t tio_read32(TiffIO *io, const uint8_t *p) {
    if (io->big_endian)
        return ((uint32_t)p[0]<<24)|((uint32_t)p[1]<<16)|
               ((uint32_t)p[2]<<8)|(uint32_t)p[3];
    return ((uint32_t)p[3]<<24)|((uint32_t)p[2]<<16)|
           ((uint32_t)p[1]<<8)|(uint32_t)p[0];
}

static uint64_t tio_read64(TiffIO *io, const uint8_t *p) {
    if (io->big_endian) {
        return ((uint64_t)tio_read32(io, p) << 32) | tio_read32(io, p + 4);
    }
    return ((uint64_t)tio_read32(io, p + 4) << 32) | tio_read32(io, p);
}

static float tio_readf32(TiffIO *io, const uint8_t *p) {
    uint32_t bits = tio_read32(io, p);
    float f;
    memcpy(&f, &bits, 4);
    return f;
}

static double tio_readf64(TiffIO *io, const uint8_t *p) {
    uint64_t bits = tio_read64(io, p);
    double d;
    memcpy(&d, &bits, 8);
    return d;
}

static int tio_read_at(TiffIO *io, int64_t offset, void *buf, size_t n) {
    if (fseek(io->fp, (long)offset, SEEK_SET) != 0) return -1;
    if (fread(buf, 1, n, io->fp) != n) return -1;
    return 0;
}

/* ================================================================== */
/*  Tag value reader                                                   */
/* ================================================================== */

static int tiff_type_size(int dtype) {
    switch (dtype) {
    case TIFF_BYTE: case TIFF_SBYTE: case TIFF_ASCII: return 1;
    case TIFF_SHORT: case TIFF_SSHORT: return 2;
    case TIFF_LONG: case TIFF_SLONG: case TIFF_FLOAT: return 4;
    case TIFF_RATIONAL: case TIFF_DOUBLE: case TIFF_LONG8: case TIFF_SLONG8:
        return 8;
    default: return 1;
    }
}

static int64_t *read_tag_ints(TiffIO *io, int dtype, int64_t count,
                               const uint8_t *value_or_offset,
                               int entry_val_bytes) {
    int item_size = tiff_type_size(dtype);
    int64_t total = count * item_size;
    int64_t *out = (int64_t *)malloc((size_t)count * sizeof(int64_t));
    if (!out) return NULL;

    uint8_t *raw;
    uint8_t stack_buf[256];
    int need_free = 0;

    if (total <= entry_val_bytes) {
        raw = (uint8_t *)value_or_offset;
    } else {
        int64_t off;
        if (io->bigtiff)
            off = (int64_t)tio_read64(io, value_or_offset);
        else
            off = (int64_t)tio_read32(io, value_or_offset);

        if (total <= (int64_t)sizeof(stack_buf)) {
            raw = stack_buf;
        } else {
            raw = (uint8_t *)malloc((size_t)total);
            need_free = 1;
        }
        if (tio_read_at(io, off, raw, (size_t)total) != 0) {
            if (need_free) free(raw);
            free(out);
            return NULL;
        }
    }

    for (int64_t i = 0; i < count; i++) {
        const uint8_t *p = raw + i * item_size;
        switch (dtype) {
        case TIFF_BYTE:   out[i] = p[0]; break;
        case TIFF_SBYTE:  out[i] = (int8_t)p[0]; break;
        case TIFF_SHORT:  out[i] = tio_read16(io, p); break;
        case TIFF_SSHORT: out[i] = (int16_t)tio_read16(io, p); break;
        case TIFF_LONG:   out[i] = tio_read32(io, p); break;
        case TIFF_SLONG:  out[i] = (int32_t)tio_read32(io, p); break;
        case TIFF_LONG8:  out[i] = (int64_t)tio_read64(io, p); break;
        case TIFF_SLONG8: out[i] = (int64_t)tio_read64(io, p); break;
        default:          out[i] = 0; break;
        }
    }

    if (need_free) free(raw);
    return out;
}

static double *read_tag_doubles(TiffIO *io, int dtype, int64_t count,
                                 const uint8_t *value_or_offset,
                                 int entry_val_bytes) {
    int item_size = tiff_type_size(dtype);
    int64_t total = count * item_size;
    double *out = (double *)malloc((size_t)count * sizeof(double));
    if (!out) return NULL;

    uint8_t *raw;
    uint8_t stack_buf[256];
    int need_free = 0;

    if (total <= entry_val_bytes) {
        raw = (uint8_t *)value_or_offset;
    } else {
        int64_t off;
        if (io->bigtiff)
            off = (int64_t)tio_read64(io, value_or_offset);
        else
            off = (int64_t)tio_read32(io, value_or_offset);

        if (total <= (int64_t)sizeof(stack_buf)) {
            raw = stack_buf;
        } else {
            raw = (uint8_t *)malloc((size_t)total);
            need_free = 1;
        }
        if (tio_read_at(io, off, raw, (size_t)total) != 0) {
            if (need_free) free(raw);
            free(out);
            return NULL;
        }
    }

    for (int64_t i = 0; i < count; i++) {
        const uint8_t *p = raw + i * item_size;
        switch (dtype) {
        case TIFF_FLOAT:  out[i] = tio_readf32(io, p); break;
        case TIFF_DOUBLE: out[i] = tio_readf64(io, p); break;
        case TIFF_SHORT:  out[i] = (double)tio_read16(io, p); break;
        case TIFF_LONG:   out[i] = (double)tio_read32(io, p); break;
        case TIFF_LONG8:  out[i] = (double)tio_read64(io, p); break;
        default:          out[i] = 0.0; break;
        }
    }

    if (need_free) free(raw);
    return out;
}

static char *read_tag_ascii(TiffIO *io, int64_t count,
                             const uint8_t *value_or_offset,
                             int entry_val_bytes) {
    char *out = (char *)malloc((size_t)(count + 1));
    if (!out) return NULL;

    if (count <= entry_val_bytes) {
        memcpy(out, value_or_offset, (size_t)count);
    } else {
        int64_t off;
        if (io->bigtiff)
            off = (int64_t)tio_read64(io, value_or_offset);
        else
            off = (int64_t)tio_read32(io, value_or_offset);
        if (tio_read_at(io, off, out, (size_t)count) != 0) {
            free(out);
            return NULL;
        }
    }
    out[count] = '\0';
    return out;
}

/* ================================================================== */
/*  Reader struct                                                      */
/* ================================================================== */

struct TiffReader {
    TiffIO io;

    int64_t width;
    int64_t height;
    int     n_bands;
    int     bits_per_sample;
    int     sample_format;     /* SAMPLE_UINT, SAMPLE_INT, SAMPLE_FLOAT */
    int     compression;
    int     planar_config;     /* 1=chunky, 2=planar */

    /* Unified block layout: a "block" is one strip OR one tile.
       For strips, block_width == width and n_blocks_x == 1, so block_idx
       enumerates strips top-to-bottom; for tiles, blocks are arranged
       row-major (block_idx = by * n_blocks_x + bx).
       Strips may be shorter than block_height in the last block row;
       tiles are always exactly block_width * block_height in storage
       (image edges are padded). */
    int      is_tiled;
    int64_t  block_width;
    int64_t  block_height;
    int64_t  n_blocks_x;
    int64_t  n_blocks_y;
    int64_t *block_offsets;
    int64_t *block_byte_counts;
    int64_t  n_blocks;

    double gt[6];
    int    has_geotransform;

    double nodata;
    int    has_nodata;

    char  *metadata;   /* GDAL_METADATA (tag 42112), NULL if absent */

    int32_t epsg;          /* 0 if no GeoKey directory or no EPSG resolved */
    char   *crs_citation;  /* NULL if absent */

    char errmsg[256];
};

/* Resolve EPSG + citation from a captured GeoKey directory.
   dir is the raw SHORT array; ascii_params is the geo_ascii blob (may be NULL).
   See GeoTIFF spec section 7 for GeoKey directory layout:
     dir[0..3] = (Version, Major, Minor, NumberOfKeys)
     dir[4 + 4*i .. 4 + 4*i + 3] = (KeyID, TIFFTagLocation, Count, Value_Offset)
   For inline (TIFFTagLocation == 0), Value_Offset *is* the SHORT value.
   For TIFFTagLocation == 34737, Value_Offset is the byte offset into the
   geo_ascii_params blob and Count is the byte length. */
static void parse_geokeys(TiffReader *r,
                          const int64_t *dir, int64_t dir_count,
                          const char *ascii_params, int64_t ascii_count) {
    if (!dir || dir_count < 4) return;
    int64_t n_keys = dir[3];
    if (n_keys < 0 || dir_count < 4 + n_keys * 4) return;

    int32_t epsg_pcs = 0, epsg_geog = 0;
    int64_t cit_pcs_off  = -1, cit_pcs_count  = 0;
    int64_t cit_gt_off   = -1, cit_gt_count   = 0;
    int64_t cit_geog_off = -1, cit_geog_count = 0;

    for (int64_t i = 0; i < n_keys; i++) {
        int64_t key_id  = dir[4 + i * 4 + 0];
        int64_t tag_loc = dir[4 + i * 4 + 1];
        int64_t count   = dir[4 + i * 4 + 2];
        int64_t value   = dir[4 + i * 4 + 3];

        if (tag_loc == 0) {
            if (key_id == GEO_KEY_PROJECTED_CS_TYPE)      epsg_pcs  = (int32_t)value;
            else if (key_id == GEO_KEY_GEOGRAPHIC_TYPE)   epsg_geog = (int32_t)value;
        } else if (tag_loc == TAG_GEO_ASCII_PARAMS) {
            if (key_id == GEO_KEY_PCS_CITATION) {
                cit_pcs_off = value; cit_pcs_count = count;
            } else if (key_id == GEO_KEY_GT_CITATION) {
                cit_gt_off = value; cit_gt_count = count;
            } else if (key_id == GEO_KEY_GEOG_CITATION) {
                cit_geog_off = value; cit_geog_count = count;
            }
        }
    }

    /* Prefer projected EPSG; fall back to geographic. */
    if (epsg_pcs > 0)       r->epsg = epsg_pcs;
    else if (epsg_geog > 0) r->epsg = epsg_geog;

    /* Prefer PCS citation, then GT, then geographic. */
    int64_t cit_off = -1, cit_count = 0;
    if (cit_pcs_off >= 0) {
        cit_off = cit_pcs_off; cit_count = cit_pcs_count;
    } else if (cit_gt_off >= 0) {
        cit_off = cit_gt_off; cit_count = cit_gt_count;
    } else if (cit_geog_off >= 0) {
        cit_off = cit_geog_off; cit_count = cit_geog_count;
    }

    if (cit_off >= 0 && cit_count > 0 && ascii_params &&
        cit_off + cit_count <= ascii_count) {
        /* GeoAsciiParams strings are pipe-terminated; trim if present. */
        int64_t actual = cit_count;
        if (actual > 0 && ascii_params[cit_off + actual - 1] == '|') actual--;
        if (actual > 0) {
            r->crs_citation = (char *)malloc((size_t)actual + 1);
            if (r->crs_citation) {
                memcpy(r->crs_citation, ascii_params + cit_off, (size_t)actual);
                r->crs_citation[actual] = '\0';
            }
        }
    }
}

/* ================================================================== */
/*  IFD parsing                                                        */
/* ================================================================== */

static int parse_ifd(TiffReader *r) {
    TiffIO *io = &r->io;

    uint8_t hdr[16];
    if (tio_read_at(io, 0, hdr, io->bigtiff ? 16 : 8) != 0) {
        snprintf(r->errmsg, 256, "cannot read TIFF header");
        return -1;
    }

    int64_t ifd_offset;
    if (io->bigtiff) {
        ifd_offset = (int64_t)tio_read64(io, hdr + 8);
    } else {
        ifd_offset = (int64_t)tio_read32(io, hdr + 4);
    }

    uint8_t cnt_buf[8];
    if (tio_read_at(io, ifd_offset, cnt_buf, io->bigtiff ? 8 : 2) != 0) {
        snprintf(r->errmsg, 256, "cannot read IFD count");
        return -1;
    }

    int64_t n_entries;
    if (io->bigtiff)
        n_entries = (int64_t)tio_read64(io, cnt_buf);
    else
        n_entries = tio_read16(io, cnt_buf);

    int entry_size = io->bigtiff ? 20 : 12;
    int entry_val_bytes = io->bigtiff ? 8 : 4;
    int64_t entries_offset = ifd_offset + (io->bigtiff ? 8 : 2);

    /* Defaults */
    r->planar_config = 1;
    r->n_bands = 1;
    r->bits_per_sample = 8;
    r->sample_format = SAMPLE_UINT;
    r->compression = COMPRESS_NONE;

    /* Captured GeoKey directory + ascii params; resolved after the IFD loop
       so tag order doesn't matter. */
    int64_t *geokey_dir       = NULL;
    int64_t  geokey_dir_count = 0;
    char    *geo_ascii        = NULL;
    int64_t  geo_ascii_count  = 0;

    /* Capture strip and tile descriptors separately; reconcile after the
       loop into the unified block layout. Strip and tile tags are mutually
       exclusive in well-formed TIFFs, so picking the populated set works. */
    int64_t *strip_offsets     = NULL, *strip_byte_counts = NULL;
    int64_t  strip_offset_count = 0, strip_count_count = 0;
    int      rows_per_strip    = 0;
    int64_t *tile_offsets      = NULL, *tile_byte_counts  = NULL;
    int64_t  tile_offset_count  = 0, tile_count_count   = 0;
    int64_t  tile_width        = 0, tile_length         = 0;

    for (int64_t i = 0; i < n_entries; i++) {
        uint8_t entry[20];
        if (tio_read_at(io, entries_offset + i * entry_size,
                         entry, (size_t)entry_size) != 0)
            continue;

        uint16_t tag = tio_read16(io, entry);
        uint16_t dtype = tio_read16(io, entry + 2);
        int64_t count;
        const uint8_t *valp;

        if (io->bigtiff) {
            count = (int64_t)tio_read64(io, entry + 4);
            valp = entry + 12;
        } else {
            count = (int64_t)tio_read32(io, entry + 4);
            valp = entry + 8;
        }

        switch (tag) {
        case TAG_IMAGE_WIDTH: {
            int64_t *v = read_tag_ints(io, dtype, 1, valp, entry_val_bytes);
            if (v) { r->width = v[0]; free(v); }
            break;
        }
        case TAG_IMAGE_LENGTH: {
            int64_t *v = read_tag_ints(io, dtype, 1, valp, entry_val_bytes);
            if (v) { r->height = v[0]; free(v); }
            break;
        }
        case TAG_BITS_PER_SAMPLE: {
            int64_t *v = read_tag_ints(io, dtype, count, valp, entry_val_bytes);
            if (v) { r->bits_per_sample = (int)v[0]; free(v); }
            break;
        }
        case TAG_COMPRESSION: {
            int64_t *v = read_tag_ints(io, dtype, 1, valp, entry_val_bytes);
            if (v) { r->compression = (int)v[0]; free(v); }
            break;
        }
        case TAG_SAMPLES_PER_PIXEL: {
            int64_t *v = read_tag_ints(io, dtype, 1, valp, entry_val_bytes);
            if (v) { r->n_bands = (int)v[0]; free(v); }
            break;
        }
        case TAG_ROWS_PER_STRIP: {
            int64_t *v = read_tag_ints(io, dtype, 1, valp, entry_val_bytes);
            if (v) { rows_per_strip = (int)v[0]; free(v); }
            break;
        }
        case TAG_STRIP_OFFSETS: {
            free(strip_offsets);
            strip_offsets = read_tag_ints(io, dtype, count,
                                          valp, entry_val_bytes);
            strip_offset_count = count;
            break;
        }
        case TAG_STRIP_BYTE_COUNTS: {
            free(strip_byte_counts);
            strip_byte_counts = read_tag_ints(io, dtype, count,
                                              valp, entry_val_bytes);
            strip_count_count = count;
            break;
        }
        case TAG_TILE_WIDTH: {
            int64_t *v = read_tag_ints(io, dtype, 1, valp, entry_val_bytes);
            if (v) { tile_width = v[0]; free(v); }
            break;
        }
        case TAG_TILE_LENGTH: {
            int64_t *v = read_tag_ints(io, dtype, 1, valp, entry_val_bytes);
            if (v) { tile_length = v[0]; free(v); }
            break;
        }
        case TAG_TILE_OFFSETS: {
            free(tile_offsets);
            tile_offsets = read_tag_ints(io, dtype, count,
                                         valp, entry_val_bytes);
            tile_offset_count = count;
            break;
        }
        case TAG_TILE_BYTE_COUNTS: {
            free(tile_byte_counts);
            tile_byte_counts = read_tag_ints(io, dtype, count,
                                             valp, entry_val_bytes);
            tile_count_count = count;
            break;
        }
        case TAG_PLANAR_CONFIG: {
            int64_t *v = read_tag_ints(io, dtype, 1, valp, entry_val_bytes);
            if (v) { r->planar_config = (int)v[0]; free(v); }
            break;
        }
        case TAG_SAMPLE_FORMAT: {
            int64_t *v = read_tag_ints(io, dtype, count, valp, entry_val_bytes);
            if (v) { r->sample_format = (int)v[0]; free(v); }
            break;
        }
        case TAG_MODEL_PIXEL_SCALE: {
            double *v = read_tag_doubles(io, dtype, count,
                                          valp, entry_val_bytes);
            if (v && count >= 2) {
                r->gt[1] = v[0];       /* xres */
                r->gt[5] = -v[1];      /* -yres (north-up) */
                r->has_geotransform = 1;
            }
            free(v);
            break;
        }
        case TAG_MODEL_TIEPOINT: {
            double *v = read_tag_doubles(io, dtype, count,
                                          valp, entry_val_bytes);
            if (v && count >= 6) {
                r->gt[0] = v[3] - v[0] * r->gt[1];
                r->gt[3] = v[4] - v[1] * r->gt[5];
                r->has_geotransform = 1;
            }
            free(v);
            break;
        }
        case TAG_GEO_KEY_DIRECTORY: {
            free(geokey_dir);
            geokey_dir = read_tag_ints(io, dtype, count, valp, entry_val_bytes);
            geokey_dir_count = count;
            break;
        }
        case TAG_GEO_ASCII_PARAMS: {
            free(geo_ascii);
            geo_ascii = read_tag_ascii(io, count, valp, entry_val_bytes);
            geo_ascii_count = count;
            break;
        }
        case TAG_GDAL_METADATA: {
            r->metadata = read_tag_ascii(io, count, valp, entry_val_bytes);
            break;
        }
        case TAG_GDAL_NODATA: {
            char *s = read_tag_ascii(io, count, valp, entry_val_bytes);
            if (s) {
                char *end;
                double val = strtod(s, &end);
                if (end != s) {
                    r->nodata = val;
                    r->has_nodata = 1;
                }
                free(s);
            }
            break;
        }
        }
    }

    if (r->width <= 0 || r->height <= 0) {
        snprintf(r->errmsg, 256, "invalid dimensions: %lld x %lld",
                 (long long)r->width, (long long)r->height);
        goto fail;
    }
    if (r->compression != COMPRESS_NONE &&
        r->compression != COMPRESS_DEFLATE &&
        r->compression != COMPRESS_ADOBE_DEFLATE) {
        snprintf(r->errmsg, 256,
                 "unsupported compression: %d (only none/deflate supported)",
                 r->compression);
        goto fail;
    }
    if (r->n_bands > TIFF_MAX_BANDS) {
        snprintf(r->errmsg, 256, "too many bands: %d", r->n_bands);
        goto fail;
    }

    /* Reconcile strip vs tile layout into the unified block fields. */
    int have_tiles  = tile_offsets && tile_byte_counts &&
                      tile_width > 0 && tile_length > 0;
    int have_strips = strip_offsets && strip_byte_counts;

    if (have_tiles) {
        r->is_tiled = 1;
        r->block_width  = tile_width;
        r->block_height = tile_length;
        r->n_blocks_x   = (r->width  + tile_width  - 1) / tile_width;
        r->n_blocks_y   = (r->height + tile_length - 1) / tile_length;
        int64_t expect = r->n_blocks_x * r->n_blocks_y;
        if (tile_offset_count != expect || tile_count_count != expect) {
            snprintf(r->errmsg, 256,
                     "tile array length mismatch: got %lld offsets, %lld counts, "
                     "expected %lld",
                     (long long)tile_offset_count, (long long)tile_count_count,
                     (long long)expect);
            goto fail;
        }
        r->n_blocks         = expect;
        r->block_offsets    = tile_offsets;     tile_offsets = NULL;
        r->block_byte_counts = tile_byte_counts; tile_byte_counts = NULL;
    } else if (have_strips) {
        r->is_tiled = 0;
        r->block_width  = r->width;
        if (rows_per_strip <= 0) rows_per_strip = (int)r->height;
        r->block_height = rows_per_strip;
        r->n_blocks_x   = 1;
        r->n_blocks_y   = (r->height + rows_per_strip - 1) / rows_per_strip;
        if (strip_offset_count != r->n_blocks_y ||
            strip_count_count  != r->n_blocks_y) {
            snprintf(r->errmsg, 256,
                     "strip array length mismatch: got %lld offsets, %lld counts, "
                     "expected %lld",
                     (long long)strip_offset_count, (long long)strip_count_count,
                     (long long)r->n_blocks_y);
            goto fail;
        }
        r->n_blocks         = r->n_blocks_y;
        r->block_offsets    = strip_offsets;     strip_offsets = NULL;
        r->block_byte_counts = strip_byte_counts; strip_byte_counts = NULL;
    } else {
        snprintf(r->errmsg, 256,
                 "missing pixel offsets (no strip or tile tags found)");
        goto fail;
    }

    if (!r->has_geotransform) {
        r->gt[0] = 0.0; r->gt[1] = 1.0; r->gt[2] = 0.0;
        r->gt[3] = (double)r->height; r->gt[4] = 0.0; r->gt[5] = -1.0;
    }

    /* Resolve CRS metadata from the captured GeoKey directory. */
    parse_geokeys(r, geokey_dir, geokey_dir_count, geo_ascii, geo_ascii_count);
    free(geokey_dir);
    free(geo_ascii);
    free(strip_offsets);
    free(strip_byte_counts);
    free(tile_offsets);
    free(tile_byte_counts);

    return 0;

fail:
    free(geokey_dir);
    free(geo_ascii);
    free(strip_offsets);
    free(strip_byte_counts);
    free(tile_offsets);
    free(tile_byte_counts);
    return -1;
}

/* ================================================================== */
/*  Block decompression (uniform path for strips and tiles)            */
/* ================================================================== */

/* Number of stored pixel rows in this block. Tiles always store
   block_height rows (image edges padded). The last strip in a strip-based
   TIFF may be shorter than block_height if height is not a multiple. */
static int64_t block_stored_rows(TiffReader *r, int64_t block_idx) {
    if (r->is_tiled) return r->block_height;
    int64_t row_start = block_idx * r->block_height;
    int64_t rows = r->block_height;
    if (row_start + rows > r->height) rows = r->height - row_start;
    return rows;
}

static int64_t block_expected_bytes(TiffReader *r, int64_t block_idx) {
    int bps = r->bits_per_sample / 8;
    int64_t rows = block_stored_rows(r, block_idx);
    if (r->planar_config == 1)
        return rows * r->block_width * r->n_bands * bps;
    return rows * r->block_width * bps;
}

static uint8_t *read_block(TiffReader *r, int64_t block_idx,
                           int64_t *out_len) {
    int64_t offset = r->block_offsets[block_idx];
    int64_t compressed_len = r->block_byte_counts[block_idx];
    int64_t expected_bytes = block_expected_bytes(r, block_idx);

    if (r->compression == COMPRESS_NONE) {
        uint8_t *buf = (uint8_t *)malloc((size_t)compressed_len);
        if (!buf) return NULL;
        if (tio_read_at(&r->io, offset, buf, (size_t)compressed_len) != 0) {
            free(buf);
            return NULL;
        }
        *out_len = compressed_len;
        return buf;
    }

    /* DEFLATE */
    uint8_t *comp = (uint8_t *)malloc((size_t)compressed_len);
    if (!comp) return NULL;
    if (tio_read_at(&r->io, offset, comp, (size_t)compressed_len) != 0) {
        free(comp);
        return NULL;
    }

    uLong dest_len = (uLong)expected_bytes;
    uint8_t *decomp = (uint8_t *)malloc((size_t)expected_bytes);
    if (!decomp) { free(comp); return NULL; }

    int rc = uncompress(decomp, &dest_len, comp, (uLong)compressed_len);
    free(comp);

    if (rc != Z_OK) {
        free(decomp);
        return NULL;
    }

    *out_len = (int64_t)dest_len;
    return decomp;
}

/* ================================================================== */
/*  Pixel value extraction                                             */
/* ================================================================== */

static double extract_pixel(const uint8_t *raw, int64_t byte_offset,
                             int bits, int sample_format, TiffIO *io) {
    const uint8_t *p = raw + byte_offset;

    if (bits == 8) {
        if (sample_format == SAMPLE_INT) return (double)(int8_t)p[0];
        return (double)p[0];
    }
    if (bits == 16) {
        uint16_t v = io->big_endian ?
            (uint16_t)((p[0]<<8)|p[1]) : (uint16_t)((p[1]<<8)|p[0]);
        if (sample_format == SAMPLE_INT) return (double)(int16_t)v;
        return (double)v;
    }
    if (bits == 32) {
        if (sample_format == SAMPLE_FLOAT) return (double)tio_readf32(io, p);
        uint32_t v = tio_read32(io, p);
        if (sample_format == SAMPLE_INT) return (double)(int32_t)v;
        return (double)v;
    }
    if (bits == 64) {
        if (sample_format == SAMPLE_FLOAT) return (double)tio_readf64(io, p);
        uint64_t v = tio_read64(io, p);
        return (double)(int64_t)v;
    }
    return 0.0;
}

/* ================================================================== */
/*  Reader public API                                                  */
/* ================================================================== */

int tiff_reader_open(const char *path, TiffReader **out) {
    TiffReader *r = (TiffReader *)calloc(1, sizeof(TiffReader));
    if (!r) return -1;

    r->io.fp = fopen(path, "rb");
    if (!r->io.fp) {
        snprintf(r->errmsg, 256, "cannot open: %s", path);
        *out = r;
        return -1;
    }

    uint8_t bom[4];
    if (fread(bom, 1, 4, r->io.fp) != 4) {
        snprintf(r->errmsg, 256, "cannot read TIFF header");
        *out = r;
        return -1;
    }

    if (bom[0] == 'I' && bom[1] == 'I') r->io.big_endian = 0;
    else if (bom[0] == 'M' && bom[1] == 'M') r->io.big_endian = 1;
    else {
        snprintf(r->errmsg, 256, "not a TIFF file");
        *out = r;
        return -1;
    }

    uint16_t magic = tio_read16(&r->io, bom + 2);
    if (magic == 42) {
        r->io.bigtiff = 0;
    } else if (magic == 43) {
        r->io.bigtiff = 1;
    } else {
        snprintf(r->errmsg, 256, "bad TIFF magic: %d", magic);
        *out = r;
        return -1;
    }

    r->nodata = NAN;

    if (parse_ifd(r) != 0) {
        *out = r;
        return -1;
    }

    *out = r;
    return 0;
}

int64_t tiff_reader_width(TiffReader *r) { return r->width; }
int64_t tiff_reader_height(TiffReader *r) { return r->height; }
int tiff_reader_nbands(TiffReader *r) { return r->n_bands; }
const double *tiff_reader_geotransform(TiffReader *r) { return r->gt; }
double tiff_reader_nodata(TiffReader *r) { return r->nodata; }
int tiff_reader_has_nodata(TiffReader *r) { return r->has_nodata; }

const char *tiff_reader_metadata(TiffReader *r) { return r->metadata; }
int32_t     tiff_reader_epsg(TiffReader *r) { return r->epsg; }
const char *tiff_reader_crs_citation(TiffReader *r) { return r->crs_citation; }
const char *tiff_reader_errmsg(TiffReader *r) { return r->errmsg; }

int tiff_reader_read_rows(TiffReader *r, int64_t row_start, int64_t n_rows,
                           double *out_x, double *out_y,
                           double **out_bands) {
    int64_t W = r->width;
    int nb = r->n_bands;
    int bps = r->bits_per_sample;
    int bytes_per_sample = bps / 8;
    int pixel_bytes = bytes_per_sample * nb;

    for (int64_t row = 0; row < n_rows; row++) {
        double y = r->gt[3] + (row_start + row + 0.5) * r->gt[5];
        for (int64_t col = 0; col < W; col++) {
            int64_t idx = row * W + col;
            out_x[idx] = r->gt[0] + (col + 0.5) * r->gt[1];
            out_y[idx] = y;
        }
    }

    /* Iterate the block rows that intersect [row_start, row_start + n_rows).
       For strip-based TIFFs n_blocks_x == 1, so this collapses to the
       sequential strip walk; for tiled TIFFs we additionally iterate
       block columns left-to-right inside each block row. */
    int64_t bw = r->block_width;
    int64_t bh = r->block_height;
    int64_t row_end = row_start + n_rows;
    int64_t first_by = row_start / bh;
    int64_t last_by  = (row_end - 1) / bh;

    for (int64_t by = first_by; by <= last_by && by < r->n_blocks_y; by++) {
        int64_t blk_row0 = by * bh;
        int64_t stored_rows = block_stored_rows(r, by);

        for (int64_t bx = 0; bx < r->n_blocks_x; bx++) {
            int64_t blk_col0 = bx * bw;
            /* Only iterate the in-block columns that map into the image. */
            int64_t cols_in_block = bw;
            if (blk_col0 + cols_in_block > W) cols_in_block = W - blk_col0;

            int64_t block_idx = by * r->n_blocks_x + bx;
            int64_t actual_len = 0;
            uint8_t *blk = read_block(r, block_idx, &actual_len);
            if (!blk) {
                snprintf(r->errmsg, 256, "failed to read block %lld",
                         (long long)block_idx);
                return -1;
            }

            int64_t row_stride = (r->planar_config == 1)
                                  ? bw * pixel_bytes
                                  : bw * bytes_per_sample;

            for (int64_t in_row = 0; in_row < stored_rows; in_row++) {
                int64_t abs_row = blk_row0 + in_row;
                if (abs_row < row_start || abs_row >= row_end) continue;
                int64_t out_row = abs_row - row_start;

                for (int64_t in_col = 0; in_col < cols_in_block; in_col++) {
                    int64_t abs_col = blk_col0 + in_col;
                    int64_t out_idx = out_row * W + abs_col;

                    if (r->planar_config == 1) {
                        int64_t pixel_off = in_row * row_stride
                                          + in_col * pixel_bytes;
                        for (int b = 0; b < nb; b++) {
                            double val = extract_pixel(blk,
                                pixel_off + b * bytes_per_sample,
                                bps, r->sample_format, &r->io);
                            if (r->has_nodata && val == r->nodata) val = NAN;
                            out_bands[b][out_idx] = val;
                        }
                    } else {
                        /* Planar: simplified — only handles chunky well */
                        int64_t pixel_off = in_row * row_stride
                                          + in_col * bytes_per_sample;
                        for (int b = 0; b < nb; b++) {
                            double val = extract_pixel(blk, pixel_off,
                                bps, r->sample_format, &r->io);
                            if (r->has_nodata && val == r->nodata) val = NAN;
                            out_bands[b][out_idx] = val;
                        }
                    }
                }
            }

            free(blk);
        }
    }

    return 0;
}

/* ================================================================== */
/*  Point extraction: sample band values at (x,y) coordinates          */
/* ================================================================== */

/* Comparison for sorting point indices by block index */
typedef struct { int64_t idx; int64_t block; int64_t col; int64_t row; } PointLoc;

static int pointloc_cmp(const void *a, const void *b) {
    const PointLoc *pa = (const PointLoc *)a;
    const PointLoc *pb = (const PointLoc *)b;
    if (pa->block < pb->block) return -1;
    if (pa->block > pb->block) return  1;
    return 0;
}

int tiff_reader_extract_points(TiffReader *r, int64_t n_points,
                                const double *xs, const double *ys,
                                double **out_bands) {
    int64_t W = r->width;
    int64_t H = r->height;
    int nb = r->n_bands;
    int bps = r->bits_per_sample;
    int bytes_per_sample = bps / 8;
    int pixel_bytes = bytes_per_sample * nb;
    int64_t bw = r->block_width;
    int64_t bh = r->block_height;

    /* Precompute inverse affine geotransform.
       Forward:  x = gt[0] + (c+0.5)*gt[1] + (r+0.5)*gt[2]
                 y = gt[3] + (c+0.5)*gt[4] + (r+0.5)*gt[5]
       Inverse:  solve for (c+0.5, r+0.5) given (x, y).  */
    double det = r->gt[1] * r->gt[5] - r->gt[2] * r->gt[4];
    if (fabs(det) < 1e-30) {
        snprintf(r->errmsg, 256, "degenerate geotransform (det ~ 0)");
        return -1;
    }
    double inv_det = 1.0 / det;
    /* inv[0]*dx + inv[1]*dy = col+0.5,  inv[2]*dx + inv[3]*dy = row+0.5 */
    double inv0 =  r->gt[5] * inv_det;
    double inv1 = -r->gt[2] * inv_det;
    double inv2 = -r->gt[4] * inv_det;
    double inv3 =  r->gt[1] * inv_det;

    /* Invert geotransform for each point, build sorted work list */
    PointLoc *locs = (PointLoc *)malloc((size_t)n_points * sizeof(PointLoc));
    if (!locs) {
        snprintf(r->errmsg, 256, "alloc failed for point extraction");
        return -1;
    }

    /* Initialize all outputs to NaN (out-of-bounds default).
       Compiler auto-vectorizes this assignment loop. */
    for (int b = 0; b < nb; b++)
        for (int64_t i = 0; i < n_points; i++)
            out_bands[b][i] = NAN;

    int64_t n_valid = 0;
    for (int64_t i = 0; i < n_points; i++) {
        double dx = xs[i] - r->gt[0];
        double dy = ys[i] - r->gt[3];
        /* fc, fr are fractional pixel coordinates (col+0.5, row+0.5) */
        double fc = inv0 * dx + inv1 * dy - 0.5;
        double fr = inv2 * dx + inv3 * dy - 0.5;
        int64_t col = (int64_t)floor(fc + 0.5); /* nearest pixel */
        int64_t row = (int64_t)floor(fr + 0.5);

        if (col < 0 || col >= W || row < 0 || row >= H)
            continue; /* out of bounds — already NaN */

        int64_t bx = col / bw;
        int64_t by = row / bh;
        locs[n_valid].idx   = i;
        locs[n_valid].col   = col;
        locs[n_valid].row   = row;
        locs[n_valid].block = by * r->n_blocks_x + bx;
        n_valid++;
    }

    /* Sort by block for sequential I/O */
    if (n_valid > 1)
        qsort(locs, (size_t)n_valid, sizeof(PointLoc), pointloc_cmp);

    /* Process points block by block */
    int64_t pi = 0;
    while (pi < n_valid) {
        int64_t cur_block = locs[pi].block;
        int64_t bx = cur_block % r->n_blocks_x;
        int64_t by = cur_block / r->n_blocks_x;
        int64_t blk_col0 = bx * bw;
        int64_t blk_row0 = by * bh;

        int64_t actual_len = 0;
        uint8_t *blk = read_block(r, cur_block, &actual_len);
        if (!blk) {
            snprintf(r->errmsg, 256, "failed to read block %lld",
                     (long long)cur_block);
            free(locs);
            return -1;
        }

        int64_t row_stride = (r->planar_config == 1)
                              ? bw * pixel_bytes
                              : bw * bytes_per_sample;

        if (r->planar_config == 1) {
            while (pi < n_valid && locs[pi].block == cur_block) {
                int64_t oi = locs[pi].idx;
                int64_t in_col = locs[pi].col - blk_col0;
                int64_t in_row = locs[pi].row - blk_row0;
                int64_t pixel_off = in_row * row_stride
                                  + in_col * pixel_bytes;
                for (int b = 0; b < nb; b++) {
                    double val = extract_pixel(blk,
                        pixel_off + b * bytes_per_sample,
                        bps, r->sample_format, &r->io);
                    if (r->has_nodata && val == r->nodata) val = NAN;
                    out_bands[b][oi] = val;
                }
                pi++;
            }
        } else {
            while (pi < n_valid && locs[pi].block == cur_block) {
                int64_t oi = locs[pi].idx;
                int64_t in_col = locs[pi].col - blk_col0;
                int64_t in_row = locs[pi].row - blk_row0;
                int64_t pixel_off = in_row * row_stride
                                  + in_col * bytes_per_sample;
                for (int b = 0; b < nb; b++) {
                    double val = extract_pixel(blk, pixel_off,
                        bps, r->sample_format, &r->io);
                    if (r->has_nodata && val == r->nodata) val = NAN;
                    out_bands[b][oi] = val;
                }
                pi++;
            }
        }

        free(blk);
    }

    free(locs);
    return 0;
}

void tiff_reader_close(TiffReader *r) {
    if (!r) return;
    if (r->io.fp) fclose(r->io.fp);
    free(r->block_offsets);
    free(r->block_byte_counts);
    free(r->metadata);
    free(r->crs_citation);
    free(r);
}

/* ================================================================== */
/*  LZW encoder (TIFF flavour)                                         */
/* ================================================================== */
/*
 *  TIFF LZW differs from GIF/Unix-compress LZW in two ways:
 *    1. Bit packing is MSB-first (most-significant bit of each output byte
 *       is filled first); GIF is LSB-first.
 *    2. Code width grows one step early — a writer must increase the code
 *       width when the next code to be emitted is one less than 2^width
 *       (i.e. emit `nextcode` at the new width). Most LZW bugs in TIFF
 *       readers/writers come from getting that off-by-one wrong.
 *
 *  Codes:
 *    0..255  literal byte values
 *    256     ClearCode (reset dictionary, reset width to 9)
 *    257     EoiCode (end of stream)
 *    258..   string codes
 *
 *  Width starts at 9 bits and grows up to 12 bits (max code 4095). When
 *  the dictionary fills (next code would be 4094 with width still 12), we
 *  emit ClearCode and reset. We pre-emit ClearCode at the start of the
 *  stream as required by the TIFF spec.
 *
 *  Dictionary representation: a flat hash table keyed on (prefix_code,
 *  next_byte). Each non-empty slot stores its own (prefix, byte) so we
 *  can detect collisions; the value is the assigned code.
 */

#define TIFF_LZW_CLEAR_CODE   256
#define TIFF_LZW_EOI_CODE     257
#define TIFF_LZW_FIRST_CODE   258
#define TIFF_LZW_MAX_CODE     4094  /* TIFF spec: clear before code 4095 */
#define TIFF_LZW_HASH_SIZE    9001  /* prime, > 4096 */
#define TIFF_LZW_EMPTY        (-1)

typedef struct {
    int32_t prefix;   /* prefix code (-1 if slot empty) */
    int32_t byte;     /* next byte (0..255) */
    int32_t code;     /* assigned code */
} TiffLzwEntry;

typedef struct {
    uint8_t *out;
    size_t   cap;
    size_t   len;
    /* MSB-first bit accumulator */
    uint32_t accum;
    int      nbits_in_accum;
} TiffLzwBitWriter;

static int tiff_lzw_bw_grow(TiffLzwBitWriter *bw, size_t need) {
    if (bw->len + need <= bw->cap) return 0;
    size_t ncap = bw->cap ? bw->cap * 2 : 4096;
    while (ncap < bw->len + need) ncap *= 2;
    uint8_t *p = (uint8_t *)realloc(bw->out, ncap);
    if (!p) return -1;
    bw->out = p;
    bw->cap = ncap;
    return 0;
}

static int tiff_lzw_bw_put(TiffLzwBitWriter *bw, uint32_t code, int width) {
    /* Append `width` bits of `code` MSB-first. */
    bw->accum = (bw->accum << width) | (code & ((1u << width) - 1u));
    bw->nbits_in_accum += width;
    while (bw->nbits_in_accum >= 8) {
        if (tiff_lzw_bw_grow(bw, 1) != 0) return -1;
        int shift = bw->nbits_in_accum - 8;
        bw->out[bw->len++] = (uint8_t)((bw->accum >> shift) & 0xFFu);
        bw->nbits_in_accum -= 8;
        bw->accum &= (shift == 0) ? 0u : ((1u << bw->nbits_in_accum) - 1u);
    }
    return 0;
}

static int tiff_lzw_bw_flush(TiffLzwBitWriter *bw) {
    if (bw->nbits_in_accum > 0) {
        if (tiff_lzw_bw_grow(bw, 1) != 0) return -1;
        int shift = 8 - bw->nbits_in_accum;
        bw->out[bw->len++] = (uint8_t)((bw->accum << shift) & 0xFFu);
        bw->accum = 0;
        bw->nbits_in_accum = 0;
    }
    return 0;
}

/* Look up (prefix, byte) in the hash table; return code if present, else -1
 * and write the empty slot index to *out_slot for insertion. */
static int32_t tiff_lzw_lookup(const TiffLzwEntry *table, int32_t prefix,
                               int32_t byte, int *out_slot) {
    /* Mix prefix and byte; classic LZW hash. */
    uint32_t h = (uint32_t)((prefix << 8) ^ byte) % TIFF_LZW_HASH_SIZE;
    uint32_t step = 1u + (h % (TIFF_LZW_HASH_SIZE - 2));
    for (;;) {
        const TiffLzwEntry *e = &table[h];
        if (e->prefix == TIFF_LZW_EMPTY) {
            *out_slot = (int)h;
            return -1;
        }
        if (e->prefix == prefix && e->byte == byte) return e->code;
        h += step;
        if (h >= TIFF_LZW_HASH_SIZE) h -= TIFF_LZW_HASH_SIZE;
    }
}

static void tiff_lzw_reset(TiffLzwEntry *table, int32_t *next_code,
                           int *width) {
    for (int i = 0; i < TIFF_LZW_HASH_SIZE; i++) {
        table[i].prefix = TIFF_LZW_EMPTY;
        table[i].byte = 0;
        table[i].code = 0;
    }
    *next_code = TIFF_LZW_FIRST_CODE;
    *width = 9;
}

/* Encode `n` bytes into a fresh malloc'd output buffer.
 * Returns 0 on success; *out_buf and *out_len point at the encoded bytes.
 * Caller frees *out_buf. */
static int tiff_lzw_encode(const uint8_t *in, size_t n,
                           uint8_t **out_buf, size_t *out_len) {
    TiffLzwEntry *table =
        (TiffLzwEntry *)malloc(TIFF_LZW_HASH_SIZE * sizeof(TiffLzwEntry));
    if (!table) return -1;

    TiffLzwBitWriter bw;
    bw.out = NULL; bw.cap = 0; bw.len = 0;
    bw.accum = 0; bw.nbits_in_accum = 0;

    int32_t next_code;
    int     width;
    tiff_lzw_reset(table, &next_code, &width);

    /* Always start with ClearCode. */
    if (tiff_lzw_bw_put(&bw, TIFF_LZW_CLEAR_CODE, width) != 0) goto fail;

    if (n > 0) {
        int32_t prefix = in[0];
        for (size_t i = 1; i < n; i++) {
            int32_t b = in[i];
            int slot;
            int32_t found = tiff_lzw_lookup(table, prefix, b, &slot);
            if (found >= 0) {
                prefix = found;
            } else {
                if (tiff_lzw_bw_put(&bw, (uint32_t)prefix, width) != 0) goto fail;

                /* Insert (prefix, b) → next_code. */
                table[slot].prefix = prefix;
                table[slot].byte = b;
                table[slot].code = next_code;
                next_code++;

                /* Grow width when next_code no longer fits in the current
                 * width — i.e. after incrementing, next_code >= 1 << width.
                 * The TIFF spec's "EarlyChange" applies on the decoder
                 * side (which lags by one deferred entry); the libtiff
                 * encoder bumps exactly here, so we match. */
                if (next_code >= (1 << width) && width < 12) {
                    width++;
                }

                if (next_code >= TIFF_LZW_MAX_CODE) {
                    /* Dictionary full: emit ClearCode and reset. */
                    if (tiff_lzw_bw_put(&bw, TIFF_LZW_CLEAR_CODE, width) != 0)
                        goto fail;
                    tiff_lzw_reset(table, &next_code, &width);
                }

                prefix = b;
            }
        }
        /* Emit final prefix. */
        if (tiff_lzw_bw_put(&bw, (uint32_t)prefix, width) != 0) goto fail;
    }

    /* End of information. */
    if (tiff_lzw_bw_put(&bw, TIFF_LZW_EOI_CODE, width) != 0) goto fail;
    if (tiff_lzw_bw_flush(&bw) != 0) goto fail;

    free(table);
    *out_buf = bw.out;
    *out_len = bw.len;
    return 0;

fail:
    free(table);
    free(bw.out);
    return -1;
}

/* ================================================================== */
/*  Predictor 2 (horizontal differencing)                              */
/* ================================================================== */
/*
 *  For each scanline, replace each sample with the difference from the
 *  previous sample of the same band. Channels are interleaved chunky
 *  (sample0_band0, sample0_band1, ..., sample1_band0, ...), so the
 *  "previous sample of the same band" is `nbands` positions back, not 1.
 *
 *  Difference is computed in the integer width of the sample. Wraparound
 *  on overflow is intentional — the inverse predictor sums modulo 2^N and
 *  recovers the original exactly.
 */

static void tiff_predictor2_apply_row_u8(uint8_t *row, int64_t W, int nb) {
    for (int64_t col = W - 1; col >= 1; col--) {
        for (int b = 0; b < nb; b++) {
            row[col * nb + b] =
                (uint8_t)(row[col * nb + b] - row[(col - 1) * nb + b]);
        }
    }
}

static void tiff_predictor2_apply_row_u16(uint8_t *row, int64_t W, int nb) {
    /* Little-endian 16-bit samples in chunky layout. Operate on uint16_t
     * values via memcpy to stay alignment-safe on platforms that care. */
    int stride = nb * 2;
    for (int64_t col = W - 1; col >= 1; col--) {
        for (int b = 0; b < nb; b++) {
            uint16_t a, c;
            memcpy(&a, row + (col - 1) * stride + b * 2, 2);
            memcpy(&c, row + col       * stride + b * 2, 2);
            uint16_t d = (uint16_t)(c - a);
            memcpy(row + col * stride + b * 2, &d, 2);
        }
    }
}

static void tiff_predictor2_apply_row_u32(uint8_t *row, int64_t W, int nb) {
    int stride = nb * 4;
    for (int64_t col = W - 1; col >= 1; col--) {
        for (int b = 0; b < nb; b++) {
            uint32_t a, c;
            memcpy(&a, row + (col - 1) * stride + b * 4, 4);
            memcpy(&c, row + col       * stride + b * 4, 4);
            uint32_t d = c - a;
            memcpy(row + col * stride + b * 4, &d, 4);
        }
    }
}

/* Apply Predictor 2 to a chunky-interleaved buffer of `nrows` x `W` pixels
 * with `nb` bands. `bytes_per_sample` selects 1 / 2 / 4. */
static void tiff_predictor2_apply(uint8_t *buf, int64_t nrows, int64_t W,
                                  int nb, int bytes_per_sample) {
    int64_t row_bytes = W * nb * bytes_per_sample;
    for (int64_t r = 0; r < nrows; r++) {
        uint8_t *row = buf + r * row_bytes;
        switch (bytes_per_sample) {
        case 1: tiff_predictor2_apply_row_u8(row, W, nb); break;
        case 2: tiff_predictor2_apply_row_u16(row, W, nb); break;
        case 4: tiff_predictor2_apply_row_u32(row, W, nb); break;
        default: /* unsupported sample width — leave untouched */ break;
        }
    }
}

/* ================================================================== */
/*  Writer: little-endian classic TIFF with Float64 output             */
/* ================================================================== */

static void write_le16(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)(v & 0xFF);
    p[1] = (uint8_t)((v >> 8) & 0xFF);
}

static void write_le32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v & 0xFF);
    p[1] = (uint8_t)((v >> 8) & 0xFF);
    p[2] = (uint8_t)((v >> 16) & 0xFF);
    p[3] = (uint8_t)((v >> 24) & 0xFF);
}

static void write_le64(uint8_t *p, uint64_t v) {
    write_le32(p,     (uint32_t)(v & 0xFFFFFFFFu));
    write_le32(p + 4, (uint32_t)(v >> 32));
}

/* 64-bit-safe ftell: ftell()'s long return is 32-bit on Win32 / MinGW,
   which would silently truncate file positions past 2 GB and corrupt
   BigTIFF outputs. Use _ftelli64 on MSVC/MinGW, ftello elsewhere. */
static int64_t tiff_ftell64(FILE *fp) {
#if defined(_WIN32)
    return (int64_t)_ftelli64(fp);
#elif defined(_LARGEFILE_SOURCE) || defined(_LARGEFILE64_SOURCE) || \
      defined(__APPLE__) || defined(__linux__)
    return (int64_t)ftello(fp);
#else
    return (int64_t)ftell(fp);
#endif
}

static int tiff_fseek64(FILE *fp, int64_t offset, int whence) {
#if defined(_WIN32)
    return _fseeki64(fp, offset, whence);
#elif defined(_LARGEFILE_SOURCE) || defined(_LARGEFILE64_SOURCE) || \
      defined(__APPLE__) || defined(__linux__)
    return fseeko(fp, (off_t)offset, whence);
#else
    return fseek(fp, (long)offset, whence);
#endif
}

struct TiffWriter {
    FILE    *fp;
    int64_t  width;
    int64_t  height;
    int      n_bands;
    double   gt[6];
    double   nodata;
    int      has_nodata;
    int      compression;      /* TIFF tag 259 value: 1=NONE, 5=LZW, 8=DEFLATE */
    int      predictor;        /* TIFF tag 317 value: 1=none, 2=horizontal diff */
    int      bigtiff;          /* 0 = classic TIFF (32-bit offsets), 1 = BigTIFF */
    int      rows_per_strip;

    int      pixel_type;       /* TIFF_PIXEL_* enum */
    int      bytes_per_sample; /* derived: 1, 2, 4, or 8 */
    int      bits_per_sample;  /* derived: 8, 16, 32, or 64 */
    int      sample_format;    /* derived: SAMPLE_UINT/INT/FLOAT */

    /* Strip mode (is_tiled == 0) */
    int64_t  n_strips;
    uint64_t *strip_offsets;       /* 64-bit so the same array works for
                                       both classic and BigTIFF */
    uint64_t *strip_byte_counts;
    int64_t   strips_written;

    /* Tiled mode (is_tiled == 1): tags 322/323/324/325 instead of 273/278/279.
       Edge tiles are padded with NoData/NaN to tile_width * tile_height. */
    int       is_tiled;
    int       tile_width;
    int       tile_height;
    int64_t   n_tiles_x;       /* ceil(width  / tile_width)  */
    int64_t   n_tiles_y;       /* ceil(height / tile_height) */
    int64_t   n_tiles;         /* n_tiles_x * n_tiles_y      */
    uint64_t *tile_offsets;
    uint64_t *tile_byte_counts;

    char    *metadata;         /* GDAL_METADATA XML (tag 42112), or NULL */

    /* CRS to embed via GeoKey directory. Caller picks one of:
         epsg_geographic > 0   → GeographicTypeGeoKey (2048)
         epsg_projected  > 0   → ProjectedCSTypeGeoKey (3072)
       If both are zero, no GeoKey directory is written. */
    int32_t  epsg_geographic;
    int32_t  epsg_projected;
    char    *crs_citation;     /* optional GeoAsciiParams citation */

    char errmsg[256];
};

/* Compute pixel type properties */
static void pixel_type_props(int pixel_type,
                              int *bytes_out, int *bits_out, int *fmt_out) {
    switch (pixel_type) {
    case TIFF_PIXEL_FLOAT32: *bytes_out = 4; *bits_out = 32; *fmt_out = SAMPLE_FLOAT; return;
    case TIFF_PIXEL_INT16:   *bytes_out = 2; *bits_out = 16; *fmt_out = SAMPLE_INT;   return;
    case TIFF_PIXEL_INT32:   *bytes_out = 4; *bits_out = 32; *fmt_out = SAMPLE_INT;   return;
    case TIFF_PIXEL_UINT8:   *bytes_out = 1; *bits_out = 8;  *fmt_out = SAMPLE_UINT;  return;
    case TIFF_PIXEL_UINT16:  *bytes_out = 2; *bits_out = 16; *fmt_out = SAMPLE_UINT;  return;
    default: /* TIFF_PIXEL_FLOAT64 */
        *bytes_out = 8; *bits_out = 64; *fmt_out = SAMPLE_FLOAT; return;
    }
}

/* Estimate the raw (uncompressed) raster payload to decide whether
   classic TIFF's 4 GB ceiling will be exceeded. We add a generous
   header budget so the auto-promote threshold leaves room for the
   IFD, GeoKey blocks, metadata strings, etc. */
#define TIFF_CLASSIC_BUDGET_BYTES ((int64_t)(4LL * 1024 * 1024 * 1024 \
                                              - 64 * 1024 * 1024))

static int tiff_should_use_bigtiff(int64_t width, int64_t height, int n_bands,
                                   int bytes_per_sample) {
    /* Multiply with 64-bit math to avoid 32-bit overflow on the estimate
       itself. If anything overflows even uint64_t, treat as needs-bigtiff. */
    uint64_t pixels = (uint64_t)width * (uint64_t)height;
    uint64_t samples = pixels * (uint64_t)n_bands;
    uint64_t bytes = samples * (uint64_t)bytes_per_sample;
    return bytes > (uint64_t)TIFF_CLASSIC_BUDGET_BYTES;
}

/* Internal: shared open for strip/tiled and classic/BigTIFF modes.
   compression: one of TIFF_COMPRESS_* constants.
   tile_w/tile_h > 0 selects tiled layout; 0 selects strip layout.
   bigtiff_mode: AUTO/OFF/FORCE per TIFF_BIGTIFF_*. */
static int tiff_writer_open_impl(const char *path, TiffWriter **out,
                                 int64_t width, int64_t height, int n_bands,
                                 const double *gt, double nodata,
                                 int compression, int pixel_type,
                                 int tile_w, int tile_h,
                                 int bigtiff_mode) {
    TiffWriter *w = (TiffWriter *)calloc(1, sizeof(TiffWriter));
    if (!w) return -1;

    w->fp = fopen(path, "wb");
    if (!w->fp) {
        snprintf(w->errmsg, 256, "cannot open: %s", path);
        *out = w;
        return -1;
    }

    w->width = width;
    w->height = height;
    w->n_bands = n_bands;
    /* Translate writer-API compression code to the on-disk TIFF tag value. */
    switch (compression) {
    case TIFF_COMPRESS_NONE:    w->compression = COMPRESS_NONE;    break;
    case TIFF_COMPRESS_DEFLATE: w->compression = COMPRESS_DEFLATE; break;
    case TIFF_COMPRESS_LZW:     w->compression = COMPRESS_LZW;     break;
    default:                    w->compression = COMPRESS_NONE;    break;
    }

    w->pixel_type = pixel_type;
    pixel_type_props(pixel_type, &w->bytes_per_sample,
                     &w->bits_per_sample, &w->sample_format);

    /* Predictor 2 (horizontal differencing) is only well-defined for
     * integer sample formats. Float types skip the predictor — same as
     * GDAL's default for LZW + float32 / float64. */
    if (w->compression == COMPRESS_LZW &&
        w->sample_format != SAMPLE_FLOAT &&
        (w->bytes_per_sample == 1 || w->bytes_per_sample == 2 ||
         w->bytes_per_sample == 4)) {
        w->predictor = 2;
    } else {
        w->predictor = 1;
    }

    /* Resolve bigtiff mode */
    if (bigtiff_mode == TIFF_BIGTIFF_FORCE) {
        w->bigtiff = 1;
    } else if (bigtiff_mode == TIFF_BIGTIFF_OFF) {
        w->bigtiff = 0;
    } else { /* AUTO */
        w->bigtiff = tiff_should_use_bigtiff(width, height, n_bands,
                                              w->bytes_per_sample);
    }

    if (gt) {
        memcpy(w->gt, gt, 6 * sizeof(double));
    } else {
        w->gt[0] = 0; w->gt[1] = 1; w->gt[2] = 0;
        w->gt[3] = (double)height; w->gt[4] = 0; w->gt[5] = -1;
    }

    w->has_nodata = !isnan(nodata);
    w->nodata = nodata;

    if (tile_w > 0 && tile_h > 0) {
        /* Tiled + BigTIFF would need 8-byte tile offset arrays (LONG8) —
           not yet implemented. Refuse the combination explicitly so callers
           don't get silent truncation past 4 GB. */
        if (w->bigtiff) {
            snprintf(w->errmsg, 256,
                     "tiled BigTIFF not yet supported");
            *out = w;
            return -1;
        }
        /* TIFF spec requires tile dimensions to be multiples of 16. */
        if ((tile_w % 16) != 0 || (tile_h % 16) != 0) {
            snprintf(w->errmsg, 256,
                     "tile dimensions must be multiples of 16, got %dx%d",
                     tile_w, tile_h);
            *out = w;
            return -1;
        }
        w->is_tiled = 1;
        w->tile_width  = tile_w;
        w->tile_height = tile_h;
        w->n_tiles_x = (width  + tile_w - 1) / tile_w;
        w->n_tiles_y = (height + tile_h - 1) / tile_h;
        w->n_tiles   = w->n_tiles_x * w->n_tiles_y;
        if (w->n_tiles <= 0) {
            snprintf(w->errmsg, 256, "invalid tiled raster dimensions");
            *out = w;
            return -1;
        }
        w->tile_offsets = (uint64_t *)calloc((size_t)w->n_tiles,
                                              sizeof(uint64_t));
        w->tile_byte_counts = (uint64_t *)calloc((size_t)w->n_tiles,
                                                  sizeof(uint64_t));
        if (!w->tile_offsets || !w->tile_byte_counts) {
            snprintf(w->errmsg, 256, "alloc failed for tile arrays");
            *out = w;
            return -1;
        }
    } else {
        w->is_tiled = 0;
        w->rows_per_strip = 256;
        if (w->rows_per_strip > height) w->rows_per_strip = (int)height;
        w->n_strips = (height + w->rows_per_strip - 1) / w->rows_per_strip;
        w->strip_offsets = (uint64_t *)calloc((size_t)w->n_strips,
                                               sizeof(uint64_t));
        w->strip_byte_counts = (uint64_t *)calloc((size_t)w->n_strips,
                                                   sizeof(uint64_t));
        w->strips_written = 0;
    }

    if (w->bigtiff) {
        /* BigTIFF header (16 bytes):
             [0..1]  byte order: 'II' (little-endian)
             [2..3]  version: 0x002B = 43
             [4..5]  offset size: 8 (LONG8 width)
             [6..7]  always 0
             [8..15] offset of first IFD (patched on finish)
        */
        uint8_t hdr[16] = {0};
        hdr[0] = 'I'; hdr[1] = 'I';
        write_le16(hdr + 2, 43);     /* BigTIFF magic */
        write_le16(hdr + 4, 8);      /* offset bytesize */
        write_le16(hdr + 6, 0);      /* always zero */
        /* hdr[8..15] = first-IFD-offset, zero for now */
        fwrite(hdr, 1, 16, w->fp);
    } else {
        /* Classic TIFF: little-endian, magic=42, IFD offset=0 (patched later) */
        uint8_t hdr[8] = {'I', 'I', 42, 0, 0, 0, 0, 0};
        fwrite(hdr, 1, 8, w->fp);
    }

    *out = w;
    return 0;
}

int tiff_writer_open(const char *path, TiffWriter **out,
                           int64_t width, int64_t height, int n_bands,
                           const double *gt, double nodata,
                           int compression, int pixel_type) {
    return tiff_writer_open_impl(path, out, width, height, n_bands, gt,
                                 nodata, compression, pixel_type, 0, 0,
                                 TIFF_BIGTIFF_AUTO);
}

int tiff_writer_open_ex(const char *path, TiffWriter **out,
                        int64_t width, int64_t height, int n_bands,
                        const double *gt, double nodata,
                        int compression, int pixel_type,
                        int bigtiff_mode) {
    return tiff_writer_open_impl(path, out, width, height, n_bands, gt,
                                 nodata, compression, pixel_type, 0, 0,
                                 bigtiff_mode);
}

int tiff_writer_open_tiled(const char *path, TiffWriter **out,
                           int64_t width, int64_t height, int n_bands,
                           const double *gt, double nodata,
                           int compression, int pixel_type,
                           int tile_width, int tile_height) {
    return tiff_writer_open_impl(path, out, width, height, n_bands, gt,
                                 nodata, compression, pixel_type,
                                 tile_width, tile_height,
                                 TIFF_BIGTIFF_AUTO);
}

void tiff_writer_set_metadata(TiffWriter *w, const char *xml) {
    free(w->metadata);
    w->metadata = xml ? strdup(xml) : NULL;
}

void tiff_writer_set_crs(TiffWriter *w, int32_t epsg_geographic,
                         int32_t epsg_projected, const char *citation) {
    w->epsg_geographic = epsg_geographic > 0 ? epsg_geographic : 0;
    w->epsg_projected  = epsg_projected  > 0 ? epsg_projected  : 0;
    free(w->crs_citation);
    w->crs_citation = citation ? strdup(citation) : NULL;
}

/* Write a single pixel sample to raw buffer at given offset */
static void write_pixel(uint8_t *raw, int64_t off, double val,
                        TiffWriter *w) {
    int pt = w->pixel_type;

    /* Handle NaN → nodata substitution for integer types */
    if (isnan(val) && pt != TIFF_PIXEL_FLOAT64 && pt != TIFF_PIXEL_FLOAT32) {
        val = w->nodata;
    }

    switch (pt) {
    case TIFF_PIXEL_FLOAT32: {
        float f = (float)val;
        memcpy(raw + off, &f, 4);
        break;
    }
    case TIFF_PIXEL_INT16: {
        double clamped = val < -32768.0 ? -32768.0 :
                         val > 32767.0  ? 32767.0  : val;
        int16_t iv = (int16_t)lrint(clamped);
        write_le16(raw + off, (uint16_t)iv);
        break;
    }
    case TIFF_PIXEL_INT32: {
        double clamped = val < -2147483648.0 ? -2147483648.0 :
                         val > 2147483647.0  ? 2147483647.0  : val;
        int32_t iv = (int32_t)lrint(clamped);
        write_le32(raw + off, (uint32_t)iv);
        break;
    }
    case TIFF_PIXEL_UINT8: {
        double clamped = val < 0.0 ? 0.0 : val > 255.0 ? 255.0 : val;
        raw[off] = (uint8_t)lrint(clamped);
        break;
    }
    case TIFF_PIXEL_UINT16: {
        double clamped = val < 0.0 ? 0.0 : val > 65535.0 ? 65535.0 : val;
        write_le16(raw + off, (uint16_t)lrint(clamped));
        break;
    }
    default: /* TIFF_PIXEL_FLOAT64 */
        memcpy(raw + off, &val, 8);
        break;
    }
}

/* Fill n samples with the nodata/NaN pattern */
static void fill_nodata(uint8_t *raw, int64_t n_samples, TiffWriter *w) {
    int bps = w->bytes_per_sample;

    if (w->pixel_type == TIFF_PIXEL_FLOAT64) {
        double nan_val = NAN;
        for (int64_t i = 0; i < n_samples; i++)
            memcpy(raw + i * 8, &nan_val, 8);
    } else if (w->pixel_type == TIFF_PIXEL_FLOAT32) {
        float nan_val = NAN;
        for (int64_t i = 0; i < n_samples; i++)
            memcpy(raw + i * 4, &nan_val, 4);
    } else {
        /* Integer types: fill with nodata pattern */
        uint8_t pattern[8];
        write_pixel(pattern, 0, w->nodata, w);
        for (int64_t i = 0; i < n_samples; i++)
            memcpy(raw + i * bps, pattern, (size_t)bps);
    }
}

/* Compress a raw block (strip or tile) and write it; record offset/size.
   Returns 0 on success, -1 on error. The block is freed by the caller.
   Predictor 2 (horizontal differencing) is the caller's responsibility — it
   must be applied to `raw` before calling this helper, since the predictor
   stride differs between strips and tiles. */
static int write_block(TiffWriter *w, const uint8_t *raw, int64_t raw_size,
                       uint64_t *out_offset, uint64_t *out_byte_count) {
    const uint8_t *out_data = raw;
    int64_t out_size = raw_size;

    uint8_t *comp_buf = NULL;
    if (w->compression == COMPRESS_DEFLATE) {
        uLong comp_len = compressBound((uLong)raw_size);
        comp_buf = (uint8_t *)malloc((size_t)comp_len);
        if (comp_buf) {
            int rc = compress2(comp_buf, &comp_len, raw, (uLong)raw_size, 6);
            if (rc == Z_OK) {
                out_data = comp_buf;
                out_size = (int64_t)comp_len;
            }
            /* On compression failure, fall through to uncompressed. */
        }
    } else if (w->compression == COMPRESS_LZW) {
        size_t lzw_len = 0;
        if (tiff_lzw_encode(raw, (size_t)raw_size,
                            &comp_buf, &lzw_len) != 0) {
            snprintf(w->errmsg, 256, "LZW encode failed");
            return -1;
        }
        out_data = comp_buf;
        out_size = (int64_t)lzw_len;
    }

    *out_offset     = (uint64_t)tiff_ftell64(w->fp);
    *out_byte_count = (uint64_t)out_size;
    fwrite(out_data, 1, (size_t)out_size, w->fp);
    free(comp_buf);
    return 0;
}

/* Tiled write path: render each tile (with edge padding) and emit it. */
static int tiff_writer_write_rows_tiled(TiffWriter *w, int64_t row_start,
                                         int64_t n_rows,
                                         const double *const *bands) {
    int64_t W   = w->width;
    int64_t H   = w->height;
    int     nb  = w->n_bands;
    int     bps = w->bytes_per_sample;
    int     TW  = w->tile_width;
    int     TH  = w->tile_height;

    /* Tile-rows touched by [row_start, row_start + n_rows). */
    int64_t first_ty = row_start / TH;
    int64_t last_ty  = (row_start + n_rows - 1) / TH;
    int64_t tile_pixels = (int64_t)TW * (int64_t)TH;
    int64_t tile_raw_size = tile_pixels * nb * bps;

    uint8_t *raw = (uint8_t *)malloc((size_t)tile_raw_size);
    if (!raw) {
        snprintf(w->errmsg, 256, "alloc failed for tile data");
        return -1;
    }

    for (int64_t ty = first_ty; ty <= last_ty; ty++) {
        int64_t tile_row0 = ty * TH;
        int64_t tile_row1 = tile_row0 + TH;
        if (tile_row1 > H) tile_row1 = H;       /* image-edge clip */

        /* Caller may not have given us the rows for this tile yet — only
           emit a tile when [tile_row0, tile_row0 + TH) is fully buffered.
           Currently the node writer hands us all rows in one call, so this
           condition is always met; the check stays for streaming callers. */
        int64_t need_r1 = tile_row0 + TH;
        if (need_r1 > H) need_r1 = H;
        if (tile_row0 < row_start || need_r1 > row_start + n_rows) continue;

        for (int64_t tx = 0; tx < w->n_tiles_x; tx++) {
            int64_t tile_col0 = tx * TW;
            int64_t tile_col1 = tile_col0 + TW;
            if (tile_col1 > W) tile_col1 = W;   /* image-edge clip */

            /* Pad the entire tile with NoData/NaN first, then overwrite the
               in-image region. The TIFF spec requires fully padded tiles. */
            fill_nodata(raw, tile_pixels * nb, w);

            for (int64_t row = tile_row0; row < tile_row1; row++) {
                int64_t src_row = row - row_start;
                int64_t dst_row = row - tile_row0;
                for (int64_t col = tile_col0; col < tile_col1; col++) {
                    int64_t src_idx = src_row * W + col;
                    int64_t dst_off = (dst_row * TW + (col - tile_col0)) *
                                       nb * bps;
                    for (int b = 0; b < nb; b++) {
                        write_pixel(raw, dst_off + b * bps,
                                    bands[b][src_idx], w);
                    }
                }
            }

            int64_t tile_idx = ty * w->n_tiles_x + tx;
            if (tile_idx >= 0 && tile_idx < w->n_tiles) {
                if (write_block(w, raw, tile_raw_size,
                                &w->tile_offsets[tile_idx],
                                &w->tile_byte_counts[tile_idx]) != 0) {
                    free(raw);
                    return -1;
                }
            }
        }
    }

    free(raw);
    return 0;
}

int tiff_writer_write_rows(TiffWriter *w, int64_t row_start, int64_t n_rows,
                           const double *const *bands) {
    if (w->is_tiled)
        return tiff_writer_write_rows_tiled(w, row_start, n_rows, bands);

    int64_t W = w->width;
    int nb = w->n_bands;
    int rps = w->rows_per_strip;
    int bps = w->bytes_per_sample;

    /* Write one strip at a time */
    int64_t first_strip = row_start / rps;
    int64_t last_strip = (row_start + n_rows - 1) / rps;

    for (int64_t s = first_strip; s <= last_strip; s++) {
        int64_t srow_start = s * rps;
        int64_t srow_end = srow_start + rps;
        if (srow_end > w->height) srow_end = w->height;

        /* Clip to the rows we actually have */
        int64_t r0 = srow_start < row_start ? row_start : srow_start;
        int64_t r1 = srow_end < (row_start + n_rows) ? srow_end : (row_start + n_rows);
        int64_t srows = r1 - r0;
        if (srows <= 0) continue;

        /* Build raw strip: chunky interleaved, typed */
        int64_t raw_size = srows * W * nb * bps;
        uint8_t *raw = (uint8_t *)malloc((size_t)raw_size);
        if (!raw) {
            snprintf(w->errmsg, 256, "alloc failed for strip data");
            return -1;
        }

        for (int64_t row = r0; row < r1; row++) {
            int64_t src_row = row - row_start;
            int64_t dst_row = row - srow_start;
            for (int64_t col = 0; col < W; col++) {
                int64_t src_idx = src_row * W + col;
                int64_t dst_off = (dst_row * W + col) * nb * bps;
                for (int b = 0; b < nb; b++) {
                    write_pixel(raw, dst_off + b * bps, bands[b][src_idx], w);
                }
            }
        }

        /* Actual strip size (may be smaller than rps for last strip) */
        int64_t actual_rows = srow_end - srow_start;
        int64_t strip_raw_size = actual_rows * W * nb * bps;

        /* If we don't have all rows for this strip yet, pad with nodata */
        if (srows < actual_rows) {
            uint8_t *full = (uint8_t *)calloc(1, (size_t)strip_raw_size);
            if (!full) {
                free(raw);
                return -1;
            }
            fill_nodata(full, actual_rows * W * nb, w);
            /* Copy the rows we have */
            int64_t dst_offset = (r0 - srow_start) * W * nb * bps;
            memcpy(full + dst_offset, raw, (size_t)raw_size);
            free(raw);
            raw = full;
            raw_size = strip_raw_size;
        } else {
            raw_size = strip_raw_size;
        }

        /* Apply Predictor 2 (horizontal differencing) before compression.
         * Only LZW + integer types use it; float and uncompressed/DEFLATE
         * keep predictor = 1 (none). */
        if (w->predictor == 2) {
            tiff_predictor2_apply(raw, actual_rows, W, nb, bps);
        }

        /* Compress and write via shared helper */
        if (s < w->n_strips) {
            if (write_block(w, raw, raw_size,
                            &w->strip_offsets[s],
                            &w->strip_byte_counts[s]) != 0) {
                free(raw);
                return -1;
            }
            w->strips_written++;
        }
        free(raw);
    }

    return 0;
}

/* ------------------------------------------------------------------ */
/*  IFD entry emission                                                  */
/*                                                                       */
/*  Classic TIFF entry (12 bytes):                                       */
/*     [0..1]  tag                                                       */
/*     [2..3]  type                                                      */
/*     [4..7]  count   (LONG, 32-bit)                                    */
/*     [8..11] value or offset (32-bit)                                  */
/*                                                                       */
/*  BigTIFF entry (20 bytes):                                            */
/*     [0..1]   tag                                                      */
/*     [2..3]   type                                                     */
/*     [4..11]  count   (LONG8, 64-bit)                                  */
/*     [12..19] value or offset (LONG8, 64-bit)                          */
/*                                                                       */
/*  Inline value field is 4 bytes in classic and 8 bytes in BigTIFF —    */
/*  larger payloads go to a separate offset.                             */
/* ------------------------------------------------------------------ */

/* Number of bytes that fit inline in the entry's value/offset field. */
static inline int tiff_entry_inline_capacity(const TiffWriter *w) {
    return w->bigtiff ? 8 : 4;
}

/* Total entry size: 12 bytes classic, 20 bytes BigTIFF. */
static inline int tiff_entry_size(const TiffWriter *w) {
    return w->bigtiff ? 20 : 12;
}

/* Emit one IFD entry. The caller has already decided whether the value
   fits inline or needs to be referenced by `value_or_offset` (which is
   then a file position). For inline values, pass the small value as an
   integer in `value_or_offset`. For arbitrary inline byte strings (e.g.
   short ASCII tags) use tiff_emit_inline_entry instead. */
static void tiff_emit_entry(TiffWriter *w, uint16_t tag, uint16_t type,
                            uint64_t count, uint64_t value_or_offset) {
    uint8_t buf[20];
    write_le16(buf,     tag);
    write_le16(buf + 2, type);
    if (w->bigtiff) {
        write_le64(buf + 4,  count);
        write_le64(buf + 12, value_or_offset);
        fwrite(buf, 1, 20, w->fp);
    } else {
        write_le32(buf + 4, (uint32_t)count);
        write_le32(buf + 8, (uint32_t)value_or_offset);
        fwrite(buf, 1, 12, w->fp);
    }
}

/* Emit an entry whose payload is `n` bytes copied from `bytes` directly
   into the inline value/offset field. The caller must guarantee
   n <= inline_capacity(w). Remaining bytes are zero-padded. */
static void tiff_emit_inline_entry(TiffWriter *w, uint16_t tag, uint16_t type,
                                   uint64_t count,
                                   const void *bytes, int n) {
    uint8_t buf[20];
    int cap = tiff_entry_inline_capacity(w);
    memset(buf, 0, sizeof(buf));
    write_le16(buf,     tag);
    write_le16(buf + 2, type);
    if (w->bigtiff) {
        write_le64(buf + 4, count);
        if (n > 0) memcpy(buf + 12, bytes, (size_t)n);
        if (n < cap) memset(buf + 12 + n, 0, (size_t)(cap - n));
        fwrite(buf, 1, 20, w->fp);
    } else {
        write_le32(buf + 4, (uint32_t)count);
        if (n > 0) memcpy(buf + 8, bytes, (size_t)n);
        if (n < cap) memset(buf + 8 + n, 0, (size_t)(cap - n));
        fwrite(buf, 1, 12, w->fp);
    }
}

/* Emit a SHORT(s) entry inline-packed when count fits.
   Used for BitsPerSample / SampleFormat. */
static void tiff_emit_short_array_entry(TiffWriter *w, uint16_t tag,
                                        uint16_t value, int count,
                                        uint64_t fallback_offset) {
    int cap = tiff_entry_inline_capacity(w);
    int needed = count * 2;
    if (needed <= cap) {
        uint8_t small[8] = {0};
        for (int i = 0; i < count; i++) write_le16(small + i * 2, value);
        tiff_emit_inline_entry(w, tag, TIFF_SHORT, (uint64_t)count,
                               small, needed);
    } else {
        tiff_emit_entry(w, tag, TIFF_SHORT, (uint64_t)count, fallback_offset);
    }
}

int tiff_writer_finish(TiffWriter *w) {
    int64_t ifd_data_start = tiff_ftell64(w->fp);

    int nb = w->n_bands;
    /* Block-array size: strips for strip mode, tiles for tiled mode. The two
       layouts use disjoint IFD tags (273/279 vs 324/325) but share a single
       on-disk array of LONG offsets/counts; we reuse those vars. */
    int64_t n_blocks = w->is_tiled ? w->n_tiles : w->n_strips;
    uint16_t bps_val = (uint16_t)w->bits_per_sample;
    uint16_t sf_val  = (uint16_t)w->sample_format;
    int big = w->bigtiff;

    /* Block offsets/byte counts use TIFF_LONG (4 B) for classic or
       TIFF_LONG8 (8 B) for BigTIFF. Tiled+BigTIFF is rejected at open(),
       so when w->is_tiled is true, big is always 0 here. */
    int block_dtype = big ? TIFF_LONG8 : TIFF_LONG;
    int block_elem  = big ? 8 : 4;

    /* Block offsets array (StripOffsets / TileOffsets) */
    uint64_t off_block_offsets = (uint64_t)tiff_ftell64(w->fp);
    for (int64_t i = 0; i < n_blocks; i++) {
        uint8_t buf[8];
        uint64_t v = w->is_tiled ? (uint64_t)w->tile_offsets[i]
                                  : w->strip_offsets[i];
        if (big) write_le64(buf, v);
        else     write_le32(buf, (uint32_t)v);
        fwrite(buf, 1, (size_t)block_elem, w->fp);
    }

    /* Block byte counts array (StripByteCounts / TileByteCounts) */
    uint64_t off_block_counts = (uint64_t)tiff_ftell64(w->fp);
    for (int64_t i = 0; i < n_blocks; i++) {
        uint8_t buf[8];
        uint64_t v = w->is_tiled ? (uint64_t)w->tile_byte_counts[i]
                                  : w->strip_byte_counts[i];
        if (big) write_le64(buf, v);
        else     write_le32(buf, (uint32_t)v);
        fwrite(buf, 1, (size_t)block_elem, w->fp);
    }

    /* BitsPerSample array (used only when count*2 > inline capacity) */
    uint64_t off_bps = (uint64_t)tiff_ftell64(w->fp);
    for (int b = 0; b < nb; b++) {
        uint8_t buf[2];
        write_le16(buf, bps_val);
        fwrite(buf, 1, 2, w->fp);
    }

    /* SampleFormat array */
    uint64_t off_sf = (uint64_t)tiff_ftell64(w->fp);
    for (int b = 0; b < nb; b++) {
        uint8_t buf[2];
        write_le16(buf, sf_val);
        fwrite(buf, 1, 2, w->fp);
    }

    /* ModelPixelScale: 3 doubles */
    uint64_t off_scale = (uint64_t)tiff_ftell64(w->fp);
    {
        double scale[3] = { w->gt[1], -w->gt[5], 0.0 };
        fwrite(scale, sizeof(double), 3, w->fp);
    }

    /* ModelTiepoint: 6 doubles */
    uint64_t off_tiepoint = (uint64_t)tiff_ftell64(w->fp);
    {
        double tp[6] = { 0, 0, 0, w->gt[0], w->gt[3], 0 };
        fwrite(tp, sizeof(double), 6, w->fp);
    }

    /* GeoKey directory + GeoAsciiParams (see classic-TIFF comment in prior
       revision for the spec details — the on-disk layout is identical
       between classic TIFF and BigTIFF; only the IFD entry referencing
       these blobs differs in width). */
    int      have_crs       = (w->epsg_geographic > 0 || w->epsg_projected > 0);
    int      n_geo_keys     = 0;
    uint16_t geo_keys[5 * 4];
    uint64_t off_geo_dir    = 0;
    uint64_t off_geo_ascii  = 0;
    int      geo_dir_count  = 0;
    int      geo_ascii_len  = 0;

    if (have_crs) {
        int is_projected = (w->epsg_projected > 0);

        geo_keys[n_geo_keys * 4 + 0] = 1024;
        geo_keys[n_geo_keys * 4 + 1] = 0;
        geo_keys[n_geo_keys * 4 + 2] = 1;
        geo_keys[n_geo_keys * 4 + 3] = (uint16_t)(is_projected ? 1 : 2);
        n_geo_keys++;

        geo_keys[n_geo_keys * 4 + 0] = 1025;
        geo_keys[n_geo_keys * 4 + 1] = 0;
        geo_keys[n_geo_keys * 4 + 2] = 1;
        geo_keys[n_geo_keys * 4 + 3] = 1;
        n_geo_keys++;

        if (is_projected) {
            geo_keys[n_geo_keys * 4 + 0] = 3072;
            geo_keys[n_geo_keys * 4 + 1] = 0;
            geo_keys[n_geo_keys * 4 + 2] = 1;
            geo_keys[n_geo_keys * 4 + 3] = (uint16_t)w->epsg_projected;
            n_geo_keys++;
        } else {
            geo_keys[n_geo_keys * 4 + 0] = 2048;
            geo_keys[n_geo_keys * 4 + 1] = 0;
            geo_keys[n_geo_keys * 4 + 2] = 1;
            geo_keys[n_geo_keys * 4 + 3] = (uint16_t)w->epsg_geographic;
            n_geo_keys++;
        }

        if (w->crs_citation && w->crs_citation[0]) {
            int cit_raw = (int)strlen(w->crs_citation);
            geo_ascii_len = cit_raw + 1;
            off_geo_ascii = (uint64_t)tiff_ftell64(w->fp);
            fwrite(w->crs_citation, 1, (size_t)cit_raw, w->fp);
            uint8_t bar = (uint8_t)'|';
            fwrite(&bar, 1, 1, w->fp);

            geo_keys[n_geo_keys * 4 + 0] = (uint16_t)(is_projected ? 3073 : 2049);
            geo_keys[n_geo_keys * 4 + 1] = TAG_GEO_ASCII_PARAMS;
            geo_keys[n_geo_keys * 4 + 2] = (uint16_t)geo_ascii_len;
            geo_keys[n_geo_keys * 4 + 3] = 0;
            n_geo_keys++;
        }

        geo_dir_count = 4 + 4 * n_geo_keys;
        off_geo_dir = (uint64_t)tiff_ftell64(w->fp);
        uint8_t hdr_buf[8];
        write_le16(hdr_buf + 0, 1);
        write_le16(hdr_buf + 2, 1);
        write_le16(hdr_buf + 4, 0);
        write_le16(hdr_buf + 6, (uint16_t)n_geo_keys);
        fwrite(hdr_buf, 1, 8, w->fp);
        for (int i = 0; i < n_geo_keys * 4; i++) {
            uint8_t buf[2];
            write_le16(buf, geo_keys[i]);
            fwrite(buf, 1, 2, w->fp);
        }
    }

    /* GDAL_METADATA string (tag 42112) */
    uint64_t off_metadata = 0;
    int metadata_len = 0;
    if (w->metadata) {
        metadata_len = (int)strlen(w->metadata) + 1;
        off_metadata = (uint64_t)tiff_ftell64(w->fp);
        fwrite(w->metadata, 1, (size_t)metadata_len, w->fp);
    }

    /* GDAL_NODATA string */
    uint64_t off_nodata = 0;
    int nodata_len = 0;
    char nodata_str[64];
    if (w->has_nodata) {
        nodata_len = snprintf(nodata_str, 64, "%.17g", w->nodata);
        nodata_len++; /* include null terminator */
        off_nodata = (uint64_t)tiff_ftell64(w->fp);
        fwrite(nodata_str, 1, (size_t)nodata_len, w->fp);
    }

    /* Count IFD entries.
       Common (9): Width, Length, BPS, Compression, Photometric, SPP,
                   PlanarConfig, SampleFormat, ModelPixelScale.
       Strip mode adds 3: StripOffsets, RowsPerStrip, StripByteCounts.
       Tiled mode adds 4: TileWidth, TileLength, TileOffsets, TileByteCounts. */
    int n_tags = 9;
    n_tags += w->is_tiled ? 4 : 3;
    n_tags++; /* ModelTiepoint */
    if (w->predictor != 1) n_tags++; /* Predictor (317) — only when != default */
    if (have_crs) {
        n_tags++; /* GeoKeyDirectory */
        if (geo_ascii_len > 0) n_tags++; /* GeoAsciiParams */
    }
    if (w->metadata) n_tags++;
    if (w->has_nodata) n_tags++;

    /* Write IFD */
    uint64_t ifd_offset = (uint64_t)tiff_ftell64(w->fp);

    /* Entry count: SHORT (2 B) classic, LONG8 (8 B) BigTIFF */
    if (big) {
        uint8_t cnt[8];
        write_le64(cnt, (uint64_t)n_tags);
        fwrite(cnt, 1, 8, w->fp);
    } else {
        uint8_t cnt[2];
        write_le16(cnt, (uint16_t)n_tags);
        fwrite(cnt, 1, 2, w->fp);
    }

    /* Entries (sorted by tag). Inline values populate the value/offset
       field directly; large payloads pass the previously-recorded byte
       offset. */

    /* 256: ImageWidth */
    tiff_emit_entry(w, TAG_IMAGE_WIDTH, TIFF_LONG, 1, (uint64_t)w->width);

    /* 257: ImageLength */
    tiff_emit_entry(w, TAG_IMAGE_LENGTH, TIFF_LONG, 1, (uint64_t)w->height);

    /* 258: BitsPerSample (SHORT) */
    tiff_emit_short_array_entry(w, TAG_BITS_PER_SAMPLE, bps_val, nb, off_bps);

    /* 259: Compression */
    tiff_emit_entry(w, TAG_COMPRESSION, TIFF_SHORT, 1,
                    (uint64_t)w->compression);

    /* 262: PhotometricInterpretation = 1 (MinIsBlack) */
    tiff_emit_entry(w, TAG_PHOTOMETRIC, TIFF_SHORT, 1, 1);

    /* Strip-only tags 273/278/279 come before 277/284, tile tags 322-325
       come after 284. IFD entries MUST be sorted by tag id. */
    if (!w->is_tiled) {
        /* 273: StripOffsets */
        if (n_blocks == 1) {
            tiff_emit_entry(w, TAG_STRIP_OFFSETS, block_dtype, 1,
                            w->strip_offsets[0]);
        } else {
            tiff_emit_entry(w, TAG_STRIP_OFFSETS, block_dtype,
                            (uint64_t)n_blocks, off_block_offsets);
        }
    }

    /* 277: SamplesPerPixel */
    tiff_emit_entry(w, TAG_SAMPLES_PER_PIXEL, TIFF_SHORT, 1, (uint64_t)nb);

    if (!w->is_tiled) {
        /* 278: RowsPerStrip */
        tiff_emit_entry(w, TAG_ROWS_PER_STRIP, TIFF_LONG, 1,
                        (uint64_t)w->rows_per_strip);

        /* 279: StripByteCounts */
        if (n_blocks == 1) {
            tiff_emit_entry(w, TAG_STRIP_BYTE_COUNTS, block_dtype, 1,
                            w->strip_byte_counts[0]);
        } else {
            tiff_emit_entry(w, TAG_STRIP_BYTE_COUNTS, block_dtype,
                            (uint64_t)n_blocks, off_block_counts);
        }
    }

    /* 284: PlanarConfiguration = 1 (chunky) */
    tiff_emit_entry(w, TAG_PLANAR_CONFIG, TIFF_SHORT, 1, 1);

    /* 317: Predictor — only emit when non-default (1 = none) */
    if (w->predictor != 1) {
        tiff_emit_entry(w, TAG_PREDICTOR, TIFF_SHORT, 1,
                        (uint64_t)w->predictor);
    }

    if (w->is_tiled) {
        /* 322: TileWidth */
        tiff_emit_entry(w, TAG_TILE_WIDTH, TIFF_LONG, 1,
                        (uint64_t)w->tile_width);

        /* 323: TileLength */
        tiff_emit_entry(w, TAG_TILE_LENGTH, TIFF_LONG, 1,
                        (uint64_t)w->tile_height);

        /* 324: TileOffsets */
        if (n_blocks == 1) {
            tiff_emit_entry(w, TAG_TILE_OFFSETS, block_dtype, 1,
                            (uint64_t)w->tile_offsets[0]);
        } else {
            tiff_emit_entry(w, TAG_TILE_OFFSETS, block_dtype,
                            (uint64_t)n_blocks, off_block_offsets);
        }

        /* 325: TileByteCounts */
        if (n_blocks == 1) {
            tiff_emit_entry(w, TAG_TILE_BYTE_COUNTS, block_dtype, 1,
                            (uint64_t)w->tile_byte_counts[0]);
        } else {
            tiff_emit_entry(w, TAG_TILE_BYTE_COUNTS, block_dtype,
                            (uint64_t)n_blocks, off_block_counts);
        }
    }

    /* 339: SampleFormat (SHORT) */
    tiff_emit_short_array_entry(w, TAG_SAMPLE_FORMAT, sf_val, nb, off_sf);

    /* 33550: ModelPixelScale (3 DOUBLEs = 24 B → never inline) */
    tiff_emit_entry(w, TAG_MODEL_PIXEL_SCALE, TIFF_DOUBLE, 3, off_scale);

    /* 33922: ModelTiepoint (6 DOUBLEs = 48 B → never inline) */
    tiff_emit_entry(w, TAG_MODEL_TIEPOINT, TIFF_DOUBLE, 6, off_tiepoint);

    /* 34735: GeoKeyDirectory */
    if (have_crs) {
        tiff_emit_entry(w, TAG_GEO_KEY_DIRECTORY, TIFF_SHORT,
                        (uint64_t)geo_dir_count, off_geo_dir);

        if (geo_ascii_len > 0) {
            tiff_emit_entry(w, TAG_GEO_ASCII_PARAMS, TIFF_ASCII,
                            (uint64_t)geo_ascii_len, off_geo_ascii);
        }
    }

    /* 42112: GDAL_METADATA */
    if (w->metadata) {
        if (metadata_len <= tiff_entry_inline_capacity(w)) {
            tiff_emit_inline_entry(w, TAG_GDAL_METADATA, TIFF_ASCII,
                                   (uint64_t)metadata_len,
                                   w->metadata, metadata_len);
        } else {
            tiff_emit_entry(w, TAG_GDAL_METADATA, TIFF_ASCII,
                            (uint64_t)metadata_len, off_metadata);
        }
    }

    /* 42113: GDAL_NODATA */
    if (w->has_nodata) {
        if (nodata_len <= tiff_entry_inline_capacity(w)) {
            tiff_emit_inline_entry(w, TAG_GDAL_NODATA, TIFF_ASCII,
                                   (uint64_t)nodata_len,
                                   nodata_str, nodata_len);
        } else {
            tiff_emit_entry(w, TAG_GDAL_NODATA, TIFF_ASCII,
                            (uint64_t)nodata_len, off_nodata);
        }
    }

    /* Next IFD offset = 0 (no more IFDs). 4 bytes classic / 8 BigTIFF. */
    if (big) {
        uint8_t zero8[8] = {0};
        fwrite(zero8, 1, 8, w->fp);
    } else {
        uint8_t zero4[4] = {0};
        fwrite(zero4, 1, 4, w->fp);
    }

    /* Patch header with IFD offset.
         Classic: 4-byte LONG at offset 4.
         BigTIFF: 8-byte LONG8 at offset 8. */
    if (big) {
        tiff_fseek64(w->fp, 8, SEEK_SET);
        uint8_t off_buf[8];
        write_le64(off_buf, ifd_offset);
        fwrite(off_buf, 1, 8, w->fp);
    } else {
        tiff_fseek64(w->fp, 4, SEEK_SET);
        uint8_t off_buf[4];
        write_le32(off_buf, (uint32_t)ifd_offset);
        fwrite(off_buf, 1, 4, w->fp);
    }

    (void)ifd_data_start;
    return 0;
}

const char *tiff_writer_errmsg(TiffWriter *w) { return w->errmsg; }

void tiff_writer_close(TiffWriter *w) {
    if (!w) return;
    if (w->fp) fclose(w->fp);
    free(w->strip_offsets);
    free(w->strip_byte_counts);
    free(w->tile_offsets);
    free(w->tile_byte_counts);
    free(w->metadata);
    free(w->crs_citation);
    free(w);
}
