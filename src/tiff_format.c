#include "tiff_format.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <zlib.h>

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
#define TAG_SAMPLE_FORMAT        339
#define TAG_MODEL_TIEPOINT       33922
#define TAG_MODEL_PIXEL_SCALE    33550
#define TAG_GDAL_NODATA          42113

#define COMPRESS_NONE     1
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
    int     rows_per_strip;
    int     planar_config;     /* 1=chunky, 2=planar */

    int64_t *strip_offsets;
    int64_t *strip_byte_counts;
    int64_t  n_strips;

    double gt[6];
    int    has_geotransform;

    double nodata;
    int    has_nodata;

    char errmsg[256];
};

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
    r->rows_per_strip = (int)r->height;
    r->planar_config = 1;
    r->n_bands = 1;
    r->bits_per_sample = 8;
    r->sample_format = SAMPLE_UINT;
    r->compression = COMPRESS_NONE;

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
            int64_t *v = read_tag_ints(io, dtype, 1, valp, entry_val_bytes);
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
            if (v) { r->rows_per_strip = (int)v[0]; free(v); }
            break;
        }
        case TAG_STRIP_OFFSETS: {
            r->strip_offsets = read_tag_ints(io, dtype, count,
                                              valp, entry_val_bytes);
            r->n_strips = count;
            break;
        }
        case TAG_STRIP_BYTE_COUNTS: {
            r->strip_byte_counts = read_tag_ints(io, dtype, count,
                                                  valp, entry_val_bytes);
            break;
        }
        case TAG_PLANAR_CONFIG: {
            int64_t *v = read_tag_ints(io, dtype, 1, valp, entry_val_bytes);
            if (v) { r->planar_config = (int)v[0]; free(v); }
            break;
        }
        case TAG_SAMPLE_FORMAT: {
            int64_t *v = read_tag_ints(io, dtype, 1, valp, entry_val_bytes);
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
        return -1;
    }
    if (!r->strip_offsets || !r->strip_byte_counts) {
        snprintf(r->errmsg, 256,
                 "missing strip offsets/counts (tiled TIFFs not supported)");
        return -1;
    }
    if (r->compression != COMPRESS_NONE &&
        r->compression != COMPRESS_DEFLATE &&
        r->compression != COMPRESS_ADOBE_DEFLATE) {
        snprintf(r->errmsg, 256,
                 "unsupported compression: %d (only none/deflate supported)",
                 r->compression);
        return -1;
    }
    if (r->n_bands > TIFF_MAX_BANDS) {
        snprintf(r->errmsg, 256, "too many bands: %d", r->n_bands);
        return -1;
    }

    if (!r->has_geotransform) {
        r->gt[0] = 0.0; r->gt[1] = 1.0; r->gt[2] = 0.0;
        r->gt[3] = (double)r->height; r->gt[4] = 0.0; r->gt[5] = -1.0;
    }

    return 0;
}

/* ================================================================== */
/*  Strip decompression                                                */
/* ================================================================== */

static uint8_t *read_strip(TiffReader *r, int64_t strip_idx,
                             int64_t expected_bytes, int64_t *out_len) {
    int64_t offset = r->strip_offsets[strip_idx];
    int64_t compressed_len = r->strip_byte_counts[strip_idx];

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

    int rps = r->rows_per_strip;
    int64_t first_strip = row_start / rps;
    int64_t last_strip = (row_start + n_rows - 1) / rps;

    for (int64_t s = first_strip; s <= last_strip && s < r->n_strips; s++) {
        int64_t strip_row_start = s * rps;
        int64_t strip_rows = rps;
        if (strip_row_start + strip_rows > r->height)
            strip_rows = r->height - strip_row_start;

        int64_t expected_bytes;
        if (r->planar_config == 1)
            expected_bytes = strip_rows * W * pixel_bytes;
        else
            expected_bytes = strip_rows * W * bytes_per_sample;

        int64_t actual_len = 0;
        uint8_t *strip_data = read_strip(r, s, expected_bytes, &actual_len);
        if (!strip_data) {
            snprintf(r->errmsg, 256, "failed to read strip %lld",
                     (long long)s);
            return -1;
        }

        for (int64_t sr = 0; sr < strip_rows; sr++) {
            int64_t abs_row = strip_row_start + sr;
            if (abs_row < row_start || abs_row >= row_start + n_rows)
                continue;
            int64_t out_row = abs_row - row_start;

            for (int64_t col = 0; col < W; col++) {
                int64_t out_idx = out_row * W + col;

                if (r->planar_config == 1) {
                    int64_t pixel_off = (sr * W + col) * pixel_bytes;
                    for (int b = 0; b < nb; b++) {
                        double val = extract_pixel(strip_data,
                            pixel_off + b * bytes_per_sample,
                            bps, r->sample_format, &r->io);
                        if (r->has_nodata && val == r->nodata)
                            val = NAN;
                        out_bands[b][out_idx] = val;
                    }
                } else {
                    /* Planar: simplified — only handles chunky well */
                    for (int b = 0; b < nb; b++) {
                        int64_t pixel_off = (sr * W + col) * bytes_per_sample;
                        double val = extract_pixel(strip_data, pixel_off,
                            bps, r->sample_format, &r->io);
                        if (r->has_nodata && val == r->nodata)
                            val = NAN;
                        out_bands[b][out_idx] = val;
                    }
                }
            }
        }

        free(strip_data);
    }

    return 0;
}

void tiff_reader_close(TiffReader *r) {
    if (!r) return;
    if (r->io.fp) fclose(r->io.fp);
    free(r->strip_offsets);
    free(r->strip_byte_counts);
    free(r);
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

struct TiffWriter {
    FILE    *fp;
    int64_t  width;
    int64_t  height;
    int      n_bands;
    double   gt[6];
    double   nodata;
    int      has_nodata;
    int      use_deflate;
    int      rows_per_strip;

    int64_t  n_strips;
    uint32_t *strip_offsets;
    uint32_t *strip_byte_counts;
    int64_t   strips_written;

    char errmsg[256];
};

int tiff_writer_open(const char *path, TiffWriter **out,
                     int64_t width, int64_t height, int n_bands,
                     const double *gt, double nodata, int use_deflate) {
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
    w->use_deflate = use_deflate;
    w->rows_per_strip = 256;
    if (w->rows_per_strip > height) w->rows_per_strip = (int)height;

    if (gt) {
        memcpy(w->gt, gt, 6 * sizeof(double));
    } else {
        w->gt[0] = 0; w->gt[1] = 1; w->gt[2] = 0;
        w->gt[3] = (double)height; w->gt[4] = 0; w->gt[5] = -1;
    }

    w->has_nodata = !isnan(nodata);
    w->nodata = nodata;

    w->n_strips = (height + w->rows_per_strip - 1) / w->rows_per_strip;
    w->strip_offsets = (uint32_t *)calloc((size_t)w->n_strips, sizeof(uint32_t));
    w->strip_byte_counts = (uint32_t *)calloc((size_t)w->n_strips, sizeof(uint32_t));
    w->strips_written = 0;

    /* Write TIFF header: little-endian, magic=42, IFD offset=0 (patched later) */
    uint8_t hdr[8] = {'I', 'I', 42, 0, 0, 0, 0, 0};
    fwrite(hdr, 1, 8, w->fp);

    *out = w;
    return 0;
}

int tiff_writer_write_rows(TiffWriter *w, int64_t row_start, int64_t n_rows,
                           const double *const *bands) {
    int64_t W = w->width;
    int nb = w->n_bands;
    int rps = w->rows_per_strip;

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

        /* Build raw strip: chunky interleaved Float64 */
        int64_t raw_size = srows * W * nb * 8;
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
                int64_t dst_off = (dst_row * W + col) * nb * 8;
                for (int b = 0; b < nb; b++) {
                    double val = bands[b][src_idx];
                    memcpy(raw + dst_off + b * 8, &val, 8);
                }
            }
        }

        /* Actual strip size (may be smaller than rps for last strip) */
        int64_t actual_rows = srow_end - srow_start;
        int64_t strip_raw_size = actual_rows * W * nb * 8;

        /* If we don't have all rows for this strip yet, pad with nodata */
        if (srows < actual_rows) {
            /* Need to fill missing rows with NaN */
            uint8_t *full = (uint8_t *)calloc(1, (size_t)strip_raw_size);
            if (!full) {
                free(raw);
                return -1;
            }
            /* Fill with NaN */
            double nan_val = NAN;
            for (int64_t i = 0; i < actual_rows * W * nb; i++)
                memcpy(full + i * 8, &nan_val, 8);
            /* Copy the rows we have */
            int64_t dst_offset = (r0 - srow_start) * W * nb * 8;
            memcpy(full + dst_offset, raw, (size_t)raw_size);
            free(raw);
            raw = full;
            raw_size = strip_raw_size;
        } else {
            raw_size = strip_raw_size;
        }

        /* Compress if requested */
        uint8_t *out_data = raw;
        int64_t out_size = raw_size;

        uint8_t *comp_buf = NULL;
        if (w->use_deflate) {
            uLong comp_len = compressBound((uLong)raw_size);
            comp_buf = (uint8_t *)malloc((size_t)comp_len);
            if (comp_buf) {
                int rc = compress2(comp_buf, &comp_len,
                                   raw, (uLong)raw_size, 6);
                if (rc == Z_OK) {
                    out_data = comp_buf;
                    out_size = (int64_t)comp_len;
                }
                /* On compression failure, fall through to uncompressed */
            }
        }

        /* Record offset and size */
        if (s < w->n_strips) {
            w->strip_offsets[s] = (uint32_t)ftell(w->fp);
            w->strip_byte_counts[s] = (uint32_t)out_size;
        }

        fwrite(out_data, 1, (size_t)out_size, w->fp);
        w->strips_written++;

        free(raw);
        free(comp_buf);
    }

    return 0;
}

/* Write an IFD entry */
static void write_ifd_entry(uint8_t *p, uint16_t tag, uint16_t type,
                             uint32_t count, uint32_t value) {
    write_le16(p, tag);
    write_le16(p + 2, type);
    write_le32(p + 4, count);
    write_le32(p + 8, value);
}

int tiff_writer_finish(TiffWriter *w) {
    long ifd_data_start = ftell(w->fp);

    int nb = w->n_bands;
    int64_t ns = w->n_strips;

    /* Write auxiliary data blocks before IFD:
       1. StripOffsets array (4*ns bytes)
       2. StripByteCounts array (4*ns bytes)
       3. BitsPerSample array (2*nb bytes) — if nb > 1
       4. SampleFormat array (2*nb bytes) — if nb > 1
       5. ModelPixelScale (3 doubles = 24 bytes)
       6. ModelTiepoint (6 doubles = 48 bytes)
       7. GDAL_NODATA string
    */

    /* StripOffsets */
    uint32_t off_strip_offsets = (uint32_t)ftell(w->fp);
    for (int64_t i = 0; i < ns; i++) {
        uint8_t buf[4];
        write_le32(buf, w->strip_offsets[i]);
        fwrite(buf, 1, 4, w->fp);
    }

    /* StripByteCounts */
    uint32_t off_strip_counts = (uint32_t)ftell(w->fp);
    for (int64_t i = 0; i < ns; i++) {
        uint8_t buf[4];
        write_le32(buf, w->strip_byte_counts[i]);
        fwrite(buf, 1, 4, w->fp);
    }

    /* BitsPerSample array */
    uint32_t off_bps = (uint32_t)ftell(w->fp);
    for (int b = 0; b < nb; b++) {
        uint8_t buf[2];
        write_le16(buf, 64);
        fwrite(buf, 1, 2, w->fp);
    }

    /* SampleFormat array */
    uint32_t off_sf = (uint32_t)ftell(w->fp);
    for (int b = 0; b < nb; b++) {
        uint8_t buf[2];
        write_le16(buf, SAMPLE_FLOAT);
        fwrite(buf, 1, 2, w->fp);
    }

    /* ModelPixelScale: 3 doubles */
    uint32_t off_scale = (uint32_t)ftell(w->fp);
    {
        double scale[3] = { w->gt[1], -w->gt[5], 0.0 };
        fwrite(scale, sizeof(double), 3, w->fp);
    }

    /* ModelTiepoint: 6 doubles */
    uint32_t off_tiepoint = (uint32_t)ftell(w->fp);
    {
        double tp[6] = { 0, 0, 0, w->gt[0], w->gt[3], 0 };
        fwrite(tp, sizeof(double), 6, w->fp);
    }

    /* GDAL_NODATA string */
    uint32_t off_nodata = 0;
    int nodata_len = 0;
    char nodata_str[64];
    if (w->has_nodata) {
        nodata_len = snprintf(nodata_str, 64, "%.17g", w->nodata);
        nodata_len++; /* include null terminator */
        off_nodata = (uint32_t)ftell(w->fp);
        fwrite(nodata_str, 1, (size_t)nodata_len, w->fp);
    }

    /* Count IFD entries */
    int n_tags = 12; /* Width, Length, BPS, Compression, Photometric,
                        StripOffsets, SPP, RowsPerStrip, StripByteCounts,
                        PlanarConfig, SampleFormat, ModelPixelScale */
    n_tags++; /* ModelTiepoint */
    if (w->has_nodata) n_tags++;

    /* Write IFD */
    uint32_t ifd_offset = (uint32_t)ftell(w->fp);

    /* Entry count */
    uint8_t cnt[2];
    write_le16(cnt, (uint16_t)n_tags);
    fwrite(cnt, 1, 2, w->fp);

    /* Entries (12 bytes each, sorted by tag) */
    uint8_t ent[12];

    /* 256: ImageWidth */
    write_ifd_entry(ent, TAG_IMAGE_WIDTH, TIFF_LONG, 1, (uint32_t)w->width);
    fwrite(ent, 1, 12, w->fp);

    /* 257: ImageLength */
    write_ifd_entry(ent, TAG_IMAGE_LENGTH, TIFF_LONG, 1, (uint32_t)w->height);
    fwrite(ent, 1, 12, w->fp);

    /* 258: BitsPerSample */
    if (nb == 1) {
        write_ifd_entry(ent, TAG_BITS_PER_SAMPLE, TIFF_SHORT, 1, 64);
    } else {
        write_ifd_entry(ent, TAG_BITS_PER_SAMPLE, TIFF_SHORT,
                         (uint32_t)nb, off_bps);
    }
    fwrite(ent, 1, 12, w->fp);

    /* 259: Compression */
    write_ifd_entry(ent, TAG_COMPRESSION, TIFF_SHORT, 1,
                     w->use_deflate ? COMPRESS_DEFLATE : COMPRESS_NONE);
    fwrite(ent, 1, 12, w->fp);

    /* 262: PhotometricInterpretation = 1 (MinIsBlack) */
    write_ifd_entry(ent, TAG_PHOTOMETRIC, TIFF_SHORT, 1, 1);
    fwrite(ent, 1, 12, w->fp);

    /* 273: StripOffsets */
    if (ns == 1) {
        write_ifd_entry(ent, TAG_STRIP_OFFSETS, TIFF_LONG, 1,
                         w->strip_offsets[0]);
    } else {
        write_ifd_entry(ent, TAG_STRIP_OFFSETS, TIFF_LONG,
                         (uint32_t)ns, off_strip_offsets);
    }
    fwrite(ent, 1, 12, w->fp);

    /* 277: SamplesPerPixel */
    write_ifd_entry(ent, TAG_SAMPLES_PER_PIXEL, TIFF_SHORT, 1,
                     (uint32_t)nb);
    fwrite(ent, 1, 12, w->fp);

    /* 278: RowsPerStrip */
    write_ifd_entry(ent, TAG_ROWS_PER_STRIP, TIFF_LONG, 1,
                     (uint32_t)w->rows_per_strip);
    fwrite(ent, 1, 12, w->fp);

    /* 279: StripByteCounts */
    if (ns == 1) {
        write_ifd_entry(ent, TAG_STRIP_BYTE_COUNTS, TIFF_LONG, 1,
                         w->strip_byte_counts[0]);
    } else {
        write_ifd_entry(ent, TAG_STRIP_BYTE_COUNTS, TIFF_LONG,
                         (uint32_t)ns, off_strip_counts);
    }
    fwrite(ent, 1, 12, w->fp);

    /* 284: PlanarConfiguration = 1 (chunky) */
    write_ifd_entry(ent, TAG_PLANAR_CONFIG, TIFF_SHORT, 1, 1);
    fwrite(ent, 1, 12, w->fp);

    /* 339: SampleFormat */
    if (nb == 1) {
        write_ifd_entry(ent, TAG_SAMPLE_FORMAT, TIFF_SHORT, 1, SAMPLE_FLOAT);
    } else {
        write_ifd_entry(ent, TAG_SAMPLE_FORMAT, TIFF_SHORT,
                         (uint32_t)nb, off_sf);
    }
    fwrite(ent, 1, 12, w->fp);

    /* 33550: ModelPixelScale */
    write_ifd_entry(ent, TAG_MODEL_PIXEL_SCALE, TIFF_DOUBLE, 3, off_scale);
    fwrite(ent, 1, 12, w->fp);

    /* 33922: ModelTiepoint */
    write_ifd_entry(ent, TAG_MODEL_TIEPOINT, TIFF_DOUBLE, 6, off_tiepoint);
    fwrite(ent, 1, 12, w->fp);

    /* 42113: GDAL_NODATA */
    if (w->has_nodata) {
        if (nodata_len <= 4) {
            write_ifd_entry(ent, TAG_GDAL_NODATA, TIFF_ASCII,
                             (uint32_t)nodata_len, 0);
            /* Inline the short string */
            memcpy(ent + 8, nodata_str, (size_t)nodata_len);
            if (nodata_len < 4) memset(ent + 8 + nodata_len, 0,
                                        (size_t)(4 - nodata_len));
        } else {
            write_ifd_entry(ent, TAG_GDAL_NODATA, TIFF_ASCII,
                             (uint32_t)nodata_len, off_nodata);
        }
        fwrite(ent, 1, 12, w->fp);
    }

    /* Next IFD offset = 0 (no more IFDs) */
    uint8_t zero4[4] = {0, 0, 0, 0};
    fwrite(zero4, 1, 4, w->fp);

    /* Patch header: write IFD offset at byte 4 */
    fseek(w->fp, 4, SEEK_SET);
    uint8_t ifd_off_buf[4];
    write_le32(ifd_off_buf, ifd_offset);
    fwrite(ifd_off_buf, 1, 4, w->fp);

    (void)ifd_data_start;
    return 0;
}

const char *tiff_writer_errmsg(TiffWriter *w) { return w->errmsg; }

void tiff_writer_close(TiffWriter *w) {
    if (!w) return;
    if (w->fp) fclose(w->fp);
    free(w->strip_offsets);
    free(w->strip_byte_counts);
    free(w);
}
