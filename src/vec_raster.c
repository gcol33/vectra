/*
 * vec_raster.c — VECR raster writer/reader (Phase 1).
 *
 * Each tile becomes a self-describing tdc_block_record. The codec chain is
 * PRED_2D Paeth + (ZIGZAG for signed ints) + BYTE_SHUFFLE + LZ — chosen to
 * be lossless across all supported sample dtypes and to give a sensible
 * baseline before Phase 2 codec probing lands.
 *
 * The file layout, index entry layout, and header layout are documented in
 * vec_raster.h. The implementation here owns the file I/O (FILE*-based, not
 * mmap; mmap is a Phase 5 perf concern), the tile iteration, and the tdc
 * encode/decode bridge. Edge tiles smaller than tile_size are encoded at
 * their actual dimensions — the decoder reads the dim[] from the block
 * header rather than assuming square tiles.
 */

#include "vec_raster.h"
#include "error.h"
#include "vec_omp.h"

#include "tdc/types.h"
#include "tdc/codec.h"
#include "tdc/format.h"

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(VecrHeader) == VECR_HEADER_SIZE,
               "VecrHeader must be 160 bytes");
_Static_assert(sizeof(VecrIndexEntry) == VECR_INDEX_ENTRY_SIZE,
               "VecrIndexEntry must be 64 bytes");
#endif

/* ====================================================================== */
/*  Helpers                                                                */
/* ====================================================================== */

size_t vecr_dtype_size(uint8_t dtype) {
    switch (dtype) {
    case VECR_DT_I8: case VECR_DT_U8:                 return 1;
    case VECR_DT_I16: case VECR_DT_U16:               return 2;
    case VECR_DT_I32: case VECR_DT_U32: case VECR_DT_F32: return 4;
    case VECR_DT_I64: case VECR_DT_U64: case VECR_DT_F64: return 8;
    default: return 0;
    }
}

static int vecr_dtype_is_float(uint8_t dt) {
    return dt == VECR_DT_F32 || dt == VECR_DT_F64;
}

static int vecr_dtype_is_signed(uint8_t dt) {
    return dt == VECR_DT_I8 || dt == VECR_DT_I16 ||
           dt == VECR_DT_I32 || dt == VECR_DT_I64;
}

/* tdc_dtype values are numerically aligned with VECR_DT_* (intentional). */
static tdc_dtype vecr_to_tdc(uint8_t dt) { return (tdc_dtype)dt; }

static int vecr_fseek64(FILE *fp, int64_t off, int whence) {
#if defined(_WIN32)
    return _fseeki64(fp, off, whence);
#else
    return fseeko(fp, (off_t)off, whence);
#endif
}

static int64_t vecr_ftell64(FILE *fp) {
#if defined(_WIN32)
    return _ftelli64(fp);
#else
    return (int64_t)ftello(fp);
#endif
}

static void *vecr_realloc_shim(void *user, void *ptr, size_t new_size) {
    (void)user;
    if (new_size == 0) { free(ptr); return NULL; }
    return realloc(ptr, new_size);
}

/* Read a sample at offset `i` from a typed buffer as a double, applying
 * a NaN encoding for "this sample equals nodata". Returns NaN for the
 * "nodata pixel" sentinel, or the sample value otherwise. */
static double vecr_load_double(const void *buf, int64_t i, uint8_t dt) {
    switch (dt) {
    case VECR_DT_I8:  return (double)((const int8_t *)buf)[i];
    case VECR_DT_U8:  return (double)((const uint8_t *)buf)[i];
    case VECR_DT_I16: return (double)((const int16_t *)buf)[i];
    case VECR_DT_U16: return (double)((const uint16_t *)buf)[i];
    case VECR_DT_I32: return (double)((const int32_t *)buf)[i];
    case VECR_DT_U32: return (double)((const uint32_t *)buf)[i];
    case VECR_DT_I64: return (double)((const int64_t *)buf)[i];
    case VECR_DT_U64: return (double)((const uint64_t *)buf)[i];
    case VECR_DT_F32: return (double)((const float *)buf)[i];
    case VECR_DT_F64: return ((const double *)buf)[i];
    default:          return NAN;
    }
}

static void vecr_store_double(void *buf, int64_t i, uint8_t dt, double v) {
    switch (dt) {
    case VECR_DT_I8:  ((int8_t  *)buf)[i] = (int8_t)v; break;
    case VECR_DT_U8:  ((uint8_t *)buf)[i] = (uint8_t)v; break;
    case VECR_DT_I16: ((int16_t *)buf)[i] = (int16_t)v; break;
    case VECR_DT_U16: ((uint16_t*)buf)[i] = (uint16_t)v; break;
    case VECR_DT_I32: ((int32_t *)buf)[i] = (int32_t)v; break;
    case VECR_DT_U32: ((uint32_t*)buf)[i] = (uint32_t)v; break;
    case VECR_DT_I64: ((int64_t *)buf)[i] = (int64_t)v; break;
    case VECR_DT_U64: ((uint64_t*)buf)[i] = (uint64_t)v; break;
    case VECR_DT_F32: ((float   *)buf)[i] = (float)v; break;
    case VECR_DT_F64: ((double  *)buf)[i] = v; break;
    default: break;
    }
}

/* Pack an "is-this-pixel-nodata" check.
 *
 *   For floats: nodata match is exact bit equality on the f32/f64 sample,
 *   PLUS NaN-pixel = NaN-nodata (since NaN never equals itself in fp).
 *   For integers: integer cast of nodata equals the sample.
 *   No nodata recorded: always 0. */
static int vecr_is_nodata(const void *buf, int64_t i, uint8_t dt,
                          int has_nodata, double nodata_val) {
    if (!has_nodata) return 0;
    if (dt == VECR_DT_F64) {
        double v = ((const double *)buf)[i];
        if (isnan(nodata_val)) return isnan(v);
        return v == nodata_val;
    }
    if (dt == VECR_DT_F32) {
        float v = ((const float *)buf)[i];
        if (isnan(nodata_val)) return isnan(v);
        return (double)v == nodata_val;
    }
    /* Integer dtypes: cast nodata to int64 then to the sample's width. */
    int64_t target = (int64_t)nodata_val;
    switch (dt) {
    case VECR_DT_I8:  return ((const int8_t  *)buf)[i] == (int8_t) target;
    case VECR_DT_U8:  return ((const uint8_t *)buf)[i] == (uint8_t)target;
    case VECR_DT_I16: return ((const int16_t *)buf)[i] == (int16_t)target;
    case VECR_DT_U16: return ((const uint16_t*)buf)[i] == (uint16_t)target;
    case VECR_DT_I32: return ((const int32_t *)buf)[i] == (int32_t)target;
    case VECR_DT_U32: return ((const uint32_t*)buf)[i] == (uint32_t)target;
    case VECR_DT_I64: return ((const int64_t *)buf)[i] == target;
    case VECR_DT_U64: return ((const uint64_t*)buf)[i] == (uint64_t)target;
    }
    return 0;
}

/* Pack a dtype-native value into 8 little-endian bytes for index storage. */
static void vecr_pack_native(void *out8, double v, uint8_t dt) {
    uint64_t bits = 0;
    union { double d; uint64_t u; } cv;
    union { float  f; uint32_t u; } cf;
    switch (dt) {
    case VECR_DT_I8:  { int8_t  x = (int8_t)v;  bits = (uint64_t)(uint8_t)x; break; }
    case VECR_DT_U8:  { uint8_t x = (uint8_t)v; bits = (uint64_t)x; break; }
    case VECR_DT_I16: { int16_t x = (int16_t)v; bits = (uint64_t)(uint16_t)x; break; }
    case VECR_DT_U16: { uint16_t x= (uint16_t)v;bits = (uint64_t)x; break; }
    case VECR_DT_I32: { int32_t x = (int32_t)v; bits = (uint64_t)(uint32_t)x; break; }
    case VECR_DT_U32: { uint32_t x= (uint32_t)v;bits = (uint64_t)x; break; }
    case VECR_DT_I64: { int64_t x = (int64_t)v; bits = (uint64_t)x; break; }
    case VECR_DT_U64: { uint64_t x= (uint64_t)v;bits = x; break; }
    case VECR_DT_F32: { cf.f = (float)v; bits = (uint64_t)cf.u; break; }
    case VECR_DT_F64: { cv.d = v; bits = cv.u; break; }
    }
    memcpy(out8, &bits, 8);
}

/* ====================================================================== */
/*  Codec spec selection                                                   */
/* ====================================================================== */

typedef struct {
    tdc_codec_spec    spec;
    tdc_pred2d_params pp;
} VecrCodec;

/* Build a baseline spec: PRED_2D + (ZIGZAG for signed) + (BYTE_SHUFFLE for
 * multi-byte dtypes) + entropy. Returns the same shape regardless of
 * entropy choice — we just swap the last stage for probing. */
static void vecr_codec_pred2d(VecrCodec *c, uint8_t dt, tdc_entropy_id e) {
    memset(c, 0, sizeof(*c));
    c->pp.kind = TDC_PRED2D_AUTO;
    c->spec.model = TDC_MODEL_PRED_2D;
    c->spec.model_params = &c->pp;
    int xi = 0;
    if (vecr_dtype_is_signed(dt)) {
        c->spec.xform[xi++] = TDC_XFORM_ZIGZAG;
    }
    if (vecr_dtype_size(dt) > 1) {
        c->spec.xform[xi++] = TDC_XFORM_BYTE_SHUFFLE;
    }
    c->spec.entropy[0] = e;
}

/* RAW + BYTE_SHUFFLE + entropy. Fallback when the predictor stage itself
 * is hurting (e.g. high-frequency / random data). */
static void vecr_codec_raw(VecrCodec *c, uint8_t dt, tdc_entropy_id e) {
    memset(c, 0, sizeof(*c));
    c->spec.model = TDC_MODEL_RAW;
    if (vecr_dtype_size(dt) > 1) {
        c->spec.xform[0] = TDC_XFORM_BYTE_SHUFFLE;
    }
    c->spec.entropy[0] = e;
}

static void vecr_codec_for(VecrCodec *c, uint8_t dt) {
    vecr_codec_pred2d(c, dt, TDC_ENTROPY_LZ);
}

/* Fill a candidate set for the given compression level. cands must hold at
 * least 8 entries. n_out receives the active count. */
static void vecr_build_candidates(uint8_t dt, int compression,
                                  VecrCodec *cands, int *n_out) {
    int n = 0;
    switch (compression) {
    case VECR_COMPRESS_BALANCED:
        vecr_codec_pred2d(&cands[n++], dt, TDC_ENTROPY_LZ);
        vecr_codec_pred2d(&cands[n++], dt, TDC_ENTROPY_LZ_SPLIT);
        break;
    case VECR_COMPRESS_MAX:
        vecr_codec_pred2d(&cands[n++], dt, TDC_ENTROPY_LZ);
        vecr_codec_pred2d(&cands[n++], dt, TDC_ENTROPY_LZ_SPLIT);
        vecr_codec_pred2d(&cands[n++], dt, TDC_ENTROPY_HUFFMAN4);
        vecr_codec_pred2d(&cands[n++], dt, TDC_ENTROPY_FSE);
        vecr_codec_raw(&cands[n++],    dt, TDC_ENTROPY_LZ);
        vecr_codec_raw(&cands[n++],    dt, TDC_ENTROPY_FSE);
        break;
    case VECR_COMPRESS_FAST:
    default:
        vecr_codec_pred2d(&cands[n++], dt, TDC_ENTROPY_LZ);
        break;
    }
    *n_out = n;
}

/* ====================================================================== */
/*  Writer                                                                 */
/* ====================================================================== */

struct VecrWriter {
    FILE   *fp;
    char    errbuf[256];

    VecrHeader hdr;
    char  **band_names_owned;     /* heap copies for ownership while writing */
    char   *band_names_blob;       /* nul-separated, length = hdr.band_names_size */

    /* Tile grid */
    int64_t tiles_x;
    int64_t tiles_y;

    /* Index accumulator */
    VecrIndexEntry *index;
    int64_t         index_len;
    int64_t         index_cap;

    int compression;   /* VECR_COMPRESS_* */
    int64_t cur_time;  /* time stamp recorded on subsequent tiles; 0 = unset */
    int finished;
};

void vecr_writer_set_compression(VecrWriter *w, int level) {
    if (!w) return;
    if (level != VECR_COMPRESS_BALANCED && level != VECR_COMPRESS_MAX) {
        level = VECR_COMPRESS_FAST;
    }
    w->compression = level;
}

void vecr_writer_set_time(VecrWriter *w, int64_t time) {
    if (w) w->cur_time = time;
}

static void vecr_writer_set_err(VecrWriter *w, const char *msg) {
    if (!w) return;
    snprintf(w->errbuf, sizeof(w->errbuf), "%s", msg);
}

const char *vecr_writer_errmsg(VecrWriter *w) {
    return (w && w->errbuf[0]) ? w->errbuf : "";
}

void vecr_writer_close(VecrWriter *w) {
    if (!w) return;
    if (w->fp) fclose(w->fp);
    free(w->index);
    if (w->band_names_owned) {
        for (int i = 0; i < w->hdr.n_bands; ++i) free(w->band_names_owned[i]);
        free(w->band_names_owned);
    }
    free(w->band_names_blob);
    free(w);
}

int vecr_writer_open(const char *path,
                     int64_t width, int64_t height,
                     int n_bands, uint16_t tile_size,
                     uint8_t sample_dtype,
                     const double *gt,
                     int32_t epsg,
                     double nodata,
                     const char *const *band_names,
                     VecrWriter **out) {
    if (!path || !out) return -1;
    *out = NULL;
    if (width <= 0 || height <= 0 || n_bands <= 0) return -1;
    if (vecr_dtype_size(sample_dtype) == 0) return -1;
    if (tile_size == 0) tile_size = 512;

    VecrWriter *w = (VecrWriter *)calloc(1, sizeof(*w));
    if (!w) return -1;
    *out = w;

    w->fp = fopen(path, "wb");
    if (!w->fp) { vecr_writer_set_err(w, "fopen failed"); return -1; }

    w->hdr.magic        = VECR_MAGIC;
    w->hdr.version      = VECR_VERSION;
    w->hdr.flags        = 0;
    w->hdr.width        = width;
    w->hdr.height       = height;
    w->hdr.n_bands      = n_bands;
    w->hdr.tile_size    = tile_size;
    w->hdr.sample_dtype = sample_dtype;
    w->hdr.n_levels     = 1;

    if (gt) {
        memcpy(w->hdr.geotransform, gt, sizeof(double) * 6);
    } else {
        w->hdr.geotransform[0] = 0.0;
        w->hdr.geotransform[1] = 1.0;
        w->hdr.geotransform[2] = 0.0;
        w->hdr.geotransform[3] = 0.0;
        w->hdr.geotransform[4] = 0.0;
        w->hdr.geotransform[5] = 1.0;
    }

    if (!isnan(nodata)) {
        w->hdr.flags |= VECR_FLAG_HAS_NODATA;
        w->hdr.nodata = nodata;
    } else {
        w->hdr.nodata = NAN;
    }

    if (epsg > 0) {
        w->hdr.flags |= VECR_FLAG_HAS_CRS;
        w->hdr.epsg = epsg;
    }

    /* Band names */
    int have_band_names = 0;
    if (band_names) {
        have_band_names = 1;
        for (int i = 0; i < n_bands; ++i) {
            if (!band_names[i]) { have_band_names = 0; break; }
        }
    }
    if (have_band_names) {
        size_t total = 0;
        for (int i = 0; i < n_bands; ++i) total += strlen(band_names[i]) + 1;
        w->band_names_blob = (char *)malloc(total ? total : 1);
        if (!w->band_names_blob) {
            vecr_writer_set_err(w, "alloc failed for band names");
            return -1;
        }
        size_t off = 0;
        for (int i = 0; i < n_bands; ++i) {
            size_t L = strlen(band_names[i]) + 1;
            memcpy(w->band_names_blob + off, band_names[i], L);
            off += L;
        }
        w->hdr.band_names_size = (uint32_t)total;
        w->hdr.flags |= VECR_FLAG_HAS_BAND_NAMES;
    }

    w->tiles_x = (width  + tile_size - 1) / tile_size;
    w->tiles_y = (height + tile_size - 1) / tile_size;

    /* Reserve header space — finalized in vecr_writer_finish. */
    static const uint8_t zero_hdr[VECR_HEADER_SIZE] = {0};
    if (fwrite(zero_hdr, 1, VECR_HEADER_SIZE, w->fp) != VECR_HEADER_SIZE) {
        vecr_writer_set_err(w, "write header placeholder failed");
        return -1;
    }
    if (w->hdr.band_names_size > 0) {
        if (fwrite(w->band_names_blob, 1, w->hdr.band_names_size, w->fp)
              != w->hdr.band_names_size) {
            vecr_writer_set_err(w, "write band names failed");
            return -1;
        }
    }

    return 0;
}

/* Encode the tile with a single spec into a fresh tdc_buffer. The buffer's
 * .data is owned by the caller (free via free()). On failure, .data is NULL. */
static tdc_status vecr_try_spec(const tdc_block *blk,
                                const tdc_codec_spec *spec,
                                tdc_buffer *out) {
    out->data = NULL;
    out->size = 0;
    out->capacity = 0;
    out->realloc_fn = vecr_realloc_shim;
    out->user = NULL;
    tdc_status st = tdc_encode_block(blk, spec, out);
    if (st != TDC_OK) {
        free(out->data);
        out->data = NULL;
        out->size = 0;
    }
    return st;
}

/* Encode the tile choosing the smallest of `n_specs` candidates. Returns 0
 * on success and writes the winning bytes to *winner_out (caller frees). */
static int vecr_encode_best(VecrWriter *w,
                            const tdc_block *blk,
                            const VecrCodec *candidates, int n_specs,
                            tdc_buffer *winner_out) {
    int best = -1;
    tdc_buffer best_buf = {0};
    for (int i = 0; i < n_specs; ++i) {
        tdc_buffer b = {0};
        tdc_status st = vecr_try_spec(blk, &candidates[i].spec, &b);
        if (st != TDC_OK) continue;
        if (best < 0 || b.size < best_buf.size) {
            free(best_buf.data);
            best_buf = b;
            best = i;
        } else {
            free(b.data);
        }
    }
    if (best < 0) {
        snprintf(w->errbuf, sizeof(w->errbuf),
                 "all codec candidates failed");
        return -1;
    }
    *winner_out = best_buf;
    return 0;
}

/* Encode one tile and append to the file. Updates the index on success. */
static int vecr_writer_emit_tile(VecrWriter *w,
                                 int band_index,
                                 int64_t tx, int64_t ty,
                                 const void *tile_buf,
                                 int64_t tw, int64_t th,
                                 double t_min, double t_max,
                                 int64_t n_valid) {
    uint8_t dt = w->hdr.sample_dtype;

    tdc_block blk = {0};
    blk.data = (void *)tile_buf;
    blk.dtype = vecr_to_tdc(dt);
    blk.layout = TDC_LAYOUT_RASTER_2D;
    blk.shape.rank = 2;
    blk.shape.dim[0] = th;   /* rows (y) */
    blk.shape.dim[1] = tw;   /* cols (x) */
    tdc_shape_set_contiguous(&blk.shape);

    VecrCodec cands[8];
    int n_cands = 0;
    vecr_build_candidates(dt, w->compression, cands, &n_cands);

    tdc_buffer buf = {0};
    if (vecr_encode_best(w, &blk, cands, n_cands, &buf) != 0) {
        return -1;
    }

    int64_t off = vecr_ftell64(w->fp);
    if (off < 0) { free(buf.data); vecr_writer_set_err(w, "ftell failed"); return -1; }
    if (fwrite(buf.data, 1, buf.size, w->fp) != buf.size) {
        free(buf.data);
        vecr_writer_set_err(w, "write tile bytes failed");
        return -1;
    }

    /* Grow index */
    if (w->index_len == w->index_cap) {
        int64_t new_cap = w->index_cap ? w->index_cap * 2 : 256;
        VecrIndexEntry *p = (VecrIndexEntry *)realloc(
            w->index, (size_t)new_cap * sizeof(VecrIndexEntry));
        if (!p) {
            free(buf.data);
            vecr_writer_set_err(w, "alloc failed for index");
            return -1;
        }
        w->index = p;
        w->index_cap = new_cap;
    }

    VecrIndexEntry *e = &w->index[w->index_len++];
    memset(e, 0, sizeof(*e));
    e->level   = 0;
    e->band    = (uint16_t)band_index;
    e->tile_x  = (int32_t)tx;
    e->tile_y  = (int32_t)ty;
    e->offset  = off;
    e->size    = (int64_t)buf.size;
    e->n_valid = n_valid;
    e->time    = w->cur_time;
    if (n_valid > 0) {
        vecr_pack_native(&e->min_bits, t_min, dt);
        vecr_pack_native(&e->max_bits, t_max, dt);
    }

    free(buf.data);
    return 0;
}

int vecr_writer_write_band(VecrWriter *w, int band_index, const void *pixels) {
    if (!w || !pixels) return -1;
    if (band_index < 0 || band_index >= w->hdr.n_bands) {
        vecr_writer_set_err(w, "band_index out of range");
        return -1;
    }

    uint8_t dt = w->hdr.sample_dtype;
    size_t  esz = vecr_dtype_size(dt);
    int     has_nd = (w->hdr.flags & VECR_FLAG_HAS_NODATA) ? 1 : 0;
    double  nd = w->hdr.nodata;
    uint16_t TS = w->hdr.tile_size;
    int64_t W = w->hdr.width, H = w->hdr.height;

    /* Tile scratch — sized once at full TS*TS, shrunk per edge tile. */
    void *tile_buf = malloc((size_t)TS * (size_t)TS * esz);
    if (!tile_buf) { vecr_writer_set_err(w, "alloc tile buf"); return -1; }

    for (int64_t ty = 0; ty < w->tiles_y; ++ty) {
        int64_t r0 = ty * TS;
        int64_t th = (r0 + TS <= H) ? TS : (H - r0);
        for (int64_t tx = 0; tx < w->tiles_x; ++tx) {
            int64_t c0 = tx * TS;
            int64_t tw = (c0 + TS <= W) ? TS : (W - c0);

            /* Copy tile from row-major full raster into tile_buf row-major. */
            for (int64_t r = 0; r < th; ++r) {
                const uint8_t *src = (const uint8_t *)pixels
                    + (size_t)((r0 + r) * W + c0) * esz;
                uint8_t *dst = (uint8_t *)tile_buf + (size_t)(r * tw) * esz;
                memcpy(dst, src, (size_t)tw * esz);
            }

            /* Compute min/max/n_valid over the tile. */
            int64_t n_valid = 0;
            double t_min = INFINITY, t_max = -INFINITY;
            int    saw_finite = 0;
            int64_t n_pix = tw * th;
            for (int64_t i = 0; i < n_pix; ++i) {
                if (vecr_is_nodata(tile_buf, i, dt, has_nd, nd)) continue;
                double v = vecr_load_double(tile_buf, i, dt);
                if (vecr_dtype_is_float(dt) && isnan(v)) continue;
                ++n_valid;
                if (!saw_finite || v < t_min) t_min = v;
                if (!saw_finite || v > t_max) t_max = v;
                saw_finite = 1;
            }

            if (vecr_writer_emit_tile(w, band_index, tx, ty, tile_buf,
                                       tw, th, t_min, t_max, n_valid) != 0) {
                free(tile_buf);
                return -1;
            }
        }
    }

    free(tile_buf);
    return 0;
}

int vecr_writer_finish(VecrWriter *w) {
    if (!w) return -1;
    if (w->finished) return 0;

    /* Write index at current position. */
    int64_t idx_off = vecr_ftell64(w->fp);
    if (idx_off < 0) { vecr_writer_set_err(w, "ftell failed"); return -1; }

    if (w->index_len > 0) {
        size_t bytes = (size_t)w->index_len * sizeof(VecrIndexEntry);
        if (fwrite(w->index, 1, bytes, w->fp) != bytes) {
            vecr_writer_set_err(w, "write index failed");
            return -1;
        }
    }

    w->hdr.index_offset  = idx_off;
    w->hdr.index_size    = w->index_len * VECR_INDEX_ENTRY_SIZE;
    w->hdr.n_tiles_total = w->index_len;

    /* Patch header. */
    if (vecr_fseek64(w->fp, 0, SEEK_SET) != 0) {
        vecr_writer_set_err(w, "seek to header failed");
        return -1;
    }
    if (fwrite(&w->hdr, 1, VECR_HEADER_SIZE, w->fp) != VECR_HEADER_SIZE) {
        vecr_writer_set_err(w, "patch header failed");
        return -1;
    }
    if (fflush(w->fp) != 0) {
        vecr_writer_set_err(w, "fflush failed");
        return -1;
    }
    w->finished = 1;
    return 0;
}

/* ====================================================================== */
/*  Reader                                                                 */
/* ====================================================================== */

struct VecrReader {
    FILE   *fp;
    char    errbuf[256];

    VecrHeader hdr;

    /* Band-name table built from the band-names blob. Indexed [0..n_bands). */
    char  *band_names_blob;
    char **band_names;      /* pointers into band_names_blob; NULL if absent */

    /* Tile index, sorted as written. We also build a per-level 2-D lookup
     * table keyed by (level, band, ty, tx) -> entry to avoid a linear
     * scan per window read. */
    VecrIndexEntry *index;
    int64_t         n_index;

    /* Per-level grid dims and lookup. tiles_x/y are level-L pixel-grid
     * sizes divided by tile_size, rounded up. Layout:
     *   lookup[L][band][ty * tiles_x[L] + tx] -> &index entry.
     */
    int64_t  *tiles_x;        /* length n_levels */
    int64_t  *tiles_y;        /* length n_levels */
    VecrIndexEntry ****lookup; /* [level][band][...] */
};

static void vecr_reader_set_err(VecrReader *r, const char *msg) {
    if (!r) return;
    snprintf(r->errbuf, sizeof(r->errbuf), "%s", msg);
}

const char *vecr_reader_errmsg(VecrReader *r) {
    return (r && r->errbuf[0]) ? r->errbuf : "";
}

void vecr_reader_close(VecrReader *r) {
    if (!r) return;
    if (r->fp) fclose(r->fp);
    free(r->band_names_blob);
    free(r->band_names);
    free(r->index);
    if (r->lookup) {
        for (int L = 0; L < r->hdr.n_levels; ++L) {
            if (!r->lookup[L]) continue;
            for (int b = 0; b < r->hdr.n_bands; ++b) free(r->lookup[L][b]);
            free(r->lookup[L]);
        }
        free(r->lookup);
    }
    free(r->tiles_x);
    free(r->tiles_y);
    free(r);
}

int vecr_reader_open(const char *path, VecrReader **out) {
    if (!path || !out) return -1;
    *out = NULL;
    VecrReader *r = (VecrReader *)calloc(1, sizeof(*r));
    if (!r) return -1;
    *out = r;

    r->fp = fopen(path, "rb");
    if (!r->fp) { vecr_reader_set_err(r, "fopen failed"); return -1; }

    if (fread(&r->hdr, 1, VECR_HEADER_SIZE, r->fp) != VECR_HEADER_SIZE) {
        vecr_reader_set_err(r, "short read on header");
        return -1;
    }
    if (r->hdr.magic != VECR_MAGIC) {
        vecr_reader_set_err(r, "not a VECR file (bad magic)");
        return -1;
    }
    if (r->hdr.version != VECR_VERSION) {
        snprintf(r->errbuf, sizeof(r->errbuf),
                 "unsupported VECR version %u", (unsigned)r->hdr.version);
        return -1;
    }
    if (vecr_dtype_size(r->hdr.sample_dtype) == 0) {
        vecr_reader_set_err(r, "unsupported sample dtype");
        return -1;
    }

    /* Band names */
    if (r->hdr.band_names_size > 0) {
        r->band_names_blob = (char *)malloc(r->hdr.band_names_size + 1);
        if (!r->band_names_blob) { vecr_reader_set_err(r, "alloc band names"); return -1; }
        if (fread(r->band_names_blob, 1, r->hdr.band_names_size, r->fp)
              != r->hdr.band_names_size) {
            vecr_reader_set_err(r, "short read on band names");
            return -1;
        }
        r->band_names_blob[r->hdr.band_names_size] = '\0';

        r->band_names = (char **)calloc((size_t)r->hdr.n_bands, sizeof(char *));
        if (!r->band_names) { vecr_reader_set_err(r, "alloc band name table"); return -1; }
        char *p = r->band_names_blob;
        char *end = r->band_names_blob + r->hdr.band_names_size;
        int b = 0;
        while (p < end && b < r->hdr.n_bands) {
            r->band_names[b++] = p;
            p += strlen(p) + 1;
        }
    }

    /* Index */
    r->n_index = r->hdr.n_tiles_total;
    if (r->n_index > 0) {
        r->index = (VecrIndexEntry *)malloc((size_t)r->n_index *
                                             sizeof(VecrIndexEntry));
        if (!r->index) { vecr_reader_set_err(r, "alloc index"); return -1; }
        if (vecr_fseek64(r->fp, r->hdr.index_offset, SEEK_SET) != 0) {
            vecr_reader_set_err(r, "seek to index failed");
            return -1;
        }
        size_t bytes = (size_t)r->n_index * sizeof(VecrIndexEntry);
        if (fread(r->index, 1, bytes, r->fp) != bytes) {
            vecr_reader_set_err(r, "short read on index");
            return -1;
        }
    }

    /* Build per-level tile-grid sizes and lookups. */
    uint16_t TS = r->hdr.tile_size;
    int n_levels = r->hdr.n_levels;
    if (n_levels < 1) n_levels = 1;

    r->tiles_x = (int64_t *)calloc((size_t)n_levels, sizeof(int64_t));
    r->tiles_y = (int64_t *)calloc((size_t)n_levels, sizeof(int64_t));
    r->lookup  = (VecrIndexEntry ****)calloc((size_t)n_levels,
                                             sizeof(VecrIndexEntry ***));
    if (!r->tiles_x || !r->tiles_y || !r->lookup) {
        vecr_reader_set_err(r, "alloc level tables"); return -1;
    }
    for (int L = 0; L < n_levels; ++L) {
        int64_t lvl_w = (r->hdr.width  + ((int64_t)1 << L) - 1) >> L;
        int64_t lvl_h = (r->hdr.height + ((int64_t)1 << L) - 1) >> L;
        if (lvl_w < 1) lvl_w = 1;
        if (lvl_h < 1) lvl_h = 1;
        r->tiles_x[L] = (lvl_w + TS - 1) / TS;
        r->tiles_y[L] = (lvl_h + TS - 1) / TS;
        int64_t per_band = r->tiles_x[L] * r->tiles_y[L];
        r->lookup[L] = (VecrIndexEntry ***)calloc((size_t)r->hdr.n_bands,
                                                   sizeof(VecrIndexEntry **));
        if (!r->lookup[L]) { vecr_reader_set_err(r, "alloc lookup level"); return -1; }
        for (int b = 0; b < r->hdr.n_bands; ++b) {
            r->lookup[L][b] = (VecrIndexEntry **)calloc((size_t)per_band,
                                                        sizeof(VecrIndexEntry *));
            if (!r->lookup[L][b]) {
                vecr_reader_set_err(r, "alloc lookup band"); return -1;
            }
        }
    }
    for (int64_t i = 0; i < r->n_index; ++i) {
        VecrIndexEntry *e = &r->index[i];
        if (e->level >= n_levels) continue;
        if (e->band >= r->hdr.n_bands) continue;
        if (e->time != 0) continue;   /* timed slices use linear scan */
        int L = e->level;
        if (e->tile_x < 0 || e->tile_x >= r->tiles_x[L]) continue;
        if (e->tile_y < 0 || e->tile_y >= r->tiles_y[L]) continue;
        r->lookup[L][e->band][e->tile_y * r->tiles_x[L] + e->tile_x] = e;
    }
    return 0;
}

int vecr_reader_has_time(VecrReader *r) {
    if (!r) return 0;
    for (int64_t i = 0; i < r->n_index; ++i) {
        if (r->index[i].time != 0) return 1;
    }
    return 0;
}

int64_t       vecr_reader_width(VecrReader *r)    { return r ? r->hdr.width : 0; }
int64_t       vecr_reader_height(VecrReader *r)   { return r ? r->hdr.height : 0; }
int           vecr_reader_nbands(VecrReader *r)   { return r ? r->hdr.n_bands : 0; }
uint16_t      vecr_reader_tile_size(VecrReader *r){ return r ? r->hdr.tile_size : 0; }
uint8_t       vecr_reader_dtype(VecrReader *r)    { return r ? r->hdr.sample_dtype : 0; }
const double *vecr_reader_geotransform(VecrReader *r) {
    return r ? r->hdr.geotransform : NULL;
}
int32_t       vecr_reader_epsg(VecrReader *r) {
    return (r && (r->hdr.flags & VECR_FLAG_HAS_CRS)) ? r->hdr.epsg : 0;
}
double        vecr_reader_nodata(VecrReader *r) {
    return r ? r->hdr.nodata : NAN;
}
int           vecr_reader_has_nodata(VecrReader *r) {
    return (r && (r->hdr.flags & VECR_FLAG_HAS_NODATA)) ? 1 : 0;
}
int           vecr_reader_n_levels(VecrReader *r) {
    return r ? (int)r->hdr.n_levels : 0;
}
const char   *vecr_reader_band_name(VecrReader *r, int band) {
    if (!r || !r->band_names) return NULL;
    if (band < 0 || band >= r->hdr.n_bands) return NULL;
    return r->band_names[band];
}

/* Decode one tile from disk into a tile-sized buffer. Returns 0 on success. */
static int vecr_reader_decode_tile(VecrReader *r,
                                   const VecrIndexEntry *e,
                                   void *out_tile_buf,
                                   int64_t expected_w, int64_t expected_h) {
    if (!e) return -1;
    uint8_t *bytes = (uint8_t *)malloc((size_t)e->size);
    if (!bytes) { vecr_reader_set_err(r, "alloc tile bytes"); return -1; }
    if (vecr_fseek64(r->fp, e->offset, SEEK_SET) != 0) {
        free(bytes);
        vecr_reader_set_err(r, "seek to tile failed");
        return -1;
    }
    if (fread(bytes, 1, (size_t)e->size, r->fp) != (size_t)e->size) {
        free(bytes);
        vecr_reader_set_err(r, "short read on tile");
        return -1;
    }

    tdc_block dst = {0};
    dst.data   = out_tile_buf;
    dst.dtype  = vecr_to_tdc(r->hdr.sample_dtype);
    dst.layout = TDC_LAYOUT_RASTER_2D;
    dst.shape.rank = 2;
    dst.shape.dim[0] = expected_h;
    dst.shape.dim[1] = expected_w;
    tdc_shape_set_contiguous(&dst.shape);

    tdc_status st = tdc_decode_block_into(bytes, (size_t)e->size, &dst);
    free(bytes);
    if (st != TDC_OK) {
        snprintf(r->errbuf, sizeof(r->errbuf),
                 "tdc_decode_block_into failed (status=%d, tile=(%d,%d))",
                 (int)st, e->tile_x, e->tile_y);
        return -1;
    }
    return 0;
}

/* Core window-decode loop. `entries` is a row-major array of size
 * (ty_hi-ty_lo+1)*(tx_hi-tx_lo+1) holding pointers into r->index for the
 * relevant tiles (NULL for missing tiles, which stay nodata). The output
 * buffer must already be filled with the nodata pattern. */
static int vecr_reader_decode_window_entries(
    VecrReader *r, int band, int64_t level,
    int64_t col_min, int64_t row_min,
    int64_t col_max, int64_t row_max,
    int64_t cmin, int64_t rmin, int64_t cmax, int64_t rmax,
    int64_t tx_lo, int64_t tx_hi, int64_t ty_lo, int64_t ty_hi,
    VecrIndexEntry **entries,
    void *out) {

    uint8_t  dt  = r->hdr.sample_dtype;
    size_t   esz = vecr_dtype_size(dt);
    uint16_t TS  = r->hdr.tile_size;
    int64_t W = (r->hdr.width  + ((int64_t)1 << level) - 1) >> level;
    int64_t H = (r->hdr.height + ((int64_t)1 << level) - 1) >> level;
    if (W < 1) W = 1;
    if (H < 1) H = 1;
    int64_t out_w = col_max - col_min + 1;
    int64_t n_tx = tx_hi - tx_lo + 1;

    int64_t n_tiles = n_tx * (ty_hi - ty_lo + 1);
    int n_threads = vec_omp_threads();
    if (n_threads > n_tiles) n_threads = (int)n_tiles;
    if (n_threads < 1) n_threads = 1;

    int rc_global = 0;
    (void)band; (void)level;  /* used below when fetching from entries[] */

#ifdef _OPENMP
    #pragma omp parallel num_threads(n_threads)
#endif
    {
        void *tile_buf = malloc((size_t)TS * (size_t)TS * esz);
        if (!tile_buf) {
#ifdef _OPENMP
            #pragma omp atomic write
#endif
            rc_global = -1;
        } else {
#ifdef _OPENMP
            #pragma omp for collapse(2) schedule(dynamic) nowait
#endif
            for (int64_t ty = ty_lo; ty <= ty_hi; ++ty) {
                for (int64_t tx = tx_lo; tx <= tx_hi; ++tx) {
                    if (rc_global) continue;
                    int64_t tile_r0 = ty * TS;
                    int64_t th = (tile_r0 + TS <= H) ? TS : (H - tile_r0);
                    int64_t tile_c0 = tx * TS;
                    int64_t tw = (tile_c0 + TS <= W) ? TS : (W - tile_c0);

                    VecrIndexEntry *e = entries[(ty - ty_lo) * n_tx + (tx - tx_lo)];
                    if (!e) continue;

                    uint8_t *bytes = (uint8_t *)malloc((size_t)e->size);
                    if (!bytes) {
#ifdef _OPENMP
                        #pragma omp atomic write
#endif
                        rc_global = -1;
                        continue;
                    }
                    int io_ok = 1;
#ifdef _OPENMP
                    #pragma omp critical (vecr_fread)
#endif
                    {
                        if (vecr_fseek64(r->fp, e->offset, SEEK_SET) != 0 ||
                            fread(bytes, 1, (size_t)e->size, r->fp)
                                != (size_t)e->size) {
                            io_ok = 0;
                        }
                    }
                    if (!io_ok) {
                        free(bytes);
#ifdef _OPENMP
                        #pragma omp atomic write
#endif
                        rc_global = -1;
                        continue;
                    }

                    tdc_block dst = {0};
                    dst.data = tile_buf;
                    dst.dtype = vecr_to_tdc(dt);
                    dst.layout = TDC_LAYOUT_RASTER_2D;
                    dst.shape.rank = 2;
                    dst.shape.dim[0] = th;
                    dst.shape.dim[1] = tw;
                    tdc_shape_set_contiguous(&dst.shape);
                    tdc_status st = tdc_decode_block_into(bytes,
                                                          (size_t)e->size, &dst);
                    free(bytes);
                    if (st != TDC_OK) {
#ifdef _OPENMP
                        #pragma omp atomic write
#endif
                        rc_global = -1;
                        continue;
                    }

                    int64_t r_lo = tile_r0 > rmin ? tile_r0 : rmin;
                    int64_t r_hi = (tile_r0 + th - 1) < rmax ? (tile_r0 + th - 1) : rmax;
                    int64_t c_lo = tile_c0 > cmin ? tile_c0 : cmin;
                    int64_t c_hi = (tile_c0 + tw - 1) < cmax ? (tile_c0 + tw - 1) : cmax;
                    int64_t copy_h = r_hi - r_lo + 1;
                    int64_t copy_w = c_hi - c_lo + 1;
                    for (int64_t rr = 0; rr < copy_h; ++rr) {
                        int64_t src_r = (r_lo - tile_r0) + rr;
                        int64_t dst_r = (r_lo - row_min) + rr;
                        int64_t src_c = c_lo - tile_c0;
                        int64_t dst_c = c_lo - col_min;
                        const uint8_t *src = (const uint8_t *)tile_buf
                            + (size_t)(src_r * tw + src_c) * esz;
                        uint8_t *dout = (uint8_t *)out
                            + (size_t)(dst_r * out_w + dst_c) * esz;
                        memcpy(dout, src, (size_t)copy_w * esz);
                    }
                }
            }
            free(tile_buf);
        }
    }
    return rc_global;
}

int vecr_reader_read_window(VecrReader *r,
                            int band, uint8_t level,
                            int64_t col_min, int64_t row_min,
                            int64_t col_max, int64_t row_max,
                            void *out) {
    if (!r || !out) return -1;
    if (band < 0 || band >= r->hdr.n_bands) {
        vecr_reader_set_err(r, "band out of range"); return -1;
    }
    if (level >= r->hdr.n_levels) {
        snprintf(r->errbuf, sizeof(r->errbuf),
                 "level %u out of range (n_levels=%u)",
                 (unsigned)level, (unsigned)r->hdr.n_levels);
        return -1;
    }
    if (col_min > col_max || row_min > row_max) {
        vecr_reader_set_err(r, "empty window");
        return -1;
    }

    uint8_t  dt  = r->hdr.sample_dtype;
    size_t   esz = vecr_dtype_size(dt);
    int      has_nd = (r->hdr.flags & VECR_FLAG_HAS_NODATA) ? 1 : 0;
    double   nd  = r->hdr.nodata;
    uint16_t TS  = r->hdr.tile_size;
    /* Per-level pixel-grid dimensions: ceil(width / 2^level). */
    int64_t  W = (r->hdr.width  + ((int64_t)1 << level) - 1) >> level;
    int64_t  H = (r->hdr.height + ((int64_t)1 << level) - 1) >> level;
    if (W < 1) W = 1;
    if (H < 1) H = 1;

    int64_t out_w = col_max - col_min + 1;
    int64_t out_h = row_max - row_min + 1;
    int64_t out_n = out_w * out_h;

    /* Pre-fill output with nodata (NaN for floats with no recorded nodata). */
    double fill = has_nd ? nd : NAN;
    if (vecr_dtype_is_float(dt)) {
        if (dt == VECR_DT_F32) {
            float ff = (float)fill;
            float *p = (float *)out;
            for (int64_t i = 0; i < out_n; ++i) p[i] = ff;
        } else {
            double *p = (double *)out;
            for (int64_t i = 0; i < out_n; ++i) p[i] = fill;
        }
    } else {
        /* Integer: fill with cast of nodata if set; else 0. */
        if (has_nd) {
            for (int64_t i = 0; i < out_n; ++i) {
                vecr_store_double(out, i, dt, fill);
            }
        } else {
            memset(out, 0, (size_t)out_n * esz);
        }
    }

    /* Intersect window with raster extent. */
    int64_t cmin = col_min < 0 ? 0 : col_min;
    int64_t rmin = row_min < 0 ? 0 : row_min;
    int64_t cmax = col_max >= W ? W - 1 : col_max;
    int64_t rmax = row_max >= H ? H - 1 : row_max;
    if (cmin > cmax || rmin > rmax) return 0;  /* fully outside; out is nodata */

    int64_t tx_lo = cmin / TS, tx_hi = cmax / TS;
    int64_t ty_lo = rmin / TS, ty_hi = rmax / TS;
    int64_t n_tx = tx_hi - tx_lo + 1;
    int64_t n_ty = ty_hi - ty_lo + 1;
    int64_t n_entries = n_tx * n_ty;

    VecrIndexEntry **entries = (VecrIndexEntry **)calloc((size_t)n_entries,
                                                          sizeof(VecrIndexEntry *));
    if (!entries) { vecr_reader_set_err(r, "alloc entries"); return -1; }

    for (int64_t ty = ty_lo; ty <= ty_hi; ++ty) {
        for (int64_t tx = tx_lo; tx <= tx_hi; ++tx) {
            entries[(ty - ty_lo) * n_tx + (tx - tx_lo)] =
                r->lookup[level][band][ty * r->tiles_x[level] + tx];
        }
    }

    int rc = vecr_reader_decode_window_entries(
        r, band, level,
        col_min, row_min, col_max, row_max,
        cmin, rmin, cmax, rmax,
        tx_lo, tx_hi, ty_lo, ty_hi,
        entries, out);
    free(entries);
    if (rc != 0) {
        vecr_reader_set_err(r, "tile decode failed");
        return -1;
    }
    return 0;
}

int vecr_reader_read_window_t(VecrReader *r,
                              int band, uint8_t level,
                              int64_t time,
                              int64_t col_min, int64_t row_min,
                              int64_t col_max, int64_t row_max,
                              void *out) {
    if (!r || !out) return -1;
    if (band < 0 || band >= r->hdr.n_bands) {
        vecr_reader_set_err(r, "band out of range"); return -1;
    }
    if (level >= r->hdr.n_levels) {
        vecr_reader_set_err(r, "level out of range"); return -1;
    }
    if (col_min > col_max || row_min > row_max) {
        vecr_reader_set_err(r, "empty window"); return -1;
    }

    /* Mirror read_window's intersection / nodata-fill / tile-range setup,
     * then build the entries[] from a linear scan of the index. */
    uint8_t  dt  = r->hdr.sample_dtype;
    size_t   esz = vecr_dtype_size(dt);
    int      has_nd = (r->hdr.flags & VECR_FLAG_HAS_NODATA) ? 1 : 0;
    double   nd  = r->hdr.nodata;
    uint16_t TS  = r->hdr.tile_size;
    int64_t  W = (r->hdr.width  + ((int64_t)1 << level) - 1) >> level;
    int64_t  H = (r->hdr.height + ((int64_t)1 << level) - 1) >> level;
    if (W < 1) W = 1;
    if (H < 1) H = 1;

    int64_t out_w = col_max - col_min + 1;
    int64_t out_h = row_max - row_min + 1;
    int64_t out_n = out_w * out_h;

    double fill = has_nd ? nd : NAN;
    if (vecr_dtype_is_float(dt)) {
        if (dt == VECR_DT_F32) {
            float ff = (float)fill;
            float *p = (float *)out;
            for (int64_t i = 0; i < out_n; ++i) p[i] = ff;
        } else {
            double *p = (double *)out;
            for (int64_t i = 0; i < out_n; ++i) p[i] = fill;
        }
    } else if (has_nd) {
        for (int64_t i = 0; i < out_n; ++i) vecr_store_double(out, i, dt, fill);
    } else {
        memset(out, 0, (size_t)out_n * esz);
    }

    int64_t cmin = col_min < 0 ? 0 : col_min;
    int64_t rmin = row_min < 0 ? 0 : row_min;
    int64_t cmax = col_max >= W ? W - 1 : col_max;
    int64_t rmax = row_max >= H ? H - 1 : row_max;
    if (cmin > cmax || rmin > rmax) return 0;

    int64_t tx_lo = cmin / TS, tx_hi = cmax / TS;
    int64_t ty_lo = rmin / TS, ty_hi = rmax / TS;
    int64_t n_tx = tx_hi - tx_lo + 1;
    int64_t n_ty = ty_hi - ty_lo + 1;
    int64_t n_entries = n_tx * n_ty;

    VecrIndexEntry **entries = (VecrIndexEntry **)calloc((size_t)n_entries,
                                                          sizeof(VecrIndexEntry *));
    if (!entries) { vecr_reader_set_err(r, "alloc entries"); return -1; }

    int found_any = 0;
    for (int64_t i = 0; i < r->n_index; ++i) {
        VecrIndexEntry *e = &r->index[i];
        if (e->level != level) continue;
        if ((int)e->band != band) continue;
        if (e->time != time) continue;
        if (e->tile_x < tx_lo || e->tile_x > tx_hi) continue;
        if (e->tile_y < ty_lo || e->tile_y > ty_hi) continue;
        entries[(e->tile_y - ty_lo) * n_tx + (e->tile_x - tx_lo)] = e;
        found_any = 1;
    }
    if (!found_any) {
        free(entries);
        snprintf(r->errbuf, sizeof(r->errbuf),
                 "no tiles match band=%d level=%u time=%lld",
                 band, (unsigned)level, (long long)time);
        return -1;
    }

    int rc = vecr_reader_decode_window_entries(
        r, band, level,
        col_min, row_min, col_max, row_max,
        cmin, rmin, cmax, rmax,
        tx_lo, tx_hi, ty_lo, ty_hi,
        entries, out);
    free(entries);
    if (rc != 0) {
        vecr_reader_set_err(r, "tile decode failed");
        return -1;
    }
    return 0;
}

/* Map (x, y) in CRS to (col, row) using the geotransform.
 *   col = (x - gt[0]) / gt[1]
 *   row = (y - gt[3]) / gt[5]
 * (rotation terms gt[2]/gt[4] currently assumed zero — same simplification
 * as tiff_format.c/tiff_reader_extract_points). */
static int vecr_xy_to_rc(const double *gt, double x, double y,
                         int64_t *col, int64_t *row) {
    if (gt[1] == 0.0 || gt[5] == 0.0) return -1;
    double cf = (x - gt[0]) / gt[1];
    double rf = (y - gt[3]) / gt[5];
    *col = (int64_t)floor(cf);
    *row = (int64_t)floor(rf);
    return 0;
}

int vecr_reader_extract_points(VecrReader *r, int band,
                               int64_t n_points,
                               const double *xs, const double *ys,
                               double *out) {
    if (!r || !xs || !ys || !out) return -1;
    if (band < 0 || band >= r->hdr.n_bands) {
        vecr_reader_set_err(r, "band out of range"); return -1;
    }
    uint8_t  dt = r->hdr.sample_dtype;
    size_t   esz = vecr_dtype_size(dt);
    int      has_nd = (r->hdr.flags & VECR_FLAG_HAS_NODATA) ? 1 : 0;
    double   nd  = r->hdr.nodata;
    uint16_t TS = r->hdr.tile_size;
    int64_t  W = r->hdr.width, H = r->hdr.height;
    const double *gt = r->hdr.geotransform;

    /* Iterate points; cache the most recently decoded tile to amortize
     * decode cost when many points fall in the same tile. */
    int64_t cached_tx = -1, cached_ty = -1;
    int64_t cached_tw = 0, cached_th = 0;
    void *tile_buf = malloc((size_t)TS * (size_t)TS * esz);
    if (!tile_buf) { vecr_reader_set_err(r, "alloc tile buf"); return -1; }

    for (int64_t i = 0; i < n_points; ++i) {
        int64_t col, row;
        if (vecr_xy_to_rc(gt, xs[i], ys[i], &col, &row) != 0) {
            out[i] = NAN; continue;
        }
        if (col < 0 || col >= W || row < 0 || row >= H) {
            out[i] = NAN; continue;
        }

        int64_t tx = col / TS, ty = row / TS;
        if (tx != cached_tx || ty != cached_ty) {
            VecrIndexEntry *e = r->lookup[0][band][ty * r->tiles_x[0] + tx];
            if (!e) {
                out[i] = NAN;
                cached_tx = -1; cached_ty = -1;
                continue;
            }
            int64_t tile_c0 = tx * TS, tile_r0 = ty * TS;
            cached_tw = (tile_c0 + TS <= W) ? TS : (W - tile_c0);
            cached_th = (tile_r0 + TS <= H) ? TS : (H - tile_r0);
            if (vecr_reader_decode_tile(r, e, tile_buf, cached_tw, cached_th) != 0) {
                free(tile_buf);
                return -1;
            }
            cached_tx = tx; cached_ty = ty;
        }

        int64_t lc = col - cached_tx * TS;
        int64_t lr = row - cached_ty * TS;
        int64_t k  = lr * cached_tw + lc;
        if (vecr_is_nodata(tile_buf, k, dt, has_nd, nd)) {
            out[i] = NAN;
        } else {
            double v = vecr_load_double(tile_buf, k, dt);
            if (vecr_dtype_is_float(dt) && isnan(v)) out[i] = NAN;
            else                                     out[i] = v;
        }
    }

    free(tile_buf);
    return 0;
}

/* ====================================================================== */
/*  Overviews                                                              */
/* ====================================================================== */

static double vecr_resample_2x2_avg(const double *src, int64_t W, int64_t H,
                                     int64_t r, int64_t c) {
    int64_t r2 = r * 2, c2 = c * 2;
    double acc = 0; int n = 0;
    for (int dr = 0; dr < 2; ++dr) {
        for (int dc = 0; dc < 2; ++dc) {
            int64_t rr = r2 + dr, cc = c2 + dc;
            if (rr >= H || cc >= W) continue;
            double v = src[rr * W + cc];
            if (isnan(v)) continue;
            acc += v; ++n;
        }
    }
    return n > 0 ? acc / n : NAN;
}

static double vecr_resample_2x2_nearest(const double *src, int64_t W, int64_t H,
                                         int64_t r, int64_t c) {
    int64_t r2 = r * 2, c2 = c * 2;
    if (r2 >= H || c2 >= W) return NAN;
    return src[r2 * W + c2];
}

static double vecr_resample_2x2_mode(const double *src, int64_t W, int64_t H,
                                      int64_t r, int64_t c) {
    int64_t r2 = r * 2, c2 = c * 2;
    double vals[4]; int counts[4]; int n = 0;
    for (int dr = 0; dr < 2; ++dr) {
        for (int dc = 0; dc < 2; ++dc) {
            int64_t rr = r2 + dr, cc = c2 + dc;
            if (rr >= H || cc >= W) continue;
            double v = src[rr * W + cc];
            if (isnan(v)) continue;
            int found = 0;
            for (int i = 0; i < n; ++i) {
                if (vals[i] == v) { ++counts[i]; found = 1; break; }
            }
            if (!found) { vals[n] = v; counts[n] = 1; ++n; }
        }
    }
    if (n == 0) return NAN;
    int best = 0;
    for (int i = 1; i < n; ++i) {
        if (counts[i] > counts[best]) best = i;
    }
    return vals[best];
}

/* Bilinear-style: 3x3 weighted [1,2,1; 2,4,2; 1,2,1]/16 anchored at the
 * source pixel that maps to (r, c) at the output, then 2x decimate. Skips
 * NaN cells in the weighted sum and renormalises by the active weight. */
static double vecr_resample_2x2_bilinear(const double *src, int64_t W, int64_t H,
                                          int64_t r, int64_t c) {
    int64_t r2 = r * 2, c2 = c * 2;
    static const int wgrid[3][3] = {{1, 2, 1}, {2, 4, 2}, {1, 2, 1}};
    double acc = 0; int wtot = 0;
    for (int dr = -1; dr <= 1; ++dr) {
        for (int dc = -1; dc <= 1; ++dc) {
            int64_t rr = r2 + dr, cc = c2 + dc;
            if (rr < 0 || rr >= H || cc < 0 || cc >= W) continue;
            double v = src[rr * W + cc];
            if (isnan(v)) continue;
            int w = wgrid[dr + 1][dc + 1];
            acc += v * w; wtot += w;
        }
    }
    if (wtot == 0) return NAN;
    return acc / wtot;
}

typedef double (*VecrResampleFn)(const double *src, int64_t W, int64_t H,
                                  int64_t r, int64_t c);

static VecrResampleFn vecr_resample_fn(int kind) {
    switch (kind) {
    case VECR_RESAMPLE_NEAREST:  return vecr_resample_2x2_nearest;
    case VECR_RESAMPLE_AVERAGE:  return vecr_resample_2x2_avg;
    case VECR_RESAMPLE_BILINEAR: return vecr_resample_2x2_bilinear;
    case VECR_RESAMPLE_MODE:     return vecr_resample_2x2_mode;
    case VECR_RESAMPLE_GAUSS:    return vecr_resample_2x2_bilinear;  /* alias */
    }
    return vecr_resample_2x2_avg;
}

/* Decode the full level-0 raster of one band as doubles (NaN for nodata). */
static double *vecr_decode_band_doubles(VecrReader *r, int band) {
    int64_t W = r->hdr.width, H = r->hdr.height;
    double *out = (double *)malloc((size_t)W * (size_t)H * sizeof(double));
    if (!out) return NULL;
    uint8_t  dt  = r->hdr.sample_dtype;
    size_t   esz = vecr_dtype_size(dt);
    void *raw = malloc((size_t)W * (size_t)H * esz);
    if (!raw) { free(out); return NULL; }
    if (vecr_reader_read_window(r, band, 0, 0, 0, W - 1, H - 1, raw) != 0) {
        free(raw); free(out); return NULL;
    }
    int has_nd = (r->hdr.flags & VECR_FLAG_HAS_NODATA) ? 1 : 0;
    double nd = r->hdr.nodata;
    int64_t n = W * H;
    for (int64_t i = 0; i < n; ++i) {
        if (vecr_is_nodata(raw, i, dt, has_nd, nd)) { out[i] = NAN; continue; }
        double v = vecr_load_double(raw, i, dt);
        out[i] = (vecr_dtype_is_float(dt) && isnan(v)) ? NAN : v;
    }
    free(raw);
    return out;
}

static void vecr_doubles_to_dtype_with_nodata(const double *src, int64_t n,
                                               uint8_t dt,
                                               int has_nd, double nd,
                                               void *dst) {
    for (int64_t i = 0; i < n; ++i) {
        double v = src[i];
        double w = isnan(v) ? (has_nd ? nd : (vecr_dtype_is_float(dt) ? NAN : 0))
                            : v;
        vecr_store_double(dst, i, dt, w);
    }
}

int vecr_build_overviews(const char *path,
                         int n_levels,
                         int resampling,
                         int compression,
                         char *errbuf, size_t errbuf_size) {
    if (!path) {
        if (errbuf && errbuf_size) snprintf(errbuf, errbuf_size, "null path");
        return -1;
    }
    if (n_levels < 2 || n_levels > 16) {
        if (errbuf && errbuf_size)
            snprintf(errbuf, errbuf_size,
                     "n_levels must be in [2..16], got %d", n_levels);
        return -1;
    }
    if (compression < 0) compression = VECR_COMPRESS_FAST;

    VecrResampleFn kernel = vecr_resample_fn(resampling);

    /* Pull existing state via the reader, copy what we need, then close. */
    VecrReader *r = NULL;
    double **band_l0 = NULL;
    VecrIndexEntry *old_index = NULL;
    int64_t n_old_index = 0;
    VecrHeader hdr;
    int n_bands = 0;

    if (vecr_reader_open(path, &r) != 0) {
        const char *msg = r ? vecr_reader_errmsg(r) : "open failed";
        if (errbuf && errbuf_size) snprintf(errbuf, errbuf_size, "%s", msg);
        vecr_reader_close(r);
        return -1;
    }
    hdr = r->hdr;
    if (hdr.n_levels >= n_levels) {
        if (errbuf && errbuf_size)
            snprintf(errbuf, errbuf_size,
                     "file already has %u levels; requested %d",
                     (unsigned)hdr.n_levels, n_levels);
        vecr_reader_close(r);
        return -1;
    }
    n_bands = hdr.n_bands;
    band_l0 = (double **)calloc((size_t)n_bands, sizeof(double *));
    if (!band_l0) { vecr_reader_close(r); return -1; }
    for (int b = 0; b < n_bands; ++b) {
        band_l0[b] = vecr_decode_band_doubles(r, b);
        if (!band_l0[b]) {
            if (errbuf && errbuf_size)
                snprintf(errbuf, errbuf_size, "failed to decode level 0 band %d", b);
            for (int j = 0; j < n_bands; ++j) free(band_l0[j]);
            free(band_l0);
            vecr_reader_close(r);
            return -1;
        }
    }
    n_old_index = r->n_index;
    old_index = (VecrIndexEntry *)malloc((size_t)n_old_index * sizeof(*old_index));
    if (!old_index) {
        for (int b = 0; b < n_bands; ++b) free(band_l0[b]);
        free(band_l0); vecr_reader_close(r); return -1;
    }
    memcpy(old_index, r->index, (size_t)n_old_index * sizeof(*old_index));
    vecr_reader_close(r);

    FILE *fp = fopen(path, "r+b");
    if (!fp) {
        if (errbuf && errbuf_size) snprintf(errbuf, errbuf_size, "fopen r+b failed");
        for (int b = 0; b < n_bands; ++b) free(band_l0[b]);
        free(band_l0); free(old_index); return -1;
    }
    if (vecr_fseek64(fp, hdr.index_offset, SEEK_SET) != 0) {
        if (errbuf && errbuf_size) snprintf(errbuf, errbuf_size, "seek failed");
        fclose(fp);
        for (int b = 0; b < n_bands; ++b) free(band_l0[b]);
        free(band_l0); free(old_index); return -1;
    }

    uint8_t  dt  = hdr.sample_dtype;
    size_t   esz = vecr_dtype_size(dt);
    int      has_nd = (hdr.flags & VECR_FLAG_HAS_NODATA) ? 1 : 0;
    double   nd  = hdr.nodata;
    uint16_t TS  = hdr.tile_size;

    int64_t cap_new = 256;
    VecrIndexEntry *new_index = (VecrIndexEntry *)malloc((size_t)cap_new * sizeof(*new_index));
    int64_t n_new = 0;
    void *tile_native = malloc((size_t)TS * (size_t)TS * esz);
    double *tile_dbl = (double *)malloc((size_t)TS * (size_t)TS * sizeof(double));
    if (!new_index || !tile_native || !tile_dbl) {
        free(new_index); free(tile_native); free(tile_dbl);
        fclose(fp);
        for (int b = 0; b < n_bands; ++b) free(band_l0[b]);
        free(band_l0); free(old_index);
        if (errbuf && errbuf_size) snprintf(errbuf, errbuf_size, "alloc failed");
        return -1;
    }

    int rc = 0;
    int64_t W_prev = hdr.width, H_prev = hdr.height;
    for (int L = 1; L < n_levels && rc == 0; ++L) {
        int64_t W_cur = (W_prev + 1) / 2;
        int64_t H_cur = (H_prev + 1) / 2;
        int64_t tx_n = (W_cur + TS - 1) / TS;
        int64_t ty_n = (H_cur + TS - 1) / TS;

        for (int b = 0; b < n_bands && rc == 0; ++b) {
            double *cur = (double *)malloc((size_t)W_cur * (size_t)H_cur * sizeof(double));
            if (!cur) { rc = -1; break; }
            for (int64_t r2 = 0; r2 < H_cur; ++r2) {
                for (int64_t c2 = 0; c2 < W_cur; ++c2) {
                    cur[r2 * W_cur + c2] = kernel(band_l0[b], W_prev, H_prev, r2, c2);
                }
            }

            VecrCodec cands[8];
            int n_cands = 0;
            vecr_build_candidates(dt, compression, cands, &n_cands);

            for (int64_t ty = 0; ty < ty_n && rc == 0; ++ty) {
                int64_t r0 = ty * TS;
                int64_t th = (r0 + TS <= H_cur) ? TS : (H_cur - r0);
                for (int64_t tx = 0; tx < tx_n && rc == 0; ++tx) {
                    int64_t c0 = tx * TS;
                    int64_t tw = (c0 + TS <= W_cur) ? TS : (W_cur - c0);

                    for (int64_t rr = 0; rr < th; ++rr) {
                        memcpy(tile_dbl + rr * tw,
                               cur + (r0 + rr) * W_cur + c0,
                               (size_t)tw * sizeof(double));
                    }

                    int64_t n_pix = tw * th;
                    int64_t n_valid = 0;
                    double t_min = INFINITY, t_max = -INFINITY;
                    int saw = 0;
                    for (int64_t i = 0; i < n_pix; ++i) {
                        double v = tile_dbl[i];
                        if (isnan(v)) continue;
                        ++n_valid;
                        if (!saw || v < t_min) t_min = v;
                        if (!saw || v > t_max) t_max = v;
                        saw = 1;
                    }
                    vecr_doubles_to_dtype_with_nodata(tile_dbl, n_pix, dt,
                                                      has_nd, nd, tile_native);

                    tdc_block blk = {0};
                    blk.data = tile_native;
                    blk.dtype = vecr_to_tdc(dt);
                    blk.layout = TDC_LAYOUT_RASTER_2D;
                    blk.shape.rank = 2;
                    blk.shape.dim[0] = th;
                    blk.shape.dim[1] = tw;
                    tdc_shape_set_contiguous(&blk.shape);

                    int best = -1;
                    tdc_buffer best_buf = {0};
                    for (int i = 0; i < n_cands; ++i) {
                        tdc_buffer bb = {0};
                        tdc_status st = vecr_try_spec(&blk, &cands[i].spec, &bb);
                        if (st != TDC_OK) continue;
                        if (best < 0 || bb.size < best_buf.size) {
                            free(best_buf.data);
                            best_buf = bb;
                            best = i;
                        } else {
                            free(bb.data);
                        }
                    }
                    if (best < 0) { rc = -1; break; }

                    int64_t off = vecr_ftell64(fp);
                    if (off < 0 ||
                        fwrite(best_buf.data, 1, best_buf.size, fp) != best_buf.size) {
                        free(best_buf.data); rc = -1; break;
                    }

                    if (n_new == cap_new) {
                        cap_new *= 2;
                        VecrIndexEntry *p = (VecrIndexEntry *)realloc(
                            new_index, (size_t)cap_new * sizeof(*new_index));
                        if (!p) { free(best_buf.data); rc = -1; break; }
                        new_index = p;
                    }
                    VecrIndexEntry *e = &new_index[n_new++];
                    memset(e, 0, sizeof(*e));
                    e->level   = (uint8_t)L;
                    e->band    = (uint16_t)b;
                    e->tile_x  = (int32_t)tx;
                    e->tile_y  = (int32_t)ty;
                    e->offset  = off;
                    e->size    = (int64_t)best_buf.size;
                    e->n_valid = n_valid;
                    if (n_valid > 0) {
                        vecr_pack_native(&e->min_bits, t_min, dt);
                        vecr_pack_native(&e->max_bits, t_max, dt);
                    }
                    free(best_buf.data);
                }
            }

            free(band_l0[b]);
            band_l0[b] = cur;
        }
        W_prev = W_cur;
        H_prev = H_cur;
    }

    free(tile_dbl);
    free(tile_native);
    for (int b = 0; b < n_bands; ++b) free(band_l0[b]);
    free(band_l0);

    if (rc != 0) {
        if (errbuf && errbuf_size) snprintf(errbuf, errbuf_size, "tile encode failed");
        free(new_index); free(old_index); fclose(fp); return -1;
    }

    int64_t new_index_off = vecr_ftell64(fp);
    if (new_index_off < 0) {
        if (errbuf && errbuf_size) snprintf(errbuf, errbuf_size, "ftell failed");
        free(new_index); free(old_index); fclose(fp); return -1;
    }
    if (n_old_index > 0 &&
        fwrite(old_index, 1, (size_t)n_old_index * sizeof(VecrIndexEntry), fp)
            != (size_t)n_old_index * sizeof(VecrIndexEntry)) {
        if (errbuf && errbuf_size) snprintf(errbuf, errbuf_size, "write old index failed");
        free(new_index); free(old_index); fclose(fp); return -1;
    }
    if (n_new > 0 &&
        fwrite(new_index, 1, (size_t)n_new * sizeof(VecrIndexEntry), fp)
            != (size_t)n_new * sizeof(VecrIndexEntry)) {
        if (errbuf && errbuf_size) snprintf(errbuf, errbuf_size, "write new index failed");
        free(new_index); free(old_index); fclose(fp); return -1;
    }
    free(new_index);
    free(old_index);

    hdr.n_levels      = (uint8_t)n_levels;
    hdr.index_offset  = new_index_off;
    hdr.n_tiles_total = n_old_index + n_new;
    hdr.index_size    = hdr.n_tiles_total * VECR_INDEX_ENTRY_SIZE;

    if (vecr_fseek64(fp, 0, SEEK_SET) != 0 ||
        fwrite(&hdr, 1, VECR_HEADER_SIZE, fp) != VECR_HEADER_SIZE ||
        fflush(fp) != 0) {
        if (errbuf && errbuf_size) snprintf(errbuf, errbuf_size, "patch header failed");
        fclose(fp); return -1;
    }
    fclose(fp);
    return 0;
}
