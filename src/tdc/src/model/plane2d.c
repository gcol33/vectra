/*
 * src/model/plane2d.c
 *
 * TDC_MODEL_PLANE_2D — per-tile least-squares plane fit predictor for
 * RASTER_2D blocks.
 *
 * For each tile_size x tile_size tile of the raster, fit
 *
 *     pred(lx, ly) = a + b * lx + c * ly      (lx,ly = local tile coords)
 *
 * by solving the closed-form 3x3 normal equations
 *
 *     [ s_1   s_x   s_y  ] [a]   [s_v ]
 *     [ s_x   s_xx  s_xy ] [b] = [s_vx]
 *     [ s_y   s_xy  s_yy ] [c]   [s_vy]
 *
 * The (a, b, c) coefficients are stored as int32 with an implied 8-bit
 * fractional scale (round(coef * 256)) so the side-metadata footprint is
 * deterministic and small. The reconstructed predictor uses the SAME
 * fixed-point arithmetic, so encode/decode round-trip exactly under
 * modular wrap at the input dtype's width.
 *
 * Source: vectra/src/vtr_codec.c:1666-1786 (plane_encode, plane_decode).
 * The math kernel is preserved one-to-one. The wrapping is rewritten for
 * tdc:
 *   - tdc_buffer / realloc_fn allocation everywhere (no malloc, no
 *     longjmp on alloc failure)
 *   - typed dtype dispatch instead of vectra's int64-only path
 *   - side metadata format matches the documented layout in tdc/format.h
 *     (u16 tile_size + u32 n_tiles + n_tiles*3*i32 coeffs)
 *   - bit-exact round trip via modular arithmetic at the input width,
 *     same convention as src/model/pred2d.c
 *
 * Accepted dtypes: i8, i16, i32, u8, u16, u32. (Same as PRED_2D and for
 * the same reason: 64-bit raster imagery is rare and the int64
 * accumulator inside the predictor cannot guard against overflow at
 * that width.) Floats are rejected — quantize first.
 *
 * Accepted layout: RASTER_2D only.
 *   ny = shape.dim[0]   (rows)
 *   nx = shape.dim[1]   (columns)
 *   idx = row * nx + col
 *
 * Validity bitmap: ignored, same convention as every other v0 model.
 */

#include "tdc/model.h"
#include "tdc/codec.h"
#include "model_internal.h"
#include "../core/buffer.h"

#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Acceptance bitmasks ------------------------------------------------ */

#define PLANE2D_DT_BIT(dt) (1u << (uint32_t)(dt))

#define PLANE2D_ACCEPTED_DTYPES (         \
    PLANE2D_DT_BIT(TDC_DT_I8)  |          \
    PLANE2D_DT_BIT(TDC_DT_I16) |          \
    PLANE2D_DT_BIT(TDC_DT_I32) |          \
    PLANE2D_DT_BIT(TDC_DT_U8)  |          \
    PLANE2D_DT_BIT(TDC_DT_U16) |          \
    PLANE2D_DT_BIT(TDC_DT_U32))

#define PLANE2D_ACCEPTED_LAYOUTS (1u << (uint32_t)TDC_LAYOUT_RASTER_2D)

#define PLANE2D_DEFAULT_TILE_SIZE 32u

/* Coefficients are stored as int32 with this fixed-point scale. The
 * inverse divides by COEFF_SCALE_F to recover the floating-point plane.
 * The 256x scale gives 8 bits of fractional precision, which is enough
 * for the predictor sub-pixel slope without overflowing int32 for any
 * realistic raster magnitude. */
#define PLANE2D_COEFF_SCALE_F 256.0

static int plane2d_dtype_accepted(tdc_dtype dt) {
    return (PLANE2D_ACCEPTED_DTYPES & PLANE2D_DT_BIT(dt)) != 0u;
}

/* ----- Type-generic load / modular store --------------------------------- */
/*
 * Same convention as src/model/pred2d.c: load with sign- or zero-extend
 * into int64; store low-N bits via the unsigned counterpart so the wrap
 * is well-defined under C semantics.
 */
static int64_t plane2d_load(tdc_dtype dt, const uint8_t *base, int64_t i) {
    switch (dt) {
        case TDC_DT_I8:  { int8_t   v; memcpy(&v, base + (size_t)i,        1u); return (int64_t)v; }
        case TDC_DT_I16: { int16_t  v; memcpy(&v, base + (size_t)i * 2u,   2u); return (int64_t)v; }
        case TDC_DT_I32: { int32_t  v; memcpy(&v, base + (size_t)i * 4u,   4u); return (int64_t)v; }
        case TDC_DT_U8:  { uint8_t  v; memcpy(&v, base + (size_t)i,        1u); return (int64_t)v; }
        case TDC_DT_U16: { uint16_t v; memcpy(&v, base + (size_t)i * 2u,   2u); return (int64_t)v; }
        case TDC_DT_U32: { uint32_t v; memcpy(&v, base + (size_t)i * 4u,   4u); return (int64_t)v; }
        default:         return 0;
    }
}

static void plane2d_store(tdc_dtype dt, uint8_t *base, int64_t i, int64_t v) {
    switch (dt) {
        case TDC_DT_I8:
        case TDC_DT_U8:  { uint8_t  x = (uint8_t)(uint64_t)v;  memcpy(base + (size_t)i,        &x, 1u); break; }
        case TDC_DT_I16:
        case TDC_DT_U16: { uint16_t x = (uint16_t)(uint64_t)v; memcpy(base + (size_t)i * 2u,   &x, 2u); break; }
        case TDC_DT_I32:
        case TDC_DT_U32: { uint32_t x = (uint32_t)(uint64_t)v; memcpy(base + (size_t)i * 4u,   &x, 4u); break; }
        default: break;
    }
}

/* ----- Tile geometry ------------------------------------------------------ */

typedef struct {
    uint32_t tiles_x;
    uint32_t tiles_y;
    uint32_t n_tiles;
} plane2d_tiling;

static plane2d_tiling plane2d_tile_count(int64_t nx, int64_t ny, uint32_t tile_size) {
    plane2d_tiling t;
    t.tiles_x = (uint32_t)(((uint64_t)nx + tile_size - 1u) / tile_size);
    t.tiles_y = (uint32_t)(((uint64_t)ny + tile_size - 1u) / tile_size);
    t.n_tiles = t.tiles_x * t.tiles_y;
    return t;
}

/* ----- Predictor evaluation ----------------------------------------------- */
/*
 * Reconstruct the int64 predictor value at local coordinates (lx, ly)
 * from the fixed-point plane coefficients. The arithmetic is identical
 * to vectra's plane_decode so encode and decode produce the same
 * predictor at every pixel.
 */
static inline int64_t plane2d_eval(const int32_t *coeffs_for_tile,
                                   uint32_t lx, uint32_t ly) {
    double a = (double)coeffs_for_tile[0];
    double b = (double)coeffs_for_tile[1];
    double c = (double)coeffs_for_tile[2];
    double pred = (a + b * (double)lx + c * (double)ly) / PLANE2D_COEFF_SCALE_F;
    return (int64_t)round(pred);
}

/* ----- Side metadata serialization ---------------------------------------- */
/*
 * Layout (matches tdc/codec.h plane_2d documentation):
 *
 *     u16  tile_size
 *     u32  n_tiles
 *     n_tiles * 3 * i32   coefficients (a, b, c per tile)
 *
 * The decoder reads tile_size and n_tiles from the header, then walks
 * the coefficient array directly. tile_size + (nx, ny) determine the
 * tile geometry exactly, so n_tiles is redundant — it is written
 * anyway as a self-describing cross-check (the decoder rejects mismatch).
 */
#define PLANE2D_META_HDR_BYTES 6u   /* u16 + u32 */

static tdc_status plane2d_side_write(tdc_buffer *side_out,
                                     uint16_t    tile_size,
                                     uint32_t    n_tiles,
                                     const int32_t *coeffs) {
    size_t bytes = (size_t)PLANE2D_META_HDR_BYTES + (size_t)n_tiles * 3u * 4u;
    tdc_status st = tdc_buf_reserve(side_out, bytes);
    if (st != TDC_OK) return st;

    uint8_t *p = side_out->data;
    memcpy(p + 0, &tile_size, 2u);
    memcpy(p + 2, &n_tiles,   4u);
    memcpy(p + PLANE2D_META_HDR_BYTES, coeffs, (size_t)n_tiles * 3u * 4u);
    side_out->size = bytes;
    return TDC_OK;
}

static tdc_status plane2d_side_read(const uint8_t *side_meta, size_t side_size,
                                    uint16_t *tile_size,
                                    uint32_t *n_tiles,
                                    const int32_t **coeffs_out) {
    if (side_size < PLANE2D_META_HDR_BYTES) return TDC_E_CORRUPT;
    uint16_t ts;
    uint32_t nt;
    memcpy(&ts, side_meta + 0, 2u);
    memcpy(&nt, side_meta + 2, 4u);
    if (ts == 0u) return TDC_E_CORRUPT;

    size_t need = (size_t)PLANE2D_META_HDR_BYTES + (size_t)nt * 3u * 4u;
    if (side_size != need) return TDC_E_CORRUPT;

    *tile_size  = ts;
    *n_tiles    = nt;
    *coeffs_out = (const int32_t *)(side_meta + PLANE2D_META_HDR_BYTES);
    return TDC_OK;
}

/* ----- Per-tile fit ------------------------------------------------------- */
/*
 * Closed-form 3-coefficient plane fit. Walks the tile once to accumulate
 * the symmetric normal-equation moments, solves the 3x3 system by
 * Cramer's rule, rounds to int32 fixed-point, and returns the resolved
 * coefficients via out_a/out_b/out_c.
 *
 * Degenerate cases (count < 3, det == 0): fall back to a constant plane
 * at the tile mean. count == 0 returns zeros (the tile is outside the
 * raster — should not happen given the tiles_x/tiles_y math, but is
 * handled defensively).
 */
static void plane2d_fit_tile(tdc_dtype dt, const uint8_t *src,
                             int64_t nx,
                             uint32_t x0, uint32_t y0,
                             uint32_t x1, uint32_t y1,
                             int32_t *out_a, int32_t *out_b, int32_t *out_c) {
    double s_1 = 0, s_x = 0, s_y = 0;
    double s_xx = 0, s_xy = 0, s_yy = 0;
    double s_v = 0, s_vx = 0, s_vy = 0;
    uint32_t count = 0;

    for (uint32_t py = y0; py < y1; ++py) {
        for (uint32_t px = x0; px < x1; ++px) {
            int64_t idx = (int64_t)py * nx + (int64_t)px;
            double v  = (double)plane2d_load(dt, src, idx);
            double lx = (double)(px - x0);
            double ly = (double)(py - y0);
            s_1  += 1.0;
            s_x  += lx;
            s_y  += ly;
            s_xx += lx * lx;
            s_xy += lx * ly;
            s_yy += ly * ly;
            s_v  += v;
            s_vx += v * lx;
            s_vy += v * ly;
            count++;
        }
    }

    double a = 0.0, b = 0.0, c = 0.0;
    if (count >= 3) {
        double det = s_1 * (s_xx * s_yy - s_xy * s_xy)
                   - s_x * (s_x  * s_yy - s_xy * s_y)
                   + s_y * (s_x  * s_xy - s_xx * s_y);
        if (det != 0.0 && det == det) {
            double inv_det = 1.0 / det;
            a = (s_v  * (s_xx * s_yy - s_xy * s_xy)
               - s_vx * (s_x  * s_yy - s_xy * s_y)
               + s_vy * (s_x  * s_xy - s_xx * s_y)) * inv_det;
            b = (s_1  * (s_vx * s_yy - s_vy * s_xy)
               - s_x  * (s_v  * s_yy - s_vy * s_y)
               + s_y  * (s_v  * s_xy - s_vx * s_y)) * inv_det;
            c = (s_1  * (s_xx * s_vy - s_xy * s_vx)
               - s_x  * (s_x  * s_vy - s_xy * s_v)
               + s_y  * (s_x  * s_vx - s_xx * s_v)) * inv_det;
        } else {
            a = s_v / s_1;
        }
    } else if (count > 0) {
        a = s_v / (double)count;
    }

    *out_a = (int32_t)round(a * PLANE2D_COEFF_SCALE_F);
    *out_b = (int32_t)round(b * PLANE2D_COEFF_SCALE_F);
    *out_c = (int32_t)round(c * PLANE2D_COEFF_SCALE_F);
}

/* ----- Encode ------------------------------------------------------------- */

static tdc_status plane2d_encode(const tdc_block *in,
                                 const void      *params,
                                 tdc_buffer      *residual_out,
                                 tdc_dtype       *residual_dtype,
                                 tdc_buffer      *side_out) {
    if (!in || !residual_out || !residual_out->realloc_fn) return TDC_E_INVAL;
    if (!side_out || !side_out->realloc_fn)                return TDC_E_INVAL;
    if (in->layout != TDC_LAYOUT_RASTER_2D) return TDC_E_LAYOUT;
    if (in->shape.rank != 2)                return TDC_E_SHAPE;
    if (!plane2d_dtype_accepted(in->dtype)) return TDC_E_DTYPE;

    int64_t ny = in->shape.dim[0];
    int64_t nx = in->shape.dim[1];
    if (nx < 0 || ny < 0)                                          return TDC_E_SHAPE;
    if (nx != 0 && ny != 0 && nx > INT64_MAX / ny)                 return TDC_E_SHAPE;
    if (nx > (int64_t)UINT32_MAX || ny > (int64_t)UINT32_MAX)      return TDC_E_SHAPE;

    size_t elem_size = tdc_dtype_size(in->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    /* Resolve tile size. Default if no params or tile_size == 0. */
    uint16_t tile_size = PLANE2D_DEFAULT_TILE_SIZE;
    if (params) {
        const tdc_plane2d_params *p = (const tdc_plane2d_params *)params;
        if (p->tile_size != 0u) tile_size = p->tile_size;
    }

    int64_t n     = nx * ny;
    size_t  bytes = (size_t)n * elem_size;

    /* Reserve residual output. Empty raster: write empty side_meta+residual
     * but still set residual_dtype so the chain plumbing is consistent. */
    if (residual_dtype) *residual_dtype = in->dtype;

    if (n == 0) {
        /* Self-describing empty record: header with n_tiles == 0. */
        tdc_status st = plane2d_side_write(side_out, tile_size, 0u, NULL);
        if (st != TDC_OK) return st;
        residual_out->size = 0;
        return TDC_OK;
    }
    if (!in->data) return TDC_E_INVAL;

    plane2d_tiling t = plane2d_tile_count(nx, ny, tile_size);
    if (t.tiles_x == 0u || t.tiles_y == 0u) return TDC_E_SHAPE;
    if (t.n_tiles / t.tiles_x != t.tiles_y) return TDC_E_SHAPE; /* mul overflow */

    /* Allocate the coefficient table via the side_out buffer's realloc.
     * We size it for header + coeffs up front and write the header at
     * the end so the coefficient pointer remains stable for the fit
     * loop. */
    size_t side_bytes = (size_t)PLANE2D_META_HDR_BYTES + (size_t)t.n_tiles * 3u * 4u;
    tdc_status st = tdc_buf_reserve(side_out, side_bytes);
    if (st != TDC_OK) return st;
    int32_t *coeffs = (int32_t *)(side_out->data + PLANE2D_META_HDR_BYTES);
    memset(coeffs, 0, (size_t)t.n_tiles * 3u * 4u);

    st = tdc_buf_reserve(residual_out, bytes);
    if (st != TDC_OK) return st;

    const uint8_t *src_p = (const uint8_t *)in->data;
    uint8_t       *dst_p = residual_out->data;

    /* Fit and emit residuals one tile at a time. The two halves of each
     * tile share the same fixed-point arithmetic so the decoder
     * reproduces the exact same predictor. */
    for (uint32_t ty = 0; ty < t.tiles_y; ++ty) {
        for (uint32_t tx = 0; tx < t.tiles_x; ++tx) {
            uint32_t x0 = tx * tile_size;
            uint32_t y0 = ty * tile_size;
            uint32_t x1 = x0 + tile_size; if (x1 > (uint32_t)nx) x1 = (uint32_t)nx;
            uint32_t y1 = y0 + tile_size; if (y1 > (uint32_t)ny) y1 = (uint32_t)ny;

            uint32_t tidx = ty * t.tiles_x + tx;
            int32_t *cf   = coeffs + (size_t)tidx * 3u;
            plane2d_fit_tile(in->dtype, src_p, nx, x0, y0, x1, y1,
                             &cf[0], &cf[1], &cf[2]);

            for (uint32_t py = y0; py < y1; ++py) {
                for (uint32_t px = x0; px < x1; ++px) {
                    int64_t idx  = (int64_t)py * nx + (int64_t)px;
                    int64_t val  = plane2d_load(in->dtype, src_p, idx);
                    int64_t pred = plane2d_eval(cf, px - x0, py - y0);
                    plane2d_store(in->dtype, dst_p, idx, val - pred);
                }
            }
        }
    }

    /* Now stamp the side-meta header. The coefficient buffer is already
     * in place — we wrote into it above through the same pointer the
     * side_out->data points to. */
    uint16_t ts_le = tile_size;
    uint32_t nt_le = t.n_tiles;
    memcpy(side_out->data + 0, &ts_le, 2u);
    memcpy(side_out->data + 2, &nt_le, 4u);
    side_out->size     = side_bytes;
    residual_out->size = bytes;

    return TDC_OK;
}

/* ----- Decode ------------------------------------------------------------- */

static tdc_status plane2d_decode(tdc_block      *out,
                                 const void     *params,
                                 tdc_dtype       residual_dtype,
                                 const uint8_t  *residuals, size_t residual_size,
                                 const uint8_t  *side_meta, size_t side_size) {
    (void)params;
    if (!out) return TDC_E_INVAL;
    if (out->layout != TDC_LAYOUT_RASTER_2D) return TDC_E_LAYOUT;
    if (out->shape.rank != 2)                return TDC_E_SHAPE;
    if (residual_dtype != out->dtype)        return TDC_E_DTYPE;
    if (!plane2d_dtype_accepted(out->dtype)) return TDC_E_DTYPE;

    int64_t ny = out->shape.dim[0];
    int64_t nx = out->shape.dim[1];
    if (nx < 0 || ny < 0)                                          return TDC_E_SHAPE;
    if (nx != 0 && ny != 0 && nx > INT64_MAX / ny)                 return TDC_E_SHAPE;
    if (nx > (int64_t)UINT32_MAX || ny > (int64_t)UINT32_MAX)      return TDC_E_SHAPE;

    size_t elem_size = tdc_dtype_size(out->dtype);
    if (elem_size == 0) return TDC_E_DTYPE;

    int64_t n     = nx * ny;
    size_t  bytes = (size_t)n * elem_size;
    if (residual_size != bytes) return TDC_E_CORRUPT;

    uint16_t       tile_size = 0;
    uint32_t       n_tiles_meta = 0;
    const int32_t *coeffs = NULL;
    tdc_status st = plane2d_side_read(side_meta, side_size,
                                      &tile_size, &n_tiles_meta, &coeffs);
    if (st != TDC_OK) return st;

    if (n == 0) {
        /* Empty raster: meta n_tiles must be 0 too. */
        if (n_tiles_meta != 0u) return TDC_E_CORRUPT;
        return TDC_OK;
    }

    plane2d_tiling t = plane2d_tile_count(nx, ny, tile_size);
    if (t.tiles_x == 0u || t.tiles_y == 0u) return TDC_E_CORRUPT;
    if (t.n_tiles != n_tiles_meta)          return TDC_E_CORRUPT;

    if (!out->data || !residuals) return TDC_E_INVAL;

    uint8_t *dst_p = (uint8_t *)out->data;

    /* Walk every pixel; locate its tile; reconstruct the predictor;
     * add the residual mod 2^N. The address arithmetic mirrors the
     * encoder, including the int32 fixed-point evaluation. */
    for (int64_t i = 0; i < n; ++i) {
        uint32_t px   = (uint32_t)(i % nx);
        uint32_t py   = (uint32_t)(i / nx);
        uint32_t tx   = px / tile_size;
        uint32_t ty   = py / tile_size;
        uint32_t tidx = ty * t.tiles_x + tx;
        int64_t  pred = plane2d_eval(coeffs + (size_t)tidx * 3u,
                                     px - tx * tile_size,
                                     py - ty * tile_size);
        int64_t  r    = plane2d_load(out->dtype, residuals, i);
        plane2d_store(out->dtype, dst_p, i, r + pred);
    }

    return TDC_OK;
}

/* ----- Vtable ------------------------------------------------------------- */

const tdc_model_vt tdc_model_plane2d_vt = {
    .id               = TDC_MODEL_PLANE_2D,
    .name             = "plane2d",
    .accepted_dtypes  = PLANE2D_ACCEPTED_DTYPES,
    .accepted_layouts = PLANE2D_ACCEPTED_LAYOUTS,
    .encode           = plane2d_encode,
    .decode           = plane2d_decode,
};
