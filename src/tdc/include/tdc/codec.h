/*
 * tdc/codec.h — frozen v0
 *
 * Codec specification: how to encode one block, plus the one-shot
 * tdc_encode_block / tdc_decode_block entry points.
 *
 * Design rules:
 *   1. The transform stage is a CHAIN, not a single id, from day 0. v0 keeps
 *      the chain as a fixed-capacity array (TDC_MAX_TRANSFORMS = 4) to avoid
 *      a heap allocation per encode. A 0 entry terminates the chain.
 *   2. Symbolization is owned by REPRESENTATION (the transform stage). It is
 *      not its own pipeline phase. The model emits a flat residual stream;
 *      transforms turn that stream into entropy-friendly bytes. This was
 *      Option B in the design discussion and is the only way the pipeline
 *      stays the same shape across all dimensionalities.
 *   3. v0 has a STATIC registry. The ids below are an enum, not a runtime
 *      registration table. A future plugin API can be added without changing
 *      the on-disk format because the ids are u16 and id ranges are reserved
 *      below for "core" vs "experimental" vs "user".
 *   4. Per-stage params are passed as opaque pointers. Each stage knows how
 *      to cast them. This keeps tdc_codec_spec small and POD.
 */

#ifndef TDC_CODEC_H
#define TDC_CODEC_H

#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TDC_MAX_TRANSFORMS 4

/* ----- Stage ids ----------------------------------------------------------- */
/*
 * Reserved id ranges (for both model, transform, entropy enums):
 *     0x0000          = NONE / sentinel (chain terminator for transforms)
 *     0x0001 - 0x00FF = core (shipped with tdc)
 *     0x0100 - 0x01FF = experimental (may change without version bump)
 *     0x0200 - 0xFEFF = reserved
 *     0xFF00 - 0xFFFF = user-defined (post-v0, when plugin API exists)
 */

/* Models — full v0 set.
 *
 * Existing in vectra (extraction): RAW, DELTA_1D, DICT_1D, PRED_2D, PLANE_2D.
 * New in v0 (write from scratch):  STACK_2D, PRED_3D.
 *
 * PRED_2D covers the LEFT/UP/AVERAGE/PAETH predictor family. PLANE is a
 * SEPARATE model id (not a tdc_pred2d_kind) because its side metadata is
 * structurally incompatible: PRED_2D side meta is 1 byte (the resolved
 * kind); PLANE side meta is u16 tile_size + u32 n_tiles + 3*i32 per tile.
 * Cramming both shapes under one model id would force a "primary path +
 * fallback path" branch on the side-meta layout — exactly the anti-pattern
 * the project rules forbid. */
typedef enum {
    TDC_MODEL_NONE      = 0x0000,  /* invalid; never written to disk */
    TDC_MODEL_RAW       = 0x0001,  /* identity; any layout, any dtype */
    TDC_MODEL_DELTA_1D  = 0x0002,  /* x_i - x_{i-1}; VECTOR_1D, integer dtypes */
    TDC_MODEL_DICT_1D   = 0x0003,  /* dictionary + RLE indices; VECTOR_1D + STRING */
    TDC_MODEL_PRED_2D   = 0x0004,  /* LEFT/UP/AVERAGE/PAETH; RASTER_2D */
    TDC_MODEL_STACK_2D  = 0x0005,  /* per-slice 2D predictor; STACK_2D */
    TDC_MODEL_PRED_3D   = 0x0006,  /* 3D neighbor predictor; VOLUME_3D */
    TDC_MODEL_PLANE_2D  = 0x0007   /* per-tile LSQ plane fit; RASTER_2D */
} tdc_model_id;

/* Transforms (representation stage; chained) */
typedef enum {
    TDC_XFORM_NONE         = 0x0000, /* chain terminator */
    TDC_XFORM_QUANTIZE     = 0x0001, /* lossy: f32/f64 -> narrow int */
    TDC_XFORM_ZIGZAG       = 0x0002, /* signed -> unsigned, small magnitudes near 0 */
    TDC_XFORM_BYTE_SHUFFLE = 0x0003, /* transpose by byte lane (8/4/2 byte elems) */
    TDC_XFORM_BIT_SHUFFLE  = 0x0004  /* transpose by bit lane (post-v0; reserved) */
} tdc_xform_id;

/* Entropy coders */
typedef enum {
    TDC_ENTROPY_NONE    = 0x0000, /* memcpy passthrough */
    TDC_ENTROPY_LZ2     = 0x0001, /* native LZ77, separated-stream, 64K window */
    TDC_ENTROPY_DEFLATE = 0x0002  /* zlib deflate; optional link, "ratio" mode */
    /* post-v0: HUFFMAN = 0x0003, FSE = 0x0004, ANS = 0x0005 */
} tdc_entropy_id;

/* ----- Per-stage params ---------------------------------------------------- */
/*
 * These structs are public so that callers can stack-allocate them. Each
 * stage's encode() casts the (void*) it receives back to its own params type.
 *
 * Adding a field to one of these is forward-compatible IFF the new field has
 * a meaningful zero default (so old call sites that memset the struct still
 * work). Otherwise the format version must be bumped.
 */

/* TDC_XFORM_QUANTIZE */
typedef struct {
    double    scale;       /* stored = round((value - offset) * scale) */
    double    offset;
    tdc_dtype target;      /* must be a signed integer dtype */
} tdc_quantize_params;

/* TDC_MODEL_PRED_2D — predictor selection */
typedef enum {
    TDC_PRED2D_AUTO    = 0,
    TDC_PRED2D_LEFT    = 1,
    TDC_PRED2D_UP      = 2,
    TDC_PRED2D_AVERAGE = 3,
    TDC_PRED2D_PAETH   = 4
} tdc_pred2d_kind;

typedef struct {
    tdc_pred2d_kind kind;
} tdc_pred2d_params;

/* TDC_MODEL_PLANE_2D — per-tile plane fit */
typedef struct {
    uint16_t tile_size;    /* default 32 */
} tdc_plane2d_params;

/* TDC_ENTROPY_LZ2 / DEFLATE */
typedef struct {
    int level;             /* 0 = default; range is backend-specific */
} tdc_entropy_level;

/* ----- Codec spec ---------------------------------------------------------- */
/*
 * The full description of how to encode one block. POD, copy-by-value safe.
 *
 * The transform chain runs LEFT TO RIGHT during encode and RIGHT TO LEFT
 * during decode. The first 0 terminates the chain. Example:
 *
 *   model    = TDC_MODEL_PRED_2D
 *   xform[0] = TDC_XFORM_ZIGZAG
 *   xform[1] = TDC_XFORM_BYTE_SHUFFLE
 *   xform[2] = TDC_XFORM_NONE        <-- terminator
 *   entropy  = TDC_ENTROPY_LZ2
 */

typedef struct {
    tdc_model_id   model;
    tdc_xform_id   xform[TDC_MAX_TRANSFORMS];
    tdc_entropy_id entropy;

    const void *model_params;     /* points to tdc_pred2d_params, etc., or NULL */
    const void *xform_params[TDC_MAX_TRANSFORMS];
    const void *entropy_params;   /* points to tdc_entropy_level, or NULL */
} tdc_codec_spec;

/* Convenience: zero-initialize a spec to RAW + NONE + NONE. */
static inline tdc_codec_spec tdc_codec_spec_raw(void) {
    tdc_codec_spec s = {0};
    s.model   = TDC_MODEL_RAW;
    s.entropy = TDC_ENTROPY_NONE;
    return s;
}

/* ----- One-shot encode / decode entry points ------------------------------- */
/*
 * Encode a single block according to spec. Output is written into out as a
 * single tdc_block_record (header + side_meta + payload). The caller's
 * tdc_buffer is grown via its realloc_fn as needed.
 */
tdc_status tdc_encode_block(const tdc_block      *src,
                            const tdc_codec_spec *spec,
                            tdc_buffer           *out);

/*
 * Decode a single block record. dst must already have:
 *   - data pointing at a buffer of n_elems * dtype_size bytes
 *   - dtype, layout, shape filled in (these are checked against the header)
 * On TDC_OK, dst->data is filled with the reconstructed values.
 *
 * If the header's (dtype, layout, shape) disagrees with dst, returns
 * TDC_E_DTYPE / TDC_E_LAYOUT / TDC_E_SHAPE without touching dst->data.
 */
tdc_status tdc_decode_block(const uint8_t *src, size_t src_size,
                            tdc_block     *dst);

#ifdef __cplusplus
}
#endif
#endif /* TDC_CODEC_H */
