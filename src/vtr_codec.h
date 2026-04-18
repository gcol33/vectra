#ifndef VECTRA_VTR_CODEC_H
#define VECTRA_VTR_CODEC_H

/*
 * vtr_codec.h — tag constants and write-time specs consumed by the
 * tdc bridge (vtr_codec_tdc.c).
 *
 *   - VTR_COMPRESS_{NONE,FAST,SMALL}: caller-visible compression level knob
 *   - VTR_PRED_*: spatial predictor tags mapped to tdc_pred2d_kind
 *   - VtrQuantizeSpec / VtrSpatialSpec: per-column write-time specs
 *
 * Numeric values are load-bearing: r_bridge_io.c parses user input against
 * them.
 */

#include "types.h"
#include <stdint.h>
#include <stddef.h>

/*
 * Compression levels (passed to the writer).
 *
 *   NONE   RAW + passthrough entropy.
 *   FAST   default model heuristic + BYTE_SHUFFLE + LZ.
 *   SMALL  future: try-all-pick-smallest over tdc codec specs. Currently
 *          treated identically to FAST at the tdc bridge; the outer
 *          candidate loop was an artefact of the v4 codec and has not been
 *          ported yet.
 */
#define VTR_COMPRESS_NONE   0
#define VTR_COMPRESS_FAST   1
#define VTR_COMPRESS_SMALL  2

/* Spatial predictor tags (consumed by the tdc bridge's pred2d_kind mapping).
 * -1 is the "auto" sentinel; 0-4 match the tdc predictor enum order. */
#define VTR_PRED_LEFT    0
#define VTR_PRED_UP      1
#define VTR_PRED_AVERAGE 2
#define VTR_PRED_PAETH   3
#define VTR_PRED_PLANE   4

/* Per-column spatial specification (write-time). */
typedef struct {
    int      enabled;     /* 0 = no spatial encoding */
    uint32_t nx;          /* raster width */
    uint32_t ny;          /* raster height */
    int      predictor;   /* -1 = auto, 0-4 = forced predictor tag */
    uint16_t tile_size;   /* for plane predictor (default 32) */
} VtrSpatialSpec;

/* Per-column quantization specification (write-time). */
typedef struct {
    int     enabled;       /* 0 = no quantization */
    double  scale;         /* multiplier: stored = round((value - offset) * scale) */
    double  offset;        /* centering offset */
    VecType target_type;   /* VEC_INT8/16/32 */
} VtrQuantizeSpec;

#endif /* VECTRA_VTR_CODEC_H */
