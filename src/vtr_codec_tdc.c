/*
 * vtr_codec_tdc.c — side-by-side tdc-backed encode bridge (P2a).
 *
 * Builds a tdc_block view over a VecArray, picks a tdc_codec_spec from
 * (comp_level, qspec, sspec), and emits a single self-describing
 * tdc_block_record via tdc_encode_block.
 *
 * Purely additive: nothing on the read or write path calls this yet.
 * The legacy vtr_encode_column / _ex / _q / _qs in vtr_codec.c remain
 * the production encode entry points until P4 swaps them out.
 */

#include "vtr_codec_tdc.h"
#include "vtr_codec_internal.h"   /* should_delta_encode for DELTA detection */

#include <R.h>
#include <Rinternals.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* ---------- type mapping --------------------------------------------------- */

tdc_dtype vtr_type_to_tdc_dtype(VecType t) {
    switch (t) {
    case VEC_INT8:   return TDC_DT_I8;
    case VEC_INT16:  return TDC_DT_I16;
    case VEC_INT32:  return TDC_DT_I32;
    case VEC_INT64:  return TDC_DT_I64;
    case VEC_DOUBLE: return TDC_DT_F64;
    case VEC_BOOL:   return TDC_DT_U8;
    case VEC_STRING: return TDC_DT_STRING;
    }
    return (tdc_dtype)0;
}

static tdc_dtype vtr_quantize_target_to_tdc(VecType t) {
    switch (t) {
    case VEC_INT8:  return TDC_DT_I8;
    case VEC_INT16: return TDC_DT_I16;
    case VEC_INT32: return TDC_DT_I32;
    case VEC_INT64: return TDC_DT_I64;
    default:        return TDC_DT_I32;  /* default narrow target */
    }
}

static tdc_pred2d_kind vtr_pred_to_tdc_pred2d_kind(int pred) {
    switch (pred) {
    case VTR_PRED_LEFT:    return TDC_PRED2D_LEFT;
    case VTR_PRED_UP:      return TDC_PRED2D_UP;
    case VTR_PRED_AVERAGE: return TDC_PRED2D_AVERAGE;
    case VTR_PRED_PAETH:   return TDC_PRED2D_PAETH;
    default:               return TDC_PRED2D_AUTO;  /* incl. -1 */
    }
}

/* ---------- realloc shim --------------------------------------------------- */
/* Same convention as vtr_compress.c's vtr_stdlib_realloc, kept local so the
 * bridge is self-contained. */
static void *vtr_tdc_realloc(void *user, void *ptr, size_t new_size) {
    (void)user;
    if (new_size == 0) { free(ptr); return NULL; }
    return realloc(ptr, new_size);
}

/* ---------- encode bridge -------------------------------------------------- */

tdc_status vtr_encode_column_tdc(const VecArray         *col,
                                 int64_t                 n_rows,
                                 int                     comp_level,
                                 const VtrQuantizeSpec  *qspec,
                                 const VtrSpatialSpec   *sspec,
                                 tdc_buffer             *block_out) {
    if (!col || !block_out || !block_out->realloc_fn) return TDC_E_INVAL;
    if (n_rows < 0 || n_rows != col->length)          return TDC_E_INVAL;

    /* ---------- block view ------------------------------------------------ */
    tdc_block blk = {0};
    blk.dtype    = vtr_type_to_tdc_dtype(col->type);
    blk.validity = col->validity;
    blk.layout       = TDC_LAYOUT_VECTOR_1D;
    blk.shape.rank   = 1;
    blk.shape.dim[0] = n_rows;

    /* String columns need a uint32_t offsets[] view; vectra stores int64_t.
     * Narrow-cast with explicit overflow checks (per Q4 in VECTRA_REWIRE.md). */
    uint32_t *str_offsets = NULL;
    if (col->type == VEC_STRING) {
        if (col->buf.str.data_len < 0 ||
            (int64_t)(uint32_t)col->buf.str.data_len != col->buf.str.data_len) {
            return TDC_E_UNSUPPORTED;
        }
        str_offsets = (uint32_t *)block_out->realloc_fn(
            block_out->user, NULL, sizeof(uint32_t) * (size_t)(n_rows + 1));
        if (!str_offsets) return TDC_E_NOMEM;
        for (int64_t i = 0; i <= n_rows; ++i) {
            int64_t v = col->buf.str.offsets[i];
            if (v < 0 || (int64_t)(uint32_t)v != v) {
                block_out->realloc_fn(block_out->user, str_offsets, 0);
                return TDC_E_UNSUPPORTED;
            }
            str_offsets[i] = (uint32_t)v;
        }
        blk.data    = (void *)col->buf.str.data;
        blk.offsets = str_offsets;
    } else {
        switch (col->type) {
        case VEC_INT64:  blk.data = (void *)col->buf.i64; break;
        case VEC_INT32:  blk.data = (void *)col->buf.i32; break;
        case VEC_INT16:  blk.data = (void *)col->buf.i16; break;
        case VEC_INT8:   blk.data = (void *)col->buf.i8;  break;
        case VEC_DOUBLE: blk.data = (void *)col->buf.dbl; break;
        case VEC_BOOL:   blk.data = (void *)col->buf.bln; break;
        default:         return TDC_E_DTYPE;
        }
    }

    /* ---------- spec selection -------------------------------------------- *
     * Param structs are referenced by `spec.*_params`; they must outlive the
     * tdc_encode_block call. Stack-allocating here keeps lifetime obvious. */
    tdc_codec_spec      spec = tdc_codec_spec_raw();
    tdc_quantize_params qp   = {0};
    tdc_pred2d_params   pp   = {0};
    tdc_plane2d_params  plp  = {0};

    const int spatial_active  = (sspec && sspec->enabled);
    const int quantize_active = (qspec && qspec->enabled && col->type == VEC_DOUBLE);

    if (comp_level == VTR_COMPRESS_NONE) {
        /* RAW + passthrough entropy. spec already initialized to that. */
    } else if (spatial_active) {
        blk.layout       = TDC_LAYOUT_RASTER_2D;
        blk.shape.rank   = 2;
        blk.shape.dim[0] = (int64_t)sspec->ny;  /* row-major: rows first */
        blk.shape.dim[1] = (int64_t)sspec->nx;

        if (sspec->predictor == VTR_PRED_PLANE) {
            spec.model        = TDC_MODEL_PLANE_2D;
            plp.tile_size     = sspec->tile_size ? sspec->tile_size : 32;
            spec.model_params = &plp;
        } else {
            spec.model        = TDC_MODEL_PRED_2D;
            pp.kind           = vtr_pred_to_tdc_pred2d_kind(sspec->predictor);
            spec.model_params = &pp;
        }

        int xi = 0;
        if (quantize_active) {
            qp.scale  = qspec->scale;
            qp.offset = qspec->offset;
            qp.target = vtr_quantize_target_to_tdc(qspec->target_type);
            spec.xform[xi]        = TDC_XFORM_QUANTIZE;
            spec.xform_params[xi] = &qp;
            ++xi;
        }
        spec.xform[xi++] = TDC_XFORM_ZIGZAG;
        spec.xform[xi++] = TDC_XFORM_BYTE_SHUFFLE;
        spec.entropy[0]  = TDC_ENTROPY_LZ;
    } else if (quantize_active) {
        qp.scale  = qspec->scale;
        qp.offset = qspec->offset;
        qp.target = vtr_quantize_target_to_tdc(qspec->target_type);
        spec.model           = TDC_MODEL_RAW;
        spec.xform[0]        = TDC_XFORM_QUANTIZE;
        spec.xform_params[0] = &qp;
        spec.xform[1]        = TDC_XFORM_BYTE_SHUFFLE;
        spec.entropy[0]      = TDC_ENTROPY_LZ;
    } else if (col->type == VEC_STRING) {
        spec.model      = TDC_MODEL_DICT_1D;
        spec.entropy[0] = TDC_ENTROPY_LZ;
    } else if (col->type == VEC_INT64 && should_delta_encode(col, n_rows)) {
        spec.model      = TDC_MODEL_DELTA_1D;
        spec.xform[0]   = TDC_XFORM_ZIGZAG;
        spec.xform[1]   = TDC_XFORM_BYTE_SHUFFLE;
        spec.entropy[0] = TDC_ENTROPY_LZ;
    } else if (vec_type_is_fixed(col->type) && col->type != VEC_BOOL) {
        spec.model      = TDC_MODEL_RAW;
        spec.xform[0]   = TDC_XFORM_BYTE_SHUFFLE;
        spec.entropy[0] = TDC_ENTROPY_LZ;
    } else {
        /* VEC_BOOL: 1-byte elements, byte-shuffle is a no-op. */
        spec.model      = TDC_MODEL_RAW;
        spec.entropy[0] = TDC_ENTROPY_LZ;
    }

    tdc_shape_set_contiguous(&blk.shape);

    tdc_status st = tdc_block_validate(&blk);
    if (st != TDC_OK) {
        if (str_offsets) block_out->realloc_fn(block_out->user, str_offsets, 0);
        return st;
    }

    st = tdc_encode_block(&blk, &spec, block_out);

    if (str_offsets) block_out->realloc_fn(block_out->user, str_offsets, 0);
    return st;
}

/* =========================================================================
 * R bridge — minimal round-trip entry points used by the testthat unit
 * test for P2a. These are NOT part of the production write/read path.
 *
 *   C_tdc_encode_double  : encodes a REALSXP via the bridge, returns the
 *                          tdc block record bytes as a RAWSXP.
 *   C_tdc_decode_double  : decodes a RAWSXP previously emitted by the
 *                          bridge into a fresh REALSXP of length n.
 *
 * Together they exercise: bridge encode -> tdc decode (round-trip).
 * A full decode bridge that handles all VecTypes is P2b's deliverable.
 * ========================================================================= */

SEXP C_tdc_encode_double(SEXP x_sexp, SEXP comp_level_sexp) {
    if (TYPEOF(x_sexp) != REALSXP)
        Rf_error("C_tdc_encode_double: x must be REALSXP");
    if (TYPEOF(comp_level_sexp) != INTSXP || LENGTH(comp_level_sexp) != 1)
        Rf_error("C_tdc_encode_double: comp_level must be a scalar integer");

    int64_t n = (int64_t)Rf_xlength(x_sexp);
    int comp_level = INTEGER(comp_level_sexp)[0];

    VecArray col = {0};
    col.type     = VEC_DOUBLE;
    col.length   = n;
    col.buf.dbl  = REAL(x_sexp);   /* borrowed; not freed */

    tdc_buffer buf = {0};
    buf.realloc_fn = vtr_tdc_realloc;

    tdc_status st = vtr_encode_column_tdc(&col, n, comp_level,
                                          NULL, NULL, &buf);
    if (st != TDC_OK) {
        if (buf.data) vtr_tdc_realloc(NULL, buf.data, 0);
        Rf_error("vtr_encode_column_tdc failed (status=%d)", (int)st);
    }

    SEXP out = PROTECT(allocVector(RAWSXP, (R_xlen_t)buf.size));
    if (buf.size > 0) memcpy(RAW(out), buf.data, buf.size);
    vtr_tdc_realloc(NULL, buf.data, 0);
    UNPROTECT(1);
    return out;
}

SEXP C_tdc_decode_double(SEXP raw_sexp, SEXP n_sexp) {
    if (TYPEOF(raw_sexp) != RAWSXP)
        Rf_error("C_tdc_decode_double: raw must be RAWSXP");
    if (TYPEOF(n_sexp) != INTSXP && TYPEOF(n_sexp) != REALSXP)
        Rf_error("C_tdc_decode_double: n must be numeric scalar");

    R_xlen_t n = (TYPEOF(n_sexp) == INTSXP)
        ? (R_xlen_t)INTEGER(n_sexp)[0]
        : (R_xlen_t)REAL(n_sexp)[0];

    SEXP out = PROTECT(allocVector(REALSXP, n));

    tdc_block dst = {0};
    dst.dtype        = TDC_DT_F64;
    dst.layout       = TDC_LAYOUT_VECTOR_1D;
    dst.shape.rank   = 1;
    dst.shape.dim[0] = (int64_t)n;
    tdc_shape_set_contiguous(&dst.shape);
    dst.data = REAL(out);

    tdc_status st = tdc_decode_block_into(RAW(raw_sexp),
                                          (size_t)Rf_xlength(raw_sexp),
                                          &dst);
    if (st != TDC_OK) Rf_error("tdc_decode_block_into failed (status=%d)", (int)st);

    UNPROTECT(1);
    return out;
}
