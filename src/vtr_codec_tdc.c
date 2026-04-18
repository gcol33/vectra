/*
 * vtr_codec_tdc.c — tdc-backed encode/decode bridge (sole codec path).
 *
 * Encode:
 *   Builds a tdc_block view over a VecArray, picks a tdc_codec_spec from
 *   (comp_level, qspec, sspec), and emits a single self-describing
 *   tdc_block_record via tdc_encode_block.
 *
 * Decode:
 *   Reads a tdc_block_record into a pre-allocated VecArray (or raw typed
 *   buffer) via tdc_decode_block_into. Validity bytes are pulled directly
 *   out of the record because tdc v0's decode does not surface them — see
 *   the comment block on validity below.
 */

#include "vtr_codec_tdc.h"
#include "array.h"                /* vec_validity_bytes, vec_array_is_valid */

#include "tdc/format.h"           /* tdc_block_record, TDC_BLOCK_HEADER_SIZE */

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

/* ---------- DELTA heuristic ------------------------------------------------ *
 *
 * Monotonically non-decreasing int64 with at least one valid element -> pick
 * TDC_MODEL_DELTA_1D. NA rows are skipped. */
static int vtr_should_delta_encode(const VecArray *col, int64_t n_rows) {
    if (col->type != VEC_INT64 || n_rows < 2) return 0;
    int64_t prev = 0;
    int started = 0;
    for (int64_t i = 0; i < n_rows; ++i) {
        if (!vec_array_is_valid(col, i)) continue;
        if (!started) { prev = col->buf.i64[i]; started = 1; continue; }
        if (col->buf.i64[i] < prev) return 0;
        prev = col->buf.i64[i];
    }
    return started;
}

/* ---------- encode request (shared with vtr1_tdc.c) ---------------------- */

tdc_status vtr_codec_tdc_prepare_request(
    VtrTdcEncodeRequest    *req,
    const VecArray         *col,
    int64_t                 n_rows,
    int                     comp_level,
    const VtrQuantizeSpec  *qspec,
    const VtrSpatialSpec   *sspec,
    void                  *(*realloc_fn)(void *user, void *ptr, size_t n),
    void                   *alloc_user) {
    if (!req || !col || !realloc_fn) return TDC_E_INVAL;
    if (n_rows < 0 || n_rows != col->length) return TDC_E_INVAL;

    memset(req, 0, sizeof(*req));
    req->block.dtype    = vtr_type_to_tdc_dtype(col->type);
    req->block.validity = col->validity;
    req->block.layout       = TDC_LAYOUT_VECTOR_1D;
    req->block.shape.rank   = 1;
    req->block.shape.dim[0] = n_rows;
    req->spec = tdc_codec_spec_raw();

    /* String columns need a uint32_t offsets[] view; vectra stores int64_t.
     * Narrow-cast with explicit overflow checks (per Q4 in VECTRA_REWIRE.md). */
    if (col->type == VEC_STRING) {
        if (col->buf.str.data_len < 0 ||
            (int64_t)(uint32_t)col->buf.str.data_len != col->buf.str.data_len) {
            return TDC_E_UNSUPPORTED;
        }
        req->str_offsets_owned = (uint32_t *)realloc_fn(
            alloc_user, NULL, sizeof(uint32_t) * (size_t)(n_rows + 1));
        if (!req->str_offsets_owned) return TDC_E_NOMEM;
        for (int64_t i = 0; i <= n_rows; ++i) {
            int64_t v = col->buf.str.offsets[i];
            if (v < 0 || (int64_t)(uint32_t)v != v) {
                realloc_fn(alloc_user, req->str_offsets_owned, 0);
                req->str_offsets_owned = NULL;
                return TDC_E_UNSUPPORTED;
            }
            req->str_offsets_owned[i] = (uint32_t)v;
        }
        req->block.data    = (void *)col->buf.str.data;
        req->block.offsets = req->str_offsets_owned;
    } else {
        switch (col->type) {
        case VEC_INT64:  req->block.data = (void *)col->buf.i64; break;
        case VEC_INT32:  req->block.data = (void *)col->buf.i32; break;
        case VEC_INT16:  req->block.data = (void *)col->buf.i16; break;
        case VEC_INT8:   req->block.data = (void *)col->buf.i8;  break;
        case VEC_DOUBLE: req->block.data = (void *)col->buf.dbl; break;
        case VEC_BOOL:   req->block.data = (void *)col->buf.bln; break;
        default:         return TDC_E_DTYPE;
        }
    }

    const int spatial_active  = (sspec && sspec->enabled);
    const int quantize_active = (qspec && qspec->enabled && col->type == VEC_DOUBLE);

    if (comp_level == VTR_COMPRESS_NONE) {
        /* RAW + passthrough entropy. spec already initialized to that. */
    } else if (spatial_active) {
        req->block.layout       = TDC_LAYOUT_RASTER_2D;
        req->block.shape.rank   = 2;
        req->block.shape.dim[0] = (int64_t)sspec->ny;  /* row-major: rows first */
        req->block.shape.dim[1] = (int64_t)sspec->nx;

        int xi = 0;

        if (sspec->predictor == VTR_PRED_PLANE) {
            req->spec.model        = TDC_MODEL_PLANE_2D;
            req->plp.tile_size     = sspec->tile_size ? sspec->tile_size : 32;
            req->spec.model_params = &req->plp;
            if (quantize_active) {
                /* PLANE_2D doesn't fuse with QUANTIZE; surface quantize as a
                 * leading transform. */
                req->qp.scale  = qspec->scale;
                req->qp.offset = qspec->offset;
                req->qp.target = vtr_quantize_target_to_tdc(qspec->target_type);
                req->spec.xform[xi]        = TDC_XFORM_QUANTIZE;
                req->spec.xform_params[xi] = &req->qp;
                ++xi;
            }
        } else if (quantize_active) {
            /* Spatial + quantize on a float column: use the fused
             * QUANTIZE_THEN_PRED2D composite. The naive
             * "model=PRED_2D, xform[0]=QUANTIZE" order runs the predictor
             * on the raw float raster first and then re-quantizes the
             * float residual into garbage that no longer round-trips. The
             * composite quantizes BEFORE the predictor sees the data so
             * PRED_2D operates on a clean integer raster. */
            req->qpp.scale  = qspec->scale;
            req->qpp.offset = qspec->offset;
            req->qpp.target = vtr_quantize_target_to_tdc(qspec->target_type);
            req->qpp.kind   = vtr_pred_to_tdc_pred2d_kind(sspec->predictor);
            req->spec.model        = TDC_MODEL_QUANTIZE_PRED_2D;
            req->spec.model_params = &req->qpp;
        } else {
            req->spec.model        = TDC_MODEL_PRED_2D;
            req->pp.kind           = vtr_pred_to_tdc_pred2d_kind(sspec->predictor);
            req->spec.model_params = &req->pp;
        }

        /* ZIGZAG is integer-only (signed -> unsigned interleave for entropy
         * coding). When the residual is float (spatial on doubles without
         * quantize and predictor != composite), skip ZIGZAG and rely on
         * BYTE_SHUFFLE to decorrelate exponent/mantissa bytes ahead of LZ.
         * The composite emits an integer residual so ZIGZAG applies. */
        const int residual_is_float =
            (col->type == VEC_DOUBLE) && !quantize_active;
        if (!residual_is_float) {
            req->spec.xform[xi++] = TDC_XFORM_ZIGZAG;
        }
        req->spec.xform[xi++] = TDC_XFORM_BYTE_SHUFFLE;
        req->spec.entropy[0]  = TDC_ENTROPY_LZ;
    } else if (quantize_active) {
        req->qp.scale  = qspec->scale;
        req->qp.offset = qspec->offset;
        req->qp.target = vtr_quantize_target_to_tdc(qspec->target_type);
        req->spec.model           = TDC_MODEL_RAW;
        req->spec.xform[0]        = TDC_XFORM_QUANTIZE;
        req->spec.xform_params[0] = &req->qp;
        req->spec.xform[1]        = TDC_XFORM_BYTE_SHUFFLE;
        req->spec.entropy[0]      = TDC_ENTROPY_LZ;
    } else if (col->type == VEC_STRING) {
        req->spec.model      = TDC_MODEL_DICT_1D;
        req->spec.entropy[0] = TDC_ENTROPY_LZ;
    } else if (col->type == VEC_INT64 && vtr_should_delta_encode(col, n_rows)) {
        req->spec.model      = TDC_MODEL_DELTA_1D;
        req->spec.xform[0]   = TDC_XFORM_ZIGZAG;
        req->spec.xform[1]   = TDC_XFORM_BYTE_SHUFFLE;
        req->spec.entropy[0] = TDC_ENTROPY_LZ;
    } else if (vec_type_is_fixed(col->type) && col->type != VEC_BOOL) {
        req->spec.model      = TDC_MODEL_RAW;
        req->spec.xform[0]   = TDC_XFORM_BYTE_SHUFFLE;
        req->spec.entropy[0] = TDC_ENTROPY_LZ;
    } else {
        /* VEC_BOOL: 1-byte elements, byte-shuffle is a no-op. */
        req->spec.model      = TDC_MODEL_RAW;
        req->spec.entropy[0] = TDC_ENTROPY_LZ;
    }

    tdc_shape_set_contiguous(&req->block.shape);

    return tdc_block_validate(&req->block);
}

void vtr_codec_tdc_release_request(
    VtrTdcEncodeRequest *req,
    void               *(*realloc_fn)(void *user, void *ptr, size_t n),
    void                *alloc_user) {
    if (!req) return;
    if (req->str_offsets_owned && realloc_fn) {
        realloc_fn(alloc_user, req->str_offsets_owned, 0);
    }
    req->str_offsets_owned = NULL;
}

/* ---------- encode bridge -------------------------------------------------- */

tdc_status vtr_encode_column_tdc(const VecArray         *col,
                                 int64_t                 n_rows,
                                 int                     comp_level,
                                 const VtrQuantizeSpec  *qspec,
                                 const VtrSpatialSpec   *sspec,
                                 tdc_buffer             *block_out) {
    if (!block_out || !block_out->realloc_fn) return TDC_E_INVAL;

    VtrTdcEncodeRequest req;
    tdc_status st = vtr_codec_tdc_prepare_request(&req, col, n_rows,
                                                   comp_level, qspec, sspec,
                                                   block_out->realloc_fn,
                                                   block_out->user);
    if (st != TDC_OK) {
        vtr_codec_tdc_release_request(&req, block_out->realloc_fn, block_out->user);
        return st;
    }

    st = tdc_encode_block(&req.block, &req.spec, block_out);
    vtr_codec_tdc_release_request(&req, block_out->realloc_fn, block_out->user);
    return st;
}

/* ---------- decode bridge -------------------------------------------------- */
/*
 * tdc v0 documents (format.h, near TDC_BLOCK_FLAG_HAS_VALIDITY) that the
 * decoder treats the validity bitmap as opaque pass-through and does NOT
 * surface it back to the caller. The bytes still live in the record, at
 * offset = 80 + side_meta_size + xform_params_size + payload_size, with
 * length validity_size. We re-parse the record header here and copy the
 * bitmap into the destination ourselves. Shared between the fixed-width
 * (_into) and string (_tdc) entry points so they can't drift.
 *
 * When tdc grows native validity decode, deleting this block + every call
 * site reduces to a no-op.
 */
static tdc_status vtr_extract_validity_from_record(const tdc_block_record *hdr,
                                                   const uint8_t          *src,
                                                   size_t                  src_size,
                                                   int64_t                 n_rows,
                                                   uint8_t                *dst_validity) {
    if (dst_validity == NULL) return TDC_OK;
    size_t vbytes = (size_t)vec_validity_bytes(n_rows);
    if (hdr->flags & TDC_BLOCK_FLAG_HAS_VALIDITY) {
        if (hdr->validity_size != (uint32_t)vbytes) return TDC_E_CORRUPT;
        size_t v_off = (size_t)TDC_BLOCK_HEADER_SIZE
                     + hdr->side_meta_size
                     + hdr->xform_params_size
                     + hdr->payload_size;
        if (v_off + vbytes > src_size) return TDC_E_CORRUPT;
        if (vbytes > 0) memcpy(dst_validity, src + v_off, vbytes);
    } else {
        /* No bitmap on disk -> all valid. */
        if (vbytes > 0) memset(dst_validity, 0xFF, vbytes);
        /* Trim trailing bits beyond n_rows so vec_array_all_valid stays accurate. */
        int rem = (int)(n_rows % 8);
        if (rem > 0 && vbytes > 0) {
            dst_validity[vbytes - 1] = (uint8_t)((1u << rem) - 1u);
        }
    }
    return TDC_OK;
}

tdc_status vtr_decode_column_tdc_into(VecType         type,
                                      int64_t         n_rows,
                                      void           *dst_data,
                                      uint8_t        *dst_validity,
                                      const uint8_t  *src,
                                      size_t          src_size) {
    if (!src || n_rows < 0)             return TDC_E_INVAL;
    if (n_rows > 0 && dst_data == NULL) return TDC_E_INVAL;
    if (type == VEC_STRING)             return TDC_E_UNSUPPORTED;
    if (src_size < TDC_BLOCK_HEADER_SIZE) return TDC_E_CORRUPT;

    tdc_dtype expected = vtr_type_to_tdc_dtype(type);
    if (expected == (tdc_dtype)0) return TDC_E_DTYPE;

    /* Pull header (memcpy: src is not guaranteed aligned for tdc_block_record). */
    tdc_block_record hdr;
    memcpy(&hdr, src, TDC_BLOCK_HEADER_SIZE);

    tdc_block dst = {0};
    dst.dtype        = expected;
    dst.layout       = (tdc_layout)hdr.layout;
    dst.shape.rank   = hdr.rank;
    for (uint8_t i = 0; i < hdr.rank && i < TDC_MAX_RANK; ++i) {
        dst.shape.dim[i] = hdr.dim[i];
    }
    tdc_shape_set_contiguous(&dst.shape);
    dst.data = (n_rows > 0) ? dst_data : NULL;

    /* Header rank/shape carry the *block* shape (could be RASTER_2D for
     * spatial); the caller asserts only n_rows. Validate the element count
     * matches across whatever the header advertises. */
    int64_t header_n_elems = 1;
    for (uint8_t i = 0; i < hdr.rank; ++i) header_n_elems *= hdr.dim[i];
    if (header_n_elems != n_rows) return TDC_E_SHAPE;

    tdc_status st = tdc_decode_block_into(src, src_size, &dst);
    if (st != TDC_OK) return st;

    return vtr_extract_validity_from_record(&hdr, src, src_size, n_rows, dst_validity);
}

/*
 * Variable-width (VEC_STRING) decode. Replaces the placeholder
 * offsets/data buffers that vec_array_alloc(VEC_STRING, n) installed
 * with freshly malloc'd buffers sized from the record. tdc returns
 * uint32_t offsets; vectra stores int64_t — we widen on the way out.
 */
static tdc_status vtr_decode_string_column_tdc(VecArray       *col_out,
                                               const uint8_t  *src,
                                               size_t          src_size) {
    if (src_size < TDC_BLOCK_HEADER_SIZE) return TDC_E_CORRUPT;

    tdc_block_record hdr;
    memcpy(&hdr, src, TDC_BLOCK_HEADER_SIZE);

    int64_t n_rows = col_out->length;
    int64_t header_n_elems = 1;
    for (uint8_t i = 0; i < hdr.rank; ++i) header_n_elems *= hdr.dim[i];
    if (header_n_elems != n_rows) return TDC_E_SHAPE;
    if ((tdc_dtype)hdr.dtype != TDC_DT_STRING) return TDC_E_DTYPE;

    /* Fresh dst with NULL data/offsets — tdc_decode_block_varlen
     * insists the caller hasn't pre-populated them. */
    tdc_block dst = {0};
    dst.dtype      = TDC_DT_STRING;
    dst.layout     = (tdc_layout)hdr.layout;
    dst.shape.rank = hdr.rank;
    for (uint8_t i = 0; i < hdr.rank && i < TDC_MAX_RANK; ++i) {
        dst.shape.dim[i] = hdr.dim[i];
    }
    tdc_shape_set_contiguous(&dst.shape);

    tdc_buffer alloc = {0};
    alloc.realloc_fn = vtr_tdc_realloc;

    tdc_status st = tdc_decode_block_varlen(src, src_size, &dst, &alloc);
    if (st != TDC_OK) return st;

    /* Widen uint32 offsets -> int64 (vectra's storage type). */
    uint32_t *u32_offs = (uint32_t *)dst.offsets;
    size_t off_n = (size_t)(n_rows + 1);
    int64_t *new_offs = (int64_t *)malloc(sizeof(int64_t) * off_n);
    if (!new_offs) {
        if (dst.data)    vtr_tdc_realloc(NULL, dst.data,    0);
        if (dst.offsets) vtr_tdc_realloc(NULL, dst.offsets, 0);
        return TDC_E_NOMEM;
    }
    for (size_t i = 0; i < off_n; ++i) new_offs[i] = (int64_t)u32_offs[i];
    int64_t heap_bytes = new_offs[n_rows];
    vtr_tdc_realloc(NULL, dst.offsets, 0);

    /* Free the placeholder allocations vec_array_alloc(VEC_STRING, n)
     * installed (1-byte malloc'd data + calloc'd int64 offsets). */
    free(col_out->buf.str.offsets);
    if (col_out->owns_data) free(col_out->buf.str.data);

    col_out->buf.str.offsets  = new_offs;
    col_out->buf.str.data     = (char *)dst.data;  /* may be NULL when n==0 */
    col_out->buf.str.data_len = heap_bytes;
    col_out->owns_data        = 1;  /* offsets + data both came via malloc */

    return vtr_extract_validity_from_record(&hdr, src, src_size, n_rows,
                                            col_out->validity);
}

tdc_status vtr_decode_column_tdc(VecArray       *col_out,
                                 const uint8_t  *src,
                                 size_t          src_size) {
    if (!col_out) return TDC_E_INVAL;
    if (col_out->type == VEC_STRING) {
        return vtr_decode_string_column_tdc(col_out, src, src_size);
    }

    void *dst_data = NULL;
    switch (col_out->type) {
    case VEC_INT64:  dst_data = col_out->buf.i64; break;
    case VEC_INT32:  dst_data = col_out->buf.i32; break;
    case VEC_INT16:  dst_data = col_out->buf.i16; break;
    case VEC_INT8:   dst_data = col_out->buf.i8;  break;
    case VEC_DOUBLE: dst_data = col_out->buf.dbl; break;
    case VEC_BOOL:   dst_data = col_out->buf.bln; break;
    default:         return TDC_E_DTYPE;
    }

    return vtr_decode_column_tdc_into(col_out->type, col_out->length,
                                      dst_data, col_out->validity,
                                      src, src_size);
}

/* =========================================================================
 * R bridge — round-trip entry points used by the testthat tests.
 * NOT part of the production write/read path.
 *
 *   C_tdc_encode_column : encodes an R vector via the bridge, returns the
 *                         tdc block record bytes as a RAWSXP.
 *                         Dispatches on TYPEOF(x):
 *                           REALSXP -> VEC_DOUBLE
 *                           INTSXP  -> VEC_INT32
 *                           LGLSXP  -> VEC_BOOL  (LGLSXP int -> uint8 0/1)
 *   C_tdc_decode_column : decodes a RAWSXP back into an R vector. The
 *                         element type is supplied as an integer matching
 *                         the original R SEXP type code.
 * ========================================================================= */

static VecType r_sxp_to_vectype(SEXPTYPE t) {
    switch (t) {
    case REALSXP: return VEC_DOUBLE;
    case INTSXP:  return VEC_INT32;
    case LGLSXP:  return VEC_BOOL;
    default:      return (VecType)-1;
    }
}

SEXP C_tdc_encode_column(SEXP x_sexp, SEXP comp_level_sexp) {
    if (TYPEOF(comp_level_sexp) != INTSXP || LENGTH(comp_level_sexp) != 1)
        Rf_error("C_tdc_encode_column: comp_level must be a scalar integer");

    VecType vt = r_sxp_to_vectype(TYPEOF(x_sexp));
    if ((int)vt < 0)
        Rf_error("C_tdc_encode_column: unsupported R type %d", (int)TYPEOF(x_sexp));

    int64_t n = (int64_t)Rf_xlength(x_sexp);
    int comp_level = INTEGER(comp_level_sexp)[0];

    /* For LGLSXP we materialize a temporary uint8 buffer because LGLSXP
     * is stored as int32 in R but VEC_BOOL is 1-byte 0/1. */
    uint8_t *bln_tmp = NULL;
    VecArray col = {0};
    col.type   = vt;
    col.length = n;
    switch (vt) {
    case VEC_DOUBLE: col.buf.dbl = REAL(x_sexp); break;
    case VEC_INT32:  col.buf.i32 = INTEGER(x_sexp); break;
    case VEC_BOOL: {
        bln_tmp = (uint8_t *)((n > 0) ? R_alloc((size_t)n, 1) : NULL);
        const int *src = LOGICAL(x_sexp);
        for (int64_t i = 0; i < n; ++i) {
            int v = src[i];
            if (v == NA_LOGICAL) v = 0;  /* validity-less round-trip */
            bln_tmp[i] = v ? 1u : 0u;
        }
        col.buf.bln = bln_tmp;
        break;
    }
    default: Rf_error("internal: unhandled VecType in C_tdc_encode_column");
    }

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

SEXP C_tdc_decode_column(SEXP raw_sexp, SEXP n_sexp, SEXP r_type_sexp) {
    if (TYPEOF(raw_sexp) != RAWSXP)
        Rf_error("C_tdc_decode_column: raw must be RAWSXP");
    if (TYPEOF(n_sexp) != INTSXP && TYPEOF(n_sexp) != REALSXP)
        Rf_error("C_tdc_decode_column: n must be numeric scalar");
    if (TYPEOF(r_type_sexp) != INTSXP || LENGTH(r_type_sexp) != 1)
        Rf_error("C_tdc_decode_column: r_type must be a scalar integer SEXPTYPE");

    R_xlen_t n = (TYPEOF(n_sexp) == INTSXP)
        ? (R_xlen_t)INTEGER(n_sexp)[0]
        : (R_xlen_t)REAL(n_sexp)[0];
    SEXPTYPE rt = (SEXPTYPE)INTEGER(r_type_sexp)[0];
    VecType  vt = r_sxp_to_vectype(rt);
    if ((int)vt < 0)
        Rf_error("C_tdc_decode_column: unsupported r_type %d", (int)rt);

    SEXP out = PROTECT(allocVector(rt, n));

    /* For LGLSXP the on-disk dtype is u8 but R needs int32; decode into
     * a scratch byte buffer and widen. For REALSXP / INTSXP we can
     * decode straight into the SEXP backing store. */
    void   *dst_data    = NULL;
    uint8_t *bln_tmp    = NULL;
    switch (vt) {
    case VEC_DOUBLE: dst_data = REAL(out);    break;
    case VEC_INT32:  dst_data = INTEGER(out); break;
    case VEC_BOOL:
        bln_tmp  = (uint8_t *)((n > 0) ? R_alloc((size_t)n, 1) : NULL);
        dst_data = bln_tmp;
        break;
    default: Rf_error("internal: unhandled VecType in C_tdc_decode_column");
    }

    tdc_status st = vtr_decode_column_tdc_into(vt, (int64_t)n,
                                               dst_data, NULL,
                                               RAW(raw_sexp),
                                               (size_t)Rf_xlength(raw_sexp));
    if (st != TDC_OK)
        Rf_error("vtr_decode_column_tdc_into failed (status=%d)", (int)st);

    if (vt == VEC_BOOL) {
        int *dst = LOGICAL(out);
        for (R_xlen_t i = 0; i < n; ++i) dst[i] = bln_tmp[i] ? TRUE : FALSE;
    }

    UNPROTECT(1);
    return out;
}
