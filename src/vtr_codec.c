#include "vtr_codec_internal.h"
#include "array.h"
#include "error.h"

#include <stdlib.h>
#include <string.h>
#include <math.h>

/* ================================================================
 * vtr_codec.c — top-level encode/decode dispatch
 *
 * This file contains only the public entry points that compose the
 * per-column encodings (vtr_encodings.c) with the compression backend
 * (vtr_compress.c). All helper functions (plain/dict/delta/diff/
 * sparse/spatial encoders, LZ bridge, shuffle kernels) live in those
 * two sibling units.
 * ================================================================ */

/* ---------- profile API wrappers ----------
 *
 * The counters themselves are defined in vtr_compress.c (so the
 * compression hot path can reference them directly without a
 * vtable). These small wrappers expose the counters to the R-side
 * init.c bridge. */
void vtr_codec_profile_reset(void) {
    g_prof_decompress_ns = 0;
    g_prof_unshuffle_ns  = 0;
    g_prof_decode_ns     = 0;
    g_prof_calls         = 0;
    g_prof_sse2_unshuffle_calls = 0;
}
void vtr_codec_profile_get(uint64_t *decompress_ns, uint64_t *unshuffle_ns,
                           uint64_t *decode_ns, uint64_t *calls) {
    if (decompress_ns) *decompress_ns = g_prof_decompress_ns;
    if (unshuffle_ns)  *unshuffle_ns  = g_prof_unshuffle_ns;
    if (decode_ns)     *decode_ns     = g_prof_decode_ns;
    if (calls)         *calls         = g_prof_calls;
}

/* ================================================================
 * Top-level encode/decode
 * ================================================================ */

VtrEncodedCol vtr_encode_column_ex(const VecArray *col, int64_t n_rows,
                                   int comp_level) {
    VtrEncodedCol result;
    memset(&result, 0, sizeof(result));

    if (n_rows == 0) {
        result.encoding = VTR_ENC_PLAIN;
        result.compression = VTR_COMP_NONE;
        result.data = (uint8_t *)malloc(1);
        result.data_size = 0;
        result.uncompressed_size = 0;
        return result;
    }

    /* Choose encoding */
    uint8_t *raw = NULL;
    uint32_t raw_size = 0;

    if (col->type == VEC_STRING) {
        raw = try_dict_encode(col, n_rows, &raw_size);
        if (raw) result.encoding = VTR_ENC_DICTIONARY;
    }
    if (!raw && col->type == VEC_INT64 && should_delta_encode(col, n_rows)) {
        raw = delta_encode(col, n_rows, &raw_size);
        result.encoding = VTR_ENC_DELTA;
    }
    /* Sparse-zero: try first for int64/double. Encodes a bitmap of
       non-zero positions + dense list of non-zero values. Beats dict
       when the column is ≥ 75% zero because the non-zero values never
       pay an index-stream cost. Dict picks up the slack for non-sparse
       low-cardinality columns below. */
    if (!raw && comp_level != VTR_COMPRESS_NONE &&
        (col->type == VEC_INT64 || col->type == VEC_DOUBLE)) {
        raw = try_sparse_zero_encode(col, n_rows, comp_level, &raw_size);
        if (raw) {
            /* SPARSE_ZERO produces a self-contained blob that is already
               internally compressed (per-stream shuffle + LZ so gaps and
               values don't share entropy tables). The outer compression
               pass would only add overhead, so short-circuit here with
               compression = NONE. */
            result.encoding = VTR_ENC_SPARSE_ZERO;
            result.uncompressed_size = raw_size;
            result.data = raw;
            result.data_size = raw_size;
            result.compression = VTR_COMP_NONE;
            return result;
        }
    }
    /* Numeric dict: try for int64/double with low cardinality. Checked
       before DIFF because for low-card columns dict is a huge ratio win
       and DIFF still leaves 8-byte-wide residuals. Skipped when no
       compression is requested (DICT only shines after LZ on the index
       stream). */
    if (!raw && comp_level != VTR_COMPRESS_NONE &&
        (col->type == VEC_INT64 || col->type == VEC_DOUBLE)) {
        raw = try_dict_num_encode(col, n_rows, &raw_size);
        if (raw) result.encoding = VTR_ENC_DICT_NUM;
    }
    /* Skip DIFF encoding when no compression is requested: DIFF only
       reduces entropy for downstream compression and introduces
       floating-point precision loss for doubles.  Skipping also enables
       zero-copy direct reads in the collect fast path. */
    if (!raw && comp_level != VTR_COMPRESS_NONE &&
        should_diff_encode(col, n_rows)) {
        raw = diff_encode(col, n_rows, &raw_size);
        if (raw) result.encoding = VTR_ENC_DIFF;
    }
    if (!raw) {
        raw = plain_encode(col, n_rows, &raw_size);
        result.encoding = VTR_ENC_PLAIN;
    }

    result.uncompressed_size = raw_size;

    if (comp_level == VTR_COMPRESS_NONE || raw_size <= COMPRESS_THRESHOLD) {
        result.data = raw;
        result.data_size = raw_size;
        result.compression = VTR_COMP_NONE;
        return result;
    }

    /* Determine if shuffle is applicable */
    uint8_t es = vtr_shuffle_elem_size(col->type, result.encoding);
    uint32_t n_elems = (es > 0) ? raw_size / es : 0;

    /* For PLAIN fixed-width encoding, shuffle directly from column buffer
       and reuse raw as the shuffle destination (avoid extra alloc+copy). */
    uint8_t *work = NULL;
    int shuffled_in_raw = 0;
    if (es > 0 && n_elems > 0 && result.encoding == VTR_ENC_PLAIN &&
        col->type != VEC_STRING && col->type != VEC_BOOL) {
        /* raw is a malloc'd copy of the column data. Shuffle from the
           original column buffer directly into raw, repurposing it. */
        const uint8_t *src_ptr = NULL;
        switch (col->type) {
        case VEC_INT64:  src_ptr = (const uint8_t *)col->buf.i64; break;
        case VEC_INT32:  src_ptr = (const uint8_t *)col->buf.i32; break;
        case VEC_INT16:  src_ptr = (const uint8_t *)col->buf.i16; break;
        case VEC_DOUBLE: src_ptr = (const uint8_t *)col->buf.dbl; break;
        default: break;
        }
        if (src_ptr) {
            byte_shuffle(raw, src_ptr, n_elems, es);
            shuffled_in_raw = 1;
        }
    }
    if (es > 0 && n_elems > 0 && !shuffled_in_raw) {
        work = (uint8_t *)malloc(raw_size);
        if (!work) vectra_error("alloc failed in vtr_encode_column_ex");
        byte_shuffle(work, raw, n_elems, es);
    }

    const uint8_t *to_compress = shuffled_in_raw ? raw : (work ? work : raw);
    uint32_t comp_size = 0;
    uint8_t *comp = NULL;
    uint8_t comp_tag = VTR_COMP_NONE;

    comp = vtr_compress_shuffled(to_compress, raw_size, comp_level,
                                 &comp_size, &comp_tag);

    if (comp) {
        free(raw);
        free(work);
        result.data = comp;
        result.data_size = comp_size;
        result.compression = comp_tag;
        return result;
    }

    /* Compression didn't help — return uncompressed.
       If we shuffled in-place into raw, we need to restore original data. */
    free(work);
    if (shuffled_in_raw) {
        /* Re-encode from source since raw was overwritten with shuffled data */
        free(raw);
        raw = plain_encode(col, n_rows, &raw_size);
    }
    result.data = raw;
    result.data_size = raw_size;
    result.compression = VTR_COMP_NONE;
    return result;
}

VtrEncodedCol vtr_encode_column_q(const VecArray *col, int64_t n_rows,
                                  int comp_level,
                                  const VtrQuantizeSpec *qspec) {
    if (!qspec || !qspec->enabled || col->type != VEC_DOUBLE)
        return vtr_encode_column_ex(col, n_rows, comp_level);

    VtrEncodedCol result;
    memset(&result, 0, sizeof(result));

    if (n_rows == 0) {
        result.encoding = VTR_ENC_QUANTIZE;
        result.compression = VTR_COMP_NONE;
        result.data = (uint8_t *)malloc(1);
        result.data_size = 0;
        result.uncompressed_size = 0;
        result.quantize_scale = qspec->scale;
        result.quantize_offset = qspec->offset;
        result.quantize_target_type = (uint8_t)qspec->target_type;
        return result;
    }

    /* Quantize float64 → narrow int */
    VecType tt = qspec->target_type;
    uint8_t es = vec_type_elem_size(tt);
    uint32_t raw_size = (uint32_t)((uint32_t)n_rows * es);
    uint8_t *raw = (uint8_t *)calloc(raw_size > 0 ? raw_size : 1, 1);
    if (!raw) vectra_error("alloc failed in vtr_encode_column_q");

    int overflow = 0;
    quantize_float_to_int(col->buf.dbl, n_rows, col->validity,
                          qspec->scale, qspec->offset, tt, raw, &overflow);

    result.encoding = VTR_ENC_QUANTIZE;
    result.quantize_scale = qspec->scale;
    result.quantize_offset = qspec->offset;
    result.quantize_target_type = (uint8_t)tt;
    result.quantize_overflow = overflow;
    result.uncompressed_size = raw_size;

    if (comp_level == VTR_COMPRESS_NONE || raw_size <= COMPRESS_THRESHOLD) {
        result.data = raw;
        result.data_size = raw_size;
        result.compression = VTR_COMP_NONE;
        return result;
    }

    /* Byte-shuffle if elem_size > 1 */
    uint8_t *work = NULL;
    uint32_t n_elems = raw_size / es;
    if (es > 1 && n_elems > 0) {
        work = (uint8_t *)malloc(raw_size);
        if (!work) vectra_error("alloc failed in vtr_encode_column_q");
        byte_shuffle(work, raw, n_elems, es);
    }

    const uint8_t *to_compress = work ? work : raw;
    uint32_t comp_size = 0;
    uint8_t *comp = NULL;
    uint8_t comp_tag = VTR_COMP_NONE;

    comp = vtr_compress_shuffled(to_compress, raw_size, comp_level,
                                 &comp_size, &comp_tag);

    if (comp) {
        free(raw);
        free(work);
        result.data = comp;
        result.data_size = comp_size;
        result.compression = comp_tag;
        return result;
    }

    free(work);
    result.data = raw;
    result.data_size = raw_size;
    result.compression = VTR_COMP_NONE;
    return result;
}

/* Legacy wrapper: uses LZ_VTR (backward compat for old callers) */
VtrEncodedCol vtr_encode_column(const VecArray *col, int64_t n_rows) {
    return vtr_encode_column_ex(col, n_rows, VTR_COMPRESS_FAST);
}

void vtr_decode_column(VecArray *col, int64_t n_rows,
                       uint8_t encoding, uint8_t compression,
                       const uint8_t *data, uint32_t data_size,
                       uint32_t uncompressed_size) {
    if (n_rows == 0) return;

    /* Decompress + unshuffle if needed */
    const uint8_t *decoded_data = data;
    uint8_t *decompressed = NULL;

    if (compression == VTR_COMP_SHUFFLE_LZ ||
        compression == VTR_COMP_SHUFFLE_LZ_HUFF ||
        compression == VTR_COMP_SHUFFLE_LZ_STREAMS) {
        PROF_TIME_START(t0);
        decompressed = (uint8_t *)malloc((size_t)uncompressed_size);
        if (!decompressed) vectra_error("alloc failed in vtr_decode_column");
        uint8_t es = vtr_shuffle_elem_size(col->type, encoding);
        vtr_decompress_unshuffle_into(decompressed, uncompressed_size,
                                      data, data_size, compression, es);
        PROF_TIME_ACC(g_prof_decompress_ns, t0);
        PROF_INC(g_prof_calls);
        decoded_data = decompressed;
        data_size = uncompressed_size;
    } else if (compression != VTR_COMP_NONE) {
        vectra_error("unknown compression tag: 0x%02x", compression);
    }

    /* Decode */
    PROF_TIME_START(td0);
    switch (encoding) {
    case VTR_ENC_PLAIN:
        plain_decode(col, n_rows, decoded_data, data_size);
        break;
    case VTR_ENC_DICTIONARY:
        dict_decode(col, n_rows, decoded_data, data_size);
        break;
    case VTR_ENC_DICT_NUM:
        dict_num_decode(col, n_rows, decoded_data, data_size);
        break;
    case VTR_ENC_SPARSE_ZERO:
        sparse_zero_decode(col, n_rows, decoded_data, data_size);
        break;
    case VTR_ENC_DELTA:
        delta_decode(col, n_rows, decoded_data, data_size);
        break;
    case VTR_ENC_DIFF:
        diff_decode(col, n_rows, decoded_data, data_size);
        break;
    default:
        if (decompressed) free(decompressed);
        vectra_error("unknown encoding tag: 0x%02x", encoding);
    }
    PROF_TIME_ACC(g_prof_decode_ns, td0);

    free(decompressed);
}

void vtr_decode_column_raw(VecArray *col, int64_t n_rows,
                           uint8_t encoding,
                           const uint8_t *data, uint32_t data_size) {
    if (n_rows == 0) return;
    switch (encoding) {
    case VTR_ENC_PLAIN:
        plain_decode(col, n_rows, data, data_size);
        break;
    case VTR_ENC_DICTIONARY:
        dict_decode(col, n_rows, data, data_size);
        break;
    case VTR_ENC_DICT_NUM:
        dict_num_decode(col, n_rows, data, data_size);
        break;
    case VTR_ENC_SPARSE_ZERO:
        sparse_zero_decode(col, n_rows, data, data_size);
        break;
    case VTR_ENC_DELTA:
        delta_decode(col, n_rows, data, data_size);
        break;
    case VTR_ENC_DIFF:
        diff_decode(col, n_rows, data, data_size);
        break;
    default:
        vectra_error("unknown encoding tag: 0x%02x", encoding);
    }
}

int vtr_decode_column_raw_into(VecType type, int64_t n_rows,
                               uint8_t encoding,
                               const uint8_t *src, uint32_t src_size,
                               void *dst) {
    (void)src_size;
    if (n_rows == 0) return 1;
    if (!dst || !src) return 0;

    switch (encoding) {
    case VTR_ENC_PLAIN: {
        uint8_t es = vec_type_elem_size(type);
        if (es == 0) return 0; /* variable-length / unsupported */
        memcpy(dst, src, (size_t)n_rows * es);
        return 1;
    }
    case VTR_ENC_DELTA: {
        if (type != VEC_INT64) return 0;
        int64_t *out = (int64_t *)dst;
        int64_t val;
        memcpy(&val, src, 8);
        out[0] = val;
        for (int64_t i = 1; i < n_rows; i++) {
            memcpy(&val, src + i * 8, 8);
            out[i] = out[i - 1] + val;
        }
        return 1;
    }
    case VTR_ENC_DIFF: {
        if (type == VEC_INT64) {
            int64_t *out = (int64_t *)dst;
            const int64_t *in = (const int64_t *)src;
            out[0] = in[0];
            for (int64_t i = 1; i < n_rows; i++)
                out[i] = out[i - 1] + in[i];
            return 1;
        }
        if (type == VEC_DOUBLE) {
            double *out = (double *)dst;
            const double *in = (const double *)src;
            out[0] = in[0];
            for (int64_t i = 1; i < n_rows; i++)
                out[i] = out[i - 1] + in[i];
            return 1;
        }
        return 0; /* narrow ints not in the direct-write contract today */
    }
    case VTR_ENC_DICT_NUM: {
        if (type != VEC_INT64 && type != VEC_DOUBLE) return 0;
        uint32_t dict_count;
        uint8_t idx_width, value_bytes;
        const uint8_t *dict_vals;
        const uint8_t *indices;
        if (!dict_num_parse_header(src, src_size, &dict_count, &idx_width,
                                   &value_bytes, &dict_vals, &indices))
            return 0;
        dict_num_fanout_u64((uint64_t *)dst, n_rows, dict_vals, dict_count,
                            indices, idx_width);
        return 1;
    }
    case VTR_ENC_SPARSE_ZERO: {
        if (type != VEC_INT64 && type != VEC_DOUBLE) return 0;
        SparseZeroView v;
        if (!sparse_zero_parse_header(src, src_size, n_rows, &v))
            return 0;
        sparse_zero_fanout_u64((uint64_t *)dst, n_rows, &v);
        return 1;
    }
    default:
        return 0; /* DICTIONARY / QUANTIZE / SPATIAL handled by their own paths */
    }
}

/* ================================================================
 * Spatial-aware encode entry point
 *
 * If sspec is enabled, applies spatial prediction to the column
 * (after optional quantization), converting values to int64 residuals.
 * The residual column is then encoded with the standard pipeline
 * (DIFF auto-selection, compression, shuffle).
 * ================================================================ */

VtrEncodedCol vtr_encode_column_qs(const VecArray *col, int64_t n_rows,
                                   int comp_level,
                                   const VtrQuantizeSpec *qspec,
                                   const VtrSpatialSpec *sspec) {
    /* If no spatial spec, fall through to quantize-only path */
    if (!sspec || !sspec->enabled)
        return vtr_encode_column_q(col, n_rows, comp_level, qspec);

    /* Validate grid dimensions */
    uint32_t nx = sspec->nx;
    uint32_t ny = sspec->ny;
    if ((int64_t)nx * ny != n_rows)
        vectra_error("spatial: nx*ny (%u*%u=%llu) != n_rows (%lld)",
                     nx, ny, (unsigned long long)nx * ny, (long long)n_rows);

    /* First, apply quantization if requested (produces narrow int column) */
    const VecArray *src_col = col;
    VecArray q_col;
    int q_allocated = 0;

    if (qspec && qspec->enabled && col->type == VEC_DOUBLE) {
        /* Quantize to narrow int in a temp VecArray */
        memset(&q_col, 0, sizeof(q_col));
        q_col.type = qspec->target_type;
        q_col.length = n_rows;
        q_col.owns_data = 1;
        uint8_t es = vec_type_elem_size(qspec->target_type);
        uint32_t raw_size = (uint32_t)((uint32_t)n_rows * es);

        switch (qspec->target_type) {
        case VEC_INT8:
            q_col.buf.i8 = (int8_t *)calloc(raw_size > 0 ? raw_size : 1, 1);
            break;
        case VEC_INT16:
            q_col.buf.i16 = (int16_t *)calloc(raw_size > 0 ? raw_size : 1, 1);
            break;
        case VEC_INT32:
            q_col.buf.i32 = (int32_t *)calloc(raw_size > 0 ? raw_size : 1, 1);
            break;
        default:
            break;
        }
        q_col.validity = (uint8_t *)malloc((size_t)vec_validity_bytes(n_rows));
        if (!q_col.validity) vectra_error("alloc failed");
        memcpy(q_col.validity, col->validity, (size_t)vec_validity_bytes(n_rows));

        int overflow = 0;
        uint8_t *dst_ptr = NULL;
        switch (qspec->target_type) {
        case VEC_INT8:  dst_ptr = (uint8_t *)q_col.buf.i8;  break;
        case VEC_INT16: dst_ptr = (uint8_t *)q_col.buf.i16; break;
        case VEC_INT32: dst_ptr = (uint8_t *)q_col.buf.i32; break;
        default: break;
        }
        quantize_float_to_int(col->buf.dbl, n_rows, col->validity,
                              qspec->scale, qspec->offset, qspec->target_type,
                              dst_ptr, &overflow);
        src_col = &q_col;
        q_allocated = 1;
    }

    /* Get source data pointer */
    const void *src_data = NULL;
    VecType src_type = src_col->type;
    switch (src_type) {
    case VEC_INT64:  src_data = src_col->buf.i64; break;
    case VEC_INT32:  src_data = src_col->buf.i32; break;
    case VEC_INT16:  src_data = src_col->buf.i16; break;
    case VEC_INT8:   src_data = src_col->buf.i8;  break;
    case VEC_DOUBLE: src_data = src_col->buf.dbl; break;
    default:
        if (q_allocated) { free(q_col.validity); }
        vectra_error("spatial encoding requires numeric column");
    }

    /* Choose predictor */
    int predictor = sspec->predictor;
    if (predictor < 0)
        predictor = auto_select_predictor(src_data, src_type, n_rows, nx, ny);

    /* Compute residuals as int64 */
    int64_t *residuals = (int64_t *)malloc((size_t)n_rows * sizeof(int64_t));
    if (!residuals) vectra_error("alloc failed in spatial encode");

    int32_t *coeffs = NULL;
    uint32_t n_tiles = 0;
    uint16_t tile_size = sspec->tile_size > 0 ? sspec->tile_size : 32;

    if (predictor == VTR_PRED_PLANE) {
        coeffs = plane_encode(src_data, src_type, residuals, n_rows,
                              nx, ny, tile_size, &n_tiles);
    } else {
        spatial_encode_int(src_data, src_type, residuals, n_rows,
                           nx, ny, predictor);
    }

    if (q_allocated) {
        /* Free temp quantized column */
        switch (q_col.type) {
        case VEC_INT8:  free(q_col.buf.i8);  break;
        case VEC_INT16: free(q_col.buf.i16); break;
        case VEC_INT32: free(q_col.buf.i32); break;
        default: break;
        }
        free(q_col.validity);
    }

    /* Build a temp VecArray from the int64 residuals for encoding */
    VecArray res_col;
    memset(&res_col, 0, sizeof(res_col));
    res_col.type = VEC_INT64;
    res_col.length = n_rows;
    res_col.buf.i64 = residuals;
    res_col.owns_data = 0; /* we'll free residuals ourselves */
    res_col.validity = col->validity; /* borrow original validity */

    /* Encode residuals as PLAIN int64 (no DIFF/DELTA auto-selection — we don't
       store the inner encoding tag, so the reader expects raw int64).
       Apply shuffle + compression directly. */
    uint32_t raw_size = (uint32_t)((uint32_t)n_rows * 8);
    uint8_t *raw = (uint8_t *)malloc(raw_size);
    if (!raw) { free(residuals); vectra_error("alloc failed"); }
    memcpy(raw, residuals, raw_size);
    free(residuals);

    VtrEncodedCol result;
    memset(&result, 0, sizeof(result));
    result.encoding = VTR_ENC_SPATIAL;
    result.uncompressed_size = raw_size;

    if (comp_level == VTR_COMPRESS_NONE || raw_size <= COMPRESS_THRESHOLD) {
        result.data = raw;
        result.data_size = raw_size;
        result.compression = VTR_COMP_NONE;
    } else {
        /* Byte-shuffle int64 (elem_size=8) then compress */
        uint8_t *work = (uint8_t *)malloc(raw_size);
        if (!work) { free(raw); vectra_error("alloc failed"); }
        byte_shuffle(work, raw, (uint32_t)n_rows, 8);

        uint32_t comp_size = 0;
        uint8_t comp_tag = VTR_COMP_NONE;
        uint8_t *comp = vtr_compress_shuffled(work, raw_size, comp_level,
                                              &comp_size, &comp_tag);

        if (comp) {
            free(raw); free(work);
            result.data = comp;
            result.data_size = comp_size;
            result.compression = comp_tag;
        } else {
            free(work);
            result.data = raw;
            result.data_size = raw_size;
            result.compression = VTR_COMP_NONE;
        }
    }

    result.encoding = VTR_ENC_SPATIAL;
    result.spatial_predictor = (uint8_t)predictor;
    result.spatial_nx = nx;
    result.spatial_ny = ny;
    result.spatial_tile_size = tile_size;
    result.spatial_n_tiles = n_tiles;
    result.spatial_coeffs = coeffs; /* caller frees after writing */

    /* Carry quantize metadata if quantization was applied */
    if (qspec && qspec->enabled && col->type == VEC_DOUBLE) {
        result.quantize_scale = qspec->scale;
        result.quantize_offset = qspec->offset;
        result.quantize_target_type = (uint8_t)qspec->target_type;
    }

    return result;
}
