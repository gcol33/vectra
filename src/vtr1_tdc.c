/*
 * vtr1_tdc.c — tdc-backed row-group container writer/reader.
 *
 * The on-disk format is a tdc container (TDC_CONTAINER_MAGIC,
 * HETEROGENEOUS flag, attached schema, trailing row-group index) plus
 * one self-describing tdc_block_record per column per row group.
 *
 * Reader strategy: tdc_stream_decoder parses the header, schema, and
 * index at open time. We deep-copy what we need (schema -> VecSchema,
 * per-row-group offset+size table, per-row-group col_stats) and then
 * ignore the decoder's read_block API. Block bytes are fseek/fread'd
 * from our own FILE* and handed to vtr_decode_column_tdc, which
 * extracts the validity bitmap that tdc v0 leaves opaque.
 *
 * Per-column statistics are computed during encode and attached via
 * tdc_stream_encoder_set_rowgroup_stats. The dtype-native value bytes
 * go into the leading 8 bytes of the 16-byte min/max slots
 * (little-endian). Empty row groups skip the stats call so the
 * reader sees NULL via tdc_stream_decoder_get_stats.
 *
 * The annotation slot carries a length-prefixed VecType discriminator
 * followed by the verbatim user annotation; see vtr1_tdc.h for layout.
 */

#include "vtr1_tdc.h"
#include "vtr_codec_tdc.h"
#include "schema.h"
#include "batch.h"
#include "array.h"
#include "error.h"

#include "tdc/types.h"
#include "tdc/format.h"
#include "tdc/stream.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---------- realloc shim ------------------------------------------------- */

static void *vtr1_tdc_realloc(void *user, void *ptr, size_t new_size) {
    (void)user;
    if (new_size == 0) { free(ptr); return NULL; }
    return realloc(ptr, new_size);
}

/* ---------- FILE* I/O callbacks ------------------------------------------ */

static tdc_status vtr1_tdc_io_write(void *ctx, const void *data, size_t size) {
    if (size == 0) return TDC_OK;
    FILE *fp = (FILE *)ctx;
    if (fwrite(data, 1, size, fp) != size) return TDC_E_IO;
    return TDC_OK;
}

static tdc_status vtr1_tdc_io_read(void *ctx, void *buf, size_t size,
                                   size_t *bytes_read) {
    FILE *fp = (FILE *)ctx;
    size_t n = fread(buf, 1, size, fp);
    if (bytes_read) *bytes_read = n;
    if (n < size && ferror(fp)) return TDC_E_IO;
    return TDC_OK;
}

static tdc_status vtr1_tdc_io_seek(void *ctx, int64_t offset, int whence) {
    FILE *fp = (FILE *)ctx;
    int sw;
    switch (whence) {
    case TDC_SEEK_SET: sw = SEEK_SET; break;
    case TDC_SEEK_CUR: sw = SEEK_CUR; break;
    case TDC_SEEK_END: sw = SEEK_END; break;
    default:           return TDC_E_INVAL;
    }
#if defined(_WIN32)
    if (_fseeki64(fp, offset, sw) != 0) return TDC_E_IO;
#else
    if (fseeko(fp, (off_t)offset, sw) != 0) return TDC_E_IO;
#endif
    return TDC_OK;
}

/* ---------- schema mapping ----------------------------------------------- */

/* Annotation slot layout:
 *
 *     byte 0       : vt_name_len (uint8, in [3..6] for current types)
 *     bytes 1..k   : vec_type_name (no NUL), k = vt_name_len
 *     bytes k+1..n : user annotation (no NUL), n = ann_len
 *
 * The length-prefix carries the VecType discriminator unambiguously
 * (separating VEC_BOOL from a hypothetical future u8 mapping); the
 * remainder is the verbatim VecSchema.col_annotations[i] payload. The
 * empty user annotation collapses to a 1+k-byte slot. */

static VecType vec_type_from_name(const char *s, size_t len) {
    if (len == 5 && memcmp(s, "int64", 5) == 0)  return VEC_INT64;
    if (len == 6 && memcmp(s, "double", 6) == 0) return VEC_DOUBLE;
    if (len == 4 && memcmp(s, "bool", 4) == 0)   return VEC_BOOL;
    if (len == 6 && memcmp(s, "string", 6) == 0) return VEC_STRING;
    if (len == 4 && memcmp(s, "int8", 4) == 0)   return VEC_INT8;
    if (len == 5 && memcmp(s, "int16", 5) == 0)  return VEC_INT16;
    if (len == 5 && memcmp(s, "int32", 5) == 0)  return VEC_INT32;
    return (VecType)-1;
}

/* Build the packed annotation byte string. Caller frees with free().
 * Always non-NULL for any valid VecType. */
static char *vtr1_tdc_pack_annotation(VecType t, const char *user_ann,
                                      uint16_t *out_len) {
    const char *vname = vec_type_name(t);
    size_t vlen = strlen(vname);
    size_t ulen = user_ann ? strlen(user_ann) : 0;
    size_t total = 1 + vlen + ulen;
    if (total > UINT16_MAX) {
        vectra_error("annotation too large for tdc schema slot (%zu bytes)", total);
    }
    char *buf = (char *)malloc(total > 0 ? total : 1);
    if (!buf) vectra_error("alloc failed for annotation");
    buf[0] = (char)(uint8_t)vlen;
    memcpy(buf + 1, vname, vlen);
    if (ulen > 0) memcpy(buf + 1 + vlen, user_ann, ulen);
    *out_len = (uint16_t)total;
    return buf;
}

/* Parse a packed annotation. *out_user_ann is malloc'd (caller frees) when
 * a user annotation is present, or set to NULL when there is none. Returns
 * (VecType)-1 on malformed input. */
static VecType vtr1_tdc_parse_annotation(const char *data, uint16_t len,
                                         char **out_user_ann) {
    *out_user_ann = NULL;
    if (len < 1) return (VecType)-1;
    uint8_t vlen = (uint8_t)data[0];
    if ((size_t)vlen + 1 > (size_t)len) return (VecType)-1;
    VecType t = vec_type_from_name(data + 1, vlen);
    if ((int)t < 0) return (VecType)-1;
    size_t ulen = (size_t)len - 1 - (size_t)vlen;
    if (ulen > 0) {
        char *u = (char *)malloc(ulen + 1);
        if (!u) vectra_error("alloc failed for user annotation");
        memcpy(u, data + 1 + vlen, ulen);
        u[ulen] = '\0';
        *out_user_ann = u;
    }
    return t;
}

/* ---------- per-column statistics ---------------------------------------- */

/* Encode an int64 / double / bool min/max value into the leading 8 bytes
 * of a 16-byte tdc_column_stats slot. The field is little-endian dtype-
 * native bytes per the tdc/stream.h contract; the trailing 8 bytes are
 * zero-filled so byte-equality comparisons across encoders are stable. */
static void put_le_u64(uint8_t out[TDC_STATS_VALUE_SIZE], uint64_t v) {
    memset(out, 0, TDC_STATS_VALUE_SIZE);
    for (int i = 0; i < 8; i++) out[i] = (uint8_t)((v >> (8 * i)) & 0xFFu);
}

static uint64_t get_le_u64(const uint8_t in[TDC_STATS_VALUE_SIZE]) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= ((uint64_t)in[i]) << (8 * i);
    return v;
}

/* Count NA rows in a validity bitmap. validity == NULL means all valid. */
static uint64_t count_nulls(const uint8_t *validity, int64_t n_rows) {
    if (!validity || n_rows <= 0) return 0;
    uint64_t valid = 0;
    int64_t full = n_rows / 8;
    for (int64_t b = 0; b < full; b++) {
        uint8_t v = validity[b];
        v = (uint8_t)((v & 0x55) + ((v >> 1) & 0x55));
        v = (uint8_t)((v & 0x33) + ((v >> 2) & 0x33));
        v = (uint8_t)((v & 0x0F) + ((v >> 4) & 0x0F));
        valid += v;
    }
    int rem = (int)(n_rows % 8);
    if (rem > 0) {
        uint8_t v = (uint8_t)(validity[full] & ((1u << rem) - 1u));
        v = (uint8_t)((v & 0x55) + ((v >> 1) & 0x55));
        v = (uint8_t)((v & 0x33) + ((v >> 2) & 0x33));
        v = (uint8_t)((v & 0x0F) + ((v >> 4) & 0x0F));
        valid += v;
    }
    return (uint64_t)n_rows - valid;
}

/* Compute per-column stats for a single row group. Stats are filled
 * into out[c]; out is the caller-allocated array of n_cols entries.
 * Returns 1 if any column has stats (so the encoder call should fire),
 * 0 if every column collapsed to has_stats == 0 (e.g. zero-row group). */
static int vtr1_tdc_compute_rowgroup_stats(const VecBatch *batch,
                                           tdc_column_stats *out) {
    int any = 0;
    int n_cols = batch->n_cols;
    int64_t n_rows = batch->n_rows;
    for (int c = 0; c < n_cols; c++) {
        memset(&out[c], 0, sizeof(out[c]));
        const VecArray *col = &batch->columns[c];
        if (n_rows == 0) continue;
        out[c].null_count = count_nulls(col->validity, n_rows);

        if (vec_type_is_int(col->type)) {
            int64_t mn = INT64_MAX, mx = INT64_MIN;
            int found = 0;
            for (int64_t i = 0; i < n_rows; i++) {
                if (!vec_array_is_valid(col, i)) continue;
                int64_t v = vec_array_get_int(col, i);
                if (v < mn) mn = v;
                if (v > mx) mx = v;
                found = 1;
            }
            if (found) {
                put_le_u64(out[c].min, (uint64_t)mn);
                put_le_u64(out[c].max, (uint64_t)mx);
                out[c].has_stats = 1;
                any = 1;
            }
        } else if (col->type == VEC_DOUBLE) {
            double mn = HUGE_VAL, mx = -HUGE_VAL;
            int found = 0;
            for (int64_t i = 0; i < n_rows; i++) {
                if (!vec_array_is_valid(col, i)) continue;
                double v = col->buf.dbl[i];
                if (v < mn) mn = v;
                if (v > mx) mx = v;
                found = 1;
            }
            if (found) {
                uint64_t mn_bits, mx_bits;
                memcpy(&mn_bits, &mn, 8);
                memcpy(&mx_bits, &mx, 8);
                put_le_u64(out[c].min, mn_bits);
                put_le_u64(out[c].max, mx_bits);
                out[c].has_stats = 1;
                any = 1;
            }
        } else if (col->type == VEC_BOOL) {
            uint8_t has_false = 0, has_true = 0;
            for (int64_t i = 0; i < n_rows; i++) {
                if (!vec_array_is_valid(col, i)) continue;
                if (col->buf.bln[i]) has_true = 1; else has_false = 1;
            }
            if (has_true || has_false) {
                /* Pack {has_false, has_true} as the {min, max} 0/1 pair to
                 * mirror vtr1.c's Vtr1ColStat.bln layout exactly. */
                put_le_u64(out[c].min, (uint64_t)has_false);
                put_le_u64(out[c].max, (uint64_t)has_true);
                out[c].has_stats = 1;
                any = 1;
            }
        } else if (col->type == VEC_STRING) {
            /* String min/max would need a packed-prefix or dictionary
             * scan; for now we surface only null_count. has_stats=1 lets
             * the reader return a stat vector with NA min/max so the
             * null count is observable in user code. */
            out[c].has_stats = 1;
            any = 1;
        }
    }
    return any;
}

/* ============================================================ writer === */

struct Vtr1TdcWriter {
    FILE                *fp;
    tdc_stream_encoder  *enc;
    VecSchema            schema;       /* deep-copied */
    tdc_column_desc     *desc_buf;     /* sized n_cols, freed at close */
    char               **ann_buf;      /* sized n_cols, packed annotation
                                          payload owned per column */
    int                  n_cols;
};

static void vtr1_tdc_writer_free_ann(Vtr1TdcWriter *w) {
    if (!w || !w->ann_buf) return;
    for (int i = 0; i < w->n_cols; i++) free(w->ann_buf[i]);
    free(w->ann_buf);
    w->ann_buf = NULL;
}

Vtr1TdcWriter *vtr1_open_tdc_writer(const char *path, const VecSchema *schema) {
    if (!path || !schema || schema->n_cols < 0) {
        vectra_error("vtr1_open_tdc_writer: invalid arguments");
    }

    FILE *fp = fopen(path, "wb");
    if (!fp) vectra_error("cannot open file for writing: %s", path);

    Vtr1TdcWriter *w = (Vtr1TdcWriter *)calloc(1, sizeof(*w));
    if (!w) { fclose(fp); vectra_error("alloc failed for Vtr1TdcWriter"); }
    w->fp = fp;
    w->schema = vec_schema_copy(schema);
    w->n_cols = schema->n_cols;

    int n_cols = schema->n_cols;
    if (n_cols > 0) {
        w->desc_buf = (tdc_column_desc *)calloc((size_t)n_cols, sizeof(*w->desc_buf));
        w->ann_buf  = (char **)calloc((size_t)n_cols, sizeof(char *));
        if (!w->desc_buf || !w->ann_buf) {
            vtr1_tdc_writer_free_ann(w);
            free(w->desc_buf);
            vec_schema_free(&w->schema); free(w); fclose(fp);
            vectra_error("alloc failed for tdc_column_desc / annotation array");
        }
        for (int i = 0; i < n_cols; i++) {
            const char *user_ann = (w->schema.col_annotations)
                                 ? w->schema.col_annotations[i] : NULL;
            uint16_t alen = 0;
            w->ann_buf[i] = vtr1_tdc_pack_annotation(w->schema.col_types[i],
                                                     user_ann, &alen);
            w->desc_buf[i].name        = w->schema.col_names[i];
            w->desc_buf[i].name_len    = (uint16_t)strlen(w->schema.col_names[i]);
            w->desc_buf[i].dtype       = (uint8_t)vtr_type_to_tdc_dtype(w->schema.col_types[i]);
            w->desc_buf[i].annotation  = w->ann_buf[i];
            w->desc_buf[i].ann_len     = alen;
        }
    }

    tdc_schema sch = {0};
    sch.n_columns = (uint16_t)n_cols;
    sch.columns   = w->desc_buf;

    tdc_stream_encoder_config cfg = {0};
    cfg.io.write_fn = vtr1_tdc_io_write;
    cfg.io.read_fn  = vtr1_tdc_io_read;
    cfg.io.seek_fn  = vtr1_tdc_io_seek;
    cfg.io.ctx      = fp;
    cfg.flags       = TDC_CONTAINER_FLAG_HETEROGENEOUS;
    cfg.schema      = &sch;
    cfg.realloc_fn  = vtr1_tdc_realloc;
    cfg.alloc_user  = NULL;

    tdc_status st = tdc_stream_encoder_open(&cfg, &w->enc);
    if (st != TDC_OK) {
        vtr1_tdc_writer_free_ann(w);
        free(w->desc_buf);
        vec_schema_free(&w->schema);
        free(w);
        fclose(fp);
        vectra_error("tdc_stream_encoder_open failed: status=%d", (int)st);
    }
    return w;
}

void vtr1_write_rowgroup_tdc(Vtr1TdcWriter        *w,
                             const VecBatch        *batch,
                             int                    comp_level,
                             const VtrQuantizeSpec *qspecs,
                             const VtrSpatialSpec  *sspecs) {
    if (!w || !batch) vectra_error("vtr1_write_rowgroup_tdc: NULL handle/batch");
    if (batch->n_cols != w->schema.n_cols) {
        vectra_error("rowgroup n_cols=%d mismatches schema n_cols=%d",
                     batch->n_cols, w->schema.n_cols);
    }

    int n_cols = batch->n_cols;
    for (int c = 0; c < n_cols; c++) {
        const VecArray *col = &batch->columns[c];
        if (col->type != w->schema.col_types[c]) {
            vectra_error("rowgroup col %d type=%d mismatches schema type=%d",
                         c, (int)col->type, (int)w->schema.col_types[c]);
        }

        const VtrQuantizeSpec *qs = (qspecs && qspecs[c].enabled) ? &qspecs[c] : NULL;
        const VtrSpatialSpec  *ss = (sspecs && sspecs[c].enabled) ? &sspecs[c] : NULL;

        if (ss) {
            uint64_t nxny = (uint64_t)ss->nx * (uint64_t)ss->ny;
            if (nxny != (uint64_t)batch->n_rows) {
                vectra_error("spatial: nx*ny (%u*%u=%llu) != n_rows (%lld)",
                             ss->nx, ss->ny,
                             (unsigned long long)nxny,
                             (long long)batch->n_rows);
            }
        }

        VtrTdcEncodeRequest req;
        tdc_status st = vtr_codec_tdc_prepare_request(
            &req, col, batch->n_rows, comp_level, qs, ss,
            vtr1_tdc_realloc, NULL);
        if (st != TDC_OK) {
            vtr_codec_tdc_release_request(&req, vtr1_tdc_realloc, NULL);
            vectra_error("prepare_request failed for col %d: status=%d", c, (int)st);
        }

        st = tdc_stream_encoder_write_block(w->enc, &req.block, &req.spec);
        vtr_codec_tdc_release_request(&req, vtr1_tdc_realloc, NULL);
        if (st != TDC_OK) {
            vectra_error("tdc_stream_encoder_write_block failed for col %d: status=%d",
                         c, (int)st);
        }
    }

    /* Compute per-column stats and attach them before closing the rowgroup.
     * Skip the call entirely for empty row groups so the reader sees NULL
     * via tdc_stream_decoder_get_stats (matches vtr1.c's has_stats=0). */
    if (n_cols > 0 && batch->n_rows > 0) {
        tdc_column_stats *stats =
            (tdc_column_stats *)calloc((size_t)n_cols, sizeof(tdc_column_stats));
        if (!stats) vectra_error("alloc failed for tdc_column_stats");
        int any = vtr1_tdc_compute_rowgroup_stats(batch, stats);
        if (any) {
            tdc_status sst = tdc_stream_encoder_set_rowgroup_stats(
                w->enc, stats, (uint16_t)n_cols);
            free(stats);
            if (sst != TDC_OK) {
                vectra_error("tdc_stream_encoder_set_rowgroup_stats failed: status=%d",
                             (int)sst);
            }
        } else {
            free(stats);
        }
    }

    tdc_status st = tdc_stream_encoder_end_rowgroup(w->enc, (uint64_t)batch->n_rows);
    if (st != TDC_OK) {
        vectra_error("tdc_stream_encoder_end_rowgroup failed: status=%d", (int)st);
    }
}

void vtr1_close_tdc_writer(Vtr1TdcWriter *w) {
    if (!w) return;
    if (w->enc) {
        tdc_status st = tdc_stream_encoder_close(&w->enc);
        if (st != TDC_OK) {
            /* Don't leak fp/schema even on failure. Surface the error
             * to R after cleanup. */
            vtr1_tdc_writer_free_ann(w);
            free(w->desc_buf);
            vec_schema_free(&w->schema);
            if (w->fp) fclose(w->fp);
            free(w);
            vectra_error("tdc_stream_encoder_close failed: status=%d", (int)st);
        }
    }
    vtr1_tdc_writer_free_ann(w);
    free(w->desc_buf);
    vec_schema_free(&w->schema);
    if (w->fp) fclose(w->fp);
    free(w);
}

/* ============================================================ reader === */

typedef struct {
    int64_t  n_rows;
    /* Per-column raw block byte slices, indexed by schema column. */
    uint64_t *block_offset;  /* length n_cols */
    uint64_t *block_total;   /* length n_cols */
    /* Per-column statistics decoded from the tdc index, in vtr1.c's
     * Vtr1ColStat layout so scan.c can consume both backends with
     * minimal change. NULL when the row group has no stats attached
     * (e.g. zero-row group). */
    Vtr1ColStat *col_stats;
} Vtr1TdcRowgroup;

struct Vtr1TdcFile {
    FILE             *fp;
    VecSchema         schema;
    uint32_t          n_rowgroups;
    Vtr1TdcRowgroup  *rowgroups;  /* length n_rowgroups */
    /* Per-column sorted flag across row groups, length schema.n_cols.
     * NULL when n_rowgroups < 2 or alloc failed. See vtr1_tdc_col_sorted. */
    uint8_t          *col_sorted;
};

static void vtr1_tdc_file_destroy(Vtr1TdcFile *f) {
    if (!f) return;
    if (f->rowgroups) {
        for (uint32_t r = 0; r < f->n_rowgroups; r++) {
            free(f->rowgroups[r].block_offset);
            free(f->rowgroups[r].block_total);
            free(f->rowgroups[r].col_stats);
        }
        free(f->rowgroups);
    }
    free(f->col_sorted);
    vec_schema_free(&f->schema);
    if (f->fp) fclose(f->fp);
    free(f);
}

/* Translate a single tdc_column_stats slot back into Vtr1ColStat shape
 * for the requested VecType. has_stats == 0 in the source produces
 * has_stats == 0 in the destination. */
static void vtr1_tdc_decode_stat(const tdc_column_stats *src, VecType t,
                                 Vtr1ColStat *dst) {
    memset(dst, 0, sizeof(*dst));
    if (!src || !src->has_stats) return;
    dst->has_stats = 1;
    dst->null_count = src->null_count;
    if (vec_type_is_int(t)) {
        dst->i64.min = (int64_t)get_le_u64(src->min);
        dst->i64.max = (int64_t)get_le_u64(src->max);
    } else if (t == VEC_DOUBLE) {
        uint64_t mn = get_le_u64(src->min);
        uint64_t mx = get_le_u64(src->max);
        memcpy(&dst->dbl.min, &mn, 8);
        memcpy(&dst->dbl.max, &mx, 8);
    } else if (t == VEC_BOOL) {
        dst->bln.min = (uint8_t)get_le_u64(src->min);
        dst->bln.max = (uint8_t)get_le_u64(src->max);
    } else if (t == VEC_STRING) {
        /* Packed-prefix string min/max isn't populated yet. Surface
         * the universal prefix range (0 .. UINT64_MAX) so scan.c's
         * prefix-based pruning never incorrectly skips a row group;
         * the R-side reader still maps these to NA on read because
         * they are sentinel "no min/max" markers, not real strings. */
        dst->i64.min = 0;
        dst->i64.max = (int64_t)UINT64_MAX;
    } else {
        /* Unsupported type — degrade silently to no-stats. */
        dst->has_stats = 0;
    }
}

Vtr1TdcFile *vtr1_open_tdc(const char *path) {
    if (!path) return NULL;
    FILE *fp = fopen(path, "rb");
    if (!fp) return NULL;
    setvbuf(fp, NULL, _IOFBF, 256 * 1024);

    tdc_stream_decoder_config cfg = {0};
    cfg.io.write_fn = vtr1_tdc_io_write;
    cfg.io.read_fn  = vtr1_tdc_io_read;
    cfg.io.seek_fn  = vtr1_tdc_io_seek;
    cfg.io.ctx      = fp;
    cfg.realloc_fn  = vtr1_tdc_realloc;
    cfg.alloc_user  = NULL;

    tdc_stream_decoder *dec = NULL;
    tdc_status st = tdc_stream_decoder_open(&cfg, &dec);
    if (st != TDC_OK) { fclose(fp); return NULL; }

    /* A zero-rowgroup file has no trailing index — the encoder skips
     * emitting it. Treat that as a valid empty container; the schema
     * is in the header section and rowgroup_count() returns 0. */

    const tdc_schema *src_sch = tdc_stream_decoder_read_schema(dec);
    if (!src_sch) {
        tdc_stream_decoder_close(&dec);
        fclose(fp);
        return NULL;
    }

    /* Build VecSchema from the parsed tdc_schema. */
    int n_cols = (int)src_sch->n_columns;
    char    **names = (char **)calloc((size_t)(n_cols > 0 ? n_cols : 1), sizeof(char *));
    VecType  *types = (VecType *)calloc((size_t)(n_cols > 0 ? n_cols : 1), sizeof(VecType));
    char    **user_anns = (char **)calloc((size_t)(n_cols > 0 ? n_cols : 1), sizeof(char *));
    if ((!names || !types || !user_anns) && n_cols > 0) {
        free(names); free(types); free(user_anns);
        tdc_stream_decoder_close(&dec);
        fclose(fp);
        return NULL;
    }

    int parse_ok = 1;
    for (int i = 0; i < n_cols; i++) {
        const tdc_column_desc *cd = &src_sch->columns[i];
        names[i] = (char *)malloc((size_t)cd->name_len + 1);
        if (!names[i]) { parse_ok = 0; break; }
        if (cd->name_len > 0) memcpy(names[i], cd->name, cd->name_len);
        names[i][cd->name_len] = '\0';

        types[i] = vtr1_tdc_parse_annotation(cd->annotation, cd->ann_len,
                                             &user_anns[i]);
        if ((int)types[i] < 0) { parse_ok = 0; break; }
    }
    if (!parse_ok) {
        for (int i = 0; i < n_cols; i++) { free(names[i]); free(user_anns[i]); }
        free(names); free(types); free(user_anns);
        tdc_stream_decoder_close(&dec);
        fclose(fp);
        return NULL;
    }

    Vtr1TdcFile *f = (Vtr1TdcFile *)calloc(1, sizeof(*f));
    if (!f) {
        for (int i = 0; i < n_cols; i++) { free(names[i]); free(user_anns[i]); }
        free(names); free(types); free(user_anns);
        tdc_stream_decoder_close(&dec);
        fclose(fp);
        return NULL;
    }
    f->fp = fp;
    f->schema = vec_schema_create(n_cols, names, types);
    /* Hand user-annotation ownership over to the schema. */
    for (int i = 0; i < n_cols; i++) {
        f->schema.col_annotations[i] = user_anns[i];
        user_anns[i] = NULL;
        free(names[i]);
    }
    free(names); free(types); free(user_anns);

    /* Deep-copy the row-group index so we can drop the decoder. */
    uint64_t n_rg = tdc_stream_decoder_rowgroup_count(dec);
    if (n_rg > UINT32_MAX) {
        vtr1_tdc_file_destroy(f);
        tdc_stream_decoder_close(&dec);
        return NULL;
    }
    f->n_rowgroups = (uint32_t)n_rg;
    if (n_rg > 0) {
        f->rowgroups = (Vtr1TdcRowgroup *)calloc((size_t)n_rg, sizeof(Vtr1TdcRowgroup));
        if (!f->rowgroups) {
            vtr1_tdc_file_destroy(f);
            tdc_stream_decoder_close(&dec);
            return NULL;
        }
        for (uint32_t r = 0; r < n_rg; r++) {
            const tdc_rowgroup_entry *re = tdc_stream_decoder_get_rowgroup(dec, r);
            if (!re || (int)re->n_cols != n_cols) {
                vtr1_tdc_file_destroy(f);
                tdc_stream_decoder_close(&dec);
                return NULL;
            }
            f->rowgroups[r].n_rows = (int64_t)re->n_rows;
            if (n_cols > 0) {
                f->rowgroups[r].block_offset = (uint64_t *)malloc((size_t)n_cols * sizeof(uint64_t));
                f->rowgroups[r].block_total  = (uint64_t *)malloc((size_t)n_cols * sizeof(uint64_t));
                if (!f->rowgroups[r].block_offset || !f->rowgroups[r].block_total) {
                    vtr1_tdc_file_destroy(f);
                    tdc_stream_decoder_close(&dec);
                    return NULL;
                }
                for (int c = 0; c < n_cols; c++) {
                    f->rowgroups[r].block_offset[c] = re->columns[c].block_offset;
                    f->rowgroups[r].block_total[c]  = re->columns[c].block_total;
                }
            }

            /* Decode per-column stats if the rowgroup carries them. We
             * peek the first column's slot to decide; tdc only emits the
             * whole array or none-at-all. */
            const tdc_column_stats *probe =
                (n_cols > 0) ? tdc_stream_decoder_get_stats(dec, r, 0) : NULL;
            if (probe && n_cols > 0) {
                Vtr1ColStat *stats =
                    (Vtr1ColStat *)calloc((size_t)n_cols, sizeof(Vtr1ColStat));
                if (!stats) {
                    vtr1_tdc_file_destroy(f);
                    tdc_stream_decoder_close(&dec);
                    return NULL;
                }
                for (int c = 0; c < n_cols; c++) {
                    const tdc_column_stats *src =
                        tdc_stream_decoder_get_stats(dec, r, (uint16_t)c);
                    vtr1_tdc_decode_stat(src, f->schema.col_types[c], &stats[c]);
                }
                f->rowgroups[r].col_stats = stats;
            }
        }
    }

    /* Detect sorted columns: every consecutive rowgroup pair must have
     * stats[c] with sa.max <= sb.min (typed compare). Mirrors the v4
     * algorithm in vtr1.c so scan.c's binary-search RG narrowing works
     * unchanged. VEC_STRING is treated as not-sorted because tdc doesn't
     * yet pack a string min/max prefix into stats. */
    if (n_cols > 0 && f->n_rowgroups > 1) {
        f->col_sorted = (uint8_t *)calloc((size_t)n_cols, 1);
        if (f->col_sorted) {
            for (int c = 0; c < n_cols; c++) {
                VecType t = f->schema.col_types[c];
                int sorted = 1;
                for (uint32_t rg = 0; rg + 1 < f->n_rowgroups; rg++) {
                    Vtr1ColStat *sa = f->rowgroups[rg].col_stats;
                    Vtr1ColStat *sb = f->rowgroups[rg + 1].col_stats;
                    if (!sa || !sb || !sa[c].has_stats || !sb[c].has_stats) {
                        sorted = 0; break;
                    }
                    if (vec_type_is_int(t)) {
                        if (sa[c].i64.max > sb[c].i64.min) { sorted = 0; break; }
                    } else if (t == VEC_DOUBLE) {
                        if (sa[c].dbl.max > sb[c].dbl.min) { sorted = 0; break; }
                    } else if (t == VEC_BOOL) {
                        if (sa[c].bln.max > sb[c].bln.min) { sorted = 0; break; }
                    } else {
                        sorted = 0; break;
                    }
                }
                f->col_sorted[c] = (uint8_t)sorted;
            }
        }
    }

    tdc_stream_decoder_close(&dec);
    return f;
}

const VecSchema *vtr1_tdc_schema(const Vtr1TdcFile *file) {
    return file ? &file->schema : NULL;
}

uint32_t vtr1_tdc_n_rowgroups(const Vtr1TdcFile *file) {
    return file ? file->n_rowgroups : 0u;
}

int64_t vtr1_tdc_rowgroup_n_rows(const Vtr1TdcFile *file, uint32_t rg_idx) {
    if (!file || rg_idx >= file->n_rowgroups) return -1;
    return file->rowgroups[rg_idx].n_rows;
}

const Vtr1ColStat *vtr1_tdc_rowgroup_col_stats(const Vtr1TdcFile *file,
                                               uint32_t rg_idx) {
    if (!file || rg_idx >= file->n_rowgroups) return NULL;
    return file->rowgroups[rg_idx].col_stats;
}

const uint8_t *vtr1_tdc_col_sorted(const Vtr1TdcFile *file) {
    return file ? file->col_sorted : NULL;
}

/* Build a VecArray that aliases a caller-supplied direct buffer. owns_data
 * is set to 0 / data_borrowed to 1 so vec_array_free leaves dst alone but
 * callers can still distinguish "decoder honored my direct_buf" from a
 * string-arena borrow. The validity bitmap is freshly allocated and owned
 * by the array so vec_batch_free still releases it. */
static VecArray vtr1_tdc_make_borrowing_array(VecType t, int64_t n_rows,
                                              void *direct_buf) {
    VecArray arr;
    memset(&arr, 0, sizeof(arr));
    arr.type = t;
    arr.length = n_rows;
    arr.owns_data = 0;
    arr.data_borrowed = 1;
    int64_t vbytes = vec_validity_bytes(n_rows);
    arr.validity = (uint8_t *)calloc((size_t)(vbytes > 0 ? vbytes : 1), 1);
    if (!arr.validity) vectra_error("alloc failed for validity bitmap");
    switch (t) {
    case VEC_DOUBLE: arr.buf.dbl = (double  *)direct_buf; break;
    case VEC_INT64:  arr.buf.i64 = (int64_t *)direct_buf; break;
    case VEC_INT32:  arr.buf.i32 = (int32_t *)direct_buf; break;
    case VEC_INT16:  arr.buf.i16 = (int16_t *)direct_buf; break;
    case VEC_INT8:   arr.buf.i8  = (int8_t  *)direct_buf; break;
    case VEC_BOOL:   arr.buf.bln = (uint8_t *)direct_buf; break;
    default:
        free(arr.validity);
        vectra_error("vtr1_read_rowgroup_tdc_ex: type %d cannot direct-write",
                     (int)t);
    }
    return arr;
}

/* Core per-rowgroup decoder: takes an explicit FILE* and a pair of
 * caller-owned scratch slots so the parallel reader can share scratch
 * across rowgroups within a single thread. *scratch / *scratch_cap are
 * grown in place; the caller frees them. */
static VecBatch *read_rg_tdc_with_fp(Vtr1TdcFile *file, uint32_t rg_idx,
                                     const int *col_mask, FILE *fp,
                                     uint8_t **scratch, size_t *scratch_cap,
                                     void **direct_bufs) {
    const VecSchema *schema = &file->schema;
    int n_cols = schema->n_cols;
    int64_t n_rows = file->rowgroups[rg_idx].n_rows;

    int n_selected = 0;
    for (int c = 0; c < n_cols; c++) {
        if (col_mask[c]) n_selected++;
    }

    VecBatch *batch = vec_batch_alloc(n_selected, n_rows);

    /* Empty rowgroup: skip the per-column block read/decode entirely and
     * return an empty batch with placeholder arrays. Column blocks may
     * still exist on disk (the writer emits them for shape uniformity),
     * but the on-disk encoding for a zero-length payload isn't
     * round-trippable through tdc_decode_block_into in the current v0
     * codec. */
    if (n_rows == 0) {
        int out_col0 = 0;
        for (int c = 0; c < n_cols; c++) {
            if (!col_mask[c]) continue;
            VecType t = schema->col_types[c];
            void *direct = (direct_bufs ? direct_bufs[out_col0] : NULL);
            batch->columns[out_col0] = direct
                ? vtr1_tdc_make_borrowing_array(t, 0, direct)
                : vec_array_alloc(t, 0);
            size_t name_len = strlen(schema->col_names[c]);
            batch->col_names[out_col0] = (char *)malloc(name_len + 1);
            if (!batch->col_names[out_col0]) {
                vec_batch_free(batch);
                vectra_error("alloc failed for col name");
            }
            memcpy(batch->col_names[out_col0], schema->col_names[c], name_len + 1);
            out_col0++;
        }
        return batch;
    }

    int out_col = 0;
    for (int c = 0; c < n_cols; c++) {
        if (!col_mask[c]) continue;

        VecType t = schema->col_types[c];

        uint64_t off = file->rowgroups[rg_idx].block_offset[c];
        uint64_t sz  = file->rowgroups[rg_idx].block_total[c];
        if (sz == 0 || sz > (uint64_t)SIZE_MAX) {
            vec_batch_free(batch);
            vectra_error("invalid block size %llu for col %d in rg %u",
                         (unsigned long long)sz, c, rg_idx);
        }
        if (sz > *scratch_cap) {
            uint8_t *nb = (uint8_t *)realloc(*scratch, (size_t)sz);
            if (!nb) {
                vec_batch_free(batch);
                vectra_error("alloc failed for block scratch (%llu bytes)",
                             (unsigned long long)sz);
            }
            *scratch = nb;
            *scratch_cap = (size_t)sz;
        }

#if defined(_WIN32)
        if (_fseeki64(fp, (int64_t)off, SEEK_SET) != 0) {
            vec_batch_free(batch);
            vectra_error("fseek failed for col %d in rg %u", c, rg_idx);
        }
#else
        if (fseeko(fp, (off_t)off, SEEK_SET) != 0) {
            vec_batch_free(batch);
            vectra_error("fseek failed for col %d in rg %u", c, rg_idx);
        }
#endif
        if (fread(*scratch, 1, (size_t)sz, fp) != (size_t)sz) {
            vec_batch_free(batch);
            vectra_error("short read for col %d in rg %u", c, rg_idx);
        }

        void *direct = (direct_bufs ? direct_bufs[out_col] : NULL);
        VecArray arr;
        tdc_status st;
        if (direct) {
            arr = vtr1_tdc_make_borrowing_array(t, n_rows, direct);
            st = vtr_decode_column_tdc_into(t, n_rows, direct, arr.validity,
                                            *scratch, (size_t)sz);
        } else {
            arr = vec_array_alloc(t, n_rows);
            st = vtr_decode_column_tdc(&arr, *scratch, (size_t)sz);
        }
        if (st != TDC_OK) {
            vec_array_free(&arr);
            vec_batch_free(batch);
            vectra_error("vtr_decode_column_tdc failed for col %d in rg %u: status=%d",
                         c, rg_idx, (int)st);
        }
        batch->columns[out_col] = arr;

        size_t name_len = strlen(schema->col_names[c]);
        batch->col_names[out_col] = (char *)malloc(name_len + 1);
        if (!batch->col_names[out_col]) {
            vec_batch_free(batch);
            vectra_error("alloc failed for col name");
        }
        memcpy(batch->col_names[out_col], schema->col_names[c], name_len + 1);

        out_col++;
    }

    return batch;
}

VecBatch *vtr1_read_rowgroup_tdc(Vtr1TdcFile *file, uint32_t rg_idx,
                                 const int *col_mask) {
    return vtr1_read_rowgroup_tdc_ex(file, rg_idx, col_mask, NULL);
}

VecBatch *vtr1_read_rowgroup_tdc_ex(Vtr1TdcFile *file, uint32_t rg_idx,
                                    const int *col_mask,
                                    void **direct_bufs) {
    if (!file) vectra_error("vtr1_read_rowgroup_tdc_ex: NULL file");
    if (rg_idx >= file->n_rowgroups) {
        vectra_error("row group index out of range: %u >= %u",
                     rg_idx, file->n_rowgroups);
    }

    uint8_t *scratch = NULL;
    size_t   scratch_cap = 0;
    VecBatch *batch = read_rg_tdc_with_fp(file, rg_idx, col_mask, file->fp,
                                          &scratch, &scratch_cap, direct_bufs);
    free(scratch);
    return batch;
}

VecBatch **vtr1_read_parallel_tdc(Vtr1TdcFile *file, const int *col_mask,
                                  const char *path, uint32_t *out_count) {
    return vtr1_read_parallel_tdc_into(file, col_mask, path,
                                       NULL, NULL, 0, out_count);
}

/* Parallel reader. Mirrors vtr1_read_parallel_into in vtr1.c: each thread
 * opens its own FILE*, walks rowgroups via OpenMP for-schedule(dynamic),
 * and writes into per-column base buffers offset by the rowgroup's
 * cumulative row offset. col_bases entries must remain valid for the
 * duration of the call; callers must NOT touch the R API on any thread
 * inside this function. */
VecBatch **vtr1_read_parallel_tdc_into(Vtr1TdcFile *file, const int *col_mask,
                                       const char *path,
                                       void **col_bases,
                                       const size_t *col_elem_sizes,
                                       int n_out_cols,
                                       uint32_t *out_count) {
    uint32_t n_rg = file->n_rowgroups;
    *out_count = n_rg;
    if (n_rg == 0) return NULL;

    VecBatch **batches = (VecBatch **)calloc((size_t)n_rg, sizeof(VecBatch *));
    if (!batches) vectra_error("alloc failed for parallel TDC read");

    int64_t *rg_offsets = NULL;
    if (col_bases) {
        rg_offsets = (int64_t *)malloc((size_t)n_rg * sizeof(int64_t));
        if (!rg_offsets) { free(batches);
            vectra_error("alloc failed for rg_offsets"); }
        int64_t cum = 0;
        for (uint32_t r = 0; r < n_rg; r++) {
            rg_offsets[r] = cum;
            cum += file->rowgroups[r].n_rows;
        }
    }

    #pragma omp parallel
    {
        FILE *fp = fopen(path, "rb");
        if (!fp) vectra_error("parallel TDC read: cannot open %s", path);
        setvbuf(fp, NULL, _IOFBF, 256 * 1024);

        uint8_t *scratch = NULL;
        size_t   scratch_cap = 0;

        void **thread_bufs = NULL;
        if (col_bases) {
            thread_bufs = (void **)malloc((size_t)n_out_cols * sizeof(void *));
            if (!thread_bufs)
                vectra_error("alloc failed for thread_bufs");
        }

        #pragma omp for schedule(dynamic)
        for (int32_t r = 0; r < (int32_t)n_rg; r++) {
            void **bufs = NULL;
            if (col_bases) {
                int64_t off = rg_offsets[r];
                for (int i = 0; i < n_out_cols; i++) {
                    if (col_bases[i]) {
                        thread_bufs[i] = (uint8_t *)col_bases[i]
                                       + (size_t)off * col_elem_sizes[i];
                    } else {
                        thread_bufs[i] = NULL;
                    }
                }
                bufs = thread_bufs;
            }
            batches[r] = read_rg_tdc_with_fp(file, (uint32_t)r, col_mask,
                                             fp, &scratch, &scratch_cap, bufs);
        }

        free(thread_bufs);
        free(scratch);
        fclose(fp);
    }

    free(rg_offsets);
    return batches;
}

void vtr1_close_tdc(Vtr1TdcFile *file) {
    vtr1_tdc_file_destroy(file);
}

/* =========================================================================
 * R bridge — round-trip entry points used by the testthat tests.
 * NOT part of the production read/write path.
 *
 *   C_write_vtr_tdc(path, df, rowgroup_size, comp_level)
 *     df is a list of equal-length atomic vectors. Splits into row groups
 *     of at most rowgroup_size rows, writes a tdc container at path.
 *
 *   C_read_vtr_tdc(path)
 *     Reads every row group, concatenates per column, returns a named
 *     R list (one entry per column) holding the recombined data.frame.
 *
 * Type mapping (matches C_tdc_encode_column / _decode_column):
 *   REALSXP <-> VEC_DOUBLE
 *   INTSXP  <-> VEC_INT32
 *   LGLSXP  <-> VEC_BOOL  (LGLSXP int <-> uint8 0/1)
 * NA handling is minimal: NA_LOGICAL is folded to 0,
 * NA_REAL / NA_INTEGER round-trip via the bit pattern stored in the
 * payload (validity bitmap is written/read but not surfaced through
 * the R bridge).
 * ========================================================================= */

#include <R.h>
#include <Rinternals.h>

static SEXPTYPE vectype_to_sxp(VecType t) {
    switch (t) {
    case VEC_DOUBLE: return REALSXP;
    case VEC_INT32:  return INTSXP;
    case VEC_BOOL:   return LGLSXP;
    case VEC_STRING: return STRSXP;
    default:         return NILSXP;
    }
}

/* Writer: snapshots one slice of an R column into a VecArray view that
 * borrows the SEXP backing store (or, for LGLSXP, a temporary uint8
 * buffer). The validity bitmap is freshly R_alloc'd from the R NA
 * pattern: NA_REAL / NA_INTEGER / NA_LOGICAL produce a 0 bit, all other
 * values produce a 1 bit. If the slice has no NAs, validity is left NULL
 * (the bridge passes NULL through to tdc, which omits HAS_VALIDITY in
 * the record header — saves ceil(n_rows/8) bytes per col).
 *
 * The caller must ensure the SEXP outlives the VecArray. */
static void r_col_slice_into_vecarray(SEXP col, R_xlen_t row_offset,
                                      int64_t n_rows, VecArray *out,
                                      uint8_t **bln_tmp_out) {
    memset(out, 0, sizeof(*out));
    out->length = n_rows;
    int has_na = 0;
    int64_t vbytes = vec_validity_bytes(n_rows);
    uint8_t *validity = NULL;
    switch (TYPEOF(col)) {
    case REALSXP: {
        out->type = VEC_DOUBLE;
        out->buf.dbl = REAL(col) + row_offset;
        const double *src = REAL(col) + row_offset;
        for (int64_t i = 0; i < n_rows; i++) {
            if (ISNA(src[i])) { has_na = 1; break; }
        }
        if (has_na) {
            validity = (uint8_t *)R_alloc((size_t)(vbytes > 0 ? vbytes : 1), 1);
            memset(validity, 0, (size_t)vbytes);
            for (int64_t i = 0; i < n_rows; i++) {
                if (!ISNA(src[i])) validity[i / 8] |= (uint8_t)(1u << (i % 8));
            }
        }
        break;
    }
    case INTSXP: {
        out->type = VEC_INT32;
        out->buf.i32 = INTEGER(col) + row_offset;
        const int *src = INTEGER(col) + row_offset;
        for (int64_t i = 0; i < n_rows; i++) {
            if (src[i] == NA_INTEGER) { has_na = 1; break; }
        }
        if (has_na) {
            validity = (uint8_t *)R_alloc((size_t)(vbytes > 0 ? vbytes : 1), 1);
            memset(validity, 0, (size_t)vbytes);
            for (int64_t i = 0; i < n_rows; i++) {
                if (src[i] != NA_INTEGER)
                    validity[i / 8] |= (uint8_t)(1u << (i % 8));
            }
        }
        break;
    }
    case LGLSXP: {
        out->type = VEC_BOOL;
        uint8_t *tmp = (uint8_t *)((n_rows > 0) ? R_alloc((size_t)n_rows, 1) : NULL);
        const int *src = LOGICAL(col) + row_offset;
        for (int64_t i = 0; i < n_rows; i++) {
            int v = src[i];
            if (v == NA_LOGICAL) { has_na = 1; tmp[i] = 0u; }
            else                 tmp[i] = v ? 1u : 0u;
        }
        if (has_na) {
            validity = (uint8_t *)R_alloc((size_t)(vbytes > 0 ? vbytes : 1), 1);
            memset(validity, 0, (size_t)vbytes);
            for (int64_t i = 0; i < n_rows; i++) {
                if (src[i] != NA_LOGICAL)
                    validity[i / 8] |= (uint8_t)(1u << (i % 8));
            }
        }
        out->buf.bln = tmp;
        if (bln_tmp_out) *bln_tmp_out = tmp;
        break;
    }
    case STRSXP: {
        out->type = VEC_STRING;
        /* Pack the slice into flat (offsets, data) buffers. NA_STRING entries
         * contribute zero bytes to the heap and a 0 bit in the validity map.
         * Both buffers come from R_alloc — transient, reaped at .Call return.
         * The caller marks owns_data=0 and nulls pointers before vec_batch_free
         * so libc free() never touches R-managed memory. */
        int64_t total_bytes = 0;
        for (int64_t i = 0; i < n_rows; i++) {
            SEXP s = STRING_ELT(col, row_offset + i);
            if (s == NA_STRING) { has_na = 1; continue; }
            total_bytes += (int64_t)LENGTH(s);
        }
        int64_t *offs = (int64_t *)R_alloc((size_t)(n_rows + 1), sizeof(int64_t));
        char    *data = (char *)R_alloc((size_t)(total_bytes > 0 ? total_bytes : 1), 1);
        int64_t pos = 0;
        for (int64_t i = 0; i < n_rows; i++) {
            offs[i] = pos;
            SEXP s = STRING_ELT(col, row_offset + i);
            if (s == NA_STRING) continue;
            int len = LENGTH(s);
            if (len > 0) memcpy(data + pos, CHAR(s), (size_t)len);
            pos += len;
        }
        offs[n_rows] = pos;
        out->buf.str.offsets  = offs;
        out->buf.str.data     = data;
        out->buf.str.data_len = pos;
        if (has_na) {
            validity = (uint8_t *)R_alloc((size_t)(vbytes > 0 ? vbytes : 1), 1);
            memset(validity, 0, (size_t)vbytes);
            for (int64_t i = 0; i < n_rows; i++) {
                if (STRING_ELT(col, row_offset + i) != NA_STRING)
                    validity[i / 8] |= (uint8_t)(1u << (i % 8));
            }
        }
        break;
    }
    default:
        Rf_error("unsupported R column type: %d", (int)TYPEOF(col));
    }
    out->validity = validity;
}

SEXP C_write_vtr_tdc(SEXP path_sexp, SEXP df_sexp,
                     SEXP rowgroup_size_sexp, SEXP comp_level_sexp,
                     SEXP annotations_sexp) {
    if (TYPEOF(path_sexp) != STRSXP || LENGTH(path_sexp) != 1)
        Rf_error("C_write_vtr_tdc: path must be a single string");
    if (TYPEOF(df_sexp) != VECSXP)
        Rf_error("C_write_vtr_tdc: df must be a list of equal-length vectors");
    if (TYPEOF(rowgroup_size_sexp) != INTSXP || LENGTH(rowgroup_size_sexp) != 1)
        Rf_error("C_write_vtr_tdc: rowgroup_size must be a scalar integer");
    if (TYPEOF(comp_level_sexp) != INTSXP || LENGTH(comp_level_sexp) != 1)
        Rf_error("C_write_vtr_tdc: comp_level must be a scalar integer");

    const char *path    = CHAR(STRING_ELT(path_sexp, 0));
    int comp_level      = INTEGER(comp_level_sexp)[0];
    int rg_size         = INTEGER(rowgroup_size_sexp)[0];
    if (rg_size <= 0) Rf_error("rowgroup_size must be > 0");

    int n_cols = LENGTH(df_sexp);
    if (n_cols <= 0) Rf_error("df must have at least one column");

    /* PROTECT names_sexp: R_alloc inside the schema-build block below
     * can trigger GC, and rchk treats getAttrib results as fresh-allocated
     * even though they're rooted via df_sexp. */
    SEXP names_sexp = PROTECT(Rf_getAttrib(df_sexp, R_NamesSymbol));
    if (TYPEOF(names_sexp) != STRSXP || LENGTH(names_sexp) != n_cols) {
        UNPROTECT(1);
        Rf_error("df must have a names attribute of length n_cols");
    }

    /* annotations may be NULL or a character vector of length n_cols; NA_string
     * or "" entries are recorded as no-annotation. */
    if (annotations_sexp != R_NilValue) {
        if (TYPEOF(annotations_sexp) != STRSXP ||
            LENGTH(annotations_sexp) != n_cols)
            Rf_error("annotations must be NULL or a character vector of length n_cols");
    }

    R_xlen_t n_rows = Rf_xlength(VECTOR_ELT(df_sexp, 0));
    for (int c = 1; c < n_cols; c++) {
        if (Rf_xlength(VECTOR_ELT(df_sexp, c)) != n_rows)
            Rf_error("df columns have unequal lengths");
    }

    /* Build the schema and writer. */
    char **col_names = (char **)R_alloc((size_t)n_cols, sizeof(char *));
    VecType *col_types = (VecType *)R_alloc((size_t)n_cols, sizeof(VecType));
    for (int c = 0; c < n_cols; c++) {
        col_names[c] = (char *)CHAR(STRING_ELT(names_sexp, c));
        SEXP col = VECTOR_ELT(df_sexp, c);
        switch (TYPEOF(col)) {
        case REALSXP: col_types[c] = VEC_DOUBLE; break;
        case INTSXP:  col_types[c] = VEC_INT32;  break;
        case LGLSXP:  col_types[c] = VEC_BOOL;   break;
        case STRSXP:  col_types[c] = VEC_STRING; break;
        default:
            Rf_error("column %d has unsupported R type %d",
                     c + 1, (int)TYPEOF(col));
        }
    }
    VecSchema schema = vec_schema_create(n_cols, col_names, col_types);

    /* Attach user annotations into the schema (deep copy). */
    if (annotations_sexp != R_NilValue) {
        for (int c = 0; c < n_cols; c++) {
            SEXP s = STRING_ELT(annotations_sexp, c);
            if (s == NA_STRING) continue;
            const char *a = CHAR(s);
            if (a[0] == '\0') continue;
            size_t alen = strlen(a);
            schema.col_annotations[c] = (char *)malloc(alen + 1);
            if (!schema.col_annotations[c]) {
                vec_schema_free(&schema);
                Rf_error("alloc failed for column annotation");
            }
            memcpy(schema.col_annotations[c], a, alen + 1);
        }
    }

    Vtr1TdcWriter *w = vtr1_open_tdc_writer(path, &schema);

    /* Stream row groups of up to rg_size rows. */
    R_xlen_t pos = 0;
    while (pos < n_rows) {
        int64_t take = ((R_xlen_t)rg_size < n_rows - pos)
                     ? (int64_t)rg_size
                     : (int64_t)(n_rows - pos);

        VecBatch *batch = vec_batch_alloc(n_cols, take);
        vec_batch_set_names(batch, col_names);
        for (int c = 0; c < n_cols; c++) {
            VecArray view;
            uint8_t *bln_tmp = NULL;
            r_col_slice_into_vecarray(VECTOR_ELT(df_sexp, c), pos,
                                      take, &view, &bln_tmp);
            /* The batch borrows the SEXP storage (or R_alloc temp)
             * for the duration of the encode call; mark non-owning. */
            view.owns_data = 0;
            batch->columns[c] = view;
        }

        vtr1_write_rowgroup_tdc(w, batch, comp_level, NULL, NULL);

        /* Buf pointers and validity bitmap are borrowed: SEXP storage and
         * R_alloc transient memory respectively. Null both out before free
         * so vec_batch_free doesn't free what it doesn't own (R_alloc is
         * reaped at .Call return; calling free() on it is undefined). */
        for (int c = 0; c < n_cols; c++) {
            batch->columns[c].owns_data = 0;
            memset(&batch->columns[c].buf, 0, sizeof(batch->columns[c].buf));
            batch->columns[c].validity = NULL;
        }
        vec_batch_free(batch);
        pos += take;
    }

    vtr1_close_tdc_writer(w);
    vec_schema_free(&schema);
    UNPROTECT(1); /* names_sexp */
    return R_NilValue;
}

SEXP C_read_vtr_tdc(SEXP path_sexp) {
    if (TYPEOF(path_sexp) != STRSXP || LENGTH(path_sexp) != 1)
        Rf_error("C_read_vtr_tdc: path must be a single string");
    const char *path = CHAR(STRING_ELT(path_sexp, 0));

    Vtr1TdcFile *f = vtr1_open_tdc(path);
    if (!f) Rf_error("vtr1_open_tdc failed for %s", path);

    const VecSchema *schema = vtr1_tdc_schema(f);
    int n_cols = schema->n_cols;
    uint32_t n_rg = vtr1_tdc_n_rowgroups(f);

    R_xlen_t total_rows = 0;
    for (uint32_t r = 0; r < n_rg; r++) {
        total_rows += (R_xlen_t)vtr1_tdc_rowgroup_n_rows(f, r);
    }

    /* Pre-allocate one R vector per column at the total length. */
    SEXP out = PROTECT(allocVector(VECSXP, n_cols));
    SEXP nms = PROTECT(allocVector(STRSXP, n_cols));
    for (int c = 0; c < n_cols; c++) {
        SEXPTYPE rt = vectype_to_sxp(schema->col_types[c]);
        if (rt == NILSXP) {
            UNPROTECT(2);
            vtr1_close_tdc(f);
            Rf_error("column %d has unsupported VecType %d",
                     c + 1, (int)schema->col_types[c]);
        }
        SET_VECTOR_ELT(out, c, allocVector(rt, total_rows));
        SET_STRING_ELT(nms, c, mkChar(schema->col_names[c]));
    }
    Rf_setAttrib(out, R_NamesSymbol, nms);

    int *col_mask = (int *)R_alloc((size_t)n_cols, sizeof(int));
    for (int c = 0; c < n_cols; c++) col_mask[c] = 1;

    /* Direct-write base buffers. VEC_DOUBLE / VEC_INT32 are byte-compatible
     * with REAL / INTEGER SEXP storage, so the parallel decoder writes
     * each rowgroup slice straight into the output vector. VEC_BOOL stages
     * through a contiguous uint8 buffer (LGLSXP int storage is NOT byte-
     * compatible); a serial pass after the parallel decode converts to
     * LOGICAL with NA patching. */
    void  **col_bases      = (void  **)R_alloc((size_t)n_cols, sizeof(void *));
    size_t *col_elem_sizes = (size_t *)R_alloc((size_t)n_cols, sizeof(size_t));
    uint8_t **bool_stage   = (uint8_t **)R_alloc((size_t)n_cols, sizeof(uint8_t *));
    for (int c = 0; c < n_cols; c++) {
        SEXP col = VECTOR_ELT(out, c);
        bool_stage[c] = NULL;
        switch (schema->col_types[c]) {
        case VEC_DOUBLE:
            col_bases[c]      = REAL(col);
            col_elem_sizes[c] = sizeof(double);
            break;
        case VEC_INT32:
            col_bases[c]      = INTEGER(col);
            col_elem_sizes[c] = sizeof(int32_t);
            break;
        case VEC_BOOL:
            bool_stage[c] = (uint8_t *)((total_rows > 0)
                                        ? R_alloc((size_t)total_rows, 1) : NULL);
            col_bases[c]      = bool_stage[c];
            col_elem_sizes[c] = 1;
            break;
        case VEC_STRING:
            /* No direct-write path: strings need allocated offsets+heap that
             * the per-rg decoder sizes from the record. The parallel reader
             * sees col_bases[c]==NULL and falls through to
             * vec_array_alloc + vtr_decode_column_tdc, which handles strings.
             * The serial post-pass below moves bytes into the STRSXP. */
            col_bases[c]      = NULL;
            col_elem_sizes[c] = 0;
            break;
        default:
            col_bases[c]      = NULL;
            col_elem_sizes[c] = 0;
            break;
        }
    }

    /* Parallel decode. No R API may run on threads — only memory writes
     * into col_bases (REAL / INTEGER / R_alloc'd uint8). */
    uint32_t got = 0;
    VecBatch **batches = vtr1_read_parallel_tdc_into(f, col_mask, path,
                                                     col_bases, col_elem_sizes,
                                                     n_cols, &got);
    if (got != n_rg) {
        for (uint32_t r = 0; r < got; r++) vec_batch_free(batches[r]);
        free(batches);
        vtr1_close_tdc(f);
        UNPROTECT(2);
        Rf_error("parallel TDC read returned %u != %u rowgroups", got, n_rg);
    }

    /* Serial post-pass: NA-patch from each rowgroup's validity bitmap and
     * convert the staged uint8 bool slice into LGLSXP int storage. */
    R_xlen_t cursor = 0;
    for (uint32_t r = 0; r < n_rg; r++) {
        VecBatch *batch = batches[r];
        int64_t rg_rows = batch->n_rows;
        for (int c = 0; c < n_cols; c++) {
            SEXP col = VECTOR_ELT(out, c);
            const uint8_t *vmap = batch->columns[c].validity;
            switch (schema->col_types[c]) {
            case VEC_DOUBLE: {
                double *dst = REAL(col) + cursor;
                for (int64_t i = 0; i < rg_rows; i++) {
                    if (!((vmap[i / 8] >> (i % 8)) & 1u)) dst[i] = NA_REAL;
                }
                break;
            }
            case VEC_INT32: {
                int *dst = INTEGER(col) + cursor;
                for (int64_t i = 0; i < rg_rows; i++) {
                    if (!((vmap[i / 8] >> (i % 8)) & 1u)) dst[i] = NA_INTEGER;
                }
                break;
            }
            case VEC_BOOL: {
                int *dst = LOGICAL(col) + cursor;
                const uint8_t *s = bool_stage[c] + cursor;
                for (int64_t i = 0; i < rg_rows; i++) {
                    if (!((vmap[i / 8] >> (i % 8)) & 1u)) dst[i] = NA_LOGICAL;
                    else                                  dst[i] = s[i] ? TRUE : FALSE;
                }
                break;
            }
            case VEC_STRING: {
                const VecArray *arr  = &batch->columns[c];
                const int64_t  *offs = arr->buf.str.offsets;
                const char     *data = arr->buf.str.data;
                for (int64_t i = 0; i < rg_rows; i++) {
                    if (!((vmap[i / 8] >> (i % 8)) & 1u)) {
                        SET_STRING_ELT(col, cursor + i, NA_STRING);
                    } else {
                        int64_t a = offs[i], b = offs[i + 1];
                        int64_t L = b - a;
                        SET_STRING_ELT(col, cursor + i,
                                       (L == 0) ? R_BlankString
                                                : Rf_mkCharLen(data + a, (int)L));
                    }
                }
                break;
            }
            default:
                for (uint32_t k = r; k < n_rg; k++) vec_batch_free(batches[k]);
                free(batches);
                vtr1_close_tdc(f);
                UNPROTECT(2);
                Rf_error("unhandled VecType in C_read_vtr_tdc");
            }
        }
        vec_batch_free(batch);
        cursor += rg_rows;
    }
    free(batches);

    vtr1_close_tdc(f);
    UNPROTECT(2);
    return out;
}

/* Returns the per-column user annotations as a STRSXP of length n_cols.
 * NA_character_ when a column has no annotation. */
SEXP C_read_vtr_tdc_annotations(SEXP path_sexp) {
    if (TYPEOF(path_sexp) != STRSXP || LENGTH(path_sexp) != 1)
        Rf_error("C_read_vtr_tdc_annotations: path must be a single string");
    const char *path = CHAR(STRING_ELT(path_sexp, 0));

    Vtr1TdcFile *f = vtr1_open_tdc(path);
    if (!f) Rf_error("vtr1_open_tdc failed for %s", path);
    const VecSchema *schema = vtr1_tdc_schema(f);
    int n_cols = schema->n_cols;

    SEXP out = PROTECT(allocVector(STRSXP, n_cols));
    for (int c = 0; c < n_cols; c++) {
        const char *a = schema->col_annotations
                      ? schema->col_annotations[c] : NULL;
        if (a)
            SET_STRING_ELT(out, c, mkChar(a));
        else
            SET_STRING_ELT(out, c, NA_STRING);
    }
    vtr1_close_tdc(f);
    UNPROTECT(1);
    return out;
}

/* Returns per-rowgroup, per-column statistics as a list of length n_rg.
 * Each element is a list of length n_cols; each per-col entry is either
 * NULL (no stats) or a named numeric/double vector with elements
 * c(has_stats, min, max, null_count). For VEC_DOUBLE, min/max are doubles;
 * for integer/bool types the values are encoded as REALSXP for uniform
 * handling on the R side (R has no native int64). */
SEXP C_read_vtr_tdc_stats(SEXP path_sexp) {
    if (TYPEOF(path_sexp) != STRSXP || LENGTH(path_sexp) != 1)
        Rf_error("C_read_vtr_tdc_stats: path must be a single string");
    const char *path = CHAR(STRING_ELT(path_sexp, 0));

    Vtr1TdcFile *f = vtr1_open_tdc(path);
    if (!f) Rf_error("vtr1_open_tdc failed for %s", path);
    const VecSchema *schema = vtr1_tdc_schema(f);
    int n_cols = schema->n_cols;
    uint32_t n_rg = vtr1_tdc_n_rowgroups(f);

    SEXP out = PROTECT(allocVector(VECSXP, (R_xlen_t)n_rg));
    for (uint32_t r = 0; r < n_rg; r++) {
        SEXP per_rg = PROTECT(allocVector(VECSXP, n_cols));
        const Vtr1ColStat *stats = vtr1_tdc_rowgroup_col_stats(f, r);
        if (!stats) {
            for (int c = 0; c < n_cols; c++)
                SET_VECTOR_ELT(per_rg, c, R_NilValue);
        } else {
            for (int c = 0; c < n_cols; c++) {
                if (!stats[c].has_stats) {
                    SET_VECTOR_ELT(per_rg, c, R_NilValue);
                    continue;
                }
                SEXP v = PROTECT(allocVector(REALSXP, 4));
                REAL(v)[0] = 1.0;
                if (vec_type_is_int(schema->col_types[c])) {
                    REAL(v)[1] = (double)stats[c].i64.min;
                    REAL(v)[2] = (double)stats[c].i64.max;
                } else if (schema->col_types[c] == VEC_DOUBLE) {
                    REAL(v)[1] = stats[c].dbl.min;
                    REAL(v)[2] = stats[c].dbl.max;
                } else if (schema->col_types[c] == VEC_BOOL) {
                    REAL(v)[1] = (double)stats[c].bln.min;
                    REAL(v)[2] = (double)stats[c].bln.max;
                } else {
                    REAL(v)[1] = NA_REAL;
                    REAL(v)[2] = NA_REAL;
                }
                REAL(v)[3] = (double)stats[c].null_count;
                SET_VECTOR_ELT(per_rg, c, v);
                UNPROTECT(1);
            }
        }
        SET_VECTOR_ELT(out, (R_xlen_t)r, per_rg);
        UNPROTECT(1);
    }
    vtr1_close_tdc(f);
    UNPROTECT(1);
    return out;
}
