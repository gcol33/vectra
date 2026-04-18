/*
 * vtr1_tdc.c — tdc-backed row-group container writer/reader (P3).
 *
 * Side-by-side with vtr1.c. The on-disk format is a tdc container
 * (TDC_CONTAINER_MAGIC, HETEROGENEOUS flag, attached schema, trailing
 * row-group index) plus one self-describing tdc_block_record per
 * column per row group.
 *
 * Reader strategy: tdc_stream_decoder parses the header, schema, and
 * index at open time. We deep-copy what we need (schema -> VecSchema,
 * per-row-group offset+size table) and then ignore the decoder's
 * read_block API. Block bytes are fseek/fread'd from our own FILE*
 * and handed to vtr_decode_column_tdc, which extracts the validity
 * bitmap that tdc v0 leaves opaque.
 *
 * No per-column statistics. No string columns. Both gaps are
 * intentional for P3 and tracked in VECTRA_REWIRE.md.
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

/* Each schema column carries the VecType name as its annotation
 * ("int8" .. "string"). On read we parse this back, which is
 * unambiguous for the dtype-overloaded cases (TDC_DT_U8 might be
 * VEC_BOOL or a future u8). */

static const char *vec_type_annotation(VecType t) { return vec_type_name(t); }

static VecType vec_type_from_annotation(const char *s, uint16_t len) {
    if (len == 5 && memcmp(s, "int64", 5) == 0)  return VEC_INT64;
    if (len == 6 && memcmp(s, "double", 6) == 0) return VEC_DOUBLE;
    if (len == 4 && memcmp(s, "bool", 4) == 0)   return VEC_BOOL;
    if (len == 6 && memcmp(s, "string", 6) == 0) return VEC_STRING;
    if (len == 4 && memcmp(s, "int8", 4) == 0)   return VEC_INT8;
    if (len == 5 && memcmp(s, "int16", 5) == 0)  return VEC_INT16;
    if (len == 5 && memcmp(s, "int32", 5) == 0)  return VEC_INT32;
    return (VecType)-1;
}

/* ============================================================ writer === */

struct Vtr1TdcWriter {
    FILE                *fp;
    tdc_stream_encoder  *enc;
    VecSchema            schema;       /* deep-copied */
    tdc_column_desc     *desc_buf;     /* sized n_cols, freed at close */
};

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

    int n_cols = schema->n_cols;
    if (n_cols > 0) {
        w->desc_buf = (tdc_column_desc *)calloc((size_t)n_cols, sizeof(*w->desc_buf));
        if (!w->desc_buf) {
            vec_schema_free(&w->schema); free(w); fclose(fp);
            vectra_error("alloc failed for tdc_column_desc array");
        }
        for (int i = 0; i < n_cols; i++) {
            const char *ann = vec_type_annotation(w->schema.col_types[i]);
            w->desc_buf[i].name        = w->schema.col_names[i];
            w->desc_buf[i].name_len    = (uint16_t)strlen(w->schema.col_names[i]);
            w->desc_buf[i].dtype       = (uint8_t)vtr_type_to_tdc_dtype(w->schema.col_types[i]);
            w->desc_buf[i].annotation  = ann;
            w->desc_buf[i].ann_len     = (uint16_t)strlen(ann);
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
        if (col->type == VEC_STRING) {
            vectra_error("VEC_STRING not yet supported in vtr1_tdc (P3 gap)");
        }

        const VtrQuantizeSpec *qs = (qspecs && qspecs[c].enabled) ? &qspecs[c] : NULL;
        const VtrSpatialSpec  *ss = (sspecs && sspecs[c].enabled) ? &sspecs[c] : NULL;

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
            free(w->desc_buf);
            vec_schema_free(&w->schema);
            if (w->fp) fclose(w->fp);
            free(w);
            vectra_error("tdc_stream_encoder_close failed: status=%d", (int)st);
        }
    }
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
} Vtr1TdcRowgroup;

struct Vtr1TdcFile {
    FILE             *fp;
    VecSchema         schema;
    uint32_t          n_rowgroups;
    Vtr1TdcRowgroup  *rowgroups;  /* length n_rowgroups */
};

static void vtr1_tdc_file_destroy(Vtr1TdcFile *f) {
    if (!f) return;
    if (f->rowgroups) {
        for (uint32_t r = 0; r < f->n_rowgroups; r++) {
            free(f->rowgroups[r].block_offset);
            free(f->rowgroups[r].block_total);
        }
        free(f->rowgroups);
    }
    vec_schema_free(&f->schema);
    if (f->fp) fclose(f->fp);
    free(f);
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

    if (!tdc_stream_decoder_has_rowgroup_index(dec)) {
        tdc_stream_decoder_close(&dec);
        fclose(fp);
        return NULL;
    }

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
    if ((!names || !types) && n_cols > 0) {
        free(names); free(types);
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

        types[i] = vec_type_from_annotation(cd->annotation, cd->ann_len);
        if ((int)types[i] < 0) { parse_ok = 0; break; }
    }
    if (!parse_ok) {
        for (int i = 0; i < n_cols; i++) free(names[i]);
        free(names); free(types);
        tdc_stream_decoder_close(&dec);
        fclose(fp);
        return NULL;
    }

    Vtr1TdcFile *f = (Vtr1TdcFile *)calloc(1, sizeof(*f));
    if (!f) {
        for (int i = 0; i < n_cols; i++) free(names[i]);
        free(names); free(types);
        tdc_stream_decoder_close(&dec);
        fclose(fp);
        return NULL;
    }
    f->fp = fp;
    f->schema = vec_schema_create(n_cols, names, types);
    for (int i = 0; i < n_cols; i++) free(names[i]);
    free(names); free(types);

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

VecBatch *vtr1_read_rowgroup_tdc(Vtr1TdcFile *file, uint32_t rg_idx,
                                 const int *col_mask) {
    if (!file) vectra_error("vtr1_read_rowgroup_tdc: NULL file");
    if (rg_idx >= file->n_rowgroups) {
        vectra_error("row group index out of range: %u >= %u",
                     rg_idx, file->n_rowgroups);
    }

    const VecSchema *schema = &file->schema;
    int n_cols = schema->n_cols;
    int64_t n_rows = file->rowgroups[rg_idx].n_rows;

    int n_selected = 0;
    for (int c = 0; c < n_cols; c++) {
        if (col_mask[c]) n_selected++;
    }

    VecBatch *batch = vec_batch_alloc(n_selected, n_rows);

    /* Reusable scratch for raw block bytes — grows but never shrinks
     * across the rowgroup. */
    uint8_t *scratch = NULL;
    size_t   scratch_cap = 0;

    int out_col = 0;
    for (int c = 0; c < n_cols; c++) {
        if (!col_mask[c]) continue;

        VecType t = schema->col_types[c];
        if (t == VEC_STRING) {
            free(scratch);
            vec_batch_free(batch);
            vectra_error("VEC_STRING decode not supported (P3 gap)");
        }

        uint64_t off = file->rowgroups[rg_idx].block_offset[c];
        uint64_t sz  = file->rowgroups[rg_idx].block_total[c];
        if (sz == 0 || sz > (uint64_t)SIZE_MAX) {
            free(scratch);
            vec_batch_free(batch);
            vectra_error("invalid block size %llu for col %d in rg %u",
                         (unsigned long long)sz, c, rg_idx);
        }
        if (sz > scratch_cap) {
            uint8_t *nb = (uint8_t *)realloc(scratch, (size_t)sz);
            if (!nb) {
                free(scratch);
                vec_batch_free(batch);
                vectra_error("alloc failed for block scratch (%llu bytes)",
                             (unsigned long long)sz);
            }
            scratch = nb;
            scratch_cap = (size_t)sz;
        }

#if defined(_WIN32)
        if (_fseeki64(file->fp, (int64_t)off, SEEK_SET) != 0) {
            free(scratch);
            vec_batch_free(batch);
            vectra_error("fseek failed for col %d in rg %u", c, rg_idx);
        }
#else
        if (fseeko(file->fp, (off_t)off, SEEK_SET) != 0) {
            free(scratch);
            vec_batch_free(batch);
            vectra_error("fseek failed for col %d in rg %u", c, rg_idx);
        }
#endif
        if (fread(scratch, 1, (size_t)sz, file->fp) != (size_t)sz) {
            free(scratch);
            vec_batch_free(batch);
            vectra_error("short read for col %d in rg %u", c, rg_idx);
        }

        VecArray arr = vec_array_alloc(t, n_rows);
        tdc_status st = vtr_decode_column_tdc(&arr, scratch, (size_t)sz);
        if (st != TDC_OK) {
            vec_array_free(&arr);
            free(scratch);
            vec_batch_free(batch);
            vectra_error("vtr_decode_column_tdc failed for col %d in rg %u: status=%d",
                         c, rg_idx, (int)st);
        }
        batch->columns[out_col] = arr;

        size_t name_len = strlen(schema->col_names[c]);
        batch->col_names[out_col] = (char *)malloc(name_len + 1);
        if (!batch->col_names[out_col]) {
            free(scratch);
            vec_batch_free(batch);
            vectra_error("alloc failed for col name");
        }
        memcpy(batch->col_names[out_col], schema->col_names[c], name_len + 1);

        out_col++;
    }

    free(scratch);
    return batch;
}

void vtr1_close_tdc(Vtr1TdcFile *file) {
    vtr1_tdc_file_destroy(file);
}

/* =========================================================================
 * R bridge — round-trip entry points used by the testthat tests for P3.
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
 * NA handling is intentionally minimal in P3: NA_LOGICAL is folded to 0,
 * NA_REAL / NA_INTEGER round-trip via the bit pattern stored in the
 * payload (validity bitmap is written/read but not surfaced through
 * the R bridge until P4).
 * ========================================================================= */

#include <R.h>
#include <Rinternals.h>

static SEXPTYPE vectype_to_sxp(VecType t) {
    switch (t) {
    case VEC_DOUBLE: return REALSXP;
    case VEC_INT32:  return INTSXP;
    case VEC_BOOL:   return LGLSXP;
    default:         return NILSXP;
    }
}

/* Writer: snapshots one slice of an R column into a VecArray view that
 * borrows the SEXP backing store (or, for LGLSXP, a temporary uint8
 * buffer). The caller must ensure the SEXP outlives the VecArray. */
static void r_col_slice_into_vecarray(SEXP col, R_xlen_t row_offset,
                                      int64_t n_rows, VecArray *out,
                                      uint8_t **bln_tmp_out) {
    memset(out, 0, sizeof(*out));
    out->length = n_rows;
    /* validity is left NULL: the encode bridge passes NULL through to
     * tdc, which writes a record without HAS_VALIDITY. The decoder
     * fills all-valid. P3 does not exercise NA-aware round-trip. */
    switch (TYPEOF(col)) {
    case REALSXP:
        out->type = VEC_DOUBLE;
        out->buf.dbl = REAL(col) + row_offset;
        break;
    case INTSXP:
        out->type = VEC_INT32;
        out->buf.i32 = INTEGER(col) + row_offset;
        break;
    case LGLSXP: {
        out->type = VEC_BOOL;
        uint8_t *tmp = (uint8_t *)((n_rows > 0) ? R_alloc((size_t)n_rows, 1) : NULL);
        const int *src = LOGICAL(col) + row_offset;
        for (int64_t i = 0; i < n_rows; i++) {
            int v = src[i];
            tmp[i] = (v == NA_LOGICAL) ? 0u : (v ? 1u : 0u);
        }
        out->buf.bln = tmp;
        if (bln_tmp_out) *bln_tmp_out = tmp;
        break;
    }
    default:
        Rf_error("unsupported R column type: %d", (int)TYPEOF(col));
    }
}

SEXP C_write_vtr_tdc(SEXP path_sexp, SEXP df_sexp,
                     SEXP rowgroup_size_sexp, SEXP comp_level_sexp) {
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

    SEXP names_sexp = Rf_getAttrib(df_sexp, R_NamesSymbol);
    if (TYPEOF(names_sexp) != STRSXP || LENGTH(names_sexp) != n_cols)
        Rf_error("df must have a names attribute of length n_cols");

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
        default:
            Rf_error("column %d has unsupported R type %d",
                     c + 1, (int)TYPEOF(col));
        }
    }
    VecSchema schema = vec_schema_create(n_cols, col_names, col_types);

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

        /* Defensive: zero the buf union pointers before free so
         * vec_batch_free doesn't double-free borrowed storage. */
        for (int c = 0; c < n_cols; c++) {
            batch->columns[c].owns_data = 0;
            memset(&batch->columns[c].buf, 0, sizeof(batch->columns[c].buf));
            free(batch->columns[c].validity);
            batch->columns[c].validity = NULL;
        }
        vec_batch_free(batch);
        pos += take;
    }

    vtr1_close_tdc_writer(w);
    vec_schema_free(&schema);
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

    R_xlen_t cursor = 0;
    for (uint32_t r = 0; r < n_rg; r++) {
        VecBatch *batch = vtr1_read_rowgroup_tdc(f, r, col_mask);
        int64_t rg_rows = batch->n_rows;
        for (int c = 0; c < n_cols; c++) {
            SEXP col = VECTOR_ELT(out, c);
            VecArray *src = &batch->columns[c];
            switch (schema->col_types[c]) {
            case VEC_DOUBLE:
                memcpy(REAL(col) + cursor, src->buf.dbl,
                       (size_t)rg_rows * sizeof(double));
                break;
            case VEC_INT32:
                memcpy(INTEGER(col) + cursor, src->buf.i32,
                       (size_t)rg_rows * sizeof(int32_t));
                break;
            case VEC_BOOL: {
                int *dst = LOGICAL(col) + cursor;
                const uint8_t *s = src->buf.bln;
                for (int64_t i = 0; i < rg_rows; i++)
                    dst[i] = s[i] ? TRUE : FALSE;
                break;
            }
            default:
                vec_batch_free(batch);
                vtr1_close_tdc(f);
                UNPROTECT(2);
                Rf_error("unhandled VecType in C_read_vtr_tdc");
            }
        }
        vec_batch_free(batch);
        cursor += rg_rows;
    }

    vtr1_close_tdc(f);
    UNPROTECT(2);
    return out;
}
