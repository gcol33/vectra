#include "r_bridge.h"
#include "r_bridge_internal.h"
#include "types.h"
#include "csv_write.h"
#include "csv_scan.h"
#include "sql_scan.h"
#include "sql_write.h"
#include "tiff_format.h"
#include "tiff_scan.h"
#include "tiff_write.h"
#include "vtr_write.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* ---- shared helpers ---- */

/* Scan helper: path + batch_size -> format-specific constructor -> wrapped node.
 * Covers the csv and tiff scan nodes which share identical structure. */
typedef VecNode *(*ScanCreateFn)(const char *path, int64_t batch_size);

static SEXP scan_node_create(SEXP path_sexp, SEXP batch_size_sexp,
                             ScanCreateFn create_fn) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));
    int64_t batch_size = (int64_t)Rf_asReal(batch_size_sexp);
    VecNode *sn = create_fn(fpath, batch_size);
    return wrap_node(sn);
}

/* Write helper: unwrap node, extract path, call format-specific writer, free node.
 * The writer receives (node, path, ctx) where ctx carries any extra parameters. */
typedef void (*WriteNodeFn)(VecNode *node, const char *path, void *ctx);

static SEXP write_node_dispatch(SEXP node_xptr, SEXP path_sexp,
                                WriteNodeFn write_fn, void *ctx) {
    VecNode *node = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);
    const char *path = CHAR(STRING_ELT(path_sexp, 0));
    write_fn(node, path, ctx);
    node->free_node(node);
    return R_NilValue;
}

/* Format-specific writer adapters (bridge void* ctx to real signatures) */

static void csv_writer(VecNode *node, const char *path, void *ctx) {
    (void)ctx;
    csv_write_node(node, path);
}

static void sql_writer(VecNode *node, const char *path, void *ctx) {
    const char *table = (const char *)ctx;
    sql_write_node(node, path, table);
}

static void tiff_writer(VecNode *node, const char *path, void *ctx) {
    int use_deflate = *(int *)ctx;
    tiff_write_node(node, path, use_deflate);
}

typedef struct {
    int use_deflate;
    int pixel_type;
    const char *metadata_xml;
    int epsg_geographic;
    int epsg_projected;
    const char *crs_citation;
    int tile_width;
    int tile_height;
    int bigtiff_mode;  /* TIFF_BIGTIFF_* */
} TiffTypedCtx;

static void tiff_typed_writer(VecNode *node, const char *path, void *ctx) {
    TiffTypedCtx *tc = (TiffTypedCtx *)ctx;
    tiff_write_node_typed_ex(node, path, tc->use_deflate, tc->pixel_type,
                             tc->metadata_xml,
                             tc->epsg_geographic, tc->epsg_projected,
                             tc->crs_citation,
                             tc->tile_width, tc->tile_height,
                             tc->bigtiff_mode);
}

typedef struct {
    int64_t batch_size;
    int     comp_level;
    VtrQuantizeSpec *qspecs;  /* NULL or array of n_cols entries */
    int n_qspecs;
    VtrSpatialSpec *sspecs;   /* NULL or array of n_cols entries */
    int n_sspecs;
} VtrWriteCtx;

static void vtr_writer(VecNode *node, const char *path, void *ctx) {
    VtrWriteCtx *wctx = (VtrWriteCtx *)ctx;
    if (wctx->batch_size > 0)
        vtr_write_node_batched_qs(node, path, wctx->batch_size, wctx->comp_level,
                                  wctx->qspecs, wctx->sspecs);
    else
        vtr_write_node_qs(node, path, wctx->comp_level, wctx->qspecs,
                          wctx->sspecs);
}

/* ---- scan entry points ---- */

static VecNode *csv_scan_adapter(const char *path, int64_t bs) {
    return (VecNode *)csv_scan_node_create(path, bs);
}

static VecNode *tiff_scan_adapter(const char *path, int64_t bs) {
    return (VecNode *)tiff_scan_node_create(path, bs);
}

SEXP C_csv_scan_node(SEXP path_sexp, SEXP batch_size_sexp) {
    return scan_node_create(path_sexp, batch_size_sexp, csv_scan_adapter);
}

SEXP C_sql_scan_node(SEXP path_sexp, SEXP table_sexp, SEXP batch_size_sexp) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));
    const char *table = CHAR(STRING_ELT(table_sexp, 0));
    int64_t batch_size = (int64_t)Rf_asReal(batch_size_sexp);
    SqlScanNode *sn = sql_scan_node_create(fpath, table, batch_size);
    return wrap_node((VecNode *)sn);
}

SEXP C_tiff_scan_node(SEXP path_sexp, SEXP batch_size_sexp) {
    return scan_node_create(path_sexp, batch_size_sexp, tiff_scan_adapter);
}

/* ---- write entry points ---- */

SEXP C_write_csv(SEXP node_xptr, SEXP path_sexp) {
    return write_node_dispatch(node_xptr, path_sexp, csv_writer, NULL);
}

SEXP C_write_sqlite(SEXP node_xptr, SEXP path_sexp, SEXP table_sexp) {
    const char *table = CHAR(STRING_ELT(table_sexp, 0));
    return write_node_dispatch(node_xptr, path_sexp, sql_writer, (void *)table);
}

SEXP C_write_tiff(SEXP node_xptr, SEXP path_sexp, SEXP compress_sexp) {
    int use_deflate = Rf_asLogical(compress_sexp);
    return write_node_dispatch(node_xptr, path_sexp, tiff_writer, &use_deflate);
}

/* Parse a quantize R list into a VtrQuantizeSpec array.
   quantize_sexp: named list, e.g. list(temp = c(scale=1000, type="int16"))
   col_names: STRSXP of column names (from schema or data.frame)
   n_cols: number of columns
   Returns malloc'd array of n_cols entries (caller frees), or NULL. */
VtrQuantizeSpec *parse_quantize(SEXP quantize_sexp, SEXP col_names, int n_cols) {
    if (quantize_sexp == R_NilValue || TYPEOF(quantize_sexp) != VECSXP)
        return NULL;

    int n_q = Rf_length(quantize_sexp);
    if (n_q == 0) return NULL;

    /* PROTECT q_names: rchk treats getAttrib results as fresh-allocated
     * SEXPs even when they're rooted via `quantize_sexp`'s attribute
     * pairlist. The Rf_warning / Rf_asReal calls inside the loop below
     * can allocate. */
    SEXP q_names = PROTECT(Rf_getAttrib(quantize_sexp, R_NamesSymbol));
    if (q_names == R_NilValue || TYPEOF(q_names) != STRSXP) {
        UNPROTECT(1);
        return NULL;
    }

    VtrQuantizeSpec *qspecs = (VtrQuantizeSpec *)calloc((size_t)n_cols,
                                                        sizeof(VtrQuantizeSpec));
    if (!qspecs) { UNPROTECT(1); return NULL; }

    for (int qi = 0; qi < n_q; qi++) {
        const char *qcol = CHAR(STRING_ELT(q_names, qi));
        SEXP spec = VECTOR_ELT(quantize_sexp, qi);

        /* Find column index */
        int ci = -1;
        for (int c = 0; c < n_cols; c++) {
            if (strcmp(CHAR(STRING_ELT(col_names, c)), qcol) == 0) {
                ci = c;
                break;
            }
        }
        if (ci < 0) {
            Rf_warning("quantize: column '%s' not found, skipping", qcol);
            continue;
        }

        /* Parse spec: named numeric vector with scale/precision/offset/type */
        if (TYPEOF(spec) != VECSXP && TYPEOF(spec) != REALSXP &&
            TYPEOF(spec) != STRSXP) {
            Rf_warning("quantize: spec for '%s' must be a named vector", qcol);
            continue;
        }

        /* Convert to a named list for uniform access.
           Accepted forms:
             c(scale = 1000, type = "int16")
             c(precision = 0.001, type = "int16")
             list(scale = 1000, type = "int16")
        */
        double scale = 0, offset = 0;
        VecType target = VEC_INT16; /* default */
        int has_scale = 0;

        /* PROTECT snames per iteration: balanced UNPROTECT before continue
         * or end-of-iteration. The Rf_asReal / Rf_warning calls below can
         * trigger GC. */
        SEXP snames = PROTECT(Rf_getAttrib(spec, R_NamesSymbol));
        int slen = Rf_length(spec);

        for (int si = 0; si < slen; si++) {
            if (snames == R_NilValue) continue;
            const char *sn = CHAR(STRING_ELT(snames, si));

            if (strcmp(sn, "scale") == 0) {
                if (TYPEOF(spec) == REALSXP) scale = REAL(spec)[si];
                else if (TYPEOF(spec) == VECSXP) scale = Rf_asReal(VECTOR_ELT(spec, si));
                has_scale = 1;
            } else if (strcmp(sn, "precision") == 0) {
                double prec;
                if (TYPEOF(spec) == REALSXP) prec = REAL(spec)[si];
                else if (TYPEOF(spec) == VECSXP) prec = Rf_asReal(VECTOR_ELT(spec, si));
                else prec = 0;
                if (prec > 0) { scale = 1.0 / prec; has_scale = 1; }
            } else if (strcmp(sn, "offset") == 0) {
                if (TYPEOF(spec) == REALSXP) offset = REAL(spec)[si];
                else if (TYPEOF(spec) == VECSXP) offset = Rf_asReal(VECTOR_ELT(spec, si));
            } else if (strcmp(sn, "type") == 0) {
                const char *tstr = NULL;
                if (TYPEOF(spec) == STRSXP) tstr = CHAR(STRING_ELT(spec, si));
                else if (TYPEOF(spec) == VECSXP) {
                    SEXP ts = VECTOR_ELT(spec, si);
                    if (TYPEOF(ts) == STRSXP) tstr = CHAR(STRING_ELT(ts, 0));
                }
                if (tstr) {
                    if (strcmp(tstr, "int8") == 0)       target = VEC_INT8;
                    else if (strcmp(tstr, "int16") == 0)  target = VEC_INT16;
                    else if (strcmp(tstr, "int32") == 0)  target = VEC_INT32;
                }
            }
        }

        if (!has_scale || scale <= 0) {
            UNPROTECT(1); /* snames */
            Rf_warning("quantize: column '%s' needs positive scale or precision", qcol);
            continue;
        }

        qspecs[ci].enabled = 1;
        qspecs[ci].scale = scale;
        qspecs[ci].offset = offset;
        qspecs[ci].target_type = target;

        UNPROTECT(1); /* snames */
    }

    UNPROTECT(1); /* q_names */
    return qspecs;
}

/* Parse a spatial R list into a VtrSpatialSpec array.
   spatial_sexp: named list with nx, ny, and optionally cols, predictor, tile_size.
   Example: list(nx = 2000, ny = 2000)
   Or per-column: list(temp = list(nx = 2000, ny = 2000))
   Returns malloc'd array of n_cols entries (caller frees), or NULL. */
VtrSpatialSpec *parse_spatial(SEXP spatial_sexp, SEXP col_names, int n_cols) {
    if (spatial_sexp == R_NilValue || TYPEOF(spatial_sexp) != VECSXP)
        return NULL;

    /* PROTECT s_names: rchk treats getAttrib results as fresh-allocated
     * SEXPs even when they're rooted via spatial_sexp. The Rf_asReal /
     * Rf_asInteger calls below can allocate. */
    SEXP s_names = PROTECT(Rf_getAttrib(spatial_sexp, R_NamesSymbol));

    /* Check if this is a global spec (has nx/ny at top level) or per-column */
    int is_global = 0;
    if (s_names != R_NilValue) {
        for (int i = 0; i < Rf_length(spatial_sexp); i++) {
            const char *nm = CHAR(STRING_ELT(s_names, i));
            if (strcmp(nm, "nx") == 0 || strcmp(nm, "ny") == 0) {
                is_global = 1;
                break;
            }
        }
    }

    VtrSpatialSpec *sspecs = (VtrSpatialSpec *)calloc((size_t)n_cols,
                                                       sizeof(VtrSpatialSpec));
    if (!sspecs) { UNPROTECT(1); return NULL; }

    if (is_global) {
        /* Apply to all numeric columns */
        uint32_t nx = 0, ny = 0;
        int predictor = -1; /* auto */
        uint16_t tile_size = 32;

        for (int i = 0; i < Rf_length(spatial_sexp); i++) {
            const char *nm = CHAR(STRING_ELT(s_names, i));
            if (strcmp(nm, "nx") == 0)
                nx = (uint32_t)Rf_asReal(VECTOR_ELT(spatial_sexp, i));
            else if (strcmp(nm, "ny") == 0)
                ny = (uint32_t)Rf_asReal(VECTOR_ELT(spatial_sexp, i));
            else if (strcmp(nm, "predictor") == 0)
                predictor = Rf_asInteger(VECTOR_ELT(spatial_sexp, i));
            else if (strcmp(nm, "tile_size") == 0)
                tile_size = (uint16_t)Rf_asInteger(VECTOR_ELT(spatial_sexp, i));
        }

        if (nx == 0 || ny == 0) {
            free(sspecs);
            UNPROTECT(1); /* s_names */
            return NULL;
        }

        for (int c = 0; c < n_cols; c++) {
            sspecs[c].enabled = 1;
            sspecs[c].nx = nx;
            sspecs[c].ny = ny;
            sspecs[c].predictor = predictor;
            sspecs[c].tile_size = tile_size;
        }
    } else {
        /* Per-column specs */
        for (int qi = 0; qi < Rf_length(spatial_sexp); qi++) {
            if (s_names == R_NilValue) continue;
            const char *scol = CHAR(STRING_ELT(s_names, qi));
            SEXP spec = VECTOR_ELT(spatial_sexp, qi);
            if (TYPEOF(spec) != VECSXP) continue;

            int ci = -1;
            for (int c = 0; c < n_cols; c++) {
                if (strcmp(CHAR(STRING_ELT(col_names, c)), scol) == 0) {
                    ci = c;
                    break;
                }
            }
            if (ci < 0) continue;

            /* PROTECT sn per iteration; balanced UNPROTECT at end. */
            SEXP sn = PROTECT(Rf_getAttrib(spec, R_NamesSymbol));
            uint32_t nx = 0, ny = 0;
            int predictor = -1;
            uint16_t tile_size = 32;

            for (int si = 0; si < Rf_length(spec); si++) {
                if (sn == R_NilValue) continue;
                const char *nm = CHAR(STRING_ELT(sn, si));
                if (strcmp(nm, "nx") == 0)
                    nx = (uint32_t)Rf_asReal(VECTOR_ELT(spec, si));
                else if (strcmp(nm, "ny") == 0)
                    ny = (uint32_t)Rf_asReal(VECTOR_ELT(spec, si));
                else if (strcmp(nm, "predictor") == 0)
                    predictor = Rf_asInteger(VECTOR_ELT(spec, si));
                else if (strcmp(nm, "tile_size") == 0)
                    tile_size = (uint16_t)Rf_asInteger(VECTOR_ELT(spec, si));
            }

            if (nx > 0 && ny > 0) {
                sspecs[ci].enabled = 1;
                sspecs[ci].nx = nx;
                sspecs[ci].ny = ny;
                sspecs[ci].predictor = predictor;
                sspecs[ci].tile_size = tile_size;
            }

            UNPROTECT(1); /* sn */
        }
    }

    UNPROTECT(1); /* s_names */
    return sspecs;
}

SEXP C_write_vtr_node(SEXP node_xptr, SEXP path_sexp, SEXP batch_size_sexp,
                      SEXP compress_sexp, SEXP col_types_sexp,
                      SEXP quantize_sexp, SEXP spatial_sexp) {
    (void)col_types_sexp; /* col_types narrowing not yet supported for node writes */
    int comp_level = 1; /* default: fast */
    if (compress_sexp != R_NilValue && TYPEOF(compress_sexp) == STRSXP &&
        Rf_length(compress_sexp) > 0) {
        const char *cstr = CHAR(STRING_ELT(compress_sexp, 0));
        if (strcmp(cstr, "fast") == 0) comp_level = 1;
        else if (strcmp(cstr, "small") == 0) comp_level = 2;
        else if (strcmp(cstr, "none") == 0) comp_level = 0;
        else vectra_error("unknown compress level '%s' (expected \"fast\", \"small\", or \"none\")", cstr);
    }

    /* Parse quantize + spatial specs */
    VecNode *node = unwrap_node(node_xptr);
    int n_cols = node->output_schema.n_cols;
    SEXP col_names_sexp = PROTECT(Rf_allocVector(STRSXP, n_cols));
    for (int i = 0; i < n_cols; i++)
        SET_STRING_ELT(col_names_sexp, i,
                       Rf_mkChar(node->output_schema.col_names[i]));
    VtrQuantizeSpec *qspecs = parse_quantize(quantize_sexp, col_names_sexp, n_cols);
    VtrSpatialSpec *sspecs = parse_spatial(spatial_sexp, col_names_sexp, n_cols);
    UNPROTECT(1); /* col_names_sexp */

    VtrWriteCtx ctx = {
        .batch_size = (batch_size_sexp == R_NilValue) ? 0 : (int64_t)Rf_asReal(batch_size_sexp),
        .comp_level = comp_level,
        .qspecs = qspecs,
        .n_qspecs = qspecs ? n_cols : 0,
        .sspecs = sspecs,
        .n_sspecs = sspecs ? n_cols : 0
    };
    SEXP result = write_node_dispatch(node_xptr, path_sexp, vtr_writer, &ctx);
    free(qspecs);
    free(sspecs);
    return result;
}

/* --- C_tiff_extract_points --- */

SEXP C_tiff_extract_points(SEXP path_sexp, SEXP x_sexp, SEXP y_sexp) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));
    int64_t n = Rf_xlength(x_sexp);

    TiffReader *reader = NULL;
    if (tiff_reader_open(fpath, &reader) != 0) {
        const char *msg = reader ? tiff_reader_errmsg(reader) : "unknown";
        tiff_reader_close(reader);
        vectra_error("cannot open GeoTIFF: %s", msg);
    }

    int nb = tiff_reader_nbands(reader);

    /* Allocate output band arrays */
    double **bands = (double **)malloc((size_t)nb * sizeof(double *));
    if (!bands) {
        tiff_reader_close(reader);
        vectra_error("alloc failed for point extraction");
    }
    for (int b = 0; b < nb; b++) {
        bands[b] = (double *)malloc((size_t)n * sizeof(double));
        if (!bands[b]) {
            for (int j = 0; j < b; j++) free(bands[j]);
            free(bands);
            tiff_reader_close(reader);
            vectra_error("alloc failed for band data");
        }
    }

    /* Extract */
    if (tiff_reader_extract_points(reader, n, REAL(x_sexp), REAL(y_sexp),
                                    bands) != 0) {
        const char *msg = tiff_reader_errmsg(reader);
        for (int b = 0; b < nb; b++) free(bands[b]);
        free(bands);
        tiff_reader_close(reader);
        vectra_error("TIFF extract error: %s", msg);
    }

    tiff_reader_close(reader);

    /* Build R data.frame: x, y, band1, band2, ... */
    int n_cols = 2 + nb;
    SEXP result = PROTECT(Rf_allocVector(VECSXP, n_cols));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, n_cols));

    /* x column (copy input) */
    SEXP x_out = PROTECT(Rf_allocVector(REALSXP, n));
    memcpy(REAL(x_out), REAL(x_sexp), (size_t)n * sizeof(double));
    SET_VECTOR_ELT(result, 0, x_out);
    SET_STRING_ELT(names, 0, Rf_mkChar("x"));
    UNPROTECT(1); /* x_out */

    /* y column (copy input) */
    SEXP y_out = PROTECT(Rf_allocVector(REALSXP, n));
    memcpy(REAL(y_out), REAL(y_sexp), (size_t)n * sizeof(double));
    SET_VECTOR_ELT(result, 1, y_out);
    SET_STRING_ELT(names, 1, Rf_mkChar("y"));
    UNPROTECT(1); /* y_out */

    /* Band columns */
    for (int b = 0; b < nb; b++) {
        SEXP col = PROTECT(Rf_allocVector(REALSXP, n));
        double *dst = REAL(col);
        for (int64_t i = 0; i < n; i++) {
            dst[i] = isnan(bands[b][i]) ? NA_REAL : bands[b][i];
        }
        SET_VECTOR_ELT(result, 2 + b, col);
        char bname[16];
        snprintf(bname, 16, "band%d", b + 1);
        SET_STRING_ELT(names, 2 + b, Rf_mkChar(bname));
        UNPROTECT(1); /* col */
        free(bands[b]);
    }
    free(bands);

    /* Set as data.frame */
    Rf_setAttrib(result, R_NamesSymbol, names);
    SEXP rownames = PROTECT(Rf_allocVector(INTSXP, 2));
    INTEGER(rownames)[0] = NA_INTEGER;
    INTEGER(rownames)[1] = -(int)n;
    Rf_setAttrib(result, R_RowNamesSymbol, rownames);
    Rf_setAttrib(result, R_ClassSymbol, Rf_mkString("data.frame"));

    UNPROTECT(3); /* result, names, rownames */
    return result;
}

/* --- C_write_tiff_typed --- */

SEXP C_write_tiff_typed(SEXP node_xptr, SEXP path_sexp,
                        SEXP compress_sexp, SEXP pixel_type_sexp,
                        SEXP metadata_sexp,
                        SEXP epsg_geog_sexp, SEXP epsg_proj_sexp,
                        SEXP crs_citation_sexp,
                        SEXP tile_width_sexp, SEXP tile_height_sexp,
                        SEXP bigtiff_sexp) {
    TiffTypedCtx ctx;
    ctx.use_deflate = Rf_asLogical(compress_sexp);
    ctx.pixel_type = Rf_asInteger(pixel_type_sexp);
    ctx.metadata_xml = (metadata_sexp == R_NilValue)
                        ? NULL : CHAR(STRING_ELT(metadata_sexp, 0));
    ctx.epsg_geographic = (epsg_geog_sexp == R_NilValue)
                          ? 0 : Rf_asInteger(epsg_geog_sexp);
    ctx.epsg_projected  = (epsg_proj_sexp == R_NilValue)
                          ? 0 : Rf_asInteger(epsg_proj_sexp);
    if (ctx.epsg_geographic == NA_INTEGER) ctx.epsg_geographic = 0;
    if (ctx.epsg_projected  == NA_INTEGER) ctx.epsg_projected  = 0;
    ctx.crs_citation = (crs_citation_sexp == R_NilValue)
                        ? NULL : CHAR(STRING_ELT(crs_citation_sexp, 0));
    ctx.tile_width  = (tile_width_sexp  == R_NilValue)
                       ? 0 : Rf_asInteger(tile_width_sexp);
    ctx.tile_height = (tile_height_sexp == R_NilValue)
                       ? 0 : Rf_asInteger(tile_height_sexp);
    if (ctx.tile_width  == NA_INTEGER) ctx.tile_width  = 0;
    if (ctx.tile_height == NA_INTEGER) ctx.tile_height = 0;

    /* bigtiff: integer 0/1/2 = AUTO/OFF/FORCE. Defaults to AUTO when NULL. */
    ctx.bigtiff_mode = TIFF_BIGTIFF_AUTO;
    if (bigtiff_sexp != R_NilValue) {
        int bm = Rf_asInteger(bigtiff_sexp);
        if (bm == NA_INTEGER) bm = TIFF_BIGTIFF_AUTO;
        if (bm == TIFF_BIGTIFF_OFF || bm == TIFF_BIGTIFF_FORCE
            || bm == TIFF_BIGTIFF_AUTO) {
            ctx.bigtiff_mode = bm;
        }
    }

    return write_node_dispatch(node_xptr, path_sexp, tiff_typed_writer, &ctx);
}

/* --- C_tiff_read_metadata --- */

SEXP C_tiff_read_metadata(SEXP path_sexp) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));

    TiffReader *reader = NULL;
    if (tiff_reader_open(fpath, &reader) != 0) {
        const char *msg = reader ? tiff_reader_errmsg(reader) : "unknown";
        tiff_reader_close(reader);
        vectra_error("cannot open GeoTIFF: %s", msg);
    }

    const char *meta = tiff_reader_metadata(reader);
    SEXP result;
    if (meta) {
        result = PROTECT(Rf_mkString(meta));
    } else {
        result = PROTECT(Rf_ScalarString(NA_STRING));
    }

    tiff_reader_close(reader);
    UNPROTECT(1);
    return result;
}

/* --- C_tiff_scan_meta --- */

SEXP C_tiff_scan_meta(SEXP node_xptr) {
    VecNode *node = unwrap_node(node_xptr);
    if (!node->kind || strcmp(node->kind, "TiffScanNode") != 0)
        vectra_error("not a TiffScanNode");
    TiffScanNode *sn = (TiffScanNode *)node;
    TiffReader *r = sn->reader;

    SEXP result = PROTECT(Rf_allocVector(VECSXP, 7));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, 7));

    SET_STRING_ELT(names, 0, Rf_mkChar("width"));
    SET_STRING_ELT(names, 1, Rf_mkChar("height"));
    SET_STRING_ELT(names, 2, Rf_mkChar("nbands"));
    SET_STRING_ELT(names, 3, Rf_mkChar("gt"));
    SET_STRING_ELT(names, 4, Rf_mkChar("nodata"));
    SET_STRING_ELT(names, 5, Rf_mkChar("epsg"));
    SET_STRING_ELT(names, 6, Rf_mkChar("crs_citation"));

    SET_VECTOR_ELT(result, 0, Rf_ScalarReal((double)tiff_reader_width(r)));
    SET_VECTOR_ELT(result, 1, Rf_ScalarReal((double)tiff_reader_height(r)));
    SET_VECTOR_ELT(result, 2, Rf_ScalarInteger(tiff_reader_nbands(r)));

    SEXP gt_sexp = PROTECT(Rf_allocVector(REALSXP, 6));
    const double *gt = tiff_reader_geotransform(r);
    memcpy(REAL(gt_sexp), gt, 6 * sizeof(double));
    SET_VECTOR_ELT(result, 3, gt_sexp);

    if (tiff_reader_has_nodata(r))
        SET_VECTOR_ELT(result, 4, Rf_ScalarReal(tiff_reader_nodata(r)));
    else
        SET_VECTOR_ELT(result, 4, Rf_ScalarReal(NA_REAL));

    int32_t epsg = tiff_reader_epsg(r);
    SET_VECTOR_ELT(result, 5,
        Rf_ScalarInteger(epsg > 0 ? (int)epsg : NA_INTEGER));

    const char *cit = tiff_reader_crs_citation(r);
    if (cit)
        SET_VECTOR_ELT(result, 6, Rf_mkString(cit));
    else
        SET_VECTOR_ELT(result, 6, Rf_ScalarString(NA_STRING));

    Rf_setAttrib(result, R_NamesSymbol, names);
    UNPROTECT(3);
    return result;
}

/* --- C_tiff_read_crs --- */

SEXP C_tiff_read_crs(SEXP path_sexp) {
    const char *fpath = CHAR(STRING_ELT(path_sexp, 0));

    TiffReader *reader = NULL;
    if (tiff_reader_open(fpath, &reader) != 0) {
        const char *msg = reader ? tiff_reader_errmsg(reader) : "unknown";
        tiff_reader_close(reader);
        vectra_error("cannot open GeoTIFF: %s", msg);
    }

    SEXP result = PROTECT(Rf_allocVector(VECSXP, 2));
    SEXP names  = PROTECT(Rf_allocVector(STRSXP, 2));
    SET_STRING_ELT(names, 0, Rf_mkChar("epsg"));
    SET_STRING_ELT(names, 1, Rf_mkChar("citation"));

    int32_t epsg = tiff_reader_epsg(reader);
    SET_VECTOR_ELT(result, 0,
        Rf_ScalarInteger(epsg > 0 ? (int)epsg : NA_INTEGER));

    const char *cit = tiff_reader_crs_citation(reader);
    if (cit)
        SET_VECTOR_ELT(result, 1, Rf_mkString(cit));
    else
        SET_VECTOR_ELT(result, 1, Rf_ScalarString(NA_STRING));

    Rf_setAttrib(result, R_NamesSymbol, names);
    tiff_reader_close(reader);
    UNPROTECT(2);
    return result;
}

