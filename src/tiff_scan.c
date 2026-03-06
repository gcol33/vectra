#include "tiff_scan.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

/* ------------------------------------------------------------------ */
/*  VecNode vtable                                                     */
/* ------------------------------------------------------------------ */

static VecBatch *tiff_scan_next_batch(VecNode *self) {
    TiffScanNode *sn = (TiffScanNode *)self;
    if (sn->exhausted) return NULL;

    TiffReader *r = sn->reader;
    int64_t W = tiff_reader_width(r);
    int64_t H = tiff_reader_height(r);
    int nb = tiff_reader_nbands(r);

    /* How many raster rows to read this batch */
    int64_t n_rows = sn->batch_size;
    if (sn->next_row + n_rows > H)
        n_rows = H - sn->next_row;
    if (n_rows <= 0) {
        sn->exhausted = 1;
        return NULL;
    }

    int64_t n_pixels = W * n_rows;
    int n_cols = 2 + nb; /* x, y, band1, band2, ... */

    /* Allocate temp arrays for tiff_reader_read_rows */
    double *tmp_x = (double *)malloc((size_t)n_pixels * sizeof(double));
    double *tmp_y = (double *)malloc((size_t)n_pixels * sizeof(double));
    double **tmp_bands = (double **)malloc((size_t)nb * sizeof(double *));
    if (!tmp_x || !tmp_y || !tmp_bands)
        vectra_error("alloc failed for TIFF scan batch");

    for (int b = 0; b < nb; b++) {
        tmp_bands[b] = (double *)malloc((size_t)n_pixels * sizeof(double));
        if (!tmp_bands[b]) vectra_error("alloc failed for TIFF band data");
    }

    /* Read rows */
    if (tiff_reader_read_rows(r, sn->next_row, n_rows,
                                tmp_x, tmp_y, tmp_bands) != 0) {
        vectra_error("TIFF read error: %s", tiff_reader_errmsg(r));
    }

    sn->next_row += n_rows;
    if (sn->next_row >= H)
        sn->exhausted = 1;

    /* Build VecBatch: columns = x, y, band1, ... */
    VecBatch *batch = vec_batch_alloc(n_cols, n_pixels);

    for (int c = 0; c < n_cols; c++) {
        const char *nm = sn->base.output_schema.col_names[c];
        batch->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(batch->col_names[c], nm);
    }

    /* x column */
    {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n_pixels);
        vec_array_set_all_valid(&arr);
        memcpy(arr.buf.dbl, tmp_x, (size_t)n_pixels * sizeof(double));
        batch->columns[0] = arr;
    }

    /* y column */
    {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n_pixels);
        vec_array_set_all_valid(&arr);
        memcpy(arr.buf.dbl, tmp_y, (size_t)n_pixels * sizeof(double));
        batch->columns[1] = arr;
    }

    /* band columns */
    for (int b = 0; b < nb; b++) {
        VecArray arr = vec_array_alloc(VEC_DOUBLE, n_pixels);
        vec_array_set_all_valid(&arr);
        for (int64_t i = 0; i < n_pixels; i++) {
            double val = tmp_bands[b][i];
            if (isnan(val)) {
                vec_array_set_null(&arr, i);
                arr.buf.dbl[i] = 0.0;
            } else {
                arr.buf.dbl[i] = val;
            }
        }
        batch->columns[2 + b] = arr;
    }

    /* Cleanup temp arrays */
    free(tmp_x);
    free(tmp_y);
    for (int b = 0; b < nb; b++) free(tmp_bands[b]);
    free(tmp_bands);

    return batch;
}

static void tiff_scan_free(VecNode *self) {
    TiffScanNode *sn = (TiffScanNode *)self;
    tiff_reader_close(sn->reader);
    vec_schema_free(&sn->base.output_schema);
    free(sn);
}

/* ------------------------------------------------------------------ */
/*  Constructor                                                        */
/* ------------------------------------------------------------------ */

TiffScanNode *tiff_scan_node_create(const char *path, int64_t batch_size) {
    TiffReader *reader = NULL;
    if (tiff_reader_open(path, &reader) != 0) {
        const char *msg = reader ? tiff_reader_errmsg(reader) : "unknown";
        tiff_reader_close(reader);
        vectra_error("cannot open GeoTIFF: %s", msg);
    }

    int nb = tiff_reader_nbands(reader);
    int n_cols = 2 + nb;

    /* Build column names: x, y, band1, band2, ... */
    char **names = (char **)malloc((size_t)n_cols * sizeof(char *));
    VecType *types = (VecType *)malloc((size_t)n_cols * sizeof(VecType));
    if (!names || !types) vectra_error("alloc failed for TIFF schema");

    names[0] = "x";
    names[1] = "y";
    types[0] = VEC_DOUBLE;
    types[1] = VEC_DOUBLE;

    char band_names[TIFF_MAX_BANDS][16];
    for (int b = 0; b < nb; b++) {
        snprintf(band_names[b], 16, "band%d", b + 1);
        names[2 + b] = band_names[b];
        types[2 + b] = VEC_DOUBLE;
    }

    VecSchema schema = vec_schema_create(n_cols, names, types);
    free(names);
    free(types);

    TiffScanNode *sn = (TiffScanNode *)calloc(1, sizeof(TiffScanNode));
    if (!sn) vectra_error("alloc failed for TiffScanNode");

    sn->reader = reader;
    sn->n_bands = nb;
    sn->batch_size = batch_size > 0 ? batch_size : 256;
    sn->next_row = 0;
    sn->exhausted = 0;

    sn->base.output_schema = schema;
    sn->base.next_batch = tiff_scan_next_batch;
    sn->base.free_node = tiff_scan_free;
    sn->base.kind = "TiffScanNode";

    return sn;
}
