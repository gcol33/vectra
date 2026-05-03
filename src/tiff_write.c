#include "tiff_write.h"
#include "tiff_format.h"
#include "array.h"
#include "batch.h"
#include "error.h"
#include "grow.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* ------------------------------------------------------------------ */
/*  Double comparison for qsort                                        */
/* ------------------------------------------------------------------ */

static int dbl_cmp(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Growable double array                                              */
/* ------------------------------------------------------------------ */

typedef struct {
    double *data;
    int64_t len;
    int64_t cap;
} DblVec;

static void dv_init(DblVec *v) {
    v->cap = 4096;
    v->data = (double *)malloc((size_t)v->cap * sizeof(double));
    if (!v->data) vectra_error("alloc failed for DblVec");
    v->len = 0;
}

static void dv_push(DblVec *v, double val) {
    vec_grow_to((void **)&v->data, &v->cap, v->len + 1, sizeof(double),
                "DblVec");
    v->data[v->len++] = val;
}

static void dv_free(DblVec *v) { free(v->data); v->data = NULL; }

/* ------------------------------------------------------------------ */
/*  Infer regular grid from x/y coordinate arrays                      */
/* ------------------------------------------------------------------ */

/* Find unique sorted values. Returns count. Caller frees *out. */
static int64_t unique_sorted(const double *vals, int64_t n, double **out) {
    double *sorted = (double *)malloc((size_t)n * sizeof(double));
    if (!sorted) return 0;
    memcpy(sorted, vals, (size_t)n * sizeof(double));
    qsort(sorted, (size_t)n, sizeof(double), dbl_cmp);

    /* Count unique */
    int64_t count = 0;
    for (int64_t i = 0; i < n; i++) {
        if (i == 0 || sorted[i] != sorted[i-1])
            count++;
    }

    double *uniq = (double *)malloc((size_t)count * sizeof(double));
    if (!uniq) { free(sorted); return 0; }

    int64_t j = 0;
    for (int64_t i = 0; i < n; i++) {
        if (i == 0 || sorted[i] != sorted[i-1])
            uniq[j++] = sorted[i];
    }

    free(sorted);
    *out = uniq;
    return count;
}

/* Find resolution: minimum positive gap between consecutive unique values */
static double find_resolution(const double *uniq, int64_t n) {
    if (n < 2) return 1.0;
    double res = uniq[1] - uniq[0];
    for (int64_t i = 2; i < n; i++) {
        double gap = uniq[i] - uniq[i-1];
        if (gap > 0 && gap < res) res = gap;
    }
    return res;
}

/* ------------------------------------------------------------------ */
/*  Main writer function                                               */
/* ------------------------------------------------------------------ */

void tiff_write_node(VecNode *node, const char *path, int use_deflate) {
    tiff_write_node_typed(node, path, use_deflate, TIFF_PIXEL_FLOAT64, NULL,
                          0, 0, NULL, 0, 0);
}

void tiff_write_node_typed(VecNode *node, const char *path, int use_deflate,
                           int pixel_type, const char *metadata_xml,
                           int epsg_geographic, int epsg_projected,
                           const char *crs_citation,
                           int tile_width, int tile_height) {
    tiff_write_node_typed_ex(node, path, use_deflate, pixel_type, metadata_xml,
                             epsg_geographic, epsg_projected, crs_citation,
                             tile_width, tile_height, TIFF_BIGTIFF_AUTO);
}

void tiff_write_node_typed_ex(VecNode *node, const char *path, int use_deflate,
                              int pixel_type, const char *metadata_xml,
                              int epsg_geographic, int epsg_projected,
                              const char *crs_citation,
                              int tile_width, int tile_height,
                              int bigtiff_mode) {
    const VecSchema *schema = &node->output_schema;
    int n_cols = schema->n_cols;

    /* Find x, y column indices */
    int x_idx = -1, y_idx = -1;
    for (int c = 0; c < n_cols; c++) {
        if (strcmp(schema->col_names[c], "x") == 0) x_idx = c;
        else if (strcmp(schema->col_names[c], "y") == 0) y_idx = c;
    }
    if (x_idx < 0 || y_idx < 0)
        vectra_error("write_tiff: data must have 'x' and 'y' columns");

    /* Find band columns (all double columns that are not x or y) */
    int band_indices[TIFF_MAX_BANDS];
    int n_bands = 0;
    for (int c = 0; c < n_cols; c++) {
        if (c == x_idx || c == y_idx) continue;
        if (schema->col_types[c] != VEC_DOUBLE)
            vectra_error("write_tiff: band column '%s' must be double",
                         schema->col_names[c]);
        if (n_bands >= TIFF_MAX_BANDS)
            vectra_error("write_tiff: too many bands");
        band_indices[n_bands++] = c;
    }
    if (n_bands == 0)
        vectra_error("write_tiff: no band columns found");

    /* Pull all batches, accumulate x/y/band values */
    DblVec all_x, all_y;
    DblVec all_bands[TIFF_MAX_BANDS];
    dv_init(&all_x);
    dv_init(&all_y);
    for (int b = 0; b < n_bands; b++) dv_init(&all_bands[b]);

    VecBatch *batch;
    while ((batch = node->next_batch(node)) != NULL) {
        int64_t n_log = vec_batch_logical_rows(batch);

        for (int64_t li = 0; li < n_log; li++) {
            int64_t pi = vec_batch_physical_row(batch, li);

            /* Skip if x or y is NA */
            if (!vec_array_is_valid(&batch->columns[x_idx], pi)) continue;
            if (!vec_array_is_valid(&batch->columns[y_idx], pi)) continue;

            dv_push(&all_x, batch->columns[x_idx].buf.dbl[pi]);
            dv_push(&all_y, batch->columns[y_idx].buf.dbl[pi]);

            for (int b = 0; b < n_bands; b++) {
                int ci = band_indices[b];
                if (vec_array_is_valid(&batch->columns[ci], pi))
                    dv_push(&all_bands[b], batch->columns[ci].buf.dbl[pi]);
                else
                    dv_push(&all_bands[b], NAN);
            }
        }
        vec_batch_free(batch);
    }

    int64_t n_pixels = all_x.len;
    if (n_pixels == 0) {
        dv_free(&all_x); dv_free(&all_y);
        for (int b = 0; b < n_bands; b++) dv_free(&all_bands[b]);
        vectra_error("write_tiff: no valid pixels to write");
    }

    /* Infer grid from x/y coordinates */
    double *ux = NULL, *uy = NULL;
    int64_t nx = unique_sorted(all_x.data, n_pixels, &ux);
    int64_t ny = unique_sorted(all_y.data, n_pixels, &uy);

    double xres = find_resolution(ux, nx);
    double yres = find_resolution(uy, ny);
    double xmin = ux[0];
    double ymax = uy[ny - 1];

    int64_t width = nx;
    int64_t height = ny;

    /* Build geotransform (pixel edge, not center) */
    double gt[6];
    gt[0] = xmin - xres / 2.0;
    gt[1] = xres;
    gt[2] = 0.0;
    gt[3] = ymax + yres / 2.0;
    gt[4] = 0.0;
    gt[5] = -yres;

    /* Allocate grid arrays filled with NaN */
    double **grid = (double **)malloc((size_t)n_bands * sizeof(double *));
    if (!grid) vectra_error("alloc failed for TIFF grid");
    for (int b = 0; b < n_bands; b++) {
        grid[b] = (double *)malloc((size_t)(width * height) * sizeof(double));
        if (!grid[b]) vectra_error("alloc failed for TIFF grid band");
        for (int64_t i = 0; i < width * height; i++)
            grid[b][i] = NAN;
    }

    /* Place each pixel into the grid */
    for (int64_t i = 0; i < n_pixels; i++) {
        int64_t col = (int64_t)round((all_x.data[i] - xmin) / xres);
        int64_t row = (int64_t)round((ymax - all_y.data[i]) / yres);
        if (col < 0 || col >= width || row < 0 || row >= height) continue;
        int64_t idx = row * width + col;
        for (int b = 0; b < n_bands; b++)
            grid[b][idx] = all_bands[b].data[i];
    }

    /* Write TIFF */
    TiffWriter *writer = NULL;
    double nodata_val = NAN;
    /* For integer types, set a conventional nodata if caller hasn't */
    if (pixel_type == TIFF_PIXEL_INT16)  nodata_val = -32768.0;
    else if (pixel_type == TIFF_PIXEL_INT32)  nodata_val = -2147483648.0;
    else if (pixel_type == TIFF_PIXEL_UINT8)  nodata_val = 255.0;
    else if (pixel_type == TIFF_PIXEL_UINT16) nodata_val = 65535.0;

    int compression_code = use_deflate ? TIFF_COMPRESS_DEFLATE
                                       : TIFF_COMPRESS_NONE;
    int tw = tile_width  > 0 ? tile_width  : 0;
    int th = tile_height > 0 ? tile_height : 0;
    int open_rc;
    if (tw > 0 && th > 0) {
        open_rc = tiff_writer_open_tiled(path, &writer, width, height, n_bands,
                                         gt, nodata_val, compression_code,
                                         pixel_type, tw, th);
    } else {
        open_rc = tiff_writer_open_ex(path, &writer, width, height, n_bands,
                                      gt, nodata_val, compression_code,
                                      pixel_type, bigtiff_mode);
    }
    if (open_rc != 0) {
        const char *msg = writer ? tiff_writer_errmsg(writer) : "unknown";
        tiff_writer_close(writer);
        vectra_error("write_tiff failed: %s", msg);
    }

    if (metadata_xml)
        tiff_writer_set_metadata(writer, metadata_xml);

    if (epsg_geographic > 0 || epsg_projected > 0 || crs_citation)
        tiff_writer_set_crs(writer, epsg_geographic, epsg_projected,
                            crs_citation);

    /* Write all rows at once */
    if (tiff_writer_write_rows(writer, 0, height,
                                (const double *const *)grid) != 0) {
        const char *msg = tiff_writer_errmsg(writer);
        tiff_writer_close(writer);
        vectra_error("write_tiff write error: %s", msg);
    }

    tiff_writer_finish(writer);
    tiff_writer_close(writer);

    /* Cleanup */
    for (int b = 0; b < n_bands; b++) {
        free(grid[b]);
        dv_free(&all_bands[b]);
    }
    free(grid);
    free(ux);
    free(uy);
    dv_free(&all_x);
    dv_free(&all_y);
}
