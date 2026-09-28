#include "part_spill.h"
#include "array.h"
#include "builder.h"
#include "batch.h"
#include "schema.h"
#include "vtr1_tdc.h"
#include "vtr_codec.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

struct PartSpill {
    VecSchema         schema;
    int              *col_map;   /* spill column -> input column */
    int               K;
    char *const      *paths;
    Vtr1TdcWriter   **writers;   /* opened on first flush / touch */
    VecArrayBuilder **bld;       /* per-partition buffers, NULL until used */
    int64_t           held;      /* bytes buffered across partitions */
    int64_t           buf_budget;
};

PartSpill *part_spill_create(const VecSchema *schema, const int *col_map,
                             int K, char *const *paths, int64_t buf_budget) {
    PartSpill *ps = (PartSpill *)calloc(1, sizeof(PartSpill));
    if (!ps) vectra_error("alloc failed for partition spill");
    int nc = schema->n_cols;
    ps->schema = vec_schema_copy(schema);
    ps->col_map = (int *)malloc((size_t)(nc > 0 ? nc : 1) * sizeof(int));
    ps->writers = (Vtr1TdcWriter **)calloc((size_t)K, sizeof(Vtr1TdcWriter *));
    ps->bld = (VecArrayBuilder **)calloc((size_t)K, sizeof(VecArrayBuilder *));
    if (!ps->col_map || !ps->writers || !ps->bld)
        vectra_error("alloc failed for partition spill");
    for (int c = 0; c < nc; c++) ps->col_map[c] = col_map ? col_map[c] : c;
    ps->K = K;
    ps->paths = paths;
    ps->buf_budget = buf_budget;
    return ps;
}

void part_spill_touch(PartSpill *ps, int p) {
    if (!ps->writers[p])
        ps->writers[p] = vtr1_open_tdc_writer(ps->paths[p], &ps->schema);
}

int part_spill_used(const PartSpill *ps, int p) {
    return ps->writers[p] != NULL || (ps->bld[p] && ps->bld[p][0].length > 0);
}

static void flush_one(PartSpill *ps, int p) {
    VecArrayBuilder *bld = ps->bld[p];
    if (!bld || bld[0].length == 0) return;
    int n_cols = ps->schema.n_cols;
    ps->held -= vec_builders_bytes(bld, n_cols);
    VecBatch *ob = vec_batch_alloc(n_cols, bld[0].length);
    for (int c = 0; c < n_cols; c++) {
        ob->columns[c] = vec_builder_finish(&bld[c]);
        ob->col_names[c] = (char *)malloc(strlen(ps->schema.col_names[c]) + 1);
        strcpy(ob->col_names[c], ps->schema.col_names[c]);
        bld[c] = vec_builder_init(ps->schema.col_types[c]);
    }
    part_spill_touch(ps, p);
    vtr1_write_rowgroup_tdc(ps->writers[p], ob, VTR_SPILL_COMPRESS, NULL, NULL);
    vec_batch_free(ob);
}

/* Append the gathered physical rows to partition p's buffer. */
static void append_rows(PartSpill *ps, int p, const VecBatch *batch,
                        const int32_t *rows, int32_t m) {
    int n_cols = ps->schema.n_cols;
    VecArrayBuilder *bld = ps->bld[p];
    if (!bld) {
        bld = (VecArrayBuilder *)calloc((size_t)n_cols, sizeof(VecArrayBuilder));
        if (!bld) vectra_error("alloc failed for spill partition buffer");
        for (int c = 0; c < n_cols; c++)
            bld[c] = vec_builder_init(ps->schema.col_types[c]);
        ps->bld[p] = bld;
    }
    int64_t before = vec_builders_bytes(bld, n_cols);
    for (int c = 0; c < n_cols; c++) {
        VecArray g = vec_array_gather(&batch->columns[ps->col_map[c]], rows, m);
        vec_builder_append_array(&bld[c], &g);
        vec_array_free(&g);
    }
    ps->held += vec_builders_bytes(bld, n_cols) - before;
    if (bld[0].length >= PART_SPILL_RG_ROWS) flush_one(ps, p);
}

void part_spill_route(PartSpill *ps, const VecBatch *batch, const int *pid) {
    int64_t n = vec_batch_logical_rows(batch);
    if (n == 0) return;
    int K = ps->K;

    /* Counting sort of the routed rows by partition, input order kept within
       a partition, then one gather per partition and column. */
    int64_t *start = (int64_t *)calloc((size_t)K + 1, sizeof(int64_t));
    int32_t *rows = (int32_t *)malloc((size_t)n * sizeof(int32_t));
    if (!start || !rows) {
        free(start); free(rows);
        vectra_error("alloc failed for partition routing");
    }
    for (int64_t li = 0; li < n; li++) {
        int p = pid ? pid[li] : 0;
        if (p >= 0) start[p + 1]++;
    }
    for (int p = 0; p < K; p++) start[p + 1] += start[p];
    int64_t *fill = (int64_t *)malloc((size_t)K * sizeof(int64_t));
    if (!fill) { free(start); free(rows); vectra_error("alloc failed for partition routing"); }
    memcpy(fill, start, (size_t)K * sizeof(int64_t));
    for (int64_t li = 0; li < n; li++) {
        int p = pid ? pid[li] : 0;
        if (p >= 0)
            rows[fill[p]++] = (int32_t)vec_batch_physical_row(batch, li);
    }
    free(fill);

    for (int p = 0; p < K; p++) {
        int64_t m = start[p + 1] - start[p];
        if (m > 0) append_rows(ps, p, batch, rows + start[p], (int32_t)m);
    }
    free(start);
    free(rows);

    if (ps->held > ps->buf_budget)
        for (int p = 0; p < K; p++) flush_one(ps, p);
}

void part_spill_close(PartSpill *ps) {
    if (!ps) return;
    int n_cols = ps->schema.n_cols;
    for (int p = 0; p < ps->K; p++) {
        if (ps->bld[p]) {
            flush_one(ps, p);
            for (int c = 0; c < n_cols; c++) vec_builder_free(&ps->bld[p][c]);
            free(ps->bld[p]);
        }
        if (ps->writers[p]) vtr1_close_tdc_writer(ps->writers[p]);
    }
    free(ps->bld);
    free(ps->writers);
    free(ps->col_map);
    vec_schema_free(&ps->schema);
    free(ps);
}
