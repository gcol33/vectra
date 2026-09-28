#include "part_spill.h"
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
    int               K;
    char *const      *paths;
    Vtr1TdcWriter   **writers;   /* opened on first flush / touch */
    VecArrayBuilder **bld;       /* per-partition buffers, NULL until used */
    int64_t           buf_budget;
};

PartSpill *part_spill_create(const VecSchema *schema, int K,
                             char *const *paths, int64_t buf_budget) {
    PartSpill *ps = (PartSpill *)calloc(1, sizeof(PartSpill));
    if (!ps) vectra_error("alloc failed for partition spill");
    ps->schema = vec_schema_copy(schema);
    ps->K = K;
    ps->paths = paths;
    ps->writers = (Vtr1TdcWriter **)calloc((size_t)K, sizeof(Vtr1TdcWriter *));
    ps->bld = (VecArrayBuilder **)calloc((size_t)K, sizeof(VecArrayBuilder *));
    if (!ps->writers || !ps->bld) vectra_error("alloc failed for partition spill");
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

/* Flush every partition once the buffers together pass buf_budget. */
static void bound_buffers(PartSpill *ps) {
    int n_cols = ps->schema.n_cols;
    int64_t held = 0;
    for (int p = 0; p < ps->K; p++)
        if (ps->bld[p]) held += vec_builders_bytes(ps->bld[p], n_cols);
    if (held > ps->buf_budget)
        for (int p = 0; p < ps->K; p++) flush_one(ps, p);
}

void part_spill_route(PartSpill *ps, const VecBatch *batch, const int *pid) {
    int n_cols = ps->schema.n_cols;
    int64_t n = vec_batch_logical_rows(batch);
    for (int64_t li = 0; li < n; li++) {
        int p = pid ? pid[li] : 0;
        VecArrayBuilder *bld = ps->bld[p];
        if (!bld) {
            bld = (VecArrayBuilder *)calloc((size_t)n_cols,
                                            sizeof(VecArrayBuilder));
            if (!bld) vectra_error("alloc failed for spill partition buffer");
            for (int c = 0; c < n_cols; c++)
                bld[c] = vec_builder_init(ps->schema.col_types[c]);
            ps->bld[p] = bld;
        }
        int64_t pr = vec_batch_physical_row(batch, li);
        for (int c = 0; c < n_cols; c++)
            vec_builder_append_one(&bld[c], &batch->columns[c], pr);
        if (bld[0].length >= PART_SPILL_RG_ROWS) flush_one(ps, p);
        if ((li & 4095) == 4095 || li == n - 1) bound_buffers(ps);
    }
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
    vec_schema_free(&ps->schema);
    free(ps);
}
