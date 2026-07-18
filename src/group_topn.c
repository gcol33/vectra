#include "group_topn.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "sort.h"
#include "rowid.h"
#include "key_snap.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/*
 * Streaming grouped top-1 (argmin / argmax), bounded memory.
 *
 * The window-based grouped slice path materializes every input column to rank
 * rows within each group, then drops all but one row per group -- it buffers
 * the whole input, including a large geometry string column. A hash-based
 * champion store fixes the input side but still holds one champion per group
 * resident, so at high group cardinality (near one group per row) it grows with
 * the input.
 *
 * This operator routes the child through the bounded external sort, keyed by
 * the group columns plus an appended row-id. Groups then arrive contiguously
 * and, within a group, in input order (the row-id is a stable tiebreak). A
 * single running champion is kept for the open group; when a group closes its
 * champion is committed to a bounded store, and once the store fills it is
 * emitted as one batch. Peak memory is O(emit batch) -- one batch of winners
 * plus one champion plus the sort's own bounded spill -- independent of both
 * the input length and the number of groups.
 *
 * NA order values sort last, so a known value always wins; a group stays NA
 * only when every row in it is NA. Ties keep the first row in input order.
 */

/* Number of winner rows emitted per next_batch() call. */
#define GROUP_TOPN_EMIT 131072

/* Champion storage for one output column, indexed by store slot. */
typedef struct {
    VecType        type;
    int            elem;      /* fixed-width element size; 0 for strings */
    uint8_t       *valid;     /* one byte per slot: 1 = present, 0 = NA */
    unsigned char *fw;        /* fixed-width values, cap * elem bytes */
    char         **strs;      /* per-slot string data (NULL = NA) */
    int64_t       *slen;      /* per-slot string length */
    int64_t        cap;       /* capacity in slots */
} ChampCol;

static void champ_grow(ChampCol *col, int64_t need) {
    if (need <= col->cap) return;
    int64_t nc = col->cap == 0 ? 64 : col->cap;
    while (nc < need) nc *= 2;

    col->valid = (uint8_t *)realloc(col->valid, (size_t)nc);
    if (!col->valid) vectra_error("group_topn: realloc failed (champ valid)");

    if (col->elem > 0) {
        col->fw = (unsigned char *)realloc(col->fw, (size_t)nc * col->elem);
        if (!col->fw) vectra_error("group_topn: realloc failed (champ data)");
    } else {
        col->strs = (char **)realloc(col->strs, (size_t)nc * sizeof(char *));
        col->slen = (int64_t *)realloc(col->slen, (size_t)nc * sizeof(int64_t));
        if (!col->strs || !col->slen)
            vectra_error("group_topn: realloc failed (champ strings)");
        for (int64_t i = col->cap; i < nc; i++) { col->strs[i] = NULL; col->slen[i] = 0; }
    }
    col->cap = nc;
}

/* Copy row `r` of `src` into champion slot `g` (overwrites any prior value). */
static void champ_set(ChampCol *col, int64_t g, const VecArray *src, int64_t r) {
    int valid = vec_array_is_valid(src, r);
    col->valid[g] = (uint8_t)(valid ? 1 : 0);
    if (col->elem > 0) {
        if (valid) {
            const unsigned char *base = (const unsigned char *)src->buf.i64;
            memcpy(col->fw + g * col->elem, base + r * (int64_t)col->elem,
                   (size_t)col->elem);
        }
    } else {
        free(col->strs[g]);
        col->strs[g] = NULL;
        col->slen[g] = 0;
        if (valid) {
            int64_t s = src->buf.str.offsets[r];
            int64_t len = src->buf.str.offsets[r + 1] - s;
            col->strs[g] = (char *)malloc((size_t)(len > 0 ? len : 1));
            if (!col->strs[g]) vectra_error("group_topn: malloc failed (champ string)");
            if (len > 0) memcpy(col->strs[g], src->buf.str.data + s, (size_t)len);
            col->slen[g] = len;
        }
    }
}

/* Is candidate row `r` of `cand` a strictly better champion for slot `g`?
   NA candidates never win; a real value always beats an NA champion; ties keep
   the incumbent. `desc` selects max (1) over min (0). */
static int champ_better(const ChampCol *oc, int64_t g,
                        const VecArray *cand, int64_t r, int desc) {
    if (!vec_array_is_valid(cand, r)) return 0;
    if (!oc->valid[g]) return 1;

    int c = 0;
    switch (cand->type) {
    case VEC_INT64: {
        int64_t a = cand->buf.i64[r], b = *(const int64_t *)(oc->fw + g * 8);
        c = (a < b) ? -1 : (a > b) ? 1 : 0;
        break;
    }
    case VEC_INT32: {
        int32_t a = cand->buf.i32[r], b = *(const int32_t *)(oc->fw + g * 4);
        c = (a < b) ? -1 : (a > b) ? 1 : 0;
        break;
    }
    case VEC_INT16: {
        int16_t a = cand->buf.i16[r], b = *(const int16_t *)(oc->fw + g * 2);
        c = (a < b) ? -1 : (a > b) ? 1 : 0;
        break;
    }
    case VEC_INT8: {
        int8_t a = cand->buf.i8[r], b = *(const int8_t *)(oc->fw + g);
        c = (a < b) ? -1 : (a > b) ? 1 : 0;
        break;
    }
    case VEC_DOUBLE: {
        double a = cand->buf.dbl[r], b = *(const double *)(oc->fw + g * 8);
        c = (a < b) ? -1 : (a > b) ? 1 : 0;
        break;
    }
    case VEC_BOOL: {
        uint8_t a = cand->buf.bln[r], b = *(const uint8_t *)(oc->fw + g);
        c = (int)a - (int)b;
        break;
    }
    case VEC_STRING: {
        int64_t s = cand->buf.str.offsets[r];
        int64_t la = cand->buf.str.offsets[r + 1] - s;
        int64_t lb = oc->slen[g];
        int64_t m = la < lb ? la : lb;
        c = (m > 0) ? memcmp(cand->buf.str.data + s, oc->strs[g], (size_t)m) : 0;
        if (c == 0) c = (la < lb) ? -1 : (la > lb) ? 1 : 0;
        break;
    }
    }
    return desc ? (c > 0) : (c < 0);
}

/* Materialize champion slots [lo, hi) of column `col` into a fresh VecArray. */
static VecArray champ_finish_range(const ChampCol *col, int64_t lo, int64_t hi) {
    int64_t m = hi - lo;
    if (col->elem > 0) {
        VecArray a = vec_array_alloc(col->type, m);
        unsigned char *base = (unsigned char *)a.buf.i64;
        for (int64_t i = 0; i < m; i++) {
            int64_t g = lo + i;
            if (col->valid[g]) {
                vec_array_set_valid(&a, i);
                memcpy(base + i * (int64_t)col->elem,
                       col->fw + g * (int64_t)col->elem, (size_t)col->elem);
            }
        }
        return a;
    }

    int64_t total = 0;
    for (int64_t g = lo; g < hi; g++)
        if (col->valid[g]) total += col->slen[g];

    VecArray a;
    memset(&a, 0, sizeof(a));
    a.type = VEC_STRING;
    a.length = m;
    a.owns_data = 1;
    int64_t vbytes = vec_validity_bytes(m);
    a.validity = (uint8_t *)calloc((size_t)(vbytes > 0 ? vbytes : 1), 1);
    a.buf.str.offsets = (int64_t *)malloc((size_t)(m + 1) * sizeof(int64_t));
    a.buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
    if (!a.validity || !a.buf.str.offsets || !a.buf.str.data)
        vectra_error("group_topn: alloc failed (string output)");
    a.buf.str.data_len = total;

    int64_t off = 0;
    for (int64_t i = 0; i < m; i++) {
        int64_t g = lo + i;
        a.buf.str.offsets[i] = off;
        if (col->valid[g]) {
            vec_array_set_valid(&a, i);
            if (col->slen[g] > 0)
                memcpy(a.buf.str.data + off, col->strs[g], (size_t)col->slen[g]);
            off += col->slen[g];
        }
    }
    a.buf.str.offsets[m] = off;
    return a;
}

/* Release champion storage. */
static void champ_free(ChampCol *champ, int n_cols) {
    if (!champ) return;
    for (int c = 0; c < n_cols; c++) {
        free(champ[c].valid);
        if (champ[c].elem > 0) {
            free(champ[c].fw);
        } else {
            for (int64_t g = 0; g < champ[c].cap; g++) free(champ[c].strs[g]);
            free(champ[c].strs);
            free(champ[c].slen);
        }
    }
    free(champ);
}

/* Overwrite champion slot `g` with every output column of row `row`. */
static void champ_set_all(ChampCol *champ, int64_t g, const VecBatch *b,
                          int64_t row, int n_cols) {
    for (int c = 0; c < n_cols; c++) {
        champ_grow(&champ[c], g + 1);
        champ_set(&champ[c], g, &b->columns[c], row);
    }
}

/* ------------------------------------------------------------------ */
/*  Node body                                                          */
/* ------------------------------------------------------------------ */

static void group_topn_init(GroupTopNNode *gn) {
    const VecSchema *os = &gn->base.output_schema;  /* original child schema */
    int nc = gn->n_cols;
    ChampCol *champ = (ChampCol *)calloc((size_t)nc, sizeof(ChampCol));
    if (!champ) vectra_error("group_topn: alloc failed (champ store)");
    for (int c = 0; c < nc; c++) {
        champ[c].type = os->col_types[c];
        champ[c].elem = vec_type_elem_size(os->col_types[c]);
    }
    gn->champ = champ;

    VecType *ktypes = (VecType *)malloc(
        (size_t)(gn->n_keys > 0 ? gn->n_keys : 1) * sizeof(VecType));
    for (int k = 0; k < gn->n_keys; k++)
        ktypes[k] = os->col_types[gn->key_idx[k]];
    gn->snap = snap_create(gn->n_keys, ktypes);
    free(ktypes);

    gn->initialized = 1;
}

/* Build a result batch from champion slots [0, count). */
static VecBatch *group_topn_build_result(GroupTopNNode *gn, int64_t count) {
    ChampCol *champ = (ChampCol *)gn->champ;
    const VecSchema *os = &gn->base.output_schema;
    VecBatch *result = vec_batch_alloc(gn->n_cols, count);
    for (int c = 0; c < gn->n_cols; c++) {
        result->columns[c] = champ_finish_range(&champ[c], 0, count);
        const char *nm = os->col_names[c];
        result->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(result->col_names[c], nm);
    }
    return result;
}

static VecBatch *group_topn_next_batch(VecNode *self) {
    GroupTopNNode *gn = (GroupTopNNode *)self;
    if (gn->done && gn->fill == 0 && !gn->has_group) return NULL;
    if (!gn->initialized) group_topn_init(gn);

    ChampCol *champ = (ChampCol *)gn->champ;
    const int nc = gn->n_cols;

    for (;;) {
        if (gn->cur_batch == NULL || gn->cur_row >= gn->cur_batch->n_rows) {
            if (gn->cur_batch) { vec_batch_free(gn->cur_batch); gn->cur_batch = NULL; }
            if (!gn->input_done) {
                gn->cur_batch = gn->child->next_batch(gn->child);
                gn->cur_row = 0;
                if (gn->cur_batch == NULL) gn->input_done = 1;
            }
            if (gn->cur_batch == NULL) break;    /* input exhausted */
        }

        VecBatch *b = gn->cur_batch;
        int64_t n = b->n_rows;
        for (; gn->cur_row < n; gn->cur_row++) {
            int64_t row = gn->cur_row;
            if (!snap_matches(&gn->snap, b, row, gn->key_idx)) {
                /* Group boundary: commit the open group, maybe emit a batch. */
                if (gn->has_group) {
                    gn->fill++;
                    if (gn->fill >= GROUP_TOPN_EMIT) {
                        VecBatch *out = group_topn_build_result(gn, gn->fill);
                        gn->fill = 0;
                        snap_update(&gn->snap, b, row, gn->key_idx);
                        champ_set_all(champ, 0, b, row, nc);
                        gn->has_group = 1;
                        gn->cur_row++;    /* this row is now consumed */
                        return out;
                    }
                }
                snap_update(&gn->snap, b, row, gn->key_idx);
                champ_set_all(champ, gn->fill, b, row, nc);
                gn->has_group = 1;
            } else if (champ_better(&champ[gn->order_idx], gn->fill,
                                    &b->columns[gn->order_idx], row,
                                    gn->descending)) {
                champ_set_all(champ, gn->fill, b, row, nc);
            }
        }
        /* batch fully scanned; loop pulls the next one */
    }

    /* Input exhausted: commit the last open group and emit the tail. */
    if (gn->has_group) { gn->fill++; gn->has_group = 0; }
    gn->done = 1;
    if (gn->fill == 0) return NULL;

    VecBatch *out = group_topn_build_result(gn, gn->fill);
    gn->fill = 0;
    return out;
}

static void group_topn_free(VecNode *self) {
    GroupTopNNode *gn = (GroupTopNNode *)self;
    if (gn->champ) champ_free((ChampCol *)gn->champ, gn->n_cols);
    if (gn->initialized) snap_free(&gn->snap);
    if (gn->cur_batch) vec_batch_free(gn->cur_batch);
    gn->child->free_node(gn->child);
    free(gn->key_idx);
    vec_schema_free(&gn->base.output_schema);
    free(gn);
}

GroupTopNNode *group_topn_node_create(VecNode *child, int n_keys,
                                      const int *key_idx, int order_idx,
                                      int descending,
                                      int64_t mem_budget, const char *temp_dir) {
    GroupTopNNode *gn = (GroupTopNNode *)calloc(1, sizeof(GroupTopNNode));
    if (!gn) vectra_error("alloc failed for GroupTopNNode");
    gn->n_keys = n_keys;
    gn->key_idx = (int *)malloc((size_t)(n_keys > 0 ? n_keys : 1) * sizeof(int));
    for (int k = 0; k < n_keys; k++) gn->key_idx[k] = key_idx[k];
    gn->order_idx = order_idx;
    gn->descending = descending;

    /* Output schema is the original child schema; the row-id is internal. */
    gn->base.output_schema = vec_schema_copy(&child->output_schema);
    gn->n_cols = gn->base.output_schema.n_cols;

    /* Append a row-id, then sort by (keys, row-id): groups become contiguous
       and rows within a group keep input order (stable tiebreak on ties). */
    RowIdNode *rid = rowid_node_create(child, "__vectra_topn_rowid");
    int rowid_idx = rid->base.output_schema.n_cols - 1;
    int nsk = n_keys + 1;
    SortKey *sk = (SortKey *)malloc((size_t)nsk * sizeof(SortKey));
    for (int k = 0; k < n_keys; k++) {
        sk[k].col_index = key_idx[k];
        sk[k].descending = 0;
        sk[k].na_last = 0;   /* cluster NA group keys consistently */
    }
    sk[n_keys].col_index = rowid_idx;
    sk[n_keys].descending = 0;
    sk[n_keys].na_last = 0;  /* row-id is never NA */
    int64_t sort_mem = mem_budget > 0 ? mem_budget : VECTRA_SORT_MEM_DEFAULT;
    /* sort_node_create takes ownership of sk (freed in sort_node_free). */
    SortNode *sn = sort_node_create((VecNode *)rid, nsk, sk, temp_dir, sort_mem);
    gn->child = (VecNode *)sn;

    gn->base.next_batch = group_topn_next_batch;
    gn->base.free_node = group_topn_free;
    gn->base.kind = "GroupTopNNode";
    gn->base.row_count_hint = -1;

    return gn;
}
