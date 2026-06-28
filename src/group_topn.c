#include "group_topn.h"
#include "array.h"
#include "batch.h"
#include "builder.h"
#include "hash.h"
#include "schema.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/*
 * Streaming grouped top-1 (argmin / argmax).
 *
 * The window-based grouped slice path materializes every input column to
 * rank rows within each group, then drops all but one row per group. That
 * buffers the whole input — including a large geometry string column — which
 * defeats the larger-than-RAM design and exhausts memory on dense overlay
 * outputs.
 *
 * This operator instead keeps only the running champion row per group. Each
 * incoming batch row either opens a new group (champion = that row) or beats
 * the current champion on the order column (champion replaced). Peak memory
 * is O(#groups) — the size of the result — independent of the input length.
 *
 * Group identity uses the shared open-addressing hash table. The distinct key
 * values seen so far live in append-only key builders; each lookup aliases
 * them with a non-owning view (no per-group rebuild, so grouping stays
 * O(#rows) rather than O(#rows * #groups)).
 */

/* Champion storage for one output column, indexed by group id. */
typedef struct {
    VecType        type;
    int            elem;      /* fixed-width element size; 0 for strings */
    uint8_t       *valid;     /* one byte per group: 1 = present, 0 = NA */
    unsigned char *fw;        /* fixed-width values, cap * elem bytes */
    char         **strs;      /* per-group string data (NULL = NA) */
    int64_t       *slen;      /* per-group string length */
    int64_t        cap;       /* capacity in groups */
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

/* Is candidate row `r` of `cand` a strictly better champion for group `g`?
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

/* Materialize champion rows [lo, hi) of column `col` into a fresh VecArray, so
   the winners can be emitted in bounded batches rather than all at once. */
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

/* Number of winner rows emitted per next_batch() call. */
#define GROUP_TOPN_EMIT 131072

/* One streaming pass over the child: assemble the per-group champions and store
   them on the node. Working storage (key arena, hash table) is released here;
   only the champions survive, to be emitted in bounded batches. */
static void group_topn_build(GroupTopNNode *gn) {
    const VecSchema *cschema = &gn->child->output_schema;
    int n_cols = cschema->n_cols;
    int n_keys = gn->n_keys;

    ChampCol *champ = (ChampCol *)calloc((size_t)n_cols, sizeof(ChampCol));
    for (int c = 0; c < n_cols; c++) {
        champ[c].type = cschema->col_types[c];
        champ[c].elem = vec_type_elem_size(cschema->col_types[c]);
    }

    VecArrayBuilder *key_arena = (VecArrayBuilder *)calloc(
        (size_t)(n_keys > 0 ? n_keys : 1), sizeof(VecArrayBuilder));
    for (int k = 0; k < n_keys; k++)
        key_arena[k] = vec_builder_init(cschema->col_types[gn->key_idx[k]]);

    VecArray *arena_view = (VecArray *)malloc(
        (size_t)(n_keys > 0 ? n_keys : 1) * sizeof(VecArray));
    VecArray *key_cols = (VecArray *)malloc(
        (size_t)(n_keys > 0 ? n_keys : 1) * sizeof(VecArray));

    VecHashTable ht = vec_ht_create(1024);
    int64_t n_groups = 0;

    VecBatch *batch;
    while ((batch = gn->child->next_batch(gn->child)) != NULL) {
        int64_t n_logical = vec_batch_logical_rows(batch);
        for (int k = 0; k < n_keys; k++)
            key_cols[k] = batch->columns[gn->key_idx[k]];

        for (int64_t li = 0; li < n_logical; li++) {
            int64_t pi = vec_batch_physical_row(batch, li);

            uint64_t h = 0;
            for (int k = 0; k < n_keys; k++) {
                uint64_t kh = vec_hash_value(&key_cols[k], pi);
                h = (k == 0) ? kh : vec_hash_combine(h, kh);
            }
            for (int k = 0; k < n_keys; k++)
                arena_view[k] = vec_builder_view(&key_arena[k]);

            int was_new = 0;
            int64_t gid = vec_ht_find_or_insert(&ht, h, key_cols, n_keys, pi,
                                                arena_view, n_groups, &was_new);

            if (was_new) {
                for (int k = 0; k < n_keys; k++)
                    vec_builder_append_one(&key_arena[k],
                                           &batch->columns[gn->key_idx[k]], pi);
                for (int c = 0; c < n_cols; c++) {
                    champ_grow(&champ[c], gid + 1);
                    champ_set(&champ[c], gid, &batch->columns[c], pi);
                }
                n_groups = gid + 1;
            } else if (champ_better(&champ[gn->order_idx], gid,
                                    &batch->columns[gn->order_idx], pi,
                                    gn->descending)) {
                for (int c = 0; c < n_cols; c++)
                    champ_set(&champ[c], gid, &batch->columns[c], pi);
            }
        }
        vec_batch_free(batch);
    }

    for (int k = 0; k < n_keys; k++) vec_builder_free(&key_arena[k]);
    free(key_arena);
    free(arena_view);
    free(key_cols);
    vec_ht_free(&ht);

    gn->champ = champ;
    gn->n_cols = n_cols;
    gn->n_groups = n_groups;
    gn->emit_pos = 0;
    gn->built = 1;
}

static VecBatch *group_topn_next_batch(VecNode *self) {
    GroupTopNNode *gn = (GroupTopNNode *)self;
    if (!gn->built) group_topn_build(gn);

    ChampCol *champ = (ChampCol *)gn->champ;
    if (gn->emit_pos >= gn->n_groups) {
        champ_free(champ, gn->n_cols);
        gn->champ = NULL;
        return NULL;
    }

    int n_cols = gn->n_cols;
    int64_t lo = gn->emit_pos;
    int64_t hi = lo + GROUP_TOPN_EMIT;
    if (hi > gn->n_groups) hi = gn->n_groups;

    const VecSchema *cschema = &gn->child->output_schema;
    VecBatch *result = vec_batch_alloc(n_cols, hi - lo);
    for (int c = 0; c < n_cols; c++) {
        result->columns[c] = champ_finish_range(&champ[c], lo, hi);
        const char *nm = cschema->col_names[c];
        result->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(result->col_names[c], nm);
    }
    gn->emit_pos = hi;
    return result;
}

static void group_topn_free(VecNode *self) {
    GroupTopNNode *gn = (GroupTopNNode *)self;
    champ_free((ChampCol *)gn->champ, gn->n_cols);   /* freed already if drained */
    gn->child->free_node(gn->child);
    free(gn->key_idx);
    vec_schema_free(&gn->base.output_schema);
    free(gn);
}

GroupTopNNode *group_topn_node_create(VecNode *child, int n_keys,
                                      const int *key_idx, int order_idx,
                                      int descending) {
    GroupTopNNode *gn = (GroupTopNNode *)calloc(1, sizeof(GroupTopNNode));
    if (!gn) vectra_error("alloc failed for GroupTopNNode");
    gn->child = child;
    gn->n_keys = n_keys;
    gn->key_idx = (int *)malloc((size_t)(n_keys > 0 ? n_keys : 1) * sizeof(int));
    for (int k = 0; k < n_keys; k++) gn->key_idx[k] = key_idx[k];
    gn->order_idx = order_idx;
    gn->descending = descending;
    /* built/champ/n_cols/n_groups/emit_pos zeroed by calloc */

    gn->base.output_schema = vec_schema_copy(&child->output_schema);
    gn->base.next_batch = group_topn_next_batch;
    gn->base.free_node = group_topn_free;
    gn->base.kind = "GroupTopNNode";
    gn->base.row_count_hint = -1;

    return gn;
}
