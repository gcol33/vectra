#include "topn.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/*
 * Top-N via a bounded keep-store + binary heap.
 *
 * For ascending sort (smallest N): a MAX-heap of size k over the kept rows,
 * so the root is the LARGEST of the kept set. An incoming row is admitted
 * only if it compares smaller than the root, in which case it overwrites the
 * root's slot and the heap re-sifts. Non-admitted rows are never copied.
 *
 * Peak memory is O(min(k, n_rows)): the keep-store holds at most k rows
 * (fixed-width values overwritten in place; strings held as per-slot owned
 * copies) and the heap holds k slot indices. This is the streaming form the
 * old materialize-all-rows implementation deferred; strings are handled by
 * giving each kept slot its own small copy, freed on eviction.
 *
 * Time is O(n log k): each row costs one root comparison, plus a sift only
 * when it is admitted.
 */

/* Per-column storage for the kept rows. Fixed-width types use `data`
   (cap * elem_size); VEC_STRING uses per-slot owned copies (sptr/slen). */
typedef struct {
    VecType   type;
    uint8_t  *valid;   /* cap bytes, 1 = valid */
    void     *data;    /* fixed-width: cap * elem_size; unused for strings */
    char    **sptr;    /* strings: cap owned copies (NULL slot = empty) */
    int64_t  *slen;    /* strings: cap lengths */
} StoreCol;

typedef struct {
    int       n_cols;
    int64_t   cap;     /* current allocated slots (<= limit) */
    int64_t   limit;   /* k: hard cap on slots */
    StoreCol *cols;
} RowStore;

typedef struct {
    RowStore *store;
    int       n_keys;
    SortKey  *keys;
} StoreCtx;

/* A single comparable value pulled from either a live batch column or a
   store slot, so both comparison directions share one per-type path. */
typedef struct {
    int valid;
    union {
        int64_t i;                              /* int8/16/32/64, bool */
        double  d;                              /* double */
        struct { const char *p; int64_t n; } s; /* string */
    } u;
} TCell;

static size_t elem_size(VecType t) {
    switch (t) {
    case VEC_INT64:  return sizeof(int64_t);
    case VEC_INT32:  return sizeof(int32_t);
    case VEC_INT16:  return sizeof(int16_t);
    case VEC_INT8:   return sizeof(int8_t);
    case VEC_DOUBLE: return sizeof(double);
    case VEC_BOOL:   return sizeof(uint8_t);
    case VEC_STRING: return 0;
    }
    return 0;
}

static TCell cell_from_array(const VecArray *col, int64_t phys) {
    TCell c;
    c.valid = vec_array_is_valid(col, phys);
    if (!c.valid) return c;
    switch (col->type) {
    case VEC_DOUBLE: c.u.d = col->buf.dbl[phys]; break;
    case VEC_INT64:  c.u.i = col->buf.i64[phys]; break;
    case VEC_INT32:  c.u.i = col->buf.i32[phys]; break;
    case VEC_INT16:  c.u.i = col->buf.i16[phys]; break;
    case VEC_INT8:   c.u.i = col->buf.i8[phys];  break;
    case VEC_BOOL:   c.u.i = col->buf.bln[phys]; break;
    case VEC_STRING: {
        int64_t s = col->buf.str.offsets[phys];
        c.u.s.p = col->buf.str.data + s;
        c.u.s.n = col->buf.str.offsets[phys + 1] - s;
        break;
    }
    }
    return c;
}

static TCell cell_from_store(const StoreCol *sc, int64_t slot) {
    TCell c;
    c.valid = sc->valid[slot];
    if (!c.valid) return c;
    switch (sc->type) {
    case VEC_DOUBLE: c.u.d = ((double  *)sc->data)[slot]; break;
    case VEC_INT64:  c.u.i = ((int64_t *)sc->data)[slot]; break;
    case VEC_INT32:  c.u.i = ((int32_t *)sc->data)[slot]; break;
    case VEC_INT16:  c.u.i = ((int16_t *)sc->data)[slot]; break;
    case VEC_INT8:   c.u.i = ((int8_t  *)sc->data)[slot]; break;
    case VEC_BOOL:   c.u.i = ((uint8_t *)sc->data)[slot]; break;
    case VEC_STRING: c.u.s.p = sc->sptr[slot]; c.u.s.n = sc->slen[slot]; break;
    }
    return c;
}

/* Compare two valid cells of the same type. Returns <0 / 0 / >0. */
static int cmp_valid(VecType type, TCell a, TCell b) {
    switch (type) {
    case VEC_DOUBLE:
        return (a.u.d < b.u.d) ? -1 : (a.u.d > b.u.d) ? 1 : 0;
    case VEC_STRING: {
        int64_t minlen = a.u.s.n < b.u.s.n ? a.u.s.n : b.u.s.n;
        int cmp = memcmp(a.u.s.p, b.u.s.p, (size_t)minlen);
        if (cmp == 0)
            cmp = (a.u.s.n < b.u.s.n) ? -1 : (a.u.s.n > b.u.s.n) ? 1 : 0;
        return cmp;
    }
    default: /* all integer widths + bool live in u.i */
        return (a.u.i < b.u.i) ? -1 : (a.u.i > b.u.i) ? 1 : 0;
    }
}

/* One-key comparison in final (top-N) order. NA (invalid) always sorts last,
   independent of `desc`, matching dplyr slice_min/slice_max and the with_ties
   R path (order(..., na.last = TRUE)). Returns 0 for a tie on this key so the
   caller falls through to the next key. */
static int key_cmp(VecType type, TCell a, TCell b, int desc) {
    if (!a.valid && !b.valid) return 0;
    if (!a.valid) return 1;   /* a last */
    if (!b.valid) return -1;  /* b last */
    int cmp = cmp_valid(type, a, b);
    return cmp ? (desc ? -cmp : cmp) : 0;
}

/* Compare two store slots across all sort keys. */
static int cmp_slot_slot(const StoreCtx *ctx, int64_t a, int64_t b) {
    for (int k = 0; k < ctx->n_keys; k++) {
        int ci = ctx->keys[k].col_index;
        StoreCol *sc = &ctx->store->cols[ci];
        int cmp = key_cmp(sc->type, cell_from_store(sc, a),
                          cell_from_store(sc, b), ctx->keys[k].descending);
        if (cmp != 0) return cmp;
    }
    return 0;
}

/* Compare an incoming batch row against a store slot across all sort keys. */
static int cmp_incoming_slot(const StoreCtx *ctx, const VecBatch *batch,
                             int64_t phys, int64_t slot) {
    for (int k = 0; k < ctx->n_keys; k++) {
        int ci = ctx->keys[k].col_index;
        StoreCol *sc = &ctx->store->cols[ci];
        int cmp = key_cmp(sc->type, cell_from_array(&batch->columns[ci], phys),
                          cell_from_store(sc, slot), ctx->keys[k].descending);
        if (cmp != 0) return cmp;
    }
    return 0;
}

/* Grow every column's slot capacity to at least `need` (doubling, capped at
   limit). Only ever called during the fill phase, so need <= limit. */
static void store_ensure(RowStore *st, int64_t need) {
    if (need <= st->cap) return;
    int64_t ncap = st->cap ? st->cap : 16;
    while (ncap < need) ncap *= 2;
    if (ncap > st->limit) ncap = st->limit;

    for (int c = 0; c < st->n_cols; c++) {
        StoreCol *sc = &st->cols[c];
        sc->valid = (uint8_t *)realloc(sc->valid, (size_t)ncap);
        memset(sc->valid + st->cap, 0, (size_t)(ncap - st->cap));
        if (sc->type == VEC_STRING) {
            sc->sptr = (char **)realloc(sc->sptr, (size_t)ncap * sizeof(char *));
            sc->slen = (int64_t *)realloc(sc->slen, (size_t)ncap * sizeof(int64_t));
            memset(sc->sptr + st->cap, 0, (size_t)(ncap - st->cap) * sizeof(char *));
            memset(sc->slen + st->cap, 0, (size_t)(ncap - st->cap) * sizeof(int64_t));
        } else {
            size_t es = elem_size(sc->type);
            sc->data = realloc(sc->data, (size_t)ncap * es);
        }
    }
    st->cap = ncap;
}

/* Copy row `phys` of `batch` into slot `slot` (overwriting whatever was
   there, freeing an evicted string first). */
static void store_put(RowStore *st, int64_t slot,
                      const VecBatch *batch, int64_t phys) {
    for (int c = 0; c < st->n_cols; c++) {
        StoreCol *sc = &st->cols[c];
        const VecArray *col = &batch->columns[c];
        if (sc->type == VEC_STRING) {
            free(sc->sptr[slot]);
            sc->sptr[slot] = NULL;
            sc->slen[slot] = 0;
        }
        if (!vec_array_is_valid(col, phys)) { sc->valid[slot] = 0; continue; }
        sc->valid[slot] = 1;
        switch (sc->type) {
        case VEC_DOUBLE: ((double  *)sc->data)[slot] = col->buf.dbl[phys]; break;
        case VEC_INT64:  ((int64_t *)sc->data)[slot] = col->buf.i64[phys]; break;
        case VEC_INT32:  ((int32_t *)sc->data)[slot] = col->buf.i32[phys]; break;
        case VEC_INT16:  ((int16_t *)sc->data)[slot] = col->buf.i16[phys]; break;
        case VEC_INT8:   ((int8_t  *)sc->data)[slot] = col->buf.i8[phys];  break;
        case VEC_BOOL:   ((uint8_t *)sc->data)[slot] = col->buf.bln[phys]; break;
        case VEC_STRING: {
            int64_t s = col->buf.str.offsets[phys];
            int64_t n = col->buf.str.offsets[phys + 1] - s;
            char *p = (char *)malloc((size_t)(n > 0 ? n : 1));
            if (n > 0) memcpy(p, col->buf.str.data + s, (size_t)n);
            sc->sptr[slot] = p;
            sc->slen[slot] = n;
            break;
        }
        }
    }
}

/* Binary max-heap over slot indices (heap[i] is a slot into the store). */
static void heap_sift_down(int64_t *heap, int64_t size, int64_t pos,
                           const StoreCtx *ctx) {
    while (1) {
        int64_t largest = pos;
        int64_t left = 2 * pos + 1;
        int64_t right = 2 * pos + 2;
        if (left < size && cmp_slot_slot(ctx, heap[left], heap[largest]) > 0)
            largest = left;
        if (right < size && cmp_slot_slot(ctx, heap[right], heap[largest]) > 0)
            largest = right;
        if (largest == pos) break;
        int64_t tmp = heap[pos]; heap[pos] = heap[largest]; heap[largest] = tmp;
        pos = largest;
    }
}

static void heap_sift_up(int64_t *heap, int64_t pos, const StoreCtx *ctx) {
    while (pos > 0) {
        int64_t parent = (pos - 1) / 2;
        if (cmp_slot_slot(ctx, heap[pos], heap[parent]) <= 0) break;
        int64_t tmp = heap[pos]; heap[pos] = heap[parent]; heap[parent] = tmp;
        pos = parent;
    }
}

/* Final ordering of the selected slots (ascending in sort order). */
static void topn_merge_sort(int64_t *idx, int64_t *tmp, int64_t n,
                            const StoreCtx *ctx) {
    if (n <= 1) return;
    int64_t mid = n / 2;
    topn_merge_sort(idx, tmp, mid, ctx);
    topn_merge_sort(idx + mid, tmp, n - mid, ctx);
    int64_t i = 0, j = mid, k = 0;
    while (i < mid && j < n) {
        if (cmp_slot_slot(ctx, idx[i], idx[j]) <= 0) tmp[k++] = idx[i++];
        else                                          tmp[k++] = idx[j++];
    }
    while (i < mid) tmp[k++] = idx[i++];
    while (j < n)   tmp[k++] = idx[j++];
    memcpy(idx, tmp, (size_t)n * sizeof(int64_t));
}

/* Build an output VecArray of length n by gathering store slots in `order`. */
static VecArray store_to_array(const StoreCol *sc, const int64_t *order,
                               int64_t n) {
    VecArray dst = vec_array_alloc(sc->type, n);
    if (sc->type == VEC_STRING) {
        int64_t total = 0;
        for (int64_t i = 0; i < n; i++)
            if (sc->valid[order[i]]) total += sc->slen[order[i]];
        free(dst.buf.str.data);
        dst.buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
        dst.buf.str.data_len = total;
        int64_t off = 0;
        for (int64_t i = 0; i < n; i++) {
            dst.buf.str.offsets[i] = off;
            int64_t s = order[i];
            if (sc->valid[s]) {
                vec_array_set_valid(&dst, i);
                if (sc->slen[s] > 0)
                    memcpy(dst.buf.str.data + off, sc->sptr[s],
                           (size_t)sc->slen[s]);
                off += sc->slen[s];
            }
        }
        dst.buf.str.offsets[n] = off;
        return dst;
    }
    for (int64_t i = 0; i < n; i++) {
        int64_t s = order[i];
        if (!sc->valid[s]) continue;
        vec_array_set_valid(&dst, i);
        switch (sc->type) {
        case VEC_DOUBLE: dst.buf.dbl[i] = ((double  *)sc->data)[s]; break;
        case VEC_INT64:  dst.buf.i64[i] = ((int64_t *)sc->data)[s]; break;
        case VEC_INT32:  dst.buf.i32[i] = ((int32_t *)sc->data)[s]; break;
        case VEC_INT16:  dst.buf.i16[i] = ((int16_t *)sc->data)[s]; break;
        case VEC_INT8:   dst.buf.i8[i]  = ((int8_t  *)sc->data)[s]; break;
        case VEC_BOOL:   dst.buf.bln[i] = ((uint8_t *)sc->data)[s]; break;
        case VEC_STRING: break; /* handled above */
        }
    }
    return dst;
}

static void store_free(RowStore *st) {
    if (!st->cols) return;
    for (int c = 0; c < st->n_cols; c++) {
        StoreCol *sc = &st->cols[c];
        free(sc->valid);
        free(sc->data);
        if (sc->sptr) {
            for (int64_t s = 0; s < st->cap; s++) free(sc->sptr[s]);
            free(sc->sptr);
        }
        free(sc->slen);
    }
    free(st->cols);
    st->cols = NULL;
}

static VecBatch *emit_schema_only(TopNNode *tn, int n_cols) {
    VecBatch *result = vec_batch_alloc(n_cols, 0);
    for (int c = 0; c < n_cols; c++) {
        result->columns[c] =
            vec_array_alloc(tn->child->output_schema.col_types[c], 0);
        const char *nm = tn->child->output_schema.col_names[c];
        result->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(result->col_names[c], nm);
    }
    return result;
}

static VecBatch *topn_next_batch(VecNode *self) {
    TopNNode *tn = (TopNNode *)self;
    if (tn->done) return NULL;
    tn->done = 1;

    int n_cols = tn->child->output_schema.n_cols;
    int64_t k = tn->limit;
    if (k <= 0) {
        /* Drain child so upstream frees, then emit an empty result. */
        VecBatch *b;
        while ((b = tn->child->next_batch(tn->child)) != NULL) vec_batch_free(b);
        return emit_schema_only(tn, n_cols);
    }

    RowStore store;
    store.n_cols = n_cols;
    store.cap = 0;
    store.limit = k;
    store.cols = (StoreCol *)calloc((size_t)n_cols, sizeof(StoreCol));
    for (int c = 0; c < n_cols; c++)
        store.cols[c].type = tn->child->output_schema.col_types[c];

    StoreCtx ctx;
    ctx.store = &store;
    ctx.n_keys = tn->n_keys;
    ctx.keys = tn->keys;

    int64_t *heap = (int64_t *)malloc((size_t)k * sizeof(int64_t));
    int64_t heap_size = 0;

    VecBatch *batch;
    while ((batch = tn->child->next_batch(tn->child)) != NULL) {
        int64_t nlog = vec_batch_logical_rows(batch);
        for (int64_t li = 0; li < nlog; li++) {
            int64_t phys = vec_batch_physical_row(batch, li);
            if (heap_size < k) {
                /* Fill phase: admit unconditionally into a fresh slot. */
                store_ensure(&store, heap_size + 1);
                store_put(&store, heap_size, batch, phys);
                heap[heap_size] = heap_size;
                heap_sift_up(heap, heap_size, &ctx);
                heap_size++;
            } else if (cmp_incoming_slot(&ctx, batch, phys, heap[0]) < 0) {
                /* Admit: overwrite the root slot, then re-sift. */
                store_put(&store, heap[0], batch, phys);
                heap_sift_down(heap, heap_size, 0, &ctx);
            }
        }
        vec_batch_free(batch);
    }

    if (heap_size == 0) {
        free(heap);
        store_free(&store);
        return emit_schema_only(tn, n_cols);
    }

    int64_t *tmp = (int64_t *)malloc((size_t)heap_size * sizeof(int64_t));
    topn_merge_sort(heap, tmp, heap_size, &ctx);
    free(tmp);

    VecBatch *result = vec_batch_alloc(n_cols, heap_size);
    for (int c = 0; c < n_cols; c++) {
        result->columns[c] = store_to_array(&store.cols[c], heap, heap_size);
        const char *nm = tn->child->output_schema.col_names[c];
        result->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(result->col_names[c], nm);
    }

    free(heap);
    store_free(&store);
    return result;
}

static void topn_free(VecNode *self) {
    TopNNode *tn = (TopNNode *)self;
    tn->child->free_node(tn->child);
    free(tn->keys);
    vec_schema_free(&tn->base.output_schema);
    free(tn);
}

TopNNode *topn_node_create(VecNode *child, int n_keys, SortKey *keys,
                            int64_t limit) {
    TopNNode *tn = (TopNNode *)calloc(1, sizeof(TopNNode));
    if (!tn) vectra_error("alloc failed for TopNNode");
    tn->child = child;
    tn->n_keys = n_keys;
    tn->keys = keys;
    tn->limit = limit;
    tn->done = 0;

    tn->base.output_schema = vec_schema_copy(&child->output_schema);
    tn->base.next_batch = topn_next_batch;
    tn->base.free_node = topn_free;
    tn->base.kind = "TopNNode";

    return tn;
}
