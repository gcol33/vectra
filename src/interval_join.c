#include "interval_join.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "builder.h"
#include "sort.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/*
 * Bounded interval overlap join (serial sweep-merge).
 *
 * Both sides are routed through the external sort, keyed by (block, start), so
 * their rows arrive in one merged coordinate order without either side being
 * held resident. A single forward sweep advances whichever side's next interval
 * opens first; each side keeps an "active set" of intervals whose start has
 * passed but whose end has not. When an interval opens it overlaps exactly the
 * currently-active intervals of the opposite side (they started earlier and end
 * no earlier than this start), so the overlaps are emitted immediately and the
 * matches stream out rather than buffering the whole cross product. An active
 * entry carries a one-row snapshot of its columns so the pair can be emitted
 * after the source batch is gone; peak state is the two active sets (bounded by
 * the overlap depth) plus one output batch plus the two sorts' own bounded
 * spill -- independent of the input length and the number of matches.
 *
 * Blocking: the sort key leads with the block column, so a block's intervals
 * are contiguous; the active sets are flushed at every block boundary, so a
 * probe never overlaps a build from a different block. A NA block key (or a NA
 * endpoint, or start > end, or a zero-length interval under strict overlap) is
 * dropped, matching the resident implementation.
 */

/* ------------------------------------------------------------------ */
/*  Numeric endpoint access                                            */
/* ------------------------------------------------------------------ */

static inline double iv_value(const VecArray *a, int64_t row) {
    if (a->type == VEC_DOUBLE) return a->buf.dbl[row];
    return (double)vec_array_get_int(a, row);  /* int8/16/32/64 */
}

/* ------------------------------------------------------------------ */
/*  A one-row snapshot of a side's columns (owns its arrays)           */
/* ------------------------------------------------------------------ */

typedef struct {
    VecArray *cols;   /* ncols one-row arrays */
    int       ncols;
} IvSnap;

static IvSnap iv_snap_take(const VecArray *src, int ncols, int64_t row) {
    IvSnap s;
    s.ncols = ncols;
    s.cols = (VecArray *)malloc((size_t)ncols * sizeof(VecArray));
    int32_t r = (int32_t)row;
    for (int c = 0; c < ncols; c++)
        s.cols[c] = vec_array_gather(&src[c], &r, 1);
    return s;
}

static void iv_snap_free(IvSnap *s) {
    if (!s->cols) return;
    for (int c = 0; c < s->ncols; c++) vec_array_free(&s->cols[c]);
    free(s->cols);
    s->cols = NULL;
}

/* ------------------------------------------------------------------ */
/*  Active set: open intervals of one side                             */
/* ------------------------------------------------------------------ */

typedef struct {
    double end;
    int    matched;   /* has emitted at least one pair (left join) */
    IvSnap snap;
} IvActive;

typedef struct {
    IvActive *e;
    int64_t   n, cap;
} IvActiveSet;

static void ivset_push(IvActiveSet *s, double end, IvSnap snap) {
    if (s->n >= s->cap) {
        s->cap = s->cap ? s->cap * 2 : 16;
        s->e = (IvActive *)realloc(s->e, (size_t)s->cap * sizeof(IvActive));
        if (!s->e) vectra_error("interval_join: realloc failed (active set)");
    }
    s->e[s->n].end = end;
    s->e[s->n].matched = 0;
    s->e[s->n].snap = snap;
    s->n++;
}

/* ------------------------------------------------------------------ */
/*  Sorted-stream cursor over one child                                */
/* ------------------------------------------------------------------ */

typedef struct {
    VecNode  *node;
    VecBatch *batch;
    int64_t   li;      /* logical row */
    int64_t   nlog;
    int       done;
    int       start_col, end_col, block_col;
    int       closed;
    /* current valid interval (skips NA / inverted / zero-length-strict rows) */
    int64_t   phys;    /* physical row of the current interval */
    double    start, end;
    const char *bkey; int64_t blen;  /* block key (NULL/0 when unblocked) */
} IvCursor;

/* Load the next child batch into the cursor, or mark done. */
static int cursor_load(IvCursor *c) {
    if (c->batch) { vec_batch_free(c->batch); c->batch = NULL; }
    c->batch = c->node->next_batch(c->node);
    if (!c->batch) { c->done = 1; return 0; }
    c->li = 0;
    c->nlog = vec_batch_logical_rows(c->batch);
    return 1;
}

/* Advance the cursor to its next VALID interval (or done). */
static void cursor_advance(IvCursor *c) {
    while (1) {
        if (c->done) return;
        if (!c->batch || c->li >= c->nlog) {
            if (!cursor_load(c)) return;
            continue;
        }
        int64_t phys = vec_batch_physical_row(c->batch, c->li);
        c->li++;
        const VecArray *s = &c->batch->columns[c->start_col];
        const VecArray *e = &c->batch->columns[c->end_col];
        if (!vec_array_is_valid(s, phys) || !vec_array_is_valid(e, phys)) continue;
        double a = iv_value(s, phys), b = iv_value(e, phys);
        if (a > b) continue;                 /* inverted */
        if (!c->closed && a == b) continue;  /* zero length, strict */
        if (c->block_col >= 0) {
            const VecArray *bl = &c->batch->columns[c->block_col];
            if (!vec_array_is_valid(bl, phys)) continue;  /* NA block */
            c->bkey = bl->buf.str.data + bl->buf.str.offsets[phys];
            c->blen = bl->buf.str.offsets[phys + 1] - bl->buf.str.offsets[phys];
        } else {
            c->bkey = NULL; c->blen = 0;
        }
        c->phys = phys; c->start = a; c->end = b;
        return;
    }
}

/* Compare two block keys (lexicographic). */
static int block_cmp(const char *a, int64_t la, const char *b, int64_t lb) {
    int64_t m = la < lb ? la : lb;
    int r = (m > 0) ? memcmp(a, b, (size_t)m) : 0;
    if (r != 0) return r < 0 ? -1 : 1;
    return (la < lb) ? -1 : (la > lb) ? 1 : 0;
}

/* Order two cursors by (block, start). Returns <0 if a opens first, >0 if b
   first, 0 if equal. A done cursor never opens first. */
static int cursor_order(const IvCursor *a, const IvCursor *b) {
    if (a->done) return 1;
    if (b->done) return -1;
    if (a->block_col >= 0) {
        int bc = block_cmp(a->bkey, a->blen, b->bkey, b->blen);
        if (bc != 0) return bc;
    }
    return (a->start < b->start) ? -1 : (a->start > b->start) ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/*  Output schema: probe cols + build cols (suffix on collision)       */
/* ------------------------------------------------------------------ */

static VecSchema build_output_schema(IntervalJoinNode *ij) {
    const VecSchema *pschema = &ij->probe_node->output_schema;
    const VecSchema *bschema = &ij->build_node->output_schema;

    int total = pschema->n_cols + bschema->n_cols;
    char   **names = (char **)malloc((size_t)total * sizeof(char *));
    VecType *types = (VecType *)malloc((size_t)total * sizeof(VecType));
    if (!names || !types) vectra_error("alloc failed for output schema");

    int idx = 0;
    for (int c = 0; c < pschema->n_cols; c++) {
        names[idx] = strdup(pschema->col_names[c]);
        types[idx] = pschema->col_types[c];
        idx++;
    }
    for (int c = 0; c < bschema->n_cols; c++) {
        const char *bname = bschema->col_names[c];
        int collision = 0;
        for (int pc = 0; pc < pschema->n_cols; pc++) {
            if (strcmp(pschema->col_names[pc], bname) == 0) { collision = 1; break; }
        }
        if (collision && ij->suffix_y) {
            size_t len = strlen(bname) + strlen(ij->suffix_y) + 1;
            names[idx] = (char *)malloc(len);
            snprintf(names[idx], len, "%s%s", bname, ij->suffix_y);
        } else {
            names[idx] = strdup(bname);
        }
        types[idx] = bschema->col_types[c];
        idx++;
    }

    ij->out_ncols = idx;
    VecSchema schema = vec_schema_create(idx, names, types);
    for (int i = 0; i < idx; i++) free(names[i]);
    free(names);
    free(types);
    return schema;
}

/* ------------------------------------------------------------------ */
/*  Sweep state (persists across next_batch calls)                     */
/* ------------------------------------------------------------------ */

typedef struct {
    IvCursor p, b;
    IvActiveSet act_p, act_b;
    int have_block; const char *cur_block; int64_t cur_blen; char *cur_block_own;
    VecArrayBuilder *builders;   /* out_ncols */
    int64_t produced;            /* rows appended to builders this batch */
    int started;
    int finished;
} IvSweep;

#define IV_EMIT_BATCH 8192

/* Append one probe row + one build row (or NA build) to the output builders. */
static void emit_pair(IntervalJoinNode *ij, IvSweep *sw,
                      const VecArray *pcols, int64_t prow,
                      const VecArray *bcols, int64_t brow) {
    int col = 0;
    for (int c = 0; c < ij->p_ncols; c++)
        vec_builder_append_one(&sw->builders[col++], &pcols[c], prow);
    if (bcols) {
        for (int c = 0; c < ij->b_ncols; c++)
            vec_builder_append_one(&sw->builders[col++], &bcols[c], brow);
    } else {
        for (int c = 0; c < ij->b_ncols; c++)
            vec_builder_append_na(&sw->builders[col++]);
    }
    sw->produced++;
}

/* Evict from `set` the intervals that can no longer overlap: end < pos for
   closed overlap, end <= pos for strict. For the left join, an evicted probe
   that never matched emits an NA-build row (set == active probes, is_probe). */
static void evict(IntervalJoinNode *ij, IvSweep *sw, IvActiveSet *set,
                  double pos, int is_probe) {
    int64_t w = 0;
    for (int64_t i = 0; i < set->n; i++) {
        int expired = ij->closed ? (set->e[i].end < pos) : (set->e[i].end <= pos);
        if (expired) {
            if (is_probe && ij->kind == IJOIN_LEFT && !set->e[i].matched)
                emit_pair(ij, sw, set->e[i].snap.cols, 0, NULL, 0);
            iv_snap_free(&set->e[i].snap);
        } else {
            if (w != i) set->e[w] = set->e[i];
            w++;
        }
    }
    set->n = w;
}

/* Flush both active sets at a block boundary / at end: emit left-join
   unmatched probes, free snapshots. */
static void flush_block(IntervalJoinNode *ij, IvSweep *sw) {
    for (int64_t i = 0; i < sw->act_p.n; i++) {
        if (ij->kind == IJOIN_LEFT && !sw->act_p.e[i].matched)
            emit_pair(ij, sw, sw->act_p.e[i].snap.cols, 0, NULL, 0);
        iv_snap_free(&sw->act_p.e[i].snap);
    }
    sw->act_p.n = 0;
    for (int64_t i = 0; i < sw->act_b.n; i++) iv_snap_free(&sw->act_b.e[i].snap);
    sw->act_b.n = 0;
}

/* Process one opening interval (the earlier of the two cursors). */
static void sweep_step(IntervalJoinNode *ij, IvSweep *sw) {
    int ord = cursor_order(&sw->p, &sw->b);
    IvCursor *opener = (ord <= 0) ? &sw->p : &sw->b;
    int opener_is_probe = (opener == &sw->p);

    /* Block boundary: when the opener starts a new block, flush active sets. */
    if (opener->block_col >= 0) {
        int newblock = !sw->have_block ||
            block_cmp(sw->cur_block, sw->cur_blen, opener->bkey, opener->blen) != 0;
        if (newblock) {
            flush_block(ij, sw);
            free(sw->cur_block_own);
            sw->cur_block_own = (char *)malloc((size_t)(opener->blen > 0 ? opener->blen : 1));
            if (opener->blen > 0) memcpy(sw->cur_block_own, opener->bkey, (size_t)opener->blen);
            sw->cur_block = sw->cur_block_own;
            sw->cur_blen = opener->blen;
            sw->have_block = 1;
        }
    }

    double pos = opener->start;
    /* Expire intervals that end before this opener starts. */
    evict(ij, sw, &sw->act_p, pos, 1);
    evict(ij, sw, &sw->act_b, pos, 0);

    const VecArray *ocols = opener->batch->columns;
    int64_t orow = opener->phys;

    if (opener_is_probe) {
        for (int64_t i = 0; i < sw->act_b.n; i++) {
            emit_pair(ij, sw, ocols, orow, sw->act_b.e[i].snap.cols, 0);
            sw->act_b.e[i].matched = 1;
        }
        int any = (sw->act_b.n > 0);
        IvSnap snap = iv_snap_take(ocols, ij->p_ncols, orow);
        ivset_push(&sw->act_p, opener->end, snap);
        if (any) sw->act_p.e[sw->act_p.n - 1].matched = 1;
    } else {
        for (int64_t i = 0; i < sw->act_p.n; i++) {
            emit_pair(ij, sw, sw->act_p.e[i].snap.cols, 0, ocols, orow);
            sw->act_p.e[i].matched = 1;
        }
        IvSnap snap = iv_snap_take(ocols, ij->b_ncols, orow);
        ivset_push(&sw->act_b, opener->end, snap);
    }

    cursor_advance(opener);
}

/* ------------------------------------------------------------------ */
/*  next_batch                                                         */
/* ------------------------------------------------------------------ */

static void builders_init(IntervalJoinNode *ij, IvSweep *sw) {
    const VecSchema *out = &ij->base.output_schema;
    sw->builders = (VecArrayBuilder *)calloc((size_t)ij->out_ncols,
                                             sizeof(VecArrayBuilder));
    for (int c = 0; c < ij->out_ncols; c++)
        sw->builders[c] = vec_builder_init(out->col_types[c]);
    sw->produced = 0;
}

static VecBatch *builders_finish(IntervalJoinNode *ij, IvSweep *sw) {
    const VecSchema *out = &ij->base.output_schema;
    int64_t nr = sw->produced;
    VecBatch *batch = vec_batch_alloc(ij->out_ncols, nr);
    for (int c = 0; c < ij->out_ncols; c++)
        batch->columns[c] = vec_builder_finish(&sw->builders[c]);
    for (int c = 0; c < ij->out_ncols; c++) {
        free(batch->col_names[c]);
        batch->col_names[c] = strdup(out->col_names[c]);
    }
    batch->n_rows = nr;
    free(sw->builders);
    sw->builders = NULL;
    return batch;
}

static VecBatch *interval_join_next_batch(VecNode *self) {
    IntervalJoinNode *ij = (IntervalJoinNode *)self;
    IvSweep *sw = (IvSweep *)ij->sweep;

    if (sw->finished) return NULL;

    if (!sw->started) {
        sw->started = 1;
        sw->p.node = ij->probe_node; sw->p.start_col = ij->probe_start_col;
        sw->p.end_col = ij->probe_end_col; sw->p.block_col = ij->probe_block_col;
        sw->p.closed = ij->closed;
        sw->b.node = ij->build_node; sw->b.start_col = ij->build_start_col;
        sw->b.end_col = ij->build_end_col; sw->b.block_col = ij->build_block_col;
        sw->b.closed = ij->closed;
        cursor_advance(&sw->p);
        cursor_advance(&sw->b);
    }

    builders_init(ij, sw);

    while (!(sw->p.done && sw->b.done)) {
        sweep_step(ij, sw);
        if (sw->produced >= IV_EMIT_BATCH)
            return builders_finish(ij, sw);
    }

    /* Both streams drained: flush remaining active intervals. */
    flush_block(ij, sw);
    sw->finished = 1;

    if (sw->produced == 0) {
        for (int c = 0; c < ij->out_ncols; c++) vec_builder_free(&sw->builders[c]);
        free(sw->builders);
        sw->builders = NULL;
        return NULL;
    }
    return builders_finish(ij, sw);
}

/* ------------------------------------------------------------------ */
/*  Cleanup                                                            */
/* ------------------------------------------------------------------ */

static void interval_join_free(VecNode *self) {
    IntervalJoinNode *ij = (IntervalJoinNode *)self;
    IvSweep *sw = (IvSweep *)ij->sweep;
    if (sw) {
        if (sw->p.batch) vec_batch_free(sw->p.batch);
        if (sw->b.batch) vec_batch_free(sw->b.batch);
        for (int64_t i = 0; i < sw->act_p.n; i++) iv_snap_free(&sw->act_p.e[i].snap);
        for (int64_t i = 0; i < sw->act_b.n; i++) iv_snap_free(&sw->act_b.e[i].snap);
        free(sw->act_p.e);
        free(sw->act_b.e);
        free(sw->cur_block_own);
        if (sw->builders) {
            for (int c = 0; c < ij->out_ncols; c++) vec_builder_free(&sw->builders[c]);
            free(sw->builders);
        }
        free(sw);
    }
    if (ij->probe_node) ij->probe_node->free_node(ij->probe_node);
    if (ij->build_node) ij->build_node->free_node(ij->build_node);
    free(ij->suffix_y);
    vec_schema_free(&ij->base.output_schema);
    free(ij);
}

/* ------------------------------------------------------------------ */
/*  Constructor                                                        */
/* ------------------------------------------------------------------ */

/* Wrap a child in a SortNode keyed by (block, start). Resolves the ordering
   column indices in the child schema (unchanged by the sort). */
static VecNode *sort_by_block_start(VecNode *child, int block_col, int start_col,
                                    const char *temp_dir, int64_t mem_budget) {
    int nk = (block_col >= 0) ? 2 : 1;
    SortKey *sk = (SortKey *)malloc((size_t)nk * sizeof(SortKey));
    int i = 0;
    if (block_col >= 0) { sk[i].col_index = block_col; sk[i].descending = 0; sk[i].na_last = 0; i++; }
    sk[i].col_index = start_col; sk[i].descending = 0; sk[i].na_last = 0;
    int64_t m = mem_budget > 0 ? mem_budget : VECTRA_SORT_MEM_DEFAULT;
    /* sort_node_create takes ownership of sk. */
    SortNode *sn = sort_node_create(child, nk, sk, temp_dir, m);
    return (VecNode *)sn;
}

IntervalJoinNode *interval_join_node_create(
    VecNode *probe, VecNode *build,
    int probe_start_col, int probe_end_col,
    int build_start_col, int build_end_col,
    int probe_block_col, int build_block_col,
    IntervalJoinKind kind, int closed, int n_threads,
    const char *suffix_y, int64_t mem_budget, const char *temp_dir)
{
    IntervalJoinNode *ij = (IntervalJoinNode *)calloc(1, sizeof(IntervalJoinNode));
    if (!ij) vectra_error("alloc failed for IntervalJoinNode");

    ij->probe_start_col = probe_start_col;
    ij->probe_end_col   = probe_end_col;
    ij->build_start_col = build_start_col;
    ij->build_end_col   = build_end_col;
    ij->probe_block_col = probe_block_col;
    ij->build_block_col = build_block_col;
    ij->kind = kind;
    ij->closed = closed;
    ij->n_threads = n_threads;
    ij->suffix_y = suffix_y ? strdup(suffix_y) : strdup(".y");

    ij->p_ncols = probe->output_schema.n_cols;
    ij->b_ncols = build->output_schema.n_cols;

    /* Output schema is built from the child schemas; the sort keeps column
       layout, only reordering rows, so point at the originals for the schema
       first, then wrap each side in its (block, start) sort. */
    ij->probe_node = probe;
    ij->build_node = build;
    ij->base.output_schema = build_output_schema(ij);

    ij->probe_node = sort_by_block_start(probe, probe_block_col, probe_start_col,
                                         temp_dir, mem_budget);
    ij->build_node = sort_by_block_start(build, build_block_col, build_start_col,
                                         temp_dir, mem_budget);

    ij->sweep = calloc(1, sizeof(IvSweep));
    if (!ij->sweep) vectra_error("alloc failed for interval sweep");

    ij->base.next_batch = interval_join_next_batch;
    ij->base.kind = "IntervalJoinNode";
    ij->base.free_node = interval_join_free;

    return ij;
}
