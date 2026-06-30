#include "interval_join.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "builder.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

#include "vec_omp.h"

/* ------------------------------------------------------------------ */
/*  Numeric value access                                               */
/* ------------------------------------------------------------------ */

static inline double iv_value(const VecArray *a, int64_t row) {
    if (a->type == VEC_DOUBLE) return a->buf.dbl[row];
    return (double)vec_array_get_int(a, row);  /* int8/16/32/64 */
}

static inline int iv_endpoints_valid(const VecArray *s, const VecArray *e,
                                     int64_t row) {
    return vec_array_is_valid(s, row) && vec_array_is_valid(e, row);
}

/* ------------------------------------------------------------------ */
/*  Growable match buffer (one per thread)                             */
/* ------------------------------------------------------------------ */

typedef struct {
    IntervalMatch *buf;
    int64_t        count;
    int64_t        capacity;
} IvMatchBuf;

static void imbuf_init(IvMatchBuf *b, int64_t cap) {
    b->buf = (IntervalMatch *)malloc((size_t)cap * sizeof(IntervalMatch));
    b->count = 0;
    b->capacity = cap;
    if (!b->buf) vectra_error("alloc failed for IvMatchBuf");
}

static void imbuf_push(IvMatchBuf *b, int64_t pi, int64_t bi) {
    if (b->count >= b->capacity) {
        b->capacity *= 2;
        b->buf = (IntervalMatch *)realloc(b->buf,
            (size_t)b->capacity * sizeof(IntervalMatch));
        if (!b->buf) vectra_error("realloc failed for IvMatchBuf");
    }
    b->buf[b->count++] = (IntervalMatch){pi, bi};
}

static void imbuf_free(IvMatchBuf *b) {
    free(b->buf);
    b->buf = NULL;
    b->count = 0;
    b->capacity = 0;
}

/* ------------------------------------------------------------------ */
/*  Sweep-line over one block partition                                */
/* ------------------------------------------------------------------ */

/* An interval endpoint event. rank orders ties at equal coords so that
   closed intervals count touching endpoints as overlap and strict ones do
   not (see build below). */
typedef struct {
    double  coord;
    int     rank;   /* sort key for equal coords */
    int     side;   /* 0 = probe, 1 = build */
    int64_t idx;    /* compact local index into that side's interval arrays */
} IvEvent;

static int cmp_event(const void *a, const void *b) {
    const IvEvent *x = (const IvEvent *)a;
    const IvEvent *y = (const IvEvent *)b;
    if (x->coord < y->coord) return -1;
    if (x->coord > y->coord) return  1;
    if (x->rank  < y->rank)  return -1;
    if (x->rank  > y->rank)  return  1;
    return 0;
}

/* Collect the valid intervals of one side in a partition into compact arrays.
   Drops rows with a NA endpoint or with start > end. Under strict overlap
   (closed == 0) also drops zero-length intervals (start == end): they can
   never strictly overlap, and their coincident open/close events would
   otherwise reorder under the strict tie-break. Returns the count. */
static int64_t collect_intervals(const JoinPartition *part,
                                 const VecArray *start, const VecArray *end,
                                 int closed,
                                 double **out_lo, double **out_hi,
                                 int64_t **out_glob) {
    int64_t n = part->n_rows;
    double  *lo = (double *)malloc((size_t)(n > 0 ? n : 1) * sizeof(double));
    double  *hi = (double *)malloc((size_t)(n > 0 ? n : 1) * sizeof(double));
    int64_t *gl = (int64_t *)malloc((size_t)(n > 0 ? n : 1) * sizeof(int64_t));
    if (!lo || !hi || !gl) vectra_error("alloc failed in interval collect");

    int64_t m = 0;
    for (int64_t i = 0; i < n; i++) {
        int64_t row = part->rows[i];
        if (!iv_endpoints_valid(start, end, row)) continue;
        double a = iv_value(start, row);
        double b = iv_value(end, row);
        if (a > b) continue;             /* degenerate interval: never matches */
        if (!closed && a == b) continue; /* zero length: no strict overlap */
        lo[m] = a; hi[m] = b; gl[m] = row; m++;
    }
    *out_lo = lo; *out_hi = hi; *out_glob = gl;
    return m;
}

/* Doubly-linked active set over compact local indices. */
typedef struct {
    int64_t *next;
    int64_t *prev;
    int64_t  head;
} ActiveSet;

static void active_init(ActiveSet *s, int64_t n) {
    s->next = (int64_t *)malloc((size_t)(n > 0 ? n : 1) * sizeof(int64_t));
    s->prev = (int64_t *)malloc((size_t)(n > 0 ? n : 1) * sizeof(int64_t));
    if (!s->next || !s->prev) vectra_error("alloc failed for active set");
    s->head = -1;
}

static void active_free(ActiveSet *s) {
    free(s->next); free(s->prev);
    s->next = s->prev = NULL; s->head = -1;
}

static inline void active_insert(ActiveSet *s, int64_t i) {
    s->next[i] = s->head;
    s->prev[i] = -1;
    if (s->head != -1) s->prev[s->head] = i;
    s->head = i;
}

static inline void active_remove(ActiveSet *s, int64_t i) {
    if (s->prev[i] != -1) s->next[s->prev[i]] = s->next[i];
    else                  s->head = s->next[i];
    if (s->next[i] != -1) s->prev[s->next[i]] = s->prev[i];
}

/* Run the sweep for one partition, pushing overlapping (probe,build) pairs
   into buf. When probe_matched != NULL (left join), marks each matched probe
   global row. */
static void sweep_partition(IntervalJoinNode *ij, int64_t part,
                            IvMatchBuf *buf, uint8_t *probe_matched) {
    const JoinPartition *pp = &ij->probe_parts[part];
    const JoinPartition *bp = &ij->build_parts[part];
    if (pp->n_rows == 0 || bp->n_rows == 0) return;

    const VecArray *ps = &ij->p_cols[ij->probe_start_col];
    const VecArray *pe = &ij->p_cols[ij->probe_end_col];
    const VecArray *bs = &ij->b_cols[ij->build_start_col];
    const VecArray *be = &ij->b_cols[ij->build_end_col];

    double *p_lo, *p_hi; int64_t *p_glob;
    double *b_lo, *b_hi; int64_t *b_glob;
    int64_t np = collect_intervals(pp, ps, pe, ij->closed, &p_lo, &p_hi, &p_glob);
    int64_t nb = collect_intervals(bp, bs, be, ij->closed, &b_lo, &b_hi, &b_glob);

    if (np == 0 || nb == 0) {
        free(p_lo); free(p_hi); free(p_glob);
        free(b_lo); free(b_hi); free(b_glob);
        return;
    }

    /* Tie-break ranks. open_rank < close_rank => closed (touching overlaps);
       close_rank < open_rank => strict. */
    int open_rank  = ij->closed ? 0 : 1;
    int close_rank = ij->closed ? 1 : 0;

    int64_t n_ev = 2 * (np + nb);
    IvEvent *ev = (IvEvent *)malloc((size_t)n_ev * sizeof(IvEvent));
    if (!ev) vectra_error("alloc failed for sweep events");

    int64_t k = 0;
    for (int64_t i = 0; i < np; i++) {
        ev[k++] = (IvEvent){p_lo[i], open_rank,  0, i};
        ev[k++] = (IvEvent){p_hi[i], close_rank, 0, i};
    }
    for (int64_t i = 0; i < nb; i++) {
        ev[k++] = (IvEvent){b_lo[i], open_rank,  1, i};
        ev[k++] = (IvEvent){b_hi[i], close_rank, 1, i};
    }
    qsort(ev, (size_t)n_ev, sizeof(IvEvent), cmp_event);

    ActiveSet act_p, act_b;
    active_init(&act_p, np);
    active_init(&act_b, nb);

    for (int64_t i = 0; i < n_ev; i++) {
        IvEvent *e = &ev[i];
        int is_open = (e->rank == open_rank);
        if (is_open) {
            if (e->side == 0) {
                /* probe opens: overlaps every active build */
                for (int64_t j = act_b.head; j != -1; j = act_b.next[j]) {
                    imbuf_push(buf, p_glob[e->idx], b_glob[j]);
                    if (probe_matched) probe_matched[p_glob[e->idx]] = 1;
                }
                active_insert(&act_p, e->idx);
            } else {
                /* build opens: overlaps every active probe */
                for (int64_t j = act_p.head; j != -1; j = act_p.next[j]) {
                    imbuf_push(buf, p_glob[j], b_glob[e->idx]);
                    if (probe_matched) probe_matched[p_glob[j]] = 1;
                }
                active_insert(&act_b, e->idx);
            }
        } else {
            if (e->side == 0) active_remove(&act_p, e->idx);
            else              active_remove(&act_b, e->idx);
        }
    }

    active_free(&act_p);
    active_free(&act_b);
    free(ev);
    free(p_lo); free(p_hi); free(p_glob);
    free(b_lo); free(b_hi); free(b_glob);
}

/* ------------------------------------------------------------------ */
/*  Match phase: sweep every partition in parallel                     */
/* ------------------------------------------------------------------ */

static void interval_match_phase(IntervalJoinNode *ij) {
    int n_threads = ij->n_threads;
    if (n_threads < 1) n_threads = 1;
#ifdef _OPENMP
    if (n_threads > omp_get_max_threads()) n_threads = omp_get_max_threads();
#else
    n_threads = 1;
#endif

    uint8_t *probe_matched = NULL;
    if (ij->kind == IJOIN_LEFT)
        probe_matched = (uint8_t *)calloc((size_t)(ij->p_nrows > 0 ?
                                          ij->p_nrows : 1), sizeof(uint8_t));

    IvMatchBuf *tbufs = (IvMatchBuf *)calloc((size_t)n_threads,
                                             sizeof(IvMatchBuf));
    if (!tbufs) vectra_error("alloc failed for thread match buffers");
    for (int t = 0; t < n_threads; t++) imbuf_init(&tbufs[t], 4096);

#ifdef _OPENMP
    #pragma omp parallel for schedule(dynamic, 1) num_threads(n_threads)
#endif
    for (int64_t p = 0; p < ij->n_parts; p++) {
#ifdef _OPENMP
        int tid = omp_get_thread_num();
#else
        int tid = 0;
#endif
        sweep_partition(ij, p, &tbufs[tid], probe_matched);
    }

    /* Merge thread buffers */
    int64_t total = 0;
    for (int t = 0; t < n_threads; t++) total += tbufs[t].count;

    /* Left join: count probe rows that matched nothing (they get an NA row) */
    int64_t n_unmatched = 0;
    if (probe_matched) {
        for (int64_t r = 0; r < ij->p_nrows; r++)
            if (!probe_matched[r]) n_unmatched++;
    }

    int64_t cap = total + n_unmatched;
    ij->matches = (IntervalMatch *)malloc(
        (size_t)(cap > 0 ? cap : 1) * sizeof(IntervalMatch));
    if (!ij->matches) vectra_error("alloc failed for merged matches");

    int64_t pos = 0;
    for (int t = 0; t < n_threads; t++) {
        if (tbufs[t].count > 0) {
            memcpy(ij->matches + pos, tbufs[t].buf,
                   (size_t)tbufs[t].count * sizeof(IntervalMatch));
            pos += tbufs[t].count;
        }
        imbuf_free(&tbufs[t]);
    }
    free(tbufs);

    if (probe_matched) {
        for (int64_t r = 0; r < ij->p_nrows; r++)
            if (!probe_matched[r])
                ij->matches[pos++] = (IntervalMatch){r, -1};
        free(probe_matched);
    }

    ij->n_matches = pos;
}

/* ------------------------------------------------------------------ */
/*  Sort matches by (probe_idx, build_idx) for deterministic output    */
/* ------------------------------------------------------------------ */

static int cmp_match(const void *a, const void *b) {
    const IntervalMatch *ma = (const IntervalMatch *)a;
    const IntervalMatch *mb = (const IntervalMatch *)b;
    if (ma->probe_idx < mb->probe_idx) return -1;
    if (ma->probe_idx > mb->probe_idx) return  1;
    if (ma->build_idx < mb->build_idx) return -1;
    if (ma->build_idx > mb->build_idx) return  1;
    return 0;
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
/*  next_batch                                                         */
/* ------------------------------------------------------------------ */

#define IV_EMIT_BATCH 8192

static VecBatch *interval_join_next_batch(VecNode *self) {
    IntervalJoinNode *ij = (IntervalJoinNode *)self;

    if (ij->state == IJSTATE_MATERIALIZE) {
        join_materialize_side(ij->probe_node,
                              ij->probe_node->output_schema.n_cols,
                              &ij->p_cols, &ij->p_nrows);
        ij->p_ncols = ij->probe_node->output_schema.n_cols;

        join_materialize_side(ij->build_node,
                              ij->build_node->output_schema.n_cols,
                              &ij->b_cols, &ij->b_nrows);
        ij->b_ncols = ij->build_node->output_schema.n_cols;

        JoinPartitionSet ps = join_partition_build(
            ij->p_cols, ij->p_nrows, ij->probe_block_col,
            ij->b_cols, ij->b_nrows, ij->build_block_col);
        ij->probe_parts = ps.probe_parts;
        ij->build_parts = ps.build_parts;
        ij->n_parts = ps.n_parts;

        interval_match_phase(ij);

        if (ij->n_matches > 1)
            qsort(ij->matches, (size_t)ij->n_matches,
                  sizeof(IntervalMatch), cmp_match);

        ij->emit_pos = 0;
        ij->state = IJSTATE_EMIT;
    }

    if (ij->state == IJSTATE_DONE) return NULL;

    int64_t remaining = ij->n_matches - ij->emit_pos;
    if (remaining <= 0) {
        ij->state = IJSTATE_DONE;
        return NULL;
    }

    int64_t batch_size = remaining < IV_EMIT_BATCH ? remaining : IV_EMIT_BATCH;
    int total_cols = ij->out_ncols;

    VecArrayBuilder *builders = (VecArrayBuilder *)calloc(
        (size_t)total_cols, sizeof(VecArrayBuilder));
    if (!builders) vectra_error("alloc failed for output builders");

    const VecSchema *out = &ij->base.output_schema;
    for (int c = 0; c < total_cols; c++) {
        builders[c] = vec_builder_init(out->col_types[c]);
        vec_builder_reserve(&builders[c], batch_size);
    }

    int p_ncols = ij->p_ncols;
    int b_ncols = ij->b_ncols;

    for (int64_t m = 0; m < batch_size; m++) {
        IntervalMatch *match = &ij->matches[ij->emit_pos + m];
        int64_t pi = match->probe_idx;
        int64_t bi = match->build_idx;
        int col = 0;
        for (int c = 0; c < p_ncols; c++)
            vec_builder_append_one(&builders[col++], &ij->p_cols[c], pi);
        if (bi >= 0) {
            for (int c = 0; c < b_ncols; c++)
                vec_builder_append_one(&builders[col++], &ij->b_cols[c], bi);
        } else {
            for (int c = 0; c < b_ncols; c++)
                vec_builder_append_na(&builders[col++]);
        }
    }

    ij->emit_pos += batch_size;

    VecBatch *batch = vec_batch_alloc(total_cols, batch_size);
    for (int c = 0; c < total_cols; c++)
        batch->columns[c] = vec_builder_finish(&builders[c]);
    for (int c = 0; c < total_cols; c++) {
        free(batch->col_names[c]);
        batch->col_names[c] = strdup(out->col_names[c]);
    }
    batch->n_rows = batch_size;
    free(builders);
    return batch;
}

/* ------------------------------------------------------------------ */
/*  Cleanup                                                            */
/* ------------------------------------------------------------------ */

static void interval_join_free(VecNode *self) {
    IntervalJoinNode *ij = (IntervalJoinNode *)self;

    if (ij->probe_node) ij->probe_node->free_node(ij->probe_node);
    if (ij->build_node) ij->build_node->free_node(ij->build_node);

    if (ij->p_cols) {
        for (int c = 0; c < ij->p_ncols; c++) vec_array_free(&ij->p_cols[c]);
        free(ij->p_cols);
    }
    if (ij->b_cols) {
        for (int c = 0; c < ij->b_ncols; c++) vec_array_free(&ij->b_cols[c]);
        free(ij->b_cols);
    }

    join_partition_free(ij->probe_parts, ij->n_parts);
    join_partition_free(ij->build_parts, ij->n_parts);

    free(ij->matches);
    free(ij->suffix_y);
    vec_schema_free(&ij->base.output_schema);
    free(ij);
}

/* ------------------------------------------------------------------ */
/*  Constructor                                                        */
/* ------------------------------------------------------------------ */

IntervalJoinNode *interval_join_node_create(
    VecNode *probe, VecNode *build,
    int probe_start_col, int probe_end_col,
    int build_start_col, int build_end_col,
    int probe_block_col, int build_block_col,
    IntervalJoinKind kind, int closed, int n_threads,
    const char *suffix_y)
{
    IntervalJoinNode *ij = (IntervalJoinNode *)calloc(1, sizeof(IntervalJoinNode));
    if (!ij) vectra_error("alloc failed for IntervalJoinNode");

    ij->probe_node = probe;
    ij->build_node = build;
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
    ij->state = IJSTATE_MATERIALIZE;

    ij->base.output_schema = build_output_schema(ij);
    ij->base.next_batch = interval_join_next_batch;
    ij->base.kind = "IntervalJoinNode";
    ij->base.free_node = interval_join_free;

    return ij;
}
