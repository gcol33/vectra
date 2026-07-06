#include "fuzzy_join.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "builder.h"
#include "error.h"
#include "string_distance.h"
#include "join_partition.h"   /* join_materialize_side (resident build side) */
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>

#include "vec_omp.h"

/*
 * Fuzzy join: streaming-probe, resident-build.
 *
 * The build (right) side is materialized once and, when a blocking column is
 * given, indexed by exact block key -- every probe row must be compared
 * against candidate build rows, so the build side is inherently resident.
 * The probe (left) side is streamed one batch at a time; each batch's matches
 * are computed, ordered, and emitted before the next batch is pulled. Peak
 * memory is therefore the build side plus one probe batch plus that batch's
 * matches -- never the whole probe side or the whole cross-product of matches,
 * which the earlier materialize-everything implementation held at once.
 */

/* ------------------------------------------------------------------ */
/*  Distance helpers                                                   */
/* ------------------------------------------------------------------ */

static inline const char *str_ptr(const VecArray *arr, int64_t row) {
    return arr->buf.str.data + arr->buf.str.offsets[row];
}

static inline int64_t str_len(const VecArray *arr, int64_t row) {
    return arr->buf.str.offsets[row + 1] - arr->buf.str.offsets[row];
}

static inline double compute_dist(FuzzyMethod method,
                                   const char *a, int64_t la,
                                   const char *b, int64_t lb,
                                   double max_dist) {
    if (method == FUZZY_JW) {
        double sim = strdist_jaro_winkler(a, la, b, lb);
        return 1.0 - sim;  /* similarity -> distance */
    }
    int64_t max_len = la > lb ? la : lb;
    if (max_len == 0) return 0.0;
    int64_t max_raw = (int64_t)ceil(max_dist * (double)max_len);
    int64_t raw = (method == FUZZY_DL)
        ? strdist_dl(a, la, b, lb, max_raw)
        : strdist_levenshtein(a, la, b, lb, max_raw);
    if (raw > max_raw) return max_dist + 1.0;  /* exceeded threshold */
    return (double)raw / (double)max_len;
}

/* ------------------------------------------------------------------ */
/*  Per-thread match buffer                                            */
/* ------------------------------------------------------------------ */

static void fmbuf_init(FuzzyMatchBuf *buf, int64_t initial_cap) {
    buf->buf = (FuzzyMatch *)malloc((size_t)initial_cap * sizeof(FuzzyMatch));
    buf->count = 0;
    buf->capacity = initial_cap;
    if (!buf->buf) vectra_error("alloc failed for FuzzyMatchBuf");
}

static void fmbuf_push(FuzzyMatchBuf *buf, int64_t pi, int64_t bi, double d) {
    if (buf->count >= buf->capacity) {
        buf->capacity *= 2;
        buf->buf = (FuzzyMatch *)realloc(buf->buf,
            (size_t)buf->capacity * sizeof(FuzzyMatch));
        if (!buf->buf) vectra_error("realloc failed for FuzzyMatchBuf");
    }
    buf->buf[buf->count++] = (FuzzyMatch){pi, bi, d};
}

static void fmbuf_free(FuzzyMatchBuf *buf) {
    free(buf->buf);
    buf->buf = NULL;
    buf->count = 0;
    buf->capacity = 0;
}

/* ------------------------------------------------------------------ */
/*  Build-side block index: exact block key -> list of build rows      */
/* ------------------------------------------------------------------ */

typedef struct { int64_t *rows; int64_t n, cap; } BiBucket;

struct BlockIndex {
    int64_t   n_slots;      /* power of two */
    int64_t  *slot_group;   /* -1 = empty, else group id */
    uint64_t *slot_hash;
    BiBucket *groups;
    int64_t  *group_rep;    /* representative build row per group */
    int64_t   n_groups, groups_cap;
    const VecArray *keycol; /* the (resident) build block-key column */
};

static uint64_t fnv1a(const char *s, int64_t n) {
    uint64_t h = 1469598103934665603ULL;
    for (int64_t i = 0; i < n; i++) {
        h ^= (uint8_t)s[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static int bi_key_eq(const struct BlockIndex *bi, int64_t grp,
                     const char *s, int64_t n) {
    int64_t r = bi->group_rep[grp];
    int64_t gl = str_len(bi->keycol, r);
    if (gl != n) return 0;
    return memcmp(str_ptr(bi->keycol, r), s, (size_t)n) == 0;
}

static void bi_bucket_push(BiBucket *b, int64_t row) {
    if (b->n >= b->cap) {
        b->cap = b->cap ? b->cap * 2 : 8;
        b->rows = (int64_t *)realloc(b->rows, (size_t)b->cap * sizeof(int64_t));
        if (!b->rows) vectra_error("realloc failed for block bucket");
    }
    b->rows[b->n++] = row;
}

/* Return the group id for a key, inserting it if absent (used at build time). */
static int64_t bi_find_or_insert(struct BlockIndex *bi, const char *s,
                                  int64_t n, uint64_t h, int64_t rep_row) {
    int64_t mask = bi->n_slots - 1;
    int64_t idx = (int64_t)(h & (uint64_t)mask);
    while (1) {
        int64_t g = bi->slot_group[idx];
        if (g < 0) {
            if (bi->n_groups >= bi->groups_cap) {
                bi->groups_cap *= 2;
                bi->groups = (BiBucket *)realloc(bi->groups,
                    (size_t)bi->groups_cap * sizeof(BiBucket));
                bi->group_rep = (int64_t *)realloc(bi->group_rep,
                    (size_t)bi->groups_cap * sizeof(int64_t));
                if (!bi->groups || !bi->group_rep)
                    vectra_error("realloc failed for block index groups");
                for (int64_t k = bi->n_groups; k < bi->groups_cap; k++)
                    bi->groups[k] = (BiBucket){NULL, 0, 0};
            }
            int64_t ng = bi->n_groups++;
            bi->group_rep[ng] = rep_row;
            bi->slot_group[idx] = ng;
            bi->slot_hash[idx] = h;
            return ng;
        }
        if (bi->slot_hash[idx] == h && bi_key_eq(bi, g, s, n)) return g;
        idx = (idx + 1) & mask;
    }
}

/* Look up a probe key; return group id or -1 if absent (read-only). */
static int64_t bi_lookup(const struct BlockIndex *bi, const char *s,
                         int64_t n, uint64_t h) {
    int64_t mask = bi->n_slots - 1;
    int64_t idx = (int64_t)(h & (uint64_t)mask);
    while (1) {
        int64_t g = bi->slot_group[idx];
        if (g < 0) return -1;
        if (bi->slot_hash[idx] == h && bi_key_eq(bi, g, s, n)) return g;
        idx = (idx + 1) & mask;
    }
}

static struct BlockIndex *block_index_build(const VecArray *keycol,
                                            int64_t nrows) {
    struct BlockIndex *bi =
        (struct BlockIndex *)calloc(1, sizeof(struct BlockIndex));
    if (!bi) vectra_error("alloc failed for BlockIndex");
    bi->keycol = keycol;
    /* Size slots to keep load <= 0.5 (distinct keys <= nrows). */
    bi->n_slots = 16;
    while (bi->n_slots < nrows * 2) bi->n_slots <<= 1;
    bi->slot_group = (int64_t *)malloc((size_t)bi->n_slots * sizeof(int64_t));
    bi->slot_hash = (uint64_t *)calloc((size_t)bi->n_slots, sizeof(uint64_t));
    if (!bi->slot_group || !bi->slot_hash)
        vectra_error("alloc failed for BlockIndex slots");
    for (int64_t i = 0; i < bi->n_slots; i++) bi->slot_group[i] = -1;
    bi->groups_cap = 64;
    bi->groups = (BiBucket *)calloc((size_t)bi->groups_cap, sizeof(BiBucket));
    bi->group_rep = (int64_t *)malloc((size_t)bi->groups_cap * sizeof(int64_t));
    if (!bi->groups || !bi->group_rep)
        vectra_error("alloc failed for BlockIndex groups");

    for (int64_t r = 0; r < nrows; r++) {
        if (!vec_array_is_valid(keycol, r)) continue;  /* NULL block key dropped */
        const char *s = str_ptr(keycol, r);
        int64_t n = str_len(keycol, r);
        uint64_t h = fnv1a(s, n);
        int64_t g = bi_find_or_insert(bi, s, n, h, r);
        bi_bucket_push(&bi->groups[g], r);
    }
    return bi;
}

static void block_index_free(struct BlockIndex *bi) {
    if (!bi) return;
    for (int64_t g = 0; g < bi->n_groups; g++) free(bi->groups[g].rows);
    free(bi->groups);
    free(bi->group_rep);
    free(bi->slot_group);
    free(bi->slot_hash);
    free(bi);
}

/* ------------------------------------------------------------------ */
/*  Match one probe batch against the resident build side              */
/* ------------------------------------------------------------------ */

static int cmp_match_by_probe(const void *a, const void *b) {
    const FuzzyMatch *ma = (const FuzzyMatch *)a;
    const FuzzyMatch *mb = (const FuzzyMatch *)b;
    if (ma->probe_idx < mb->probe_idx) return -1;
    if (ma->probe_idx > mb->probe_idx) return  1;
    if (ma->dist < mb->dist) return -1;   /* lower distance first */
    if (ma->dist > mb->dist) return  1;
    return 0;
}

/* Compute all matches for `batch` into fj->cur_matches / fj->cur_n, ordered by
   (probe local row, distance). probe_idx in each match is the batch-local
   logical row index (mapped to a physical row via the batch selection vector
   at emit time). */
static void fuzzy_match_batch(FuzzyJoinNode *fj, VecBatch *batch) {
    int64_t nlog = vec_batch_logical_rows(batch);
    const VecArray *p_key = &batch->columns[fj->probe_key_col];
    const VecArray *b_key = &fj->b_cols[fj->build_key_col];
    const VecArray *p_block =
        fj->probe_block_col >= 0 ? &batch->columns[fj->probe_block_col] : NULL;

    int n_threads = fj->n_threads;
    if (n_threads < 1) n_threads = 1;
#ifdef _OPENMP
    if (n_threads > omp_get_max_threads()) n_threads = omp_get_max_threads();
#else
    n_threads = 1;
#endif

    FuzzyMatchBuf *tbufs =
        (FuzzyMatchBuf *)calloc((size_t)n_threads, sizeof(FuzzyMatchBuf));
    if (!tbufs) vectra_error("alloc failed for thread match buffers");
    for (int t = 0; t < n_threads; t++) fmbuf_init(&tbufs[t], 1024);

    FuzzyMethod method = fj->method;
    double max_dist = fj->max_dist;
    struct BlockIndex *bidx = fj->bidx;
    int64_t b_nrows = fj->b_nrows;

#ifdef _OPENMP
    #pragma omp parallel for schedule(dynamic, 64) num_threads(n_threads)
#endif
    for (int64_t li = 0; li < nlog; li++) {
#ifdef _OPENMP
        int tid = omp_get_thread_num();
#else
        int tid = 0;
#endif
        FuzzyMatchBuf *buf = &tbufs[tid];

        int64_t phys = vec_batch_physical_row(batch, li);
        if (!vec_array_is_valid(p_key, phys)) continue;
        const char *ps = str_ptr(p_key, phys);
        int64_t pl = str_len(p_key, phys);

        /* Candidate build rows: the block bucket, or all build rows. */
        const int64_t *brows = NULL;
        int64_t bn;
        if (bidx) {
            if (!vec_array_is_valid(p_block, phys)) continue;  /* NULL block */
            const char *bs = str_ptr(p_block, phys);
            int64_t bl = str_len(p_block, phys);
            int64_t g = bi_lookup(bidx, bs, bl, fnv1a(bs, bl));
            if (g < 0) continue;                               /* no such block */
            brows = bidx->groups[g].rows;
            bn = bidx->groups[g].n;
        } else {
            bn = b_nrows;
        }

        for (int64_t j = 0; j < bn; j++) {
            int64_t bi = brows ? brows[j] : j;
            if (!vec_array_is_valid(b_key, bi)) continue;
            double d = compute_dist(method, ps, pl,
                                    str_ptr(b_key, bi), str_len(b_key, bi),
                                    max_dist);
            if (d <= max_dist) fmbuf_push(buf, li, bi, d);
        }
    }

    int64_t total = 0;
    for (int t = 0; t < n_threads; t++) total += tbufs[t].count;

    FuzzyMatch *matches =
        (FuzzyMatch *)malloc((size_t)(total > 0 ? total : 1) * sizeof(FuzzyMatch));
    if (!matches) vectra_error("alloc failed for merged matches");
    int64_t pos = 0;
    for (int t = 0; t < n_threads; t++) {
        if (tbufs[t].count > 0) {
            memcpy(matches + pos, tbufs[t].buf,
                   (size_t)tbufs[t].count * sizeof(FuzzyMatch));
            pos += tbufs[t].count;
        }
        fmbuf_free(&tbufs[t]);
    }
    free(tbufs);

    if (total > 1)
        qsort(matches, (size_t)total, sizeof(FuzzyMatch), cmp_match_by_probe);

    fj->cur_matches = matches;
    fj->cur_n = total;
    fj->emit_pos = 0;
}

/* ------------------------------------------------------------------ */
/*  Output schema                                                      */
/* ------------------------------------------------------------------ */

static VecSchema build_output_schema(FuzzyJoinNode *fj) {
    const VecSchema *pschema = &fj->probe_node->output_schema;
    const VecSchema *bschema = &fj->build_node->output_schema;

    int total = pschema->n_cols + bschema->n_cols + 1;
    char **names = (char **)malloc((size_t)total * sizeof(char *));
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
        if (collision && fj->suffix_y) {
            size_t len = strlen(bname) + strlen(fj->suffix_y) + 1;
            names[idx] = (char *)malloc(len);
            snprintf(names[idx], len, "%s%s", bname, fj->suffix_y);
        } else {
            names[idx] = strdup(bname);
        }
        types[idx] = bschema->col_types[c];
        idx++;
    }
    names[idx] = strdup("fuzzy_dist");
    types[idx] = VEC_DOUBLE;
    idx++;

    fj->out_ncols = idx;
    VecSchema schema = vec_schema_create(idx, names, types);
    for (int i = 0; i < idx; i++) free(names[i]);
    free(names);
    free(types);
    return schema;
}

/* ------------------------------------------------------------------ */
/*  Emit one chunk of the current batch's matches                      */
/* ------------------------------------------------------------------ */

#define EMIT_BATCH_SIZE 8192

static VecBatch *emit_chunk(FuzzyJoinNode *fj) {
    int64_t remaining = fj->cur_n - fj->emit_pos;
    int64_t chunk = remaining < EMIT_BATCH_SIZE ? remaining : EMIT_BATCH_SIZE;
    int total_cols = fj->out_ncols;
    int p_ncols = fj->p_ncols;
    int b_ncols = fj->b_ncols;
    const VecSchema *out = &fj->base.output_schema;
    VecBatch *pbatch = fj->cur_batch;

    VecArrayBuilder *builders =
        (VecArrayBuilder *)calloc((size_t)total_cols, sizeof(VecArrayBuilder));
    if (!builders) vectra_error("alloc failed for output builders");
    for (int c = 0; c < total_cols; c++) {
        builders[c] = vec_builder_init(out->col_types[c]);
        vec_builder_reserve(&builders[c], chunk);
    }

    for (int64_t m = 0; m < chunk; m++) {
        FuzzyMatch *match = &fj->cur_matches[fj->emit_pos + m];
        int64_t p_phys = vec_batch_physical_row(pbatch, match->probe_idx);
        int64_t bi = match->build_idx;
        int col = 0;
        for (int c = 0; c < p_ncols; c++)
            vec_builder_append_one(&builders[col++], &pbatch->columns[c], p_phys);
        for (int c = 0; c < b_ncols; c++)
            vec_builder_append_one(&builders[col++], &fj->b_cols[c], bi);
        VecArrayBuilder *dist_b = &builders[col];
        if (dist_b->length >= dist_b->capacity) vec_builder_reserve(dist_b, 1);
        dist_b->buf.dbl[dist_b->length] = match->dist;
        dist_b->validity[dist_b->length / 8] |=
            (uint8_t)(1 << (dist_b->length % 8));
        dist_b->length++;
    }

    fj->emit_pos += chunk;

    VecBatch *batch = vec_batch_alloc(total_cols, chunk);
    for (int c = 0; c < total_cols; c++)
        batch->columns[c] = vec_builder_finish(&builders[c]);
    for (int c = 0; c < total_cols; c++) {
        free(batch->col_names[c]);
        batch->col_names[c] = strdup(out->col_names[c]);
    }
    batch->n_rows = chunk;
    free(builders);
    return batch;
}

/* ------------------------------------------------------------------ */
/*  next_batch                                                         */
/* ------------------------------------------------------------------ */

static VecBatch *fuzzy_join_next_batch(VecNode *self) {
    FuzzyJoinNode *fj = (FuzzyJoinNode *)self;

    if (fj->state == FSTATE_BUILD) {
        join_materialize_side(fj->build_node,
                              fj->build_node->output_schema.n_cols,
                              &fj->b_cols, &fj->b_nrows);
        fj->b_ncols = fj->build_node->output_schema.n_cols;
        fj->p_ncols = fj->probe_node->output_schema.n_cols;
        if (fj->build_block_col >= 0)
            fj->bidx = block_index_build(&fj->b_cols[fj->build_block_col],
                                         fj->b_nrows);
        fj->state = FSTATE_STREAM;
    }

    if (fj->state == FSTATE_DONE) return NULL;

    while (1) {
        /* Drain the current batch's matches in EMIT_BATCH_SIZE chunks. */
        if (fj->cur_matches && fj->emit_pos < fj->cur_n)
            return emit_chunk(fj);

        /* Current batch exhausted: release it and pull the next probe batch. */
        if (fj->cur_batch) { vec_batch_free(fj->cur_batch); fj->cur_batch = NULL; }
        free(fj->cur_matches);
        fj->cur_matches = NULL;
        fj->cur_n = 0;
        fj->emit_pos = 0;

        VecBatch *pb = fj->probe_node->next_batch(fj->probe_node);
        if (!pb) { fj->state = FSTATE_DONE; return NULL; }
        fj->cur_batch = pb;
        fuzzy_match_batch(fj, pb);
        /* Loop: emit this batch's matches, or (if it produced none) advance. */
    }
}

/* ------------------------------------------------------------------ */
/*  Cleanup + constructor                                              */
/* ------------------------------------------------------------------ */

static void fuzzy_join_free(VecNode *self) {
    FuzzyJoinNode *fj = (FuzzyJoinNode *)self;
    if (fj->probe_node) fj->probe_node->free_node(fj->probe_node);
    if (fj->build_node) fj->build_node->free_node(fj->build_node);
    if (fj->b_cols) {
        for (int c = 0; c < fj->b_ncols; c++) vec_array_free(&fj->b_cols[c]);
        free(fj->b_cols);
    }
    block_index_free(fj->bidx);
    if (fj->cur_batch) vec_batch_free(fj->cur_batch);
    free(fj->cur_matches);
    free(fj->suffix_y);
    vec_schema_free(&fj->base.output_schema);
    free(fj);
}

FuzzyJoinNode *fuzzy_join_node_create(
    VecNode     *probe,
    VecNode     *build,
    int          probe_key_col,
    int          build_key_col,
    int          probe_block_col,
    int          build_block_col,
    FuzzyMethod  method,
    double       max_dist,
    int          n_threads,
    const char  *suffix_y)
{
    FuzzyJoinNode *fj = (FuzzyJoinNode *)calloc(1, sizeof(FuzzyJoinNode));
    if (!fj) vectra_error("alloc failed for FuzzyJoinNode");

    fj->probe_node = probe;
    fj->build_node = build;
    fj->probe_key_col = probe_key_col;
    fj->build_key_col = build_key_col;
    fj->probe_block_col = probe_block_col;
    fj->build_block_col = build_block_col;
    fj->method = method;
    fj->max_dist = max_dist;
    fj->n_threads = n_threads;
    fj->suffix_y = suffix_y ? strdup(suffix_y) : strdup(".y");
    fj->state = FSTATE_BUILD;

    fj->base.output_schema = build_output_schema(fj);
    fj->base.next_batch = fuzzy_join_next_batch;
    fj->base.kind = "FuzzyJoinNode";
    fj->base.free_node = fuzzy_join_free;

    return fj;
}
