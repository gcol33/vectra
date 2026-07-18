#include "fuzzy_join.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "builder.h"
#include "error.h"
#include "string_distance.h"
#include "join_partition.h"   /* join_materialize_side (resident build side) */
#include "vtr1_tdc.h"         /* build-side spill run file */
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>

#include "vec_omp.h"

#define FUZZY_MEM_DEFAULT (1LL << 30)   /* 1 GiB when no budget is given */

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

/* Returns 1 on allocation failure so a caller inside a parallel region can
   latch it and raise on the master thread rather than longjmp off a worker. */
static int fmbuf_push(FuzzyMatchBuf *buf, int64_t pi, int64_t bi, double d) {
    if (buf->count >= buf->capacity) {
        int64_t new_cap = buf->capacity * 2;
        FuzzyMatch *nb = (FuzzyMatch *)realloc(buf->buf,
            (size_t)new_cap * sizeof(FuzzyMatch));
        if (!nb) return 1;
        buf->buf = nb;
        buf->capacity = new_cap;
    }
    buf->buf[buf->count++] = (FuzzyMatch){pi, bi, d};
    return 0;
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
    return n == 0 || memcmp(str_ptr(bi->keycol, r), s, (size_t)n) == 0;
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

/* Compute all matches for `batch` against a build side (bcols / bnrows / bidx)
   into fj->cur_matches / fj->cur_n, ordered by (probe local row, distance).
   probe_idx in each match is the batch-local logical row index (mapped to a
   physical row via the batch selection vector at emit time); build_idx indexes
   bcols (the resident build side, or one spilled rowgroup chunk). */
static void fuzzy_match_batch_vs(FuzzyJoinNode *fj, VecBatch *batch,
                                 const VecArray *bcols, int64_t bnrows,
                                 struct BlockIndex *bidx) {
    int64_t nlog = vec_batch_logical_rows(batch);
    const VecArray *p_key = &batch->columns[fj->probe_key_col];
    const VecArray *b_key = &bcols[fj->build_key_col];
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
    int64_t b_nrows = bnrows;
    volatile int oom = 0;

#ifdef _OPENMP
    #pragma omp parallel for schedule(dynamic, 64) num_threads(n_threads)
#endif
    for (int64_t li = 0; li < nlog; li++) {
#ifdef _OPENMP
        int tid = omp_get_thread_num();
#else
        int tid = 0;
#endif
        if (oom) continue;
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
            if (d <= max_dist && fmbuf_push(buf, li, bi, d)) {
                #pragma omp atomic write
                oom = 1;
                break;
            }
        }
    }

    if (oom) {
        for (int t = 0; t < n_threads; t++) fmbuf_free(&tbufs[t]);
        free(tbufs);
        vectra_error("alloc failed for fuzzy-join matches");
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
            vec_builder_append_one(&builders[col++], &fj->emit_bcols[c], bi);
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
/*  Build phase: materialize the build side, spilling if it overflows  */
/* ------------------------------------------------------------------ */

static int64_t builders_bytes(const VecArrayBuilder *b, int n) {
    int64_t t = 0;
    for (int c = 0; c < n; c++) {
        int e = vec_type_elem_size(b[c].type);
        if (e > 0) t += b[c].length * e;
        else       t += b[c].str_data_len + b[c].length * 8;  /* + offsets */
    }
    return t;
}

static char *fuzzy_spill_path(const char *temp_dir) {
    static int counter = 0;
    int id = counter++;
    int len = snprintf(NULL, 0, "%s/vectra_fuzzy_%d.vtr", temp_dir, id);
    char *p = (char *)malloc((size_t)(len + 1));
    if (!p) vectra_error("fuzzy_join: alloc failed");
    snprintf(p, (size_t)(len + 1), "%s/vectra_fuzzy_%d.vtr", temp_dir, id);
    return p;
}

/* Finish the accumulated builders into one rowgroup, write it, re-init them. */
static void fuzzy_flush_rowgroup(VecArrayBuilder *builders, int n,
                                 const VecSchema *schema, Vtr1TdcWriter *w) {
    int64_t nr = builders[0].length;
    if (nr == 0) return;
    VecBatch *b = vec_batch_alloc(n, nr);
    for (int c = 0; c < n; c++) {
        b->columns[c] = vec_builder_finish(&builders[c]);
        free(b->col_names[c]);
        b->col_names[c] = strdup(schema->col_names[c]);
    }
    vtr1_write_rowgroup_tdc(w, b, VTR_COMPRESS_FAST, NULL, NULL);
    vec_batch_free(b);
    for (int c = 0; c < n; c++) builders[c] = vec_builder_init(schema->col_types[c]);
}

static void fuzzy_build_phase(FuzzyJoinNode *fj) {
    const VecSchema *bschema = &fj->build_node->output_schema;
    int nc = bschema->n_cols;
    fj->b_ncols = nc;
    fj->p_ncols = fj->probe_node->output_schema.n_cols;

    int64_t budget = fj->mem_budget > 0 ? fj->mem_budget : FUZZY_MEM_DEFAULT;

    VecArrayBuilder *builders =
        (VecArrayBuilder *)malloc((size_t)nc * sizeof(VecArrayBuilder));
    for (int c = 0; c < nc; c++) builders[c] = vec_builder_init(bschema->col_types[c]);

    Vtr1TdcWriter *w = NULL;
    int spilled = 0;

    VecBatch *bb;
    while ((bb = fj->build_node->next_batch(fj->build_node)) != NULL) {
        if (!bb->sel) {
            for (int c = 0; c < nc; c++)
                vec_builder_append_array(&builders[c], &bb->columns[c]);
        } else {
            int64_t nl = vec_batch_logical_rows(bb);
            for (int64_t li = 0; li < nl; li++) {
                int64_t pi = vec_batch_physical_row(bb, li);
                for (int c = 0; c < nc; c++)
                    vec_builder_append_one(&builders[c], &bb->columns[c], pi);
            }
        }
        vec_batch_free(bb);

        if (builders_bytes(builders, nc) >= budget) {
            if (!spilled) {
                spilled = 1;
                fj->build_spill_path = fuzzy_spill_path(fj->temp_dir);
                w = vtr1_open_tdc_writer(fj->build_spill_path, bschema);
            }
            fuzzy_flush_rowgroup(builders, nc, bschema, w);
        }
    }

    if (spilled) {
        fuzzy_flush_rowgroup(builders, nc, bschema, w);   /* residual (may be 0) */
        for (int c = 0; c < nc; c++) vec_builder_free(&builders[c]);
        free(builders);
        vtr1_close_tdc_writer(w);
        fj->spilled = 1;
        fj->build_file = vtr1_open_tdc(fj->build_spill_path);
        if (!fj->build_file)
            vectra_error("fuzzy_join: cannot reopen build spill %s",
                         fj->build_spill_path);
        fj->build_n_rgs = vtr1_tdc_n_rowgroups((Vtr1TdcFile *)fj->build_file);
    } else {
        fj->b_nrows = builders[0].length;
        fj->b_cols = (VecArray *)malloc((size_t)nc * sizeof(VecArray));
        for (int c = 0; c < nc; c++) fj->b_cols[c] = vec_builder_finish(&builders[c]);
        free(builders);
        if (fj->build_block_col >= 0)
            fj->bidx = block_index_build(&fj->b_cols[fj->build_block_col], fj->b_nrows);
        fj->emit_bcols = fj->b_cols;
    }
}

/* Load one build rowgroup as the resident chunk (+ its block index). */
static void fuzzy_load_chunk(FuzzyJoinNode *fj, uint32_t rg) {
    int nc = fj->b_ncols;
    int *mask = (int *)malloc((size_t)nc * sizeof(int));
    for (int c = 0; c < nc; c++) mask[c] = 1;
    fj->chunk_batch = vtr1_read_rowgroup_tdc((Vtr1TdcFile *)fj->build_file, rg, mask);
    free(mask);
    fj->chunk_cols = fj->chunk_batch->columns;
    fj->chunk_nrows = fj->chunk_batch->n_rows;
    fj->chunk_bidx = (fj->build_block_col >= 0)
        ? block_index_build(&fj->chunk_cols[fj->build_block_col], fj->chunk_nrows)
        : NULL;
}

static void fuzzy_free_chunk(FuzzyJoinNode *fj) {
    if (fj->chunk_bidx) { block_index_free(fj->chunk_bidx); fj->chunk_bidx = NULL; }
    if (fj->chunk_batch) { vec_batch_free(fj->chunk_batch); fj->chunk_batch = NULL; }
    fj->chunk_cols = NULL;
    fj->chunk_nrows = 0;
}

/* ------------------------------------------------------------------ */
/*  next_batch                                                         */
/* ------------------------------------------------------------------ */

static VecBatch *fuzzy_join_next_batch(VecNode *self) {
    FuzzyJoinNode *fj = (FuzzyJoinNode *)self;

    if (fj->state == FSTATE_BUILD) {
        fuzzy_build_phase(fj);
        fj->state = FSTATE_STREAM;
    }
    if (fj->state == FSTATE_DONE) return NULL;

    if (!fj->spilled) {
        /* Resident build: one match pass per probe batch. */
        while (1) {
            if (fj->cur_matches && fj->emit_pos < fj->cur_n)
                return emit_chunk(fj);
            if (fj->cur_batch) { vec_batch_free(fj->cur_batch); fj->cur_batch = NULL; }
            free(fj->cur_matches);
            fj->cur_matches = NULL; fj->cur_n = 0; fj->emit_pos = 0;

            VecBatch *pb = fj->probe_node->next_batch(fj->probe_node);
            if (!pb) { fj->state = FSTATE_DONE; return NULL; }
            fj->cur_batch = pb;
            fj->emit_bcols = fj->b_cols;
            fuzzy_match_batch_vs(fj, pb, fj->b_cols, fj->b_nrows, fj->bidx);
        }
    }

    /* Spilled build: match each probe batch against one rowgroup chunk at a
       time. cur_matches index the current chunk (emit_bcols = chunk_cols), so a
       chunk is freed only after its matches have been drained. */
    while (1) {
        if (fj->cur_matches && fj->emit_pos < fj->cur_n)
            return emit_chunk(fj);
        free(fj->cur_matches);
        fj->cur_matches = NULL; fj->cur_n = 0; fj->emit_pos = 0;

        if (fj->cur_batch && fj->chunk_rg < fj->build_n_rgs) {
            fuzzy_free_chunk(fj);
            fuzzy_load_chunk(fj, fj->chunk_rg++);
            fj->emit_bcols = fj->chunk_cols;
            fuzzy_match_batch_vs(fj, fj->cur_batch, fj->chunk_cols,
                                 fj->chunk_nrows, fj->chunk_bidx);
            continue;
        }

        /* Current probe batch matched against every chunk; pull the next one. */
        fuzzy_free_chunk(fj);
        if (fj->cur_batch) { vec_batch_free(fj->cur_batch); fj->cur_batch = NULL; }
        VecBatch *pb = fj->probe_node->next_batch(fj->probe_node);
        if (!pb) { fj->state = FSTATE_DONE; return NULL; }
        fj->cur_batch = pb;
        fj->chunk_rg = 0;
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
    fuzzy_free_chunk(fj);
    if (fj->build_file) vtr1_close_tdc((Vtr1TdcFile *)fj->build_file);
    if (fj->build_spill_path) { remove(fj->build_spill_path); free(fj->build_spill_path); }
    free(fj->temp_dir);
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
    const char  *suffix_y,
    int64_t      mem_budget,
    const char  *temp_dir)
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
    fj->mem_budget = mem_budget;
    fj->temp_dir = temp_dir ? strdup(temp_dir) : NULL;
    fj->state = FSTATE_BUILD;

    fj->base.output_schema = build_output_schema(fj);
    fj->base.next_batch = fuzzy_join_next_batch;
    fj->base.kind = "FuzzyJoinNode";
    fj->base.free_node = fuzzy_join_free;

    return fj;
}
