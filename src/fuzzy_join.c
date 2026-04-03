#include "fuzzy_join.h"
#include "hash.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "builder.h"
#include "error.h"
#include "string_distance.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

#ifdef _OPENMP
#include <omp.h>
#endif

/* Thin wrappers around shared implementations in string_distance.h,
   preserving the call-site names used throughout this file. */
static int64_t fj_levenshtein(const char *s, int64_t len_s,
                               const char *t, int64_t len_t,
                               int64_t max_dist) {
    return strdist_levenshtein(s, len_s, t, len_t, max_dist);
}

static int64_t fj_dl_distance(const char *s, int64_t len_s,
                               const char *t, int64_t len_t,
                               int64_t max_dist) {
    return strdist_dl(s, len_s, t, len_t, max_dist);
}

static double fj_jaro_winkler(const char *s, int64_t len_s,
                               const char *t, int64_t len_t) {
    return strdist_jaro_winkler(s, len_s, t, len_t);
}

/* ------------------------------------------------------------------ */
/*  String access helpers for materialized VecArrays                   */
/* ------------------------------------------------------------------ */

static inline const char *str_ptr(const VecArray *arr, int64_t row) {
    return arr->buf.str.data + arr->buf.str.offsets[row];
}

static inline int64_t str_len(const VecArray *arr, int64_t row) {
    return arr->buf.str.offsets[row + 1] - arr->buf.str.offsets[row];
}

/* ------------------------------------------------------------------ */
/*  Compute normalized distance between two strings                    */
/* ------------------------------------------------------------------ */

static inline double compute_dist(FuzzyMethod method,
                                   const char *a, int64_t la,
                                   const char *b, int64_t lb,
                                   double max_dist) {
    if (method == FUZZY_JW) {
        double sim = fj_jaro_winkler(a, la, b, lb);
        return 1.0 - sim;  /* convert similarity to distance */
    }

    int64_t max_len = la > lb ? la : lb;
    if (max_len == 0) return 0.0;

    /* Convert normalized threshold to max raw edits for early termination */
    int64_t max_raw = (int64_t)ceil(max_dist * (double)max_len);

    int64_t raw;
    if (method == FUZZY_DL) {
        raw = fj_dl_distance(a, la, b, lb, max_raw);
    } else {
        raw = fj_levenshtein(a, la, b, lb, max_raw);
    }

    if (raw > max_raw) return max_dist + 1.0;  /* exceeded threshold */
    return (double)raw / (double)max_len;
}

/* ------------------------------------------------------------------ */
/*  FuzzyMatchBuf helpers                                              */
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
/*  Partition helpers                                                   */
/* ------------------------------------------------------------------ */

static void partition_push(FuzzyPartition *p, int64_t row) {
    if (p->n_rows >= p->capacity) {
        p->capacity = p->capacity ? p->capacity * 2 : 64;
        p->rows = (int64_t *)realloc(p->rows,
            (size_t)p->capacity * sizeof(int64_t));
        if (!p->rows) vectra_error("realloc failed for FuzzyPartition");
    }
    p->rows[p->n_rows++] = row;
}

static void partition_free(FuzzyPartition *p) {
    free(p->rows);
    p->rows = NULL;
    p->n_rows = 0;
    p->capacity = 0;
}

/* ------------------------------------------------------------------ */
/*  Materialize a child node into flat VecArrays                       */
/* ------------------------------------------------------------------ */

static void materialize_side(VecNode *child, int ncols,
                             VecArray **out_cols, int64_t *out_nrows) {
    VecArrayBuilder *builders = (VecArrayBuilder *)calloc(
        (size_t)ncols, sizeof(VecArrayBuilder));
    if (!builders) vectra_error("alloc failed in fuzzy_join materialize");

    const VecSchema *schema = &child->output_schema;
    for (int c = 0; c < ncols; c++)
        builders[c] = vec_builder_init(schema->col_types[c]);

    VecBatch *batch;
    while ((batch = child->next_batch(child)) != NULL) {
        if (!batch->sel) {
            for (int c = 0; c < ncols; c++)
                vec_builder_append_array(&builders[c], &batch->columns[c]);
        } else {
            int64_t n_logical = vec_batch_logical_rows(batch);
            for (int c = 0; c < ncols; c++)
                vec_builder_reserve(&builders[c], n_logical);
            for (int64_t li = 0; li < n_logical; li++) {
                int64_t pi = vec_batch_physical_row(batch, li);
                for (int c = 0; c < ncols; c++)
                    vec_builder_append_one(&builders[c],
                                           &batch->columns[c], pi);
            }
        }
        vec_batch_free(batch);
    }

    int64_t nrows = builders[0].length;
    VecArray *cols = (VecArray *)malloc((size_t)ncols * sizeof(VecArray));
    if (!cols) vectra_error("alloc failed in fuzzy_join materialize");
    for (int c = 0; c < ncols; c++)
        cols[c] = vec_builder_finish(&builders[c]);
    free(builders);

    *out_cols = cols;
    *out_nrows = nrows;
}

/* ------------------------------------------------------------------ */
/*  Build partitions by blocking key (hash-based grouping)             */
/* ------------------------------------------------------------------ */

/* FNV-1a for a string */
static uint64_t hash_string(const char *s, int64_t len) {
    uint64_t h = 14695981039346656037ULL;
    for (int64_t i = 0; i < len; i++)
        h = (h ^ (uint8_t)s[i]) * 1099511628211ULL;
    return h;
}

/* Hash table for mapping block key strings to partition IDs */
typedef struct {
    int64_t   n_slots;
    int64_t  *part_id;      /* partition ID at slot, or -1 */
    uint64_t *slot_hash;    /* hash at slot */
    /* Key storage: pointer + length into the source VecArray */
    const char **slot_key;
    int64_t    *slot_key_len;
    int64_t     n_parts;    /* number of unique partitions so far */
} BlockHT;

static BlockHT block_ht_create(int64_t estimated_groups) {
    BlockHT ht;
    int64_t n_slots = 64;
    while (n_slots < estimated_groups * 2) n_slots *= 2;
    ht.n_slots = n_slots;
    ht.part_id = (int64_t *)malloc((size_t)n_slots * sizeof(int64_t));
    ht.slot_hash = (uint64_t *)malloc((size_t)n_slots * sizeof(uint64_t));
    ht.slot_key = (const char **)calloc((size_t)n_slots, sizeof(const char *));
    ht.slot_key_len = (int64_t *)calloc((size_t)n_slots, sizeof(int64_t));
    ht.n_parts = 0;
    if (!ht.part_id || !ht.slot_hash)
        vectra_error("alloc failed for BlockHT");
    memset(ht.part_id, -1, (size_t)n_slots * sizeof(int64_t));
    return ht;
}

static void block_ht_free(BlockHT *ht) {
    free(ht->part_id);
    free(ht->slot_hash);
    free((void *)ht->slot_key);
    free(ht->slot_key_len);
}

/* Find or insert a block key, return partition ID */
static int64_t block_ht_find_or_insert(BlockHT *ht,
                                        const char *key, int64_t key_len,
                                        uint64_t hash) {
    int64_t mask = ht->n_slots - 1;
    int64_t slot = (int64_t)(hash & (uint64_t)mask);
    for (;;) {
        if (ht->part_id[slot] == -1) {
            /* New partition */
            int64_t pid = ht->n_parts++;
            ht->part_id[slot] = pid;
            ht->slot_hash[slot] = hash;
            ht->slot_key[slot] = key;
            ht->slot_key_len[slot] = key_len;
            return pid;
        }
        if (ht->slot_hash[slot] == hash &&
            ht->slot_key_len[slot] == key_len &&
            memcmp(ht->slot_key[slot], key, (size_t)key_len) == 0) {
            return ht->part_id[slot];
        }
        slot = (slot + 1) & mask;
    }
}

static void build_partitions(FuzzyJoinNode *fj) {
    int has_block = (fj->probe_block_col >= 0 && fj->build_block_col >= 0);

    if (!has_block) {
        /* No blocking: single partition with all rows */
        fj->n_parts = 1;
        fj->probe_parts = (FuzzyPartition *)calloc(1, sizeof(FuzzyPartition));
        fj->build_parts = (FuzzyPartition *)calloc(1, sizeof(FuzzyPartition));

        fj->probe_parts[0].rows = (int64_t *)malloc(
            (size_t)fj->p_nrows * sizeof(int64_t));
        fj->probe_parts[0].n_rows = fj->p_nrows;
        fj->probe_parts[0].capacity = fj->p_nrows;
        for (int64_t i = 0; i < fj->p_nrows; i++)
            fj->probe_parts[0].rows[i] = i;

        fj->build_parts[0].rows = (int64_t *)malloc(
            (size_t)fj->b_nrows * sizeof(int64_t));
        fj->build_parts[0].n_rows = fj->b_nrows;
        fj->build_parts[0].capacity = fj->b_nrows;
        for (int64_t i = 0; i < fj->b_nrows; i++)
            fj->build_parts[0].rows[i] = i;
        return;
    }

    /* Estimate partition count from build side */
    int64_t est_parts = fj->b_nrows / 50;
    if (est_parts < 64) est_parts = 64;
    BlockHT ht = block_ht_create(est_parts);

    /* Capacity for partitions — will grow via realloc */
    int64_t parts_cap = est_parts;
    FuzzyPartition *b_parts = (FuzzyPartition *)calloc(
        (size_t)parts_cap, sizeof(FuzzyPartition));
    if (!b_parts) vectra_error("alloc failed for build partitions");

    /* Partition build side */
    const VecArray *b_block = &fj->b_cols[fj->build_block_col];
    for (int64_t r = 0; r < fj->b_nrows; r++) {
        if (!vec_array_is_valid(b_block, r)) continue;
        const char *key = str_ptr(b_block, r);
        int64_t klen = str_len(b_block, r);
        uint64_t h = hash_string(key, klen);
        int64_t pid = block_ht_find_or_insert(&ht, key, klen, h);

        /* Grow partition array if needed */
        if (pid >= parts_cap) {
            int64_t new_cap = parts_cap * 2;
            while (pid >= new_cap) new_cap *= 2;
            b_parts = (FuzzyPartition *)realloc(b_parts,
                (size_t)new_cap * sizeof(FuzzyPartition));
            if (!b_parts) vectra_error("realloc failed for build partitions");
            memset(b_parts + parts_cap, 0,
                   (size_t)(new_cap - parts_cap) * sizeof(FuzzyPartition));
            parts_cap = new_cap;
        }
        partition_push(&b_parts[pid], r);
    }

    int64_t n_parts = ht.n_parts;

    /* Allocate probe partitions (same count) */
    FuzzyPartition *p_parts = (FuzzyPartition *)calloc(
        (size_t)n_parts, sizeof(FuzzyPartition));
    if (!p_parts) vectra_error("alloc failed for probe partitions");

    /* Partition probe side using same hash table */
    const VecArray *p_block = &fj->p_cols[fj->probe_block_col];
    for (int64_t r = 0; r < fj->p_nrows; r++) {
        if (!vec_array_is_valid(p_block, r)) continue;
        const char *key = str_ptr(p_block, r);
        int64_t klen = str_len(p_block, r);
        uint64_t h = hash_string(key, klen);

        /* Look up only — don't create new partitions for probe keys
           absent from build (they can't match anything) */
        int64_t mask = ht.n_slots - 1;
        int64_t slot = (int64_t)(h & (uint64_t)mask);
        int64_t pid = -1;
        for (;;) {
            if (ht.part_id[slot] == -1) break;  /* not found */
            if (ht.slot_hash[slot] == h &&
                ht.slot_key_len[slot] == klen &&
                memcmp(ht.slot_key[slot], key, (size_t)klen) == 0) {
                pid = ht.part_id[slot];
                break;
            }
            slot = (slot + 1) & mask;
        }
        if (pid >= 0) {
            partition_push(&p_parts[pid], r);
        }
    }

    block_ht_free(&ht);

    fj->n_parts = n_parts;
    fj->probe_parts = p_parts;
    fj->build_parts = b_parts;
}

/* ------------------------------------------------------------------ */
/*  Parallel match phase                                               */
/* ------------------------------------------------------------------ */

static void fuzzy_match_phase(FuzzyJoinNode *fj) {
    int n_threads = fj->n_threads;
    if (n_threads < 1) n_threads = 1;

#ifndef _OPENMP
    n_threads = 1;
#endif

    const VecArray *p_key = &fj->p_cols[fj->probe_key_col];
    const VecArray *b_key = &fj->b_cols[fj->build_key_col];

    /* Allocate per-thread match buffers */
    FuzzyMatchBuf *tbufs = (FuzzyMatchBuf *)calloc(
        (size_t)n_threads, sizeof(FuzzyMatchBuf));
    if (!tbufs) vectra_error("alloc failed for thread match buffers");
    for (int t = 0; t < n_threads; t++)
        fmbuf_init(&tbufs[t], 4096);

    FuzzyMethod method = fj->method;
    double max_dist = fj->max_dist;

#ifdef _OPENMP
    #pragma omp parallel for schedule(dynamic, 1) num_threads(n_threads)
#endif
    for (int64_t p = 0; p < fj->n_parts; p++) {
        FuzzyPartition *pp = &fj->probe_parts[p];
        FuzzyPartition *bp = &fj->build_parts[p];
        if (pp->n_rows == 0 || bp->n_rows == 0) continue;

#ifdef _OPENMP
        int tid = omp_get_thread_num();
#else
        int tid = 0;
#endif
        FuzzyMatchBuf *buf = &tbufs[tid];

        for (int64_t i = 0; i < pp->n_rows; i++) {
            int64_t pi = pp->rows[i];
            if (!vec_array_is_valid(p_key, pi)) continue;
            const char *ps = str_ptr(p_key, pi);
            int64_t pl = str_len(p_key, pi);

            for (int64_t j = 0; j < bp->n_rows; j++) {
                int64_t bi = bp->rows[j];
                if (!vec_array_is_valid(b_key, bi)) continue;
                const char *bs = str_ptr(b_key, bi);
                int64_t bl = str_len(b_key, bi);

                double d = compute_dist(method, ps, pl, bs, bl, max_dist);
                if (d <= max_dist) {
                    fmbuf_push(buf, pi, bi, d);
                }
            }
        }
    }

    /* Merge thread buffers */
    int64_t total = 0;
    for (int t = 0; t < n_threads; t++)
        total += tbufs[t].count;

    fj->matches = (FuzzyMatch *)malloc(
        (size_t)(total > 0 ? total : 1) * sizeof(FuzzyMatch));
    if (!fj->matches) vectra_error("alloc failed for merged matches");
    fj->n_matches = total;

    int64_t pos = 0;
    for (int t = 0; t < n_threads; t++) {
        if (tbufs[t].count > 0) {
            memcpy(fj->matches + pos, tbufs[t].buf,
                   (size_t)tbufs[t].count * sizeof(FuzzyMatch));
            pos += tbufs[t].count;
        }
        fmbuf_free(&tbufs[t]);
    }
    free(tbufs);
}

/* ------------------------------------------------------------------ */
/*  Sort matches by probe_idx for stable output order                  */
/* ------------------------------------------------------------------ */

static int cmp_match_by_probe(const void *a, const void *b) {
    const FuzzyMatch *ma = (const FuzzyMatch *)a;
    const FuzzyMatch *mb = (const FuzzyMatch *)b;
    if (ma->probe_idx < mb->probe_idx) return -1;
    if (ma->probe_idx > mb->probe_idx) return  1;
    /* Secondary: lower distance first */
    if (ma->dist < mb->dist) return -1;
    if (ma->dist > mb->dist) return  1;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Output schema construction                                         */
/* ------------------------------------------------------------------ */

static VecSchema build_output_schema(FuzzyJoinNode *fj) {
    const VecSchema *pschema = &fj->probe_node->output_schema;
    const VecSchema *bschema = &fj->build_node->output_schema;

    /* Output: all probe cols + all build cols + fuzzy_dist */
    int total = pschema->n_cols + bschema->n_cols + 1;
    char **names = (char **)malloc((size_t)total * sizeof(char *));
    VecType *types = (VecType *)malloc((size_t)total * sizeof(VecType));
    if (!names || !types) vectra_error("alloc failed for output schema");

    int idx = 0;

    /* Probe columns */
    for (int c = 0; c < pschema->n_cols; c++) {
        names[idx] = strdup(pschema->col_names[c]);
        types[idx] = pschema->col_types[c];
        idx++;
    }

    /* Build columns (add suffix on collision) */
    for (int c = 0; c < bschema->n_cols; c++) {
        const char *bname = bschema->col_names[c];
        /* Check for collision with probe columns */
        int collision = 0;
        for (int pc = 0; pc < pschema->n_cols; pc++) {
            if (strcmp(pschema->col_names[pc], bname) == 0) {
                collision = 1;
                break;
            }
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

    /* fuzzy_dist column */
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
/*  next_batch: emit matches in batches                                */
/* ------------------------------------------------------------------ */

#define EMIT_BATCH_SIZE 8192

static VecBatch *fuzzy_join_next_batch(VecNode *self) {
    FuzzyJoinNode *fj = (FuzzyJoinNode *)self;

    if (fj->state == FSTATE_MATERIALIZE) {
        /* Materialize both sides */
        materialize_side(fj->probe_node,
                         fj->probe_node->output_schema.n_cols,
                         &fj->p_cols, &fj->p_nrows);
        fj->p_ncols = fj->probe_node->output_schema.n_cols;

        materialize_side(fj->build_node,
                         fj->build_node->output_schema.n_cols,
                         &fj->b_cols, &fj->b_nrows);
        fj->b_ncols = fj->build_node->output_schema.n_cols;

        /* Partition by block key */
        build_partitions(fj);

        /* Parallel fuzzy match */
        fuzzy_match_phase(fj);

        /* Sort by probe_idx for deterministic output */
        if (fj->n_matches > 1) {
            qsort(fj->matches, (size_t)fj->n_matches,
                  sizeof(FuzzyMatch), cmp_match_by_probe);
        }

        fj->emit_pos = 0;
        fj->state = FSTATE_EMIT;
    }

    if (fj->state == FSTATE_DONE) return NULL;

    /* Emit a batch of matches */
    int64_t remaining = fj->n_matches - fj->emit_pos;
    if (remaining <= 0) {
        fj->state = FSTATE_DONE;
        return NULL;
    }

    int64_t batch_size = remaining < EMIT_BATCH_SIZE ? remaining : EMIT_BATCH_SIZE;
    int total_cols = fj->out_ncols;

    VecArrayBuilder *builders = (VecArrayBuilder *)calloc(
        (size_t)total_cols, sizeof(VecArrayBuilder));
    if (!builders) vectra_error("alloc failed for output builders");

    const VecSchema *out = &fj->base.output_schema;
    for (int c = 0; c < total_cols; c++) {
        builders[c] = vec_builder_init(out->col_types[c]);
        vec_builder_reserve(&builders[c], batch_size);
    }

    int p_ncols = fj->p_ncols;
    int b_ncols = fj->b_ncols;

    for (int64_t m = 0; m < batch_size; m++) {
        FuzzyMatch *match = &fj->matches[fj->emit_pos + m];
        int64_t pi = match->probe_idx;
        int64_t bi = match->build_idx;
        int col = 0;

        /* Probe columns */
        for (int c = 0; c < p_ncols; c++) {
            vec_builder_append_one(&builders[col++], &fj->p_cols[c], pi);
        }
        /* Build columns */
        for (int c = 0; c < b_ncols; c++) {
            vec_builder_append_one(&builders[col++], &fj->b_cols[c], bi);
        }
        /* fuzzy_dist */
        VecArrayBuilder *dist_b = &builders[col];
        if (dist_b->length >= dist_b->capacity) {
            vec_builder_reserve(dist_b, 1);
        }
        dist_b->buf.dbl[dist_b->length] = match->dist;
        dist_b->validity[dist_b->length / 8] |=
            (uint8_t)(1 << (dist_b->length % 8));
        dist_b->length++;
    }

    fj->emit_pos += batch_size;

    /* Build output batch */
    VecBatch *batch = vec_batch_alloc(total_cols, batch_size);
    for (int c = 0; c < total_cols; c++) {
        batch->columns[c] = vec_builder_finish(&builders[c]);
    }
    /* Set col_names from schema */
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

static void fuzzy_join_free(VecNode *self) {
    FuzzyJoinNode *fj = (FuzzyJoinNode *)self;

    if (fj->probe_node) fj->probe_node->free_node(fj->probe_node);
    if (fj->build_node) fj->build_node->free_node(fj->build_node);

    /* Free materialized arrays */
    if (fj->p_cols) {
        for (int c = 0; c < fj->p_ncols; c++)
            vec_array_free(&fj->p_cols[c]);
        free(fj->p_cols);
    }
    if (fj->b_cols) {
        for (int c = 0; c < fj->b_ncols; c++)
            vec_array_free(&fj->b_cols[c]);
        free(fj->b_cols);
    }

    /* Free partitions */
    if (fj->probe_parts) {
        for (int64_t i = 0; i < fj->n_parts; i++)
            partition_free(&fj->probe_parts[i]);
        free(fj->probe_parts);
    }
    if (fj->build_parts) {
        for (int64_t i = 0; i < fj->n_parts; i++)
            partition_free(&fj->build_parts[i]);
        free(fj->build_parts);
    }

    free(fj->matches);
    free(fj->suffix_y);
    vec_schema_free(&fj->base.output_schema);
    free(fj);
}

/* ------------------------------------------------------------------ */
/*  Constructor                                                        */
/* ------------------------------------------------------------------ */

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
    fj->state = FSTATE_MATERIALIZE;

    /* Build output schema (needs child schemas to be available) */
    fj->base.output_schema = build_output_schema(fj);
    fj->base.next_batch = fuzzy_join_next_batch;
    fj->base.kind = "FuzzyJoinNode";
    fj->base.free_node = fuzzy_join_free;

    return fj;
}
