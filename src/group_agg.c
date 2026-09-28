#include "group_agg.h"
#include "plan_budget.h"
#include "hash.h"
#include "key_arena.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "coerce.h"
#include "builder.h"
#include "sort.h"
#include "key_snap.h"
#include "error.h"
#include "vec_omp.h"
#include "scan.h"
#include "vtr1_tdc.h"
#include "vtr_codec.h"
#include "part_spill.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

static int agg_is_holistic(AggKind kind) {
    return kind == AGG_MEDIAN || kind == AGG_N_DISTINCT;
}

/* Per-group spill budget for one holistic accumulator (median / n_distinct).
   `total` is split across the holistic aggregations so their concurrent
   in-RAM buffers for a single group sum to <= total before any spills. Scalar
   aggregations ignore the value (they hold O(1) state). */
static int64_t agg_holistic_budget(const GroupAggNode *ga, int64_t total) {
    int n_holistic = 0;
    for (int a = 0; a < ga->n_aggs; a++)
        if (agg_is_holistic(ga->agg_specs[a].kind))
            n_holistic++;
    if (n_holistic < 1) n_holistic = 1;
    return total / n_holistic;
}

/* Output column type of an aggregate. Every aggregate emits a double except
   first()/last() on a string column, which preserve the string type. */
static VecType agg_output_type(AggKind kind, VecType input_type) {
    if ((kind == AGG_FIRST || kind == AGG_LAST) && input_type == VEC_STRING)
        return VEC_STRING;
    return VEC_DOUBLE;
}

static char *str_dup(const char *s) {
    size_t len = strlen(s);
    char *d = (char *)malloc(len + 1);
    memcpy(d, s, len + 1);
    return d;
}

/* Resolve each aggregate's input column index and type in schema cs; n() has
   no input column (index -1, typed int64). */
static void agg_resolve_inputs(const GroupAggNode *ga, const VecSchema *cs,
                               int *idx, VecType *types) {
    for (int a = 0; a < ga->n_aggs; a++) {
        if (ga->agg_specs[a].kind == AGG_COUNT_STAR) {
            idx[a] = -1;
            types[a] = VEC_INT64;
        } else {
            idx[a] = vec_schema_find_col(cs, ga->agg_specs[a].input_col);
            if (idx[a] < 0)
                vectra_error("summarise: column not found: %s",
                             ga->agg_specs[a].input_col);
            types[a] = cs->col_types[idx[a]];
        }
    }
}

/* Feed row r of batch into every aggregate of group gid. */
static inline void agg_feed_row(AggAccum *accums, int n_aggs, const int *idx,
                                int64_t gid, const VecBatch *batch, int64_t r) {
    for (int a = 0; a < n_aggs; a++) {
        if (idx[a] >= 0)
            agg_accum_feed(&accums[a], gid, &batch->columns[idx[a]], r);
        else
            agg_accum_feed(&accums[a], gid, NULL, 0);
    }
}

/* Set the result batch's column names: keys, then aggregate outputs. */
static void group_agg_name_columns(const GroupAggNode *ga, VecBatch *out) {
    for (int k = 0; k < ga->n_keys; k++)
        out->col_names[k] = str_dup(ga->key_names[k]);
    for (int a = 0; a < ga->n_aggs; a++)
        out->col_names[ga->n_keys + a] = str_dup(ga->agg_specs[a].output_name);
}

/* Emitted result batches hold at most this many groups. */
#define GROUP_AGG_EMIT 131072

typedef enum { GAGG_AUTO, GAGG_SORTED } GroupAggMode;

static GroupAggNode *group_agg_create(VecNode *child, int n_keys,
                                      char **key_names, int n_aggs,
                                      AggSpec *agg_specs, const char *temp_dir,
                                      int64_t mem_budget, GroupAggMode mode);
static void group_agg_share_grant(GroupAggNode *ga, VecMemGrant *grant);

/* ================================================================== */
/*  Hash aggregation with a bounded partitioned fallback              */
/*                                                                    */
/*  Groups are aggregated in hash tables while their resident bytes   */
/*  stay under the budget. The key space is split into shards by a    */
/*  mixed key hash, one table (VecHashTable + KeyArena + one AggAccum */
/*  per aggregate) per shard, so a batch is aggregated by all shards  */
/*  in parallel with no shared state and no merge: a group lives in   */
/*  exactly one shard. Once a shard's share of the budget is reached  */
/*  the shard is frozen: rows of groups already in it keep            */
/*  aggregating in place, and rows of any other group are routed by  */
/*  a salted key hash into HAGG_PARTS run files. Every group          */
/*  therefore lives either wholly in a table or wholly in one         */
/*  partition, so no partial state is ever merged. After the input is */
/*  drained the tables are emitted and freed, then each partition is  */
/*  aggregated by a nested HashAggNode over a scan of its run file,   */
/*  one at a time, with a deeper salt so a partition that still       */
/*  overflows splits across different children. At HAGG_MAX_DEPTH a   */
/*  partition drops to the sort-based path, which is bounded for any  */
/*  key skew.                                                         */
/* ================================================================== */

#define HAGG_PARTS 64
#define HAGG_MAX_DEPTH 3
#define HAGG_MAX_SHARDS 64
/* Group headroom reserved per shard before a parallel pass; a shard that
   exhausts it stops, is regrown on the master, and resumes. */
#define HAGG_HEADROOM_MIN 1024
/* Byte budget for the partition buffers (part_spill), floored at
   HAGG_PART_BUF_MIN so a tiny budget cannot degrade into one row group per
   row. */
#define HAGG_PART_BUF_MIN    (256LL * 1024)
#define HAGG_PART_BUF_MAX    (64LL * 1024 * 1024)

/* The tables index slots by the low bits of the raw hash, so shards and
   partitions are chosen from mixed bits: keys that share a shard or partition
   do not share their slot bits. The shard is picked by the murmur3 fmix of the
   hash (hagg_shard_mix); the spill partition by part_spill_salt() under salt
   depth + 1, so each level splits independently. */
static inline uint64_t hagg_shard_mix(uint64_t h) {
    h ^= h >> 33; h *= 0xFF51AFD7ED558CCDULL;
    h ^= h >> 33; h *= 0xC4CEB9FE1A85EC53ULL;
    h ^= h >> 33;
    return h;
}

enum { HAGG_CONSUME, HAGG_EMIT_TABLE, HAGG_PARTITIONS, HAGG_DONE };

typedef struct {
    VecHashTable  ht;
    KeyArena      arena;
    AggAccum     *accums;
    int           frozen;
    int64_t       cap;        /* groups insertable without allocating */
    int64_t       pos, end;   /* this batch's rows in the shard order array */
    VecArray     *fin;        /* finished aggregate arrays, at emit */
} HaggShard;

typedef struct {
    VecNode        base;
    GroupAggNode  *ga;          /* borrowed: key names, agg specs, temp_dir */
    VecNode       *in;          /* input stream */
    int            own_in;      /* 1 = free `in` with this node */
    int            depth;       /* partitioning level (0 = top) */
    int64_t        budget;      /* table budget in bytes; <= 0 = unbounded */
    int64_t        shard_freeze;/* per-shard bytes past which a shard freezes */
    int            sorted_emit; /* 1 = emit groups ordered by key */
    int            phase;

    int           *key_idx;
    VecType       *key_types;
    int           *agg_idx;
    VecType       *agg_types;

    int            n_shards;    /* power of two */
    int            shard_bits;
    int            parallel;    /* 1 = shards run in an OpenMP region */
    HaggShard     *shards;
    int            tables_live;
    int            frozen;      /* some shard froze: partitions exist */

    /* Spill: the key and aggregate-input columns, deduplicated, by name. */
    int            n_spill_cols;
    int           *spill_src;       /* input column index per spill column */
    VecSchema      spill_schema;
    char         **part_paths;      /* NULL = partition received no rows */
    PartSpill     *ps;              /* open while the input is consumed */
    int64_t        pbuf_budget;

    int64_t        n_out;           /* groups across shards, at emit */
    int64_t       *order;           /* emit order: shard + gid * n_shards */
    int64_t        emit_pos;
    int            emitted_any;

    int            cur_part;
    VecNode       *sub;             /* aggregation of the current partition */
} HashAggNode;

/* Resident bytes of one shard: slots, arena, accumulators, plus what emitting
   it adds -- the finished result arrays (bounded by the accumulators' own size)
   and 16 bytes per group for the emit order and its merge buffer. */
static int64_t hagg_shard_bytes(const HashAggNode *h, const HaggShard *s) {
    const GroupAggNode *ga = h->ga;
    int64_t bytes = vec_ht_bytes(&s->ht);
    for (int k = 0; k < ga->n_keys; k++) {
        int w = vec_type_elem_size(h->key_types[k]);
        bytes += s->arena.capacity * (int64_t)((w ? w : 8) + 1);
        if (h->key_types[k] == VEC_STRING)
            bytes += s->arena.str_data_cap[k];
    }
    for (int a = 0; a < ga->n_aggs; a++)
        bytes += 2 * agg_accum_bytes(&s->accums[a]);
    bytes += 2 * (int64_t)sizeof(int64_t) * s->ht.n_groups;
    return bytes;
}

static void hagg_setup(HashAggNode *h) {
    GroupAggNode *ga = h->ga;
    const VecSchema *cs = &h->in->output_schema;
    int nk = ga->n_keys > 0 ? ga->n_keys : 1;
    int na = ga->n_aggs > 0 ? ga->n_aggs : 1;
    h->key_idx   = (int *)malloc((size_t)nk * sizeof(int));
    h->key_types = (VecType *)malloc((size_t)nk * sizeof(VecType));
    for (int k = 0; k < ga->n_keys; k++) {
        h->key_idx[k] = vec_schema_find_col(cs, ga->key_names[k]);
        if (h->key_idx[k] < 0)
            vectra_error("group_by: column not found: %s", ga->key_names[k]);
        h->key_types[k] = cs->col_types[h->key_idx[k]];
    }
    h->agg_idx   = (int *)malloc((size_t)na * sizeof(int));
    h->agg_types = (VecType *)malloc((size_t)na * sizeof(VecType));
    agg_resolve_inputs(ga, cs, h->agg_idx, h->agg_types);

    /* Shards run in parallel only when no feed allocates (string first/last,
       median, n_distinct), since nothing inside the region may raise. */
    int alloc_feed = 0;
    for (int a = 0; a < ga->n_aggs; a++)
        if (agg_accum_feed_allocates(ga->agg_specs[a].kind, h->agg_types[a]))
            alloc_feed = 1;
    int threads = 1;
#ifdef _OPENMP
    if (!omp_in_parallel()) threads = omp_get_max_threads();
#endif
    h->n_shards = 1;
    h->shard_bits = 0;
    if (ga->n_keys > 0 && !alloc_feed) {
        while (h->n_shards * 2 <= threads && h->n_shards < HAGG_MAX_SHARDS) {
            h->n_shards *= 2;
            h->shard_bits++;
        }
    }
    h->parallel = h->n_shards > 1;
    h->shard_freeze = (h->budget - h->pbuf_budget) / h->n_shards;

    int64_t store_mem = agg_holistic_budget(ga, h->budget);
    h->shards = (HaggShard *)calloc((size_t)h->n_shards, sizeof(HaggShard));
    if (!h->shards) vectra_error("alloc failed for hash aggregation shards");
    for (int s = 0; s < h->n_shards; s++) {
        HaggShard *sh = &h->shards[s];
        sh->accums = (AggAccum *)malloc((size_t)na * sizeof(AggAccum));
        for (int a = 0; a < ga->n_aggs; a++)
            sh->accums[a] = agg_accum_init(ga->agg_specs[a].kind,
                                           h->agg_types[a],
                                           ga->agg_specs[a].na_rm,
                                           store_mem, ga->temp_dir);
        sh->ht = vec_ht_create(64);
        key_arena_init(&sh->arena, ga->n_keys, h->key_types);
    }
    h->tables_live = 1;

    /* Spill schema: keys first, then each distinct aggregate input column. */
    h->spill_src = (int *)malloc((size_t)(nk + na) * sizeof(int));
    int n = 0;
    for (int k = 0; k < ga->n_keys; k++) h->spill_src[n++] = h->key_idx[k];
    for (int a = 0; a < ga->n_aggs; a++) {
        int ci = h->agg_idx[a];
        if (ci < 0) continue;
        int dup = 0;
        for (int j = 0; j < n; j++) if (h->spill_src[j] == ci) { dup = 1; break; }
        if (!dup) h->spill_src[n++] = ci;
    }
    h->n_spill_cols = n;
}

static char *hagg_part_path(const char *temp_dir, int depth, int p) {
    static int hagg_counter = 0;
    int id = hagg_counter++;
    int len = snprintf(NULL, 0, "%s/vectra_hagg_%d_d%d_p%d.vtr",
                       temp_dir, id, depth, p);
    char *path = (char *)malloc((size_t)(len + 1));
    snprintf(path, (size_t)(len + 1), "%s/vectra_hagg_%d_d%d_p%d.vtr",
             temp_dir, id, depth, p);
    return path;
}

/* First freeze: open the partition spill. */
static void hagg_open_partitions(HashAggNode *h) {
    const VecSchema *cs = &h->in->output_schema;
    char **names = (char **)malloc((size_t)h->n_spill_cols * sizeof(char *));
    VecType *types = (VecType *)malloc((size_t)h->n_spill_cols * sizeof(VecType));
    for (int c = 0; c < h->n_spill_cols; c++) {
        names[c] = cs->col_names[h->spill_src[c]];
        types[c] = cs->col_types[h->spill_src[c]];
    }
    h->spill_schema = vec_schema_create(h->n_spill_cols, names, types);
    free(names);
    free(types);

    h->part_paths = (char **)calloc(HAGG_PARTS, sizeof(char *));
    for (int p = 0; p < HAGG_PARTS; p++)
        h->part_paths[p] = hagg_part_path(h->ga->temp_dir, h->depth, p);
    h->ps = part_spill_create(&h->spill_schema, h->spill_src, HAGG_PARTS,
                              h->part_paths, h->pbuf_budget);
    h->frozen = 1;
}

/* Route this batch's flagged rows (groups no frozen table holds) to their
   partitions, keeping input order within each partition. */
static void hagg_spill_batch(HashAggNode *h, VecBatch *batch,
                             const uint8_t *spill, const uint64_t *hashes,
                             int64_t n) {
    int *pid = (int *)malloc((size_t)(n > 0 ? n : 1) * sizeof(int));
    if (!pid) vectra_error("alloc failed for hash aggregation spill");
    int64_t n_sp = 0;
    for (int64_t li = 0; li < n; li++) {
        if (!spill[li]) { pid[li] = -1; continue; }
        pid[li] = (int)(part_spill_salt(hashes[li], (uint64_t)h->depth + 1) %
                        (uint64_t)HAGG_PARTS);
        n_sp++;
    }
    if (n_sp > 0) part_spill_route(h->ps, batch, pid);
    free(pid);
}

/* Close the partition spill; a partition that received no rows keeps no
   file and no path. */
static void hagg_close_partitions(HashAggNode *h) {
    if (!h->ps) return;
    uint8_t used[HAGG_PARTS];
    for (int p = 0; p < HAGG_PARTS; p++) used[p] = (uint8_t)part_spill_used(h->ps, p);
    part_spill_close(h->ps);
    h->ps = NULL;
    for (int p = 0; p < HAGG_PARTS; p++) {
        if (used[p]) continue;
        free(h->part_paths[p]);
        h->part_paths[p] = NULL;
    }
}

/* Table bytes the pool lets this node hold now: the node's cap (half the
   budget at the top level, so the result sort keeps the other half), or less
   when other nodes of the plan already hold the rest. */
static int64_t hagg_table_cap(const HashAggNode *h) {
    int64_t allow = vec_mem_allowance(&h->ga->mem);
    return allow < h->budget ? allow : h->budget;
}

/* Reserve what the tables and the partition buffers hold now. */
static void hagg_account(HashAggNode *h) {
    int64_t bytes = h->frozen ? h->pbuf_budget : 0;
    for (int s = 0; s < h->n_shards; s++)
        bytes += hagg_shard_bytes(h, &h->shards[s]);
    vec_mem_set(&h->ga->mem, bytes);
}

/* 1 when shard s can take the group at physical row r without allocating. */
static inline int hagg_room(const HaggShard *sh, const VecArray *bkeys,
                            int n_keys, int64_t r) {
    if (sh->ht.n_groups >= sh->cap) return 0;
    for (int k = 0; k < n_keys; k++) {
        if (bkeys[k].type != VEC_STRING || !vec_array_is_valid(&bkeys[k], r))
            continue;
        int64_t slen = bkeys[k].buf.str.offsets[r + 1] - bkeys[k].buf.str.offsets[r];
        if (sh->arena.str_data_len[k] + slen > sh->arena.str_data_cap[k])
            return 0;
    }
    return 1;
}

/* Aggregate shard s's pending rows of this batch, from sh->pos, until done or
   until the next new group does not fit the pre-reserved capacity. Performs no
   allocation (and so cannot raise) when the node runs shards in parallel. Rows
   of a frozen shard's unknown groups are flagged for the serial spill pass. */
static void hagg_run_shard(HashAggNode *h, HaggShard *sh, const VecBatch *batch,
                           const VecArray *bkeys, const int64_t *order,
                           const uint64_t *hashes, uint8_t *spill) {
    GroupAggNode *ga = h->ga;
    for (; sh->pos < sh->end; sh->pos++) {
        int64_t li = order[sh->pos];
        int64_t r = vec_batch_physical_row((VecBatch *)batch, li);
        int64_t gid = vec_ht_find(&sh->ht, hashes[li], bkeys, ga->n_keys, r,
                                  sh->arena.arenas);
        if (gid < 0) {
            if (sh->frozen) { spill[li] = 1; continue; }
            if (!hagg_room(sh, bkeys, ga->n_keys, r)) return;
            int was_new = 0;
            gid = vec_ht_find_or_insert(&sh->ht, hashes[li], bkeys, ga->n_keys,
                                        r, sh->arena.arenas, sh->arena.length,
                                        &was_new);
            key_arena_append_row(&sh->arena, bkeys, r);
            for (int a = 0; a < ga->n_aggs; a++)
                agg_accum_ensure(&sh->accums[a], sh->ht.n_groups);
            if (h->budget > 0 && ga->n_keys > 0 &&
                hagg_shard_bytes(h, sh) > h->shard_freeze)
                sh->frozen = 1;
        }
        agg_feed_row(sh->accums, ga->n_aggs, h->agg_idx, gid, batch, r);
    }
}

/* Grow shard s so its next pending rows can open new groups without
   allocating: room for the lesser of its pending rows and a headroom that
   scales with the shard, plus the string key bytes of the rows that would
   fill it. */
static void hagg_reserve_shard(HashAggNode *h, HaggShard *sh,
                               const VecArray *bkeys, const VecBatch *batch,
                               const int64_t *order) {
    GroupAggNode *ga = h->ga;
    int64_t pending = sh->end - sh->pos;
    int64_t head = sh->ht.n_groups / 2;
    if (head < HAGG_HEADROOM_MIN) head = HAGG_HEADROOM_MIN;
    if (head > pending) head = pending;
    int64_t cap = sh->ht.n_groups + head;
    int64_t str_extra[16] = {0};
    int64_t *extra = ga->n_keys <= 16 ? str_extra
        : (int64_t *)calloc((size_t)ga->n_keys, sizeof(int64_t));
    for (int k = 0; k < ga->n_keys; k++) {
        if (bkeys[k].type != VEC_STRING) continue;
        for (int64_t i = 0; i < head; i++) {
            int64_t r = vec_batch_physical_row((VecBatch *)batch, order[sh->pos + i]);
            extra[k] += bkeys[k].buf.str.offsets[r + 1] - bkeys[k].buf.str.offsets[r];
        }
    }
    vec_ht_reserve(&sh->ht, cap);
    key_arena_reserve(&sh->arena, cap, extra);
    for (int a = 0; a < ga->n_aggs; a++)
        agg_accum_reserve(&sh->accums[a], cap);
    sh->cap = cap;
    if (extra != str_extra) free(extra);
}

/* Drain the input into the tables (and, once frozen, the partitions). */
static void hagg_consume(HashAggNode *h) {
    GroupAggNode *ga = h->ga;
    hagg_setup(h);
    int ns = h->n_shards;
    int64_t *counts = (int64_t *)malloc((size_t)ns * sizeof(int64_t));

    VecArray *bkeys = (VecArray *)malloc(
        (size_t)(ga->n_keys > 0 ? ga->n_keys : 1) * sizeof(VecArray));
    VecBatch *batch;
    while ((batch = h->in->next_batch(h->in)) != NULL) {
        for (int k = 0; k < ga->n_keys; k++)
            bkeys[k] = batch->columns[h->key_idx[k]];
        int64_t n = vec_batch_logical_rows(batch);
        size_t nn = (size_t)(n > 0 ? n : 1);
        uint64_t *hashes = (uint64_t *)malloc(nn * sizeof(uint64_t));
        uint8_t  *sid    = (uint8_t *)malloc(nn);
        int64_t  *order  = (int64_t *)malloc(nn * sizeof(int64_t));
        uint8_t  *spill  = (uint8_t *)calloc(nn, 1);
        if (!hashes || !sid || !order || !spill) {
            free(hashes); free(sid); free(order); free(spill);
            vectra_error("alloc failed for hash aggregation batch");
        }

        int sbits = h->shard_bits;
        #ifdef _OPENMP
        #pragma omp parallel for if(n > VEC_OMP_THRESHOLD) schedule(static)
        #endif
        for (int64_t li = 0; li < n; li++) {
            int64_t r = vec_batch_physical_row(batch, li);
            uint64_t hv = 0;
            for (int k = 0; k < ga->n_keys; k++) {
                uint64_t kh = vec_hash_value(&bkeys[k], r);
                hv = (k == 0) ? kh : vec_hash_combine(hv, kh);
            }
            hashes[li] = hv;
            sid[li] = sbits ? (uint8_t)(hagg_shard_mix(hv) >> (64 - sbits)) : 0;
        }

        /* Counting sort of the rows by shard, keeping input order within a
           shard so first()/last() see each group's rows in order. */
        memset(counts, 0, (size_t)ns * sizeof(int64_t));
        for (int64_t li = 0; li < n; li++) counts[sid[li]]++;
        int64_t off = 0;
        for (int s = 0; s < ns; s++) {
            h->shards[s].pos = h->shards[s].end = off;
            off += counts[s];
        }
        for (int64_t li = 0; li < n; li++)
            order[h->shards[sid[li]].end++] = li;

        /* A shard freezes at its share of what the pool gives the tables
           now, read on the master before the shards run. */
        if (h->budget > 0)
            h->shard_freeze = (hagg_table_cap(h) - h->pbuf_budget) / ns;

        /* Reserve on the master, run every shard until done or out of room,
           repeat for the shards that stopped. */
        for (;;) {
            int pending = 0;
            for (int s = 0; s < ns; s++) {
                HaggShard *sh = &h->shards[s];
                if (sh->pos >= sh->end) continue;
                pending = 1;
                if (!sh->frozen && sh->ht.n_groups >= sh->cap)
                    hagg_reserve_shard(h, sh, bkeys, batch, order);
            }
            if (!pending) break;
            if (h->parallel && n > VEC_OMP_THRESHOLD) {
                #ifdef _OPENMP
                #pragma omp parallel for schedule(dynamic, 1)
                #endif
                for (int s = 0; s < ns; s++)
                    hagg_run_shard(h, &h->shards[s], batch, bkeys, order,
                                   hashes, spill);
            } else {
                for (int s = 0; s < ns; s++)
                    hagg_run_shard(h, &h->shards[s], batch, bkeys, order,
                                   hashes, spill);
            }
            /* A shard stopped for want of string key bytes has room in groups;
               force its regrow on the next round. */
            for (int s = 0; s < ns; s++) {
                HaggShard *sh = &h->shards[s];
                if (sh->pos < sh->end && !sh->frozen) sh->cap = sh->ht.n_groups;
            }
        }

        int any_frozen = 0;
        for (int s = 0; s < ns; s++) any_frozen |= h->shards[s].frozen;
        if (any_frozen) {
            if (!h->frozen) hagg_open_partitions(h);
            hagg_spill_batch(h, batch, spill, hashes, n);
        }
        free(hashes); free(sid); free(order); free(spill);
        vec_batch_free(batch);
        hagg_account(h);
    }
    free(bkeys);
    free(counts);

    hagg_close_partitions(h);
    hagg_account(h);
    h->phase = HAGG_EMIT_TABLE;
}

/* Group order the sort-based path produces: ascending by each key in turn,
   NA as the largest value (SortKey na_last = 0). Entries encode
   shard + gid * n_shards. */
static int hagg_key_cmp(const HashAggNode *h, int64_t x, int64_t y) {
    int ns = h->n_shards;
    const KeyArena *ax = &h->shards[x % ns].arena;
    const KeyArena *ay = &h->shards[y % ns].arena;
    int64_t gx = x / ns, gy = y / ns;
    for (int k = 0; k < h->ga->n_keys; k++) {
        int c = sort_compare_value(&ax->arenas[k], gx, &ay->arenas[k], gy, 0, 0);
        if (c) return c;
    }
    return 0;
}

/* Bottom-up merge sort of the emit order by key. */
static void hagg_sort_order(HashAggNode *h) {
    int64_t n = h->n_out;
    int64_t *a = h->order;
    int64_t *tmp = (int64_t *)malloc((size_t)(n > 0 ? n : 1) * sizeof(int64_t));
    if (!tmp) vectra_error("alloc failed for group order");
    for (int64_t w = 1; w < n; w *= 2) {
        for (int64_t lo = 0; lo < n; lo += 2 * w) {
            int64_t mid = lo + w < n ? lo + w : n;
            int64_t hi = lo + 2 * w < n ? lo + 2 * w : n;
            int64_t i = lo, j = mid, o = lo;
            while (i < mid && j < hi)
                tmp[o++] = hagg_key_cmp(h, a[j], a[i]) < 0 ? a[j++] : a[i++];
            while (i < mid) tmp[o++] = a[i++];
            while (j < hi)  tmp[o++] = a[j++];
        }
        int64_t *t = a; a = tmp; tmp = t;
    }
    free(tmp);
    h->order = a;
}

/* Finish every shard's aggregates and lay out the emit order: shard by shard,
   or by key when sorted_emit. */
static void hagg_prepare_emit(HashAggNode *h) {
    GroupAggNode *ga = h->ga;
    int ns = h->n_shards;
    h->n_out = 0;
    for (int s = 0; s < ns; s++) h->n_out += h->shards[s].ht.n_groups;
    h->order = (int64_t *)malloc((size_t)(h->n_out > 0 ? h->n_out : 1) *
                                 sizeof(int64_t));
    if (!h->order) vectra_error("alloc failed for group order");
    int64_t o = 0;
    for (int s = 0; s < ns; s++) {
        HaggShard *sh = &h->shards[s];
        for (int64_t g = 0; g < sh->ht.n_groups; g++)
            h->order[o++] = s + g * ns;
        sh->fin = (VecArray *)calloc((size_t)(ga->n_aggs > 0 ? ga->n_aggs : 1),
                                     sizeof(VecArray));
        for (int a = 0; a < ga->n_aggs; a++) {
            sh->accums[a].n_groups = sh->ht.n_groups;
            sh->fin[a] = agg_accum_finish(&sh->accums[a]);
        }
    }
    if (h->sorted_emit && ga->n_keys > 0 && h->n_out > 1)
        hagg_sort_order(h);
}

static void hagg_free_tables(HashAggNode *h) {
    if (!h->tables_live) return;
    for (int s = 0; s < h->n_shards; s++) {
        HaggShard *sh = &h->shards[s];
        for (int a = 0; a < h->ga->n_aggs; a++) {
            agg_accum_free(&sh->accums[a]);
            if (sh->fin) vec_array_free(&sh->fin[a]);
        }
        free(sh->accums);
        free(sh->fin);
        vec_ht_free(&sh->ht);
        key_arena_free(&sh->arena);
    }
    free(h->shards);
    h->shards = NULL;
    free(h->order);
    h->order = NULL;
    h->tables_live = 0;
    vec_mem_set(&h->ga->mem, 0);
}

/* Emit the next slice of table groups in emit order. */
static VecBatch *hagg_emit_table(HashAggNode *h) {
    GroupAggNode *ga = h->ga;
    int ns = h->n_shards;
    int64_t m = h->n_out - h->emit_pos;
    if (m > GROUP_AGG_EMIT) m = GROUP_AGG_EMIT;
    const int64_t *ord = h->order + h->emit_pos;

    VecBatch *out = vec_batch_alloc(ga->n_keys + ga->n_aggs, m);
    for (int k = 0; k < ga->n_keys; k++) {
        VecArrayBuilder b = vec_builder_init(h->key_types[k]);
        vec_builder_reserve(&b, m);
        for (int64_t i = 0; i < m; i++)
            vec_builder_append_one(&b, &h->shards[ord[i] % ns].arena.arenas[k],
                                   ord[i] / ns);
        out->columns[k] = vec_builder_finish(&b);
    }
    for (int a = 0; a < ga->n_aggs; a++) {
        VecArrayBuilder b = vec_builder_init(
            agg_output_type(ga->agg_specs[a].kind, h->agg_types[a]));
        vec_builder_reserve(&b, m);
        for (int64_t i = 0; i < m; i++)
            vec_builder_append_one(&b, &h->shards[ord[i] % ns].fin[a],
                                   ord[i] / ns);
        out->columns[ga->n_keys + a] = vec_builder_finish(&b);
    }
    group_agg_name_columns(ga, out);
    h->emit_pos += m;
    h->emitted_any = 1;
    return out;
}

static VecNode *hagg_node_create(GroupAggNode *ga, VecNode *in, int own_in,
                                 int depth, int64_t budget, int sorted_emit);

/* Aggregation of partition p's run file: a nested HashAggNode one level
   deeper, or the sort-based path once the depth cap is reached. */
static VecNode *hagg_partition_node(HashAggNode *h, int p) {
    GroupAggNode *ga = h->ga;
    VecNode *scan = (VecNode *)scan_node_create(h->part_paths[p], NULL, 0);
    if (h->depth + 1 < HAGG_MAX_DEPTH)
        return hagg_node_create(ga, scan, 1, h->depth + 1, h->budget, 0);

    char **keys = (char **)malloc((size_t)ga->n_keys * sizeof(char *));
    for (int k = 0; k < ga->n_keys; k++) keys[k] = str_dup(ga->key_names[k]);
    AggSpec *specs = (AggSpec *)calloc((size_t)(ga->n_aggs > 0 ? ga->n_aggs : 1),
                                       sizeof(AggSpec));
    for (int a = 0; a < ga->n_aggs; a++) {
        specs[a] = ga->agg_specs[a];
        specs[a].output_name = str_dup(ga->agg_specs[a].output_name);
        specs[a].input_col = ga->agg_specs[a].input_col
            ? str_dup(ga->agg_specs[a].input_col) : NULL;
    }
    GroupAggNode *sub = group_agg_create(scan, ga->n_keys, keys, ga->n_aggs,
                                         specs, ga->temp_dir, h->budget,
                                         GAGG_SORTED);
    group_agg_share_grant(sub, ga->mem.grant);
    return (VecNode *)sub;
}

static VecBatch *hagg_next_batch(VecNode *self) {
    HashAggNode *h = (HashAggNode *)self;
    if (h->phase == HAGG_CONSUME) hagg_consume(h);

    if (h->phase == HAGG_EMIT_TABLE) {
        if (h->order == NULL) hagg_prepare_emit(h);
        if (h->emit_pos < h->n_out || !h->emitted_any)
            return hagg_emit_table(h);
        hagg_free_tables(h);
        h->phase = h->frozen ? HAGG_PARTITIONS : HAGG_DONE;
    }

    while (h->phase == HAGG_PARTITIONS) {
        if (h->sub == NULL) {
            while (h->cur_part < HAGG_PARTS && h->part_paths[h->cur_part] == NULL)
                h->cur_part++;
            if (h->cur_part >= HAGG_PARTS) { h->phase = HAGG_DONE; break; }
            h->sub = hagg_partition_node(h, h->cur_part);
        }
        VecBatch *out = h->sub->next_batch(h->sub);
        if (out) return out;
        h->sub->free_node(h->sub);
        h->sub = NULL;
        remove(h->part_paths[h->cur_part]);
        free(h->part_paths[h->cur_part]);
        h->part_paths[h->cur_part] = NULL;
        h->cur_part++;
    }
    return NULL;
}

static void hagg_free(VecNode *self) {
    HashAggNode *h = (HashAggNode *)self;
    hagg_free_tables(h);
    if (h->sub) h->sub->free_node(h->sub);
    if (h->frozen) {
        hagg_close_partitions(h);
        for (int p = 0; p < HAGG_PARTS; p++)
            if (h->part_paths[p]) { remove(h->part_paths[p]); free(h->part_paths[p]); }
        free(h->part_paths);
        vec_schema_free(&h->spill_schema);
    }
    if (h->own_in) h->in->free_node(h->in);
    free(h->key_idx);
    free(h->key_types);
    free(h->agg_idx);
    free(h->agg_types);
    free(h->spill_src);
    vec_schema_free(&h->base.output_schema);
    free(h);
}

static VecNode *hagg_node_create(GroupAggNode *ga, VecNode *in, int own_in,
                                 int depth, int64_t budget, int sorted_emit) {
    HashAggNode *h = (HashAggNode *)calloc(1, sizeof(HashAggNode));
    if (!h) vectra_error("alloc failed for HashAggNode");
    h->ga = ga;
    h->in = in;
    h->own_in = own_in;
    h->depth = depth;
    h->budget = budget;
    h->pbuf_budget = budget / 16;
    if (h->pbuf_budget > HAGG_PART_BUF_MAX) h->pbuf_budget = HAGG_PART_BUF_MAX;
    if (h->pbuf_budget < HAGG_PART_BUF_MIN) h->pbuf_budget = HAGG_PART_BUF_MIN;
    h->sorted_emit = sorted_emit;
    h->phase = HAGG_CONSUME;
    h->base.output_schema = vec_schema_copy(&ga->base.output_schema);
    h->base.next_batch = hagg_next_batch;
    h->base.free_node = hagg_free;
    h->base.kind = "HashAggNode";
    return (VecNode *)h;
}

/* ================================================================== */
/*  Sort-based aggregation (spill-safe path)                          */
/*                                                                    */
/*  Pre-condition: child is a SortNode sorted by the key columns.     */
/*  Linear scan: consecutive rows with identical keys belong to the   */
/*  same group.  Accumulators hold state for ONE group at a time.     */
/* ================================================================== */

/* KeySnap (group-boundary detection over a key-sorted stream) is shared with
   group_topn; see key_snap.h. */

/* Flush completed group: append key snapshot + agg results to builders */
static void flush_group(const KeySnap *snap,
                        VecArrayBuilder *key_builders, int n_keys,
                        VecArrayBuilder *agg_builders, int n_aggs,
                        AggAccum *accums, const VecType *agg_types,
                        const AggSpec *agg_specs,
                        int64_t mem_budget, const char *temp_dir) {
    /* Append key values */
    for (int k = 0; k < n_keys; k++) {
        VecArrayBuilder *b = &key_builders[k];
        if (!snap->valid[k]) {
            vec_builder_append_na(b);
        } else {
            /* Ensure capacity for 1 row */
            vec_builder_reserve(b, 1);
            b->validity[b->length / 8] |= (uint8_t)(1 << (b->length % 8));
            switch (snap->types[k]) {
            case VEC_INT64:  b->buf.i64[b->length] = snap->i64[k]; break;
            case VEC_INT32:  b->buf.i32[b->length] = (int32_t)snap->i64[k]; break;
            case VEC_INT16:  b->buf.i16[b->length] = (int16_t)snap->i64[k]; break;
            case VEC_INT8:   b->buf.i8[b->length]  = (int8_t)snap->i64[k]; break;
            case VEC_DOUBLE: b->buf.dbl[b->length] = snap->dbl[k]; break;
            case VEC_BOOL:   b->buf.bln[b->length] = snap->bln[k]; break;
            case VEC_STRING: {
                int64_t soff = snap->str_offs[k];
                int64_t slen = snap->str_offs[k + 1] - soff;
                /* Manually append string */
                if (slen > 0) {
                    int64_t needed = b->str_data_len + slen;
                    if (needed > b->str_data_cap) {
                        int64_t nc = b->str_data_cap == 0 ? 256 : b->str_data_cap;
                        while (nc < needed) nc *= 2;
                        b->str_data = (char *)realloc(b->str_data, (size_t)nc);
                        b->str_data_cap = nc;
                    }
                    b->str_offsets[b->length] = b->str_data_len;
                    memcpy(b->str_data + b->str_data_len,
                           snap->str_data + soff, (size_t)slen);
                    b->str_data_len += slen;
                    b->str_offsets[b->length + 1] = b->str_data_len;
                } else {
                    b->str_offsets[b->length] = b->str_data_len;
                    b->str_offsets[b->length + 1] = b->str_data_len;
                }
                break;
            }
            }
            b->length++;
        }
    }

    /* Append agg results (each accumulator has n_groups=1) */
    for (int a = 0; a < n_aggs; a++) {
        VecArray arr = agg_accum_finish(&accums[a]);
        vec_builder_append_one(&agg_builders[a], &arr, 0);
        vec_array_free(&arr);
        /* Free this group's accumulator (buffers, spill run files) before
           reusing the slot for the next group -- otherwise every group but the
           last leaks its state, which for median/n_distinct is the whole
           group. Then reinitialize for the next group. */
        agg_accum_free(&accums[a]);
        accums[a] = agg_accum_init(agg_specs[a].kind, agg_types[a],
                                    agg_specs[a].na_rm, mem_budget, temp_dir);
        agg_accum_ensure(&accums[a], 1);
    }
}

/* Emit the sorted grouped result in bounded batches rather than one giant batch
   whose size is O(#groups). State persists on the node across next_batch calls;
   completed groups accumulate in the builders and are flushed out once the
   emit threshold is reached, while the open group's accumulator + key snapshot
   carry over. Peak resident output is the emit threshold plus one child batch
   of groups -- bounded by the child rowgroup size, not the total group count. */

typedef struct {
    int              inited;
    int              scan_done;   /* child exhausted; last group flushed */
    int             *key_indices;
    VecType         *key_types;
    int             *agg_col_indices;
    VecType         *agg_types;
    VecArrayBuilder *key_builders;
    VecArrayBuilder *agg_builders;
    AggAccum        *accums;
    int64_t          store_mem;
    KeySnap          snap;
    VecBatch        *cur_batch;   /* child batch being scanned (mid-batch resume) */
    int64_t          cur_row;     /* next row to process in cur_batch */
} SortedAggState;

/* (Re)initialize the keys+aggs output builders after a flush-out. */
static void sagg_reset_builders(SortedAggState *st, GroupAggNode *ga) {
    for (int k = 0; k < ga->n_keys; k++)
        st->key_builders[k] = vec_builder_init(st->key_types[k]);
    for (int a = 0; a < ga->n_aggs; a++)
        st->agg_builders[a] = vec_builder_init(
            agg_output_type(ga->agg_specs[a].kind, st->agg_types[a]));
}

static SortedAggState *sagg_init(GroupAggNode *ga) {
    const VecSchema *child_schema = &ga->child->output_schema;
    SortedAggState *st = (SortedAggState *)calloc(1, sizeof(SortedAggState));
    if (!st) vectra_error("alloc failed for SortedAggState");

    st->key_indices = (int *)malloc((size_t)ga->n_keys * sizeof(int));
    st->key_types = (VecType *)malloc((size_t)ga->n_keys * sizeof(VecType));
    for (int k = 0; k < ga->n_keys; k++) {
        st->key_indices[k] = vec_schema_find_col(child_schema, ga->key_names[k]);
        if (st->key_indices[k] < 0)
            vectra_error("group_by: column not found: %s", ga->key_names[k]);
        st->key_types[k] = child_schema->col_types[st->key_indices[k]];
    }

    st->agg_col_indices = (int *)malloc((size_t)(ga->n_aggs > 0 ? ga->n_aggs : 1) * sizeof(int));
    st->agg_types = (VecType *)malloc((size_t)(ga->n_aggs > 0 ? ga->n_aggs : 1) * sizeof(VecType));
    agg_resolve_inputs(ga, child_schema, st->agg_col_indices, st->agg_types);

    st->key_builders = (VecArrayBuilder *)calloc(
        (size_t)ga->n_keys, sizeof(VecArrayBuilder));
    st->agg_builders = (VecArrayBuilder *)calloc(
        (size_t)ga->n_aggs, sizeof(VecArrayBuilder));
    sagg_reset_builders(st, ga);

    /* The holistic stores reserve half of what the pool gives this node
       now; the input sort, which shares the grant, has the rest. */
    int64_t allow = vec_mem_allowance(&ga->mem);
    int64_t stores = allow == INT64_MAX ? ga->mem_budget : allow / 2;
    int has_holistic = 0;
    for (int a = 0; a < ga->n_aggs; a++)
        if (agg_is_holistic(ga->agg_specs[a].kind)) has_holistic = 1;
    if (has_holistic && stores > 0) vec_mem_set(&ga->mem, stores);
    st->store_mem = agg_holistic_budget(ga, stores);
    st->accums = (AggAccum *)malloc((size_t)ga->n_aggs * sizeof(AggAccum));
    for (int a = 0; a < ga->n_aggs; a++) {
        st->accums[a] = agg_accum_init(ga->agg_specs[a].kind, st->agg_types[a],
                                       ga->agg_specs[a].na_rm,
                                       st->store_mem, ga->temp_dir);
        agg_accum_ensure(&st->accums[a], 1);
    }

    st->snap = snap_create(ga->n_keys, st->key_types);
    st->inited = 1;
    return st;
}

static void sagg_free(SortedAggState *st, int n_keys, int n_aggs) {
    if (!st) return;
    if (st->cur_batch) { vec_batch_free(st->cur_batch); st->cur_batch = NULL; }
    for (int a = 0; a < n_aggs; a++)
        agg_accum_free(&st->accums[a]);
    free(st->accums);
    /* Builders that were never finished (e.g. abandoned mid-stream) still own
       their buffers; finishing frees them. A finished builder is already empty. */
    for (int k = 0; k < n_keys; k++) {
        VecArray a = vec_builder_finish(&st->key_builders[k]);
        vec_array_free(&a);
    }
    for (int a = 0; a < n_aggs; a++) {
        VecArray arr = vec_builder_finish(&st->agg_builders[a]);
        vec_array_free(&arr);
    }
    free(st->key_builders);
    free(st->agg_builders);
    free(st->key_indices);
    free(st->key_types);
    free(st->agg_col_indices);
    free(st->agg_types);
    snap_free(&st->snap);
    free(st);
}

/* Finish the current builders into a result batch and reset them for reuse. */
static VecBatch *sagg_emit(SortedAggState *st, GroupAggNode *ga) {
    int64_t n_groups = st->key_builders[0].length;
    int n_out = ga->n_keys + ga->n_aggs;
    VecBatch *result = vec_batch_alloc(n_out, n_groups);
    for (int k = 0; k < ga->n_keys; k++)
        result->columns[k] = vec_builder_finish(&st->key_builders[k]);
    for (int a = 0; a < ga->n_aggs; a++)
        result->columns[ga->n_keys + a] = vec_builder_finish(&st->agg_builders[a]);
    group_agg_name_columns(ga, result);
    sagg_reset_builders(st, ga);   /* fresh builders; open group carries over */
    return result;
}

static VecBatch *sorted_agg_next_batch(GroupAggNode *ga) {
    SortedAggState *st = (SortedAggState *)ga->sagg;
    if (st == NULL) {
        st = sagg_init(ga);
        ga->sagg = st;
    }
    if (st->scan_done)
        return NULL;

    /* Linear scan of sorted input with mid-batch resume. The emit threshold is
       checked after every row (not just at child-batch boundaries) because the
       child sort can hand back one arbitrarily large batch; buffering a whole
       such batch of groups would defeat the bound. Completed groups sit in the
       builders; the open group's accumulator + snap carry over between calls. */
    while (1) {
        if (st->cur_batch == NULL) {
            st->cur_batch = ga->child->next_batch(ga->child);
            st->cur_row = 0;
            if (st->cur_batch == NULL) break;   /* child exhausted */
        }
        VecBatch *batch = st->cur_batch;
        int64_t n_rows = batch->n_rows;
        while (st->cur_row < n_rows) {
            int64_t row = st->cur_row;
            if (!snap_matches(&st->snap, batch, row, st->key_indices)) {
                if (st->snap.initialized)
                    flush_group(&st->snap, st->key_builders, ga->n_keys,
                                st->agg_builders, ga->n_aggs,
                                st->accums, st->agg_types, ga->agg_specs,
                                st->store_mem, ga->temp_dir);
                snap_update(&st->snap, batch, row, st->key_indices);
            }
            agg_feed_row(st->accums, ga->n_aggs, st->agg_col_indices, 0,
                         batch, row);
            st->cur_row++;
            /* The open group (the one just fed) is not yet flushed, so the
               builders hold only completed groups. Emit and resume here. */
            if (st->key_builders[0].length >= GROUP_AGG_EMIT)
                return sagg_emit(st, ga);
        }
        vec_batch_free(batch);
        st->cur_batch = NULL;
    }

    /* Child exhausted: flush the last open group and emit the tail. */
    if (st->snap.initialized)
        flush_group(&st->snap, st->key_builders, ga->n_keys,
                    st->agg_builders, ga->n_aggs,
                    st->accums, st->agg_types, ga->agg_specs,
                    st->store_mem, ga->temp_dir);
    st->scan_done = 1;
    return sagg_emit(st, ga);   /* may be an empty batch (0 groups) */
}

/* ================================================================== */
/*  GroupAggNode interface                                            */
/* ================================================================== */

/* Hash path setup. The table (with its spill buffers) is charged half of the
   node budget; the other half goes to the external sort that orders the result
   once the table has overflowed, since from then on both are live. A table that
   never overflows is emitted in key order from an in-RAM permutation and needs
   no sort at all. */
static void group_agg_start_hash(GroupAggNode *ga) {
    int64_t budget = vec_mem_allowance(&ga->mem);
    if (budget == INT64_MAX) budget = VECTRA_SORT_MEM_DEFAULT;
    int64_t table_budget = ga->temp_dir ? budget / 2 : 0;
    HashAggNode *h = (HashAggNode *)hagg_node_create(ga, ga->child, 0, 0,
                                                     table_budget, 1);
    ga->out = (VecNode *)h;
    hagg_consume(h);
    if (h->frozen) {
        h->sorted_emit = 0;
        SortKey *keys = (SortKey *)malloc((size_t)ga->n_keys * sizeof(SortKey));
        for (int k = 0; k < ga->n_keys; k++) {
            keys[k].col_index = k;
            keys[k].descending = 0;
            keys[k].na_last = 0;
        }
        SortNode *sn = sort_node_create((VecNode *)h, ga->n_keys, keys,
                                        ga->temp_dir, budget - table_budget);
        sort_node_share_grant(sn, ga->mem.grant);
        ga->out = (VecNode *)sn;
    }
}

static VecBatch *group_agg_next_batch(VecNode *self) {
    GroupAggNode *ga = (GroupAggNode *)self;
    /* Sorted path streams its output in bounded batches and signals completion
       by returning NULL itself (via SortedAggState.scan_done). */
    if (ga->use_sorted)
        return sorted_agg_next_batch(ga);
    if (ga->out == NULL)
        group_agg_start_hash(ga);
    return ga->out->next_batch(ga->out);
}

static void group_agg_free(VecNode *self) {
    GroupAggNode *ga = (GroupAggNode *)self;
    if (ga->sagg) {
        sagg_free((SortedAggState *)ga->sagg, ga->n_keys, ga->n_aggs);
        ga->sagg = NULL;
    }
    if (ga->out) {
        ga->out->free_node(ga->out);
        ga->out = NULL;
    }
    ga->child->free_node(ga->child);
    for (int k = 0; k < ga->n_keys; k++)
        free(ga->key_names[k]);
    free(ga->key_names);
    for (int a = 0; a < ga->n_aggs; a++) {
        free(ga->agg_specs[a].output_name);
        free(ga->agg_specs[a].input_col);
    }
    free(ga->agg_specs);
    free(ga->temp_dir);
    vec_mem_acct_free(&ga->mem);
    vec_schema_free(&ga->base.output_schema);
    free(ga);
}

VEC_ONE_CHILD_FN(group_agg_children, GroupAggNode, child)

/* The plan's grant for this node, shared by its internal sorts. */
static void group_agg_set_grant(VecNode *self, VecMemGrant *grant) {
    GroupAggNode *ga = (GroupAggNode *)self;
    vec_mem_acct_rebind(&ga->mem, grant);
    if (ga->use_sorted) sort_node_share_grant((SortNode *)ga->child, grant);
}

/* Reserve through a parent's grant rather than as a node of the plan. */
static void group_agg_share_grant(GroupAggNode *ga, VecMemGrant *grant) {
    vec_node_clear_budgeted(&ga->base);
    group_agg_set_grant(&ga->base, grant);
}

static GroupAggNode *group_agg_create(VecNode *child, int n_keys,
                                      char **key_names, int n_aggs,
                                      AggSpec *agg_specs, const char *temp_dir,
                                      int64_t mem_budget, GroupAggMode mode) {
    GroupAggNode *ga = (GroupAggNode *)calloc(1, sizeof(GroupAggNode));
    if (!ga) vectra_error("alloc failed for GroupAggNode");

    ga->mem_budget = mem_budget;
    ga->mem = vec_mem_acct(NULL, mem_budget);
    if (temp_dir) {
        ga->temp_dir = (char *)malloc(strlen(temp_dir) + 1);
        strcpy(ga->temp_dir, temp_dir);
    }

    /* The sort-based path serves median() / n_distinct(), whose per-group
       stores are built to hold one group at a time, and partitions that stayed
       over budget through every level of hash partitioning. Everything else
       aggregates by hash. */
    int has_holistic = 0;
    for (int a = 0; a < n_aggs; a++)
        if (agg_is_holistic(agg_specs[a].kind)) has_holistic = 1;

    if (temp_dir && n_keys > 0 && (mode == GAGG_SORTED || has_holistic)) {
        int64_t sort_mem = mem_budget > 0 ? mem_budget : VECTRA_SORT_MEM_DEFAULT;
        const VecSchema *cs = &child->output_schema;
        SortKey *sort_keys = (SortKey *)malloc((size_t)n_keys * sizeof(SortKey));
        for (int k = 0; k < n_keys; k++) {
            int idx = vec_schema_find_col(cs, key_names[k]);
            if (idx < 0)
                vectra_error("group_by: column not found: %s", key_names[k]);
            sort_keys[k].col_index = idx;
            sort_keys[k].descending = 0;
            sort_keys[k].na_last = 0;   /* cluster NA keys as one group */
        }
        SortNode *sn = sort_node_create(child, n_keys, sort_keys, temp_dir,
                                        sort_mem);
        sort_node_share_grant(sn, NULL);   /* counted through this node */
        child = (VecNode *)sn;
        ga->use_sorted = 1;
    }

    ga->child = child;
    ga->n_keys = n_keys;
    ga->key_names = key_names;
    ga->n_aggs = n_aggs;
    ga->agg_specs = agg_specs;

    /* Build output schema: key columns + agg columns */
    int n_out = n_keys + n_aggs;
    char **out_names = (char **)malloc((size_t)(n_out > 0 ? n_out : 1) * sizeof(char *));
    VecType *out_types = (VecType *)malloc((size_t)(n_out > 0 ? n_out : 1) * sizeof(VecType));

    const VecSchema *cs = &child->output_schema;
    for (int k = 0; k < n_keys; k++) {
        out_names[k] = key_names[k];
        int idx = vec_schema_find_col(cs, key_names[k]);
        out_types[k] = (idx >= 0) ? cs->col_types[idx] : VEC_DOUBLE;
    }
    for (int a = 0; a < n_aggs; a++) {
        out_names[n_keys + a] = agg_specs[a].output_name;
        VecType it = VEC_DOUBLE;
        if (agg_specs[a].kind != AGG_COUNT_STAR && agg_specs[a].input_col) {
            int ci = vec_schema_find_col(cs, agg_specs[a].input_col);
            if (ci >= 0) it = cs->col_types[ci];
        }
        out_types[n_keys + a] = agg_output_type(agg_specs[a].kind, it);
    }

    ga->base.output_schema = vec_schema_create(n_out, out_names, out_types);
    free(out_names);
    free(out_types);

    ga->base.next_batch = group_agg_next_batch;
    ga->base.kind = "GroupAggNode";
    ga->base.children = group_agg_children;
    vec_node_set_budgeted(&ga->base, mem_budget, group_agg_set_grant);
    ga->base.free_node = group_agg_free;

    return ga;
}

GroupAggNode *group_agg_node_create(VecNode *child,
                                    int n_keys, char **key_names,
                                    int n_aggs, AggSpec *agg_specs,
                                    const char *temp_dir, int64_t mem_budget) {
    return group_agg_create(child, n_keys, key_names, n_aggs, agg_specs,
                            temp_dir, mem_budget, GAGG_AUTO);
}
