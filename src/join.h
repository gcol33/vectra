#ifndef VECTRA_JOIN_H
#define VECTRA_JOIN_H

#include "types.h"

typedef enum {
    JOIN_INNER,
    JOIN_LEFT,
    JOIN_FULL,
    JOIN_SEMI,
    JOIN_ANTI
} JoinKind;

typedef struct {
    int   left_col;    /* column index in left schema */
    int   right_col;   /* column index in right schema */
} JoinKey;

/*
 * Join execution states:
 *   BUILD:    materialize right (build) side into hash table
 *   PROBE:    stream left (probe) side batch-by-batch
 *   FINALIZE: emit unmatched build rows (full_join only)
 *   DONE:     no more output
 */
typedef enum {
    JSTATE_BUILD,
    JSTATE_PROBE,
    JSTATE_MERGE,
    JSTATE_FINALIZE,
    JSTATE_DONE
} JoinState;

/*
 * JoinHT: open-addressing hash table mapping composite keys to linked
 * lists of build-side row indices.
 */
typedef struct {
    int64_t   n_slots;
    int64_t  *head;       /* head[slot] = first build row, or -1 */
    uint64_t *slot_hash;  /* hash per slot */
    int64_t  *build_next; /* build_next[row] = next row in chain, or -1 */
    int64_t   n_build;    /* total build rows */
} JoinHT;

typedef struct {
    VecNode   base;
    VecNode  *left;
    VecNode  *right;
    JoinKind  kind;
    int       n_keys;
    int       na_matches;    /* 1 = NA matches NA (dplyr default), 0 = SQL */
    JoinKey  *keys;
    char     *suffix_x;
    char     *suffix_y;

    /* Grace-hash spill: when the materialized build side exceeds mem_budget
       bytes, both sides are hash-partitioned to run-files and joined one
       partition at a time. mem_budget <= 0 disables spilling (unbounded).
       A partition still over budget is re-partitioned by its sub-join with a
       depth-salted hash (so colliding keys redistribute across levels); a
       partition a single hot key makes un-splittable falls back to a
       block-nested-loop at JOIN_MAX_SPILL_DEPTH. Peak stays bounded regardless
       of key skew. */
    int64_t   mem_budget;
    char     *temp_dir;       /* directory for partition spill files */
    int       spill_depth;    /* grace-hash recursion depth (0 at the top) */

    int        spill;         /* 1 = partitioned spill mode active */
    int        n_parts;       /* partition count (K) */
    char     **left_parts;    /* K left-side partition .vtr paths */
    char     **right_parts;   /* K right-side partition .vtr paths */
    int        cur_part;      /* partition currently being joined */
    VecNode   *sub_join;      /* active sub-join over cur_part (owns its scans) */

    /* Block-nested-loop terminal fallback (single-hot-key partition). The build
       side is streamed in <= mem_budget blocks; the probe side is re-scanned
       once per block. Peak = one build block + one probe batch, plus 1-bit/row
       matched bitsets. */
    int        bnl;           /* 1 = block-nested-loop mode active */
    char      *bnl_rpath;     /* whole build (right) partition, one .vtr file */
    char      *bnl_lpath;     /* whole probe (left) partition, one .vtr file */
    int64_t    bnl_rrows;     /* total build rows */
    int64_t    bnl_lrows;     /* total probe rows */
    VecNode   *bnl_rscan;     /* sequential scan over the build file (blocks) */
    VecNode   *bnl_pscan;     /* current scan over the probe file (per block) */
    int64_t    bnl_block_base;/* global build-row ordinal at start of block */
    int64_t    bnl_pbase;     /* global probe-row ordinal at current pscan pos */
    int        bnl_stage;     /* 0 = load block, 1 = probe block, 2 = finalize */
    int        bnl_fin_side;  /* finalize sub-stage: 0 = probe scan, 1 = build */
    uint8_t   *bnl_pmatched;  /* bitset over probe rows (non-inner kinds) */
    uint8_t   *bnl_bmatched;  /* bitset over build rows (full only) */
    /* Resumable cursor over one BNL probe batch (bounds the many-to-many emit
       against a build block, the same way the resident probe path does). */
    VecBatch  *bnl_pb;           /* active probe batch (owned); NULL = none */
    VecArray  *bnl_pcols;        /* coerced probe key columns */
    VecArray  *bnl_pcoerced[16]; /* owned coerced key arrays, freed at drain */
    VecArray  *bnl_phcols;       /* hash_cols out-param from coerce */
    int64_t    bnl_probe_li;     /* next logical probe row in the batch */
    int64_t    bnl_chain_br;     /* resume build row mid-chain, or -1 */

    /* State machine */
    JoinState state;

    /* Build-side materialized data (owned, survives across next_batch calls) */
    int        r_ncols;
    VecArray  *r_cols;       /* materialized build columns */
    JoinHT     jht;          /* hash table over r_cols */
    int       *rkey_idx;     /* key column indices into r_cols */
    int       *lkey_idx;     /* key column indices into probe batches */

    /* Output column mapping (precomputed) */
    int       *r_non_key_idx;  /* indices of non-key right columns */
    int        r_non_key_count;

    /* full_join only */
    uint8_t   *build_matched;  /* bitset: which build rows were matched */
    int64_t    finalize_cursor; /* current build row in finalize phase */

    /* Merge join state (used when use_merge == 1) */
    int        use_merge;        /* 1 = merge join, 0 = hash join */
    int64_t    merge_r_cursor;   /* current position in sorted build side */
    VecBatch  *merge_l_batch;    /* current left batch being consumed */
    int64_t    merge_l_pos;      /* current logical row in merge_l_batch */
    int        merge_l_done;     /* left side exhausted */
    int64_t    merge_r_group;    /* start of current equal-key group in build */
    int64_t    merge_r_group_end;/* end (exclusive) of current group */
    int64_t    merge_r_sub;      /* current position within group (for M:N) */

    /* Resumable hash-probe cursor: bounds the many-to-many output so one hot
       key in a probe batch cannot materialize batch_size * chain_len rows at
       once. State persists across next_batch calls for one probe batch. */
    VecBatch  *probe_cur;            /* active probe batch (owned); NULL = none */
    VecArray  *probe_cols;           /* coerced probe key columns (from coerce) */
    VecArray  *probe_coerced[16];    /* owned coerced key arrays, freed at drain */
    VecArray  *probe_hash_cols;      /* hash_cols out-param from coerce */
    uint64_t  *probe_hash;           /* per-logical-row key hashes */
    uint8_t   *probe_matched;        /* left/full: matched-probe bitset (else NULL) */
    int64_t    probe_li;             /* next logical row (match phase) */
    int64_t    probe_chain_br;       /* resume build row mid-chain, or -1 */
    int        probe_phase;          /* 0 = matching, 1 = emit-unmatched (left/full) */
    int64_t    probe_unmatched_li;   /* cursor for the emit-unmatched phase */
} JoinNode;

JoinNode *join_node_create(VecNode *left, VecNode *right,
                           JoinKind kind, int n_keys, JoinKey *keys,
                           const char *suffix_x, const char *suffix_y,
                           int64_t mem_budget, const char *temp_dir);

#endif /* VECTRA_JOIN_H */
