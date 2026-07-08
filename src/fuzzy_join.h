#ifndef VECTRA_FUZZY_JOIN_H
#define VECTRA_FUZZY_JOIN_H

#include "types.h"
#include "join_partition.h"

/* Distance method for fuzzy matching */
typedef enum {
    FUZZY_DL,           /* Damerau-Levenshtein (normalized) */
    FUZZY_LEVENSHTEIN,  /* Levenshtein (normalized) */
    FUZZY_JW            /* Jaro-Winkler (1 - similarity) */
} FuzzyMethod;

/* A single match triple: probe row matched build row at distance */
typedef struct {
    int64_t probe_idx;  /* row index into materialized probe */
    int64_t build_idx;  /* row index into materialized build */
    double  dist;       /* normalized distance (0 = exact) */
} FuzzyMatch;

/* Growable buffer for match triples (one per thread) */
typedef struct {
    FuzzyMatch *buf;
    int64_t     count;
    int64_t     capacity;
} FuzzyMatchBuf;

/* State machine */
typedef enum {
    FSTATE_BUILD,   /* materialize + block-index the build side (once) */
    FSTATE_STREAM,  /* pull probe batches, emit their matches in chunks */
    FSTATE_DONE
} FuzzyState;

/* Build-side block index: block key -> list of build rows. Private to the
   implementation; NULL when no blocking column is used. */
struct BlockIndex;

typedef struct {
    VecNode    base;

    /* Child nodes (owned) */
    VecNode   *probe_node;
    VecNode   *build_node;

    /* Column indices (resolved from names) */
    int        probe_key_col;     /* fuzzy distance column in probe */
    int        build_key_col;     /* fuzzy distance column in build */
    int        probe_block_col;   /* blocking key in probe (-1 = none) */
    int        build_block_col;   /* blocking key in build (-1 = none) */

    /* Config */
    FuzzyMethod method;
    double      max_dist;
    int         n_threads;

    /* Probe side is streamed, not materialized; only its column count is kept. */
    int         p_ncols;

    /* Materialized build side (resident: every probe row compares against it).
       When it fits mem_budget this holds the whole build side; when it overflows
       it is spilled to build_spill_path instead and b_cols stays NULL. */
    int         b_ncols;
    VecArray   *b_cols;
    int64_t     b_nrows;

    /* Build-side block index (NULL when no blocking) */
    struct BlockIndex *bidx;

    /* Bounded-memory spill of the build side. When the build side exceeds
       mem_budget it is written to a .vtr run file in budget-sized rowgroups;
       each probe batch is then matched against one rowgroup chunk at a time
       (loaded into chunk_cols with a per-chunk block index), so peak build
       state is one rowgroup, not the whole side. */
    int64_t     mem_budget;
    char       *temp_dir;         /* owned run-file dir */
    int         spilled;          /* 1 = build side lives in build_file */
    char       *build_spill_path; /* owned run-file path */
    void       *build_file;       /* Vtr1TdcFile* reader (opaque) */
    uint32_t    build_n_rgs;      /* rowgroups in the spill file */
    uint32_t    chunk_rg;         /* next rowgroup for the current probe batch */
    VecBatch   *chunk_batch;      /* one resident build rowgroup (owned) */
    VecArray   *chunk_cols;       /* alias of chunk_batch->columns */
    int64_t     chunk_nrows;
    struct BlockIndex *chunk_bidx;/* block index over the current chunk */

    /* Build columns the current matches emit from: b_cols (resident) or
       chunk_cols (spilled). */
    const VecArray *emit_bcols;

    /* Streaming state: the probe batch being emitted and its matches. The
       match buffer is bounded to one probe batch, never the whole cross set. */
    VecBatch   *cur_batch;       /* current probe batch (owned until drained) */
    FuzzyMatch *cur_matches;     /* matches for cur_batch (probe_idx = local row) */
    int64_t     cur_n;           /* number of matches in cur_matches */
    int64_t     emit_pos;        /* cursor into cur_matches */

    /* Output state */
    FuzzyState  state;

    /* Output schema column mapping */
    int         out_ncols;       /* total output columns */
    char       *suffix_y;        /* suffix for build columns on collision */
} FuzzyJoinNode;


FuzzyJoinNode *fuzzy_join_node_create(
    VecNode     *probe,
    VecNode     *build,
    int          probe_key_col,
    int          build_key_col,
    int          probe_block_col,   /* -1 for no blocking */
    int          build_block_col,   /* -1 for no blocking */
    FuzzyMethod  method,
    double       max_dist,
    int          n_threads,
    const char  *suffix_y,
    int64_t      mem_budget,        /* build-side spill threshold; <=0 => default */
    const char  *temp_dir           /* run-file dir (copied) */
);

#endif /* VECTRA_FUZZY_JOIN_H */
