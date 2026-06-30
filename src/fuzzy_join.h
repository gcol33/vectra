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
    FSTATE_MATERIALIZE,
    FSTATE_EMIT,
    FSTATE_DONE
} FuzzyState;

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

    /* Materialized probe side */
    int         p_ncols;
    VecArray   *p_cols;
    int64_t     p_nrows;

    /* Materialized build side */
    int         b_ncols;
    VecArray   *b_cols;
    int64_t     b_nrows;

    /* Partitions (one per unique block key value) */
    JoinPartition *probe_parts;
    JoinPartition *build_parts;
    int64_t        n_parts;

    /* Match results (merged from all threads) */
    FuzzyMatch *matches;
    int64_t     n_matches;

    /* Output state */
    FuzzyState  state;
    int64_t     emit_pos;        /* cursor into matches array */

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
    const char  *suffix_y
);

#endif /* VECTRA_FUZZY_JOIN_H */
