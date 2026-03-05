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
    JoinKey  *keys;
    char     *suffix_x;
    char     *suffix_y;

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
    int       *l_non_key_idx;  /* indices of non-key left columns */
    int        l_non_key_count;
    int64_t    finalize_cursor; /* current build row in finalize phase */
} JoinNode;

JoinNode *join_node_create(VecNode *left, VecNode *right,
                           JoinKind kind, int n_keys, JoinKey *keys,
                           const char *suffix_x, const char *suffix_y);

#endif /* VECTRA_JOIN_H */
