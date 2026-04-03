#ifndef VECTRA_BLOCK_H
#define VECTRA_BLOCK_H

#include "types.h"
#include <stdint.h>

/*
 * ColumnBlock: materialized columnar data, reusable across lookups.
 *
 * Unlike VecNode (pull-based, single-use), a ColumnBlock is a persistent
 * in-memory column store.  Created by consuming a VecNode, then used for
 * repeated hash lookups without re-scanning.
 *
 * Usage pattern:
 *   ColumnBlock *blk = block_materialize(node);
 *   // ... multiple block_lookup() calls ...
 *   block_free(blk);
 */
typedef struct ColumnBlock ColumnBlock;
typedef struct BlockIndex  BlockIndex;

struct ColumnBlock {
    int64_t    n_rows;
    int        n_cols;
    VecSchema  schema;
    VecArray  *columns;      /* n_cols arrays, each of length n_rows */

    /* Cached hash indices (lazily built, one per column) */
    BlockIndex **indices;    /* [n_cols], NULL until first lookup */
    BlockIndex **ci_indices; /* [n_cols], NULL until first CI lookup */
};

/*
 * BlockIndex: open-addressing hash table mapping string keys to row lists.
 *
 * For each unique key, stores a chain of row indices (handles duplicate
 * keys, e.g. synonyms with the same canonical name).
 *
 * Slot layout (parallel arrays, cache-friendly):
 *   heads[slot]      = first entry index at this slot, or -1
 *   entry_hash[i]    = hash of entry i
 *   entry_row[i]     = row index in ColumnBlock
 *   entry_next[i]    = next entry in collision chain, or -1
 */
struct BlockIndex {
    int64_t  n_slots;        /* power of 2 */
    int64_t  n_entries;
    int64_t *heads;          /* [n_slots] */
    uint64_t *entry_hash;    /* [n_entries] */
    int64_t  *entry_row;     /* [n_entries] */
    int64_t  *entry_next;    /* [n_entries] */

    /* Source column reference (for key comparison during probe) */
    const VecArray *col;     /* points into block->columns[col_idx] */
    int ci;                  /* case-insensitive flag */
};

/* --- Core API --- */

/* Consume a VecNode, materializing all batches into a reusable block. */
ColumnBlock *block_materialize(VecNode *node);

/* Free a block and all cached indices. */
void block_free(ColumnBlock *blk);

/* Build a hash index on a string column.  Cached on the block. */
BlockIndex *block_get_index(ColumnBlock *blk, int col_idx, int ci);

/*
 * Probe a hash index with a vector of query keys.
 *
 * For each key in keys[0..n_keys), finds all matching rows in the block.
 * Returns parallel arrays: out_query_idx[i], out_block_row[i] for each
 * (query, block_row) match pair.  Returns total number of matches.
 *
 * Caller must free *out_query_idx and *out_block_row.
 */
int64_t block_probe(const BlockIndex *idx,
                    const char **keys, const int *key_lens, int64_t n_keys,
                    int64_t **out_query_idx, int64_t **out_block_row);

#endif /* VECTRA_BLOCK_H */
