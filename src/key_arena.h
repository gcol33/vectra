#ifndef VECTRA_KEY_ARENA_H
#define VECTRA_KEY_ARENA_H

#include "types.h"

/* Key arena: stores one copy of each unique group-key combination discovered
   during a hash aggregation. Column k of the arena holds, at row g, the value
   of key column k for group g (0-based, insertion order). Used together with
   the VecHashTable in hash.h: the hash table maps a probe row to a group id,
   the arena holds the canonical key values so they can be emitted at the end.

   String key columns borrow their bytes from a per-column growable buffer
   (str_data), so the arena arrays carry owns_data == 0; the buffer is freed by
   key_arena_free. Shared by group_agg (summarise) and kmer (k-mer spectrum). */
typedef struct {
    int        n_keys;
    VecType   *key_types;
    int64_t    capacity;
    int64_t    length;        /* number of groups stored */
    VecArray  *arenas;        /* one array per key column */
    char     **str_data;      /* per-column string byte buffer */
    int64_t   *str_data_len;
    int64_t   *str_data_cap;
} KeyArena;

/* Initialize an arena for n_keys columns of the given types. */
void key_arena_init(KeyArena *ka, int n_keys, VecType *key_types);

/* Append the key values at `row` of `keys` (an array of n_keys VecArrays) as a
   new group. Grows storage as needed. */
void key_arena_append_row(KeyArena *ka, const VecArray *keys, int64_t row);

/* Free all arena storage. */
void key_arena_free(KeyArena *ka);

#endif /* VECTRA_KEY_ARENA_H */
