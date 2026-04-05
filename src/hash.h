#ifndef VECTRA_HASH_H
#define VECTRA_HASH_H

#include "types.h"
#include <stdint.h>

/* Hash a single value. Returns a 64-bit hash. */
uint64_t vec_hash_value(const VecArray *arr, int64_t row);

/* Combine two hashes */
static inline uint64_t vec_hash_combine(uint64_t h1, uint64_t h2) {
    /* Rotate h1 left by 5 then XOR with h2 */
    return ((h1 << 5) | (h1 >> 59)) ^ h2;
}

/* Compare single values in two arrays at given rows */
int vec_val_equal(const VecArray *a, int64_t ra, const VecArray *b, int64_t rb);

/* Compare two rows across key columns */
int vec_keys_equal(const VecArray *keys_a, int n_keys, int64_t row_a,
                   const VecArray *keys_b, int64_t row_b);

/* --- Open-addressing hash table --- */

/* Cache-friendly entry: slot + hash co-located in one cache line */
typedef struct {
    int64_t  slot;         /* -1 = empty, otherwise group_id */
    uint64_t hash;
} VecHTEntry;

typedef struct {
    int64_t    n_slots;
    int64_t    n_groups;
    VecHTEntry *entries;   /* co-located slot+hash for cache efficiency */
} VecHashTable;

VecHashTable vec_ht_create(int64_t initial_cap);
void vec_ht_free(VecHashTable *ht);

/* Find or insert. Returns group_id (0-based, insertion order).
   If inserted, *was_new = 1. */
int64_t vec_ht_find_or_insert(VecHashTable *ht, uint64_t hash,
                               const VecArray *keys, int n_keys,
                               int64_t row,
                               const VecArray *key_arena, int64_t arena_len,
                               int *was_new);

#endif /* VECTRA_HASH_H */
