#include "hash.h"
#include "array.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* FNV-1a constants */
#define FNV_OFFSET 14695981039346656037ULL
#define FNV_PRIME  1099511628211ULL

uint64_t vec_hash_value(const VecArray *arr, int64_t row) {
    if (!vec_array_is_valid(arr, row))
        return FNV_OFFSET ^ 0xFF; /* hash for NA */

    uint64_t h = FNV_OFFSET;
    switch (arr->type) {
    case VEC_INT64: {
        const uint8_t *p = (const uint8_t *)&arr->buf.i64[row];
        for (int k = 0; k < 8; k++) { h ^= p[k]; h *= FNV_PRIME; }
        break;
    }
    case VEC_DOUBLE: {
        double v = arr->buf.dbl[row];
        /* Normalize -0 to +0 */
        if (v == 0.0) v = 0.0;
        const uint8_t *p = (const uint8_t *)&v;
        for (int k = 0; k < 8; k++) { h ^= p[k]; h *= FNV_PRIME; }
        break;
    }
    case VEC_BOOL: {
        h ^= arr->buf.bln[row]; h *= FNV_PRIME;
        break;
    }
    case VEC_STRING: {
        int64_t s = arr->buf.str.offsets[row];
        int64_t e = arr->buf.str.offsets[row + 1];
        assert(arr->buf.str.data != NULL && "hash: string data pointer is NULL");
        const uint8_t *p = (const uint8_t *)(arr->buf.str.data + s);
        for (int64_t k = 0; k < (e - s); k++) { h ^= p[k]; h *= FNV_PRIME; }
        break;
    }
    }
    return h;
}

/* Compare single values */
static int val_equal(const VecArray *a, int64_t ra, const VecArray *b, int64_t rb) {
    int va = vec_array_is_valid(a, ra);
    int vb = vec_array_is_valid(b, rb);
    if (!va && !vb) return 1; /* both NA */
    if (!va || !vb) return 0;

    switch (a->type) {
    case VEC_INT64:  return a->buf.i64[ra] == b->buf.i64[rb];
    case VEC_DOUBLE: return a->buf.dbl[ra] == b->buf.dbl[rb];
    case VEC_BOOL:   return a->buf.bln[ra] == b->buf.bln[rb];
    case VEC_STRING: {
        int64_t as = a->buf.str.offsets[ra], ae = a->buf.str.offsets[ra + 1];
        int64_t bs = b->buf.str.offsets[rb], be = b->buf.str.offsets[rb + 1];
        int64_t alen = ae - as, blen = be - bs;
        if (alen != blen) return 0;
        assert(a->buf.str.data != NULL && "val_equal: lhs string data is NULL");
        assert(b->buf.str.data != NULL && "val_equal: rhs string data is NULL");
        return memcmp(a->buf.str.data + as, b->buf.str.data + bs, (size_t)alen) == 0;
    }
    }
    return 0;
}

int vec_keys_equal(const VecArray *keys_a, int n_keys, int64_t row_a,
                   const VecArray *keys_b, int64_t row_b) {
    for (int k = 0; k < n_keys; k++) {
        if (!val_equal(&keys_a[k], row_a, &keys_b[k], row_b))
            return 0;
    }
    return 1;
}

VecHashTable vec_ht_create(int64_t initial_cap) {
    VecHashTable ht;
    ht.n_slots = initial_cap;
    ht.n_groups = 0;
    ht.slots = (int64_t *)malloc((size_t)initial_cap * sizeof(int64_t));
    ht.hashes = (uint64_t *)malloc((size_t)initial_cap * sizeof(uint64_t));
    if (!ht.slots || !ht.hashes) vectra_error("alloc failed for hash table");
    memset(ht.slots, -1, (size_t)initial_cap * sizeof(int64_t));
    return ht;
}

void vec_ht_free(VecHashTable *ht) {
    free(ht->slots);
    free(ht->hashes);
    ht->slots = NULL;
    ht->hashes = NULL;
    ht->n_slots = 0;
    ht->n_groups = 0;
}

static void ht_resize(VecHashTable *ht, const VecArray *key_arena,
                       int n_keys, int64_t arena_len);

int64_t vec_ht_find_or_insert(VecHashTable *ht, uint64_t hash,
                               const VecArray *keys, int n_keys,
                               int64_t row,
                               const VecArray *key_arena, int64_t arena_len,
                               int *was_new) {
    /* Resize if load > 70% */
    if (ht->n_groups * 10 > ht->n_slots * 7) {
        ht_resize(ht, key_arena, n_keys, arena_len);
    }

    int64_t mask = ht->n_slots - 1;
    int64_t idx = (int64_t)(hash & (uint64_t)mask);

    for (;;) {
        if (ht->slots[idx] == -1) {
            /* Empty slot: insert */
            ht->slots[idx] = ht->n_groups;
            ht->hashes[idx] = hash;
            *was_new = 1;
            return ht->n_groups++;
        }
        if (ht->hashes[idx] == hash) {
            int64_t gid = ht->slots[idx];
            /* Compare keys: arena[gid] vs keys[row] */
            if (vec_keys_equal(key_arena, n_keys, gid, keys, row)) {
                *was_new = 0;
                return gid;
            }
        }
        idx = (idx + 1) & mask;
    }
}

static void ht_resize(VecHashTable *ht, const VecArray *key_arena,
                       int n_keys, int64_t arena_len) {
    int64_t old_slots = ht->n_slots;
    int64_t *old_slot_data = ht->slots;
    uint64_t *old_hash_data = ht->hashes;

    int64_t new_n = old_slots * 2;
    ht->n_slots = new_n;
    ht->slots = (int64_t *)malloc((size_t)new_n * sizeof(int64_t));
    ht->hashes = (uint64_t *)malloc((size_t)new_n * sizeof(uint64_t));
    if (!ht->slots || !ht->hashes) vectra_error("alloc failed for hash table resize");
    memset(ht->slots, -1, (size_t)new_n * sizeof(int64_t));

    int64_t mask = new_n - 1;
    for (int64_t i = 0; i < old_slots; i++) {
        if (old_slot_data[i] == -1) continue;
        uint64_t h = old_hash_data[i];
        int64_t idx = (int64_t)(h & (uint64_t)mask);
        while (ht->slots[idx] != -1) idx = (idx + 1) & mask;
        ht->slots[idx] = old_slot_data[i];
        ht->hashes[idx] = h;
    }

    free(old_slot_data);
    free(old_hash_data);

    (void)key_arena;
    (void)n_keys;
    (void)arena_len;
}
