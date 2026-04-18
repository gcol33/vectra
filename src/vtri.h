#ifndef VECTRA_VTRI_H
#define VECTRA_VTRI_H

#include "types.h"
#include <stdint.h>

/*
 * VtrIndex: persistent on-disk hash index (.vtri sidecar file).
 *
 * Maps key hashes → row group indices for O(1) equality lookups.
 * Built explicitly via create_index(), loaded automatically by ScanNode
 * when a .vtri file exists alongside the .vtr file.
 *
 * File format:
 *   "VTRI" magic (4 bytes)
 *   version: u16 (1)
 *   col_idx: u16 (indexed column in file schema)
 *   ci: u8 (case-insensitive flag)
 *   n_entries: u64
 *   n_slots: u64
 *   entry_hash[n_entries]: u64 each
 *   entry_rg[n_entries]: u32 each
 *   heads[n_slots]: i64 each (-1 = empty)
 *   entry_next[n_entries]: i64 each (-1 = end of chain)
 */

typedef struct VtrIndex {
    uint16_t  col_idx;      /* primary column index (v1) */
    uint8_t   ci;
    int64_t   n_entries;
    int64_t   n_slots;
    uint64_t *entry_hash;   /* [n_entries] */
    uint32_t *entry_rg;     /* [n_entries] — row group index */
    int64_t  *heads;        /* [n_slots] */
    int64_t  *entry_next;   /* [n_entries] */
    char     *col_name;     /* column name (resolved from schema at load time; v1 compat) */
    /* v2: composite index fields */
    uint16_t  n_cols;       /* number of indexed columns (1 for v1) */
    uint16_t *col_indices;  /* [n_cols] column indices */
    char    **col_names;    /* [n_cols] column names (resolved at load time) */
} VtrIndex;

/* ---- Hashing helpers (shared between vtri.c and scan.c) ---- */

#define VTRI_FNV_OFFSET 0xcbf29ce484222325ULL
#define VTRI_FNV_PRIME  0x00000100000001B3ULL

static inline uint64_t vtri_fnv1a(const uint8_t *data, int64_t len) {
    uint64_t h = VTRI_FNV_OFFSET;
    for (int64_t i = 0; i < len; i++) { h ^= data[i]; h *= VTRI_FNV_PRIME; }
    return h;
}

static inline uint64_t vtri_fnv1a_ci(const char *s, int64_t len) {
    uint64_t h = VTRI_FNV_OFFSET;
    for (int64_t i = 0; i < len; i++) {
        h ^= (uint8_t)((unsigned char)s[i] >= 'A' && (unsigned char)s[i] <= 'Z'
              ? (unsigned char)s[i] + 32 : (unsigned char)s[i]);
        h *= VTRI_FNV_PRIME;
    }
    return h;
}

static inline uint64_t vtri_hash_int64(int64_t val) {
    return vtri_fnv1a((const uint8_t *)&val, 8);
}

static inline uint64_t vtri_hash_double(double val) {
    if (val == 0.0) val = 0.0; /* normalize -0 */
    return vtri_fnv1a((const uint8_t *)&val, 8);
}

/* Open a .vtri sidecar index file. Returns NULL if file doesn't exist. */
VtrIndex *vtri_open(const char *vtri_path, const VecSchema *schema);

/* Close and free an index. */
void vtri_close(VtrIndex *idx);

/* Build and write a .vtri index for a column in a .vtr file.
   vtr_path: path to .vtr file
   col_name: column to index
   ci: case-insensitive flag */
void vtri_build(const char *vtr_path, const char *col_name, int ci);

/* Build a composite index on multiple columns (v2 format).
   col_names: array of column name strings
   n_cols: number of columns */
void vtri_build_composite(const char *vtr_path, const char **col_names,
                          int n_cols, int ci);

/* Probe the index with a string key.
   Returns a row-group bitmap (caller frees).
   bitmap[rg] = 1 if key might be in that row group. */
uint8_t *vtri_probe_string(const VtrIndex *idx, const char *key,
                           int64_t key_len, uint32_t n_rowgroups);

/* Probe the index with an int64 key. */
uint8_t *vtri_probe_int64(const VtrIndex *idx, int64_t key,
                          uint32_t n_rowgroups);

/* Probe the index with a double key. */
uint8_t *vtri_probe_double(const VtrIndex *idx, double key,
                           uint32_t n_rowgroups);

/* Probe composite index with array of hash values (one per indexed column).
   Returns row-group bitmap. */
uint8_t *vtri_probe_composite(const VtrIndex *idx, const uint64_t *col_hashes,
                              int n_cols, uint32_t n_rowgroups);

/* Construct the .vtri path from a .vtr path and column name.
   Returns malloc'd string. Caller frees. */
char *vtri_make_path(const char *vtr_path, const char *col_name);

/* Construct composite .vtri path from multiple column names.
   Format: <vtr_path>.<col1>_<col2>_...<colN>.vtri */
char *vtri_make_path_composite(const char *vtr_path, const char **col_names,
                               int n_cols);

#endif /* VECTRA_VTRI_H */
