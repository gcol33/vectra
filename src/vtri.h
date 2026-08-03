#ifndef VECTRA_VTRI_H
#define VECTRA_VTRI_H

#include "types.h"
#include "file_map.h"
#include <stdint.h>
#include <string.h>

/*
 * VtrIndex: persistent on-disk hash index (.vtri sidecar file).
 *
 * Maps key hashes -> row group indices, so an equality predicate can name the
 * row groups that may hold a key without reading any column data. Built
 * explicitly via create_index(); a ScanNode opens the sidecar for the column
 * its predicate actually filters on.
 *
 * One entry per (distinct key hash, row group), NOT one per row: an index over
 * a column with few distinct values stays small no matter how many rows the
 * store holds, which is what keeps both the file and the open cost off the
 * store size. A row group repeats a key only once, so the entry count is
 * sum over row groups of the distinct keys in that row group.
 *
 * The header stamps the row and row-group counts the index was built against.
 * vtri_open() compares them with the store it is being opened for and reports
 * no index when they disagree: a row append rewrites every row group, so an
 * index built before it maps keys to row groups that have moved, and probing it
 * would silently drop rows.
 *
 * File format (version 3; a single layout for one or many columns):
 *   "VTRI" magic (4 bytes)
 *   version: u16 (3)
 *   n_cols: u16 (number of indexed columns)
 *   ci: u8 (case-insensitive flag)
 *   col_indices[n_cols]: u16 each (schema column indices, ascending)
 *   src_n_rows: u64 (rows in the store at build time)
 *   src_n_rowgroups: u32 (row groups in the store at build time)
 *   n_entries: u64
 *   n_slots: u64
 *   entry_hash[n_entries]: u64 each
 *   entry_rg[n_entries]: u32 each
 *   heads[n_slots]: i64 each (-1 = empty)
 *   entry_next[n_entries]: i64 each (-1 = end of chain)
 *
 * Versions 1 and 2 (one entry per row, no stamp) are superseded: vtri_open()
 * reports no index for them, and vtri_read_spec() reads their column list so
 * they can be rebuilt.
 *
 * An open index is backed one of two ways, chosen by size, and a probe reads
 * through accessors so it does not care which:
 *
 *   resident  the four arrays are read into memory. Costs one copy of the file
 *             and holds no handle afterwards.
 *   mapped    the file is mapped read-only and the arrays are read out of the
 *             mapping in place. Costs the pages a probe touches, so an index
 *             far larger than RAM is still probeable, and a probe's cost stays
 *             off the size of the index.
 *
 * Reading the file whole is the cheaper of the two while it is small, and it
 * leaves nothing open behind it; past VTRI_RESIDENT_MAX_BYTES that copy is what
 * a lookup pays for, and a chained-hash probe touches a handful of pages
 * whatever the file's size, so the mapping takes over. An index too large to be
 * mapped reports absent, as any other unusable index does -- a lookup falls
 * back to reading the store instead of exhausting memory.
 */

#define VTRI_MAX_COLS 8
#define VTRI_VERSION  3

/* Read an index below this size, map it above. A few MB reads in a couple of
   milliseconds and costs one allocation that is freed straight after, which
   beats faulting pages in for a single probe; from here up, reading grows with
   the index while a mapped probe does not. */
#define VTRI_RESIDENT_MAX_BYTES (4LL * 1024 * 1024)

typedef struct VtrIndex {
    uint16_t  col_idx;      /* first indexed column (== col_indices[0]) */
    uint8_t   ci;
    int64_t   n_entries;
    int64_t   n_slots;
    int64_t   src_n_rows;      /* rows in the store at build time */
    int64_t   src_n_rowgroups; /* row groups in the store at build time */

    /* Resident backing: NULL when the index is mapped instead. */
    uint64_t *entry_hash;   /* [n_entries] */
    uint32_t *entry_rg;     /* [n_entries] — row group index */
    int64_t  *heads;        /* [n_slots] */
    int64_t  *entry_next;   /* [n_entries] */

    /* Mapped backing: map.base is NULL when the index is resident instead.
       The offsets locate each array within the file and carry no alignment,
       so vtri_entry_hash() and friends memcpy rather than dereference. */
    VecFileMap map;
    int64_t   off_hash;
    int64_t   off_rg;
    int64_t   off_heads;
    int64_t   off_next;

    char     *col_name;     /* first column name (resolved from schema at load time) */
    uint16_t  n_cols;       /* number of indexed columns */
    uint16_t *col_indices;  /* [n_cols] column indices */
    char    **col_names;    /* [n_cols] column names (resolved at load time) */
} VtrIndex;

/* ---- Backing-agnostic element reads ----

   Every index array is read through one of these, so the probe and the rebuild
   are written once and work against either backing. Indices are assumed in
   range; vtri_open validates the file's declared sizes against its actual
   length, and probe_by_hash bounds every chain step it takes. */

static inline uint64_t vtri_entry_hash(const struct VtrIndex *idx, int64_t i) {
    if (idx->entry_hash) return idx->entry_hash[i];
    uint64_t v;
    memcpy(&v, idx->map.base + idx->off_hash + i * 8, 8);
    return v;
}

static inline uint32_t vtri_entry_rg(const struct VtrIndex *idx, int64_t i) {
    if (idx->entry_rg) return idx->entry_rg[i];
    uint32_t v;
    memcpy(&v, idx->map.base + idx->off_rg + i * 4, 4);
    return v;
}

static inline int64_t vtri_head(const struct VtrIndex *idx, int64_t slot) {
    if (idx->heads) return idx->heads[slot];
    int64_t v;
    memcpy(&v, idx->map.base + idx->off_heads + slot * 8, 8);
    return v;
}

static inline int64_t vtri_entry_next(const struct VtrIndex *idx, int64_t i) {
    if (idx->entry_next) return idx->entry_next[i];
    int64_t v;
    memcpy(&v, idx->map.base + idx->off_next + i * 8, 8);
    return v;
}

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

/* Open a .vtri sidecar index file.

   src_n_rows / src_n_rowgroups describe the store the index is about to be used
   against; the index is reported as absent (NULL) when its stamp disagrees with
   them, which is how an index left behind by a row append is kept from pruning.
   Pass -1 for either to skip that check.

   Returns NULL whenever there is no index to probe: the file does not exist, is
   not a .vtri, was written by a superseded or newer version, does not match the
   store, is malformed, or is too large both to read and to map. An index only
   ever saves a scan work, so an unusable one costs speed and never rows --
   reporting absent keeps a bad sidecar from turning a readable store into an
   unopenable one. Allocation failure on the resident path still raises, being a
   fact about the machine rather than about the index; running out of memory is
   not how a large index is handled, since one past
   VTRI_RESIDENT_MAX_BYTES is mapped rather than read. */
VtrIndex *vtri_open(const char *vtri_path, const VecSchema *schema,
                    int64_t src_n_rows, int64_t src_n_rowgroups);

/* Close and free an index. */
void vtri_close(VtrIndex *idx);

/* Read only the column list of a .vtri file, for any version, so a superseded
   index can be rebuilt from the columns it was built on. Writes at most
   VTRI_MAX_COLS indices. Returns the number of columns, or 0 if the file cannot
   be read as an index. */
int vtri_read_spec(const char *vtri_path, uint16_t *out_col_indices, int *out_ci);

/* Build and write a .vtri index over one or more columns of a .vtr file.
   col_names: array of column name strings (any order; canonicalized to schema
              order so the file name and the key hashing match what a probe does)
   n_cols: number of columns (1..VTRI_MAX_COLS)
   ci: case-insensitive flag */
void vtri_build(const char *vtr_path, const char **col_names, int n_cols, int ci);

/* Bring an existing .vtri up to date with a store that has gained row groups
   since it was built, scanning only the row groups it does not already cover.

   An entry names the row group its key sits in, and a row append moves no
   existing row group, so the entries an index already holds stay true; only the
   appended groups need reading. That keeps maintaining an index off the size of
   the store, which is what stops an indexed store's append being quadratic.

   Returns 1 when the index was rewritten, or 0 when it cannot be extended --
   unreadable, or built against a store this one is not an extension of -- in
   which case the caller should rebuild it with vtri_build. */
int vtri_extend(const char *vtr_path, const char *vtri_path);

/* Resolve index column names against a schema, sorted into schema order.
   Writes n_cols entries to out_idx and out_names. Returns 1 on success, or 0
   with *bad_name / *bad_reason describing the offending column (either may be
   NULL if the caller does not want them). */
int vtri_resolve_cols(const VecSchema *schema, const char **col_names,
                      int n_cols, int *out_idx, const char **out_names,
                      const char **bad_name, const char **bad_reason);

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
