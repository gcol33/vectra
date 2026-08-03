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
 * File format (version 4; a single layout for one or many columns):
 *   "VTRI" magic (4 bytes)
 *   version: u16 (4)
 *   n_cols: u16 (number of indexed columns)
 *   ci: u8 (case-insensitive flag)
 *   col_indices[n_cols]: u16 each (schema column indices, ascending)
 *   src_n_rows: u64 (rows in the store at build time)
 *   src_n_rowgroups: u32 (row groups in the store at build time)
 *   n_entries: u64
 *   dir_bits: u8 (directory prefix width; 0 = no directory)
 *   entries[n_entries]: { hash u64; rg u32 } packed, 12 bytes each,
 *                       ascending by (hash, rg)
 *   dir[(1 << dir_bits) + 1]: u32 each (see below; absent when dir_bits = 0)
 *
 * The entries are SORTED rather than chained, and that is what lets an index be
 * built without holding it in memory. A chained table has to know every entry's
 * bucket before it can write the first one, so it materializes the whole index:
 * building a 2.12 GB sidecar peaked at 2.68 GB resident, and an index too large
 * for RAM could be read but not made. Sorted entries are produced by an external
 * sort and written in one forward pass, so the build's peak is its sort buffer
 * whatever the index's size. The bytes come out identical whether an index is
 * built from a whole store or extended after an append, since the order is the
 * data's rather than the scan's.
 *
 * Sorting also drops the per-entry chain pointer and the bucket array: 12 bytes
 * per entry against the 20 + ~16 the chained layout spent, so a sidecar is about
 * a third the size it was.
 *
 * `dir` keeps a probe off log2(n_entries): dir[j] is the first entry whose hash
 * begins with prefix j, taking the top dir_bits bits, so a probe reads one
 * directory slot and binary-searches the handful of entries between dir[j] and
 * dir[j+1] rather than the whole file. It is sized at a few entries to a slot
 * and capped at VTRI_DIR_MAX_BITS, which bounds it to 16 MB and keeps it the one
 * array the build holds in memory -- a fixed cost rather than one that grows
 * with the index. Past the cap the slots simply cover more entries each, and the
 * search inside a slot absorbs the difference.
 *
 * Versions 1 to 3 are superseded: vtri_open() reports no index for them, and
 * vtri_read_spec() reads their column list so they can be rebuilt.
 *
 * An open index is backed one of two ways, chosen by size, and a probe reads
 * through accessors so it does not care which:
 *
 *   resident  the arrays are read into memory. Costs one copy of the file and
 *             holds no handle afterwards.
 *   mapped    the file is mapped read-only and the arrays are read out of the
 *             mapping in place. Costs the pages a probe touches, so an index
 *             far larger than RAM is still probeable, and a probe's cost stays
 *             off the size of the index.
 *
 * Reading the file whole is the cheaper of the two while it is small, and it
 * leaves nothing open behind it; past VTRI_RESIDENT_MAX_BYTES that copy is what
 * a lookup pays for, and a probe touches a handful of pages whatever the file's
 * size, so the mapping takes over. An index too large to be mapped reports
 * absent, as any other unusable index does -- a lookup falls back to reading the
 * store instead of exhausting memory.
 */

#define VTRI_MAX_COLS 8
#define VTRI_VERSION  4

/* Bytes on disk per entry: u64 hash + u32 row group, packed. */
#define VTRI_ENTRY_BYTES 12

/* Directory ceiling: 2^22 slots of u32 is 16 MB, the largest array the build
   keeps resident. */
#define VTRI_DIR_MAX_BITS 22

/* Entries a directory slot spans, below the ceiling. */
#define VTRI_DIR_ENTRIES_PER_SLOT 4

/* Read an index below this size, map it above. A few MB reads in a couple of
   milliseconds and costs one allocation that is freed straight after, which
   beats faulting pages in for a single probe; from here up, reading grows with
   the index while a mapped probe does not. */
#define VTRI_RESIDENT_MAX_BYTES (4LL * 1024 * 1024)

typedef struct VtrIndex {
    uint16_t  col_idx;      /* first indexed column (== col_indices[0]) */
    uint8_t   ci;
    int64_t   n_entries;
    int       dir_bits;
    int64_t   src_n_rows;      /* rows in the store at build time */
    int64_t   src_n_rowgroups; /* row groups in the store at build time */

    /* The arrays region -- entries then directory -- however it is backed.
       `arr` points into arr_owned when the index was read, or into the mapping
       when it was mapped; exactly one of the two is set. Neither is aligned,
       since the header before it is a whole number of bytes rather than of
       words, so the accessors below memcpy rather than dereference. */
    const uint8_t *arr;
    uint8_t       *arr_owned;  /* NULL when mapped */
    VecFileMap     map;        /* map.base NULL when resident */

    char     *col_name;     /* first column name (resolved from schema at load time) */
    uint16_t  n_cols;       /* number of indexed columns */
    uint16_t *col_indices;  /* [n_cols] column indices */
    char    **col_names;    /* [n_cols] column names (resolved at load time) */
} VtrIndex;

/* ---- Backing-agnostic element reads ----

   Every array is read through one of these, so the probe and the rebuild's copy
   of a seed index are written once and work against either backing. Indices are
   assumed in range; vtri_open validates the file's declared sizes against its
   actual length, and probe_by_hash clamps the range a directory slot hands it. */

static inline uint64_t vtri_entry_hash(const struct VtrIndex *idx, int64_t i) {
    uint64_t v;
    memcpy(&v, idx->arr + i * VTRI_ENTRY_BYTES, 8);
    return v;
}

static inline uint32_t vtri_entry_rg(const struct VtrIndex *idx, int64_t i) {
    uint32_t v;
    memcpy(&v, idx->arr + i * VTRI_ENTRY_BYTES + 8, 4);
    return v;
}

/* First entry carrying directory prefix `j`. Only called when dir_bits > 0. */
static inline int64_t vtri_dir(const struct VtrIndex *idx, int64_t j) {
    uint32_t v;
    memcpy(&v, idx->arr + idx->n_entries * VTRI_ENTRY_BYTES + j * 4, 4);
    return (int64_t)v;
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

/* The key an index files an NA under. A predicate that matches NA -- `%in%` with
   an NA in its set -- has to keep the row groups holding one, so a probe needs
   the same hash the build used. */
#define VTRI_NA_HASH (VTRI_FNV_OFFSET ^ 0xFFULL)

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
   ci: case-insensitive flag
   mem_budget: bytes the entry sort may hold before spilling (<= 0 = default)
   temp_dir: directory for the sort's run files; NULL keeps the sort in RAM,
             which is what makes the build's peak grow with the index again

   The entries are sorted externally and written in one forward pass, so peak
   resident memory is the sort buffer, the directory, and one decoded row group
   -- none of which grow with the number of entries. */
void vtri_build(const char *vtr_path, const char **col_names, int n_cols, int ci,
                int64_t mem_budget, const char *temp_dir);

/* Bring an existing .vtri up to date with a store that has gained row groups
   since it was built, scanning only the row groups it does not already cover.

   An entry names the row group its key sits in, and a row append moves no
   existing row group, so the entries an index already holds stay true; only the
   appended groups need reading. That keeps maintaining an index off the size of
   the store, which is what stops an indexed store's append being quadratic.

   The index already on disk is a sorted stream, so its entries are merged with
   the appended ones as the new file is written rather than gathered first: an
   extend costs one sequential pass over the old sidecar plus a sort of what the
   appended row groups contribute, and holds neither in memory.

   Returns 1 when the index was rewritten, or 0 when it cannot be extended --
   unreadable, or built against a store this one is not an extension of -- in
   which case the caller should rebuild it with vtri_build. */
int vtri_extend(const char *vtr_path, const char *vtri_path,
                int64_t mem_budget, const char *temp_dir);

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

/* Probe the index for the rows whose key is NA. */
uint8_t *vtri_probe_na(const VtrIndex *idx, uint32_t n_rowgroups);

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
