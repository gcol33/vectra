#include "vtri.h"
#include "vtr1_tdc.h"
#include "vtr_fileops.h"
#include "rec_spill.h"
#include "batch.h"
#include "schema.h"
#include "array.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>

/* An index holds 20 bytes per entry plus 8 per slot, so it passes 2 GB on a store of a few tens of
   millions of rows. ftell()/fseek() carry a 32-bit `long` on Windows and cannot address that, so
   offsets into a .vtri go through the 64-bit calls, as in vtr1_tdc.c and byte_reader.c. */
#ifdef _WIN32
#  define VTRI_FTELL64(fp)          _ftelli64(fp)
#  define VTRI_FSEEK64(fp, off, wh) _fseeki64((fp), (int64_t)(off), (wh))
#else
#  define VTRI_FTELL64(fp)          ftello(fp)
#  define VTRI_FSEEK64(fp, off, wh) fseeko((fp), (off_t)(off), (wh))
#endif

/* Entries written per fwrite while the merge streams them out. */
#define VTRI_WRITE_BATCH 8192

/* Use shared hash functions from vtri.h (vtri_fnv1a, vtri_hash_int64, etc.) */
/* Aliases for local use */
#define fnv1a     vtri_fnv1a
#define fnv1a_ci  vtri_fnv1a_ci
#define hash_int64  vtri_hash_int64
#define hash_double vtri_hash_double
#define FNV_OFFSET  VTRI_FNV_OFFSET

static uint64_t hash_string(const VecArray *col, int64_t row, int ci) {
    if (!vec_array_is_valid(col, row))
        return VTRI_NA_HASH;
    int64_t s = col->buf.str.offsets[row];
    int64_t e = col->buf.str.offsets[row + 1];
    int64_t len = e - s;
    if (ci)
        return fnv1a_ci(col->buf.str.data + s, len);
    return fnv1a((const uint8_t *)(col->buf.str.data + s), len);
}

static uint64_t hash_array_value(const VecArray *col, int64_t row, int ci) {
    if (!vec_array_is_valid(col, row))
        return VTRI_NA_HASH;
    switch (col->type) {
    case VEC_STRING: return hash_string(col, row, ci);
    case VEC_INT64:  return hash_int64(col->buf.i64[row]);
    case VEC_INT32:  return hash_int64((int64_t)col->buf.i32[row]);
    case VEC_INT16:  return hash_int64((int64_t)col->buf.i16[row]);
    case VEC_INT8:   return hash_int64((int64_t)col->buf.i8[row]);
    case VEC_DOUBLE: return hash_double(col->buf.dbl[row]);
    case VEC_BOOL:   { uint8_t v = col->buf.bln[row]; return fnv1a(&v, 1); }
    }
    return 0;
}

/* Combine multiple column hashes into a composite hash */
static uint64_t combine_hashes(const uint64_t *hashes, int n) {
    uint64_t h = VTRI_FNV_OFFSET;
    for (int i = 0; i < n; i++) {
        h ^= hashes[i];
        h *= VTRI_FNV_PRIME;
    }
    return h;
}

/* Next power of 2 >= n, minimum 16 */
static int64_t next_pow2(int64_t n) {
    int64_t p = 16;
    while (p < n) p <<= 1;
    return p;
}

/* ------------------------------------------------------------------ */
/*  (hash, row group) entries                                          */
/* ------------------------------------------------------------------ */

/* One entry as it sits both in the sort and on disk: 12 packed bytes, so
   neither the sort's buffer nor the file pays for alignment. */
typedef unsigned char VtrEnt[VTRI_ENTRY_BYTES];

static inline void ent_put(VtrEnt e, uint64_t h, uint32_t rg) {
    memcpy(e, &h, 8);
    memcpy(e + 8, &rg, 4);
}

static inline uint64_t ent_hash(const void *e) {
    uint64_t h;
    memcpy(&h, e, 8);
    return h;
}

static inline uint32_t ent_rg(const void *e) {
    uint32_t rg;
    memcpy(&rg, (const unsigned char *)e + 8, 4);
    return rg;
}

/* Ascending by hash, then by row group. The row group breaks the tie so that a
   rebuild and an extend of the same store write the same bytes. */
static int ent_cmp(const void *a, const void *b) {
    uint64_t ha = ent_hash(a), hb = ent_hash(b);
    if (ha < hb) return -1;
    if (ha > hb) return 1;
    uint32_t ra = ent_rg(a), rb = ent_rg(b);
    if (ra < rb) return -1;
    if (ra > rb) return 1;
    return 0;
}

/* Directory width: one slot per VTRI_DIR_ENTRIES_PER_SLOT entries, capped at
   VTRI_DIR_MAX_BITS so the one array the build holds never passes 16 MB. A few
   entries to a slot rather than one costs a couple of comparisons inside a slot
   -- all within the bytes a single page already holds, so the page count a probe
   pays is the same -- and takes the directory from 4 bytes an entry to 1.

   A directory addresses entries with a u32, so an index with more entries than
   that can hold does without one and is probed by binary search over the whole
   file. */
static int dir_bits_for(int64_t n_entries) {
    if (n_entries <= 0 || n_entries > (int64_t)UINT32_MAX) return 0;
    int64_t want = n_entries / VTRI_DIR_ENTRIES_PER_SLOT;
    int d = 0;
    while (d < VTRI_DIR_MAX_BITS && ((int64_t)1 << d) < want) d++;
    return d;
}

/* ------------------------------------------------------------------ */
/*  Per-row-group dedup set                                            */
/* ------------------------------------------------------------------ */

/* Open addressing over a power-of-two slot count sized to twice the largest row
   group, so a row group's inserts never pass a 50% load factor and the probe
   loop always terminates. */
typedef struct {
    uint64_t *hash;
    uint8_t  *used;
    int64_t   n_slots;
} HashSet;

static int hs_init(HashSet *hs, int64_t capacity) {
    hs->n_slots = next_pow2(capacity > 0 ? capacity * 2 : 1);
    hs->hash = (uint64_t *)malloc((size_t)hs->n_slots * sizeof(uint64_t));
    hs->used = (uint8_t *)malloc((size_t)hs->n_slots);
    return hs->hash && hs->used;
}

static void hs_clear(HashSet *hs) { memset(hs->used, 0, (size_t)hs->n_slots); }

/* Returns 1 if h was not already present (and inserts it), 0 if it was. */
static int hs_insert(HashSet *hs, uint64_t h) {
    int64_t mask = hs->n_slots - 1;
    int64_t s = (int64_t)(h & (uint64_t)mask);
    while (hs->used[s]) {
        if (hs->hash[s] == h) return 0;
        s = (s + 1) & mask;
    }
    hs->used[s] = 1;
    hs->hash[s] = h;
    return 1;
}

static void hs_free(HashSet *hs) { free(hs->hash); free(hs->used); }

/* ------------------------------------------------------------------ */
/*  Path helpers                                                       */
/* ------------------------------------------------------------------ */

char *vtri_make_path(const char *vtr_path, const char *col_name) {
    /* Format: <vtr_path>.<col_name>.vtri */
    size_t plen = strlen(vtr_path);
    size_t clen = strlen(col_name);
    char *path = (char *)malloc(plen + 1 + clen + 5 + 1);
    if (!path) return NULL;
    memcpy(path, vtr_path, plen);
    path[plen] = '.';
    memcpy(path + plen + 1, col_name, clen);
    memcpy(path + plen + 1 + clen, ".vtri", 6); /* includes '\0' */
    return path;
}

char *vtri_make_path_composite(const char *vtr_path, const char **col_names,
                               int n_cols) {
    if (n_cols == 1) return vtri_make_path(vtr_path, col_names[0]);
    /* Format: <vtr_path>.<col1>_<col2>_...<colN>.vtri */
    size_t plen = strlen(vtr_path);
    size_t total = plen + 1; /* dot */
    for (int i = 0; i < n_cols; i++) {
        total += strlen(col_names[i]);
        if (i < n_cols - 1) total += 1; /* underscore */
    }
    total += 5 + 1; /* .vtri + null */
    char *path = (char *)malloc(total);
    if (!path) return NULL;
    size_t pos = 0;
    memcpy(path + pos, vtr_path, plen); pos += plen;
    path[pos++] = '.';
    for (int i = 0; i < n_cols; i++) {
        size_t clen = strlen(col_names[i]);
        memcpy(path + pos, col_names[i], clen); pos += clen;
        if (i < n_cols - 1) path[pos++] = '_';
    }
    memcpy(path + pos, ".vtri", 6); /* includes null */
    return path;
}

/* ------------------------------------------------------------------ */
/*  Column resolution                                                  */
/* ------------------------------------------------------------------ */

/* Both the file name and the composite key hash depend on the column order, so
   build and probe have to agree on one. Schema order is that order: the caller
   may name the columns in any order. */
int vtri_resolve_cols(const VecSchema *schema, const char **col_names,
                      int n_cols, int *out_idx, const char **out_names,
                      const char **bad_name, const char **bad_reason) {
    if (bad_name) *bad_name = NULL;
    if (bad_reason) *bad_reason = NULL;
    if (n_cols < 1 || n_cols > VTRI_MAX_COLS) return 0;
    for (int c = 0; c < n_cols; c++) {
        int ci = vec_schema_find_col(schema, col_names[c]);
        if (ci < 0) {
            if (bad_name) *bad_name = col_names[c];
            if (bad_reason) *bad_reason = "not found in schema";
            return 0;
        }
        for (int p = 0; p < c; p++)
            if (out_idx[p] == ci) {
                if (bad_name) *bad_name = col_names[c];
                if (bad_reason) *bad_reason = "named twice in one index";
                return 0;
            }
        out_idx[c] = ci;
        out_names[c] = schema->col_names[ci];
    }
    for (int i = 1; i < n_cols; i++) {
        int ki = out_idx[i];
        const char *kn = out_names[i];
        int j = i - 1;
        while (j >= 0 && out_idx[j] > ki) {
            out_idx[j + 1] = out_idx[j];
            out_names[j + 1] = out_names[j];
            j--;
        }
        out_idx[j + 1] = ki;
        out_names[j + 1] = kn;
    }
    return 1;
}

/* ------------------------------------------------------------------ */
/*  I/O helpers                                                        */
/* ------------------------------------------------------------------ */

/* Writes carry an ok flag so a full disk is reported at build time rather than
   leaving a truncated sidecar that every later open has to reject. */
typedef struct { FILE *fp; int ok; } Writer;

static void w_bytes(Writer *w, const void *p, size_t n) {
    if (!w->ok || n == 0) return;
    if (fwrite(p, 1, n, w->fp) != n) w->ok = 0;
}
static void w_u8(Writer *w, uint8_t v)   { w_bytes(w, &v, 1); }
static void w_u16(Writer *w, uint16_t v) { w_bytes(w, &v, 2); }
static void w_u32(Writer *w, uint32_t v) { w_bytes(w, &v, 4); }
static void w_u64(Writer *w, uint64_t v) { w_bytes(w, &v, 8); }

static uint8_t read_u8_f(FILE *fp) {
    uint8_t v = 0;
    if (fread(&v, 1, 1, fp) != 1) vectra_error("vtri: unexpected EOF");
    return v;
}
static uint16_t read_u16_f(FILE *fp) {
    uint16_t v = 0;
    if (fread(&v, 2, 1, fp) != 1) vectra_error("vtri: unexpected EOF");
    return v;
}
static uint32_t read_u32_f(FILE *fp) {
    uint32_t v = 0;
    if (fread(&v, 4, 1, fp) != 1) vectra_error("vtri: unexpected EOF");
    return v;
}
static uint64_t read_u64_f(FILE *fp) {
    uint64_t v = 0;
    if (fread(&v, 8, 1, fp) != 1) vectra_error("vtri: unexpected EOF");
    return v;
}

/* ------------------------------------------------------------------ */
/*  vtri_build: build and write a .vtri index                          */
/* ------------------------------------------------------------------ */

/*
 * Build a .vtri over `col_names` and write it.
 *
 * Scanning starts at row group `first_rg`, and the entries of `seed`, an
 * already-open index, are merged in as the file is written. That is what lets an
 * index be brought up to date after an append without rereading the store: an
 * entry names the row group its key sits in, and a row append moves no existing
 * row group, so the entries already in an index stay true and only the appended
 * row groups need scanning. Pass first_rg = 0 and seed = NULL to build from
 * nothing.
 *
 * Nothing here holds the index. The scan pushes each entry into an external
 * sort, which spills past mem_budget; the seed is already sorted, so it is read
 * as a second stream and the two are merged straight into the output file. Peak
 * resident memory is the sort's buffer, the directory (16 MB at most), and one
 * decoded row group -- none of which grow with the number of entries, so an
 * index far larger than RAM can be built and not just read. A chained layout
 * could not do this: it has to know every entry's bucket before writing the
 * first, which is what used to make the build cost the size of the index.
 *
 * The seed is read through the index accessors, so it may be either resident or
 * mapped -- a mapped one is read a page at a time as the merge walks it.
 *
 * The stamp is always taken from the store as it now stands, and the entry order
 * is the data's rather than the scan's, so an extended index is byte-identical
 * to one rebuilt from scratch.
 */
static void vtri_build_core(const char *vtr_path, const char **col_names,
                            int n_cols, int ci,
                            uint32_t first_rg,
                            VtrIndex *seed,
                            int64_t mem_budget, const char *temp_dir) {
    if (n_cols < 1 || n_cols > VTRI_MAX_COLS) {
        vtri_close(seed);
        vectra_error("an index spans 1 to %d columns, got %d",
                     VTRI_MAX_COLS, n_cols);
    }

    Vtr1TdcFile *file = vtr1_open_tdc(vtr_path);
    if (!file) {
        vtri_close(seed);
        vectra_error("vtr1_open_tdc failed for %s", vtr_path);
    }
    const VecSchema *schema = vtr1_tdc_schema(file);

    int col_idx[VTRI_MAX_COLS];
    const char *cols[VTRI_MAX_COLS];
    const char *bad = NULL, *why = NULL;
    if (!vtri_resolve_cols(schema, col_names, n_cols, col_idx, cols, &bad, &why)) {
        vtr1_close_tdc(file);
        vtri_close(seed);
        if (bad && why) vectra_error("column '%s' %s", bad, why);
        vectra_error("an index spans 1 to %d columns", VTRI_MAX_COLS);
    }

    uint32_t n_rg = vtr1_tdc_n_rowgroups(file);
    int64_t total_rows = 0, max_rg_rows = 0;
    for (uint32_t rg = 0; rg < n_rg; rg++) {
        int64_t r = vtr1_tdc_rowgroup_n_rows(file, rg);
        total_rows += r;
        if (r > max_rg_rows) max_rg_rows = r;
    }

    /* Read only the indexed columns */
    int *col_mask = (int *)calloc((size_t)schema->n_cols, sizeof(int));
    if (!col_mask) {
        vtr1_close_tdc(file); vtri_close(seed); vectra_error("alloc failed");
    }
    for (int c = 0; c < n_cols; c++) col_mask[col_idx[c]] = 1;

    /* Map each indexed column to its position in the masked read */
    int out_col[VTRI_MAX_COLS];
    for (int c = 0; c < n_cols; c++) {
        out_col[c] = 0;
        for (int j = 0; j < col_idx[c]; j++)
            if (col_mask[j]) out_col[c]++;
    }

    /* The dedup set spans one row group, which is decoded whole to be scanned
       anyway, so it is bounded by the store's row-group size rather than by the
       store. */
    HashSet seen;
    if (!hs_init(&seen, max_rg_rows)) {
        hs_free(&seen); free(col_mask); vtr1_close_tdc(file); vtri_close(seed);
        vectra_error("alloc failed building vtri index");
    }

    RecSpill ev;
    rec_spill_init(&ev, VTRI_ENTRY_BYTES, ent_cmp, mem_budget, temp_dir);

    uint64_t per_col_hash[VTRI_MAX_COLS];
    VtrEnt rec;
    int oom = 0;

    for (uint32_t rg = first_rg; rg < n_rg && !oom; rg++) {
        VecBatch *batch = vtr1_read_rowgroup_tdc(file, rg, col_mask);
        if (!batch) {
            oom = 1;
            break;
        }
        hs_clear(&seen);
        for (int64_t r = 0; r < batch->n_rows; r++) {
            uint64_t h;
            if (n_cols == 1) {
                h = hash_array_value(&batch->columns[out_col[0]], r, ci);
            } else {
                for (int c = 0; c < n_cols; c++)
                    per_col_hash[c] =
                        hash_array_value(&batch->columns[out_col[c]], r, ci);
                h = combine_hashes(per_col_hash, n_cols);
            }
            /* One entry per distinct key in this row group */
            if (hs_insert(&seen, h)) {
                ent_put(rec, h, rg);
                rec_spill_push(&ev, rec);
            }
        }
        vec_batch_free(batch);
    }

    hs_free(&seen);
    free(col_mask);

    if (oom) {
        rec_spill_free(&ev);
        vtr1_close_tdc(file);
        vtri_close(seed);
        vectra_error("alloc failed building vtri index");
    }

    int64_t n_seed   = seed ? seed->n_entries : 0;
    int64_t n_entries = n_seed + rec_spill_total(&ev);

    /* The directory is filled as the entries stream past, so it is the only
       array held whole -- capped, and so not a function of the index's size. */
    int dir_bits = dir_bits_for(n_entries);
    int64_t dir_len = dir_bits ? ((int64_t)1 << dir_bits) + 1 : 0;
    uint32_t *dir = dir_len ? (uint32_t *)malloc((size_t)dir_len * sizeof(uint32_t))
                            : NULL;
    if (dir_len && !dir) {
        rec_spill_free(&ev); vtr1_close_tdc(file); vtri_close(seed);
        vectra_error("alloc failed building vtri index");
    }

    /* Write to a temp path and rename, so a failed write never replaces a good
       index with a truncated one. */
    char *vtri_path = vtri_make_path_composite(vtr_path, cols, n_cols);
    char *tmp_path = NULL;
    if (vtri_path) {
        size_t vlen = strlen(vtri_path);
        tmp_path = (char *)malloc(vlen + 5);
        if (tmp_path) { memcpy(tmp_path, vtri_path, vlen); memcpy(tmp_path + vlen, ".tmp", 5); }
    }
    FILE *fp = (vtri_path && tmp_path) ? fopen(tmp_path, "wb") : NULL;
    if (!fp) {
        int no_path = !vtri_path || !tmp_path;
        free(vtri_path); free(tmp_path); free(dir);
        rec_spill_free(&ev); vtr1_close_tdc(file); vtri_close(seed);
        vectra_error(no_path ? "alloc failed for vtri path"
                             : "cannot create vtri index file");
    }

    Writer w = { fp, 1 };
    w_bytes(&w, "VTRI", 4);
    w_u16(&w, VTRI_VERSION);
    w_u16(&w, (uint16_t)n_cols);
    w_u8(&w, (uint8_t)ci);
    for (int c = 0; c < n_cols; c++) w_u16(&w, (uint16_t)col_idx[c]);
    w_u64(&w, (uint64_t)total_rows);
    w_u32(&w, n_rg);
    w_u64(&w, (uint64_t)n_entries);
    w_u8(&w, (uint8_t)dir_bits);

    /* Merge the seed's entries with the newly scanned ones, both ascending, and
       write the result as it goes. */
    RecMerge *m = rec_spill_merge_begin(&ev);
    VtrEnt fresh;
    int have_fresh = rec_spill_merge_next(m, fresh);

    VtrEnt out_buf[VTRI_WRITE_BATCH];
    int64_t out_n = 0, si = 0, next_dir = 0, written = 0;

    while (si < n_seed || have_fresh) {
        uint64_t h;
        uint32_t rg;
        int take_seed;
        if (si < n_seed && have_fresh) {
            uint64_t sh = vtri_entry_hash(seed, si);
            uint64_t fh = ent_hash(fresh);
            take_seed = sh < fh ||
                        (sh == fh && vtri_entry_rg(seed, si) <= ent_rg(fresh));
        } else {
            take_seed = si < n_seed;
        }
        if (take_seed) {
            h  = vtri_entry_hash(seed, si);
            rg = vtri_entry_rg(seed, si);
            si++;
        } else {
            h  = ent_hash(fresh);
            rg = ent_rg(fresh);
            have_fresh = rec_spill_merge_next(m, fresh);
        }

        if (dir_bits) {
            int64_t b = (int64_t)(h >> (64 - dir_bits));
            while (next_dir <= b) dir[next_dir++] = (uint32_t)written;
        }
        ent_put(out_buf[out_n], h, rg);
        out_n++;
        written++;
        if (out_n == VTRI_WRITE_BATCH) {
            w_bytes(&w, out_buf, (size_t)out_n * VTRI_ENTRY_BYTES);
            out_n = 0;
        }
    }
    if (out_n > 0) w_bytes(&w, out_buf, (size_t)out_n * VTRI_ENTRY_BYTES);
    rec_spill_merge_end(m);

    /* Slots past the last entry's prefix, and any the entries skipped over, end
       where the entries do -- an empty range, which is what a probe for an
       absent key reads. */
    while (next_dir < dir_len) dir[next_dir++] = (uint32_t)written;
    if (dir_len) w_bytes(&w, dir, (size_t)dir_len * sizeof(uint32_t));

    int wrote_all = w.ok && written == n_entries;
    int ok = (fclose(fp) == 0) && wrote_all;

    free(dir);
    rec_spill_free(&ev);
    vtr1_close_tdc(file);

    /* Close the seed before the replace: a seed opened over a large index holds
       a mapping of the very file being replaced, and on Windows a live mapping
       is what a replace has to wait for. */
    vtri_close(seed);

    /* A reader that opened this index mapped it rather than copying it, so the
       old file can still be open when the new one arrives; vtr_atomic_replace
       waits out a sharing violation instead of failing on it. */
    if (ok) ok = vtr_atomic_replace(tmp_path, vtri_path) == 0;
    if (!ok) remove(tmp_path);

    free(vtri_path);
    free(tmp_path);
    if (!ok) vectra_error("failed writing vtri index file");
}

void vtri_build(const char *vtr_path, const char **col_names, int n_cols,
                int ci, int64_t mem_budget, const char *temp_dir) {
    vtri_build_core(vtr_path, col_names, n_cols, ci, 0, NULL,
                    mem_budget, temp_dir);
}

/* ------------------------------------------------------------------ */
/*  vtri_extend: take in row groups appended since the index was built */
/* ------------------------------------------------------------------ */

int vtri_extend(const char *vtr_path, const char *vtri_path,
                int64_t mem_budget, const char *temp_dir) {
    /* Opened with the stamp check skipped: the store has already grown past
       what this index was built against, which is precisely the case being
       handled. The stamp is then checked by hand, against the store's shape
       BEFORE the append rather than after. */
    VtrIndex *idx = vtri_open(vtri_path, NULL, -1, -1);
    if (!idx) return 0;

    Vtr1TdcFile *file = vtr1_open_tdc(vtr_path);
    if (!file) { vtri_close(idx); return 0; }

    const VecSchema *schema = vtr1_tdc_schema(file);
    uint32_t n_rg = vtr1_tdc_n_rowgroups(file);

    /* The index's entries name row groups by position, so they survive only if
       the row groups they name are still the same ones. Verify that the store
       opens with the index's row groups as an unchanged PREFIX: the count has
       not shrunk, and those first groups still hold exactly the rows the index
       was built over. A store rewritten rather than appended to fails this and
       falls back to a full rebuild. */
    int extendable = (idx->src_n_rowgroups >= 0 &&
                      idx->src_n_rowgroups <= (int64_t)n_rg);
    if (extendable) {
        int64_t prefix_rows = 0;
        for (uint32_t rg = 0; rg < (uint32_t)idx->src_n_rowgroups; rg++)
            prefix_rows += vtr1_tdc_rowgroup_n_rows(file, rg);
        extendable = (prefix_rows == idx->src_n_rows);
    }

    /* Resolve the indexed columns by name against the current schema, since
       that is what the rebuild takes. A store whose columns moved (a column
       append puts new ones at the tail, so positions are stable, but be
       defensive) cannot reuse the old entries. */
    const char *cols[VTRI_MAX_COLS];
    if (extendable) {
        for (int c = 0; c < idx->n_cols; c++) {
            uint16_t ci_col = idx->col_indices[c];
            if (ci_col >= (uint16_t)schema->n_cols) { extendable = 0; break; }
            cols[c] = schema->col_names[ci_col];
        }
    }

    if (!extendable) {
        vtr1_close_tdc(file);
        vtri_close(idx);
        return 0;
    }

    /* Nothing new to take in: the index already covers the store. Rewrite it
       anyway, so its stamp matches a store whose row count changed without
       gaining a row group (an appended empty batch). */
    uint32_t first_rg = (uint32_t)idx->src_n_rowgroups;

    int n_idx_cols = idx->n_cols;
    int idx_ci     = idx->ci;

    /* vtri_build_core reopens the store itself; close this handle first so the
       two are never open at once. The column names are held by the index's own
       resolved copies, which outlive the close. */
    char *col_copies[VTRI_MAX_COLS];
    for (int c = 0; c < n_idx_cols; c++) {
        size_t len = strlen(cols[c]);
        col_copies[c] = (char *)malloc(len + 1);
        if (!col_copies[c]) {
            for (int j = 0; j < c; j++) free(col_copies[j]);
            vtr1_close_tdc(file);
            vtri_close(idx);
            vectra_error("alloc failed extending vtri index");
        }
        memcpy(col_copies[c], cols[c], len + 1);
    }
    vtr1_close_tdc(file);

    const char *col_ptrs[VTRI_MAX_COLS];
    for (int c = 0; c < n_idx_cols; c++) col_ptrs[c] = col_copies[c];

    /* The seed is handed over: vtri_build_core merges its entries into the new
       file and closes it before that file is moved into its place, so nothing of
       the old index is still open by then. */
    vtri_build_core(vtr_path, col_ptrs, n_idx_cols, idx_ci, first_rg, idx,
                    mem_budget, temp_dir);

    for (int c = 0; c < n_idx_cols; c++) free(col_copies[c]);
    return 1;
}

/* ------------------------------------------------------------------ */
/*  vtri_read_spec: header-only read, any version                      */
/* ------------------------------------------------------------------ */

int vtri_read_spec(const char *vtri_path, uint16_t *out_col_indices,
                   int *out_ci) {
    FILE *fp = fopen(vtri_path, "rb");
    if (!fp) return 0;

    char magic[4];
    uint16_t version = 0;
    if (fread(magic, 1, 4, fp) != 4 || memcmp(magic, "VTRI", 4) != 0 ||
        fread(&version, 2, 1, fp) != 1) {
        fclose(fp);
        return 0;
    }

    int n_cols = 0;
    uint16_t first = 0;
    uint8_t ci = 0;
    if (version == 1) {
        /* v1: col_idx, ci */
        if (fread(&first, 2, 1, fp) != 1 || fread(&ci, 1, 1, fp) != 1) {
            fclose(fp); return 0;
        }
        out_col_indices[0] = first;
        n_cols = 1;
    } else if (version == 2 || version == 3 || version == VTRI_VERSION) {
        uint16_t nc = 0;
        if (fread(&nc, 2, 1, fp) != 1 || fread(&ci, 1, 1, fp) != 1 ||
            nc < 1 || nc > VTRI_MAX_COLS) {
            fclose(fp); return 0;
        }
        for (uint16_t c = 0; c < nc; c++) {
            if (fread(&out_col_indices[c], 2, 1, fp) != 1) {
                fclose(fp); return 0;
            }
        }
        n_cols = (int)nc;
    } else {
        fclose(fp);
        return 0;
    }

    fclose(fp);
    if (out_ci) *out_ci = ci ? 1 : 0;
    return n_cols;
}

/* ------------------------------------------------------------------ */
/*  vtri_open: read a .vtri index                                      */
/* ------------------------------------------------------------------ */

/* An index is an optimization: it names the row groups a key may sit in, and
   every query it accelerates is answerable by reading the store. So no query's
   correctness rests on one, and every way of failing to read one -- absent,
   superseded, written by a newer vectra, stale against the store, malformed --
   reports no index and leaves the scan to read the store. Raising an error
   instead would make an unusable sidecar an unreadable store, which is the
   worse failure: a file that reads fine becomes one that cannot be opened at
   all, and not even a full scan gets the caller their rows.

   Allocation failure is the one exception, and a different kind of thing: it
   says nothing about the index, so it is raised rather than swallowed. */
VtrIndex *vtri_open(const char *vtri_path, const VecSchema *schema,
                    int64_t src_n_rows, int64_t src_n_rowgroups) {
    FILE *fp = fopen(vtri_path, "rb");
    if (!fp) return NULL;

    /* Magic */
    char magic[4];
    if (fread(magic, 1, 4, fp) != 4 || memcmp(magic, "VTRI", 4) != 0) {
        fclose(fp);
        return NULL;
    }

    uint16_t version = read_u16_f(fp);
    if (version != VTRI_VERSION) {
        /* Versions 1 to 3 are superseded (1 and 2 hold one entry per row and no
           build stamp, so they can neither be verified against the store nor
           opened cheaply; 3 chains its entries, which is the layout a bounded
           build cannot write); a version above this build's is from a newer
           vectra and its layout is unknown. Either way there is no index to
           probe, and create_index() rebuilds in the current format. */
        fclose(fp);
        return NULL;
    }

    VtrIndex *idx = (VtrIndex *)calloc(1, sizeof(VtrIndex));
    if (!idx) { fclose(fp); vectra_error("alloc failed for VtrIndex"); }

    idx->n_cols = read_u16_f(fp);
    idx->ci     = read_u8_f(fp);
    if (idx->n_cols < 1 || idx->n_cols > VTRI_MAX_COLS) {
        fclose(fp); vtri_close(idx);
        return NULL;
    }
    idx->col_indices = (uint16_t *)malloc((size_t)idx->n_cols * sizeof(uint16_t));
    idx->col_names   = (char **)calloc((size_t)idx->n_cols, sizeof(char *));
    if (!idx->col_indices || !idx->col_names) {
        fclose(fp); vtri_close(idx);
        vectra_error("alloc failed reading vtri index");
    }
    for (int c = 0; c < idx->n_cols; c++) {
        idx->col_indices[c] = read_u16_f(fp);
        if (schema && idx->col_indices[c] < (uint16_t)schema->n_cols) {
            const char *nm = schema->col_names[idx->col_indices[c]];
            idx->col_names[c] = (char *)malloc(strlen(nm) + 1);
            if (idx->col_names[c]) strcpy(idx->col_names[c], nm);
        }
    }
    idx->col_idx = idx->col_indices[0];
    if (idx->col_names[0]) {
        idx->col_name = (char *)malloc(strlen(idx->col_names[0]) + 1);
        if (idx->col_name) strcpy(idx->col_name, idx->col_names[0]);
    }

    idx->src_n_rows      = (int64_t)read_u64_f(fp);
    idx->src_n_rowgroups = (int64_t)read_u32_f(fp);

    /* A row append rewrites every row group, so an index built before it points
       at row groups that have moved. Probing it would prune groups that now hold
       matching rows, which silently drops them from the result: report no index
       instead, and leave the scan to read the store. */
    if ((src_n_rows >= 0 && idx->src_n_rows != src_n_rows) ||
        (src_n_rowgroups >= 0 && idx->src_n_rowgroups != src_n_rowgroups)) {
        fclose(fp);
        vtri_close(idx);
        return NULL;
    }

    idx->n_entries = (int64_t)read_u64_f(fp);
    idx->dir_bits  = (int)read_u8_f(fp);

    int64_t ne = idx->n_entries;

    /* Validate the declared sizes against the actual file before allocating, so
       a crafted/corrupt header cannot overflow the size arithmetic, request an
       enormous allocation, or drive the fill loops past EOF. What follows is
       VTRI_ENTRY_BYTES*ne bytes of entries and, when there is a directory,
       4*(2^dir_bits + 1) bytes after them. The entry bound is written
       division-first so the product never overflows. */
    int64_t hdr_end = (int64_t)VTRI_FTELL64(fp);
    if (hdr_end < 0 || VTRI_FSEEK64(fp, 0, SEEK_END) != 0) {
        fclose(fp); vtri_close(idx);
        return NULL;
    }
    int64_t fsize = (int64_t)VTRI_FTELL64(fp);
    if (fsize < 0 || VTRI_FSEEK64(fp, hdr_end, SEEK_SET) != 0) {
        fclose(fp); vtri_close(idx);
        return NULL;
    }
    int64_t remaining = (int64_t)fsize - (int64_t)hdr_end;
    if (ne < 0 || remaining < 0 || idx->dir_bits < 0 ||
        idx->dir_bits > VTRI_DIR_MAX_BITS ||
        ne > remaining / VTRI_ENTRY_BYTES) {
        fclose(fp); vtri_close(idx);
        return NULL;
    }
    int64_t dir_bytes = idx->dir_bits
                      ? (((int64_t)1 << idx->dir_bits) + 1) * 4 : 0;
    int64_t arrays_bytes = ne * VTRI_ENTRY_BYTES + dir_bytes;
    if (arrays_bytes > remaining) {
        fclose(fp); vtri_close(idx);
        return NULL;
    }

    /* A big index is mapped rather than read: reading it would make every
       lookup cost a copy of the whole file, which is the cost this size is
       exactly when it starts to hurt. Reading stays the path for a small one,
       where the copy is a couple of milliseconds and leaves no handle open. */
    if (arrays_bytes > VTRI_RESIDENT_MAX_BYTES) {
        fclose(fp);
        if (!vec_file_map_open(&idx->map, vtri_path) ||
            idx->map.size < hdr_end + arrays_bytes) {
            /* Nothing here says the machine is out of memory -- a mapping is
               address space, not pages -- so this is an index that cannot be
               used, and reports absent like any other. */
            vtri_close(idx);
            return NULL;
        }
        idx->arr = idx->map.base + hdr_end;
        return idx;
    }

    idx->arr_owned = (uint8_t *)malloc((size_t)(arrays_bytes > 0 ? arrays_bytes : 1));
    if (!idx->arr_owned) {
        fclose(fp);
        vtri_close(idx);
        vectra_error("alloc failed reading vtri index");
    }
    idx->arr = idx->arr_owned;

    int short_read = arrays_bytes > 0 &&
        fread(idx->arr_owned, 1, (size_t)arrays_bytes, fp) != (size_t)arrays_bytes;

    fclose(fp);
    if (short_read) {
        vtri_close(idx);
        return NULL;
    }
    return idx;
}

/* ------------------------------------------------------------------ */
/*  vtri_close                                                         */
/* ------------------------------------------------------------------ */

void vtri_close(VtrIndex *idx) {
    if (!idx) return;
    free(idx->arr_owned);
    vec_file_map_close(&idx->map);
    free(idx->col_name);
    if (idx->col_names) {
        for (int c = 0; c < idx->n_cols; c++) free(idx->col_names[c]);
        free(idx->col_names);
    }
    free(idx->col_indices);
    free(idx);
}

/* ------------------------------------------------------------------ */
/*  Probe helpers                                                      */
/* ------------------------------------------------------------------ */

/* The entries are ascending by hash, so a probe is a binary search for the first
   entry carrying `h` followed by a walk over the run of entries that share it --
   one per row group the key appears in. The directory narrows the search to the
   entries sharing the hash's leading bits before it starts, which is what keeps
   the read count flat as the index grows rather than following log2(n_entries).

   The range the directory hands over is clamped rather than trusted: a corrupt
   file is a file that prunes the wrong row groups whatever the layout, but it
   must not be one that reads outside the mapping. */
static uint8_t *probe_by_hash(const VtrIndex *idx, uint64_t h,
                              uint32_t n_rowgroups) {
    uint8_t *bitmap = (uint8_t *)calloc((size_t)n_rowgroups, 1);
    if (!bitmap) return NULL;
    if (idx->n_entries <= 0) return bitmap;

    int64_t lo = 0, hi = idx->n_entries;
    if (idx->dir_bits) {
        int64_t b = (int64_t)(h >> (64 - idx->dir_bits));
        lo = vtri_dir(idx, b);
        hi = vtri_dir(idx, b + 1);
        if (lo < 0 || lo > idx->n_entries) lo = 0;
        if (hi < lo || hi > idx->n_entries) hi = idx->n_entries;
    }

    /* Lower bound: the first entry whose hash is not below h. */
    while (lo < hi) {
        int64_t mid = lo + (hi - lo) / 2;
        if (vtri_entry_hash(idx, mid) < h) lo = mid + 1;
        else hi = mid;
    }

    for (int64_t e = lo; e < idx->n_entries && vtri_entry_hash(idx, e) == h; e++) {
        uint32_t rg = vtri_entry_rg(idx, e);
        if (rg < n_rowgroups)
            bitmap[rg] = 1;
    }

    return bitmap;
}

uint8_t *vtri_probe_string(const VtrIndex *idx, const char *key,
                           int64_t key_len, uint32_t n_rowgroups) {
    uint64_t h;
    if (idx->ci)
        h = fnv1a_ci(key, key_len);
    else
        h = fnv1a((const uint8_t *)key, key_len);
    return probe_by_hash(idx, h, n_rowgroups);
}

uint8_t *vtri_probe_int64(const VtrIndex *idx, int64_t key,
                          uint32_t n_rowgroups) {
    return probe_by_hash(idx, hash_int64(key), n_rowgroups);
}

uint8_t *vtri_probe_double(const VtrIndex *idx, double key,
                           uint32_t n_rowgroups) {
    return probe_by_hash(idx, hash_double(key), n_rowgroups);
}

uint8_t *vtri_probe_na(const VtrIndex *idx, uint32_t n_rowgroups) {
    return probe_by_hash(idx, VTRI_NA_HASH, n_rowgroups);
}

uint8_t *vtri_probe_composite(const VtrIndex *idx, const uint64_t *col_hashes,
                              int n_cols, uint32_t n_rowgroups) {
    uint64_t h = combine_hashes(col_hashes, n_cols);
    return probe_by_hash(idx, h, n_rowgroups);
}
