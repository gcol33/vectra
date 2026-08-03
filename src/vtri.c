#include "vtri.h"
#include "vtr1_tdc.h"
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

/* Use shared hash functions from vtri.h (vtri_fnv1a, vtri_hash_int64, etc.) */
/* Aliases for local use */
#define fnv1a     vtri_fnv1a
#define fnv1a_ci  vtri_fnv1a_ci
#define hash_int64  vtri_hash_int64
#define hash_double vtri_hash_double
#define FNV_OFFSET  VTRI_FNV_OFFSET

static uint64_t hash_string(const VecArray *col, int64_t row, int ci) {
    if (!vec_array_is_valid(col, row))
        return FNV_OFFSET ^ 0xFF;
    int64_t s = col->buf.str.offsets[row];
    int64_t e = col->buf.str.offsets[row + 1];
    int64_t len = e - s;
    if (ci)
        return fnv1a_ci(col->buf.str.data + s, len);
    return fnv1a((const uint8_t *)(col->buf.str.data + s), len);
}

static uint64_t hash_array_value(const VecArray *col, int64_t row, int ci) {
    if (!vec_array_is_valid(col, row))
        return FNV_OFFSET ^ 0xFF;
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
/*  Growable (hash, row group) entry list                              */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t *hash;
    uint32_t *rg;
    int64_t   n, cap;
} EntryVec;

static int ev_push(EntryVec *ev, uint64_t h, uint32_t rg) {
    if (ev->n == ev->cap) {
        int64_t cap = ev->cap ? ev->cap * 2 : 1024;
        uint64_t *nh = (uint64_t *)realloc(ev->hash, (size_t)cap * sizeof(uint64_t));
        if (!nh) return 0;
        ev->hash = nh;
        uint32_t *nr = (uint32_t *)realloc(ev->rg, (size_t)cap * sizeof(uint32_t));
        if (!nr) return 0;
        ev->rg = nr;
        ev->cap = cap;
    }
    ev->hash[ev->n] = h;
    ev->rg[ev->n]   = rg;
    ev->n++;
    return 1;
}

static void ev_free(EntryVec *ev) { free(ev->hash); free(ev->rg); }

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

void vtri_build(const char *vtr_path, const char **col_names, int n_cols,
                int ci) {
    if (n_cols < 1 || n_cols > VTRI_MAX_COLS)
        vectra_error("an index spans 1 to %d columns, got %d",
                     VTRI_MAX_COLS, n_cols);

    Vtr1TdcFile *file = vtr1_open_tdc(vtr_path);
    if (!file) vectra_error("vtr1_open_tdc failed for %s", vtr_path);
    const VecSchema *schema = vtr1_tdc_schema(file);

    int col_idx[VTRI_MAX_COLS];
    const char *cols[VTRI_MAX_COLS];
    const char *bad = NULL, *why = NULL;
    if (!vtri_resolve_cols(schema, col_names, n_cols, col_idx, cols, &bad, &why)) {
        vtr1_close_tdc(file);
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
    if (!col_mask) { vtr1_close_tdc(file); vectra_error("alloc failed"); }
    for (int c = 0; c < n_cols; c++) col_mask[col_idx[c]] = 1;

    /* Map each indexed column to its position in the masked read */
    int out_col[VTRI_MAX_COLS];
    for (int c = 0; c < n_cols; c++) {
        out_col[c] = 0;
        for (int j = 0; j < col_idx[c]; j++)
            if (col_mask[j]) out_col[c]++;
    }

    HashSet seen;
    if (!hs_init(&seen, max_rg_rows)) {
        hs_free(&seen); free(col_mask); vtr1_close_tdc(file);
        vectra_error("alloc failed building vtri index");
    }

    EntryVec ev = {0};
    uint64_t per_col_hash[VTRI_MAX_COLS];
    int oom = 0;

    for (uint32_t rg = 0; rg < n_rg && !oom; rg++) {
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
            if (hs_insert(&seen, h) && !ev_push(&ev, h, rg)) {
                oom = 1;
                break;
            }
        }
        vec_batch_free(batch);
    }

    hs_free(&seen);
    free(col_mask);

    if (oom) {
        ev_free(&ev);
        vtr1_close_tdc(file);
        vectra_error("alloc failed building vtri index");
    }

    /* Chain the entries into hash buckets */
    int64_t n_entries = ev.n;
    int64_t n_slots = next_pow2(n_entries * 2); /* ~50% load factor */
    int64_t *heads = (int64_t *)malloc((size_t)n_slots * sizeof(int64_t));
    int64_t *entry_next = (int64_t *)malloc((size_t)(n_entries > 0 ? n_entries : 1)
                                            * sizeof(int64_t));
    if (!heads || !entry_next) {
        free(heads); free(entry_next); ev_free(&ev);
        vtr1_close_tdc(file);
        vectra_error("alloc failed building vtri index");
    }
    for (int64_t s = 0; s < n_slots; s++) heads[s] = -1;
    int64_t mask = n_slots - 1;
    for (int64_t i = 0; i < n_entries; i++) {
        int64_t slot = (int64_t)(ev.hash[i] & (uint64_t)mask);
        entry_next[i] = heads[slot];
        heads[slot] = i;
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
    if (!vtri_path || !tmp_path) {
        free(vtri_path); free(tmp_path);
        free(heads); free(entry_next); ev_free(&ev);
        vtr1_close_tdc(file);
        vectra_error("alloc failed for vtri path");
    }

    FILE *fp = fopen(tmp_path, "wb");
    if (!fp) {
        free(vtri_path); free(tmp_path);
        free(heads); free(entry_next); ev_free(&ev);
        vtr1_close_tdc(file);
        vectra_error("cannot create vtri index file");
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
    w_u64(&w, (uint64_t)n_slots);
    w_bytes(&w, ev.hash, (size_t)n_entries * sizeof(uint64_t));
    w_bytes(&w, ev.rg, (size_t)n_entries * sizeof(uint32_t));
    w_bytes(&w, heads, (size_t)n_slots * sizeof(int64_t));
    w_bytes(&w, entry_next, (size_t)n_entries * sizeof(int64_t));

    int ok = w.ok && fclose(fp) == 0;

    free(heads);
    free(entry_next);
    ev_free(&ev);
    vtr1_close_tdc(file);

    if (ok) {
        remove(vtri_path);
        ok = rename(tmp_path, vtri_path) == 0;
    }
    if (!ok) remove(tmp_path);

    free(vtri_path);
    free(tmp_path);
    if (!ok) vectra_error("failed writing vtri index file");
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
    } else if (version == 2 || version == VTRI_VERSION) {
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
    if (version == 1 || version == 2) {
        /* Superseded layouts: one entry per row and no build stamp, so they can
           neither be verified against the store nor opened cheaply. Report no
           index; create_index() rebuilds in the current format. */
        fclose(fp);
        return NULL;
    }
    if (version != VTRI_VERSION) {
        fclose(fp);
        vectra_error(".vtri version %u was written by a newer vectra", version);
    }

    VtrIndex *idx = (VtrIndex *)calloc(1, sizeof(VtrIndex));
    if (!idx) { fclose(fp); vectra_error("alloc failed for VtrIndex"); }

    idx->n_cols = read_u16_f(fp);
    idx->ci     = read_u8_f(fp);
    if (idx->n_cols < 1 || idx->n_cols > VTRI_MAX_COLS) {
        fclose(fp); vtri_close(idx);
        vectra_error("corrupt .vtri: index spans %d columns", (int)idx->n_cols);
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
    idx->n_slots   = (int64_t)read_u64_f(fp);

    /* Read arrays */
    int64_t ne = idx->n_entries;
    int64_t ns = idx->n_slots;

    /* Validate the declared sizes against the actual file before allocating, so
       a crafted/corrupt header cannot overflow the size arithmetic, request an
       enormous allocation, or drive the fill loops past EOF. The arrays that
       follow occupy 20*ne bytes (hash8 + rg4 + next8 per entry) plus 8*ns bytes
       (one head per slot); n_slots must be >= 1 for the probe mask to be a valid
       index. The bound is written division-first so 20*ne never overflows. */
    int64_t hdr_end = (int64_t)VTRI_FTELL64(fp);
    if (hdr_end < 0 || VTRI_FSEEK64(fp, 0, SEEK_END) != 0) {
        fclose(fp); vtri_close(idx);
        vectra_error("corrupt .vtri: cannot size index file");
    }
    int64_t fsize = (int64_t)VTRI_FTELL64(fp);
    if (fsize < 0 || VTRI_FSEEK64(fp, hdr_end, SEEK_SET) != 0) {
        fclose(fp); vtri_close(idx);
        vectra_error("corrupt .vtri: cannot size index file");
    }
    int64_t remaining = (int64_t)fsize - (int64_t)hdr_end;
    if (ne < 0 || ns < 1 || remaining < 0 ||
        ns > remaining / 8 || ne > (remaining - 8 * ns) / 20) {
        fclose(fp); vtri_close(idx);
        vectra_error("corrupt .vtri: entry/slot counts exceed file size");
    }

    idx->entry_hash = (uint64_t *)malloc((size_t)(ne > 0 ? ne : 1) * sizeof(uint64_t));
    idx->entry_rg   = (uint32_t *)malloc((size_t)(ne > 0 ? ne : 1) * sizeof(uint32_t));
    idx->heads       = (int64_t *)malloc((size_t)ns * sizeof(int64_t));
    idx->entry_next  = (int64_t *)malloc((size_t)(ne > 0 ? ne : 1) * sizeof(int64_t));

    if (!idx->entry_hash || !idx->entry_rg || !idx->heads || !idx->entry_next) {
        fclose(fp);
        vtri_close(idx);
        vectra_error("alloc failed reading vtri index");
    }

    int short_read =
        (ne > 0 && fread(idx->entry_hash, sizeof(uint64_t), (size_t)ne, fp) != (size_t)ne) ||
        (ne > 0 && fread(idx->entry_rg, sizeof(uint32_t), (size_t)ne, fp) != (size_t)ne) ||
        (fread(idx->heads, sizeof(int64_t), (size_t)ns, fp) != (size_t)ns) ||
        (ne > 0 && fread(idx->entry_next, sizeof(int64_t), (size_t)ne, fp) != (size_t)ne);

    fclose(fp);
    if (short_read) {
        vtri_close(idx);
        vectra_error("vtri: unexpected EOF");
    }
    return idx;
}

/* ------------------------------------------------------------------ */
/*  vtri_close                                                         */
/* ------------------------------------------------------------------ */

void vtri_close(VtrIndex *idx) {
    if (!idx) return;
    free(idx->entry_hash);
    free(idx->entry_rg);
    free(idx->heads);
    free(idx->entry_next);
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

static uint8_t *probe_by_hash(const VtrIndex *idx, uint64_t h,
                              uint32_t n_rowgroups) {
    uint8_t *bitmap = (uint8_t *)calloc((size_t)n_rowgroups, 1);
    if (!bitmap) return NULL;

    /* Defensive: n_slots >= 1 and every chain index in [0, n_entries) is
       guaranteed by vtri_open's validation, but bound them here too so a probe
       can never walk out of range or loop forever on a crafted chain. */
    if (idx->n_slots <= 0 || idx->n_entries <= 0) return bitmap;

    int64_t mask = idx->n_slots - 1;
    int64_t slot = (int64_t)(h & (uint64_t)mask);
    int64_t e = idx->heads[slot];

    int64_t steps = 0;
    while (e >= 0 && e < idx->n_entries && steps++ < idx->n_entries) {
        if (idx->entry_hash[e] == h) {
            uint32_t rg = idx->entry_rg[e];
            if (rg < n_rowgroups)
                bitmap[rg] = 1;
        }
        e = idx->entry_next[e];
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

uint8_t *vtri_probe_composite(const VtrIndex *idx, const uint64_t *col_hashes,
                              int n_cols, uint32_t n_rowgroups) {
    uint64_t h = combine_hashes(col_hashes, n_cols);
    return probe_by_hash(idx, h, n_rowgroups);
}
