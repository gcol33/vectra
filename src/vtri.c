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
/*  I/O helpers                                                        */
/* ------------------------------------------------------------------ */

static void write_u8(FILE *fp, uint8_t v)   { fwrite(&v, 1, 1, fp); }
static void write_u16(FILE *fp, uint16_t v) { fwrite(&v, 2, 1, fp); }
static void write_u32(FILE *fp, uint32_t v) { fwrite(&v, 4, 1, fp); }
static void write_u64(FILE *fp, uint64_t v) { fwrite(&v, 8, 1, fp); }
static void write_i64(FILE *fp, int64_t v)  { fwrite(&v, 8, 1, fp); }

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
static int64_t read_i64_f(FILE *fp) {
    int64_t v = 0;
    if (fread(&v, 8, 1, fp) != 1) vectra_error("vtri: unexpected EOF");
    return v;
}

/* ------------------------------------------------------------------ */
/*  vtri_build: build and write a .vtri index                          */
/* ------------------------------------------------------------------ */

void vtri_build(const char *vtr_path, const char *col_name, int ci) {
    Vtr1TdcFile *file = vtr1_open_tdc(vtr_path);
    if (!file) vectra_error("vtr1_open_tdc failed for %s", vtr_path);
    const VecSchema *schema = vtr1_tdc_schema(file);

    int col_idx = vec_schema_find_col(schema, col_name);
    if (col_idx < 0) {
        vtr1_close_tdc(file);
        vectra_error("column '%s' not found in schema", col_name);
    }

    /* Count total rows across all row groups */
    uint32_t n_rg = vtr1_tdc_n_rowgroups(file);
    int64_t total_rows = 0;
    for (uint32_t rg = 0; rg < n_rg; rg++)
        total_rows += vtr1_tdc_rowgroup_n_rows(file, rg);

    /* Allocate entry arrays */
    int64_t n_entries = total_rows;
    int64_t n_slots = next_pow2(n_entries * 2); /* ~50% load factor */

    uint64_t *entry_hash = (uint64_t *)malloc((size_t)n_entries * sizeof(uint64_t));
    uint32_t *entry_rg   = (uint32_t *)malloc((size_t)n_entries * sizeof(uint32_t));
    int64_t  *heads      = (int64_t *)malloc((size_t)n_slots * sizeof(int64_t));
    int64_t  *entry_next = (int64_t *)malloc((size_t)n_entries * sizeof(int64_t));
    if (!entry_hash || !entry_rg || !heads || !entry_next) {
        vtr1_close_tdc(file);
        vectra_error("alloc failed building vtri index");
    }

    for (int64_t s = 0; s < n_slots; s++)
        heads[s] = -1;

    /* Build a column mask that reads only the indexed column */
    int *col_mask = (int *)calloc((size_t)schema->n_cols, sizeof(int));
    if (!col_mask) { vtr1_close_tdc(file); vectra_error("alloc failed"); }
    col_mask[col_idx] = 1;

    /* Find the output column index (position of col_idx among selected cols) */
    int out_col = 0;
    for (int c = 0; c < col_idx; c++)
        if (col_mask[c]) out_col++;

    int64_t entry_pos = 0;
    int64_t mask = n_slots - 1;

    for (uint32_t rg = 0; rg < n_rg; rg++) {
        VecBatch *batch = vtr1_read_rowgroup_tdc(file, rg, col_mask);
        const VecArray *col = &batch->columns[out_col];

        for (int64_t r = 0; r < batch->n_rows; r++) {
            uint64_t h = hash_array_value(col, r, ci);
            int64_t slot = (int64_t)(h & (uint64_t)mask);

            entry_hash[entry_pos] = h;
            entry_rg[entry_pos]   = rg;
            entry_next[entry_pos] = heads[slot];
            heads[slot] = entry_pos;
            entry_pos++;
        }

        vec_batch_free(batch);
    }

    free(col_mask);

    /* Write .vtri file */
    char *vtri_path = vtri_make_path(vtr_path, col_name);
    if (!vtri_path) { vtr1_close_tdc(file); vectra_error("alloc failed for vtri path"); }

    FILE *fp = fopen(vtri_path, "wb");
    if (!fp) {
        free(vtri_path);
        free(entry_hash);
        free(entry_rg);
        free(heads);
        free(entry_next);
        vtr1_close_tdc(file);
        vectra_error("cannot create vtri index file");
    }

    /* Header */
    fwrite("VTRI", 1, 4, fp);
    write_u16(fp, 1);                   /* version */
    write_u16(fp, (uint16_t)col_idx);   /* col_idx */
    write_u8(fp, (uint8_t)ci);          /* case-insensitive */
    write_u64(fp, (uint64_t)n_entries);
    write_u64(fp, (uint64_t)n_slots);

    /* Entry arrays */
    for (int64_t i = 0; i < n_entries; i++)
        write_u64(fp, entry_hash[i]);
    for (int64_t i = 0; i < n_entries; i++)
        write_u32(fp, entry_rg[i]);
    for (int64_t i = 0; i < n_slots; i++)
        write_i64(fp, heads[i]);
    for (int64_t i = 0; i < n_entries; i++)
        write_i64(fp, entry_next[i]);

    fclose(fp);
    free(vtri_path);
    free(entry_hash);
    free(entry_rg);
    free(heads);
    free(entry_next);
    vtr1_close_tdc(file);
}

/* ------------------------------------------------------------------ */
/*  vtri_build_composite: build v2 composite index                     */
/* ------------------------------------------------------------------ */

void vtri_build_composite(const char *vtr_path, const char **col_names,
                          int n_cols, int ci) {
    if (n_cols == 1) {
        vtri_build(vtr_path, col_names[0], ci);
        return;
    }

    Vtr1TdcFile *file = vtr1_open_tdc(vtr_path);
    if (!file) vectra_error("vtr1_open_tdc failed for %s", vtr_path);
    const VecSchema *schema = vtr1_tdc_schema(file);

    /* Resolve column indices */
    int *col_idx = (int *)malloc((size_t)n_cols * sizeof(int));
    if (!col_idx) { vtr1_close_tdc(file); vectra_error("alloc failed"); }
    for (int c = 0; c < n_cols; c++) {
        col_idx[c] = vec_schema_find_col(schema, col_names[c]);
        if (col_idx[c] < 0) {
            free(col_idx); vtr1_close_tdc(file);
            vectra_error("column '%s' not found in schema", col_names[c]);
        }
    }

    /* Count total rows */
    uint32_t n_rg = vtr1_tdc_n_rowgroups(file);
    int64_t total_rows = 0;
    for (uint32_t rg = 0; rg < n_rg; rg++)
        total_rows += vtr1_tdc_rowgroup_n_rows(file, rg);

    int64_t n_entries = total_rows;
    int64_t n_slots = next_pow2(n_entries * 2);

    uint64_t *entry_hash = (uint64_t *)malloc((size_t)n_entries * sizeof(uint64_t));
    uint32_t *entry_rg   = (uint32_t *)malloc((size_t)n_entries * sizeof(uint32_t));
    int64_t  *heads      = (int64_t *)malloc((size_t)n_slots * sizeof(int64_t));
    int64_t  *entry_next = (int64_t *)malloc((size_t)n_entries * sizeof(int64_t));
    if (!entry_hash || !entry_rg || !heads || !entry_next) {
        free(col_idx); vtr1_close_tdc(file);
        vectra_error("alloc failed building composite vtri index");
    }

    for (int64_t s = 0; s < n_slots; s++) heads[s] = -1;

    /* Build col_mask for the indexed columns */
    int *col_mask = (int *)calloc((size_t)schema->n_cols, sizeof(int));
    if (!col_mask) { free(col_idx); vtr1_close_tdc(file); vectra_error("alloc failed"); }
    for (int c = 0; c < n_cols; c++) col_mask[col_idx[c]] = 1;

    /* Map col_idx[c] -> output column index in the masked read */
    int *out_col = (int *)malloc((size_t)n_cols * sizeof(int));
    if (!out_col) { free(col_idx); free(col_mask); vtr1_close_tdc(file); vectra_error("alloc failed"); }
    for (int c = 0; c < n_cols; c++) {
        out_col[c] = 0;
        for (int j = 0; j < col_idx[c]; j++)
            if (col_mask[j]) out_col[c]++;
    }

    uint64_t *per_col_hash = (uint64_t *)malloc((size_t)n_cols * sizeof(uint64_t));

    int64_t entry_pos = 0;
    int64_t mask = n_slots - 1;

    for (uint32_t rg = 0; rg < n_rg; rg++) {
        VecBatch *batch = vtr1_read_rowgroup_tdc(file, rg, col_mask);
        for (int64_t r = 0; r < batch->n_rows; r++) {
            for (int c = 0; c < n_cols; c++)
                per_col_hash[c] = hash_array_value(&batch->columns[out_col[c]], r, ci);
            uint64_t h = combine_hashes(per_col_hash, n_cols);
            int64_t slot = (int64_t)(h & (uint64_t)mask);

            entry_hash[entry_pos] = h;
            entry_rg[entry_pos]   = rg;
            entry_next[entry_pos] = heads[slot];
            heads[slot] = entry_pos;
            entry_pos++;
        }
        vec_batch_free(batch);
    }

    free(col_mask);
    free(out_col);
    free(per_col_hash);

    /* Write v2 .vtri file */
    char *vtri_path = vtri_make_path_composite(vtr_path, col_names, n_cols);
    if (!vtri_path) { free(col_idx); vtr1_close_tdc(file); vectra_error("alloc failed"); }

    FILE *fp = fopen(vtri_path, "wb");
    if (!fp) {
        free(vtri_path); free(col_idx);
        free(entry_hash); free(entry_rg); free(heads); free(entry_next);
        vtr1_close_tdc(file);
        vectra_error("cannot create vtri index file");
    }

    /* v2 Header */
    fwrite("VTRI", 1, 4, fp);
    write_u16(fp, 2);                  /* version 2 */
    write_u16(fp, (uint16_t)n_cols);   /* number of columns */
    write_u8(fp, (uint8_t)ci);
    for (int c = 0; c < n_cols; c++)
        write_u16(fp, (uint16_t)col_idx[c]);
    write_u64(fp, (uint64_t)n_entries);
    write_u64(fp, (uint64_t)n_slots);

    /* Entry arrays (same as v1) */
    for (int64_t i = 0; i < n_entries; i++) write_u64(fp, entry_hash[i]);
    for (int64_t i = 0; i < n_entries; i++) write_u32(fp, entry_rg[i]);
    for (int64_t i = 0; i < n_slots; i++)  write_i64(fp, heads[i]);
    for (int64_t i = 0; i < n_entries; i++) write_i64(fp, entry_next[i]);

    fclose(fp);
    free(vtri_path);
    free(col_idx);
    free(entry_hash);
    free(entry_rg);
    free(heads);
    free(entry_next);
    vtr1_close_tdc(file);
}

/* ------------------------------------------------------------------ */
/*  vtri_open: read a .vtri index                                      */
/* ------------------------------------------------------------------ */

VtrIndex *vtri_open(const char *vtri_path, const VecSchema *schema) {
    FILE *fp = fopen(vtri_path, "rb");
    if (!fp) return NULL;

    /* Magic */
    char magic[4];
    if (fread(magic, 1, 4, fp) != 4 || memcmp(magic, "VTRI", 4) != 0) {
        fclose(fp);
        return NULL;
    }

    uint16_t version = read_u16_f(fp);
    if (version != 1 && version != 2) {
        fclose(fp);
        vectra_error("unsupported .vtri version: %u", version);
    }

    VtrIndex *idx = (VtrIndex *)calloc(1, sizeof(VtrIndex));
    if (!idx) { fclose(fp); vectra_error("alloc failed for VtrIndex"); }

    if (version == 1) {
        idx->col_idx   = read_u16_f(fp);
        idx->ci        = read_u8_f(fp);
        idx->n_cols    = 1;
        idx->col_indices = (uint16_t *)malloc(sizeof(uint16_t));
        idx->col_indices[0] = idx->col_idx;
        /* Resolve column name from schema */
        if (schema && idx->col_idx < (uint16_t)schema->n_cols) {
            idx->col_name = (char *)malloc(strlen(schema->col_names[idx->col_idx]) + 1);
            if (idx->col_name) strcpy(idx->col_name, schema->col_names[idx->col_idx]);
            idx->col_names = (char **)malloc(sizeof(char *));
            idx->col_names[0] = (char *)malloc(strlen(idx->col_name) + 1);
            strcpy(idx->col_names[0], idx->col_name);
        }
    } else {
        /* v2: composite index */
        idx->n_cols = read_u16_f(fp);
        idx->ci     = read_u8_f(fp);
        idx->col_indices = (uint16_t *)malloc((size_t)idx->n_cols * sizeof(uint16_t));
        idx->col_names   = (char **)calloc((size_t)idx->n_cols, sizeof(char *));
        for (int c = 0; c < idx->n_cols; c++) {
            idx->col_indices[c] = read_u16_f(fp);
            if (schema && idx->col_indices[c] < (uint16_t)schema->n_cols) {
                const char *nm = schema->col_names[idx->col_indices[c]];
                idx->col_names[c] = (char *)malloc(strlen(nm) + 1);
                strcpy(idx->col_names[c], nm);
            }
        }
        idx->col_idx = idx->col_indices[0]; /* compat */
        if (idx->col_names[0]) {
            idx->col_name = (char *)malloc(strlen(idx->col_names[0]) + 1);
            strcpy(idx->col_name, idx->col_names[0]);
        }
    }

    idx->n_entries = (int64_t)read_u64_f(fp);
    idx->n_slots   = (int64_t)read_u64_f(fp);

    /* Read arrays */
    int64_t ne = idx->n_entries;
    int64_t ns = idx->n_slots;

    idx->entry_hash = (uint64_t *)malloc((size_t)ne * sizeof(uint64_t));
    idx->entry_rg   = (uint32_t *)malloc((size_t)ne * sizeof(uint32_t));
    idx->heads       = (int64_t *)malloc((size_t)ns * sizeof(int64_t));
    idx->entry_next  = (int64_t *)malloc((size_t)ne * sizeof(int64_t));

    if (!idx->entry_hash || !idx->entry_rg || !idx->heads || !idx->entry_next) {
        fclose(fp);
        vtri_close(idx);
        vectra_error("alloc failed reading vtri index");
    }

    for (int64_t i = 0; i < ne; i++)
        idx->entry_hash[i] = read_u64_f(fp);
    for (int64_t i = 0; i < ne; i++)
        idx->entry_rg[i] = read_u32_f(fp);
    for (int64_t i = 0; i < ns; i++)
        idx->heads[i] = read_i64_f(fp);
    for (int64_t i = 0; i < ne; i++)
        idx->entry_next[i] = read_i64_f(fp);

    fclose(fp);
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

    int64_t mask = idx->n_slots - 1;
    int64_t slot = (int64_t)(h & (uint64_t)mask);
    int64_t e = idx->heads[slot];

    while (e >= 0) {
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
