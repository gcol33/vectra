/*
 * src/format/rowgroup.c
 *
 * Row-group index serialization and parsing for TDC1 v2 containers.
 *
 * Wire format (all little-endian):
 *
 *   u64 n_rowgroups
 *   per row group:
 *       u64 offset
 *       u64 n_rows
 *       u16 n_cols
 *       u16 _pad       (0)
 *       u32 _reserved  (0)
 *       per column:
 *           u64 block_offset
 *           u64 block_total
 *
 * All allocations go through the caller-supplied realloc_fn.
 */

#include "rowgroup_internal.h"

#include <string.h>  /* memcpy, memset */

/* ---- Little-endian helpers (no-op on LE targets) ------------------------- */

static void write_u16(uint8_t *dst, uint16_t v) { memcpy(dst, &v, 2); }
static void write_u32(uint8_t *dst, uint32_t v) { memcpy(dst, &v, 4); }
static void write_u64(uint8_t *dst, uint64_t v) { memcpy(dst, &v, 8); }

static uint16_t read_u16(const uint8_t *src) { uint16_t v; memcpy(&v, src, 2); return v; }
static uint32_t read_u32(const uint8_t *src) { uint32_t v; memcpy(&v, src, 4); return v; }
static uint64_t read_u64(const uint8_t *src) { uint64_t v; memcpy(&v, src, 8); return v; }

/* ---- Serialized size ----------------------------------------------------- */

size_t tdc_rowgroup_index_serialized_size(const tdc_rowgroup_index *idx)
{
    if (!idx) return 0;

    /* 8 bytes for n_rowgroups */
    size_t total = 8;

    for (uint64_t i = 0; i < idx->n_rowgroups; ++i) {
        /* 24-byte fixed header + 16 bytes per column */
        total += TDC_RG_HEADER_SIZE
               + (size_t)idx->entries[i].n_cols * TDC_RG_COL_SIZE;
    }
    return total;
}

/* ---- Serialize ----------------------------------------------------------- */

size_t tdc_rowgroup_index_serialize(const tdc_rowgroup_index *idx,
                                   uint8_t *buf)
{
    if (!idx || !buf) return 0;

    uint8_t *p = buf;

    write_u64(p, idx->n_rowgroups);
    p += 8;

    for (uint64_t i = 0; i < idx->n_rowgroups; ++i) {
        const tdc_rowgroup_entry *rg = &idx->entries[i];

        write_u64(p,      rg->offset);
        write_u64(p + 8,  rg->n_rows);
        write_u16(p + 16, rg->n_cols);
        write_u16(p + 18, 0);   /* _pad */
        write_u32(p + 20, 0);   /* _reserved */
        p += TDC_RG_HEADER_SIZE;

        for (uint16_t c = 0; c < rg->n_cols; ++c) {
            write_u64(p,     rg->columns[c].block_offset);
            write_u64(p + 8, rg->columns[c].block_total);
            p += TDC_RG_COL_SIZE;
        }
    }

    return (size_t)(p - buf);
}

/* ---- Parse --------------------------------------------------------------- */

tdc_status tdc_rowgroup_index_parse(
    const uint8_t *buf, size_t buf_size,
    void *(*realloc_fn)(void *user, void *ptr, size_t sz),
    void *alloc_user,
    tdc_rowgroup_index *out)
{
    if (!buf || !realloc_fn || !out) return TDC_E_INVAL;

    memset(out, 0, sizeof(*out));

    /* Need at least 8 bytes for n_rowgroups. */
    if (buf_size < 8) return TDC_E_CORRUPT;

    uint64_t n_rg = read_u64(buf);
    const uint8_t *p = buf + 8;
    size_t remaining = buf_size - 8;

    /* Sanity: each row group needs at least TDC_RG_HEADER_SIZE bytes
     * (n_cols >= 1 enforced below, but the header alone is 24). */
    if (n_rg > remaining / TDC_RG_HEADER_SIZE) return TDC_E_CORRUPT;

    /* Zero row groups is valid (empty container). */
    if (n_rg == 0) {
        out->n_rowgroups = 0;
        out->entries     = NULL;
        return TDC_OK;
    }

    /* Allocate entries array. */
    tdc_rowgroup_entry *entries = (tdc_rowgroup_entry *)realloc_fn(
        alloc_user, NULL, (size_t)n_rg * sizeof(tdc_rowgroup_entry));
    if (!entries) return TDC_E_NOMEM;
    memset(entries, 0, (size_t)n_rg * sizeof(tdc_rowgroup_entry));

    for (uint64_t i = 0; i < n_rg; ++i) {
        if (remaining < TDC_RG_HEADER_SIZE) goto corrupt;

        entries[i].offset = read_u64(p);
        entries[i].n_rows = read_u64(p + 8);
        entries[i].n_cols = read_u16(p + 16);

        uint16_t pad      = read_u16(p + 18);
        uint32_t reserved = read_u32(p + 20);
        if (pad != 0 || reserved != 0) goto corrupt;

        /* At least one column per row group. */
        if (entries[i].n_cols == 0) goto corrupt;

        p         += TDC_RG_HEADER_SIZE;
        remaining -= TDC_RG_HEADER_SIZE;

        size_t col_bytes = (size_t)entries[i].n_cols * TDC_RG_COL_SIZE;
        if (remaining < col_bytes) goto corrupt;

        tdc_rg_col_entry *cols = (tdc_rg_col_entry *)realloc_fn(
            alloc_user, NULL, col_bytes);
        if (!cols) goto nomem;

        for (uint16_t c = 0; c < entries[i].n_cols; ++c) {
            cols[c].block_offset = read_u64(p);
            cols[c].block_total  = read_u64(p + 8);
            p += TDC_RG_COL_SIZE;
        }
        remaining -= col_bytes;

        entries[i].columns = cols;
    }

    out->n_rowgroups = n_rg;
    out->entries     = entries;
    return TDC_OK;

corrupt:
    /* Free everything allocated so far. */
    for (uint64_t j = 0; j < n_rg; ++j) {
        if (entries[j].columns)
            realloc_fn(alloc_user, entries[j].columns, 0);
    }
    realloc_fn(alloc_user, entries, 0);
    memset(out, 0, sizeof(*out));
    return TDC_E_CORRUPT;

nomem:
    for (uint64_t j = 0; j < n_rg; ++j) {
        if (entries[j].columns)
            realloc_fn(alloc_user, entries[j].columns, 0);
    }
    realloc_fn(alloc_user, entries, 0);
    memset(out, 0, sizeof(*out));
    return TDC_E_NOMEM;
}

/* ---- Free ---------------------------------------------------------------- */

void tdc_rowgroup_index_free(
    tdc_rowgroup_index *idx,
    void *(*realloc_fn)(void *user, void *ptr, size_t sz),
    void *alloc_user)
{
    if (!idx || !realloc_fn) return;

    for (uint64_t i = 0; i < idx->n_rowgroups; ++i) {
        if (idx->entries[i].columns)
            realloc_fn(alloc_user, idx->entries[i].columns, 0);
    }
    if (idx->entries)
        realloc_fn(alloc_user, idx->entries, 0);

    idx->n_rowgroups = 0;
    idx->entries     = NULL;
}
