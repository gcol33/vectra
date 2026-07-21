/*
 * src/format/rowgroup.c
 *
 * Row-group index wire codec for TDC1 containers. See
 * rowgroup_internal.h for the layout; this file is its only
 * implementation. The stream encoder, the stream decoder, and the
 * widening encoder all call through here, so the writer and the reader
 * cannot drift apart.
 *
 * All allocations go through the caller-supplied realloc_fn.
 */

#include "rowgroup_internal.h"
#include "stats_internal.h"

#include <string.h>  /* memcpy, memset */

/* ---- Little-endian helpers (no-op on LE targets) ------------------------- */

static void write_u16(uint8_t *dst, uint16_t v) { memcpy(dst, &v, 2); }
static void write_u32(uint8_t *dst, uint32_t v) { memcpy(dst, &v, 4); }
static void write_u64(uint8_t *dst, uint64_t v) { memcpy(dst, &v, 8); }

static uint16_t read_u16(const uint8_t *src) { uint16_t v; memcpy(&v, src, 2); return v; }
static uint32_t read_u32(const uint8_t *src) { uint32_t v; memcpy(&v, src, 4); return v; }
static uint64_t read_u64(const uint8_t *src) { uint64_t v; memcpy(&v, src, 8); return v; }

/* Bytes of the stats block attached to one row group (0 when absent). */
static uint32_t rg_stats_size(const tdc_rg_group *g) {
    return g->has_stats
         ? (uint32_t)g->entry.n_cols * TDC_STATS_ENTRY_SIZE
         : 0u;
}

/* ---- Serialized size ----------------------------------------------------- */

size_t tdc_rowgroup_index_serialized_size(const tdc_rowgroup_index *idx)
{
    if (!idx) return 0;

    /* 8 bytes for n_rowgroups */
    size_t total = 8;

    for (uint64_t i = 0; i < idx->n_rowgroups; ++i) {
        const tdc_rg_group *g = &idx->groups[i];
        total += TDC_RG_HEADER_SIZE
               + (size_t)g->entry.n_cols * TDC_RG_COL_SIZE
               + (size_t)rg_stats_size(g);
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
        const tdc_rg_group *g          = &idx->groups[i];
        uint32_t            stats_size = rg_stats_size(g);

        write_u64(p,      g->entry.offset);
        write_u64(p + 8,  g->entry.n_rows);
        write_u16(p + 16, g->entry.n_cols);
        write_u16(p + 18, 0);            /* _pad */
        write_u32(p + 20, stats_size);
        p += TDC_RG_HEADER_SIZE;

        for (uint16_t c = 0; c < g->entry.n_cols; ++c) {
            write_u64(p,     g->entry.columns[c].block_offset);
            write_u64(p + 8, g->entry.columns[c].block_total);
            p += TDC_RG_COL_SIZE;
        }

        if (stats_size) {
            p += tdc_stats_serialize(g->stats, g->entry.n_cols, p);
        }
    }

    return (size_t)(p - buf);
}

/* ---- Parse --------------------------------------------------------------- */

/* Release a group array (partially or fully built). */
static void rg_free_groups(tdc_rg_group *groups, uint64_t n,
                           void *(*realloc_fn)(void *, void *, size_t),
                           void *alloc_user)
{
    for (uint64_t j = 0; j < n; ++j) {
        if (groups[j].entry.columns)
            realloc_fn(alloc_user, groups[j].entry.columns, 0);
        if (groups[j].stats)
            realloc_fn(alloc_user, groups[j].stats, 0);
    }
    realloc_fn(alloc_user, groups, 0);
}

tdc_status tdc_rowgroup_index_parse(
    const uint8_t *buf, size_t buf_size,
    int has_stats_flag,
    void *(*realloc_fn)(void *user, void *ptr, size_t sz),
    void *alloc_user,
    tdc_rowgroup_index *out)
{
    if (!buf || !realloc_fn || !out) return TDC_E_INVAL;

    memset(out, 0, sizeof(*out));

    /* Need at least 8 bytes for n_rowgroups. */
    if (buf_size < 8) return TDC_E_CORRUPT;

    uint64_t n_rg = read_u64(buf);

    /* Zero row groups is valid (empty container). */
    if (n_rg == 0) return TDC_OK;

    /* Bound n_rg by the smallest possible per-group footprint before
     * allocating or walking, so a corrupt count cannot drive a huge
     * allocation or a long loop. */
    size_t remaining = buf_size - 8;
    if (n_rg > remaining / TDC_RG_HEADER_SIZE) return TDC_E_CORRUPT;

    /* Pass 1: validate the whole buffer before allocating anything.
     * n_cols varies per row group, so this has to walk. */
    {
        size_t pos = 8;
        for (uint64_t i = 0; i < n_rg; ++i) {
            if (pos + TDC_RG_HEADER_SIZE > buf_size) return TDC_E_CORRUPT;
            uint16_t nc       = read_u16(buf + pos + 16);
            uint16_t pad      = read_u16(buf + pos + 18);
            uint32_t stats_sz = read_u32(buf + pos + 20);

            if (pad != 0) return TDC_E_CORRUPT;

            /* A row group always carries at least one column: the encoder
             * refuses to close an empty one. */
            if (nc == 0) return TDC_E_CORRUPT;

            /* A stats block may only appear when the container advertised
             * stats, and its size is fully determined by n_cols. */
            if (stats_sz != 0) {
                if (!has_stats_flag) return TDC_E_CORRUPT;
                if (stats_sz != (uint32_t)nc * TDC_STATS_ENTRY_SIZE)
                    return TDC_E_CORRUPT;
            }

            pos += TDC_RG_HEADER_SIZE
                 + (size_t)nc * TDC_RG_COL_SIZE
                 + (size_t)stats_sz;
            if (pos > buf_size) return TDC_E_CORRUPT;
        }
    }

    tdc_rg_group *groups = (tdc_rg_group *)realloc_fn(
        alloc_user, NULL, (size_t)n_rg * sizeof(tdc_rg_group));
    if (!groups) return TDC_E_NOMEM;
    memset(groups, 0, (size_t)n_rg * sizeof(tdc_rg_group));

    /* Pass 2: fill. Sizes are already validated, so only allocation and
     * the stats codec can fail from here. */
    const uint8_t *p = buf + 8;
    for (uint64_t i = 0; i < n_rg; ++i) {
        tdc_rg_group *g = &groups[i];

        g->entry.offset   = read_u64(p);
        g->entry.n_rows   = read_u64(p + 8);
        g->entry.n_cols   = read_u16(p + 16);
        uint32_t stats_sz = read_u32(p + 20);
        p += TDC_RG_HEADER_SIZE;

        uint16_t nc = g->entry.n_cols;
        if (nc > 0) {
            size_t col_bytes = (size_t)nc * TDC_RG_COL_SIZE;
            tdc_rg_col_entry *cols =
                (tdc_rg_col_entry *)realloc_fn(alloc_user, NULL, col_bytes);
            if (!cols) {
                rg_free_groups(groups, n_rg, realloc_fn, alloc_user);
                return TDC_E_NOMEM;
            }
            for (uint16_t c = 0; c < nc; ++c) {
                cols[c].block_offset = read_u64(p);
                cols[c].block_total  = read_u64(p + 8);
                p += TDC_RG_COL_SIZE;
            }
            g->entry.columns = cols;
        }

        if (stats_sz) {
            tdc_column_stats *st = (tdc_column_stats *)realloc_fn(
                alloc_user, NULL, (size_t)nc * sizeof(tdc_column_stats));
            if (!st) {
                rg_free_groups(groups, n_rg, realloc_fn, alloc_user);
                return TDC_E_NOMEM;
            }
            tdc_status sst = tdc_stats_parse(p, stats_sz, nc, st);
            if (sst != TDC_OK) {
                realloc_fn(alloc_user, st, 0);
                rg_free_groups(groups, n_rg, realloc_fn, alloc_user);
                return sst;
            }
            g->stats     = st;
            g->has_stats = 1;
            p += stats_sz;
        }
    }

    out->n_rowgroups = n_rg;
    out->groups      = groups;
    return TDC_OK;
}

/* ---- Free ---------------------------------------------------------------- */

void tdc_rowgroup_index_free(
    tdc_rowgroup_index *idx,
    void *(*realloc_fn)(void *user, void *ptr, size_t sz),
    void *alloc_user)
{
    if (!idx || !realloc_fn) return;

    if (idx->groups)
        rg_free_groups(idx->groups, idx->n_rowgroups, realloc_fn, alloc_user);

    idx->n_rowgroups = 0;
    idx->groups      = NULL;
}
