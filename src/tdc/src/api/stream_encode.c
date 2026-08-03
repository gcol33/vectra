/*
 * src/api/stream_encode.c
 *
 * Implements the streaming encoder API declared in tdc/stream.h.
 *
 * Features:
 *   1. An optional column schema written after the 64-byte container header.
 *   2. A row-group-level trailing index for column-level random access.
 *
 * Operating modes:
 *   - Seekable (io.seek_fn != NULL): the container header is patched at close
 *     with the final n_blocks, index_offset, index_size, and schema_size.
 *   - Forward-only (io.seek_fn == NULL): the header is written with n_blocks=0
 *     (deferred-index mode). The trailing index is still appended.
 */

#ifdef _MSC_VER
#  define _CRT_SECURE_NO_WARNINGS 1
#endif

#include "tdc/stream.h"
#include "tdc/codec.h"
#include "tdc/format.h"
#include "tdc/types.h"
#include "../format/schema_internal.h"
#include "../format/metadata_internal.h"
#include "../format/stats_internal.h"
#include "../format/rowgroup_internal.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* The row-group index layout lives in ../format/rowgroup_internal.h and is
 * serialized by ../format/rowgroup.c; this file only fills in the in-memory
 * tdc_rowgroup_index it hands over. */
typedef tdc_rg_col_entry v2_col_entry;
typedef tdc_rg_group     v2_rowgroup_entry;

/*
 * How the encoder was opened, which decides what it may write and what it
 * emits at close.
 *
 * CREATE builds a container from nothing: it wrote the header (and schema) at
 * open and appends row groups.
 *
 * WIDEN and EXTEND both REOPEN an existing container and write past its
 * trailing index, leaving everything already on disk untouched; they differ
 * only in what they add. Widening gives the row groups already present a new
 * column; extending adds row groups carrying the columns already declared.
 * Both rebuild the index at close, patch the header last, and leave the
 * blocks region with a gap where the superseded index sits -- which is what
 * TDC_CONTAINER_VERSION_WIDENED marks.
 */
typedef enum {
    V2_MODE_CREATE = 0,
    V2_MODE_WIDEN,
    V2_MODE_EXTEND
} v2_mode;

/* ----- Internal state ----------------------------------------------------- */

/*
 * The v2 encoder uses its own internal struct (not the v1 tdc_stream_encoder)
 * because v2 needs row-group tracking. The opaque tdc_stream_encoder pointer
 * returned to the caller is actually a v2_stream_encoder_state* cast to
 * tdc_stream_encoder*. This is safe because tdc_stream_encoder is an opaque
 * incomplete type in the public header — the caller never dereferences it.
 */
typedef struct {
    tdc_io      io;
    uint16_t    flags;
    tdc_dtype   global_dtype;
    tdc_layout  global_layout;
    tdc_shape   global_shape;

    void *(*realloc_fn)(void *user, void *ptr, size_t new_size);
    void   *alloc_user;

    uint64_t    n_blocks;       /* total blocks written across all groups */
    uint64_t    write_pos;      /* current byte offset in output stream */
    int         seekable;
    uint32_t    schema_size;    /* bytes of serialized schema (0 if none) */

    /* Scratch buffer for tdc_encode_block output (reused across blocks). */
    tdc_buffer  scratch;

    /* --- Current (open) row group accumulator --- */
    v2_col_entry *cur_cols;     /* growable array */
    uint64_t      cur_cols_cap; /* entries allocated */
    uint64_t      cur_n_cols;   /* entries used */
    uint64_t      cur_group_offset; /* write_pos of first block in cur group */

    /* Per-column stats attached to the currently-open row group via
     * set_rowgroup_stats. Owned; NULL when no stats were supplied
     * for the current group. */
    tdc_column_stats *cur_stats;
    uint16_t          cur_stats_n_cols;
    uint8_t           cur_has_stats;

    /* Sticky flag: set the first time any row group receives stats.
     * Determines whether HAS_STATS is recorded in the final header. */
    uint8_t           any_stats;

    /* --- Finalized row groups --- */
    v2_rowgroup_entry *groups;
    uint64_t           groups_cap;
    uint64_t           n_groups;

    /* --- Reopened-container state (WIDEN / EXTEND) --- */
    uint8_t   mode;           /* v2_mode */
    uint64_t  blocks_start;   /* preserved from the existing container */

    /* WIDEN: the serialized replacement schema, written at the tail at close
     * because the original slot at offset 64 cannot grow in place. Owned. */
    uint8_t  *schema_buf;

    /* EXTEND: the existing schema is reused as-is, so nothing is written for
     * it and the header just records where it already sits. schema_n_cols is
     * what each appended row group must supply. */
    uint64_t  kept_schema_offset;
    uint16_t  schema_n_cols;
} v2_stream_encoder_state;

/* ----- Helpers ------------------------------------------------------------ */

static tdc_status v2_io_write_exact(const tdc_io *io,
                                    const void   *data,
                                    size_t        size) {
    return io->write_fn(io->ctx, data, size);
}

static void *v2_alloc(v2_stream_encoder_state *e, void *ptr, size_t sz) {
    return e->realloc_fn(e->alloc_user, ptr, sz);
}

static void v2_free(v2_stream_encoder_state *e, void *ptr) {
    if (ptr) e->realloc_fn(e->alloc_user, ptr, 0);
}

/* Build and write the 64-byte container header.
 *
 * schema_offset is meaningful only in widen mode, where the schema was
 * relocated to the tail: it stamps the header as a widened container and
 * records where the schema and the blocks region begin. Pass 0 otherwise. */
static tdc_status v2_write_header(v2_stream_encoder_state *e,
                                  uint64_t n_blocks,
                                  uint64_t index_offset,
                                  uint64_t index_size,
                                  uint64_t schema_offset) {
    tdc_container_header hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic   = TDC_CONTAINER_MAGIC;
    hdr.version = schema_offset ? TDC_CONTAINER_VERSION_WIDENED
                                : TDC_CONTAINER_VERSION;
    hdr.flags   = e->flags;

    if (schema_offset) {
        hdr.u.het.schema_offset = schema_offset;
        hdr.u.het.blocks_start  = e->blocks_start;
    }

    if (!(e->flags & TDC_CONTAINER_FLAG_HETEROGENEOUS)) {
        hdr.global_dtype  = (uint8_t)e->global_dtype;
        hdr.global_layout = (uint8_t)e->global_layout;
        hdr.global_rank   = e->global_shape.rank;
        for (uint8_t i = 0; i < TDC_MAX_RANK; ++i) {
            hdr.u.global_dim[i] = (i < e->global_shape.rank)
                                ? e->global_shape.dim[i] : 0;
        }
    }

    if (e->any_stats) {
        hdr.flags |= TDC_CONTAINER_FLAG_HAS_STATS;
    }

    hdr.n_blocks     = n_blocks;
    hdr.index_offset = index_offset;
    hdr.index_size   = index_size;

    hdr.schema_size  = e->schema_size;

    return v2_io_write_exact(&e->io, &hdr, TDC_CONTAINER_HEADER_SIZE);
}

/* View the accumulated groups as the index the wire codec serializes. */
static tdc_rowgroup_index v2_index_view(const v2_stream_encoder_state *e) {
    tdc_rowgroup_index idx;
    idx.n_rowgroups = e->n_groups;
    idx.groups      = e->groups;
    return idx;
}

static uint64_t v2_index_serialized_size(const v2_stream_encoder_state *e) {
    tdc_rowgroup_index idx = v2_index_view(e);
    return (uint64_t)tdc_rowgroup_index_serialized_size(&idx);
}

static size_t v2_index_serialize(const v2_stream_encoder_state *e,
                                 uint8_t *buf) {
    tdc_rowgroup_index idx = v2_index_view(e);
    return tdc_rowgroup_index_serialize(&idx, buf);
}

/* Free all row-group entries and their column arrays. */
static void v2_free_groups(v2_stream_encoder_state *e) {
    tdc_rowgroup_index idx = v2_index_view(e);
    tdc_rowgroup_index_free(&idx, e->realloc_fn, e->alloc_user);
    e->groups     = NULL;
    e->groups_cap = 0;
    e->n_groups   = 0;
}

/* ----- Public API --------------------------------------------------------- */

tdc_status tdc_stream_encoder_open(const tdc_stream_encoder_config *cfg,
                                   tdc_stream_encoder **enc) {
    if (!cfg || !enc) return TDC_E_INVAL;
    *enc = NULL;

    if (!cfg->io.write_fn) return TDC_E_INVAL;
    if (!cfg->realloc_fn)  return TDC_E_INVAL;

    /* Allocate state. */
    v2_stream_encoder_state *e = (v2_stream_encoder_state *)cfg->realloc_fn(
        cfg->alloc_user, NULL, sizeof(v2_stream_encoder_state));
    if (!e) return TDC_E_NOMEM;
    memset(e, 0, sizeof(*e));

    e->io            = cfg->io;
    e->flags         = cfg->flags;
    e->global_dtype  = cfg->global_dtype;
    e->global_layout = cfg->global_layout;
    e->global_shape  = cfg->global_shape;
    e->realloc_fn    = cfg->realloc_fn;
    e->alloc_user    = cfg->alloc_user;
    e->seekable      = (cfg->io.seek_fn != NULL);

    /* Set up the scratch buffer with the caller's allocator. */
    e->scratch.data       = NULL;
    e->scratch.size       = 0;
    e->scratch.capacity   = 0;
    e->scratch.realloc_fn = cfg->realloc_fn;
    e->scratch.user       = cfg->alloc_user;

    /* Compute schema size (if schema provided). */
    e->schema_size = 0;
    if (cfg->schema) {
        size_t sz = tdc_schema_serialized_size(cfg->schema);
        if (sz > UINT32_MAX) {
            cfg->realloc_fn(cfg->alloc_user, e, 0);
            return TDC_E_INVAL;
        }
        e->schema_size = (uint32_t)sz;
    }

    /* Write the container header (version 2, deferred fields zeroed). */
    tdc_status st = v2_write_header(e, 0, 0, 0, 0);
    if (st != TDC_OK) {
        cfg->realloc_fn(cfg->alloc_user, e, 0);
        return st;
    }
    e->write_pos = TDC_CONTAINER_HEADER_SIZE;

    /* Write the schema section if present. */
    if (e->schema_size > 0) {
        uint8_t *schema_buf = (uint8_t *)v2_alloc(e, NULL, e->schema_size);
        if (!schema_buf) {
            cfg->realloc_fn(cfg->alloc_user, e, 0);
            return TDC_E_NOMEM;
        }

        tdc_schema_serialize(cfg->schema, schema_buf);

        st = v2_io_write_exact(&e->io, schema_buf, e->schema_size);
        v2_free(e, schema_buf);

        if (st != TDC_OK) {
            cfg->realloc_fn(cfg->alloc_user, e, 0);
            return st;
        }
        e->write_pos += e->schema_size;
    }

    *enc = (tdc_stream_encoder *)e;
    return TDC_OK;
}

tdc_status tdc_stream_encoder_write_block(tdc_stream_encoder       *enc,
                                          const tdc_block          *src,
                                          const tdc_codec_spec     *spec) {
    if (!enc || !src || !spec) return TDC_E_INVAL;

    v2_stream_encoder_state *e = (v2_stream_encoder_state *)enc;

    /* A widening encoder adds columns to row groups that already exist, and
     * names the one it means per call; the open-row-group accumulator this
     * path feeds has nothing to attach to. Use widen_block there. */
    if (e->mode == V2_MODE_WIDEN) return TDC_E_INVAL;

    /* Encode the block into the scratch buffer. */
    e->scratch.size = 0;
    tdc_status st = tdc_encode_block(src, spec, &e->scratch);
    if (st != TDC_OK) return st;

    /* Track the first block's offset for the current row group. */
    if (e->cur_n_cols == 0) {
        e->cur_group_offset = e->write_pos;
    }

    /* Grow the current row group's column array if needed. */
    if (e->cur_n_cols >= e->cur_cols_cap) {
        uint64_t new_cap  = e->cur_cols_cap ? e->cur_cols_cap * 2 : 16;
        size_t   alloc_sz = (size_t)(new_cap * sizeof(v2_col_entry));
        void *p = v2_alloc(e, e->cur_cols, alloc_sz);
        if (!p) return TDC_E_NOMEM;
        e->cur_cols     = (v2_col_entry *)p;
        e->cur_cols_cap = new_cap;
    }

    /* Record this column's block entry. */
    e->cur_cols[e->cur_n_cols].block_offset = e->write_pos;
    e->cur_cols[e->cur_n_cols].block_total  = e->scratch.size;
    e->cur_n_cols++;

    /* Write the encoded block record to the output stream. */
    st = v2_io_write_exact(&e->io, e->scratch.data, e->scratch.size);
    if (st != TDC_OK) return st;

    e->write_pos += e->scratch.size;
    e->n_blocks++;
    return TDC_OK;
}

tdc_status tdc_stream_encoder_end_rowgroup(tdc_stream_encoder *enc,
                                           uint64_t            n_rows) {
    if (!enc) return TDC_E_INVAL;

    v2_stream_encoder_state *e = (v2_stream_encoder_state *)enc;

    if (e->mode == V2_MODE_WIDEN) return TDC_E_INVAL;

    /* Must have written at least one block since the last end_rowgroup. */
    if (e->cur_n_cols == 0) return TDC_E_INVAL;

    /* A row group appended to an existing container must carry exactly the
     * columns that container declares: entries are positional against the
     * schema, so a short or long group would misalign every column read out
     * of it. A fresh container has no such constraint to check against --
     * its schema is whatever the caller declared at open. */
    if (e->mode == V2_MODE_EXTEND && e->cur_n_cols != (uint64_t)e->schema_n_cols)
        return TDC_E_INVAL;

    /* Grow the finalized groups array if needed. */
    if (e->n_groups >= e->groups_cap) {
        uint64_t new_cap  = e->groups_cap ? e->groups_cap * 2 : 16;
        size_t   alloc_sz = (size_t)(new_cap * sizeof(v2_rowgroup_entry));
        void *p = v2_alloc(e, e->groups, alloc_sz);
        if (!p) return TDC_E_NOMEM;
        e->groups     = (v2_rowgroup_entry *)p;
        e->groups_cap = new_cap;
    }

    /* Finalize the current row group. Transfer ownership of cur_cols. */
    v2_rowgroup_entry *rg = &e->groups[e->n_groups];
    memset(rg, 0, sizeof(*rg));
    rg->entry.offset = e->cur_group_offset;
    rg->entry.n_rows = n_rows;
    rg->entry.n_cols = (uint16_t)e->cur_n_cols;

    /* Allocate a right-sized copy of the column entries. */
    size_t cols_sz = (size_t)(e->cur_n_cols * sizeof(v2_col_entry));
    rg->entry.columns = (v2_col_entry *)v2_alloc(e, NULL, cols_sz);
    if (!rg->entry.columns) return TDC_E_NOMEM;
    memcpy(rg->entry.columns, e->cur_cols, cols_sz);

    /* Attach pending stats (if any). set_rowgroup_stats validated that
     * cur_stats_n_cols == cur_n_cols, so we can transfer ownership. */
    if (e->cur_has_stats) {
        rg->has_stats = 1;
        rg->stats     = e->cur_stats;      /* transfer ownership */
        e->cur_stats  = NULL;
        e->cur_stats_n_cols = 0;
        e->cur_has_stats    = 0;
    }

    e->n_groups++;

    /* Reset the accumulator (keep the buffer allocated for reuse). */
    e->cur_n_cols       = 0;
    e->cur_group_offset = 0;

    return TDC_OK;
}

tdc_status tdc_stream_encoder_set_rowgroup_stats(tdc_stream_encoder     *enc,
                                                 const tdc_column_stats *stats,
                                                 uint16_t                n_cols) {
    if (!enc || !stats) return TDC_E_INVAL;

    v2_stream_encoder_state *e = (v2_stream_encoder_state *)enc;

    /* Must be called after the group's blocks are written and before
     * end_rowgroup. The n_cols argument must match. */
    if (e->cur_n_cols == 0)        return TDC_E_INVAL;
    if (e->cur_has_stats)          return TDC_E_INVAL; /* called twice */
    if ((uint64_t)n_cols != e->cur_n_cols) return TDC_E_INVAL;

    size_t bytes = (size_t)n_cols * sizeof(tdc_column_stats);
    tdc_column_stats *copy = (tdc_column_stats *)v2_alloc(e, NULL, bytes);
    if (!copy) return TDC_E_NOMEM;
    memcpy(copy, stats, bytes);

    e->cur_stats        = copy;
    e->cur_stats_n_cols = n_cols;
    e->cur_has_stats    = 1;
    e->any_stats        = 1;
    return TDC_OK;
}

tdc_status tdc_stream_encoder_close(tdc_stream_encoder **enc) {
    if (!enc || !*enc) return TDC_OK;

    v2_stream_encoder_state *e = (v2_stream_encoder_state *)*enc;
    tdc_status first_err = TDC_OK;
    uint64_t   schema_offset = 0;

    /* A reopened container records where its schema lives, since a nonzero
     * schema_offset is what stamps the header as relocated-layout -- the
     * warning a reader needs once the superseded index has left a gap in the
     * blocks region.
     *
     * Widening had to move the schema: it grew, and the original slot sits
     * immediately before the first block with no room. So it is written here,
     * at the tail, just before the rebuilt index. Extending did not change the
     * schema at all, so it stays where it already is and only its address is
     * recorded. */
    if (e->mode == V2_MODE_WIDEN && e->schema_size > 0) {
        schema_offset = e->write_pos;
        tdc_status st = v2_io_write_exact(&e->io, e->schema_buf,
                                          e->schema_size);
        if (st != TDC_OK) first_err = st;
        else e->write_pos += e->schema_size;
    } else if (e->mode == V2_MODE_EXTEND) {
        schema_offset = e->kept_schema_offset;
    }

    /* Write the trailing row-group index. */
    uint64_t index_offset = e->write_pos;
    uint64_t index_size   = v2_index_serialized_size(e);

    if (e->n_groups > 0 && first_err == TDC_OK) {
        uint8_t *idx_buf = (uint8_t *)v2_alloc(e, NULL, (size_t)index_size);
        if (!idx_buf) {
            first_err = TDC_E_NOMEM;
        } else {
            v2_index_serialize(e, idx_buf);
            tdc_status st = v2_io_write_exact(&e->io, idx_buf,
                                              (size_t)index_size);
            v2_free(e, idx_buf);
            if (st != TDC_OK && first_err == TDC_OK) first_err = st;
        }
    }

    /* If seekable, patch the container header. */
    if (e->seekable && first_err == TDC_OK) {
        tdc_status st = e->io.seek_fn(e->io.ctx, 0, TDC_SEEK_SET);
        if (st != TDC_OK && first_err == TDC_OK) first_err = st;

        if (st == TDC_OK) {
            uint64_t final_n_blocks = e->n_blocks;
            uint64_t final_idx_off  = (e->n_groups > 0) ? index_offset : 0;
            uint64_t final_idx_sz   = (e->n_groups > 0) ? index_size   : 0;

            st = v2_write_header(e, final_n_blocks, final_idx_off,
                                 final_idx_sz, schema_offset);
            if (st != TDC_OK && first_err == TDC_OK) first_err = st;
        }
    }

    /* Free internal state regardless of errors. */
    v2_free(e, e->cur_cols);
    v2_free(e, e->cur_stats);
    v2_free(e, e->schema_buf);
    v2_free_groups(e);
    if (e->scratch.data) {
        e->realloc_fn(e->alloc_user, e->scratch.data, 0);
    }
    e->realloc_fn(e->alloc_user, e, 0);
    *enc = NULL;

    return first_err;
}

/* ----- Reopening an existing container -------------------------------------- */

/*
 * Widening (add columns) and extending (add row groups) are the same trick
 * applied to the two axes, and rest on two properties of the container format:
 *
 *   1. Block records are located solely by the (row group, column) offsets
 *      in the trailing index, so a new block may sit anywhere in the file --
 *      it does not have to be adjacent to the row group it belongs to. Which
 *      also means the entries already in the index stay correct verbatim:
 *      nothing before the old trailer moves, so nothing it points at moves.
 *   2. The index and the schema are both rewritten wholesale at close, so
 *      growing them costs only their own (small) size.
 *
 * Together those make the cost of either operation proportional to what is
 * being added, not to what is already in the file.
 *
 * The one thing that cannot move is the original schema section, pinned at
 * offset 64 immediately before the first block. Widening grows the schema, so
 * rather than shift the body to make room it writes the replacement at the
 * tail and points the header there. Extending does not touch the schema, so it
 * leaves it in place and records the address it already has.
 *
 * Crash behaviour: every byte either path writes goes PAST the existing
 * trailing index rather than over it, and the 64-byte header is patched last.
 * Interrupted at any point before that patch, the file still reads as the
 * container did before -- its header, schema, blocks and index are all
 * untouched, with unreferenced bytes appended. The cost is that the stale
 * index stays behind as a gap in the blocks region, which is why a reopened
 * container is random-access only (see tdc_stream_decoder_peek_block) and is
 * stamped TDC_CONTAINER_VERSION_WIDENED whichever axis it grew along.
 */

/* Allocate and prime the state for a reopen. */
static v2_stream_encoder_state *v2_new_reopen_state(
    const tdc_io *io,
    void *(*realloc_fn)(void *user, void *ptr, size_t new_size),
    void *alloc_user,
    v2_mode mode) {
    v2_stream_encoder_state *e = (v2_stream_encoder_state *)realloc_fn(
        alloc_user, NULL, sizeof(v2_stream_encoder_state));
    if (!e) return NULL;
    memset(e, 0, sizeof(*e));

    e->io         = *io;
    e->realloc_fn = realloc_fn;
    e->alloc_user = alloc_user;
    e->seekable   = 1;
    e->mode       = (uint8_t)mode;

    e->scratch.realloc_fn = realloc_fn;
    e->scratch.user       = alloc_user;
    return e;
}

/* Teardown for an open that failed: no handle escaped, and nothing was
 * written, so this only has to give back memory. */
static void v2_open_abort(v2_stream_encoder_state *e) {
    v2_free(e, e->schema_buf);
    if (e->groups) v2_free_groups(e);
    if (e->scratch.data) e->realloc_fn(e->alloc_user, e->scratch.data, 0);
    e->realloc_fn(e->alloc_user, e, 0);
}

static tdc_status v2_read_exact(v2_stream_encoder_state *e,
                                uint8_t *buf, size_t size) {
    size_t total = 0;
    while (total < size) {
        size_t got = 0;
        tdc_status st = e->io.read_fn(e->io.ctx, buf + total,
                                      size - total, &got);
        if (st != TDC_OK) return TDC_E_IO;
        if (got == 0)     return TDC_E_CORRUPT;
        total += got;
    }
    return TDC_OK;
}

/*
 * Validate an existing container, adopt its row-group index, and leave the
 * write cursor just past its trailing index -- the first byte no reader is
 * looking at. Shared by widening and extending, which differ only in what they
 * write from there and in what becomes of the schema.
 *
 * On success the header is handed back, since each caller needs a different
 * part of it. On failure nothing is freed: the caller owns the state.
 */
static tdc_status v2_reopen_existing(v2_stream_encoder_state *e,
                                     tdc_container_header *out_hdr) {
    tdc_container_header hdr;
    tdc_status st = e->io.seek_fn(e->io.ctx, 0, TDC_SEEK_SET);
    if (st != TDC_OK) return st;

    st = v2_read_exact(e, (uint8_t *)&hdr, TDC_CONTAINER_HEADER_SIZE);
    if (st != TDC_OK) return st;

    if (hdr.magic != TDC_CONTAINER_MAGIC) return TDC_E_CORRUPT;
    if (hdr.version != TDC_CONTAINER_VERSION &&
        hdr.version != TDC_CONTAINER_VERSION_WIDENED)
        return TDC_E_VERSION;
    /* Only a heterogeneous container can be reopened: the relocated-schema
     * pointers occupy the global-shape slots, and a per-column schema is what
     * both operations are defined against. */
    if (!(hdr.flags & TDC_CONTAINER_FLAG_HETEROGENEOUS)) return TDC_E_INVAL;
    /* There must be an index to carry over. */
    if (hdr.index_offset == 0 || hdr.index_size == 0) return TDC_E_INVAL;

    e->flags = hdr.flags;
    e->blocks_start = (hdr.version == TDC_CONTAINER_VERSION_WIDENED)
                    ? hdr.u.het.blocks_start
                    : TDC_CONTAINER_HEADER_SIZE + hdr.schema_size;
    e->n_blocks  = hdr.n_blocks;
    e->any_stats = (hdr.flags & TDC_CONTAINER_FLAG_HAS_STATS) != 0;

    /* ---- Read and parse the existing row-group index. ---- */
    {
        size_t idx_bytes = (size_t)hdr.index_size;
        uint8_t *idx_buf = (uint8_t *)v2_alloc(e, NULL, idx_bytes);
        if (!idx_buf) return TDC_E_NOMEM;

        st = e->io.seek_fn(e->io.ctx, (int64_t)hdr.index_offset, TDC_SEEK_SET);
        if (st != TDC_OK) { v2_free(e, idx_buf); return st; }

        st = v2_read_exact(e, idx_buf, idx_bytes);
        if (st != TDC_OK) { v2_free(e, idx_buf); return st; }

        tdc_rowgroup_index idx;
        st = tdc_rowgroup_index_parse(idx_buf, idx_bytes, e->any_stats,
                                      e->realloc_fn, e->alloc_user, &idx);
        v2_free(e, idx_buf);
        if (st != TDC_OK) return st;

        e->groups     = idx.groups;
        e->n_groups   = idx.n_rowgroups;
        e->groups_cap = idx.n_rowgroups;
    }

    /* ---- Position past the existing trailer, and write from there. ---- */
    e->write_pos = hdr.index_offset + hdr.index_size;
    st = e->io.seek_fn(e->io.ctx, (int64_t)e->write_pos, TDC_SEEK_SET);
    if (st != TDC_OK) return st;

    if (out_hdr) *out_hdr = hdr;
    return TDC_OK;
}

tdc_status tdc_stream_encoder_open_widen(
    const tdc_stream_encoder_widen_config *cfg,
    tdc_stream_encoder **enc) {
    if (!cfg || !enc) return TDC_E_INVAL;
    *enc = NULL;

    if (!cfg->io.write_fn || !cfg->io.read_fn || !cfg->io.seek_fn)
        return TDC_E_INVAL;
    if (!cfg->realloc_fn) return TDC_E_INVAL;
    if (!cfg->schema || cfg->schema->n_columns == 0) return TDC_E_INVAL;

    v2_stream_encoder_state *e = v2_new_reopen_state(
        &cfg->io, cfg->realloc_fn, cfg->alloc_user, V2_MODE_WIDEN);
    if (!e) return TDC_E_NOMEM;

    tdc_status st = v2_reopen_existing(e, NULL);
    if (st != TDC_OK) { v2_open_abort(e); return st; }

    /* ---- Serialize the replacement schema now, write it at close. ---- */
    {
        size_t sz = tdc_schema_serialized_size(cfg->schema);
        if (sz == 0 || sz > UINT32_MAX) {
            v2_open_abort(e);
            return TDC_E_INVAL;
        }
        e->schema_buf = (uint8_t *)v2_alloc(e, NULL, sz);
        if (!e->schema_buf) {
            v2_open_abort(e);
            return TDC_E_NOMEM;
        }
        tdc_schema_serialize(cfg->schema, e->schema_buf);
        e->schema_size = (uint32_t)sz;
    }

    *enc = (tdc_stream_encoder *)e;
    return TDC_OK;
}

/* How many columns the container's schema declares. Read at open so every
 * appended row group can be checked against it; the parsed descriptors
 * themselves are not needed, only the count. */
static tdc_status v2_read_schema_n_cols(v2_stream_encoder_state *e,
                                        uint64_t offset, uint32_t size,
                                        uint16_t *out_n_cols) {
    uint8_t *buf = (uint8_t *)v2_alloc(e, NULL, (size_t)size);
    if (!buf) return TDC_E_NOMEM;

    tdc_status st = e->io.seek_fn(e->io.ctx, (int64_t)offset, TDC_SEEK_SET);
    if (st != TDC_OK) { v2_free(e, buf); return st; }

    st = v2_read_exact(e, buf, (size_t)size);
    if (st != TDC_OK) { v2_free(e, buf); return st; }

    uint16_t         n_cols = 0;
    tdc_column_desc *cols   = NULL;
    st = tdc_schema_parse(buf, (size_t)size, e->realloc_fn, e->alloc_user,
                          &n_cols, &cols);
    v2_free(e, buf);
    if (st != TDC_OK) return st;

    tdc_schema_free(cols, n_cols, e->realloc_fn, e->alloc_user);
    *out_n_cols = n_cols;
    return TDC_OK;
}

tdc_status tdc_stream_encoder_open_extend(
    const tdc_stream_encoder_extend_config *cfg,
    tdc_stream_encoder **enc) {
    if (!cfg || !enc) return TDC_E_INVAL;
    *enc = NULL;

    if (!cfg->io.write_fn || !cfg->io.read_fn || !cfg->io.seek_fn)
        return TDC_E_INVAL;
    if (!cfg->realloc_fn) return TDC_E_INVAL;

    v2_stream_encoder_state *e = v2_new_reopen_state(
        &cfg->io, cfg->realloc_fn, cfg->alloc_user, V2_MODE_EXTEND);
    if (!e) return TDC_E_NOMEM;

    tdc_container_header hdr;
    tdc_status st = v2_reopen_existing(e, &hdr);
    if (st != TDC_OK) { v2_open_abort(e); return st; }

    /* Appended row groups carry the columns the container already declares,
     * so there has to be a schema declaring them. Its address is also what
     * the patched header records, and a zero there would read as "not a
     * relocated layout" -- the one thing this container definitely is. */
    if (hdr.schema_size == 0) { v2_open_abort(e); return TDC_E_INVAL; }

    e->schema_size        = hdr.schema_size;
    e->kept_schema_offset = (hdr.version == TDC_CONTAINER_VERSION_WIDENED)
                          ? hdr.u.het.schema_offset
                          : TDC_CONTAINER_HEADER_SIZE;

    st = v2_read_schema_n_cols(e, e->kept_schema_offset, hdr.schema_size,
                               &e->schema_n_cols);
    if (st != TDC_OK)        { v2_open_abort(e); return st; }
    if (e->schema_n_cols == 0) { v2_open_abort(e); return TDC_E_CORRUPT; }

    /* Reading the schema moved the cursor; put it back past the trailer. */
    st = e->io.seek_fn(e->io.ctx, (int64_t)e->write_pos, TDC_SEEK_SET);
    if (st != TDC_OK) { v2_open_abort(e); return st; }

    *enc = (tdc_stream_encoder *)e;
    return TDC_OK;
}

tdc_status tdc_stream_encoder_widen_block(tdc_stream_encoder     *enc,
                                          uint64_t                rg_index,
                                          const tdc_block        *src,
                                          const tdc_codec_spec   *spec,
                                          const tdc_column_stats *stats) {
    if (!enc || !src || !spec) return TDC_E_INVAL;

    v2_stream_encoder_state *e = (v2_stream_encoder_state *)enc;
    if (e->mode != V2_MODE_WIDEN) return TDC_E_INVAL;
    if (rg_index >= e->n_groups) return TDC_E_INVAL;

    v2_rowgroup_entry *rg = &e->groups[rg_index];
    if (rg->entry.n_cols == UINT16_MAX) return TDC_E_INVAL;

    /* Encode first: on failure nothing about the row group has changed. */
    e->scratch.size = 0;
    tdc_status st = tdc_encode_block(src, spec, &e->scratch);
    if (st != TDC_OK) return st;

    uint16_t old_n = rg->entry.n_cols;
    uint16_t new_n = (uint16_t)(old_n + 1);

    /* Grow the column array by one. Exact-size: widening appends a known
     * number of columns, so amortized growth would only waste memory. */
    v2_col_entry *cols = (v2_col_entry *)v2_alloc(
        e, rg->entry.columns, (size_t)new_n * sizeof(v2_col_entry));
    if (!cols) return TDC_E_NOMEM;
    rg->entry.columns = cols;

    /* Grow the stats block in step. A row group that carried no stats gets
     * one allocated with the pre-existing columns marked has_stats=0, so a
     * widened column can contribute stats without inventing them for the
     * columns already on disk. */
    if (stats) {
        tdc_column_stats *sp = (tdc_column_stats *)v2_alloc(
            e, rg->stats, (size_t)new_n * sizeof(tdc_column_stats));
        if (!sp) return TDC_E_NOMEM;
        if (!rg->has_stats) {
            memset(sp, 0, (size_t)old_n * sizeof(tdc_column_stats));
            rg->has_stats = 1;
        }
        sp[old_n] = *stats;
        rg->stats = sp;
        e->any_stats = 1;
    } else if (rg->has_stats) {
        tdc_column_stats *sp = (tdc_column_stats *)v2_alloc(
            e, rg->stats, (size_t)new_n * sizeof(tdc_column_stats));
        if (!sp) return TDC_E_NOMEM;
        memset(&sp[old_n], 0, sizeof(tdc_column_stats));
        rg->stats = sp;
    }

    cols[old_n].block_offset = e->write_pos;
    cols[old_n].block_total  = e->scratch.size;

    st = v2_io_write_exact(&e->io, e->scratch.data, e->scratch.size);
    if (st != TDC_OK) return st;

    rg->entry.n_cols = new_n;
    e->write_pos += e->scratch.size;
    e->n_blocks++;
    return TDC_OK;
}

tdc_status tdc_stream_encoder_abort(tdc_stream_encoder **enc) {
    if (!enc || !*enc) return TDC_OK;

    v2_stream_encoder_state *e = (v2_stream_encoder_state *)*enc;

    /* Same teardown as close, minus every write. Nothing that was already
     * emitted is referenced by the on-disk header, so for a reopened
     * container -- widened or extended -- the file is left exactly as it was
     * found. */
    v2_free(e, e->cur_cols);
    v2_free(e, e->cur_stats);
    v2_free(e, e->schema_buf);
    v2_free_groups(e);
    if (e->scratch.data) {
        e->realloc_fn(e->alloc_user, e->scratch.data, 0);
    }
    e->realloc_fn(e->alloc_user, e, 0);
    *enc = NULL;

    return TDC_OK;
}

uint64_t tdc_stream_encoder_block_count(const tdc_stream_encoder *enc) {
    if (!enc) return 0;
    const v2_stream_encoder_state *e =
        (const v2_stream_encoder_state *)enc;
    return e->n_blocks;
}
