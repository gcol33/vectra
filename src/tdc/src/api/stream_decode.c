/*
 * src/api/stream_decode.c
 *
 * Streaming decoder: schema + row-group index.
 *
 * schema_size == 0 means no schema section; the decoder still works but
 * returns NULL from tdc_stream_decoder_schema() and row-group index
 * queries degrade to sequential-only reads when index_offset is 0.
 *
 * Container layout, version 1 (TDC_CONTAINER_VERSION):
 *
 *   [64-byte header]                (schema_size recorded in header)
 *   [schema_size bytes of schema]   (column descriptors; absent if 0)
 *   [block records ...]             (self-describing)
 *   [trailing row-group index]      (at index_offset, if present)
 *
 * Container layout, version 2 (TDC_CONTAINER_VERSION_WIDENED) — produced
 * only by tdc_stream_encoder_open_widen, which appends columns to an
 * existing container without rewriting its body:
 *
 *   [64-byte header]                (schema_offset / blocks_start set)
 *   [stale v1 schema bytes]         (dead; superseded, kept so the block
 *                                    offsets recorded in the index stay valid)
 *   [original block records ...]
 *   [stale trailing index]          (dead; see below)
 *   [appended column blocks ...]
 *   [widened schema]                (at header.u.het.schema_offset)
 *   [rebuilt trailing row-group index]
 *
 * A widened container therefore has a gap in its blocks region (the stale
 * index the widen pass wrote past rather than over, so that a crash before
 * the header patch leaves the pre-widen container intact). Blocks are
 * located solely by the index, so random access is unaffected; sequential
 * walking is not meaningful and peek_block refuses it explicitly rather
 * than silently truncating at the gap.
 *
 * Schema wire format:
 *   u16 n_columns
 *   per column: u16 name_len, name_len bytes UTF-8, u8 dtype,
 *               u16 ann_len, ann_len bytes UTF-8
 *
 * The row-group index wire format lives in ../format/rowgroup_internal.h;
 * this file parses it through tdc_rowgroup_index_parse and never hand-rolls
 * the layout.
 */

#ifdef _MSC_VER
#  define _CRT_SECURE_NO_WARNINGS 1
#endif

#include "tdc/stream.h"
#include "tdc/codec.h"
#include "tdc/format.h"
#include "tdc/types.h"
#include "../format/stats_internal.h"
#include "../format/rowgroup_internal.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ----- Internal state ---------------------------------------------------- */

struct tdc_stream_decoder {
    tdc_io      io;

    void *(*realloc_fn)(void *user, void *ptr, size_t new_size);
    void   *alloc_user;

    tdc_container_header header;

    /* Parsed schema (owned; NULL if v1 or schema_size == 0). */
    tdc_schema           schema;
    tdc_column_desc     *schema_columns;   /* owned array */
    char                *schema_strings;   /* owned: all names + annotations */
    int                  has_schema;

    /* Row-group index (owned; empty if not seekable or no index). Each
     * group carries its own optional stats block, so there is no parallel
     * stats array to keep in step. */
    tdc_rowgroup_index   rg;
    int                  has_rg_index;

    /* File offset where blocks begin (after header + schema). */
    uint64_t    blocks_start;

    /* Sequential reading state. */
    uint64_t    read_pos;
    uint64_t    blocks_read;

    /* Peek state. */
    int                  peeked;
    tdc_block_record     peeked_record;
    uint64_t             peeked_remaining;

    /* Scratch buffer for assembling full block records. */
    tdc_buffer  scratch;
};

/* ----- I/O helpers ------------------------------------------------------- */

static tdc_status v2_io_read_exact(const tdc_io *io, void *buf, size_t size) {
    size_t total = 0;
    while (total < size) {
        size_t got = 0;
        tdc_status st = io->read_fn(io->ctx,
                                     (uint8_t *)buf + total,
                                     size - total, &got);
        if (st != TDC_OK) return TDC_E_IO;
        if (got == 0) return TDC_E_CORRUPT;
        total += got;
    }
    return TDC_OK;
}

static tdc_status v2_io_read_maybe(const tdc_io *io, void *buf, size_t size,
                                   size_t *actually_read) {
    size_t total = 0;
    while (total < size) {
        size_t got = 0;
        tdc_status st = io->read_fn(io->ctx,
                                     (uint8_t *)buf + total,
                                     size - total, &got);
        if (st != TDC_OK) return TDC_E_IO;
        if (got == 0) {
            if (total == 0) { *actually_read = 0; return TDC_OK; }
            return TDC_E_CORRUPT;
        }
        total += got;
    }
    *actually_read = total;
    return TDC_OK;
}

static tdc_status v2_io_skip_bytes(const tdc_io *io, size_t n) {
    uint8_t tmp[4096];
    while (n > 0) {
        size_t chunk = n < sizeof(tmp) ? n : sizeof(tmp);
        tdc_status st = v2_io_read_exact(io, tmp, chunk);
        if (st != TDC_OK) return st;
        n -= chunk;
    }
    return TDC_OK;
}

/* ----- Allocator helpers ------------------------------------------------- */

static void *v2_alloc(struct tdc_stream_decoder *d, void *ptr, size_t sz) {
    return d->realloc_fn(d->alloc_user, ptr, sz);
}

static void v2_free(struct tdc_stream_decoder *d, void *ptr) {
    if (ptr) d->realloc_fn(d->alloc_user, ptr, 0);
}

/* ----- Little-endian read helpers ---------------------------------------- */
/*
 * The tdc on-disk format is fixed little-endian. On supported targets
 * (x86_64, aarch64 — both LE), memcpy is a no-op at the optimizer level
 * and is strictly-aliasing safe. No byte-swap needed.
 */

static uint16_t read_u16_le(const uint8_t *p) {
    uint16_t v;
    memcpy(&v, p, 2);
    return v;
}

/* The row-group index is parsed by ../format/rowgroup.c, so the only
 * width this file reads directly is the u16 the schema section uses. */

/* Peek at the block sitting at the current stream position, bypassing the
 * public entry point's sequential-walk guard. Defined below; used by
 * seek_rowgroup, which positions the stream through the index. */
static tdc_status peek_block_here(tdc_stream_decoder *dec,
                                  tdc_block_record   *rec);

/* ----- Schema parsing ---------------------------------------------------- */

/*
 * Parse the schema section from a raw byte buffer.
 *
 * Wire format:
 *   u16 n_columns
 *   per column: u16 name_len, name_len bytes, u8 dtype, u16 ann_len, ann_len bytes
 *
 * We make two passes:
 *   1. Validate and compute total string bytes needed (names + annotations
 *      + null terminators).
 *   2. Allocate and fill the column descriptor array + string buffer.
 *
 * All allocations go through d->realloc_fn. On error, nothing is allocated.
 */
static tdc_status parse_schema(struct tdc_stream_decoder *d,
                               const uint8_t *buf, size_t buf_size) {
    if (buf_size < 2) return TDC_E_CORRUPT;

    uint16_t n_cols = read_u16_le(buf);
    if (n_cols == 0) return TDC_OK; /* valid: zero-column schema */

    /* Pass 1: validate and sum string sizes. */
    size_t pos = 2;
    size_t total_str_bytes = 0;

    for (uint16_t i = 0; i < n_cols; i++) {
        /* name_len (2) + at least dtype (1) + ann_len (2) */
        if (pos + 2 > buf_size) return TDC_E_CORRUPT;
        uint16_t name_len = read_u16_le(buf + pos);
        pos += 2;

        if (pos + (size_t)name_len > buf_size) return TDC_E_CORRUPT;
        pos += name_len;
        total_str_bytes += (size_t)name_len + 1; /* +1 for null terminator */

        /* dtype */
        if (pos + 1 > buf_size) return TDC_E_CORRUPT;
        pos += 1;

        /* ann_len */
        if (pos + 2 > buf_size) return TDC_E_CORRUPT;
        uint16_t ann_len = read_u16_le(buf + pos);
        pos += 2;

        if (pos + (size_t)ann_len > buf_size) return TDC_E_CORRUPT;
        pos += ann_len;
        total_str_bytes += (size_t)ann_len + 1;
    }

    /* Allocate column descriptors. */
    tdc_column_desc *cols = (tdc_column_desc *)v2_alloc(
        d, NULL, (size_t)n_cols * sizeof(tdc_column_desc));
    if (!cols) return TDC_E_NOMEM;

    /* Allocate a single string buffer for all names + annotations. */
    char *strbuf = (char *)v2_alloc(d, NULL, total_str_bytes);
    if (!strbuf) {
        v2_free(d, cols);
        return TDC_E_NOMEM;
    }

    /* Pass 2: fill descriptors. */
    pos = 2;
    size_t str_pos = 0;

    for (uint16_t i = 0; i < n_cols; i++) {
        uint16_t name_len = read_u16_le(buf + pos);
        pos += 2;

        cols[i].name = strbuf + str_pos;
        cols[i].name_len = name_len;
        memcpy(strbuf + str_pos, buf + pos, name_len);
        str_pos += name_len;
        strbuf[str_pos++] = '\0';
        pos += name_len;

        cols[i].dtype = buf[pos];
        pos += 1;

        uint16_t ann_len = read_u16_le(buf + pos);
        pos += 2;

        cols[i].annotation = strbuf + str_pos;
        cols[i].ann_len = ann_len;
        if (ann_len > 0) {
            memcpy(strbuf + str_pos, buf + pos, ann_len);
        }
        str_pos += ann_len;
        strbuf[str_pos++] = '\0';
        pos += ann_len;
    }

    d->schema_columns  = cols;
    d->schema_strings  = strbuf;
    d->schema.n_columns = n_cols;
    d->schema.columns   = cols;
    d->has_schema       = 1;

    return TDC_OK;
}

/* ----- Free helpers for owned sub-allocations ---------------------------- */

static void free_schema(struct tdc_stream_decoder *d) {
    v2_free(d, d->schema_columns);
    v2_free(d, d->schema_strings);
    d->schema_columns = NULL;
    d->schema_strings = NULL;
    d->schema.columns = NULL;
    d->schema.n_columns = 0;
    d->has_schema = 0;
}

static void free_rg_index(struct tdc_stream_decoder *d) {
    tdc_rowgroup_index_free(&d->rg, d->realloc_fn, d->alloc_user);
    d->has_rg_index = 0;
}

/* ----- Public API -------------------------------------------------------- */

tdc_status tdc_stream_decoder_open(const tdc_stream_decoder_config *cfg,
                                      tdc_stream_decoder **dec) {
    if (!cfg || !dec) return TDC_E_INVAL;
    *dec = NULL;

    if (!cfg->io.read_fn) return TDC_E_INVAL;
    if (!cfg->realloc_fn) return TDC_E_INVAL;

    tdc_stream_decoder *d = (tdc_stream_decoder *)cfg->realloc_fn(
        cfg->alloc_user, NULL, sizeof(tdc_stream_decoder));
    if (!d) return TDC_E_NOMEM;
    memset(d, 0, sizeof(*d));

    d->io         = cfg->io;
    d->realloc_fn = cfg->realloc_fn;
    d->alloc_user = cfg->alloc_user;

    d->scratch.data       = NULL;
    d->scratch.size       = 0;
    d->scratch.capacity   = 0;
    d->scratch.realloc_fn = cfg->realloc_fn;
    d->scratch.user       = cfg->alloc_user;

    /* ---- Step 1: Read and validate the 64-byte container header. ---- */

    tdc_status st = v2_io_read_exact(&d->io, &d->header,
                                     TDC_CONTAINER_HEADER_SIZE);
    if (st != TDC_OK) { v2_free(d, d); return st; }

    if (d->header.magic != TDC_CONTAINER_MAGIC) {
        v2_free(d, d);
        return TDC_E_CORRUPT;
    }
    if (d->header.version != TDC_CONTAINER_VERSION &&
        d->header.version != TDC_CONTAINER_VERSION_WIDENED) {
        v2_free(d, d);
        return TDC_E_VERSION;
    }

    /* A widened container relocates its schema to the tail and records
     * where the blocks region starts, because the original schema slot at
     * offset 64 cannot grow in place. Reading it needs a seek, so a widened
     * container cannot be opened forward-only. */
    const int widened =
        (d->header.version == TDC_CONTAINER_VERSION_WIDENED);
    if (widened) {
        if (!cfg->io.seek_fn) { v2_free(d, d); return TDC_E_INVAL; }
        if (d->header.u.het.schema_offset == 0 ||
            d->header.u.het.blocks_start  == 0) {
            v2_free(d, d);
            return TDC_E_CORRUPT;
        }
    }

    uint64_t pos = widened ? d->header.u.het.blocks_start
                           : TDC_CONTAINER_HEADER_SIZE;

    /* ---- Step 2: Parse schema section if schema_size > 0. ---- */

    uint32_t schema_size = d->header.schema_size;

    if (schema_size > 0) {
        uint8_t *schema_buf = (uint8_t *)v2_alloc(d, NULL, schema_size);
        if (!schema_buf) {
            v2_free(d, d);
            return TDC_E_NOMEM;
        }

        /* v1: the schema follows the header. v2: it lives at the recorded
         * offset, so seek to it and leave the stream at blocks_start. */
        if (widened) {
            st = cfg->io.seek_fn(cfg->io.ctx,
                                 (int64_t)d->header.u.het.schema_offset,
                                 TDC_SEEK_SET);
            if (st != TDC_OK) {
                v2_free(d, schema_buf);
                v2_free(d, d);
                return st;
            }
        }

        st = v2_io_read_exact(&d->io, schema_buf, schema_size);
        if (st != TDC_OK) {
            v2_free(d, schema_buf);
            v2_free(d, d);
            return st;
        }

        st = parse_schema(d, schema_buf, schema_size);
        v2_free(d, schema_buf); /* raw buffer no longer needed */
        if (st != TDC_OK) {
            v2_free(d, d);
            return st;
        }

        /* v2 already positioned `pos` at blocks_start; only v1 grows it by
         * the schema it just consumed. */
        if (!widened) pos += schema_size;
    }

    d->blocks_start = pos;
    d->read_pos     = pos;

    /* ---- Step 3: Load row-group index if seekable + present. ---- */

    if (cfg->io.seek_fn &&
        d->header.index_offset > 0 && d->header.index_size > 0) {

        size_t idx_bytes = (size_t)d->header.index_size;
        uint8_t *idx_buf = (uint8_t *)v2_alloc(d, NULL, idx_bytes);
        if (!idx_buf) {
            free_schema(d);
            v2_free(d, d);
            return TDC_E_NOMEM;
        }

        st = cfg->io.seek_fn(cfg->io.ctx,
                              (int64_t)d->header.index_offset,
                              TDC_SEEK_SET);
        if (st != TDC_OK) {
            v2_free(d, idx_buf);
            free_schema(d);
            v2_free(d, d);
            return st;
        }

        st = v2_io_read_exact(&d->io, idx_buf, idx_bytes);
        if (st != TDC_OK) {
            v2_free(d, idx_buf);
            free_schema(d);
            v2_free(d, d);
            return st;
        }

        st = tdc_rowgroup_index_parse(
            idx_buf, idx_bytes,
            (d->header.flags & TDC_CONTAINER_FLAG_HAS_STATS) != 0,
            d->realloc_fn, d->alloc_user, &d->rg);
        v2_free(d, idx_buf);
        if (st != TDC_OK) {
            free_schema(d);
            v2_free(d, d);
            return st;
        }
        d->has_rg_index = (d->rg.n_rowgroups > 0);

        /* Seek back to first block position. */
        st = cfg->io.seek_fn(cfg->io.ctx,
                              (int64_t)d->blocks_start,
                              TDC_SEEK_SET);
        if (st != TDC_OK) {
            free_rg_index(d);
            free_schema(d);
            v2_free(d, d);
            return st;
        }
        d->read_pos = d->blocks_start;
    }

    *dec = d;
    return TDC_OK;
}

const tdc_container_header *tdc_stream_decoder_header(
    const tdc_stream_decoder *dec) {
    return dec ? &dec->header : NULL;
}

const tdc_schema *tdc_stream_decoder_read_schema(
    const tdc_stream_decoder *dec) {
    if (!dec || !dec->has_schema) return NULL;
    return &dec->schema;
}

int tdc_stream_decoder_has_rowgroup_index(
    const tdc_stream_decoder *dec) {
    return dec ? dec->has_rg_index : 0;
}

uint64_t tdc_stream_decoder_rowgroup_count(
    const tdc_stream_decoder *dec) {
    if (!dec) return 0;
    return dec->has_rg_index ? dec->rg.n_rowgroups : 0;
}

const tdc_rowgroup_entry *tdc_stream_decoder_get_rowgroup(
    const tdc_stream_decoder *dec, uint64_t rg_index) {
    if (!dec || !dec->has_rg_index) return NULL;
    if (rg_index >= dec->rg.n_rowgroups) return NULL;
    return &dec->rg.groups[rg_index].entry;
}

const tdc_column_stats *tdc_stream_decoder_get_stats(
    const tdc_stream_decoder *dec,
    uint64_t                  rg_index,
    uint16_t                  col_index) {
    if (!dec || !dec->has_rg_index)      return NULL;
    if (rg_index >= dec->rg.n_rowgroups) return NULL;

    const tdc_rg_group *g = &dec->rg.groups[rg_index];
    if (!g->has_stats) return NULL;
    if (col_index >= g->entry.n_cols) return NULL;
    return &g->stats[col_index];
}

tdc_status tdc_stream_decoder_seek_rowgroup(tdc_stream_decoder *dec,
                                                uint64_t rg_index,
                                                uint16_t col_index,
                                                tdc_block_record *rec) {
    if (!dec || !rec) return TDC_E_INVAL;
    if (!dec->has_rg_index) return TDC_E_INVAL;
    if (!dec->io.seek_fn) return TDC_E_INVAL;
    if (rg_index >= dec->rg.n_rowgroups) return TDC_E_INVAL;

    const tdc_rowgroup_entry *rg = &dec->rg.groups[rg_index].entry;
    if (col_index >= rg->n_cols) return TDC_E_INVAL;

    /* Cancel any outstanding peek and reset the sequential block
     * counter — seek_rowgroup repositions the stream, so the
     * blocks_read guard in peek_block must not prevent reading. */
    dec->peeked      = 0;
    dec->blocks_read = 0;

    /* Seek to the column's block offset. */
    uint64_t target = rg->columns[col_index].block_offset;
    tdc_status st = dec->io.seek_fn(dec->io.ctx,
                                     (int64_t)target,
                                     TDC_SEEK_SET);
    if (st != TDC_OK) return st;

    dec->read_pos = target;

    /* Peek the block header at the new position. Goes straight to the
     * shared helper: the position came from the index, so the sequential
     * guard in the public peek does not apply. */
    return peek_block_here(dec, rec);
}

/* ----- Block-level operations (same protocol as v1) ---------------------- */

/* Peek at wherever the stream currently sits. Shared by the public
 * sequential peek and by seek_rowgroup, which has already positioned the
 * stream on a block located through the index. */
static tdc_status peek_block_here(tdc_stream_decoder *dec,
                                  tdc_block_record   *rec) {
    if (!dec || !rec) return TDC_E_INVAL;
    if (dec->peeked) return TDC_E_INVAL;

    /* Check if we've consumed all blocks (known count). */
    if (dec->header.n_blocks > 0 &&
        dec->blocks_read >= dec->header.n_blocks) {
        memset(rec, 0, sizeof(*rec));
        return TDC_OK;
    }

    /* Read the 4-byte magic to detect end-of-blocks. */
    uint32_t magic = 0;
    size_t got = 0;
    tdc_status st = v2_io_read_maybe(&dec->io, &magic, sizeof(magic), &got);
    if (st != TDC_OK) return st;

    if (got == 0) {
        memset(rec, 0, sizeof(*rec));
        return TDC_OK;
    }

    if (magic != TDC_BLOCK_MAGIC) {
        memset(rec, 0, sizeof(*rec));
        return TDC_OK;
    }

    /* Magic matched — read the remaining 76 bytes. */
    tdc_block_record hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic = magic;
    st = v2_io_read_exact(&dec->io,
                           (uint8_t *)&hdr + sizeof(magic),
                           TDC_BLOCK_HEADER_SIZE - sizeof(magic));
    if (st != TDC_OK) return st;

    st = tdc_block_record_validate(&hdr);
    if (st != TDC_OK) return st;

    uint64_t remaining = (uint64_t)hdr.side_meta_size
                       + (uint64_t)hdr.xform_params_size
                       + (uint64_t)hdr.payload_size
                       + (uint64_t)hdr.validity_size;

    dec->peeked           = 1;
    dec->peeked_record    = hdr;
    dec->peeked_remaining = remaining;
    dec->read_pos        += TDC_BLOCK_HEADER_SIZE;

    *rec = hdr;
    return TDC_OK;
}

tdc_status tdc_stream_decoder_peek_block(tdc_stream_decoder *dec,
                                         tdc_block_record   *rec) {
    if (!dec || !rec) return TDC_E_INVAL;

    /* A widened container's blocks region is not contiguous: the widen
     * pass appended the new column blocks past the stale trailing index
     * rather than over it, so a forward walk would hit those dead bytes,
     * read a magic mismatch, and report a clean end-of-blocks partway
     * through. Refuse the walk instead of silently truncating it -- every
     * block is still reachable through seek_rowgroup. */
    if (dec->header.version == TDC_CONTAINER_VERSION_WIDENED)
        return TDC_E_INVAL;

    return peek_block_here(dec, rec);
}

tdc_status tdc_stream_decoder_read_block(tdc_stream_decoder *dec,
                                            tdc_block             *dst) {
    if (!dec || !dst) return TDC_E_INVAL;
    if (!dec->peeked) return TDC_E_INVAL;

    size_t remaining = (size_t)dec->peeked_remaining;
    size_t total     = TDC_BLOCK_HEADER_SIZE + remaining;

    /* Grow scratch to hold the full block record. */
    size_t need = total > 0 ? total : 1;
    if (dec->scratch.capacity < need) {
        size_t new_cap = dec->scratch.capacity ? dec->scratch.capacity : 64;
        while (new_cap < need) new_cap *= 2;
        void *p = v2_alloc(dec, dec->scratch.data, new_cap);
        if (!p) return TDC_E_NOMEM;
        dec->scratch.data     = (uint8_t *)p;
        dec->scratch.capacity = new_cap;
    }

    /* Copy the already-read 80-byte header into scratch. */
    memcpy(dec->scratch.data, &dec->peeked_record, TDC_BLOCK_HEADER_SIZE);

    /* Read the remaining sections. */
    if (remaining > 0) {
        tdc_status st = v2_io_read_exact(&dec->io,
                                          dec->scratch.data + TDC_BLOCK_HEADER_SIZE,
                                          remaining);
        if (st != TDC_OK) return st;
    }

    dec->read_pos += remaining;
    dec->peeked    = 0;
    dec->blocks_read++;

    return tdc_decode_block(dec->scratch.data, total, dst);
}

tdc_status tdc_stream_decoder_skip_block(tdc_stream_decoder *dec) {
    if (!dec)         return TDC_E_INVAL;
    if (!dec->peeked) return TDC_E_INVAL;

    size_t remaining = (size_t)dec->peeked_remaining;

    tdc_status st;
    if (dec->io.seek_fn && remaining > 0) {
        st = dec->io.seek_fn(dec->io.ctx, (int64_t)remaining, TDC_SEEK_CUR);
    } else if (remaining > 0) {
        st = v2_io_skip_bytes(&dec->io, remaining);
    } else {
        st = TDC_OK;
    }

    dec->read_pos += remaining;
    dec->peeked    = 0;
    dec->blocks_read++;

    return st;
}

uint64_t tdc_stream_decoder_block_count(
    const tdc_stream_decoder *dec) {
    if (!dec) return 0;
    if (dec->header.n_blocks > 0) return dec->header.n_blocks;
    return dec->blocks_read;
}

tdc_status tdc_stream_decoder_close(tdc_stream_decoder **dec) {
    if (!dec || !*dec) return TDC_OK;

    tdc_stream_decoder *d = *dec;

    free_rg_index(d);
    free_schema(d);

    if (d->scratch.data) {
        d->realloc_fn(d->alloc_user, d->scratch.data, 0);
    }

    d->realloc_fn(d->alloc_user, d, 0);

    *dec = NULL;
    return TDC_OK;
}
