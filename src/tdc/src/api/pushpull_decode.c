/*
 * src/api/pushpull_decode.c
 *
 * Byte-stream push/pull decoder. Accepts record bytes in arbitrary-sized
 * chunks, buffers until a full tdc_block_record is available, then calls
 * tdc_decode_block into a decoder-owned output buffer. next() returns
 * that block to the caller; the buffer is reused across calls.
 *
 * Record-boundary handling:
 *   - `in` holds the bytes of the currently-incomplete record, starting
 *     with its 80-byte header. When enough bytes are present to cover the
 *     full record size (derived from the header), we decode it, hand it
 *     out, and on the next call drop the consumed bytes.
 *   - A push may contain bytes that span a record boundary; we always
 *     accept them wholesale, then process records greedily when next()
 *     is called.
 *   - On corruption we latch into a permanent failure state so subsequent
 *     next() calls keep returning TDC_E_CORRUPT.
 */

#ifdef _MSC_VER
#  define _CRT_SECURE_NO_WARNINGS 1
#endif

#include "tdc/pushpull.h"
#include "tdc/codec.h"
#include "tdc/format.h"
#include "tdc/types.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* Phases of the decoder state machine. */
typedef enum {
    PP_PHASE_HEADER  = 0,  /* need more bytes before a record header is complete */
    PP_PHASE_PARSED  = 1,  /* header parsed; need payload bytes to fill full record */
    PP_PHASE_READY   = 2,  /* a full record is buffered; next() will decode it */
    PP_PHASE_CORRUPT = 3   /* latched failure */
} pp_phase;

struct tdc_pushpull_decoder {
    void *(*realloc_fn)(void *user, void *ptr, size_t new_size);
    void  *alloc_user;

    /* Accumulated record bytes. Always starts at the first byte of the
     * current record in progress. */
    tdc_buffer in;
    size_t     in_head;   /* bytes of `in` already consumed from the front */

    /* Parsed header of the in-progress record (valid in PHASE_PARSED/READY). */
    tdc_block_record cur_hdr;
    size_t           cur_total;  /* full record byte size incl. header */

    /* Decoder-owned output element buffer. Reused across next() calls. */
    tdc_buffer owned_data;

    pp_phase phase;
    int      finished;         /* caller signalled end-of-input */

    uint64_t blocks_pulled;
    uint64_t bytes_produced;   /* raw element bytes handed out across all blocks */
};

/* ----- Small helpers ------------------------------------------------------ */

static tdc_status pp_buf_reserve(tdc_buffer *b, size_t need) {
    if (need <= b->capacity) return TDC_OK;
    size_t new_cap = b->capacity ? b->capacity : 64;
    while (new_cap < need) {
        if (new_cap > (SIZE_MAX / 2)) { new_cap = need; break; }
        new_cap *= 2;
    }
    void *p = b->realloc_fn(b->user, b->data, new_cap);
    if (!p) return TDC_E_NOMEM;
    b->data     = (uint8_t *)p;
    b->capacity = new_cap;
    return TDC_OK;
}

static void pp_buf_free(tdc_buffer *b) {
    if (b->data && b->realloc_fn) {
        b->realloc_fn(b->user, b->data, 0);
    }
    b->data     = NULL;
    b->size     = 0;
    b->capacity = 0;
}

/* Compact `in`: drop the first in_head bytes. Called at record boundaries. */
static void pp_in_compact(tdc_pushpull_decoder *d) {
    if (d->in_head == 0) return;
    if (d->in_head >= d->in.size) {
        d->in.size = 0;
        d->in_head = 0;
        return;
    }
    size_t remaining = d->in.size - d->in_head;
    memmove(d->in.data, d->in.data + d->in_head, remaining);
    d->in.size = remaining;
    d->in_head = 0;
}

/* Try to advance from PHASE_HEADER -> PARSED -> READY using what's in `in`
 * starting at in_head. Returns TDC_OK on success (phase may stay at HEADER
 * if not enough bytes are buffered yet) or TDC_E_CORRUPT if the header is
 * malformed. */
static tdc_status pp_try_parse(tdc_pushpull_decoder *d) {
    size_t avail = (d->in.size > d->in_head) ? (d->in.size - d->in_head) : 0;

    if (d->phase == PP_PHASE_HEADER) {
        if (avail < TDC_BLOCK_HEADER_SIZE) return TDC_OK;

        memcpy(&d->cur_hdr, d->in.data + d->in_head, TDC_BLOCK_HEADER_SIZE);

        tdc_status st = tdc_block_record_validate(&d->cur_hdr);
        if (st != TDC_OK) {
            d->phase = PP_PHASE_CORRUPT;
            return TDC_E_CORRUPT;
        }

        uint64_t total = (uint64_t)TDC_BLOCK_HEADER_SIZE
                       + (uint64_t)d->cur_hdr.side_meta_size
                       + (uint64_t)d->cur_hdr.xform_params_size
                       + (uint64_t)d->cur_hdr.payload_size
                       + (uint64_t)d->cur_hdr.validity_size;
        /* Sanity: total must fit in size_t. */
        if (total > SIZE_MAX) {
            d->phase = PP_PHASE_CORRUPT;
            return TDC_E_CORRUPT;
        }
        d->cur_total = (size_t)total;
        d->phase     = PP_PHASE_PARSED;
        avail        = (d->in.size > d->in_head) ? (d->in.size - d->in_head) : 0;
    }

    if (d->phase == PP_PHASE_PARSED && avail >= d->cur_total) {
        d->phase = PP_PHASE_READY;
    }
    return TDC_OK;
}

/* Decode the ready record into the decoder-owned output buffer and fill
 * *dst. Advances in_head past the record and moves phase back to HEADER. */
static tdc_status pp_decode_ready(tdc_pushpull_decoder *d, tdc_block *dst) {
    /* Number of elements in the block. */
    int64_t n_elems = 1;
    for (uint8_t i = 0; i < d->cur_hdr.rank; ++i) {
        n_elems *= d->cur_hdr.dim[i];
    }
    if (n_elems < 0) {
        d->phase = PP_PHASE_CORRUPT;
        return TDC_E_CORRUPT;
    }

    size_t elem_size = tdc_dtype_size((tdc_dtype)d->cur_hdr.dtype);
    if (elem_size == 0) {
        /* Variable-length dtypes not supported in v1 push/pull. */
        d->phase = PP_PHASE_CORRUPT;
        return TDC_E_UNSUPPORTED;
    }

    size_t data_bytes = (size_t)n_elems * elem_size;
    tdc_status st = pp_buf_reserve(&d->owned_data,
                                   data_bytes > 0 ? data_bytes : 1);
    if (st != TDC_OK) return st;

    /* Build dst descriptor matching the header so tdc_decode_block's
     * dtype/layout/shape cross-checks pass. */
    memset(dst, 0, sizeof(*dst));
    dst->data   = d->owned_data.data;
    dst->dtype  = (tdc_dtype)d->cur_hdr.dtype;
    dst->layout = (tdc_layout)d->cur_hdr.layout;
    dst->shape.rank = d->cur_hdr.rank;
    for (uint8_t i = 0; i < TDC_MAX_RANK; ++i) {
        dst->shape.dim[i] = (i < d->cur_hdr.rank) ? d->cur_hdr.dim[i] : 0;
    }
    tdc_shape_set_contiguous(&dst->shape);

    st = tdc_decode_block(d->in.data + d->in_head, d->cur_total, dst);
    if (st != TDC_OK) {
        /* Every error from decode_block here is effectively a stream-level
         * corruption from the caller's perspective: the byte-stream decoder
         * claimed to have a full valid record and the inner decoder
         * disagreed. Latch the failure. */
        d->phase = PP_PHASE_CORRUPT;
        return (st == TDC_E_CORRUPT) ? TDC_E_CORRUPT : st;
    }

    d->owned_data.size = data_bytes;

    /* Advance past the consumed record. */
    d->in_head += d->cur_total;
    pp_in_compact(d);

    d->phase = PP_PHASE_HEADER;
    d->cur_total = 0;
    d->blocks_pulled++;
    d->bytes_produced += data_bytes;
    return TDC_OK;
}

/* ----- Public API --------------------------------------------------------- */

tdc_status tdc_pushpull_decoder_open(const tdc_pushpull_decoder_config *cfg,
                                     tdc_pushpull_decoder **dec) {
    if (!cfg || !dec) return TDC_E_INVAL;
    *dec = NULL;
    if (!cfg->realloc_fn) return TDC_E_INVAL;

    tdc_pushpull_decoder *d = (tdc_pushpull_decoder *)cfg->realloc_fn(
        cfg->alloc_user, NULL, sizeof(tdc_pushpull_decoder));
    if (!d) return TDC_E_NOMEM;
    memset(d, 0, sizeof(*d));

    d->realloc_fn = cfg->realloc_fn;
    d->alloc_user = cfg->alloc_user;

    d->in.realloc_fn         = cfg->realloc_fn;
    d->in.user               = cfg->alloc_user;
    d->owned_data.realloc_fn = cfg->realloc_fn;
    d->owned_data.user       = cfg->alloc_user;

    d->phase = PP_PHASE_HEADER;

    *dec = d;
    return TDC_OK;
}

tdc_status tdc_pushpull_decoder_reset(tdc_pushpull_decoder *dec) {
    if (!dec) return TDC_E_INVAL;
    dec->in.size         = 0;
    dec->in_head         = 0;
    dec->owned_data.size = 0;
    dec->cur_total       = 0;
    dec->phase           = PP_PHASE_HEADER;
    dec->finished        = 0;
    dec->blocks_pulled   = 0;
    dec->bytes_produced  = 0;
    memset(&dec->cur_hdr, 0, sizeof(dec->cur_hdr));
    return TDC_OK;
}

void tdc_pushpull_decoder_close(tdc_pushpull_decoder **dec) {
    if (!dec || !*dec) return;
    tdc_pushpull_decoder *d = *dec;
    pp_buf_free(&d->in);
    pp_buf_free(&d->owned_data);
    d->realloc_fn(d->alloc_user, d, 0);
    *dec = NULL;
}

tdc_status tdc_pushpull_decoder_push(tdc_pushpull_decoder *dec,
                                     const uint8_t *data, size_t n_bytes,
                                     size_t *consumed) {
    if (consumed) *consumed = 0;
    if (!dec) return TDC_E_INVAL;
    if (dec->phase == PP_PHASE_CORRUPT) return TDC_E_CORRUPT;
    if (dec->finished) return TDC_E_INVAL;
    if (n_bytes == 0) return TDC_OK;
    if (!data) return TDC_E_INVAL;

    /* Pre-compact so we don't blow up capacity when push() straddles
     * record boundaries across many calls. */
    pp_in_compact(dec);

    tdc_status st = pp_buf_reserve(&dec->in, dec->in.size + n_bytes);
    if (st != TDC_OK) return st;

    memcpy(dec->in.data + dec->in.size, data, n_bytes);
    dec->in.size += n_bytes;

    if (consumed) *consumed = n_bytes;
    return TDC_OK;
}

tdc_status tdc_pushpull_decoder_finish(tdc_pushpull_decoder *dec) {
    if (!dec) return TDC_E_INVAL;
    if (dec->phase == PP_PHASE_CORRUPT) return TDC_E_CORRUPT;
    dec->finished = 1;
    return TDC_OK;
}

tdc_status tdc_pushpull_decoder_next(tdc_pushpull_decoder *dec,
                                     tdc_block *dst,
                                     int       *have_block,
                                     int       *done) {
    if (!dec || !have_block || !done) return TDC_E_INVAL;
    *have_block = 0;
    *done       = 0;

    if (dec->phase == PP_PHASE_CORRUPT) return TDC_E_CORRUPT;
    if (!dst) return TDC_E_INVAL;

    /* Attempt to advance the state machine based on buffered bytes. */
    tdc_status st = pp_try_parse(dec);
    if (st != TDC_OK) return st;

    if (dec->phase == PP_PHASE_READY) {
        st = pp_decode_ready(dec, dst);
        if (st != TDC_OK) return st;
        *have_block = 1;
        return TDC_OK;
    }

    /* No complete record buffered. Starved. */
    if (dec->finished) {
        /* Finished and no full record buffered. If there are leftover
         * bytes, that's a truncated record — corruption. */
        size_t avail = (dec->in.size > dec->in_head)
                         ? (dec->in.size - dec->in_head) : 0;
        if (avail > 0) {
            dec->phase = PP_PHASE_CORRUPT;
            return TDC_E_CORRUPT;
        }
        *done = 1;
    }
    return TDC_OK;
}

tdc_status tdc_pushpull_decoder_checkpoint(const tdc_pushpull_decoder *dec,
                                           tdc_pushpull_ckpt *out) {
    if (!dec || !out) return TDC_E_INVAL;
    out->blocks = dec->blocks_pulled;
    out->bytes  = dec->bytes_produced;
    return TDC_OK;
}
