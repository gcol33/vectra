/*
 * src/api/pushpull_encode.c
 *
 * Byte-stream push/pull encoder. Buffers pushed element bytes until the
 * configured target is reached, then encodes via tdc_encode_block and
 * appends the resulting self-describing record bytes to an output queue.
 *
 * Buffering strategy:
 *   - `in` (tdc_buffer): pending raw element bytes, growable up to
 *     target_block_bytes. Always a whole multiple of elem_size. When
 *     it reaches target_block_bytes, emit_block() is triggered.
 *   - `out` (tdc_buffer): a growing queue of already-emitted record
 *     bytes. Pull() memcpies out of it and left-shifts the remainder.
 *     Simple single-buffer design — cheap, no ring indices, and the
 *     only copy cost is the pull-side trim. For the typical consumer
 *     (drain to a file/socket quickly) the trim is a no-op because
 *     `out` is empty after each pull.
 *   - `scratch` (tdc_buffer): reused across blocks as the output of
 *     tdc_encode_block.
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

/* ----- State -------------------------------------------------------------- */

struct tdc_pushpull_encoder {
    /* Config (copied at open). */
    tdc_dtype      dtype;
    tdc_layout     layout;
    tdc_shape      shape_template;
    tdc_codec_spec spec;
    uint64_t       target_block_elems;
    size_t         elem_size;
    size_t         target_block_bytes;

    void *(*realloc_fn)(void *user, void *ptr, size_t new_size);
    void  *alloc_user;

    /* Buffers. */
    tdc_buffer in;        /* pending raw elements, size multiple of elem_size */
    tdc_buffer out;       /* queued emitted record bytes */
    size_t     out_head;  /* bytes already consumed by pull from the start of out */
    tdc_buffer scratch;   /* reusable output buffer for tdc_encode_block */

    /* Counters. */
    uint64_t blocks_emitted;
    uint64_t bytes_consumed;  /* raw element bytes absorbed by push */

    /* Lifecycle. */
    int finished;   /* finish() has been called */
};

/* ----- Small helpers ------------------------------------------------------ */

static void *pp_alloc(tdc_pushpull_encoder *e, void *p, size_t sz) {
    return e->realloc_fn(e->alloc_user, p, sz);
}

static void pp_free(tdc_pushpull_encoder *e, void *p) {
    if (p) e->realloc_fn(e->alloc_user, p, 0);
}

/* Grow a tdc_buffer so `capacity` >= need. Caller owns the buffer. */
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

/* Trim `out_head` bytes off the front of e->out. Called after a pull so the
 * queue doesn't grow unboundedly for callers that drain eagerly. */
static void pp_out_compact(tdc_pushpull_encoder *e) {
    if (e->out_head == 0) return;
    if (e->out_head >= e->out.size) {
        /* Fully drained — cheap reset, keep the capacity. */
        e->out.size = 0;
        e->out_head = 0;
        return;
    }
    size_t remaining = e->out.size - e->out_head;
    memmove(e->out.data, e->out.data + e->out_head, remaining);
    e->out.size = remaining;
    e->out_head = 0;
}

/* Build a tdc_block descriptor over the current `in` buffer, using at most
 * `n_elems` elements from the front. */
static tdc_block pp_make_block(const tdc_pushpull_encoder *e, int64_t n_elems) {
    tdc_block blk;
    memset(&blk, 0, sizeof(blk));
    blk.data    = e->in.data;
    blk.dtype   = e->dtype;
    blk.layout  = e->layout;
    blk.shape.rank   = 1;
    blk.shape.dim[0] = n_elems;
    blk.shape.dim[1] = 0;
    blk.shape.dim[2] = 0;
    tdc_shape_set_contiguous(&blk.shape);
    (void)e->shape_template; /* reserved for future multi-dim streams */
    return blk;
}

/* Encode the first `n_elems` of e->in, append the record to e->out, and
 * left-shift the remaining input bytes. */
static tdc_status pp_emit_block(tdc_pushpull_encoder *e, int64_t n_elems) {
    if (n_elems <= 0) return TDC_OK;

    size_t consumed_bytes = (size_t)n_elems * e->elem_size;
    if (consumed_bytes > e->in.size) return TDC_E_INVAL;

    tdc_block blk = pp_make_block(e, n_elems);

    /* Encode directly into scratch. tdc_encode_block appends to scratch,
     * so reset its logical size first. */
    e->scratch.size = 0;
    tdc_status st = tdc_encode_block(&blk, &e->spec, &e->scratch);
    if (st != TDC_OK) return st;

    /* Append scratch to out. */
    size_t new_out_size = e->out.size + e->scratch.size;
    st = pp_buf_reserve(&e->out, new_out_size);
    if (st != TDC_OK) return st;

    memcpy(e->out.data + e->out.size, e->scratch.data, e->scratch.size);
    e->out.size = new_out_size;

    /* Left-shift remaining input bytes. */
    size_t remaining = e->in.size - consumed_bytes;
    if (remaining > 0) {
        memmove(e->in.data, e->in.data + consumed_bytes, remaining);
    }
    e->in.size = remaining;

    e->blocks_emitted++;
    return TDC_OK;
}

/* ----- Public API --------------------------------------------------------- */

tdc_status tdc_pushpull_encoder_open(const tdc_pushpull_encoder_config *cfg,
                                     tdc_pushpull_encoder **enc) {
    if (!cfg || !enc) return TDC_E_INVAL;
    *enc = NULL;
    if (!cfg->realloc_fn) return TDC_E_INVAL;

    /* v1: only VECTOR_1D + fixed-width numeric dtypes. */
    if (cfg->layout != TDC_LAYOUT_VECTOR_1D) return TDC_E_LAYOUT;
    size_t es = tdc_dtype_size(cfg->dtype);
    if (es == 0) return TDC_E_DTYPE;

    tdc_pushpull_encoder *e = (tdc_pushpull_encoder *)cfg->realloc_fn(
        cfg->alloc_user, NULL, sizeof(tdc_pushpull_encoder));
    if (!e) return TDC_E_NOMEM;
    memset(e, 0, sizeof(*e));

    e->dtype           = cfg->dtype;
    e->layout          = cfg->layout;
    e->shape_template  = cfg->shape_template;
    e->spec            = cfg->spec;
    e->realloc_fn      = cfg->realloc_fn;
    e->alloc_user      = cfg->alloc_user;
    e->elem_size       = es;

    uint64_t target_elems = cfg->target_block_elems;
    if (target_elems == 0) target_elems = TDC_PUSHPULL_DEFAULT_BLOCK_ELEMS;
    e->target_block_elems = target_elems;
    e->target_block_bytes = (size_t)target_elems * es;

    e->in.realloc_fn      = cfg->realloc_fn;
    e->in.user            = cfg->alloc_user;
    e->out.realloc_fn     = cfg->realloc_fn;
    e->out.user           = cfg->alloc_user;
    e->scratch.realloc_fn = cfg->realloc_fn;
    e->scratch.user       = cfg->alloc_user;

    *enc = e;
    return TDC_OK;
}

tdc_status tdc_pushpull_encoder_reset(tdc_pushpull_encoder *enc) {
    if (!enc) return TDC_E_INVAL;
    enc->in.size        = 0;
    enc->out.size       = 0;
    enc->out_head       = 0;
    enc->scratch.size   = 0;
    enc->blocks_emitted = 0;
    enc->bytes_consumed = 0;
    enc->finished       = 0;
    return TDC_OK;
}

void tdc_pushpull_encoder_close(tdc_pushpull_encoder **enc) {
    if (!enc || !*enc) return;
    tdc_pushpull_encoder *e = *enc;
    pp_buf_free(&e->in);
    pp_buf_free(&e->out);
    pp_buf_free(&e->scratch);
    pp_free(e, e);
    *enc = NULL;
}

tdc_status tdc_pushpull_encoder_push(tdc_pushpull_encoder *enc,
                                     const void *data, size_t n_bytes) {
    if (!enc) return TDC_E_INVAL;
    if (enc->finished) return TDC_E_INVAL;
    if (n_bytes == 0) return TDC_OK;
    if (!data) return TDC_E_INVAL;
    if ((n_bytes % enc->elem_size) != 0) return TDC_E_INVAL;

    const uint8_t *src = (const uint8_t *)data;
    size_t remaining = n_bytes;

    /* Absorb into in-buffer, emitting blocks whenever we hit the target. */
    while (remaining > 0) {
        size_t room = enc->target_block_bytes - enc->in.size;
        size_t take = remaining < room ? remaining : room;

        tdc_status st = pp_buf_reserve(&enc->in, enc->in.size + take);
        if (st != TDC_OK) return st;

        memcpy(enc->in.data + enc->in.size, src, take);
        enc->in.size      += take;
        enc->bytes_consumed += take;
        src                += take;
        remaining          -= take;

        if (enc->in.size >= enc->target_block_bytes) {
            /* Emit exactly target_block_elems. */
            st = pp_emit_block(enc, (int64_t)enc->target_block_elems);
            if (st != TDC_OK) return st;
        }
    }
    return TDC_OK;
}

tdc_status tdc_pushpull_encoder_pull(tdc_pushpull_encoder *enc,
                                     uint8_t *out, size_t cap,
                                     size_t *out_written,
                                     int    *more_available) {
    if (!enc) return TDC_E_INVAL;
    if (!out_written || !more_available) return TDC_E_INVAL;
    *out_written    = 0;
    *more_available = 0;

    size_t queued = (enc->out.size > enc->out_head)
                      ? (enc->out.size - enc->out_head) : 0;

    if (cap == 0) {
        *more_available = queued > 0 ? 1 : 0;
        return TDC_OK;
    }
    if (!out) return TDC_E_INVAL;

    size_t take = queued < cap ? queued : cap;
    if (take > 0) {
        memcpy(out, enc->out.data + enc->out_head, take);
        enc->out_head += take;
        *out_written   = take;
    }

    /* Compact every time the head gets large enough that remaining is
     * smaller than the head — cheap amortized linear work. */
    if (enc->out_head > 0 &&
        (enc->out_head * 2 >= enc->out.size || enc->out_head >= enc->out.size)) {
        pp_out_compact(enc);
    }

    size_t after = (enc->out.size > enc->out_head)
                     ? (enc->out.size - enc->out_head) : 0;
    *more_available = after > 0 ? 1 : 0;
    return TDC_OK;
}

tdc_status tdc_pushpull_encoder_flush(tdc_pushpull_encoder *enc) {
    if (!enc) return TDC_E_INVAL;
    if (enc->in.size == 0) return TDC_OK;
    int64_t n = (int64_t)(enc->in.size / enc->elem_size);
    return pp_emit_block(enc, n);
}

tdc_status tdc_pushpull_encoder_finish(tdc_pushpull_encoder *enc) {
    if (!enc) return TDC_E_INVAL;
    tdc_status st = tdc_pushpull_encoder_flush(enc);
    if (st != TDC_OK) return st;
    enc->finished = 1;
    return TDC_OK;
}

tdc_status tdc_pushpull_encoder_checkpoint(const tdc_pushpull_encoder *enc,
                                           tdc_pushpull_ckpt *out) {
    if (!enc || !out) return TDC_E_INVAL;
    out->blocks = enc->blocks_emitted;
    out->bytes  = enc->bytes_consumed - enc->in.size;
    return TDC_OK;
}
