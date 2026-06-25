/*
 * tdc/pushpull.h — byte-stream push/pull compression API
 *
 * Byte-stream encoder/decoder that sits above tdc_encode_block /
 * tdc_decode_block. The caller pushes arbitrary-sized raw element buffers;
 * the library buffers internally and emits self-describing block records
 * whenever a configurable element count is reached. The decoder accepts
 * arbitrary-sized chunks of record bytes and hands back fully decoded
 * tdc_blocks one at a time.
 *
 * This API COEXISTS with tdc/stream.h — it does not replace it.
 *   - stream.h    : caller assembles a full tdc_block per call.
 *   - pushpull.h  : caller pushes raw bytes; library splits into blocks.
 *
 * Both use tdc_encode_block / tdc_decode_block under the hood.
 *
 * Design decisions (frozen):
 *   - Block-split policy is caller-controlled via target_block_elems.
 *   - Decode corruption aborts the whole stream with TDC_E_CORRUPT.
 *   - Decode is pull-style: next_block() returns a *have_block out-param
 *     (0 when starved, 1 when a block is ready), plus *done at stream end.
 *     Note: TDC_E_AGAIN is not part of the frozen status enum, so this API
 *     signals "starved" in-band via have_block rather than a dedicated
 *     status code.
 *   - Decoder owns the tdc_block.data buffer handed out by next_block.
 *     It is reused across calls; valid only until the next push/next call.
 *   - Checkpoint is a 16-byte POD cookie emitted at block boundaries.
 *   - No mid-stream validity bitmap in v1 (can be added at flush if needed).
 */

#ifndef TDC_PUSHPULL_H
#define TDC_PUSHPULL_H

#include <stdint.h>
#include <stddef.h>

#include "types.h"
#include "codec.h"

#ifdef __cplusplus
extern "C" {
#endif

/** @brief Default target block size in elements when cfg.target_block_elems is 0.
 *
 *  Picked so that a 1-byte element block is ~1 MiB; a 4-byte element block
 *  is ~4 MiB. Small enough for good streaming latency, large enough that
 *  per-block framing overhead is negligible and the LZ matcher has room
 *  to find long matches. */
#define TDC_PUSHPULL_DEFAULT_BLOCK_ELEMS (1u << 20)

typedef struct tdc_pushpull_encoder tdc_pushpull_encoder;
typedef struct tdc_pushpull_decoder tdc_pushpull_decoder;

/**
 * @brief Opaque 16-byte checkpoint cookie.
 *
 * Emitted only at block boundaries. Symmetric across encode/decode: a
 * checkpoint returned by tdc_pushpull_encoder_checkpoint after the Nth
 * block has been emitted matches the value returned by
 * tdc_pushpull_decoder_checkpoint after the Nth block has been pulled.
 */
typedef struct {
    uint64_t blocks;      /**< blocks emitted/pulled so far */
    uint64_t bytes;       /**< raw element bytes consumed/produced so far */
} tdc_pushpull_ckpt;

/* ----- Encoder ----------------------------------------------------------- */

/**
 * @brief Encoder configuration.
 *
 * All fields are consumed by value at open(); the caller may free any
 * referenced params after open returns.
 */
typedef struct {
    tdc_dtype      dtype;          /**< per-element type; must be fixed-width */
    tdc_layout     layout;         /**< semantic layout; only VECTOR_1D in v1 */

    /**
     * @brief Shape template for every emitted block.
     *
     * In v1 only 1D vector streams are supported: rank must be 1 and
     * dim[0] is ignored — each emitted block's dim[0] is set to the
     * actual element count in that block (target_block_elems for all
     * but the last, which carries whatever remains at flush/finish).
     */
    tdc_shape      shape_template;

    /**
     * @brief Codec spec applied to every emitted block.
     *
     * Copied into the encoder at open. The referenced params pointers
     * must remain valid for the encoder's lifetime; they are re-read
     * for every block.
     */
    tdc_codec_spec spec;

    /**
     * @brief Target block size in ELEMENTS, not bytes.
     *
     * 0 selects TDC_PUSHPULL_DEFAULT_BLOCK_ELEMS.
     */
    uint64_t       target_block_elems;

    /** @brief Allocator (realloc_fn convention). Must not be NULL. */
    void *(*realloc_fn)(void *user, void *ptr, size_t new_size);
    void  *alloc_user;
} tdc_pushpull_encoder_config;

/**
 * @brief Open a push/pull encoder.
 *
 * @param cfg Non-NULL configuration. cfg->realloc_fn must be non-NULL.
 *            Layout must be VECTOR_1D and dtype a fixed-width numeric
 *            type in v1.
 * @param enc Non-NULL output. On success set to a non-NULL handle.
 */
tdc_status tdc_pushpull_encoder_open(const tdc_pushpull_encoder_config *cfg,
                                     tdc_pushpull_encoder **enc);

/**
 * @brief Reset the encoder to its post-open state, reusing scratch.
 *
 * Drops any buffered input, any unpulled output, and resets the checkpoint
 * counters. Internal allocations are kept so the next stream on the same
 * encoder doesn't pay re-allocation cost.
 */
tdc_status tdc_pushpull_encoder_reset(tdc_pushpull_encoder *enc);

/**
 * @brief Close the encoder and free all internal state.
 *
 * @param enc Non-NULL handle pointer. *enc is set to NULL on return.
 *            Safe to call on *enc == NULL (no-op).
 */
void tdc_pushpull_encoder_close(tdc_pushpull_encoder **enc);

/**
 * @brief Push raw element bytes into the encoder.
 *
 * @param data   Pointer to n_bytes of raw elements in the configured dtype.
 * @param n_bytes Must be a whole multiple of the element width.
 *
 * Once enough bytes are buffered for a full block, the encoder encodes
 * the block internally and appends the resulting record bytes to its
 * output queue; drain via pull().
 *
 * Returns TDC_E_INVAL if called after finish().
 */
tdc_status tdc_pushpull_encoder_push(tdc_pushpull_encoder *enc,
                                     const void *data, size_t n_bytes);

/**
 * @brief Drain up to @p cap bytes of already-emitted record bytes.
 *
 * @param out              Output buffer; may be NULL iff cap == 0.
 * @param cap              Maximum bytes to copy into @p out.
 * @param out_written      Set to the actual byte count copied.
 * @param more_available   Set to 1 iff the encoder still has more output
 *                         bytes queued after this call; else 0.
 *
 * The caller may call pull() any number of times; each call returns the
 * next contiguous chunk. When cap == 0 the call reports *more_available
 * without copying anything.
 */
tdc_status tdc_pushpull_encoder_pull(tdc_pushpull_encoder *enc,
                                     uint8_t *out, size_t cap,
                                     size_t *out_written,
                                     int    *more_available);

/**
 * @brief Force a block boundary now. Does not close the stream.
 *
 * Any buffered partial-block bytes are encoded as a short block. A no-op
 * if the buffer is empty.
 */
tdc_status tdc_pushpull_encoder_flush(tdc_pushpull_encoder *enc);

/**
 * @brief Flush + mark end of stream. Subsequent push() returns TDC_E_INVAL.
 *
 * pull() continues to work until all remaining output is drained.
 */
tdc_status tdc_pushpull_encoder_finish(tdc_pushpull_encoder *enc);

/**
 * @brief Return a checkpoint cookie for the encoder's current state.
 *
 * Only meaningful at block boundaries. The cookie counts blocks emitted
 * and raw element bytes consumed; match via tdc_pushpull_decoder_checkpoint.
 */
tdc_status tdc_pushpull_encoder_checkpoint(const tdc_pushpull_encoder *enc,
                                           tdc_pushpull_ckpt *out);

/* ----- Decoder ----------------------------------------------------------- */

/**
 * @brief Decoder configuration.
 */
typedef struct {
    /** @brief Allocator (realloc_fn convention). Must not be NULL. */
    void *(*realloc_fn)(void *user, void *ptr, size_t new_size);
    void  *alloc_user;
} tdc_pushpull_decoder_config;

/**
 * @brief Open a push/pull decoder.
 */
tdc_status tdc_pushpull_decoder_open(const tdc_pushpull_decoder_config *cfg,
                                     tdc_pushpull_decoder **dec);

/**
 * @brief Reset decoder state to post-open, reusing scratch.
 *
 * Drops any buffered input, the current output block, and the end-of-stream
 * flag. Useful for reusing the decoder across independent streams.
 */
tdc_status tdc_pushpull_decoder_reset(tdc_pushpull_decoder *dec);

/** @brief Close the decoder and free all internal state. */
void tdc_pushpull_decoder_close(tdc_pushpull_decoder **dec);

/**
 * @brief Push record bytes into the decoder.
 *
 * The entire buffer is always consumed in v1 (*consumed == n_bytes on
 * success); the consumed out-param exists for forward-compatibility with
 * a future back-pressure mode and may be NULL.
 *
 * @param data     Pointer to n_bytes of record bytes.
 * @param n_bytes  Byte count.
 * @param consumed Optional; set to the byte count actually absorbed.
 */
tdc_status tdc_pushpull_decoder_push(tdc_pushpull_decoder *dec,
                                     const uint8_t *data, size_t n_bytes,
                                     size_t *consumed);

/**
 * @brief Signal end of input stream.
 *
 * After finish, next() returns *done=1 once all remaining buffered blocks
 * have been pulled. Calling push() after finish is a usage error
 * (TDC_E_INVAL).
 */
tdc_status tdc_pushpull_decoder_finish(tdc_pushpull_decoder *dec);

/**
 * @brief Pull the next fully-decoded block.
 *
 * Outcomes:
 *   - TDC_OK + *have_block=1 : @p dst is populated with a decoded block.
 *                              dst->data is owned by the decoder and is
 *                              valid until the next push/next/reset/close.
 *   - TDC_OK + *have_block=0 + *done=0 : decoder is starved; caller must
 *                              push more bytes (or call finish).
 *   - TDC_OK + *have_block=0 + *done=1 : end of stream reached, no blocks
 *                              remain. @p dst is left untouched.
 *   - TDC_E_CORRUPT          : the stream is malformed; the decoder
 *                              enters a permanent failure state. Call
 *                              close or reset.
 *
 * @param dst         Non-NULL output block (populated only when
 *                    *have_block == 1).
 * @param have_block  Non-NULL. See outcomes above.
 * @param done        Non-NULL. Set to 1 only when the stream is finished
 *                    and drained.
 */
tdc_status tdc_pushpull_decoder_next(tdc_pushpull_decoder *dec,
                                     tdc_block *dst,
                                     int       *have_block,
                                     int       *done);

/**
 * @brief Return a checkpoint cookie for the decoder's current state.
 *
 * Matches tdc_pushpull_encoder_checkpoint taken after the same number of
 * blocks.
 */
tdc_status tdc_pushpull_decoder_checkpoint(const tdc_pushpull_decoder *dec,
                                           tdc_pushpull_ckpt *out);

#ifdef __cplusplus
}
#endif
#endif /* TDC_PUSHPULL_H */
