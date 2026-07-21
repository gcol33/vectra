/*
 * tdc/stream.h
 *
 * Streaming container encode/decode API.  Wraps the block-at-a-time
 * tdc_encode_block / tdc_decode_block with container framing (64-byte
 * header, per-block records, trailing index) and I/O abstraction.
 *
 * I/O is callback-based: tdc never calls fopen/fwrite/fread/fseek.
 * The caller adapts those (or mmap, network sockets, R connections,
 * etc.) behind the tdc_io callbacks.
 *
 * Two operating modes:
 *   - Seekable:  encoder patches the container header at close time;
 *                decoder can random-access blocks via the trailing index.
 *   - Forward-only (seek_fn == NULL): encoder writes a deferred-index
 *                container (header n_blocks=0); decoder reads sequentially.
 *
 * Optional features (all backward-compatible with simple block streams):
 *   - Schema: column descriptors written after the container header.
 *   - Row-group index: hierarchical block index for column-level seek.
 */

#ifndef TDC_STREAM_H
#define TDC_STREAM_H

#include "types.h"
#include "format.h"
#include "codec.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ----- I/O abstraction -------------------------------------------------- */

/*
 * write_fn: Write exactly `size` bytes from `data`.
 *           Return TDC_OK on success, TDC_E_IO on failure.
 *           Must be non-NULL for encoding.
 *
 * read_fn:  Read up to `size` bytes into `buf`.  Set *bytes_read to the
 *           actual count.  Return TDC_OK on success (including EOF with
 *           *bytes_read == 0), TDC_E_IO on failure.
 *           Must be non-NULL for decoding.
 *
 * seek_fn:  Seek to `offset` relative to `whence` (TDC_SEEK_*).
 *           May be NULL.  When NULL:
 *             - Encoder omits header patching (deferred-index mode).
 *             - Decoder supports sequential mode only.
 *
 * ctx:      Opaque pointer passed as first argument to every callback.
 */

typedef struct {
    tdc_status (*write_fn)(void *ctx, const void *data, size_t size);
    tdc_status (*read_fn)(void *ctx, void *buf, size_t size,
                          size_t *bytes_read);
    tdc_status (*seek_fn)(void *ctx, int64_t offset, int whence);
    void *ctx;
} tdc_io;

/* Whence constants for seek_fn.  Defined here so callers don't need
 * <stdio.h> just for SEEK_SET/CUR/END. */
#define TDC_SEEK_SET 0
#define TDC_SEEK_CUR 1
#define TDC_SEEK_END 2

/* ----- Schema types ----------------------------------------------------- */

/* Descriptor for a single column in the schema section. */
typedef struct {
    const char *name;        /* null-terminated by the decoder */
    uint16_t    name_len;
    uint8_t     dtype;       /* tdc_dtype value */
    const char *annotation;  /* null-terminated; empty string if ann_len==0 */
    uint16_t    ann_len;
} tdc_column_desc;

/* Parsed schema. Owned by the decoder; valid for its lifetime. */
typedef struct {
    uint16_t              n_columns;
    const tdc_column_desc *columns;  /* array of n_columns entries */
} tdc_schema;

/* ----- Row-group index types -------------------------------------------- */

/* Per-column entry within a row group. */
typedef struct {
    uint64_t block_offset;
    uint64_t block_total;
} tdc_rg_col_entry;

/* Per-row-group entry in the trailing index. */
typedef struct {
    uint64_t          offset;    /* file offset of first block in group */
    uint64_t          n_rows;
    uint16_t          n_cols;
    tdc_rg_col_entry *columns;   /* array of n_cols entries; owned */
} tdc_rowgroup_entry;

/* ----- Column statistics ------------------------------------------------ */

/*
 * Per-column statistics attached to a row group. The min/max fields are
 * raw dtype-native bytes, zero-padded to 16 bytes, stored little-endian.
 * For strings, they hold the first 8 bytes of the lexicographic min/max.
 *
 * null_count is caller-supplied: the encoder never inspects block data
 * to compute it; instead `set_rowgroup_stats` records whatever the caller
 * passes in. Pipelines that don't track null counts should leave it zero.
 */
#define TDC_STATS_VALUE_SIZE 16

typedef struct {
    uint8_t  has_stats;
    uint8_t  min[TDC_STATS_VALUE_SIZE];
    uint8_t  max[TDC_STATS_VALUE_SIZE];
    uint64_t null_count;
} tdc_column_stats;

/* ----- Encoder ---------------------------------------------------------- */

/* Opaque encoder state.  Allocated by open, freed by close. */
typedef struct tdc_stream_encoder tdc_stream_encoder;

/* Configuration for opening a streaming encoder.  POD, copy-by-value. */
typedef struct {
    tdc_io      io;
    uint16_t    flags;          /* TDC_CONTAINER_FLAG_* */
    tdc_dtype   global_dtype;   /* 0 if HETEROGENEOUS */
    tdc_layout  global_layout;  /* 0 if HETEROGENEOUS */
    tdc_shape   global_shape;   /* zeroed if HETEROGENEOUS */

    /*
     * Optional column schema.  If non-NULL, serialized and written
     * immediately after the 64-byte container header.  Its serialized
     * byte count is recorded in the container header's schema_size
     * field (0 when no schema is attached).  The pointer is only read
     * during open; does not need to remain valid after open returns.
     */
    const tdc_schema *schema;

    /* Allocator for internal scratch/index buffers.  Must not be NULL. */
    void *(*realloc_fn)(void *user, void *ptr, size_t new_size);
    void   *alloc_user;
} tdc_stream_encoder_config;

/*
 * Open a streaming encoder.  Writes the 64-byte container header
 * immediately (and the schema section, if provided).  If io.seek_fn
 * is NULL, the header is written with n_blocks=0 and index_offset=0
 * (deferred-index mode).
 *
 * On success, *enc is set to a non-NULL opaque handle.
 * On failure, *enc is set to NULL.
 */
tdc_status tdc_stream_encoder_open(const tdc_stream_encoder_config *cfg,
                                   tdc_stream_encoder **enc);

/*
 * Encode one block and write it to the output stream.  Calls
 * tdc_encode_block internally, then writes the resulting block record
 * bytes through io.write_fn.  Tracks the block's file offset and
 * total size for the trailing index.
 *
 * Blocks must be written in order.  Each call writes immediately.
 */
tdc_status tdc_stream_encoder_write_block(tdc_stream_encoder       *enc,
                                          const tdc_block          *src,
                                          const tdc_codec_spec     *spec);

/*
 * Signal end of current row group.  n_rows is the row count.
 * Must be called after writing all n_cols blocks for the group.
 * Optional — if never called, no row-group index is written.
 */
tdc_status tdc_stream_encoder_end_rowgroup(tdc_stream_encoder *enc,
                                           uint64_t n_rows);

/*
 * Attach per-column statistics to the currently-open row group.
 *
 * Must be called after all n_cols blocks for the group have been written
 * via write_block, but BEFORE end_rowgroup. n_cols must match the number
 * of blocks written since the last end_rowgroup. The stats array is
 * copied internally; the caller retains ownership of the argument.
 *
 * Calling this function on any row group causes the container header's
 * TDC_CONTAINER_FLAG_HAS_STATS bit to be set at close time. Row groups
 * that do not receive a set_rowgroup_stats call are recorded as having
 * no stats; tdc_stream_decoder_get_stats returns NULL for them.
 *
 * May be called at most once per row group.
 */
tdc_status tdc_stream_encoder_set_rowgroup_stats(tdc_stream_encoder     *enc,
                                                 const tdc_column_stats *stats,
                                                 uint16_t                n_cols);

/*
 * Finalize the container:
 *   1. Write the trailing row-group index (if any row groups were ended).
 *   2. If io.seek_fn is available, seek back to offset 0 and rewrite
 *      the header with the final n_blocks, index_offset, index_size.
 *   3. Free all internal state.
 *
 * After close, *enc is set to NULL.  Calling close on a NULL *enc is
 * a safe no-op.  Internal state is freed regardless of errors.
 */
tdc_status tdc_stream_encoder_close(tdc_stream_encoder **enc);

/*
 * Discard an encoder without finalizing: frees all internal state and
 * writes nothing -- no index, no schema, no header patch. Sets *enc to
 * NULL; safe on a NULL *enc.
 *
 * For a widening encoder this is what makes a failed widen harmless: the
 * blocks it already wrote sit past the container's trailing index and are
 * referenced only by the header patch that now never happens, so the
 * container is left exactly as it was found. (The caller may truncate the
 * file back to its previous length to reclaim those bytes.)
 *
 * On an encoder from tdc_stream_encoder_open the header was already written
 * at open, so aborting leaves a container with n_blocks = 0 and no index.
 */
tdc_status tdc_stream_encoder_abort(tdc_stream_encoder **enc);

/* Number of blocks written so far. */
uint64_t tdc_stream_encoder_block_count(const tdc_stream_encoder *enc);

/* ----- Widening an existing container ------------------------------------ */

/*
 * Configuration for opening an existing container to APPEND COLUMNS.
 * io must supply write_fn, read_fn and seek_fn: the existing header and
 * row-group index are read back, and the new bytes are written past them.
 */
typedef struct {
    tdc_io io;

    /*
     * The full replacement schema: every column already in the container,
     * in its existing order, followed by the appended ones. Read only
     * during open. Column entries are positional in both the schema and
     * each row group's column array, so appended columns must come last.
     */
    const tdc_schema *schema;

    void *(*realloc_fn)(void *user, void *ptr, size_t new_size);
    void   *alloc_user;
} tdc_stream_encoder_widen_config;

/*
 * Open an existing heterogeneous container to append columns to it,
 * WITHOUT reading or rewriting the data already in the file.
 *
 * Cost is proportional to the appended columns plus the rebuilt schema and
 * index, not to the container's size: existing block records are never
 * read, decoded, or moved.
 *
 * The container must carry TDC_CONTAINER_FLAG_HETEROGENEOUS and a trailing
 * row-group index. Both container versions can be widened, and the result
 * is always stamped TDC_CONTAINER_VERSION_WIDENED, because widening
 * relocates the schema to the tail (the section at offset 64 sits directly
 * before the first block and cannot grow in place).
 *
 * Crash safety: everything is written past the existing trailing index,
 * and the 64-byte header is patched last. If the process dies before that
 * patch, the file still reads as the container did before the widen. The
 * trade is that the superseded index remains as a gap in the blocks
 * region, so a widened container is random-access only -- see
 * tdc_stream_decoder_peek_block.
 *
 * Use tdc_stream_encoder_widen_block to write the appended columns, then
 * tdc_stream_encoder_close to emit the schema, the rebuilt index, and the
 * patched header. Closing without writing any block is valid and rewrites
 * the container with the given schema.
 */
tdc_status tdc_stream_encoder_open_widen(
    const tdc_stream_encoder_widen_config *cfg,
    tdc_stream_encoder **enc);

/*
 * Encode one block and append it as a NEW TRAILING COLUMN of an existing
 * row group. Valid only on an encoder from tdc_stream_encoder_open_widen.
 *
 * rg_index selects the row group; the block becomes its column
 * n_cols (0-based), matching the tail-append of the widened schema. The
 * block's element count must equal that row group's n_rows -- this
 * function cannot verify that, as a block carries a shape rather than a
 * row count, so the caller owns the invariant.
 *
 * `stats` may be NULL. When supplied and the row group carried no stats,
 * a stats block is created with the pre-existing columns marked
 * has_stats = 0; their statistics are not invented, since computing them
 * would require reading the data this operation is designed not to touch.
 *
 * Columns may be appended to row groups in any order, and more than one
 * column may be appended to the same row group; each call adds exactly one.
 */
tdc_status tdc_stream_encoder_widen_block(tdc_stream_encoder     *enc,
                                          uint64_t                rg_index,
                                          const tdc_block        *src,
                                          const tdc_codec_spec   *spec,
                                          const tdc_column_stats *stats);

/* ----- Decoder ---------------------------------------------------------- */

/* Opaque decoder state. */
typedef struct tdc_stream_decoder tdc_stream_decoder;

/* Configuration for opening a streaming decoder. */
typedef struct {
    tdc_io io;

    /* Allocator for internal scratch buffers.  Must not be NULL. */
    void *(*realloc_fn)(void *user, void *ptr, size_t new_size);
    void   *alloc_user;
} tdc_stream_decoder_config;

/*
 * Open a streaming decoder.  Reads and validates the 64-byte container
 * header (and the schema section, if present).  If io.seek_fn is
 * available AND the header carries a valid index_offset, reads the
 * trailing row-group index for random access.
 *
 * On success, *dec is set to a non-NULL handle.
 */
tdc_status tdc_stream_decoder_open(const tdc_stream_decoder_config *cfg,
                                   tdc_stream_decoder **dec);

/*
 * Close the decoder and free all internal state.  Sets *dec to NULL.
 * Safe to call on a NULL *dec (no-op).
 */
tdc_status tdc_stream_decoder_close(tdc_stream_decoder **dec);

/* Read-only view of the container header.  Valid for the lifetime of
 * the decoder.  Returns NULL if dec is NULL. */
const tdc_container_header *tdc_stream_decoder_header(
    const tdc_stream_decoder *dec);

/* Read-only view of the parsed schema.  Returns NULL if the container
 * has no schema section or dec is NULL. */
const tdc_schema *tdc_stream_decoder_read_schema(
    const tdc_stream_decoder *dec);

/* Whether random access is available (seek_fn provided AND the
 * row-group index was successfully loaded). */
int tdc_stream_decoder_has_rowgroup_index(
    const tdc_stream_decoder *dec);

/* Number of row groups in the index (0 if no index). */
uint64_t tdc_stream_decoder_rowgroup_count(
    const tdc_stream_decoder *dec);

/* Read-only view of a row-group entry.  Returns NULL if rg_index is
 * out of range or the index is not loaded. */
const tdc_rowgroup_entry *tdc_stream_decoder_get_rowgroup(
    const tdc_stream_decoder *dec, uint64_t rg_index);

/*
 * Read-only view of the column statistics attached to a specific
 * (rg_index, col_index) cell. Returns NULL if:
 *   - dec is NULL or no row-group index is loaded,
 *   - the container was encoded without TDC_CONTAINER_FLAG_HAS_STATS,
 *   - the specific row group has no stats attached, OR
 *   - rg_index or col_index is out of range.
 *
 * The returned pointer is owned by the decoder and valid for its
 * lifetime.
 */
const tdc_column_stats *tdc_stream_decoder_get_stats(
    const tdc_stream_decoder *dec,
    uint64_t                  rg_index,
    uint16_t                  col_index);

/*
 * Random-access: seek to column `col_index` within row group `rg_index`
 * and peek its block header.  Requires has_rowgroup_index() to be true.
 *
 * After seeking, proceed with read_block or skip_block as usual.
 */
tdc_status tdc_stream_decoder_seek_rowgroup(tdc_stream_decoder *dec,
                                            uint64_t rg_index,
                                            uint16_t col_index,
                                            tdc_block_record *rec);

/*
 * Read the next block record header from the current stream position.
 * Does NOT decode the block data — only reads the header so the caller
 * can inspect dtype/layout/shape and allocate dst->data.
 *
 * On success, *rec is filled with the validated block record header.
 * At end-of-blocks (trailing index or EOF), returns TDC_OK and sets
 * rec->magic to 0 as a sentinel.
 *
 * After peeking, the caller MUST call either read_block or skip_block.
 */
tdc_status tdc_stream_decoder_peek_block(tdc_stream_decoder *dec,
                                         tdc_block_record   *rec);

/*
 * Decode the current block (previously peeked) into dst.  The caller
 * must have set dst->dtype, dst->layout, dst->shape, and allocated
 * dst->data based on the peeked record header.
 *
 * Reads the remaining block sections (side_meta, xform_params, payload,
 * validity) from the stream, then calls tdc_decode_block.
 */
tdc_status tdc_stream_decoder_read_block(tdc_stream_decoder *dec,
                                         tdc_block          *dst);

/*
 * Skip the current block (previously peeked) without decoding.
 * Advances the stream position past all block sections.
 */
tdc_status tdc_stream_decoder_skip_block(tdc_stream_decoder *dec);

/*
 * Number of blocks.  In sequential-only mode with a deferred header
 * (n_blocks == 0), returns the number of blocks read so far.
 */
uint64_t tdc_stream_decoder_block_count(
    const tdc_stream_decoder *dec);

#ifdef __cplusplus
}
#endif
#endif /* TDC_STREAM_H */
