/*
 * tdc/codec.h — frozen v0
 *
 * Codec specification: how to encode one block, plus the one-shot
 * tdc_encode_block / tdc_decode_block entry points.
 *
 * Design rules:
 *   1. The transform stage is a CHAIN, not a single id, from day 0. v0 keeps
 *      the chain as a fixed-capacity array (TDC_MAX_TRANSFORMS = 4) to avoid
 *      a heap allocation per encode. A 0 entry terminates the chain.
 *   2. Symbolization is owned by REPRESENTATION (the transform stage). It is
 *      not its own pipeline phase. The model emits a flat residual stream;
 *      transforms turn that stream into entropy-friendly bytes. This was
 *      Option B in the design discussion and is the only way the pipeline
 *      stays the same shape across all dimensionalities.
 *   3. v0 has a STATIC registry. The ids below are an enum, not a runtime
 *      registration table. A future plugin API can be added without changing
 *      the on-disk format because the ids are u16 and id ranges are reserved
 *      below for "core" vs "experimental" vs "user".
 *   4. Per-stage params are passed as opaque pointers. Each stage knows how
 *      to cast them. This keeps tdc_codec_spec small and POD.
 */

#ifndef TDC_CODEC_H
#define TDC_CODEC_H

#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TDC_MAX_TRANSFORMS 4
#define TDC_MAX_ENTROPY    4

/* ----- Stage ids ----------------------------------------------------------- */
/*
 * Reserved id ranges (for both model, transform, entropy enums):
 *     0x0000          = NONE / sentinel (chain terminator for transforms)
 *     0x0001 - 0x00FF = core (shipped with tdc)
 *     0x0100 - 0x01FF = experimental (may change without version bump)
 *     0x0200 - 0xFEFF = reserved
 *     0xFF00 - 0xFFFF = user-defined (post-v0, when plugin API exists)
 */

/* Models — full v0 set.
 *
 * Existing in vectra (extraction): RAW, DELTA_1D, DICT_1D, PRED_2D, PLANE_2D.
 * New in v0 (write from scratch):  STACK_2D, PRED_3D.
 *
 * PRED_2D covers the LEFT/UP/AVERAGE/PAETH predictor family. PLANE is a
 * SEPARATE model id (not a tdc_pred2d_kind) because its side metadata is
 * structurally incompatible: PRED_2D side meta is 1 byte (the resolved
 * kind); PLANE side meta is u16 tile_size + u32 n_tiles + 3*i32 per tile.
 * Cramming both shapes under one model id would force a "primary path +
 * fallback path" branch on the side-meta layout — exactly the anti-pattern
 * the project rules forbid. */
typedef enum {
    TDC_MODEL_NONE      = 0x0000,  /* invalid; never written to disk */
    TDC_MODEL_RAW       = 0x0001,  /* identity; any layout, any dtype */
    TDC_MODEL_DELTA_1D  = 0x0002,  /* x_i - x_{i-1}; VECTOR_1D, integer dtypes */
    TDC_MODEL_DICT_1D   = 0x0003,  /* dictionary + RLE indices; VECTOR_1D + STRING */
    TDC_MODEL_PRED_2D   = 0x0004,  /* LEFT/UP/AVERAGE/PAETH; RASTER_2D */
    TDC_MODEL_STACK_2D  = 0x0005,  /* per-slice 2D predictor; STACK_2D */
    TDC_MODEL_PRED_3D   = 0x0006,  /* 3D neighbor predictor; VOLUME_3D */
    TDC_MODEL_PLANE_2D  = 0x0007,  /* per-tile LSQ plane fit; RASTER_2D */
    TDC_MODEL_DELTA2_1D = 0x0008,  /* 2nd-order XOR-delta; VECTOR_1D, float dtypes */
    TDC_MODEL_FPC_1D    = 0x0009,  /* FCM+DFCM dual predictor; VECTOR_1D, float dtypes */
    TDC_MODEL_DICT_NUMERIC_1D = 0x000A, /* value dictionary + u32 indices;
                                        * VECTOR_1D over i16/u16/i32/u32/f32/
                                        * i64/u64/f64. Wins on low-cardinality
                                        * numeric data (e.g. quantized sensor
                                        * readings) where byte-level predictor
                                        * residuals have higher entropy than
                                        * the raw bytes. */
    TDC_MODEL_SPARSE_ZERO_1D  = 0x000B, /* sparse-zero: positions+values of
                                        * non-zero entries; VECTOR_1D over the
                                        * same numeric dtypes as DICT_NUMERIC_1D.
                                        * Wins on ≥75%-zero int/float vectors
                                        * (mask-like rasters, pruned
                                        * coefficients). Zero = all-zero bytes
                                        * (so +0.0 is zero, -0.0 is not —
                                        * byte identity, like DICT_NUMERIC_1D). */
    TDC_MODEL_QUANTIZE_PRED_2D = 0x000C /* fused quantize-then-PRED_2D for
                                        * RASTER_2D float blocks. Quantizes
                                        * f32/f64 to a narrow integer target
                                        * with the QUANTIZE arithmetic, then
                                        * applies the PRED_2D predictor on the
                                        * integer raster in one step. Residual
                                        * dtype = params->target. Solves the
                                        * ordering bug where PRED_2D applied
                                        * to raw floats produces a float
                                        * residual that QUANTIZE then mangles
                                        * into garbage. */
} tdc_model_id;

/* Transforms (representation stage; chained) */
typedef enum {
    TDC_XFORM_NONE         = 0x0000, /* chain terminator */
    TDC_XFORM_QUANTIZE     = 0x0001, /* lossy: f32/f64 -> narrow int */
    TDC_XFORM_ZIGZAG       = 0x0002, /* signed -> unsigned, small magnitudes near 0 */
    TDC_XFORM_BYTE_SHUFFLE = 0x0003, /* transpose by byte lane (8/4/2 byte elems) */
    TDC_XFORM_BIT_SHUFFLE  = 0x0004, /* transpose by bit lane (post-v0; reserved) */
    /* Experimental range (0x0100-0x01FF). Backends in this range are
     * statically compiled, reserved via an enum entry, and may change
     * semantics without a format version bump. Used as the landing pad
     * for reference backends shipped alongside the "Extending" vignette. */
    TDC_XFORM_COMPLEMENT   = 0x0100  /* bitwise-NOT (reference backend) */
} tdc_xform_id;

/* Entropy coders */
typedef enum {
    TDC_ENTROPY_NONE    = 0x0000, /* memcpy passthrough */
    TDC_ENTROPY_LZ     = 0x0001, /* native LZ77, separated-stream, 4 MiB window.
                                   * Note: LZ is a byte-level matcher. On a
                                   * multi-byte dtype with no exact byte
                                   * repetitions (e.g. an i32 ramp `1000+i*3`),
                                   * RAW+LZ returns ~1.0x. Put a model OR a
                                   * BYTE_SHUFFLE in front to expose the
                                   * per-lane structure. */
    TDC_ENTROPY_DEFLATE = 0x0002, /* zlib deflate; optional link, "ratio" mode */
    TDC_ENTROPY_HUFFMAN = 0x0003, /* native canonical static Huffman, max code length 15 */
    TDC_ENTROPY_FSE     = 0x0004, /* native tabled-ANS / Finite State Entropy */
    TDC_ENTROPY_LANE    = 0x0005, /* per-lane entropy: splits BSHUF output into
                                   * byte lanes, applies an independent sub-coder
                                   * per lane (AUTO picks Huffman/FSE/STORE per
                                   * lane based on Shannon entropy sampling). */
    TDC_ENTROPY_LZ_OPT = 0x0006, /* LZ with optimal (dynamic-programming)
                                   * parser. Emits the SAME on-disk format as
                                   * TDC_ENTROPY_LZ — decoder is shared. Slower
                                   * encode, smaller output on structured data. */
    TDC_ENTROPY_LZ_STREAMS = 0x0007, /* LZ parse split into 4 separated,
                                       * entropy-coded streams (literal bytes,
                                       * lit_len u32, match_len u32, match_off
                                       * u32). Each stream picks NONE/HUFFMAN/FSE
                                       * via Shannon entropy. Same parser as LZ,
                                       * different serializer — NOT compatible
                                       * with the single-stream decoder. */
    TDC_ENTROPY_LZ_SPLIT  = 0x0008, /* LZ with optimal parser + split entropy:
                                      * literal bytes and sequence descriptors
                                      * (tags, extensions, offsets) are separated
                                      * and Huffman-coded independently. Better
                                      * compression than LZ_OPT+HUF because the
                                      * two sub-streams have distinct byte
                                      * distributions. Self-contained — not
                                      * chainable with a second entropy stage. */
    TDC_ENTROPY_HUFFMAN4  = 0x0009  /* 4-stream canonical Huffman. Same tree as
                                      * HUFFMAN but the payload is split into 4
                                      * independent bitstreams decoded with 4
                                      * interleaved bit-readers for ILP. 2-3×
                                      * faster decode than single-stream Huffman
                                      * on out-of-order CPUs. Falls back to
                                      * single-stream HUFFMAN for inputs < 256
                                      * bytes. */
} tdc_entropy_id;

/* ----- Per-stage params ---------------------------------------------------- */
/*
 * These structs are public so that callers can stack-allocate them. Each
 * stage's encode() casts the (void*) it receives back to its own params type.
 *
 * Adding a field to one of these is forward-compatible IFF the new field has
 * a meaningful zero default (so old call sites that memset the struct still
 * work). Otherwise the format version must be bumped.
 */

/* TDC_XFORM_QUANTIZE */
typedef struct {
    double    scale;       /* stored = round((value - offset) * scale) */
    double    offset;
    tdc_dtype target;      /* must be a signed integer dtype */
} tdc_quantize_params;

/* TDC_MODEL_PRED_2D — predictor selection */
typedef enum {
    TDC_PRED2D_AUTO    = 0,
    TDC_PRED2D_LEFT    = 1,
    TDC_PRED2D_UP      = 2,
    TDC_PRED2D_AVERAGE = 3,
    TDC_PRED2D_PAETH   = 4
} tdc_pred2d_kind;

typedef struct {
    tdc_pred2d_kind kind;
} tdc_pred2d_params;

/* TDC_MODEL_QUANTIZE_PRED_2D — fused quantize-then-pred2d for float rasters.
 *
 * Combines tdc_quantize_params (scale/offset/target) and tdc_pred2d_params
 * (predictor kind) into one model so they apply atomically. Without the
 * fusion, "model = PRED_2D, xform[0] = QUANTIZE" runs PRED_2D on raw
 * doubles first (producing a float residual) and then QUANTIZE re-quantizes
 * the residual into garbage values. */
typedef struct {
    double          scale;        /* same semantics as tdc_quantize_params */
    double          offset;
    tdc_dtype       target;       /* I8/I16/I32 — narrow integer raster */
    tdc_pred2d_kind kind;         /* AUTO selects on the quantized raster */
} tdc_quantize_pred2d_params;

/* TDC_MODEL_PRED_3D — true 3D neighborhood predictor selection.
 *
 * Causal neighbor naming (offsets from voxel (z, y, x)):
 *   a   = (z,   y,   x-1)   "left"
 *   b   = (z,   y-1, x  )   "up"
 *   c   = (z-1, y,   x  )   "front"  (prior slice)
 *   ab  = (z,   y-1, x-1)
 *   ac  = (z-1, y,   x-1)
 *   bc  = (z-1, y-1, x  )
 *   abc = (z-1, y-1, x-1)
 *
 * Kinds:
 *   LEFT/UP/FRONT — single-axis predictors (pred = a, b, or c).
 *   AVG3          — mean of in-bounds face neighbors among {a, b, c}; on
 *                   the inner box pred = (a + b + c) / 3 with C truncation.
 *                   Edge cells use the count of in-bounds neighbors.
 *   GRAD3D        — trilinear linear predictor:
 *                       pred = a + b + c - ab - ac - bc + abc
 *                   This is the natural 3D extension of pred2d's linear
 *                   plane predictor (the `p` in PNG Paeth) and is exact
 *                   on any tri-affine field.
 *   PAETH3D       — pick the face neighbor in {a, b, c} closest to the
 *                   GRAD3D linear predictor. Tie-break order: a wins ties
 *                   over b, b wins ties over c. Reduces to 2D Paeth on
 *                   each face slab where one axis is on the boundary.
 *   AUTO          — encoder picks the kind with the smallest sum of
 *                   |residual| over a sample prefix; never written to disk.
 *
 * Side metadata: 1 byte = the resolved predictor kind (one of LEFT, UP,
 * FRONT, AVG3, GRAD3D, PAETH3D — never AUTO). */
typedef enum {
    TDC_PRED3D_AUTO    = 0,
    TDC_PRED3D_LEFT    = 1,
    TDC_PRED3D_UP      = 2,
    TDC_PRED3D_FRONT   = 3,
    TDC_PRED3D_AVG3    = 4,
    TDC_PRED3D_GRAD3D  = 5,
    TDC_PRED3D_PAETH3D = 6
} tdc_pred3d_kind;

typedef struct {
    tdc_pred3d_kind kind;
} tdc_pred3d_params;

/* TDC_MODEL_PLANE_2D — per-tile plane fit */
typedef struct {
    uint16_t tile_size;    /* default 32 */
} tdc_plane2d_params;

/* TDC_MODEL_STACK_2D — per-slice 2D predictor with optional inter-slice delta */
typedef struct {
    tdc_pred2d_kind kind;   /* in-plane predictor (AUTO selects on first slice) */
    int inter_slice;        /* 1 = subtract previous slice before in-plane prediction */
} tdc_stack2d_params;

/* TDC_ENTROPY_LZ / DEFLATE */
typedef struct {
    int level;             /* 0 = default; range is backend-specific */
} tdc_entropy_level;

/* TDC_ENTROPY_LZ_STREAMS — parser-side knobs.
 *
 * Extends tdc_entropy_level: the `level` field in the leading position means
 * a pointer to this struct works wherever tdc_entropy_level works (the LZ
 * entropy backends cast by layout). Extra fields are zero-default so a plain
 * tdc_entropy_level pointer still behaves identically.
 *
 * min_match: minimum emitted match length in bytes. 0 or values < 3 are
 *     treated as the baseline (3). Raising this fuses short matches back
 *     into their surrounding literals post-parse, raising bytes/seq and
 *     decode throughput at the cost of some ratio. Typical values: 3
 *     (baseline), 4-8 (speed-biased on structured data). The on-disk
 *     format is unchanged — the decoder is oblivious to this knob.
 */
typedef struct {
    int      level;
    uint32_t min_match;
    uint32_t _reserved;
} tdc_lz_streams_params;

/* TDC_ENTROPY_LANE — per-lane entropy selection.
 *
 * After BYTE_SHUFFLE transposes by byte lane, different lanes have very
 * different distributions. High-significance lanes (exponent bytes) are
 * peaked → Huffman suffices. Low-significance lanes (mantissa noise)
 * benefit from FSE's fractional-bit precision. Incompressible lanes
 * should be stored raw.
 *
 * n_lanes must match elem_size from the upstream BYTE_SHUFFLE.
 * lane_entropy[i] selects the sub-coder for lane i:
 *   TDC_ENTROPY_NONE = AUTO (heuristic picks Huffman/FSE/NONE per lane).
 *   TDC_ENTROPY_HUFFMAN / TDC_ENTROPY_FSE / explicitly selects that coder.
 *
 * On-disk header is self-describing; params are only needed at encode time. */
#define TDC_MAX_LANES 8

typedef struct {
    uint8_t        n_lanes;
    tdc_entropy_id lane_entropy[TDC_MAX_LANES];
} tdc_lane_entropy_params;

/* ----- Codec spec ---------------------------------------------------------- */
/*
 * The full description of how to encode one block. POD, copy-by-value safe.
 *
 * Both the transform chain AND the entropy chain run LEFT TO RIGHT during
 * encode and RIGHT TO LEFT during decode. The first NONE (0) terminates
 * each chain (sticky: everything after the first NONE must also be NONE).
 *
 * An all-NONE entropy chain is valid and means "passthrough" — the
 * post-transform residuals are written to the payload byte-for-byte.
 * tdc_codec_spec_raw() leaves entropy[] zeroed for exactly this reason.
 *
 * Typical chains:
 *
 *   model      = TDC_MODEL_PRED_2D
 *   xform[0]   = TDC_XFORM_ZIGZAG
 *   xform[1]   = TDC_XFORM_BYTE_SHUFFLE
 *   entropy[0] = TDC_ENTROPY_LZ            <-- single-stage entropy
 *
 *   model      = TDC_MODEL_DELTA_1D
 *   xform[0]   = TDC_XFORM_ZIGZAG
 *   xform[1]   = TDC_XFORM_BYTE_SHUFFLE
 *   entropy[0] = TDC_ENTROPY_LZ
 *   entropy[1] = TDC_ENTROPY_HUFFMAN        <-- LZ output re-coded by Huffman
 */

typedef struct {
    tdc_model_id   model;
    tdc_xform_id   xform[TDC_MAX_TRANSFORMS];
    tdc_entropy_id entropy[TDC_MAX_ENTROPY];

    const void *model_params;     /* points to tdc_pred2d_params, etc., or NULL */
    const void *xform_params[TDC_MAX_TRANSFORMS];
    const void *entropy_params[TDC_MAX_ENTROPY];
} tdc_codec_spec;

/* Convenience: zero-initialize a spec to RAW + no xforms + passthrough entropy.
 * The zeroed entropy chain is treated as NONE (memcpy passthrough) by the
 * encode/decode drivers. */
static inline tdc_codec_spec tdc_codec_spec_raw(void) {
    tdc_codec_spec s = {0};
    s.model = TDC_MODEL_RAW;
    return s;
}

/* ----- One-shot encode / decode entry points ------------------------------- */
/*
 * Encode a single block according to spec. Output is written into out as a
 * single tdc_block_record (header + side_meta + payload). The caller's
 * tdc_buffer is grown via its realloc_fn as needed.
 */
tdc_status tdc_encode_block(const tdc_block      *src,
                            const tdc_codec_spec *spec,
                            tdc_buffer           *out);

/*
 * Decode a single block record. dst must already have:
 *   - data pointing at a buffer of n_elems * dtype_size bytes
 *   - dtype, layout, shape filled in (these are checked against the header)
 * On TDC_OK, dst->data is filled with the reconstructed values.
 *
 * If the header's (dtype, layout, shape) disagrees with dst, returns
 * TDC_E_DTYPE / TDC_E_LAYOUT / TDC_E_SHAPE without touching dst->data.
 */
tdc_status tdc_decode_block(const uint8_t *src, size_t src_size,
                            tdc_block     *dst);

/*
 * Decode a single block record using a caller-supplied allocator.
 * Identical to tdc_decode_block, but internal scratch buffers are
 * allocated via scratch->realloc_fn instead of libc realloc.
 *
 * The caller must set scratch->realloc_fn before calling. The
 * scratch->data / size / capacity fields are ignored on entry and
 * are NOT used as a pre-allocated buffer — they serve only as the
 * parent template for internal ping-pong buffers.
 *
 * On return (success or error), all scratch memory allocated by the
 * function has been freed via the same realloc_fn(user, ptr, 0).
 */
tdc_status tdc_decode_block_ex(const uint8_t *src, size_t src_size,
                               tdc_block *dst, tdc_buffer *scratch);

/*
 * Decode into a caller-provided destination buffer; never touches
 * dst->data's allocation.
 *
 * Zero-allocation decode variant. Intended for callers that have already
 * allocated the destination (e.g. an R vector backed by an SEXP buffer,
 * a memory-mapped region, an arena slab) and want tdc to write the
 * reconstructed values directly into it.
 *
 * Before calling, the caller must:
 *   - set dst->data to a buffer of at least n_elems * dtype_size(dtype)
 *     bytes, where n_elems is the product of the record's dim[], and
 *   - populate dst->dtype, dst->layout, and dst->shape to EXACTLY match
 *     the record (fields are validated, not overwritten).
 *
 * Use tdc_decode_peek() on the same src to obtain the dtype / layout /
 * shape / byte count needed for pre-allocation.
 *
 * Internal ping-pong scratch (for entropy + transform stages) is still
 * required and is allocated from the C runtime — "zero-copy" refers to
 * the destination buffer, not the whole pipeline. Callers that want
 * full control over all allocations should use tdc_decode_block_ex and
 * pre-seat dst->data themselves.
 *
 * Returns:
 *   TDC_OK on success;
 *   TDC_E_INVAL  if dst->data is NULL for a non-empty block, or if
 *                dst / src is NULL;
 *   TDC_E_DTYPE / TDC_E_LAYOUT / TDC_E_SHAPE if a pre-populated field
 *                disagrees with the record header;
 *   TDC_E_CORRUPT for malformed records;
 *   other TDC_E_* on decode failure.
 *
 * dst->data is written in place. On failure its contents are unspecified
 * but its pointer is unchanged (never freed).
 */
tdc_status tdc_decode_block_into(const uint8_t *src, size_t src_size,
                                 tdc_block     *dst);

/*
 * Pre-read a block record header without decoding.
 *
 * Reads only the 80-byte block header and returns the fields the caller
 * needs to pre-allocate a destination buffer for tdc_decode_block_into.
 *
 *   src                 Pointer to the start of a tdc_block_record.
 *   src_size            Size of src in bytes; must be at least
 *                       TDC_BLOCK_HEADER_SIZE.
 *   out_meta            Non-NULL. On TDC_OK, out_meta->dtype,
 *                       out_meta->layout, and out_meta->shape
 *                       (rank + dim[] + row-major contiguous stride[])
 *                       are populated. Other fields are zeroed.
 *   out_bytes_required  May be NULL. If non-NULL, on TDC_OK receives
 *                       the byte count the caller must allocate for
 *                       dst->data: n_elems * dtype_size(dtype).
 *                       Returns TDC_E_UNSUPPORTED for dtypes without a
 *                       fixed element width (TDC_DT_STRING) — those
 *                       blocks cannot use tdc_decode_block_into in v0.
 *
 * Returns:
 *   TDC_OK on success;
 *   TDC_E_INVAL       if src or out_meta is NULL;
 *   TDC_E_CORRUPT / TDC_E_VERSION if the header is malformed;
 *   TDC_E_UNSUPPORTED if out_bytes_required is non-NULL and the dtype
 *                     has no fixed element width.
 *
 * This is the companion to tdc_decode_block_into. It does NOT decode
 * the payload and is safe to call on a partial buffer as long as the
 * first 80 bytes are present.
 */
tdc_status tdc_decode_peek(const uint8_t *src, size_t src_size,
                           tdc_block *out_meta,
                           size_t    *out_bytes_required);

/*
 * Decode a variable-width block (TDC_DT_STRING in v0).
 *
 * Variable-width blocks cannot use tdc_decode_block_into: the output
 * heap size depends on the dictionary indices in the residual stream
 * and is not derivable from the 80-byte block header alone. This entry
 * point runs the standard pipeline (header validate, entropy decode,
 * transform reverse), then sizes dst->data and dst->offsets from the
 * decoded residual + side metadata, allocates both via
 * alloc->realloc_fn, and runs the model decode in place.
 *
 * On entry:
 *   src, src_size:    the block record bytes;
 *   dst->dtype:       must be a variable-width dtype (TDC_DT_STRING in
 *                     v0). Validated against the header.
 *   dst->layout, dst->shape: must match the header (validated, not
 *                     overwritten — same convention as
 *                     tdc_decode_block_into);
 *   dst->data, dst->offsets: MUST be NULL on entry; the function
 *                     allocates them via alloc->realloc_fn. Passing
 *                     non-NULL is rejected with TDC_E_INVAL to avoid
 *                     leaking caller memory;
 *   alloc->realloc_fn: must be set; alloc->user is forwarded. The
 *                     same realloc_fn is used for internal ping-pong
 *                     scratch as well as for the final dst->data /
 *                     dst->offsets allocations.
 *
 * On TDC_OK:
 *   dst->data is a freshly allocated byte heap whose size equals
 *     dst->offsets[n_elems] (zero is allowed; in that case dst->data
 *     is left NULL);
 *   dst->offsets is a freshly allocated (n_elems + 1)-entry uint32_t
 *     table (allocated even when n_elems == 0, to a single sentinel
 *     entry of value 0).
 *   Both buffers are owned by the caller and must be freed via
 *     alloc->realloc_fn(alloc->user, ptr, 0).
 *
 * On error: dst->data and dst->offsets are NULL (any partial
 *   allocation has been freed via alloc->realloc_fn). Internal scratch
 *   is always freed before return.
 *
 * Returns:
 *   TDC_OK                on success;
 *   TDC_E_INVAL           NULL src/dst/alloc, missing realloc_fn,
 *                         non-NULL dst->data or dst->offsets on entry;
 *   TDC_E_DTYPE           dst->dtype is fixed-width (use _into instead),
 *                         or disagrees with the record header;
 *   TDC_E_LAYOUT/_SHAPE   dst->layout / dst->shape disagrees with header;
 *   TDC_E_UNSUPPORTED     model id is not a variable-width producer
 *                         (only TDC_MODEL_DICT_1D in v0);
 *   TDC_E_CORRUPT/_VERSION malformed record;
 *   TDC_E_NOMEM           alloc->realloc_fn returned NULL;
 *   other TDC_E_*         on entropy/transform/model decode failure.
 */
tdc_status tdc_decode_block_varlen(const uint8_t *src, size_t src_size,
                                   tdc_block     *dst, tdc_buffer *alloc);

#ifdef __cplusplus
}
#endif
#endif /* TDC_CODEC_H */
