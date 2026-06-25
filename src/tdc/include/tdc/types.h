/*
 * tdc/types.h — frozen v0
 *
 * Foundational types: status, dtype, layout, shape, block.
 *
 * Design rules (do not violate without bumping the format version):
 *   1. tdc_block describes MEMORY ONLY. No compression policy lives here.
 *   2. Structural rank (shape.rank) and semantic layout (tdc_layout) are
 *      independent. STACK_2D is rank 3 but iterates as a stack of 2D frames;
 *      VOLUME_3D is rank 3 with true 3D neighborhood access. The model
 *      dispatcher branches on layout, never on rank alone.
 *   3. v0 is numeric only. No string dtype until numeric blocks are stable
 *      (strings need offsets + dictionary logic and will live behind a
 *      separate model in a later milestone).
 *   4. Endianness is fixed: tdc on-disk format is little-endian. The library
 *      compiles to little-endian targets only (x86_64, aarch64). A big-endian
 *      port would require a format version bump.
 */

#ifndef TDC_TYPES_H
#define TDC_TYPES_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ----- Status codes -------------------------------------------------------- */

typedef enum {
    TDC_OK                 = 0,
    TDC_E_INVAL            = 1,  /* invalid argument */
    TDC_E_NOMEM            = 2,
    TDC_E_UNSUPPORTED      = 3,  /* model/transform/entropy id not registered */
    TDC_E_DTYPE            = 4,  /* dtype not accepted by this stage */
    TDC_E_LAYOUT           = 5,  /* layout not accepted by this stage */
    TDC_E_SHAPE            = 6,  /* shape rank or dims invalid */
    TDC_E_BUF_TOO_SMALL    = 7,
    TDC_E_CORRUPT          = 8,  /* on-disk header / payload failed validation */
    TDC_E_VERSION          = 9,  /* container version not understood */
    TDC_E_IO               = 10
} tdc_status;

/* ----- Data types ---------------------------------------------------------- */
/*
 * v0 is numeric-only. The enum reserves ids for future BYTES/STRING but the
 * decoder must reject them with TDC_E_UNSUPPORTED until that milestone lands.
 */

typedef enum {
    TDC_DT_I8     = 1,
    TDC_DT_I16    = 2,
    TDC_DT_I32    = 3,
    TDC_DT_I64    = 4,
    TDC_DT_U8     = 5,
    TDC_DT_U16    = 6,
    TDC_DT_U32    = 7,
    TDC_DT_U64    = 8,
    TDC_DT_F32    = 9,
    TDC_DT_F64    = 10,
    TDC_DT_F16    = 12,  /* IEEE 754 binary16 (half-float) */
    /* Variable-length string dtype.
     *
     * Storage convention for tdc_block when dtype == TDC_DT_STRING:
     *   data    -> uint8_t *bytes      (concatenated UTF-8/byte heap)
     *   offsets -> uint32_t *offsets   (length n_elems + 1; offsets[i+1] -
     *                                   offsets[i] is the length of string i)
     *   shape.dim[0] = n_elems         (number of strings)
     *   shape.rank   = 1               (strings are always VECTOR_1D in v0)
     *
     * For all other dtypes, offsets MUST be NULL. tdc_block_validate enforces
     * this — a single struct with one optional field, not a union. */
    TDC_DT_STRING = 11
} tdc_dtype;

/* Fixed element width in bytes. Returns 0 for variable-length dtypes
 * (currently TDC_DT_STRING) and unknown ids. */
static inline size_t tdc_dtype_size(tdc_dtype dt) {
    switch (dt) {
        case TDC_DT_I8:  case TDC_DT_U8:                  return 1;
        case TDC_DT_I16: case TDC_DT_U16: case TDC_DT_F16: return 2;
        case TDC_DT_I32: case TDC_DT_U32: case TDC_DT_F32: return 4;
        case TDC_DT_I64: case TDC_DT_U64: case TDC_DT_F64: return 8;
        default: return 0;  /* TDC_DT_STRING -> use offsets[] for sizing */
    }
}

static inline int tdc_dtype_is_variable_length(tdc_dtype dt) {
    return dt == TDC_DT_STRING;
}

static inline int tdc_dtype_is_signed(tdc_dtype dt) {
    return dt == TDC_DT_I8 || dt == TDC_DT_I16 || dt == TDC_DT_I32 || dt == TDC_DT_I64;
}

static inline int tdc_dtype_is_float(tdc_dtype dt) {
    return dt == TDC_DT_F16 || dt == TDC_DT_F32 || dt == TDC_DT_F64;
}

/* ----- Semantic layout ----------------------------------------------------- */
/*
 * Tells the model dispatcher how to traverse the block. Independent of rank.
 *
 *   VECTOR_1D : rank 1.        Used by delta1d, dictionary models.
 *   RASTER_2D : rank 2.        Used by 2D predictors (LEFT/UP/PAETH/PLANE).
 *   STACK_2D  : rank 3.        n_slices x ny x nx; predict in-plane per slice,
 *                              optionally with inter-slice residual. Picks
 *                              model/stack2d.c, not pred3d.c.
 *   VOLUME_3D : rank 3.        True 3D neighborhood. Picks model/pred3d.c.
 *
 * The user (vectra, an image tool, etc.) is responsible for setting the
 * correct layout. tdc never guesses.
 */

typedef enum {
    TDC_LAYOUT_VECTOR_1D = 1,
    TDC_LAYOUT_RASTER_2D = 2,
    TDC_LAYOUT_STACK_2D  = 3,
    TDC_LAYOUT_VOLUME_3D = 4
} tdc_layout;

/* ----- Shape --------------------------------------------------------------- */
/*
 * Fixed-size shape descriptor. Rank <= 3 covers everything in v0.
 * Strides are in elements (not bytes), row-major default.
 * For rank R, only dim[0..R-1] and stride[0..R-1] are meaningful; trailing
 * entries must be set to 1 / 0 respectively for forward compatibility.
 */

#define TDC_MAX_RANK 3

typedef struct {
    uint8_t  rank;
    int64_t  dim[TDC_MAX_RANK];
    int64_t  stride[TDC_MAX_RANK];   /* in elements */
} tdc_shape;

static inline int64_t tdc_shape_n_elems(const tdc_shape *s) {
    int64_t n = 1;
    for (uint8_t i = 0; i < s->rank; ++i) n *= s->dim[i];
    return n;
}

/* Fill row-major contiguous strides for a given shape. */
void tdc_shape_set_contiguous(tdc_shape *s);

/* ----- Block --------------------------------------------------------------- */
/*
 * The universal data carrier.
 *
 * MINIMAL BY DESIGN: this struct describes memory and nothing else. No
 * compression hints, no codec ids, no scratch fields. If you find yourself
 * wanting to add a field that is meaningful only during encoding, it belongs
 * in tdc_pipeline_t or in a per-model params struct, not here.
 *
 * Ownership: the block does NOT own data or validity. Both pointers belong
 * to the caller for the duration of the encode/decode call. tdc never frees
 * them and never retains them past return.
 */

typedef struct {
    void           *data;       /* fixed-width: element buffer.
                                 * TDC_DT_STRING: pointer to packed byte heap. */
    uint32_t       *offsets;    /* TDC_DT_STRING only: length n_elems + 1.
                                 * MUST be NULL for all other dtypes.
                                 * Validated by tdc_block_validate. */
    tdc_dtype       dtype;
    tdc_layout      layout;
    tdc_shape       shape;
    const uint8_t  *validity;   /* optional NA bitmap, 1 bit per element, may be NULL */
} tdc_block;

/* Validate that (rank, layout, dtype, dims) form a coherent block. */
tdc_status tdc_block_validate(const tdc_block *blk);

/* ----- Output buffer ------------------------------------------------------- */
/*
 * Caller-owned growable byte buffer used by encode paths. The library never
 * allocates the initial buffer; the caller passes one in (possibly empty)
 * and tdc grows it via the supplied realloc function.
 *
 * Decoupling allocation lets vectra hand tdc its own arena/scratch buffers
 * without forcing tdc to depend on a particular allocator.
 */

typedef struct {
    uint8_t *data;
    size_t   size;     /* bytes currently used */
    size_t   capacity; /* bytes allocated */
    void *(*realloc_fn)(void *user, void *ptr, size_t new_size);
    void    *user;
} tdc_buffer;

#ifdef __cplusplus
}
#endif
#endif /* TDC_TYPES_H */
