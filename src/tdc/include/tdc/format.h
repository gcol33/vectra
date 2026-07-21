/*
 * tdc/format.h — frozen v0
 *
 * On-disk format: container header + per-block record.
 *
 * Design rules (DO NOT VIOLATE without bumping format version):
 *   1. Aggressive versioning. The container header carries a u16 version
 *      and a u32 magic. The per-block record carries its OWN u16 version
 *      so future block format changes don't require rewriting the container.
 *   2. Block records are SELF-DESCRIBING. A block record contains every
 *      field needed to decode it without consulting the container header
 *      or any external metadata: model id, full transform chain (with
 *      per-stage params), entropy id, dtype, layout, rank, shape,
 *      uncompressed size, all section sizes. Yes this duplicates
 *      container-level info — that is intentional. Self-describing blocks
 *      survive being copied, concatenated, sharded, or extracted from a
 *      corrupted container.
 *   3. Side metadata is FIRST-CLASS. The block record has up to five
 *      sections:
 *          [ tdc_block_record header ]
 *          [ side metadata bytes     ]   (model params: plane coeffs, dict, ...)
 *          [ xform params bytes      ]   (TLV: per-slot transform params)
 *          [ entropy payload bytes   ]   (the actual compressed residuals)
 *          [ validity bitmap bytes   ]   (optional NA mask, opaque to v0)
 *      All side data is part of the block, not an afterthought hanging off
 *      a separate index.
 *   4. Fixed little-endian on disk. Documented here and in types.h.
 *   5. Fixed-size header. tdc_block_record is exactly 80 bytes so it can
 *      be read with a single mmap/pread without loops or growth logic.
 *      Adding fields requires version bump to v3.
 *
 * v0->v2 changes (one atomic bump, never shipped a v0 reader):
 *   - +xform_params_size (u32):  TLV section between side_meta and payload
 *   - +validity_size (u32):      explicit byte count for the trailing bitmap
 *   - +_reserved1 (u64):         padding for the next field, future use
 *   - struct grows from 64 to 80 bytes.
 */

#ifndef TDC_FORMAT_H
#define TDC_FORMAT_H

#include "types.h"
#include "codec.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ----- Magic numbers and versions ----------------------------------------- */

#define TDC_CONTAINER_MAGIC   0x31434454u   /* 'TDC1' little-endian */
#define TDC_BLOCK_MAGIC       0x424B4C42u   /* 'BLKB' little-endian */

#define TDC_CONTAINER_VERSION 1

/*
 * Version stamped on a container whose schema has been RELOCATED to the
 * tail, which is how a heterogeneous container gains columns without its
 * body being rewritten (tdc_stream_encoder_open_widen).
 *
 * The schema section of a v1 container sits at offset 64, immediately
 * before the first block record, so it cannot grow in place. A widen pass
 * therefore appends the new column blocks and a full replacement schema
 * past the end of the existing data and points the header at them.
 *
 * This is a deliberate one-way version bump: a v1-only reader must REFUSE
 * a widened container rather than read the stale schema still sitting at
 * offset 64. Containers that are never widened stay v1 and are unchanged
 * byte-for-byte.
 */
#define TDC_CONTAINER_VERSION_WIDENED 2

#define TDC_BLOCK_VERSION     2

/* ----- Container header --------------------------------------------------- */
/*
 * Written once at the start of a container file. The container is a sequence
 * of blocks followed by a trailing index. The header carries the GLOBAL
 * shape/dtype/layout — these may be NULL/zero for "heterogeneous" containers
 * where each block is independent (vectra row groups).
 *
 * Layout on disk (all little-endian).
 *
 * The field order matches natural C alignment so the struct compiles to
 * exactly 64 bytes on every supported target (x86_64, aarch64) without
 * #pragma pack — schema_size sits in the natural 4-byte gap before
 * global_dim[3] so the int64 array starts at an 8-aligned offset.
 *
 *   offset  size  field
 *   ------  ----  -----
 *        0     4  magic            = TDC_CONTAINER_MAGIC ('TDC1')
 *        4     2  version          = TDC_CONTAINER_VERSION
 *        6     2  flags
 *        8     8  n_blocks
 *       16     8  index_offset     (file offset of trailing block-index table)
 *       24     8  index_size       (bytes)
 *       32     1  global_dtype     (tdc_dtype, 0 if heterogeneous)
 *       33     1  global_layout    (tdc_layout, 0 if heterogeneous)
 *       34     1  global_rank      (0..3, 0 if heterogeneous)
 *       35     1  _reserved0
 *       36     4  schema_size      (bytes of the serialized schema section;
 *                                   0 = none)
 *       40    24  u                (union, discriminated by the
 *                                   HETEROGENEOUS flag -- see below)
 *       --   ---  total = 64 bytes
 *
 * The last 24 bytes mean different things for the two container kinds,
 * and the two meanings can never coexist:
 *
 *   homogeneous   (HETEROGENEOUS clear): u.global_dim[3], the global shape.
 *   heterogeneous (HETEROGENEOUS set):   u.het, which is all-zero for a v1
 *                                        container (the shape fields are
 *                                        required zero there) and carries
 *                                        the relocated-schema pointers for
 *                                        a v2 (widened) container.
 *
 * u.het.schema_offset is the absolute file offset of the schema section
 * and u.het.blocks_start the absolute offset of the first block record.
 * Both are 0 in a v1 container, where they are implied: the schema sits at
 * TDC_CONTAINER_HEADER_SIZE and the blocks at header + schema_size.
 */

#define TDC_CONTAINER_HEADER_SIZE 64

#define TDC_CONTAINER_FLAG_HETEROGENEOUS 0x0001u  /* per-block dtype/layout */
#define TDC_CONTAINER_FLAG_HAS_STATS     0x0002u  /* >=1 row group carries
                                                     per-column min/max stats
                                                     in the trailing index */

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t flags;
    uint64_t n_blocks;
    uint64_t index_offset;
    uint64_t index_size;
    uint8_t  global_dtype;
    uint8_t  global_layout;
    uint8_t  global_rank;
    uint8_t  _reserved0;
    uint32_t schema_size;
    union {
        /* Homogeneous containers: the global shape. */
        int64_t global_dim[TDC_MAX_RANK];
        /* Heterogeneous containers: where the schema and the blocks live.
         * Both zero unless the container has been widened (v2). */
        struct {
            uint64_t schema_offset;
            uint64_t blocks_start;
            uint64_t _reserved1;
        } het;
    } u;
} tdc_container_header;

/* ----- Per-block record header -------------------------------------------- */
/*
 * Written immediately before each block's side data + payload.
 *
 * SELF-DESCRIBING by design. Even if the container header is missing or
 * corrupted, a block record carries enough information to decode the block
 * standalone. This makes blocks safe to:
 *   - copy between containers without reformatting
 *   - extract individually with `dd` for debugging
 *   - concatenate from multiple producers
 *
 * Layout on disk (all little-endian):
 *
 *   offset  size  field
 *   ------  ----  -----
 *        0     4  magic                 = TDC_BLOCK_MAGIC ('BLKB')
 *        4     2  version               = TDC_BLOCK_VERSION
 *        6     2  flags
 *        8     2  model_id              (tdc_model_id)
 *       10     8  xform_ids[4]          (tdc_xform_id, 0 = chain end)
 *       18     8  entropy_ids[4]        (tdc_entropy_id, 0 = chain end)
 *       26     1  dtype                 (tdc_dtype)
 *       27     1  layout                (tdc_layout)
 *       28     1  rank                  (1..3)
 *       29     1  _reserved0
 *       30     2  _reserved_pad         (padding for 8-align of dim[3])
 *       32    24  dim[3]                (int64)
 *       56     8  uncompressed_size     (residual bytes BEFORE entropy stage)
 *       64     4  side_meta_size        (model side metadata)
 *       68     4  payload_size          (entropy-compressed payload bytes)
 *       72     4  xform_params_size     (TLV section size; 0 if none)
 *       76     4  validity_size         (validity bitmap byte count; 0 if none)
 *       --   ---  total = 80 bytes
 *
 * Section order on disk (each immediately follows the previous):
 *       0                                                        header        (80)
 *       80                                                       side_meta     (side_meta_size)
 *       80 + side_meta_size                                      xform_params  (xform_params_size)
 *       80 + side_meta_size + xform_params_size                  payload       (payload_size)
 *       80 + side_meta_size + xform_params_size + payload_size   validity      (validity_size)
 *
 * xform_params_size is the byte count of the TLV section that carries
 * per-slot transform parameters. Layout of the TLV section:
 *
 *     repeat until xform_params_size bytes consumed:
 *         u16  slot_index   (0..TDC_MAX_TRANSFORMS-1)
 *         u16  blob_length
 *         blob_length bytes (transform-specific payload, little-endian)
 *
 * Slots without on-disk parameters (e.g. ZIGZAG, BYTE_SHUFFLE) are simply
 * absent from the TLV stream. Slot order in the stream is unconstrained;
 * the decoder gathers entries into a per-slot pointer table by slot_index.
 *
 * validity_size, when nonzero, is exactly ceil(n_elems / 8) and the
 * bitmap is laid out 1 bit per element, LSB-first within each byte. v0
 * The current decoder treats the bitmap as opaque pass-through and does
 * not surface it back to the caller; the field exists so the on-disk
 * record is honest about the bytes it carries and a future API extension
 * can hand them back.
 */

#define TDC_BLOCK_HEADER_SIZE 80

#define TDC_BLOCK_FLAG_HAS_VALIDITY  0x0001u  /* validity bitmap follows payload */
#define TDC_BLOCK_FLAG_LOSSY         0x0002u  /* set if any transform was lossy */
#define TDC_BLOCK_FLAG_ZERO_RESIDUAL 0x0004u  /* model emitted an all-zero residual;
                                                 xform + entropy chains are skipped,
                                                 payload_size and xform_params_size
                                                 are 0, and the decoder reconstructs
                                                 a zero-filled residual of size
                                                 uncompressed_size before handing
                                                 to the model. Used as a whole-
                                                 pipeline fast path for cases
                                                 where the model fits the data
                                                 exactly. */

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t flags;
    uint16_t model_id;
    uint16_t xform_ids[TDC_MAX_TRANSFORMS];
    uint16_t entropy_ids[TDC_MAX_ENTROPY];
    uint8_t  dtype;
    uint8_t  layout;
    uint8_t  rank;
    uint8_t  _reserved0;
    uint16_t _reserved_pad;
    int64_t  dim[TDC_MAX_RANK];
    uint64_t uncompressed_size;
    uint32_t side_meta_size;
    uint32_t payload_size;
    uint32_t xform_params_size;
    uint32_t validity_size;
} tdc_block_record;

/* Compile-time assertions: the on-disk layout must match the struct layout. */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(tdc_container_header) == TDC_CONTAINER_HEADER_SIZE,
               "tdc_container_header must be 64 bytes");
_Static_assert(sizeof(tdc_block_record) == TDC_BLOCK_HEADER_SIZE,
               "tdc_block_record must be 80 bytes");
#endif

/* ----- Trailing block index ----------------------------------------------- */
/*
 * Written at index_offset, length index_size, contiguous array of:
 *
 *   offset  size  field
 *   ------  ----  -----
 *        0     8  block_offset   (absolute file offset of tdc_block_record)
 *        8     8  block_total    (header + side_meta + xform_params + payload + validity)
 *       --   ---  total = 16 bytes per entry
 */

#define TDC_INDEX_ENTRY_SIZE 16

typedef struct {
    uint64_t block_offset;
    uint64_t block_total;
} tdc_index_entry_v1;

#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(tdc_index_entry_v1) == TDC_INDEX_ENTRY_SIZE,
               "tdc_index_entry_v1 must be 16 bytes");
#endif

/* ----- Validation --------------------------------------------------------- */
/*
 * Cheap structural validators. Do NOT decompress; only check magic, version,
 * size relationships, and id ranges.
 */

tdc_status tdc_container_header_validate(const tdc_container_header *h);
tdc_status tdc_block_record_validate(const tdc_block_record *r);

#ifdef __cplusplus
}
#endif
#endif /* TDC_FORMAT_H */
