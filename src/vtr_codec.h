#ifndef VECTRA_VTR_CODEC_H
#define VECTRA_VTR_CODEC_H

/*
 * vtr_codec.h — Columnar encoding and compression for .vtr format v4+
 *
 * Encoding layer (applied per column per row group before compression):
 *   PLAIN      — raw bytes, no transformation
 *   DICTIONARY — for string columns with < 50% unique values
 *   DELTA      — for int64 columns with monotonically increasing values
 *
 * Compression layer (applied after encoding):
 *   NONE       — no compression
 *   LZ_VTR     — custom LZ77 byte compressor (no external dependencies)
 *
 * No external compression libraries. The LZ compressor is ~120 lines of C,
 * purpose-built for the post-encoding residual (small integers, short strings).
 */

#include "types.h"
#include <stdint.h>
#include <stddef.h>

/* Encoding tags (1 byte on disk) */
#define VTR_ENC_PLAIN      0x00
#define VTR_ENC_DICTIONARY 0x01
#define VTR_ENC_DELTA      0x02

/* Compression tags (1 byte on disk) */
#define VTR_COMP_NONE      0x00
#define VTR_COMP_LZ_VTR    0x03

/* Encoded column buffer (intermediate representation between encode and compress) */
typedef struct {
    uint8_t  encoding;        /* VTR_ENC_* */
    uint8_t  compression;     /* VTR_COMP_* */
    uint8_t *data;            /* encoded (and possibly compressed) bytes */
    uint32_t data_size;       /* size of data[] */
    uint32_t uncompressed_size; /* size before compression (== data_size if NONE) */
} VtrEncodedCol;

/*
 * Encode + compress a single column's data payload (not validity bitmap).
 * The caller writes the validity bitmap separately.
 *
 * The function chooses the best encoding automatically:
 *   - VEC_STRING: DICTIONARY if n_unique / n_rows < 0.5, else PLAIN
 *   - VEC_INT64:  DELTA if monotonically increasing, else PLAIN
 *   - VEC_DOUBLE/VEC_BOOL: always PLAIN
 *
 * Then compresses with a built-in LZ77 compressor if the encoded size
 * > 64 bytes (no point compressing tiny buffers). Zero external deps.
 *
 * Returns a VtrEncodedCol. Caller must free .data with free().
 */
VtrEncodedCol vtr_encode_column(const VecArray *col, int64_t n_rows);

/*
 * Decode + decompress a column chunk read from a v4 file.
 *
 * Reads the encoded bytes and populates the VecArray's data fields
 * (buf.i64, buf.dbl, buf.bln, or buf.str.*).
 *
 * The VecArray must already have:
 *   - type set
 *   - length set to n_rows
 *   - validity bitmap already read (handled by caller)
 *
 * For PLAIN encoding, this allocates and fills the data buffers.
 * For DICTIONARY/DELTA, this decodes back to the original representation.
 */
void vtr_decode_column(VecArray *col, int64_t n_rows,
                       uint8_t encoding, uint8_t compression,
                       const uint8_t *data, uint32_t data_size,
                       uint32_t uncompressed_size);

#endif /* VECTRA_VTR_CODEC_H */


