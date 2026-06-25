/*
 * src/entropy/fse_internal.h
 *
 * FSE internals exposed for in-tree callers that need to drive the
 * rANS state machine directly (currently lz_streams.c's fused-FSE
 * fast path). Not part of the public ABI.
 *
 * The generic entropy vtable path (tdc_entropy_fse_vt.decode) remains
 * the only externally supported entry; this header exists so lz_streams
 * can avoid the byte-array round-trip when all three LZ code streams
 * use FSE.
 */

#ifndef TDC_ENTROPY_FSE_INTERNAL_H
#define TDC_ENTROPY_FSE_INTERNAL_H

#include <stdint.h>
#include <stddef.h>
#include "tdc/error.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TDC_FSE_NSYMS          256
#define TDC_FSE_TABLE_LOG_MIN  5u
#define TDC_FSE_TABLE_LOG_MAX  12u
#define TDC_FSE_TABLE_SIZE_MAX (1u << TDC_FSE_TABLE_LOG_MAX)
#define TDC_FSE_L_MIN          (1u << 16)
#define TDC_FSE_HDR_PREFIX     16u

typedef struct {
    uint16_t freq;
    uint16_t cum;
    uint8_t  symbol;
    uint8_t  pad[3];
} tdc_fse_fat_decode_entry;

typedef struct {
    tdc_fse_fat_decode_entry entries[TDC_FSE_TABLE_SIZE_MAX];
} tdc_fse_fat_decode_table;

/* Build the fat decode table from a normalized PMF. Returns TDC_E_CORRUPT
 * if sum(norm) != table_size. */
tdc_status tdc_fse_build_fat_table(const uint16_t *norm, uint32_t n_syms,
                                   uint32_t table_size,
                                   tdc_fse_fat_decode_table *ft);

#ifdef __cplusplus
}
#endif

#endif /* TDC_ENTROPY_FSE_INTERNAL_H */
