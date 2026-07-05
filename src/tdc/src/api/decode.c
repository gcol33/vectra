/*
 * src/api/decode.c
 *
 * Implements: tdc_decode_block (declared in tdc/codec.h)
 *
 * Thin wrapper around driver_decode_block_impl (see decode_impl.c) which
 * holds the one true copy of the block-decode pipeline shared by this
 * function, tdc_decode_block_ex, and tdc_decode_block_into.
 *
 * This variant has no tdc_buffer argument and therefore no caller-supplied
 * realloc_fn. It wraps the C runtime allocator in a tdc_buffer so the
 * shared pipeline can stay allocator-agnostic. Lifting this requires a
 * (frozen) signature change to tdc_decode_block — tracked in PORTING.md.
 */

#ifdef _MSC_VER
#  define _CRT_SECURE_NO_WARNINGS 1
#endif

#include "tdc/codec.h"
#include "tdc/types.h"

#include "driver_internal.h"

#include <stddef.h>
#include <stdint.h>

tdc_status tdc_decode_block(const uint8_t *src, size_t src_size,
                            tdc_block     *dst) {
    tdc_buffer scratch = driver_make_libc_scratch_parent();
    return driver_decode_block_impl(src, src_size, dst, &scratch, "dec  ",
                                    NULL, NULL, 1);
}
