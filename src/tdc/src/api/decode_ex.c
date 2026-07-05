/*
 * src/api/decode_ex.c
 *
 * Implements: tdc_decode_block_ex (declared in tdc/codec.h)
 *
 * Thin wrapper around driver_decode_block_impl (see decode_impl.c). The
 * only difference from tdc_decode_block is that internal scratch buffers
 * are allocated via scratch->realloc_fn instead of libc realloc. This
 * enables zero-copy decode into R-allocated SEXP buffers (or any other
 * caller-managed arena) by letting the caller control the allocator.
 *
 * scratch->data / size / capacity are ignored on entry; only realloc_fn
 * and user are used as the parent template for internal ping-pong
 * buffers. On return (success or error), every byte allocated by the
 * pipeline has been freed via the same realloc_fn(user, ptr, 0).
 */

#ifdef _MSC_VER
#  define _CRT_SECURE_NO_WARNINGS 1
#endif

#include "tdc/codec.h"
#include "tdc/types.h"

#include "driver_internal.h"

#include <stddef.h>
#include <stdint.h>

tdc_status tdc_decode_block_ex(const uint8_t *src, size_t src_size,
                               tdc_block     *dst, tdc_buffer *scratch) {
    if (!scratch)                return TDC_E_INVAL;
    if (!scratch->realloc_fn)    return TDC_E_INVAL;
    return driver_decode_block_impl(src, src_size, dst, scratch, "decex",
                                    NULL, NULL, 1);
}
