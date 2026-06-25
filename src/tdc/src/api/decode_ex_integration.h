/*
 * src/api/decode_ex_integration.h
 *
 * Integration guide for wiring tdc_decode_block_ex into the public API.
 *
 * This file is NOT included by any .c file automatically. It exists as a
 * reference for the manual integration step: making tdc_decode_block a
 * thin wrapper around tdc_decode_block_ex.
 */

#ifndef TDC_API_DECODE_EX_INTEGRATION_H
#define TDC_API_DECODE_EX_INTEGRATION_H

#include "tdc/types.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -----------------------------------------------------------------------
 * 1. Declaration to add to include/tdc/codec.h (after tdc_decode_block):
 * -----------------------------------------------------------------------
 *
 *   / *
 *    * Decode a single block record using a caller-supplied allocator.
 *    * Identical to tdc_decode_block, but internal scratch buffers are
 *    * allocated via scratch->realloc_fn instead of libc realloc.
 *    *
 *    * The caller must set scratch->realloc_fn before calling. The
 *    * scratch->data / size / capacity fields are ignored on entry and
 *    * are NOT used as a pre-allocated buffer — they serve only as the
 *    * parent template for internal ping-pong buffers.
 *    *
 *    * On return (success or error), all scratch memory allocated by the
 *    * function has been freed via the same realloc_fn(user, ptr, 0).
 *    * /
 *   tdc_status tdc_decode_block_ex(const uint8_t *src, size_t src_size,
 *                                  tdc_block *dst, tdc_buffer *scratch);
 */

/* Forward declaration (usable from decode.c or any other translation
 * unit that needs to call the _ex variant). */
tdc_status tdc_decode_block_ex(const uint8_t *src, size_t src_size,
                               tdc_block *dst, tdc_buffer *scratch);

/* -----------------------------------------------------------------------
 * 2. How to refactor src/api/decode.c into a wrapper:
 * -----------------------------------------------------------------------
 *
 * After adding decode_ex.c to the build and confirming tests pass:
 *
 *   a. In decode.c, remove everything from "Entry point" to the end of
 *      the function and replace with:
 *
 *          tdc_status tdc_decode_block(const uint8_t *src, size_t src_size,
 *                                      tdc_block     *dst) {
 *              tdc_buffer scratch = driver_make_scratch_parent();
 *              return tdc_decode_block_ex(src, src_size, dst, &scratch);
 *          }
 *
 *   b. Add an #include for this header (or just declare the extern):
 *
 *          #include "decode_ex_integration.h"
 *
 *   c. The driver_libc_realloc and driver_make_scratch_parent statics
 *      stay in decode.c (they are still needed to construct the libc
 *      scratch parent). Alternatively, move them to driver_internal.h
 *      so both files share the definition.
 *
 *   d. The stage timer statics in decode.c can be removed (they are
 *      duplicated in decode_ex.c). If you want a single copy, move
 *      them to a shared internal header.
 *
 *   e. Run ctest to confirm all existing tests still pass.
 */

#ifdef __cplusplus
}
#endif

#endif /* TDC_API_DECODE_EX_INTEGRATION_H */
