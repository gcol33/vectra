/*
 * src/transform/bitshuffle.c
 *
 * TDC_XFORM_BIT_SHUFFLE — transpose by bit lane instead of byte lane.
 *
 * Reserved post-v0. Most useful when combined with quantization to a
 * narrow integer type, where the high-order bits are mostly zero and
 * bit-shuffling makes them collapse into long runs of 0s.
 *
 * Vtable not registered until implementation lands.
 */

#include "tdc/transform.h"
