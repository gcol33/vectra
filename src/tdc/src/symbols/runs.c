/*
 * src/symbols/runs.c
 *
 * Run-length encoding helpers, used by model/dict.c to compress
 * dictionary index streams that have long runs of repeated values.
 *
 * Source today: RLE loop in the dictionary path of vectra/src/vtr_codec.c.
 *
 * Like residuals.c, this is a helper file — not a vtable stage.
 */

#include "tdc/types.h"
