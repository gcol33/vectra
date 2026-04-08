/*
 * src/core/arena.c
 *
 * Bump-pointer scratch allocator. Used by encode/decode paths to avoid
 * malloc/free churn for per-block residual buffers, dictionary tables,
 * tile coefficient arrays, etc.
 *
 * Replaces the per-call malloc pattern in vectra/src/vtr_codec.c.
 *
 * The arena is owned by the caller (vectra hands tdc its own scratch
 * region). tdc never frees arena memory; the caller resets the arena
 * between blocks.
 */

#include "tdc/types.h"
