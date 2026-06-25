/*
 * src/core/arena.c
 *
 * Bump-pointer scratch allocator. All logic is in the static inline
 * header src/core/arena.h. This translation unit exists so the
 * CMakeLists.txt source list stays in sync with the design tree and
 * so arena.h is guaranteed at least one compilation-unit check.
 */

#include "arena.h"

/* Force a compilation check of every inline in arena.h.
 * No exported symbols — all functions are static inline in the header. */
