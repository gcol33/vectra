/*
 * src/core/stream.c
 *
 * Bit and byte stream readers/writers. All logic is in the static inline
 * header src/core/stream.h. This translation unit exists so the
 * CMakeLists.txt source list stays in sync with the design tree and
 * so stream.h is guaranteed at least one compilation-unit check.
 */

#include "stream.h"

/* Force a compilation check of every inline in stream.h.
 * No exported symbols — all functions are static inline in the header. */
