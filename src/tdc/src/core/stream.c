/*
 * src/core/stream.c
 *
 * Bit and byte stream readers/writers used by entropy coders and the
 * block-record (de)serializer. Little-endian fixed.
 *
 * Replaces ad-hoc shifting in vectra/src/vtr_codec.c.
 */

#include "tdc/types.h"
