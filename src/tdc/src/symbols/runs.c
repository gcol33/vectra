/*
 * src/symbols/runs.c
 *
 * Run-length encoding helpers, used by model/dict.c to compress
 * dictionary index streams that have long runs of repeated values.
 *
 * On-wire format per run: varint(value) + varint(count - 1).
 *
 * Like residuals.c, this is a helper file — not a vtable stage.
 */

#include "symbols_internal.h"

#include <stddef.h>
#include <stdint.h>

size_t tdc_rle_encoded_size(const uint32_t *in, size_t n) {
    if (n == 0) return 0;
    size_t total = 0;
    size_t i = 0;
    while (i < n) {
        uint32_t val = in[i];
        size_t count = 1;
        while (i + count < n && in[i + count] == val) count++;
        total += tdc_varint_size((uint64_t)val);
        total += tdc_varint_size((uint64_t)(count - 1));
        i += count;
    }
    return total;
}

size_t tdc_rle_encode(const uint32_t *in, size_t n,
                      uint8_t *out, size_t out_cap) {
    if (n == 0) return 0;
    size_t pos = 0;
    size_t i = 0;
    while (i < n) {
        uint32_t val = in[i];
        size_t count = 1;
        while (i + count < n && in[i + count] == val) count++;

        size_t need = tdc_varint_size((uint64_t)val) +
                      tdc_varint_size((uint64_t)(count - 1));
        if (pos + need > out_cap) return 0;

        pos += tdc_varint_encode((uint64_t)val, out + pos);
        pos += tdc_varint_encode((uint64_t)(count - 1), out + pos);
        i += count;
    }
    return pos;
}

size_t tdc_rle_decode(const uint8_t *in, size_t in_size,
                      uint32_t *out, size_t out_n) {
    size_t in_pos = 0;
    size_t out_pos = 0;

    while (in_pos < in_size && out_pos < out_n) {
        uint64_t val64, count_m1;
        size_t consumed;

        consumed = tdc_varint_decode(in + in_pos, in_size - in_pos, &val64);
        if (consumed == 0) return 0;
        in_pos += consumed;

        consumed = tdc_varint_decode(in + in_pos, in_size - in_pos, &count_m1);
        if (consumed == 0) return 0;
        in_pos += consumed;

        size_t count = (size_t)(count_m1 + 1);
        if (out_pos + count > out_n) return 0;

        uint32_t val = (uint32_t)val64;
        for (size_t j = 0; j < count; ++j)
            out[out_pos++] = val;
    }

    return in_pos;
}
