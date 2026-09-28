#include "parquet_column.h"
#include "parquet_meta.h"
#include "array.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <math.h>

/* Values are little-endian on disk; vectra (like tdc) assumes a little-endian
   host, so fixed-width values load with memcpy. */

enum { PQV_NONE = 0, PQV_RAW, PQV_U8, PQV_I64, PQV_STR, PQV_IDX };

static void vals_free(PqVals *v) {
    free(v->owned);
    free(v->u8);
    free(v->i64);
    free(v->sp);
    free(v->sl);
    free(v->sbuf);
    free(v->idx);
    memset(v, 0, sizeof(*v));
}

static int fail(PqCursor *c, const char *fmt, ...) {
    int n = snprintf(c->err, sizeof(c->err), "column '%s': ", c->leaf->name);
    if (n < 0 || n >= (int)sizeof(c->err)) n = 0;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(c->err + n, sizeof(c->err) - (size_t)n, fmt, ap);
    va_end(ap);
    return -1;
}

/* ------------------------------------------------------------------ */
/*  Bit reading                                                        */
/* ------------------------------------------------------------------ */

/* LSB-first bit reader bounded by `end`. */
typedef struct {
    const uint8_t *p, *end;
    uint64_t       acc;
    int            nbits;
} PqBits;

static int bits_get(PqBits *b, int w, uint64_t *out) {
    if (w == 0) { *out = 0; return 0; }
    if (w > 56) {
        uint64_t lo, hi;
        if (bits_get(b, 32, &lo) || bits_get(b, w - 32, &hi)) return -1;
        *out = lo | (hi << 32);
        return 0;
    }
    while (b->nbits < w) {
        if (b->p >= b->end) return -1;
        b->acc |= (uint64_t)*b->p++ << b->nbits;
        b->nbits += 8;
    }
    *out = b->acc & ((1ULL << w) - 1);
    b->acc >>= w;
    b->nbits -= w;
    return 0;
}

static int read_uvarint(const uint8_t **pp, const uint8_t *end, uint64_t *out) {
    const uint8_t *p = *pp;
    uint64_t r = 0;
    int shift = 0;
    for (;;) {
        if (p >= end || shift > 63) return -1;
        uint8_t b = *p++;
        r |= (uint64_t)(b & 0x7f) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    *pp = p;
    *out = r;
    return 0;
}

static int read_zigzag(const uint8_t **pp, const uint8_t *end, int64_t *out) {
    uint64_t v;
    if (read_uvarint(pp, end, &v)) return -1;
    *out = (int64_t)(v >> 1) ^ -(int64_t)(v & 1);
    return 0;
}

static int bit_width_of(uint32_t max) {
    int w = 0;
    while (max) { w++; max >>= 1; }
    return w;
}

/* RLE / bit-packed hybrid (the levels and dictionary-index encoding). */
static int rle_decode(const uint8_t *p, size_t n, int bw,
                      uint32_t *out, int64_t count) {
    const uint8_t *end = p + n;
    int64_t got = 0;
    if (bw < 0 || bw > 32) return -1;
    while (got < count) {
        uint64_t h;
        if (read_uvarint(&p, end, &h)) return -1;
        if (h & 1) {
            uint64_t groups = h >> 1;
            if (groups > ((uint64_t)1 << 40)) return -1;
            uint64_t nv = groups * 8, nbytes = groups * (uint64_t)bw;
            const uint8_t *lim =
                nbytes <= (uint64_t)(end - p) ? p + nbytes : end;
            PqBits br = { p, lim, 0, 0 };
            uint64_t want = (uint64_t)(count - got);
            int64_t take = (int64_t)(want < nv ? want : nv);
            for (int64_t j = 0; j < take; j++) {
                uint64_t v;
                if (bits_get(&br, bw, &v)) return -1;
                out[got++] = (uint32_t)v;
            }
            p = lim;
        } else {
            uint64_t run = h >> 1;
            int vb = (bw + 7) / 8;
            if ((size_t)(end - p) < (size_t)vb) return -1;
            uint32_t v = 0;
            for (int k = 0; k < vb; k++) v |= (uint32_t)p[k] << (8 * k);
            p += vb;
            uint64_t want = (uint64_t)(count - got);
            int64_t take = (int64_t)(want < run ? want : run);
            for (int64_t j = 0; j < take; j++) out[got++] = v;
        }
    }
    return 0;
}

/* Deprecated BIT_PACKED level encoding: MSB-first, no run headers. Returns
   the bytes consumed or -1. */
static int64_t bitpacked_msb_decode(const uint8_t *p, size_t n, int bw,
                                    uint32_t *out, int64_t count) {
    uint64_t nbits = (uint64_t)count * (uint64_t)bw;
    uint64_t nbytes = (nbits + 7) / 8;
    if (nbytes > n) return -1;
    uint64_t bit = 0;
    for (int64_t i = 0; i < count; i++) {
        uint32_t v = 0;
        for (int k = 0; k < bw; k++, bit++)
            v = (v << 1) | ((p[bit >> 3] >> (7 - (bit & 7))) & 1u);
        out[i] = v;
    }
    return (int64_t)nbytes;
}

/* DELTA_BINARY_PACKED: decodes exactly `count` values. Returns the bytes
   consumed or -1. */
static int64_t dbp_decode(const uint8_t *start, size_t n, int64_t *out,
                          int64_t count) {
    const uint8_t *p = start, *end = start + n;
    uint64_t block, mb, total;
    int64_t first;
    if (read_uvarint(&p, end, &block) || read_uvarint(&p, end, &mb) ||
        read_uvarint(&p, end, &total) || read_zigzag(&p, end, &first))
        return -1;
    if (total != (uint64_t)count) return -1;
    if (count == 0) return (int64_t)(p - start);
    if (block == 0 || mb == 0 || block > (1u << 20) || block % mb ||
        (block / mb) % 8)
        return -1;
    uint64_t vpm = block / mb;
    uint64_t last = (uint64_t)first;
    out[0] = first;
    int64_t got = 1;
    while (got < count) {
        int64_t min_delta;
        if (read_zigzag(&p, end, &min_delta)) return -1;
        if ((uint64_t)(end - p) < mb) return -1;
        const uint8_t *widths = p;
        p += mb;
        for (uint64_t m = 0; m < mb && got < count; m++) {
            int w = widths[m];
            if (w > 64) return -1;
            uint64_t mbytes = vpm * (uint64_t)w / 8;
            if (mbytes > (uint64_t)(end - p)) return -1;
            PqBits br = { p, p + mbytes, 0, 0 };
            uint64_t want = (uint64_t)(count - got);
            int64_t take = (int64_t)(want < vpm ? want : vpm);
            for (int64_t j = 0; j < take; j++) {
                uint64_t v;
                if (bits_get(&br, w, &v)) return -1;
                last += (uint64_t)min_delta + v;
                out[got++] = (int64_t)last;
            }
            p += mbytes;
        }
    }
    return (int64_t)(p - start);
}

/* ------------------------------------------------------------------ */
/*  Value decoding                                                     */
/* ------------------------------------------------------------------ */

static int phys_width(const PqLeaf *L) {
    switch (L->phys) {
    case PQ_INT32: case PQ_FLOAT:  return 4;
    case PQ_INT64: case PQ_DOUBLE: return 8;
    case PQ_INT96:                 return 12;
    case PQ_FIXED_LEN_BYTE_ARRAY:  return L->type_length > 0 ? L->type_length : -1;
    default:                       return -1;
    }
}

/* Byte-array values back to back, each with a 4-byte length prefix. */
static int plain_byte_array(const uint8_t *p, size_t n, int64_t nvals,
                            PqVals *v) {
    const uint8_t *end = p + n;
    v->sp = (const uint8_t **)malloc((size_t)(nvals ? nvals : 1) * sizeof(*v->sp));
    v->sl = (uint32_t *)malloc((size_t)(nvals ? nvals : 1) * sizeof(*v->sl));
    if (!v->sp || !v->sl) return -1;
    for (int64_t i = 0; i < nvals; i++) {
        if (end - p < 4) return -1;
        uint32_t len;
        memcpy(&len, p, 4);
        p += 4;
        if ((size_t)(end - p) < len) return -1;
        v->sp[i] = p;
        v->sl[i] = len;
        p += len;
    }
    v->kind = PQV_STR;
    return 0;
}

/* Byte arrays whose lengths were decoded separately; `data` holds them back
   to back. */
static int byte_array_from_lengths(const uint8_t *data, size_t n,
                                   const int64_t *lens, int64_t nvals,
                                   PqVals *v) {
    const uint8_t *end = data + n;
    v->sp = (const uint8_t **)malloc((size_t)(nvals ? nvals : 1) * sizeof(*v->sp));
    v->sl = (uint32_t *)malloc((size_t)(nvals ? nvals : 1) * sizeof(*v->sl));
    if (!v->sp || !v->sl) return -1;
    for (int64_t i = 0; i < nvals; i++) {
        if (lens[i] < 0 || (uint64_t)lens[i] > (uint64_t)(end - data)) return -1;
        v->sp[i] = data;
        v->sl[i] = (uint32_t)lens[i];
        data += lens[i];
    }
    v->kind = PQV_STR;
    return 0;
}

/* Decode `nvals` values of the given encoding. Pointers in *v may refer into
   `p`, which must outlive *v. */
static int decode_values(PqCursor *c, int enc, const uint8_t *p, size_t n,
                         int64_t nvals, PqVals *v) {
    const PqLeaf *L = c->leaf;
    memset(v, 0, sizeof(*v));
    v->n = nvals;
    size_t cnt = (size_t)(nvals ? nvals : 1);

    switch (enc) {
    case PQ_ENC_PLAIN:
        if (L->phys == PQ_BOOLEAN) {
            if ((uint64_t)(nvals + 7) / 8 > n)
                return fail(c, "boolean page shorter than its value count");
            v->u8 = (uint8_t *)malloc(cnt);
            if (!v->u8) return fail(c, "out of memory");
            for (int64_t i = 0; i < nvals; i++)
                v->u8[i] = (p[i >> 3] >> (i & 7)) & 1;
            v->kind = PQV_U8;
            return 0;
        }
        if (L->phys == PQ_BYTE_ARRAY) {
            if (plain_byte_array(p, n, nvals, v))
                return fail(c, "malformed PLAIN byte-array page");
            return 0;
        }
        {
            int w = phys_width(L);
            if (w <= 0) return fail(c, "unsupported physical type %d", L->phys);
            if ((uint64_t)nvals > n / (size_t)w)
                return fail(c, "page shorter than its value count");
            v->raw = p;
            v->width = w;
            v->kind = PQV_RAW;
            return 0;
        }

    case PQ_ENC_PLAIN_DICTIONARY:
    case PQ_ENC_RLE_DICTIONARY: {
        if (!c->has_dict)
            return fail(c, "dictionary-encoded page without a dictionary");
        v->idx = (uint32_t *)malloc(cnt * sizeof(uint32_t));
        if (!v->idx) return fail(c, "out of memory");
        if (nvals > 0) {
            if (n < 1) return fail(c, "empty dictionary-index page");
            int bw = p[0];
            if (bw > 32 || rle_decode(p + 1, n - 1, bw, v->idx, nvals))
                return fail(c, "malformed dictionary indices");
            for (int64_t i = 0; i < nvals; i++)
                if ((int64_t)v->idx[i] >= c->dict.n)
                    return fail(c, "dictionary index out of range");
        }
        v->kind = PQV_IDX;
        return 0;
    }

    case PQ_ENC_RLE: {
        if (L->phys != PQ_BOOLEAN)
            return fail(c, "RLE value encoding on a non-boolean column");
        if (n < 4) return fail(c, "truncated RLE boolean page");
        uint32_t len;
        memcpy(&len, p, 4);
        if (len > n - 4) return fail(c, "truncated RLE boolean page");
        uint32_t *tmp = (uint32_t *)malloc(cnt * sizeof(uint32_t));
        v->u8 = (uint8_t *)malloc(cnt);
        if (!tmp || !v->u8) { free(tmp); return fail(c, "out of memory"); }
        int rc = rle_decode(p + 4, len, 1, tmp, nvals);
        for (int64_t i = 0; rc == 0 && i < nvals; i++) v->u8[i] = tmp[i] != 0;
        free(tmp);
        if (rc) return fail(c, "malformed RLE boolean page");
        v->kind = PQV_U8;
        return 0;
    }

    case PQ_ENC_DELTA_BINARY_PACKED:
        if (L->phys != PQ_INT32 && L->phys != PQ_INT64)
            return fail(c, "DELTA_BINARY_PACKED on a non-integer column");
        v->i64 = (int64_t *)malloc(cnt * sizeof(int64_t));
        if (!v->i64) return fail(c, "out of memory");
        if (dbp_decode(p, n, v->i64, nvals) < 0)
            return fail(c, "malformed DELTA_BINARY_PACKED page");
        v->kind = PQV_I64;
        return 0;

    case PQ_ENC_DELTA_LENGTH_BYTE_ARRAY: {
        if (L->phys != PQ_BYTE_ARRAY)
            return fail(c, "DELTA_LENGTH_BYTE_ARRAY on a non-byte-array column");
        int64_t *lens = (int64_t *)malloc(cnt * sizeof(int64_t));
        if (!lens) return fail(c, "out of memory");
        int64_t used = dbp_decode(p, n, lens, nvals);
        int rc = used < 0 ? -1
                 : byte_array_from_lengths(p + used, n - (size_t)used,
                                           lens, nvals, v);
        free(lens);
        if (rc) return fail(c, "malformed DELTA_LENGTH_BYTE_ARRAY page");
        return 0;
    }

    case PQ_ENC_DELTA_BYTE_ARRAY: {
        if (L->phys != PQ_BYTE_ARRAY && L->phys != PQ_FIXED_LEN_BYTE_ARRAY)
            return fail(c, "DELTA_BYTE_ARRAY on a non-byte-array column");
        int64_t *pre = (int64_t *)malloc(cnt * sizeof(int64_t));
        int64_t *suf = (int64_t *)malloc(cnt * sizeof(int64_t));
        PqVals sv;
        memset(&sv, 0, sizeof(sv));
        int ok = pre && suf;
        int64_t u1 = ok ? dbp_decode(p, n, pre, nvals) : -1;
        int64_t u2 = u1 < 0 ? -1 : dbp_decode(p + u1, n - (size_t)u1, suf, nvals);
        ok = u2 >= 0 &&
             byte_array_from_lengths(p + u1 + u2, n - (size_t)(u1 + u2),
                                     suf, nvals, &sv) == 0;
        /* Each value is a prefix of the previous value plus its suffix. */
        uint64_t total = 0, prev = 0;
        for (int64_t i = 0; ok && i < nvals; i++) {
            if (pre[i] < 0 || (uint64_t)pre[i] > prev) { ok = 0; break; }
            prev = (uint64_t)pre[i] + sv.sl[i];
            if (prev > UINT32_MAX) { ok = 0; break; }
            total += prev;
        }
        if (ok && total > ((uint64_t)1 << 40)) ok = 0;
        if (ok) {
            v->sbuf = (uint8_t *)malloc((size_t)(total ? total : 1));
            v->sp = (const uint8_t **)malloc(cnt * sizeof(*v->sp));
            v->sl = (uint32_t *)malloc(cnt * sizeof(*v->sl));
            if (!v->sbuf || !v->sp || !v->sl) ok = 0;
        }
        if (ok) {
            size_t off = 0;
            const uint8_t *prevp = NULL;
            for (int64_t i = 0; i < nvals; i++) {
                uint8_t *dst = v->sbuf + off;
                if (pre[i]) memcpy(dst, prevp, (size_t)pre[i]);
                if (sv.sl[i]) memcpy(dst + pre[i], sv.sp[i], sv.sl[i]);
                v->sp[i] = dst;
                v->sl[i] = (uint32_t)pre[i] + sv.sl[i];
                prevp = dst;
                off += v->sl[i];
            }
            if (L->phys == PQ_FIXED_LEN_BYTE_ARRAY)
                for (int64_t i = 0; i < nvals; i++)
                    if ((int32_t)v->sl[i] != L->type_length) { ok = 0; break; }
        }
        free(pre);
        free(suf);
        vals_free(&sv);
        if (!ok) return fail(c, "malformed DELTA_BYTE_ARRAY page");
        v->kind = PQV_STR;
        return 0;
    }

    case PQ_ENC_BYTE_STREAM_SPLIT: {
        int w = phys_width(L);
        if (w <= 0 || L->phys == PQ_INT96)
            return fail(c, "BYTE_STREAM_SPLIT on an unsupported type");
        if ((uint64_t)nvals > n / (size_t)w)
            return fail(c, "BYTE_STREAM_SPLIT page shorter than its values");
        v->owned = (uint8_t *)malloc(cnt * (size_t)w);
        if (!v->owned) return fail(c, "out of memory");
        for (int k = 0; k < w; k++) {
            const uint8_t *s = p + (size_t)k * (size_t)nvals;
            for (int64_t i = 0; i < nvals; i++)
                v->owned[(size_t)i * (size_t)w + (size_t)k] = s[i];
        }
        v->raw = v->owned;
        v->width = w;
        v->kind = PQV_RAW;
        return 0;
    }

    default:
        return fail(c, "unsupported encoding %d", enc);
    }
}

/* ------------------------------------------------------------------ */
/*  Physical value access and conversion                               */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t        i;
    double         d;
    const uint8_t *b;
    uint32_t       len;
} PqPhys;

static inline void vals_get(const PqCursor *c, const PqVals *v, int64_t i,
                            PqPhys *o) {
    if (v->kind == PQV_IDX) {
        i = v->idx[i];
        v = &c->dict;
    }
    switch (v->kind) {
    case PQV_RAW: {
        const uint8_t *q = v->raw + (size_t)i * (size_t)v->width;
        switch (c->leaf->phys) {
        case PQ_INT32: { int32_t x; memcpy(&x, q, 4); o->i = x; break; }
        case PQ_INT64: { int64_t x; memcpy(&x, q, 8); o->i = x; break; }
        case PQ_FLOAT: { float x;   memcpy(&x, q, 4); o->d = x; break; }
        case PQ_DOUBLE:{ double x;  memcpy(&x, q, 8); o->d = x; break; }
        default: o->b = q; o->len = (uint32_t)v->width; break;
        }
        break;
    }
    case PQV_U8:
        o->i = v->u8[i];
        break;
    case PQV_I64:
        o->i = c->leaf->phys == PQ_INT32 ? (int64_t)(int32_t)v->i64[i]
                                         : v->i64[i];
        break;
    case PQV_STR:
        o->b = v->sp[i];
        o->len = v->sl[i];
        break;
    default:
        o->i = 0;
        break;
    }
}

static double half_to_double(uint16_t h) {
    int sign = h >> 15, exp = (h >> 10) & 0x1f, man = h & 0x3ff;
    double v;
    if (exp == 0) v = ldexp((double)man, -24);
    else if (exp == 31) v = man ? NAN : INFINITY;
    else v = ldexp((double)(man | 0x400), exp - 25);
    return sign ? -v : v;
}

/* Big-endian two's-complement integer of any width, as a double. */
static double be_twos_to_double(const uint8_t *b, uint32_t len) {
    if (len == 0) return 0.0;
    if (len <= 8) {
        uint64_t u = 0;
        for (uint32_t k = 0; k < len; k++) u = (u << 8) | b[k];
        if (len < 8 && (b[0] & 0x80)) u |= ~(uint64_t)0 << (8 * len);
        return (double)(int64_t)u;
    }
    double v = (double)(int8_t)b[0];
    for (uint32_t k = 1; k < len; k++) v = v * 256.0 + (double)b[k];
    return v;
}

static double int96_to_seconds(const uint8_t *b) {
    int64_t nanos;
    int32_t jd;
    memcpy(&nanos, b, 8);
    memcpy(&jd, b + 8, 4);
    return ((double)jd - 2440588.0) * 86400.0 + (double)nanos / 1e9;
}

/* Numeric value of a non-text conversion. */
static double phys_to_double(const PqLeaf *L, const PqPhys *ph) {
    switch (L->conv) {
    case PQC_BOOL:
    case PQC_I32:
    case PQC_I64:       return (double)ph->i;
    case PQC_U32:       return (double)(uint32_t)ph->i;
    case PQC_U64:       return (double)(uint64_t)ph->i;
    case PQC_INT_DIV:   return (double)ph->i / L->div;
    case PQC_INT96:     return int96_to_seconds(ph->b);
    case PQC_FLOAT:
    case PQC_DOUBLE:    return ph->d;
    case PQC_DEC_BYTES: return be_twos_to_double(ph->b, ph->len) / L->div;
    case PQC_FLOAT16:   return half_to_double((uint16_t)(ph->b[0] | (ph->b[1] << 8)));
    default:            return NAN;
    }
}

/* Growable text buffer for string output. Records failure instead of
   raising, so it can run inside a parallel region. */
typedef struct {
    char   *data;
    int64_t len, cap;
    int     err;
} StrBuf;

static int sb_reserve(StrBuf *sb, int64_t extra) {
    if (sb->err) return -1;
    if (sb->len + extra <= sb->cap) return 0;
    int64_t cap = sb->cap > 0 ? sb->cap : 256;
    while (cap < sb->len + extra) cap *= 2;
    char *d = (char *)realloc(sb->data, (size_t)cap);
    if (!d) { sb->err = 1; return -1; }
    sb->data = d;
    sb->cap = cap;
    return 0;
}

static void sb_append(StrBuf *sb, const void *p, int64_t n) {
    if (n <= 0 || sb_reserve(sb, n)) return;
    memcpy(sb->data + sb->len, p, (size_t)n);
    sb->len += n;
}

static void sb_append_uuid(StrBuf *sb, const uint8_t *b) {
    static const char hx[] = "0123456789abcdef";
    char buf[36];
    int o = 0;
    for (int k = 0; k < 16; k++) {
        if (k == 4 || k == 6 || k == 8 || k == 10) buf[o++] = '-';
        buf[o++] = hx[b[k] >> 4];
        buf[o++] = hx[b[k] & 15];
    }
    sb_append(sb, buf, 36);
}

/* Text form of one value (string columns, and list elements). */
static void append_text(const PqLeaf *L, const PqPhys *ph, StrBuf *sb) {
    char buf[64];
    int n;
    switch (L->conv) {
    case PQC_STRING:
        sb_append(sb, ph->b, ph->len);
        return;
    case PQC_UUID:
        sb_append_uuid(sb, ph->b);
        return;
    case PQC_BOOL:
        sb_append(sb, ph->i ? "TRUE" : "FALSE", ph->i ? 4 : 5);
        return;
    case PQC_I32:
    case PQC_I64:
        n = snprintf(buf, sizeof(buf), "%lld", (long long)ph->i);
        break;
    case PQC_U32:
        n = snprintf(buf, sizeof(buf), "%lld", (long long)(uint32_t)ph->i);
        break;
    default: {
        double d = phys_to_double(L, ph);
        if (isnan(d)) n = snprintf(buf, sizeof(buf), "NaN");
        else if (isinf(d)) n = snprintf(buf, sizeof(buf), d > 0 ? "Inf" : "-Inf");
        else n = snprintf(buf, sizeof(buf), "%.15g", d);
        break;
    }
    }
    if (n > 0) sb_append(sb, buf, n < (int)sizeof(buf) ? n : (int)sizeof(buf) - 1);
}

/* Convert every value of v into dst, laid out as the leaf's (fixed-width)
   output type. A dictionary-index page gathers from the pre-converted
   dictionary. */
static void convert_vals(const PqCursor *c, const PqVals *v, uint8_t *dst) {
    const PqLeaf *L = c->leaf;
    size_t es = vec_type_elem_size(L->out_type);
    int64_t n = v->n;
    if (v->kind == PQV_IDX) {
        const uint8_t *d = c->dict_out;
        const uint32_t *ix = v->idx;
        switch (es) {
        case 1:
            for (int64_t i = 0; i < n; i++) dst[i] = d[ix[i]];
            break;
        case 4: {
            const uint32_t *d4 = (const uint32_t *)(const void *)d;
            uint32_t *o = (uint32_t *)(void *)dst;
            for (int64_t i = 0; i < n; i++) o[i] = d4[ix[i]];
            break;
        }
        default: {
            const uint64_t *d8 = (const uint64_t *)(const void *)d;
            uint64_t *o = (uint64_t *)(void *)dst;
            for (int64_t i = 0; i < n; i++) o[i] = d8[ix[i]];
            break;
        }
        }
        return;
    }
    if (v->kind == PQV_RAW &&
        ((L->conv == PQC_DOUBLE && L->phys == PQ_DOUBLE) ||
         (L->conv == PQC_I64 && L->phys == PQ_INT64) ||
         (L->conv == PQC_I32 && L->phys == PQ_INT32))) {
        if (n) memcpy(dst, v->raw, (size_t)n * es);
        return;
    }
    for (int64_t i = 0; i < n; i++) {
        PqPhys ph;
        vals_get(c, v, i, &ph);
        switch (L->out_type) {
        case VEC_BOOL:  dst[i] = ph.i != 0; break;
        case VEC_INT32: { int32_t x = (int32_t)ph.i; memcpy(dst + 4 * i, &x, 4); break; }
        case VEC_INT64: {
            int64_t x = L->conv == PQC_U32 ? (int64_t)(uint32_t)ph.i : ph.i;
            memcpy(dst + 8 * i, &x, 8);
            break;
        }
        default: {
            double x = phys_to_double(L, &ph);
            memcpy(dst + 8 * i, &x, 8);
            break;
        }
        }
    }
}

/* Flat fixed-width columns decode a page (or dictionary) straight to the
   output type once, so emitting rows is a copy. */
static int materialize(PqCursor *c, const PqVals *v, uint8_t **dst) {
    const PqLeaf *L = c->leaf;
    if (L->out_type == VEC_STRING || L->max_rep > 0) return 0;
    size_t es = vec_type_elem_size(L->out_type);
    *dst = (uint8_t *)malloc((size_t)(v->n ? v->n : 1) * es);
    if (!*dst) return fail(c, "out of memory");
    convert_vals(c, v, *dst);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Pages                                                              */
/* ------------------------------------------------------------------ */

static void page_reset(PqCursor *c) {
    free(c->page_buf);
    free(c->page_out);
    free(c->def);
    free(c->rep);
    c->page_buf = NULL;
    c->page_out = NULL;
    c->def = c->rep = NULL;
    vals_free(&c->vals);
    c->n_levels = c->lvl_pos = c->val_pos = 0;
}

/* Levels of a v1 page: RLE with a 4-byte length prefix, or the deprecated
   MSB-first BIT_PACKED. Advances *pp past them. */
static int levels_v1(PqCursor *c, int enc, int max_level, const uint8_t **pp,
                     const uint8_t *end, uint32_t *out, int64_t n) {
    const uint8_t *p = *pp;
    int bw = bit_width_of((uint32_t)max_level);
    if (enc == PQ_ENC_RLE) {
        if (end - p < 4) return fail(c, "truncated level data");
        uint32_t len;
        memcpy(&len, p, 4);
        p += 4;
        if ((size_t)(end - p) < len || rle_decode(p, len, bw, out, n))
            return fail(c, "malformed level data");
        *pp = p + len;
        return 0;
    }
    if (enc == PQ_ENC_BIT_PACKED) {
        int64_t used = bitpacked_msb_decode(p, (size_t)(end - p), bw, out, n);
        if (used < 0) return fail(c, "malformed BIT_PACKED level data");
        *pp = p + used;
        return 0;
    }
    return fail(c, "unsupported level encoding %d", enc);
}

static int check_levels(PqCursor *c, const uint32_t *lv, int64_t n, int max) {
    for (int64_t i = 0; i < n; i++)
        if (lv[i] > (uint32_t)max) return fail(c, "level exceeds the schema's maximum");
    return 0;
}

static int alloc_levels(PqCursor *c, int64_t n) {
    const PqLeaf *L = c->leaf;
    size_t cnt = (size_t)(n ? n : 1);
    if (L->max_def > 0 && !(c->def = (uint32_t *)malloc(cnt * sizeof(uint32_t))))
        return fail(c, "out of memory");
    if (L->max_rep > 0 && !(c->rep = (uint32_t *)malloc(cnt * sizeof(uint32_t))))
        return fail(c, "out of memory");
    return 0;
}

static int64_t count_present(const PqCursor *c, int64_t n) {
    const PqLeaf *L = c->leaf;
    if (L->max_def == 0) return n;
    int64_t k = 0;
    for (int64_t i = 0; i < n; i++) k += c->def[i] == (uint32_t)L->max_def;
    return k;
}

static int load_data_v1(PqCursor *c, const PqPageHeader *h,
                        const uint8_t *payload) {
    const PqLeaf *L = c->leaf;
    size_t ulen = (size_t)h->uncompressed_size;
    c->page_buf = (uint8_t *)malloc(ulen ? ulen : 1);
    if (!c->page_buf) return fail(c, "out of memory");
    if (pq_decompress(&c->codec_ctx, c->codec, payload,
                      (size_t)h->compressed_size, c->page_buf, ulen))
        return fail(c, "could not decompress %s page", pq_codec_name(c->codec));
    const uint8_t *p = c->page_buf, *end = c->page_buf + ulen;
    int64_t n = h->num_values;
    if (alloc_levels(c, n)) return -1;
    if (L->max_rep > 0 &&
        (levels_v1(c, h->rep_encoding, L->max_rep, &p, end, c->rep, n) ||
         check_levels(c, c->rep, n, L->max_rep)))
        return -1;
    if (L->max_def > 0 &&
        (levels_v1(c, h->def_encoding, L->max_def, &p, end, c->def, n) ||
         check_levels(c, c->def, n, L->max_def)))
        return -1;
    if (decode_values(c, h->encoding, p, (size_t)(end - p),
                      count_present(c, n), &c->vals) ||
        materialize(c, &c->vals, &c->page_out))
        return -1;
    c->n_levels = n;
    return 0;
}

static int load_data_v2(PqCursor *c, const PqPageHeader *h,
                        const uint8_t *payload) {
    const PqLeaf *L = c->leaf;
    int64_t rl = h->rep_len, dl = h->def_len;
    if (rl + dl > h->compressed_size || rl + dl > h->uncompressed_size)
        return fail(c, "page levels overrun the page");
    const uint8_t *vp = payload + rl + dl;
    size_t clen = (size_t)(h->compressed_size - rl - dl);
    size_t ulen = (size_t)(h->uncompressed_size - rl - dl);
    c->page_buf = (uint8_t *)malloc(ulen ? ulen : 1);
    if (!c->page_buf) return fail(c, "out of memory");
    if (h->is_compressed) {
        if (pq_decompress(&c->codec_ctx, c->codec, vp, clen, c->page_buf, ulen))
            return fail(c, "could not decompress %s page",
                        pq_codec_name(c->codec));
    } else {
        if (clen != ulen) return fail(c, "uncompressed page size mismatch");
        if (ulen) memcpy(c->page_buf, vp, ulen);
    }
    int64_t n = h->num_values;
    if (alloc_levels(c, n)) return -1;
    if (L->max_rep > 0 &&
        (rle_decode(payload, (size_t)rl, bit_width_of((uint32_t)L->max_rep),
                    c->rep, n) ||
         check_levels(c, c->rep, n, L->max_rep)))
        return c->err[0] ? -1 : fail(c, "malformed repetition levels");
    if (L->max_def > 0 &&
        (rle_decode(payload + rl, (size_t)dl, bit_width_of((uint32_t)L->max_def),
                    c->def, n) ||
         check_levels(c, c->def, n, L->max_def)))
        return c->err[0] ? -1 : fail(c, "malformed definition levels");
    if (decode_values(c, h->encoding, c->page_buf, ulen,
                      count_present(c, n), &c->vals) ||
        materialize(c, &c->vals, &c->page_out))
        return -1;
    c->n_levels = n;
    return 0;
}

/* Advance to the next data page, decoding a dictionary page on the way. */
static int load_page(PqCursor *c) {
    page_reset(c);
    for (;;) {
        if (c->pos >= c->chunk_len)
            return fail(c, "column chunk ended before the row group's rows");
        PqPageHeader h;
        int64_t hl = pq_page_header_decode(c->chunk + c->pos,
                                           c->chunk_len - c->pos, &h);
        if (hl < 0) return fail(c, "malformed page header");
        c->pos += (size_t)hl;
        if ((size_t)h.compressed_size > c->chunk_len - c->pos)
            return fail(c, "page runs past the end of its column chunk");
        const uint8_t *payload = c->chunk + c->pos;
        c->pos += (size_t)h.compressed_size;

        if (h.type == PQ_PAGE_DICTIONARY) {
            if (!h.has_dict) return fail(c, "dictionary page without a header");
            if (h.encoding != PQ_ENC_PLAIN && h.encoding != PQ_ENC_PLAIN_DICTIONARY)
                return fail(c, "unsupported dictionary encoding %d", h.encoding);
            vals_free(&c->dict);
            free(c->dict_buf);
            free(c->dict_out);
            c->dict_buf = c->dict_out = NULL;
            c->has_dict = 0;
            size_t ulen = (size_t)h.uncompressed_size;
            c->dict_buf = (uint8_t *)malloc(ulen ? ulen : 1);
            if (!c->dict_buf) return fail(c, "out of memory");
            if (pq_decompress(&c->codec_ctx, c->codec, payload,
                              (size_t)h.compressed_size, c->dict_buf, ulen))
                return fail(c, "could not decompress %s dictionary page",
                            pq_codec_name(c->codec));
            if (decode_values(c, PQ_ENC_PLAIN, c->dict_buf, ulen,
                              h.num_values, &c->dict) ||
                materialize(c, &c->dict, &c->dict_out))
                return -1;
            c->has_dict = 1;
            continue;
        }
        if (h.type == PQ_PAGE_DATA) {
            if (!h.has_data) return fail(c, "data page without a header");
            return load_data_v1(c, &h, payload);
        }
        if (h.type == PQ_PAGE_DATA_V2) {
            if (!h.has_v2) return fail(c, "data page without a header");
            return load_data_v2(c, &h, payload);
        }
        /* Index pages and page types this reader does not know carry no
           rows; skip them. */
    }
}

/* ------------------------------------------------------------------ */
/*  Rows                                                               */
/* ------------------------------------------------------------------ */

static int finish_strings(PqCursor *c, VecArray *out, StrBuf *sb) {
    if (sb->err) {
        free(sb->data);
        return fail(c, "out of memory");
    }
    if (!sb->data) return 0;
    free(out->buf.str.data);
    out->buf.str.data = sb->data;
    out->buf.str.data_len = sb->len;
    return 0;
}

static int read_flat(PqCursor *c, int64_t n, VecArray *out) {
    const PqLeaf *L = c->leaf;
    StrBuf sb = { NULL, 0, 0, 0 };
    int is_str = L->out_type == VEC_STRING;
    size_t es = vec_type_elem_size(L->out_type);
    uint8_t *base = is_str ? NULL : (uint8_t *)out->buf.i64;
    const uint32_t full = (uint32_t)L->max_def;
    int64_t filled = 0;
    while (filled < n) {
        if (c->lvl_pos >= c->n_levels) {
            if (load_page(c)) { free(sb.data); return -1; }
            continue;
        }
        int64_t k = c->n_levels - c->lvl_pos;
        if (k > n - filled) k = n - filled;
        const uint32_t *def = L->max_def > 0 ? c->def + c->lvl_pos : NULL;

        int64_t present = k;
        if (def) {
            present = 0;
            for (int64_t r = 0; r < k; r++) present += def[r] == full;
        }
        if (c->vals.n - c->val_pos < present) {
            free(sb.data);
            return fail(c, "page holds fewer values than its levels require");
        }

        if (!is_str) {
            const uint8_t *src = c->page_out + (size_t)c->val_pos * es;
            uint8_t *dst = base + (size_t)filled * es;
            if (present == k) {
                memcpy(dst, src, (size_t)k * es);
                vec_validity_set_bits(out->validity, filled, k);
            } else {
                int64_t v = 0;
                for (int64_t r = 0; r < k; r++) {
                    if (def[r] != full) continue;
                    switch (es) {
                    case 1: dst[r] = src[v]; break;
                    case 4: memcpy(dst + 4 * r, src + 4 * v, 4); break;
                    default: memcpy(dst + 8 * r, src + 8 * v, 8); break;
                    }
                    v++;
                    vec_array_set_valid(out, filled + r);
                }
            }
            c->val_pos += present;
        } else {
            int dict_str = c->vals.kind == PQV_IDX && c->dict.kind == PQV_STR &&
                           L->conv == PQC_STRING;
            int64_t *offs = out->buf.str.offsets;
            for (int64_t r = 0; r < k; r++) {
                int64_t row = filled + r;
                if (!def || def[r] == full) {
                    if (dict_str) {
                        uint32_t ix = c->vals.idx[c->val_pos++];
                        sb_append(&sb, c->dict.sp[ix], c->dict.sl[ix]);
                    } else {
                        PqPhys ph;
                        vals_get(c, &c->vals, c->val_pos++, &ph);
                        append_text(L, &ph, &sb);
                    }
                    vec_array_set_valid(out, row);
                }
                offs[row + 1] = sb.len;
            }
        }
        c->lvl_pos += k;
        filled += k;
    }
    return is_str ? finish_strings(c, out, &sb) : 0;
}

/* A list column: rows start at repetition level 0; each row's elements are
   written as text joined by list_sep. A null list is NA, an empty list "",
   a null element "NA". */
static int read_list(PqCursor *c, int64_t n, VecArray *out) {
    const PqLeaf *L = c->leaf;
    StrBuf sb = { NULL, 0, 0, 0 };
    size_t seplen = strlen(c->list_sep);
    int64_t filled = 0;
    int in_row = 0, row_na = 0, n_elem = 0;

    for (;;) {
        if (c->lvl_pos >= c->n_levels) {
            if (c->pos >= c->chunk_len) break;
            if (load_page(c)) { free(sb.data); return -1; }
            continue;
        }
        uint32_t rep = c->rep[c->lvl_pos];
        uint32_t def = c->def ? c->def[c->lvl_pos] : 0;
        if (rep == 0) {
            if (in_row) {
                if (!row_na) vec_array_set_valid(out, filled);
                out->buf.str.offsets[filled + 1] = sb.len;
                filled++;
                in_row = 0;
            }
            if (filled == n) break;
            in_row = 1;
            n_elem = 0;
            row_na = (int)def < L->list_null_def;
            if ((int)def < L->list_elem_def) { c->lvl_pos++; continue; }
        } else if (!in_row) {
            free(sb.data);
            return fail(c, "list data does not start at a row boundary");
        }
        if (n_elem++) sb_append(&sb, c->list_sep, (int64_t)seplen);
        if ((int)def == L->max_def) {
            if (c->val_pos >= c->vals.n) {
                free(sb.data);
                return fail(c, "page holds fewer values than its levels require");
            }
            PqPhys ph;
            vals_get(c, &c->vals, c->val_pos++, &ph);
            append_text(L, &ph, &sb);
        } else {
            sb_append(&sb, "NA", 2);
        }
        c->lvl_pos++;
    }
    if (in_row) {
        if (!row_na) vec_array_set_valid(out, filled);
        out->buf.str.offsets[filled + 1] = sb.len;
        filled++;
    }
    if (filled != n) {
        free(sb.data);
        return fail(c, "column chunk holds fewer rows than its row group");
    }
    return finish_strings(c, out, &sb);
}

void pq_cursor_init(PqCursor *c, const PqLeaf *leaf, int codec,
                    uint8_t *chunk, size_t chunk_len, const char *list_sep) {
    memset(c, 0, sizeof(*c));
    c->leaf = leaf;
    c->codec = codec;
    c->chunk = chunk;
    c->chunk_len = chunk_len;
    c->list_sep = (char *)list_sep;
}

void pq_cursor_free(PqCursor *c) {
    if (!c->leaf) return;
    page_reset(c);
    vals_free(&c->dict);
    free(c->dict_buf);
    free(c->dict_out);
    free(c->chunk);
    pq_codec_ctx_free(&c->codec_ctx);
    memset(c, 0, sizeof(*c));
}

int pq_cursor_read(PqCursor *c, int64_t n, VecArray *out) {
    if (c->leaf->unsupported) return fail(c, "%s", c->leaf->unsupported);
    if (c->leaf->max_rep > 0) return read_list(c, n, out);
    return read_flat(c, n, out);
}

/* ------------------------------------------------------------------ */
/*  Statistics bounds                                                  */
/* ------------------------------------------------------------------ */

int pq_stat_to_int64(const PqLeaf *L, const uint8_t *b, uint32_t len,
                     int64_t *out) {
    if (L->phys == PQ_INT32 && len == 4) {
        int32_t x;
        memcpy(&x, b, 4);
        *out = L->conv == PQC_U32 ? (int64_t)(uint32_t)x : (int64_t)x;
        return 1;
    }
    if (L->phys == PQ_INT64 && len == 8) {
        memcpy(out, b, 8);
        return 1;
    }
    return 0;
}

int pq_stat_to_double(const PqLeaf *L, const uint8_t *b, uint32_t len,
                      double *out) {
    PqPhys ph;
    memset(&ph, 0, sizeof(ph));
    switch (L->phys) {
    case PQ_INT32:
    case PQ_INT64: {
        int64_t v;
        if (!pq_stat_to_int64(L, b, len, &v)) return 0;
        ph.i = v;
        break;
    }
    case PQ_FLOAT: {
        if (len != 4) return 0;
        float f;
        memcpy(&f, b, 4);
        ph.d = f;
        break;
    }
    case PQ_DOUBLE:
        if (len != 8) return 0;
        memcpy(&ph.d, b, 8);
        break;
    default:
        return 0;
    }
    if (L->conv == PQC_DEC_BYTES || L->conv == PQC_INT96 ||
        L->conv == PQC_FLOAT16)
        return 0;
    *out = phys_to_double(L, &ph);
    return !isnan(*out);
}
