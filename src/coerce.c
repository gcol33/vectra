#include "coerce.h"
#include "array.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* Largest magnitude a double may hold and still round-trip into int64.
   (double)INT64_MAX rounds up to 2^63 (not representable as int64), so the
   in-range test is d < 2^63; INT64_MIN == -2^63 is representable, so the low
   bound is d >= -2^63. Values outside this become NA on a numeric->int cast,
   matching R's as.integer and avoiding the UB of casting an out-of-range or
   non-finite double straight to int64. */
#define COERCE_I64_2P63      9223372036854775808.0   /* 2^63 */
#define COERCE_I64_NEG_2P63 (-9223372036854775808.0) /* -2^63 == INT64_MIN */

/* Copy string row i into buf (NUL-terminated), trimming surrounding ASCII
   whitespace. Returns 0 if the trimmed slice does not fit in cap. */
static int coerce_str_slice(const VecArray *arr, int64_t i, char *buf, size_t cap) {
    int64_t s = arr->buf.str.offsets[i];
    int64_t e = arr->buf.str.offsets[i + 1];
    const char *p = arr->buf.str.data + s;
    int64_t len = e - s;
    while (len > 0 && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) { p++; len--; }
    while (len > 0) {
        char c = p[len - 1];
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') len--;
        else break;
    }
    if ((size_t)len >= cap) return 0;
    memcpy(buf, p, (size_t)len);
    buf[len] = '\0';
    return 1;
}

VecType vec_common_type(VecType a, VecType b) {
    if (a == b) return a;
    /* bool < int8 < int16 < int32 < int64 < double */
    if (a == VEC_STRING || b == VEC_STRING)
        vectra_error("cannot coerce string in arithmetic/comparison");
    /* Return the "wider" type */
    if (a == VEC_DOUBLE || b == VEC_DOUBLE) return VEC_DOUBLE;
    if (a == VEC_INT64 || b == VEC_INT64) return VEC_INT64;
    if (a == VEC_INT32 || b == VEC_INT32) return VEC_INT32;
    if (a == VEC_INT16 || b == VEC_INT16) return VEC_INT16;
    if (a == VEC_INT8  || b == VEC_INT8)  return VEC_INT8;
    return VEC_BOOL;
}

VecArray *vec_coerce(const VecArray *arr, VecType target) {
    if (arr->type == target) {
        /* Copy */
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        if (!out) vectra_error("alloc failed");
        *out = vec_array_alloc(target, arr->length);
        /* Copy validity */
        memcpy(out->validity, arr->validity, (size_t)vec_validity_bytes(arr->length));
        switch (target) {
        case VEC_INT64:
            memcpy(out->buf.i64, arr->buf.i64, (size_t)arr->length * sizeof(int64_t));
            break;
        case VEC_INT32:
            memcpy(out->buf.i32, arr->buf.i32, (size_t)arr->length * sizeof(int32_t));
            break;
        case VEC_INT16:
            memcpy(out->buf.i16, arr->buf.i16, (size_t)arr->length * sizeof(int16_t));
            break;
        case VEC_INT8:
            memcpy(out->buf.i8, arr->buf.i8, (size_t)arr->length);
            break;
        case VEC_DOUBLE:
            memcpy(out->buf.dbl, arr->buf.dbl, (size_t)arr->length * sizeof(double));
            break;
        case VEC_BOOL:
            memcpy(out->buf.bln, arr->buf.bln, (size_t)arr->length);
            break;
        case VEC_STRING:
            break; /* handled below */
        }
        if (target == VEC_STRING) {
            memcpy(out->buf.str.offsets, arr->buf.str.offsets,
                   (size_t)(arr->length + 1) * sizeof(int64_t));
            out->buf.str.data_len = arr->buf.str.data_len;
            free(out->buf.str.data); /* free the 1-byte from alloc */
            out->buf.str.data = (char *)malloc((size_t)(arr->buf.str.data_len > 0 ? arr->buf.str.data_len : 1));
            if (!out->buf.str.data)
                vectra_error("alloc failed for string copy");
            if (arr->buf.str.data_len > 0)
                memcpy(out->buf.str.data, arr->buf.str.data, (size_t)arr->buf.str.data_len);
        }
        return out;
    }

    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    if (!out) vectra_error("alloc failed");
    *out = vec_array_alloc(target, arr->length);
    memcpy(out->validity, arr->validity, (size_t)vec_validity_bytes(arr->length));

    /* Any integer type → int64 (widen) */
    if (vec_type_is_int(arr->type) && target == VEC_INT64) {
        for (int64_t i = 0; i < arr->length; i++)
            out->buf.i64[i] = vec_array_get_int(arr, i);
    }
    /* Any integer type → double (widen) */
    else if (vec_type_is_int(arr->type) && target == VEC_DOUBLE) {
        for (int64_t i = 0; i < arr->length; i++)
            out->buf.dbl[i] = (double)vec_array_get_int(arr, i);
    }
    /* bool → int64 */
    else if (arr->type == VEC_BOOL && target == VEC_INT64) {
        for (int64_t i = 0; i < arr->length; i++)
            out->buf.i64[i] = (int64_t)arr->buf.bln[i];
    }
    /* bool → double */
    else if (arr->type == VEC_BOOL && target == VEC_DOUBLE) {
        for (int64_t i = 0; i < arr->length; i++)
            out->buf.dbl[i] = (double)arr->buf.bln[i];
    }
    /* int64 → double */
    else if (arr->type == VEC_INT64 && target == VEC_DOUBLE) {
        for (int64_t i = 0; i < arr->length; i++)
            out->buf.dbl[i] = (double)arr->buf.i64[i];
    }
    /* double → int64: truncate toward zero; NA/NaN/+-Inf/out-of-range -> NA */
    else if (arr->type == VEC_DOUBLE && target == VEC_INT64) {
        for (int64_t i = 0; i < arr->length; i++) {
            if (!vec_array_is_valid(arr, i)) continue;
            double d = arr->buf.dbl[i];
            if (!isfinite(d) || d >= COERCE_I64_2P63 || d < COERCE_I64_NEG_2P63)
                vec_array_set_null(out, i);
            else
                out->buf.i64[i] = (int64_t)d; /* C truncates toward zero */
        }
    }
    /* numeric -> bool: 0 -> FALSE, nonzero -> TRUE, NA/NaN -> NA */
    else if (target == VEC_BOOL &&
             (vec_type_is_int(arr->type) || arr->type == VEC_DOUBLE)) {
        for (int64_t i = 0; i < arr->length; i++) {
            if (!vec_array_is_valid(arr, i)) continue;
            if (arr->type == VEC_DOUBLE) {
                double d = arr->buf.dbl[i];
                if (d != d) { vec_array_set_null(out, i); continue; }
                out->buf.bln[i] = (uint8_t)(d != 0.0);
            } else {
                out->buf.bln[i] = (uint8_t)(vec_array_get_int(arr, i) != 0);
            }
        }
    }
    /* string -> double: R as.numeric; unparseable -> NA (no error) */
    else if (arr->type == VEC_STRING && target == VEC_DOUBLE) {
        char tmp[64];
        for (int64_t i = 0; i < arr->length; i++) {
            if (!vec_array_is_valid(arr, i)) continue;
            if (!coerce_str_slice(arr, i, tmp, sizeof tmp) || tmp[0] == '\0') {
                vec_array_set_null(out, i);
                continue;
            }
            char *end;
            double d = strtod(tmp, &end);
            if (end == tmp || *end != '\0') { vec_array_set_null(out, i); continue; }
            out->buf.dbl[i] = d;
        }
    }
    /* string -> int64: R as.integer parses as real then truncates toward zero */
    else if (arr->type == VEC_STRING && target == VEC_INT64) {
        char tmp[64];
        for (int64_t i = 0; i < arr->length; i++) {
            if (!vec_array_is_valid(arr, i)) continue;
            if (!coerce_str_slice(arr, i, tmp, sizeof tmp) || tmp[0] == '\0') {
                vec_array_set_null(out, i);
                continue;
            }
            char *end;
            double d = strtod(tmp, &end);
            if (end == tmp || *end != '\0' ||
                !isfinite(d) || d >= COERCE_I64_2P63 || d < COERCE_I64_NEG_2P63) {
                vec_array_set_null(out, i);
                continue;
            }
            out->buf.i64[i] = (int64_t)d;
        }
    }
    /* string -> bool: R's accepted spellings only; anything else -> NA */
    else if (arr->type == VEC_STRING && target == VEC_BOOL) {
        char tmp[16];
        for (int64_t i = 0; i < arr->length; i++) {
            if (!vec_array_is_valid(arr, i)) continue;
            if (!coerce_str_slice(arr, i, tmp, sizeof tmp)) { vec_array_set_null(out, i); continue; }
            if (!strcmp(tmp, "TRUE") || !strcmp(tmp, "true") ||
                !strcmp(tmp, "True") || !strcmp(tmp, "T"))
                out->buf.bln[i] = 1;
            else if (!strcmp(tmp, "FALSE") || !strcmp(tmp, "false") ||
                     !strcmp(tmp, "False") || !strcmp(tmp, "F"))
                out->buf.bln[i] = 0;
            else
                vec_array_set_null(out, i);
        }
    } else if (target == VEC_STRING) {
        /* Coerce numeric/bool to string: only valid values get converted,
           NAs stay as NAs. Single-pass: format into a growth buffer, record
           offsets as we go, then hand ownership to the output array. */
        char numbuf[64];
        /* Re-allocate as string type */
        vec_array_free(out);
        free(out);
        out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_STRING, arr->length);
        memcpy(out->validity, arr->validity, (size_t)vec_validity_bytes(arr->length));
        /* Growth buffer — start at 16 bytes per valid value estimate */
        int64_t buf_cap = arr->length * 16;
        if (buf_cap < 64) buf_cap = 64;
        free(out->buf.str.data);
        char *buf = (char *)malloc((size_t)buf_cap);
        int64_t off = 0;
        for (int64_t i = 0; i < arr->length; i++) {
            out->buf.str.offsets[i] = off;
            if (!vec_array_is_valid(arr, i)) continue;
            int len = 0;
            switch (arr->type) {
            case VEC_BOOL:   len = snprintf(numbuf, sizeof(numbuf), "%s", arr->buf.bln[i] ? "TRUE" : "FALSE"); break;
            case VEC_INT8:   len = snprintf(numbuf, sizeof(numbuf), "%d", (int)arr->buf.i8[i]); break;
            case VEC_INT16:  len = snprintf(numbuf, sizeof(numbuf), "%d", (int)arr->buf.i16[i]); break;
            case VEC_INT32:  len = snprintf(numbuf, sizeof(numbuf), "%d", (int)arr->buf.i32[i]); break;
            case VEC_INT64:  len = snprintf(numbuf, sizeof(numbuf), "%lld", (long long)arr->buf.i64[i]); break;
            case VEC_DOUBLE: {
                /* NA is handled by the validity check above; a genuine computed
                   NaN/Inf must stringify as R does ("NaN"/"Inf"/"-Inf"), not the
                   platform's lowercase "nan"/"inf" from %g. */
                double dv = arr->buf.dbl[i];
                if (isnan(dv))      len = snprintf(numbuf, sizeof(numbuf), "NaN");
                else if (isinf(dv)) len = snprintf(numbuf, sizeof(numbuf), dv < 0 ? "-Inf" : "Inf");
                else                len = snprintf(numbuf, sizeof(numbuf), "%.15g", dv);
                break;
            }
            default: break;
            }
            if (off + len > buf_cap) {
                buf_cap = (off + len) * 2;
                buf = (char *)realloc(buf, (size_t)buf_cap);
            }
            memcpy(buf + off, numbuf, (size_t)len);
            off += len;
        }
        out->buf.str.offsets[arr->length] = off;
        out->buf.str.data = buf;
        out->buf.str.data_len = off;
    } else {
        vec_array_free(out);
        free(out);
        vectra_error("unsupported coercion: %d -> %d", arr->type, target);
    }

    return out;
}
