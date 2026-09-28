#include "parquet_thrift.h"

uint64_t pqt_varint(PqThrift *t) {
    uint64_t r = 0;
    int shift = 0;
    while (!t->err && t->p < t->end) {
        uint8_t b = *t->p++;
        if (shift < 64) r |= (uint64_t)(b & 0x7f) << shift;
        if (!(b & 0x80)) return r;
        shift += 7;
        if (shift >= 70) break;
    }
    t->err = 1;
    return 0;
}

int64_t pqt_zigzag(PqThrift *t) {
    uint64_t v = pqt_varint(t);
    return (int64_t)(v >> 1) ^ -(int64_t)(v & 1);
}

uint8_t pqt_byte(PqThrift *t) {
    if (t->err || t->p >= t->end) { t->err = 1; return 0; }
    return *t->p++;
}

int pqt_field(PqThrift *t, int16_t *last_id, int16_t *id, int *type) {
    uint8_t b = pqt_byte(t);
    if (t->err || b == 0) return 0;
    *type = b & 0x0f;
    int delta = b >> 4;
    if (delta == 0) *id = (int16_t)pqt_zigzag(t);
    else *id = (int16_t)(*last_id + delta);
    *last_id = *id;
    return !t->err;
}

const uint8_t *pqt_binary(PqThrift *t, uint32_t *len) {
    uint64_t n = pqt_varint(t);
    if (t->err || n > (uint64_t)(t->end - t->p)) {
        t->err = 1;
        *len = 0;
        return NULL;
    }
    const uint8_t *r = t->p;
    t->p += n;
    *len = (uint32_t)n;
    return r;
}

uint32_t pqt_list(PqThrift *t, int *elem_type) {
    uint8_t b = pqt_byte(t);
    uint64_t n = b >> 4;
    *elem_type = b & 0x0f;
    if (n == 15) n = pqt_varint(t);
    if (t->err || n > (uint64_t)(t->end - t->p)) {
        t->err = 1;
        return 0;
    }
    return (uint32_t)n;
}

int64_t pqt_int(PqThrift *t, int type) {
    switch (type) {
    case PQT_BYTE: return (int8_t)pqt_byte(t);
    case PQT_I16:
    case PQT_I32:
    case PQT_I64:  return pqt_zigzag(t);
    default:
        pqt_skip(t, type);
        t->err = 1;
        return 0;
    }
}

static void pqt_skip_value(PqThrift *t, int type, int in_list) {
    if (t->err) return;
    if (++t->depth > PQT_MAX_DEPTH) { t->err = 1; return; }
    switch (type) {
    case PQT_TRUE:
    case PQT_FALSE:
        if (in_list) (void)pqt_byte(t);
        break;
    case PQT_BYTE:
        (void)pqt_byte(t);
        break;
    case PQT_I16:
    case PQT_I32:
    case PQT_I64:
        (void)pqt_varint(t);
        break;
    case PQT_DOUBLE:
        if (t->end - t->p < 8) t->err = 1;
        else t->p += 8;
        break;
    case PQT_BINARY: {
        uint32_t len;
        (void)pqt_binary(t, &len);
        break;
    }
    case PQT_LIST:
    case PQT_SET: {
        int et;
        uint32_t n = pqt_list(t, &et);
        for (uint32_t i = 0; i < n && !t->err; i++)
            pqt_skip_value(t, et, 1);
        break;
    }
    case PQT_MAP: {
        uint64_t n = pqt_varint(t);
        if (t->err || n > (uint64_t)(t->end - t->p)) { t->err = 1; break; }
        if (n > 0) {
            uint8_t kv = pqt_byte(t);
            for (uint64_t i = 0; i < n && !t->err; i++) {
                pqt_skip_value(t, kv >> 4, 1);
                pqt_skip_value(t, kv & 0x0f, 1);
            }
        }
        break;
    }
    case PQT_STRUCT: {
        int16_t last = 0, id;
        int ft;
        while (pqt_field(t, &last, &id, &ft))
            pqt_skip_value(t, ft, 0);
        break;
    }
    default:
        t->err = 1;
    }
    t->depth--;
}

void pqt_skip(PqThrift *t, int type) { pqt_skip_value(t, type, 0); }

void pqt_skip_elem(PqThrift *t, int type) { pqt_skip_value(t, type, 1); }
