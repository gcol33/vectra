#include "key_arena.h"
#include "array.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

void key_arena_init(KeyArena *ka, int n_keys, VecType *key_types) {
    ka->n_keys = n_keys;
    ka->key_types = (VecType *)malloc((size_t)n_keys * sizeof(VecType));
    memcpy(ka->key_types, key_types, (size_t)n_keys * sizeof(VecType));
    ka->capacity = 64;
    ka->length = 0;
    ka->arenas = (VecArray *)calloc((size_t)n_keys, sizeof(VecArray));
    ka->str_data = (char **)calloc((size_t)n_keys, sizeof(char *));
    ka->str_data_len = (int64_t *)calloc((size_t)n_keys, sizeof(int64_t));
    ka->str_data_cap = (int64_t *)calloc((size_t)n_keys, sizeof(int64_t));

    for (int k = 0; k < n_keys; k++) {
        ka->arenas[k] = vec_array_alloc(key_types[k], ka->capacity);
        if (key_types[k] == VEC_STRING)
            ka->arenas[k].owns_data = 0;
    }
}

static void key_arena_ensure(KeyArena *ka, int64_t n) {
    if (n <= ka->capacity) return;
    int64_t new_cap = ka->capacity;
    while (new_cap < n) new_cap *= 2;

    for (int k = 0; k < ka->n_keys; k++) {
        VecArray old = ka->arenas[k];
        VecArray new_arr = vec_array_alloc(ka->key_types[k], new_cap);
        memcpy(new_arr.validity, old.validity, (size_t)vec_validity_bytes(old.length));
        switch (ka->key_types[k]) {
        case VEC_INT64:
            memcpy(new_arr.buf.i64, old.buf.i64, (size_t)old.length * sizeof(int64_t));
            break;
        case VEC_INT32:
            memcpy(new_arr.buf.i32, old.buf.i32, (size_t)old.length * sizeof(int32_t));
            break;
        case VEC_INT16:
            memcpy(new_arr.buf.i16, old.buf.i16, (size_t)old.length * sizeof(int16_t));
            break;
        case VEC_INT8:
            memcpy(new_arr.buf.i8, old.buf.i8, (size_t)old.length * sizeof(int8_t));
            break;
        case VEC_DOUBLE:
            memcpy(new_arr.buf.dbl, old.buf.dbl, (size_t)old.length * sizeof(double));
            break;
        case VEC_BOOL:
            memcpy(new_arr.buf.bln, old.buf.bln, (size_t)old.length);
            break;
        case VEC_STRING:
            memcpy(new_arr.buf.str.offsets, old.buf.str.offsets,
                   (size_t)(old.length + 1) * sizeof(int64_t));
            free(new_arr.buf.str.data);
            new_arr.buf.str.data = ka->str_data[k];
            new_arr.buf.str.data_len = ka->str_data_len[k];
            new_arr.owns_data = 0;
            assert(!old.owns_data && "arena string array must be borrowed");
            assert(ka->str_data_cap[k] >= ka->str_data_len[k]);
            assert(ka->length == 0 || new_arr.buf.str.data != NULL);
            break;
        }
        new_arr.length = old.length;
        vec_array_free(&old);
        ka->arenas[k] = new_arr;
    }
    ka->capacity = new_cap;
}

void key_arena_append_row(KeyArena *ka, const VecArray *keys, int64_t row) {
    int64_t pos = ka->length;
    key_arena_ensure(ka, pos + 1);

    for (int k = 0; k < ka->n_keys; k++) {
        VecArray *a = &ka->arenas[k];
        a->length = pos + 1;
        if (vec_array_is_valid(&keys[k], row)) {
            vec_array_set_valid(a, pos);
            switch (ka->key_types[k]) {
            case VEC_INT64:  a->buf.i64[pos] = keys[k].buf.i64[row]; break;
            case VEC_INT32:  a->buf.i32[pos] = keys[k].buf.i32[row]; break;
            case VEC_INT16:  a->buf.i16[pos] = keys[k].buf.i16[row]; break;
            case VEC_INT8:   a->buf.i8[pos]  = keys[k].buf.i8[row];  break;
            case VEC_DOUBLE: a->buf.dbl[pos] = keys[k].buf.dbl[row]; break;
            case VEC_BOOL:   a->buf.bln[pos] = keys[k].buf.bln[row]; break;
            case VEC_STRING: {
                int64_t s = keys[k].buf.str.offsets[row];
                int64_t e = keys[k].buf.str.offsets[row + 1];
                int64_t slen = e - s;
                int64_t needed = ka->str_data_len[k] + slen;
                if (needed > ka->str_data_cap[k]) {
                    int64_t nc = ka->str_data_cap[k] == 0 ? 256 : ka->str_data_cap[k];
                    while (nc < needed) nc *= 2;
                    ka->str_data[k] = (char *)realloc(ka->str_data[k], (size_t)nc);
                    assert(ka->str_data[k] != NULL && "arena string realloc failed");
                    ka->str_data_cap[k] = nc;
                }
                a->buf.str.offsets[pos] = ka->str_data_len[k];
                if (slen > 0)
                    memcpy(ka->str_data[k] + ka->str_data_len[k],
                           keys[k].buf.str.data + s, (size_t)slen);
                ka->str_data_len[k] += slen;
                a->buf.str.offsets[pos + 1] = ka->str_data_len[k];
                a->buf.str.data = ka->str_data[k];
                a->buf.str.data_len = ka->str_data_len[k];
                break;
            }
            }
        } else {
            vec_array_set_null(a, pos);
            if (ka->key_types[k] == VEC_STRING) {
                int64_t cur = ka->str_data_len[k];
                a->buf.str.offsets[pos] = cur;
                a->buf.str.offsets[pos + 1] = cur;
                a->buf.str.data = ka->str_data[k];
                a->buf.str.data_len = cur;
            }
        }
    }
    ka->length = pos + 1;
}

void key_arena_free(KeyArena *ka) {
    for (int k = 0; k < ka->n_keys; k++) {
        vec_array_free(&ka->arenas[k]);
        if (ka->key_types[k] == VEC_STRING) {
            free(ka->str_data[k]);
#ifndef NDEBUG
            ka->str_data[k] = (char *)(uintptr_t)0xDEADBEEFDEADBEEFULL;
#endif
        }
    }
    free(ka->arenas);
    free(ka->key_types);
    free(ka->str_data);
    free(ka->str_data_len);
    free(ka->str_data_cap);
}
