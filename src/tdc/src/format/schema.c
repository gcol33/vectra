/*
 * src/format/schema.c
 *
 * Serialize / parse the optional column schema section that follows the
 * 64-byte container header in TDC1 v2 containers.
 *
 * Wire format (all little-endian, LE targets only — just memcpy):
 *
 *   u16 n_columns
 *   per column:
 *       u16 name_len          (must be > 0)
 *       name_len bytes        (UTF-8 name, NOT null-terminated on disk)
 *       u8  dtype             (tdc_dtype, must be known value 1..12)
 *       u16 ann_len           (0 means no annotation)
 *       ann_len bytes         (UTF-8 annotation, NOT null-terminated)
 *
 * All allocations go through the caller-supplied realloc_fn.
 */

#include "schema_internal.h"
#include "metadata_internal.h"

#include <string.h>

/* ----- dtype validation --------------------------------------------------- */

static int schema_dtype_known(uint8_t dt) {
    /* TDC_DT_I8 (1) through TDC_DT_F16 (12), including TDC_DT_STRING (11). */
    return dt >= 1 && dt <= 12;
}

/* ----- serialized size ---------------------------------------------------- */

size_t tdc_schema_serialized_size(const tdc_schema *schema) {
    if (!schema || schema->n_columns == 0) return 0;

    /* 2 bytes for n_columns */
    size_t total = 2;
    for (uint16_t i = 0; i < schema->n_columns; ++i) {
        const tdc_column_desc *c = &schema->columns[i];
        /* u16 name_len + name bytes + u8 dtype + u16 ann_len + ann bytes */
        total += 2 + (size_t)c->name_len + 1 + 2 + (size_t)c->ann_len;
    }
    return total;
}

/* ----- serialize ---------------------------------------------------------- */

size_t tdc_schema_serialize(const tdc_schema *schema, uint8_t *buf) {
    if (!schema || !buf || schema->n_columns == 0) return 0;

    uint8_t *p = buf;

    tdc_le_store_u16(p, schema->n_columns);
    p += 2;

    for (uint16_t i = 0; i < schema->n_columns; ++i) {
        const tdc_column_desc *c = &schema->columns[i];

        tdc_le_store_u16(p, c->name_len);
        p += 2;
        memcpy(p, c->name, c->name_len);
        p += c->name_len;

        *p++ = c->dtype;

        tdc_le_store_u16(p, c->ann_len);
        p += 2;
        if (c->ann_len > 0 && c->annotation) {
            memcpy(p, c->annotation, c->ann_len);
            p += c->ann_len;
        }
    }

    return (size_t)(p - buf);
}

/* ----- parse -------------------------------------------------------------- */

tdc_status tdc_schema_parse(const uint8_t *buf, size_t buf_size,
                            void *(*realloc_fn)(void *user, void *ptr, size_t sz),
                            void *alloc_user,
                            uint16_t *out_n_columns,
                            tdc_column_desc **out_columns) {
    if (!buf || !realloc_fn || !out_n_columns || !out_columns)
        return TDC_E_INVAL;

    *out_n_columns = 0;
    *out_columns   = NULL;

    /* Need at least 2 bytes for n_columns. */
    if (buf_size < 2) return TDC_E_CORRUPT;

    uint16_t n_columns = tdc_le_load_u16(buf);
    size_t pos = 2;

    if (n_columns == 0) {
        *out_n_columns = 0;
        return TDC_OK;
    }

    /* Allocate column descriptor array. */
    tdc_column_desc *cols = (tdc_column_desc *)realloc_fn(
        alloc_user, NULL, (size_t)n_columns * sizeof(tdc_column_desc));
    if (!cols) return TDC_E_NOMEM;
    memset(cols, 0, (size_t)n_columns * sizeof(tdc_column_desc));

    for (uint16_t i = 0; i < n_columns; ++i) {
        /* name_len (u16) */
        if (pos + 2 > buf_size) goto corrupt;
        uint16_t name_len = tdc_le_load_u16(buf + pos);
        pos += 2;

        /* Column must have a name. */
        if (name_len == 0) goto corrupt;

        /* name bytes */
        if (pos + (size_t)name_len > buf_size) goto corrupt;

        /* Allocate name_len + 1 for null terminator. */
        char *name = (char *)realloc_fn(alloc_user, NULL,
                                        (size_t)name_len + 1);
        if (!name) goto nomem;
        memcpy(name, buf + pos, name_len);
        name[name_len] = '\0';
        pos += name_len;

        cols[i].name     = name;
        cols[i].name_len = name_len;

        /* dtype (u8) */
        if (pos + 1 > buf_size) goto corrupt;
        uint8_t dtype = buf[pos++];
        if (!schema_dtype_known(dtype)) goto corrupt;
        cols[i].dtype = dtype;

        /* ann_len (u16) */
        if (pos + 2 > buf_size) goto corrupt;
        uint16_t ann_len = tdc_le_load_u16(buf + pos);
        pos += 2;

        /* annotation bytes */
        if (ann_len > 0) {
            if (pos + (size_t)ann_len > buf_size) goto corrupt;
            char *ann = (char *)realloc_fn(alloc_user, NULL,
                                           (size_t)ann_len + 1);
            if (!ann) goto nomem;
            memcpy(ann, buf + pos, ann_len);
            ann[ann_len] = '\0';
            pos += ann_len;
            cols[i].annotation = ann;
            cols[i].ann_len    = ann_len;
        } else {
            cols[i].annotation = NULL;
            cols[i].ann_len    = 0;
        }
    }

    /* Verify we consumed exactly buf_size bytes. */
    if (pos != buf_size) goto corrupt;

    *out_n_columns = n_columns;
    *out_columns   = cols;
    return TDC_OK;

corrupt:
    tdc_schema_free(cols, n_columns, realloc_fn, alloc_user);
    return TDC_E_CORRUPT;

nomem:
    tdc_schema_free(cols, n_columns, realloc_fn, alloc_user);
    return TDC_E_NOMEM;
}

/* ----- free --------------------------------------------------------------- */

void tdc_schema_free(tdc_column_desc *columns, uint16_t n_columns,
                     void *(*realloc_fn)(void *user, void *ptr, size_t sz),
                     void *alloc_user) {
    if (!columns || !realloc_fn) return;

    for (uint16_t i = 0; i < n_columns; ++i) {
        if (columns[i].name)
            realloc_fn(alloc_user, (void *)(uintptr_t)columns[i].name, 0);
        if (columns[i].annotation)
            realloc_fn(alloc_user, (void *)(uintptr_t)columns[i].annotation, 0);
    }
    realloc_fn(alloc_user, columns, 0);
}
