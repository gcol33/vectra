/*
 * src/format/schema_internal.h
 *
 * Internal (non-public) header for the optional column schema section that
 * follows the 64-byte container header in TDC1 v2 containers.
 *
 * Wire format (all little-endian):
 *
 *   offset 0:   u16  n_columns
 *   per column: u16  name_len
 *               name_len bytes (UTF-8, NOT null-terminated on disk)
 *               u8   dtype     (tdc_dtype)
 *               u16  ann_len
 *               ann_len bytes  (UTF-8, NOT null-terminated on disk)
 *
 * The container header carries a schema_size (u32) so the reader can skip
 * the entire section without parsing it. schema_size == 0 means no schema;
 * schema_size > 0 with n_columns == 0 means "schema present but empty".
 */

#ifndef TDC_FORMAT_SCHEMA_INTERNAL_H
#define TDC_FORMAT_SCHEMA_INTERNAL_H

#include "tdc/stream.h"   /* tdc_column_desc, tdc_schema */

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Compute the serialized byte size of a schema section (NOT including
 * the schema_size field in the container header).
 * Returns 0 if schema is NULL or n_columns == 0.
 */
size_t tdc_schema_serialized_size(const tdc_schema *schema);

/*
 * Serialize schema into buf (which must be at least
 * tdc_schema_serialized_size(schema) bytes). Returns bytes written.
 */
size_t tdc_schema_serialize(const tdc_schema *schema, uint8_t *buf);

/*
 * Parse a schema section from buf of size buf_size. Allocates the
 * column descriptors and string buffers via realloc_fn. The caller
 * must free the returned tdc_column_desc array and the strings within
 * via tdc_schema_free.
 *
 * On success, *out_n_columns and *out_columns are set.
 * Returns TDC_OK or TDC_E_CORRUPT / TDC_E_NOMEM.
 */
tdc_status tdc_schema_parse(const uint8_t *buf, size_t buf_size,
                            void *(*realloc_fn)(void *user, void *ptr, size_t sz),
                            void *alloc_user,
                            uint16_t *out_n_columns,
                            tdc_column_desc **out_columns);

/*
 * Free a parsed schema's column array and its owned strings.
 */
void tdc_schema_free(tdc_column_desc *columns, uint16_t n_columns,
                     void *(*realloc_fn)(void *user, void *ptr, size_t sz),
                     void *alloc_user);

#ifdef __cplusplus
}
#endif
#endif /* TDC_FORMAT_SCHEMA_INTERNAL_H */
