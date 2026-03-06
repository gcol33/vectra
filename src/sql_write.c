#include "sql_write.h"
#include "sqlite_format.h"
#include "array.h"
#include "batch.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

static const char *vectype_to_sqltype(VecType t) {
    switch (t) {
    case VEC_INT64:  return "INTEGER";
    case VEC_DOUBLE: return "REAL";
    case VEC_BOOL:   return "INTEGER";
    case VEC_STRING: return "TEXT";
    }
    return "TEXT";
}

void sql_write_node(VecNode *node, const char *path, const char *table_name) {
    const VecSchema *schema = &node->output_schema;
    int n_cols = schema->n_cols;

    const char *col_names[256];
    const char *col_types[256];
    if (n_cols > 256) vectra_error("write_sqlite: more than 256 columns");

    for (int c = 0; c < n_cols; c++) {
        col_names[c] = schema->col_names[c];
        col_types[c] = vectype_to_sqltype(schema->col_types[c]);
    }

    SqlfmtWriter *writer = NULL;
    if (sqlfmt_writer_create(path, table_name, n_cols,
                              col_names, col_types, &writer) != 0) {
        const char *msg = writer ? sqlfmt_writer_errmsg(writer) : "unknown";
        vectra_error("SQLite create failed: %s", msg);
    }

    VecBatch *batch;
    while ((batch = node->next_batch(node)) != NULL) {
        int64_t n_rows = vec_batch_logical_rows(batch);

        for (int64_t r = 0; r < n_rows; r++) {
            int64_t pr = vec_batch_physical_row(batch, r);

            for (int c = 0; c < n_cols; c++) {
                VecArray *col = &batch->columns[c];
                if (!vec_array_is_valid(col, pr)) {
                    sqlfmt_writer_bind_null(writer, c);
                    continue;
                }
                switch (col->type) {
                case VEC_INT64:
                    sqlfmt_writer_bind_int64(writer, c, col->buf.i64[pr]);
                    break;
                case VEC_DOUBLE:
                    sqlfmt_writer_bind_double(writer, c, col->buf.dbl[pr]);
                    break;
                case VEC_BOOL:
                    sqlfmt_writer_bind_int64(writer, c,
                                              (int64_t)col->buf.bln[pr]);
                    break;
                case VEC_STRING: {
                    int64_t start = col->buf.str.offsets[pr];
                    int64_t end = col->buf.str.offsets[pr + 1];
                    sqlfmt_writer_bind_text(writer, c,
                                             col->buf.str.data + start,
                                             (int)(end - start));
                    break;
                }
                }
            }
            sqlfmt_writer_insert(writer);
        }
        vec_batch_free(batch);
    }

    sqlfmt_writer_close(writer);
}
