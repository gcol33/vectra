#include "sql_scan.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/*  Type mapping from declared SQLite types                            */
/* ------------------------------------------------------------------ */

static VecType decltype_to_vectype(const char *decl) {
    if (!decl || decl[0] == '\0') return VEC_STRING;
    if (strstr(decl, "INT") || strstr(decl, "int")) return VEC_INT64;
    if (strstr(decl, "REAL") || strstr(decl, "real") ||
        strstr(decl, "FLOAT") || strstr(decl, "float") ||
        strstr(decl, "DOUBLE") || strstr(decl, "double") ||
        strstr(decl, "NUMERIC") || strstr(decl, "numeric"))
        return VEC_DOUBLE;
    if (strstr(decl, "BOOL") || strstr(decl, "bool")) return VEC_BOOL;
    return VEC_STRING;
}

/* ------------------------------------------------------------------ */
/*  Batch reading                                                      */
/* ------------------------------------------------------------------ */

static VecBatch *sql_read_batch(SqlScanNode *sn) {
    int n_cols = sn->n_cols;
    int64_t batch_size = sn->batch_size;

    /* Column accumulators (stack arrays, max 64 cols) */
    int64_t *col_i64[64];
    double  *col_dbl[64];
    uint8_t *col_bln[64];
    char   **col_str_ptrs[64];
    int64_t *col_str_lens[64];
    uint8_t *col_nulls[64];

    if (n_cols > 64) vectra_error("SQL scan: more than 64 columns not supported");

    int64_t rows_cap = batch_size < 1024 ? batch_size : 1024;

    for (int c = 0; c < n_cols; c++) {
        col_nulls[c] = (uint8_t *)calloc((size_t)rows_cap, sizeof(uint8_t));
        col_i64[c] = NULL; col_dbl[c] = NULL; col_bln[c] = NULL;
        col_str_ptrs[c] = NULL; col_str_lens[c] = NULL;
        switch (sn->col_types[c]) {
        case VEC_INT64:
            col_i64[c] = (int64_t *)malloc((size_t)rows_cap * sizeof(int64_t));
            break;
        case VEC_DOUBLE:
            col_dbl[c] = (double *)malloc((size_t)rows_cap * sizeof(double));
            break;
        case VEC_BOOL:
            col_bln[c] = (uint8_t *)malloc((size_t)rows_cap * sizeof(uint8_t));
            break;
        case VEC_STRING:
            col_str_ptrs[c] = (char **)malloc((size_t)rows_cap * sizeof(char *));
            col_str_lens[c] = (int64_t *)malloc((size_t)rows_cap * sizeof(int64_t));
            break;
        }
    }

    int64_t n_rows = 0;
    while (n_rows < batch_size && sqlfmt_reader_step(sn->reader) == 1) {
        /* Grow if needed */
        if (n_rows >= rows_cap) {
            rows_cap *= 2;
            for (int c = 0; c < n_cols; c++) {
                col_nulls[c] = (uint8_t *)realloc(col_nulls[c],
                    (size_t)rows_cap * sizeof(uint8_t));
                memset(col_nulls[c] + n_rows, 0,
                    (size_t)(rows_cap - n_rows));
                switch (sn->col_types[c]) {
                case VEC_INT64:
                    col_i64[c] = (int64_t *)realloc(col_i64[c],
                        (size_t)rows_cap * sizeof(int64_t)); break;
                case VEC_DOUBLE:
                    col_dbl[c] = (double *)realloc(col_dbl[c],
                        (size_t)rows_cap * sizeof(double)); break;
                case VEC_BOOL:
                    col_bln[c] = (uint8_t *)realloc(col_bln[c],
                        (size_t)rows_cap * sizeof(uint8_t)); break;
                case VEC_STRING:
                    col_str_ptrs[c] = (char **)realloc(col_str_ptrs[c],
                        (size_t)rows_cap * sizeof(char *));
                    col_str_lens[c] = (int64_t *)realloc(col_str_lens[c],
                        (size_t)rows_cap * sizeof(int64_t)); break;
                }
            }
        }

        for (int c = 0; c < n_cols; c++) {
            int ctype = sqlfmt_reader_col_type(sn->reader, c);
            if (ctype == SQLFMT_NULL) {
                col_nulls[c][n_rows] = 1;
                continue;
            }
            switch (sn->col_types[c]) {
            case VEC_INT64:
                col_i64[c][n_rows] = sqlfmt_reader_int64(sn->reader, c);
                break;
            case VEC_DOUBLE:
                col_dbl[c][n_rows] = sqlfmt_reader_double(sn->reader, c);
                break;
            case VEC_BOOL:
                col_bln[c][n_rows] =
                    (uint8_t)(sqlfmt_reader_int64(sn->reader, c) != 0);
                break;
            case VEC_STRING: {
                const char *txt = sqlfmt_reader_text(sn->reader, c);
                int len = sqlfmt_reader_bytes(sn->reader, c);
                char *copy = (char *)malloc((size_t)(len + 1));
                memcpy(copy, txt, (size_t)len);
                copy[len] = '\0';
                col_str_ptrs[c][n_rows] = copy;
                col_str_lens[c][n_rows] = len;
                break;
            }
            }
        }
        n_rows++;
    }

    if (n_rows == 0) {
        for (int c = 0; c < n_cols; c++) {
            free(col_nulls[c]); free(col_i64[c]); free(col_dbl[c]);
            free(col_bln[c]); free(col_str_ptrs[c]); free(col_str_lens[c]);
        }
        return NULL;
    }

    /* Build VecBatch */
    VecBatch *batch = vec_batch_alloc(n_cols, n_rows);
    for (int c = 0; c < n_cols; c++) {
        const char *nm = sn->base.output_schema.col_names[c];
        batch->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(batch->col_names[c], nm);
    }

    for (int c = 0; c < n_cols; c++) {
        VecType type = sn->col_types[c];
        VecArray arr = vec_array_alloc(type, n_rows);

        if (type == VEC_STRING) {
            int64_t total_len = 0;
            for (int64_t r = 0; r < n_rows; r++)
                if (!col_nulls[c][r]) total_len += col_str_lens[c][r];

            arr.buf.str.data = (char *)malloc(
                (size_t)(total_len > 0 ? total_len : 1));
            arr.buf.str.data_len = total_len;

            int64_t offset = 0;
            for (int64_t r = 0; r < n_rows; r++) {
                arr.buf.str.offsets[r] = offset;
                if (col_nulls[c][r]) {
                    vec_array_set_null(&arr, r);
                } else {
                    vec_array_set_valid(&arr, r);
                    memcpy(arr.buf.str.data + offset,
                           col_str_ptrs[c][r], (size_t)col_str_lens[c][r]);
                    offset += col_str_lens[c][r];
                    free(col_str_ptrs[c][r]);
                }
            }
            arr.buf.str.offsets[n_rows] = offset;
        } else {
            for (int64_t r = 0; r < n_rows; r++) {
                if (col_nulls[c][r]) {
                    vec_array_set_null(&arr, r);
                } else {
                    vec_array_set_valid(&arr, r);
                    switch (type) {
                    case VEC_INT64: arr.buf.i64[r] = col_i64[c][r]; break;
                    case VEC_DOUBLE: arr.buf.dbl[r] = col_dbl[c][r]; break;
                    case VEC_BOOL: arr.buf.bln[r] = col_bln[c][r]; break;
                    default: break;
                    }
                }
            }
        }

        batch->columns[c] = arr;
        /* Free remaining string pointers for null rows (before freeing col_nulls) */
        if (col_str_ptrs[c]) {
            for (int64_t r = 0; r < n_rows; r++)
                if (col_nulls[c] && col_nulls[c][r] && col_str_ptrs[c][r])
                    free(col_str_ptrs[c][r]);
            free(col_str_ptrs[c]);
        }
        if (col_str_lens[c]) free(col_str_lens[c]);
        free(col_nulls[c]); free(col_i64[c]); free(col_dbl[c]);
        free(col_bln[c]);
    }

    return batch;
}

/* ------------------------------------------------------------------ */
/*  VecNode vtable                                                     */
/* ------------------------------------------------------------------ */

static VecBatch *sql_scan_next_batch(VecNode *self) {
    SqlScanNode *sn = (SqlScanNode *)self;
    if (sn->exhausted) return NULL;
    VecBatch *batch = sql_read_batch(sn);
    if (!batch) { sn->exhausted = 1; return NULL; }
    return batch;
}

static void sql_scan_free(VecNode *self) {
    SqlScanNode *sn = (SqlScanNode *)self;
    sqlfmt_reader_close(sn->reader);
    free(sn->col_types);
    vec_schema_free(&sn->base.output_schema);
    free(sn);
}

/* ------------------------------------------------------------------ */
/*  Constructor                                                        */
/* ------------------------------------------------------------------ */

SqlScanNode *sql_scan_node_create(const char *path, const char *table,
                                   int64_t batch_size) {
    SqlfmtReader *reader = NULL;
    if (sqlfmt_reader_open(path, table, &reader) != 0) {
        const char *msg = reader ? sqlfmt_reader_errmsg(reader) : "unknown";
        sqlfmt_reader_close(reader);
        vectra_error("SQLite open failed: %s", msg);
    }

    int n_cols = sqlfmt_reader_ncols(reader);

    /* Build column types from declared types */
    VecType *col_types = (VecType *)malloc((size_t)n_cols * sizeof(VecType));
    char **names = (char **)malloc((size_t)n_cols * sizeof(char *));
    for (int c = 0; c < n_cols; c++) {
        col_types[c] = decltype_to_vectype(sqlfmt_reader_coltype(reader, c));
        const char *nm = sqlfmt_reader_colname(reader, c);
        names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(names[c], nm);
    }

    VecSchema schema = vec_schema_create(n_cols, names, col_types);
    for (int c = 0; c < n_cols; c++) free(names[c]);
    free(names);

    SqlScanNode *sn = (SqlScanNode *)calloc(1, sizeof(SqlScanNode));
    if (!sn) vectra_error("alloc failed for SqlScanNode");

    sn->reader = reader;
    sn->n_cols = n_cols;
    sn->col_types = col_types;
    sn->batch_size = batch_size > 0 ? batch_size : 65536;
    sn->exhausted = 0;

    sn->base.output_schema = schema;
    sn->base.next_batch = sql_scan_next_batch;
    sn->base.free_node = sql_scan_free;
    sn->base.kind = "SqlScanNode";

    return sn;
}
