#include "csv_write.h"
#include "array.h"
#include "batch.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

/* Write a CSV-quoted string to file. Wraps in double quotes, escaping
   internal double quotes by doubling them. */
static void csv_write_quoted(FILE *fp, const char *s, int64_t len) {
    fputc('"', fp);
    for (int64_t i = 0; i < len; i++) {
        if (s[i] == '"') fputc('"', fp);
        fputc(s[i], fp);
    }
    fputc('"', fp);
}

/* Does the string need quoting? (contains comma, quote, newline, or CR) */
static int csv_needs_quote(const char *s, int64_t len) {
    for (int64_t i = 0; i < len; i++) {
        char c = s[i];
        if (c == ',' || c == '"' || c == '\n' || c == '\r')
            return 1;
    }
    return 0;
}

/* Write one cell value for a column at a given physical row index. */
static void csv_write_cell(FILE *fp, const VecArray *col, int64_t row) {
    if (!vec_array_is_valid(col, row)) {
        fputs("NA", fp);
        return;
    }

    switch (col->type) {
    case VEC_INT64:
        fprintf(fp, "%" PRId64, col->buf.i64[row]);
        break;
    case VEC_DOUBLE: {
        double v = col->buf.dbl[row];
        /* Use enough precision to round-trip */
        fprintf(fp, "%.17g", v);
        break;
    }
    case VEC_BOOL:
        fputs(col->buf.bln[row] ? "TRUE" : "FALSE", fp);
        break;
    case VEC_STRING: {
        int64_t s = col->buf.str.offsets[row];
        int64_t e = col->buf.str.offsets[row + 1];
        int64_t len = e - s;
        const char *data = col->buf.str.data + s;
        if (csv_needs_quote(data, len))
            csv_write_quoted(fp, data, len);
        else
            fwrite(data, 1, (size_t)len, fp);
        break;
    }
    }
}

void csv_write_node(VecNode *node, const char *path) {
    FILE *fp = fopen(path, "wb");
    if (!fp)
        vectra_error("cannot open file for writing: %s", path);

    int n_cols = node->output_schema.n_cols;

    /* Write header */
    for (int c = 0; c < n_cols; c++) {
        if (c > 0) fputc(',', fp);
        const char *name = node->output_schema.col_names[c];
        int64_t nlen = (int64_t)strlen(name);
        if (csv_needs_quote(name, nlen))
            csv_write_quoted(fp, name, nlen);
        else
            fputs(name, fp);
    }
    fputc('\n', fp);

    /* Stream batches */
    VecBatch *batch;
    while ((batch = node->next_batch(node)) != NULL) {
        int64_t n_logical = vec_batch_logical_rows(batch);
        for (int64_t li = 0; li < n_logical; li++) {
            int64_t pi = vec_batch_physical_row(batch, li);
            for (int c = 0; c < n_cols; c++) {
                if (c > 0) fputc(',', fp);
                csv_write_cell(fp, &batch->columns[c], pi);
            }
            fputc('\n', fp);
        }
        vec_batch_free(batch);
    }

    fclose(fp);
}
