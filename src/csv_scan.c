#include "csv_scan.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <inttypes.h>
#include <float.h>
#include <math.h>

/* ------------------------------------------------------------------ */
/*  Growable buffer                                                    */
/* ------------------------------------------------------------------ */

typedef struct {
    char   *data;
    int64_t len;
    int64_t cap;
} GBuf;

static void gbuf_init(GBuf *g) {
    g->cap = 256;
    g->data = (char *)malloc((size_t)g->cap);
    if (!g->data) vectra_error("alloc failed for GBuf");
    g->len = 0;
}

static void gbuf_push(GBuf *g, char c) {
    if (g->len >= g->cap) {
        g->cap *= 2;
        g->data = (char *)realloc(g->data, (size_t)g->cap);
        if (!g->data) vectra_error("realloc failed for GBuf");
    }
    g->data[g->len++] = c;
}

static void gbuf_clear(GBuf *g) { g->len = 0; }
static void gbuf_free(GBuf *g) { free(g->data); g->data = NULL; }

/* Null-terminate and return pointer (valid until next push/clear) */
static const char *gbuf_str(GBuf *g) {
    gbuf_push(g, '\0');
    g->len--; /* don't count terminator in logical length */
    return g->data;
}

/* ------------------------------------------------------------------ */
/*  Growable pointer array for fields                                  */
/* ------------------------------------------------------------------ */

typedef struct {
    char  **items;
    int     n;
    int     cap;
} FieldVec;

static void fv_init(FieldVec *v) {
    v->cap = 32;
    v->items = (char **)malloc((size_t)v->cap * sizeof(char *));
    if (!v->items) vectra_error("alloc failed for FieldVec");
    v->n = 0;
}

static void fv_push(FieldVec *v, const char *s, int64_t len) {
    if (v->n >= v->cap) {
        v->cap *= 2;
        v->items = (char **)realloc(v->items, (size_t)v->cap * sizeof(char *));
        if (!v->items) vectra_error("realloc failed for FieldVec");
    }
    char *copy = (char *)malloc((size_t)(len + 1));
    if (!copy) vectra_error("alloc failed for field copy");
    memcpy(copy, s, (size_t)len);
    copy[len] = '\0';
    v->items[v->n++] = copy;
}

static void fv_free_items(FieldVec *v) {
    for (int i = 0; i < v->n; i++) free(v->items[i]);
    v->n = 0;
}

static void fv_free(FieldVec *v) {
    fv_free_items(v);
    free(v->items);
    v->items = NULL;
}

/* ------------------------------------------------------------------ */
/*  RFC 4180 CSV line parser                                           */
/* ------------------------------------------------------------------ */

/* Read one logical CSV line (handles quoted fields with embedded newlines).
   Preserves raw content including quotes so csv_split_fields can parse them.
   Returns 0 on success, -1 on EOF before any data. */
static int csv_read_line(FILE *fp, GBuf *line) {
    gbuf_clear(line);
    int c = fgetc(fp);
    if (c == EOF) return -1;

    int in_quote = 0;
    while (c != EOF) {
        if (in_quote) {
            gbuf_push(line, (char)c);
            if (c == '"') {
                int next = fgetc(fp);
                if (next == '"') {
                    /* Escaped quote — keep both */
                    gbuf_push(line, (char)next);
                } else {
                    /* End of quoted field */
                    in_quote = 0;
                    if (next == EOF) break;
                    if (next == '\n') break;
                    if (next == '\r') {
                        int peek = fgetc(fp);
                        if (peek != '\n' && peek != EOF) ungetc(peek, fp);
                        break;
                    }
                    /* Push whatever follows the closing quote */
                    gbuf_push(line, (char)next);
                }
            }
        } else {
            if (c == '"') {
                in_quote = 1;
                gbuf_push(line, (char)c);
            } else if (c == '\n') {
                break;
            } else if (c == '\r') {
                int peek = fgetc(fp);
                if (peek != '\n' && peek != EOF) ungetc(peek, fp);
                break;
            } else {
                gbuf_push(line, (char)c);
            }
        }
        c = fgetc(fp);
    }
    return 0;
}

/* Split a raw CSV line into fields. Handles quoted fields.
   The line buffer has already resolved embedded newlines in csv_read_line,
   but we still need to handle quoting for commas and quotes within fields. */
static void csv_split_fields(const char *raw, int64_t raw_len,
                              FieldVec *fields) {
    fv_free_items(fields);
    GBuf field;
    gbuf_init(&field);

    int64_t i = 0;
    while (i <= raw_len) {
        gbuf_clear(&field);
        if (i < raw_len && raw[i] == '"') {
            /* Quoted field */
            i++; /* skip opening quote */
            while (i < raw_len) {
                if (raw[i] == '"') {
                    if (i + 1 < raw_len && raw[i + 1] == '"') {
                        gbuf_push(&field, '"');
                        i += 2;
                    } else {
                        i++; /* skip closing quote */
                        break;
                    }
                } else {
                    gbuf_push(&field, raw[i]);
                    i++;
                }
            }
            /* Skip to comma or end */
            while (i < raw_len && raw[i] != ',') i++;
        } else {
            /* Unquoted field */
            while (i < raw_len && raw[i] != ',') {
                gbuf_push(&field, raw[i]);
                i++;
            }
        }
        const char *s = gbuf_str(&field);
        fv_push(fields, s, field.len);
        if (i < raw_len && raw[i] == ',') i++; /* skip comma */
        else break;
    }
    gbuf_free(&field);
}

/* ------------------------------------------------------------------ */
/*  Type inference                                                     */
/* ------------------------------------------------------------------ */

static int is_na_field(const char *s) {
    return (s[0] == '\0') ||
           (s[0] == 'N' && s[1] == 'A' && s[2] == '\0') ||
           (s[0] == 'n' && s[1] == 'a' && s[2] == '\0') ||
           (s[0] == 'N' && s[1] == 'a' && s[2] == '\0');
}

static int try_parse_int64(const char *s, int64_t *out) {
    if (s[0] == '\0') return 0;
    char *end;
    errno = 0;
    long long v = strtoll(s, &end, 10);
    if (errno != 0 || *end != '\0') return 0;
    *out = (int64_t)v;
    return 1;
}

static int try_parse_double(const char *s, double *out) {
    if (s[0] == '\0') return 0;
    char *end;
    errno = 0;
    double v = strtod(s, &end);
    if (errno != 0 || *end != '\0') return 0;
    (void)v;
    *out = v;
    return 1;
}

static int try_parse_bool(const char *s) {
    if (strcmp(s, "TRUE") == 0 || strcmp(s, "true") == 0 ||
        strcmp(s, "FALSE") == 0 || strcmp(s, "false") == 0 ||
        strcmp(s, "True") == 0 || strcmp(s, "False") == 0)
        return 1;
    return 0;
}

/* Infer types by reading up to infer_n rows from current position.
   Seeks back to original position when done. */
static VecType *csv_infer_types(FILE *fp, int n_cols, int64_t infer_n) {
    long start_pos = ftell(fp);

    /* Start everything as unknown (use -1 as sentinel) */
    int *state = (int *)calloc((size_t)n_cols, sizeof(int));
    /* state: 0=unknown, 1=int64, 2=double, 3=bool, 4=string */

    GBuf line;
    gbuf_init(&line);
    FieldVec fields;
    fv_init(&fields);

    int64_t rows_read = 0;
    while (rows_read < infer_n && csv_read_line(fp, &line) == 0) {
        csv_split_fields(line.data, line.len, &fields);
        int nc = fields.n < n_cols ? fields.n : n_cols;
        for (int c = 0; c < nc; c++) {
            const char *val = fields.items[c];
            if (is_na_field(val)) continue; /* NA doesn't constrain type */

            if (state[c] == 4) continue; /* already string, can't upgrade */

            int64_t iv;
            double dv;
            if (state[c] == 0) {
                /* First non-NA value — try types in order */
                if (try_parse_bool(val))
                    state[c] = 3;
                else if (try_parse_int64(val, &iv))
                    state[c] = 1;
                else if (try_parse_double(val, &dv))
                    state[c] = 2;
                else
                    state[c] = 4;
            } else if (state[c] == 3) {
                /* Was bool — check if still bool */
                if (!try_parse_bool(val))
                    state[c] = 4; /* demote to string */
            } else if (state[c] == 1) {
                /* Was int — check if still int, else try double */
                if (!try_parse_int64(val, &iv)) {
                    if (try_parse_double(val, &dv))
                        state[c] = 2;
                    else
                        state[c] = 4;
                }
            } else if (state[c] == 2) {
                /* Was double — check if still double */
                if (!try_parse_double(val, &dv))
                    state[c] = 4;
            }
        }
        rows_read++;
    }

    gbuf_free(&line);
    fv_free(&fields);

    /* Map states to VecType */
    VecType *types = (VecType *)malloc((size_t)n_cols * sizeof(VecType));
    if (!types) vectra_error("alloc failed for col types");
    for (int c = 0; c < n_cols; c++) {
        switch (state[c]) {
        case 1:  types[c] = VEC_INT64;  break;
        case 2:  types[c] = VEC_DOUBLE; break;
        case 3:  types[c] = VEC_BOOL;   break;
        default: types[c] = VEC_STRING; break; /* 0 (all NA) and 4 */
        }
    }
    free(state);

    fseek(fp, start_pos, SEEK_SET);
    return types;
}

/* ------------------------------------------------------------------ */
/*  Batch reading                                                      */
/* ------------------------------------------------------------------ */

/* Parse a field value into the appropriate column array at row index i.
   Sets validity bits. */
static void csv_parse_cell(VecArray *col, int64_t i, const char *val) {
    if (is_na_field(val)) {
        vec_array_set_null(col, i);
        return;
    }
    vec_array_set_valid(col, i);

    switch (col->type) {
    case VEC_INT64: {
        int64_t v;
        if (try_parse_int64(val, &v))
            col->buf.i64[i] = v;
        else
            vec_array_set_null(col, i); /* shouldn't happen after inference */
        break;
    }
    case VEC_DOUBLE: {
        double v;
        if (try_parse_double(val, &v))
            col->buf.dbl[i] = v;
        else
            vec_array_set_null(col, i);
        break;
    }
    case VEC_BOOL:
        if (strcmp(val, "TRUE") == 0 || strcmp(val, "true") == 0 ||
            strcmp(val, "True") == 0)
            col->buf.bln[i] = 1;
        else
            col->buf.bln[i] = 0;
        break;
    case VEC_STRING:
        /* Strings are accumulated in a second pass — handled by caller */
        break;
    }
}

/* Read up to batch_size rows into a VecBatch. Returns NULL on EOF. */
static VecBatch *csv_read_batch(CsvScanNode *sn) {
    int n_cols = sn->n_file_cols;
    int64_t batch_size = sn->batch_size;

    /* First pass: read raw lines into memory */
    GBuf line;
    gbuf_init(&line);

    /* Store raw field values: fields_store[row][col] */
    int64_t rows_cap = batch_size < 1024 ? batch_size : 1024;
    char ***rows_data = (char ***)malloc((size_t)rows_cap * sizeof(char **));
    if (!rows_data) vectra_error("alloc failed for CSV rows");
    int64_t n_rows = 0;

    FieldVec fields;
    fv_init(&fields);

    while (n_rows < batch_size && csv_read_line(sn->fp, &line) == 0) {
        /* Skip completely blank lines */
        if (line.len == 0) continue;

        csv_split_fields(line.data, line.len, &fields);

        if (n_rows >= rows_cap) {
            rows_cap *= 2;
            rows_data = (char ***)realloc(rows_data,
                                           (size_t)rows_cap * sizeof(char **));
            if (!rows_data) vectra_error("realloc failed for CSV rows");
        }

        /* Copy field values */
        char **row = (char **)malloc((size_t)n_cols * sizeof(char *));
        if (!row) vectra_error("alloc failed for row fields");
        for (int c = 0; c < n_cols; c++) {
            if (c < fields.n) {
                int64_t len = (int64_t)strlen(fields.items[c]);
                row[c] = (char *)malloc((size_t)(len + 1));
                memcpy(row[c], fields.items[c], (size_t)(len + 1));
            } else {
                /* Missing field → NA */
                row[c] = (char *)malloc(1);
                row[c][0] = '\0';
            }
        }
        rows_data[n_rows] = row;
        n_rows++;
    }

    gbuf_free(&line);
    fv_free(&fields);

    if (n_rows == 0) {
        free(rows_data);
        return NULL;
    }

    /* Build VecBatch */
    VecBatch *batch = vec_batch_alloc(n_cols, n_rows);
    for (int c = 0; c < n_cols; c++) {
        const char *nm = sn->base.output_schema.col_names[c];
        batch->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(batch->col_names[c], nm);
    }

    /* Allocate columns */
    for (int c = 0; c < n_cols; c++) {
        VecType type = sn->col_types[c];
        VecArray arr = vec_array_alloc(type, n_rows);

        if (type == VEC_STRING) {
            /* Two-pass for strings: compute total length, then fill */
            int64_t total_len = 0;
            for (int64_t r = 0; r < n_rows; r++) {
                if (!is_na_field(rows_data[r][c]))
                    total_len += (int64_t)strlen(rows_data[r][c]);
            }
            arr.buf.str.data = (char *)malloc(
                (size_t)(total_len > 0 ? total_len : 1));
            if (!arr.buf.str.data)
                vectra_error("alloc failed for string data");
            arr.buf.str.data_len = total_len;

            int64_t offset = 0;
            for (int64_t r = 0; r < n_rows; r++) {
                arr.buf.str.offsets[r] = offset;
                if (is_na_field(rows_data[r][c])) {
                    vec_array_set_null(&arr, r);
                } else {
                    vec_array_set_valid(&arr, r);
                    int64_t slen = (int64_t)strlen(rows_data[r][c]);
                    memcpy(arr.buf.str.data + offset,
                           rows_data[r][c], (size_t)slen);
                    offset += slen;
                }
            }
            arr.buf.str.offsets[n_rows] = offset;
        } else {
            for (int64_t r = 0; r < n_rows; r++)
                csv_parse_cell(&arr, r, rows_data[r][c]);
        }

        batch->columns[c] = arr;
    }

    /* Free raw data */
    for (int64_t r = 0; r < n_rows; r++) {
        for (int c = 0; c < n_cols; c++)
            free(rows_data[r][c]);
        free(rows_data[r]);
    }
    free(rows_data);

    return batch;
}

/* ------------------------------------------------------------------ */
/*  VecNode vtable                                                     */
/* ------------------------------------------------------------------ */

static VecBatch *csv_scan_next_batch(VecNode *self) {
    CsvScanNode *sn = (CsvScanNode *)self;
    if (sn->exhausted) return NULL;

    VecBatch *batch = csv_read_batch(sn);
    if (!batch) {
        sn->exhausted = 1;
        return NULL;
    }
    return batch;
}

static void csv_scan_free(VecNode *self) {
    CsvScanNode *sn = (CsvScanNode *)self;
    if (sn->fp) fclose(sn->fp);
    free(sn->col_types);
    vec_schema_free(&sn->base.output_schema);
    free(sn);
}

/* ------------------------------------------------------------------ */
/*  Constructor                                                        */
/* ------------------------------------------------------------------ */

#define CSV_INFER_ROWS 1000

CsvScanNode *csv_scan_node_create(const char *path, int64_t batch_size) {
    FILE *fp = fopen(path, "rb");
    if (!fp) vectra_error("cannot open CSV file: %s", path);

    /* Read header line */
    GBuf line;
    gbuf_init(&line);
    if (csv_read_line(fp, &line) != 0) {
        gbuf_free(&line);
        fclose(fp);
        vectra_error("CSV file is empty: %s", path);
    }

    FieldVec header_fields;
    fv_init(&header_fields);
    csv_split_fields(line.data, line.len, &header_fields);
    gbuf_free(&line);

    int n_cols = header_fields.n;
    if (n_cols == 0) {
        fv_free(&header_fields);
        fclose(fp);
        vectra_error("CSV header has no columns: %s", path);
    }

    /* Record data start position */
    long data_start = ftell(fp);

    /* Infer types from first N rows */
    VecType *col_types = csv_infer_types(fp, n_cols, CSV_INFER_ROWS);

    /* Seek back to data start for reading */
    fseek(fp, data_start, SEEK_SET);

    /* Build schema */
    char **names = (char **)malloc((size_t)n_cols * sizeof(char *));
    for (int i = 0; i < n_cols; i++)
        names[i] = header_fields.items[i];

    VecSchema schema = vec_schema_create(n_cols, names, col_types);
    free(names); /* schema deep-copies */

    /* Create node */
    CsvScanNode *sn = (CsvScanNode *)calloc(1, sizeof(CsvScanNode));
    if (!sn) vectra_error("alloc failed for CsvScanNode");

    sn->fp = fp;
    sn->data_start = data_start;
    sn->n_file_cols = n_cols;
    sn->col_types = col_types;
    sn->batch_size = batch_size > 0 ? batch_size : 65536;
    sn->exhausted = 0;

    sn->base.output_schema = schema;
    sn->base.next_batch = csv_scan_next_batch;
    sn->base.free_node = csv_scan_free;
    sn->base.kind = "CsvScanNode";

    fv_free(&header_fields);

    return sn;
}
