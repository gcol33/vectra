#include "bed_scan.h"
#include "byte_reader.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "error.h"
#include "grow.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <inttypes.h>

/* ------------------------------------------------------------------ */
/*  BED column schema                                                  */
/* ------------------------------------------------------------------ */

/* The standard BED fields, in order. A file is BED3..BED12; columns past
   the twelfth (a "BED N+" file's extra fields) are named V13, V14, ... and
   kept as strings. start (col 1) and end (col 2) are 0-based / half-open and
   parsed strictly; the other integer fields tolerate "." / "NA" -> NA. */
#define BED_MAX_NAMED 12

static const char *BED_NAMES[BED_MAX_NAMED] = {
    "chrom", "start", "end", "name", "score", "strand",
    "thickStart", "thickEnd", "itemRgb", "blockCount",
    "blockSizes", "blockStarts"
};
static const VecType BED_TYPES[BED_MAX_NAMED] = {
    VEC_STRING, VEC_INT64, VEC_INT64, VEC_STRING, VEC_INT64, VEC_STRING,
    VEC_INT64,  VEC_INT64, VEC_STRING, VEC_INT64, VEC_STRING, VEC_STRING
};

static VecType bed_col_type(int c) {
    return c < BED_MAX_NAMED ? BED_TYPES[c] : VEC_STRING;
}

/* Fill name into buf ("V<c+1>" for extra columns). buf must hold >= 16 bytes. */
static const char *bed_col_name(int c, char *buf) {
    if (c < BED_MAX_NAMED) return BED_NAMES[c];
    snprintf(buf, 16, "V%d", c + 1);
    return buf;
}

/* ------------------------------------------------------------------ */
/*  Growable byte buffer                                               */
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

static void gbuf_clear(GBuf *g) { g->len = 0; }
static void gbuf_free(GBuf *g) { free(g->data); g->data = NULL; }

static void gbuf_push(GBuf *g, char c) {
    vec_grow_to((void **)&g->data, &g->cap, g->len + 1, sizeof(char), "GBuf");
    g->data[g->len++] = c;
}

/* ------------------------------------------------------------------ */
/*  Growable pointer array for fields                                  */
/* ------------------------------------------------------------------ */

typedef struct {
    char    **items;
    int64_t   n;
    int64_t   cap;
} FieldVec;

static void fv_init(FieldVec *v) {
    v->cap = 16;
    v->items = (char **)malloc((size_t)v->cap * sizeof(char *));
    if (!v->items) vectra_error("alloc failed for FieldVec");
    v->n = 0;
}

static void fv_push(FieldVec *v, const char *s, int64_t len) {
    vec_grow_to((void **)&v->items, &v->cap, v->n + 1, sizeof(char *),
                "FieldVec");
    char *copy = (char *)malloc((size_t)(len + 1));
    if (!copy) vectra_error("alloc failed for field copy");
    memcpy(copy, s, (size_t)len);
    copy[len] = '\0';
    v->items[v->n++] = copy;
}

static void fv_free_items(FieldVec *v) {
    for (int64_t i = 0; i < v->n; i++) free(v->items[i]);
    v->n = 0;
}

static void fv_free(FieldVec *v) {
    fv_free_items(v);
    free(v->items);
    v->items = NULL;
}

/* ------------------------------------------------------------------ */
/*  Line reading and field splitting                                   */
/* ------------------------------------------------------------------ */

/* Read one physical line into g (trailing CR/LF stripped). Returns 1 if a
   line was read (possibly empty), 0 at EOF before any byte. */
static int read_line(ByteReader *rd, GBuf *g) {
    gbuf_clear(g);
    int c = rd->getc_fn(rd);
    if (c == -1) return 0;
    while (c != -1 && c != '\n') {
        if (c != '\r') gbuf_push(g, (char)c);
        c = rd->getc_fn(rd);
    }
    return 1;
}

/* A comment / header line skipped by the scan: blank, '#', or a UCSC
   `track`/`browser` directive (the whole first token). */
static int is_skip_line(const char *s, int64_t len) {
    int64_t i = 0;
    while (i < len && (s[i] == ' ' || s[i] == '\t')) i++;
    if (i == len) return 1;              /* blank / all whitespace */
    if (s[i] == '#') return 1;           /* comment */
    if (len - i >= 5 && memcmp(s + i, "track", 5) == 0 &&
        (i + 5 == len || s[i + 5] == ' ' || s[i + 5] == '\t'))
        return 1;
    if (len - i >= 7 && memcmp(s + i, "browser", 7) == 0 &&
        (i + 7 == len || s[i + 7] == ' ' || s[i + 7] == '\t'))
        return 1;
    return 0;
}

/* Split a line into whitespace-delimited fields (runs of space/tab are one
   separator; leading/trailing whitespace ignored), matching the BED dialect
   used by rtracklayer and bedtools. */
static void bed_split_fields(const char *line, int64_t len, FieldVec *fields) {
    fv_free_items(fields);
    int64_t i = 0;
    while (i < len) {
        while (i < len && (line[i] == ' ' || line[i] == '\t')) i++;
        if (i >= len) break;
        int64_t start = i;
        while (i < len && line[i] != ' ' && line[i] != '\t') i++;
        fv_push(fields, line + start, i - start);
    }
}

/* ------------------------------------------------------------------ */
/*  Integer field parsing                                              */
/* ------------------------------------------------------------------ */

static int field_is_na(const char *s) {
    return s[0] == '\0' ||
           (s[0] == '.' && s[1] == '\0') ||
           (s[0] == 'N' && s[1] == 'A' && s[2] == '\0');
}

/* Parse a whole-string int64. Returns 1 on success, 0 if not an integer. */
static int parse_i64(const char *s, int64_t *out) {
    if (s[0] == '\0') return 0;
    char *end;
    errno = 0;
    long long v = strtoll(s, &end, 10);
    if (errno != 0 || *end != '\0') return 0;
    *out = (int64_t)v;
    return 1;
}

/* ------------------------------------------------------------------ */
/*  Batch reading                                                      */
/* ------------------------------------------------------------------ */

static VecBatch *bed_read_batch(BedScanNode *sn) {
    int n_cols = sn->n_cols;
    int64_t batch_size = sn->batch_size;

    GBuf line;
    gbuf_init(&line);
    FieldVec fields;
    fv_init(&fields);

    int64_t rows_cap = batch_size < 1024 ? batch_size : 1024;
    if (rows_cap < 1) rows_cap = 1;
    char ***rows_data = (char ***)malloc((size_t)rows_cap * sizeof(char **));
    if (!rows_data) vectra_error("alloc failed for BED rows");
    int64_t n_rows = 0;

    while (n_rows < batch_size && read_line(sn->reader, &line)) {
        if (is_skip_line(line.data, line.len)) continue;

        bed_split_fields(line.data, line.len, &fields);
        if (fields.n != n_cols)
            vectra_error("BED line has %" PRId64 " fields, expected %d, in %s",
                         fields.n, n_cols, sn->path);

        if (n_rows >= rows_cap) {
            rows_cap *= 2;
            rows_data = (char ***)realloc(rows_data,
                                          (size_t)rows_cap * sizeof(char **));
            if (!rows_data) vectra_error("realloc failed for BED rows");
        }

        char **row = (char **)malloc((size_t)n_cols * sizeof(char *));
        if (!row) vectra_error("alloc failed for BED row");
        for (int c = 0; c < n_cols; c++) {
            int64_t flen = (int64_t)strlen(fields.items[c]);
            row[c] = (char *)malloc((size_t)(flen + 1));
            if (!row[c]) vectra_error("alloc failed for BED field");
            memcpy(row[c], fields.items[c], (size_t)(flen + 1));
        }
        rows_data[n_rows++] = row;
    }

    gbuf_free(&line);
    fv_free(&fields);

    if (n_rows == 0) {
        free(rows_data);
        return NULL;
    }

    VecBatch *batch = vec_batch_alloc(n_cols, n_rows);
    for (int c = 0; c < n_cols; c++) {
        const char *nm = sn->base.output_schema.col_names[c];
        size_t nm_len = strlen(nm);
        batch->col_names[c] = (char *)malloc(nm_len + 1);
        memcpy(batch->col_names[c], nm, nm_len + 1);
    }

    for (int c = 0; c < n_cols; c++) {
        VecType type = bed_col_type(c);
        VecArray arr = vec_array_alloc(type, n_rows);

        if (type == VEC_STRING) {
            int64_t total = 0;
            for (int64_t r = 0; r < n_rows; r++)
                total += (int64_t)strlen(rows_data[r][c]);
            free(arr.buf.str.data); /* free 1-byte placeholder */
            arr.buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
            if (!arr.buf.str.data)
                vectra_error("alloc failed for BED string data");
            arr.buf.str.data_len = total;

            int64_t off = 0;
            for (int64_t r = 0; r < n_rows; r++) {
                arr.buf.str.offsets[r] = off;
                vec_array_set_valid(&arr, r);
                int64_t slen = (int64_t)strlen(rows_data[r][c]);
                if (slen > 0) {
                    memcpy(arr.buf.str.data + off, rows_data[r][c],
                           (size_t)slen);
                    off += slen;
                }
            }
            arr.buf.str.offsets[n_rows] = off;
        } else { /* VEC_INT64: start/end strict, other int fields lenient */
            int required = (c == 1 || c == 2);
            for (int64_t r = 0; r < n_rows; r++) {
                const char *val = rows_data[r][c];
                int64_t iv;
                if (required) {
                    if (!parse_i64(val, &iv))
                        vectra_error("malformed BED: non-integer %s '%s' in %s",
                                     c == 1 ? "start" : "end", val, sn->path);
                    vec_array_set_valid(&arr, r);
                    arr.buf.i64[r] = iv;
                } else if (field_is_na(val) || !parse_i64(val, &iv)) {
                    vec_array_set_null(&arr, r);
                } else {
                    vec_array_set_valid(&arr, r);
                    arr.buf.i64[r] = iv;
                }
            }
        }

        batch->columns[c] = arr;
    }

    for (int64_t r = 0; r < n_rows; r++) {
        for (int c = 0; c < n_cols; c++) free(rows_data[r][c]);
        free(rows_data[r]);
    }
    free(rows_data);

    sn->records_emitted += n_rows;
    return batch;
}

/* ------------------------------------------------------------------ */
/*  VecNode vtable                                                     */
/* ------------------------------------------------------------------ */

static void bed_log_total(BedScanNode *sn) {
    if (sn->quiet || sn->logged) return;
    sn->logged = 1;
    REprintf("vectra: scanned %" PRId64 " features from BED '%s'\n",
             sn->records_emitted, sn->path);
}

static VecBatch *bed_scan_next_batch(VecNode *self) {
    BedScanNode *sn = (BedScanNode *)self;
    if (sn->exhausted) return NULL;

    VecBatch *batch = bed_read_batch(sn);
    if (!batch) {
        if (sn->reader->error_fn && sn->reader->error_fn(sn->reader))
            vectra_error("failed to read BED file (corrupt or truncated "
                         "compressed stream): %s", sn->path);
        sn->exhausted = 1;
        bed_log_total(sn);
    }
    return batch;
}

static void bed_scan_free(VecNode *self) {
    BedScanNode *sn = (BedScanNode *)self;
    if (sn->reader) sn->reader->close_fn(sn->reader);
    free(sn->path);
    vec_schema_free(&sn->base.output_schema);
    free(sn);
}

/* ------------------------------------------------------------------ */
/*  Constructor                                                        */
/* ------------------------------------------------------------------ */

BedScanNode *bed_scan_node_create(const char *path, int64_t batch_size,
                                  int quiet) {
    ByteReader *rd = byte_reader_open(path);
    if (!rd) vectra_error("cannot open BED file: %s", path);

    /* Find the first data line (skipping blank / comment / track / browser),
       count its fields, then seek back so the batch reader starts on it. */
    GBuf line;
    gbuf_init(&line);
    int64_t data_start = rd->tell_fn(rd);
    FieldVec fields;
    fv_init(&fields);

    int have = 0;
    while (1) {
        data_start = rd->tell_fn(rd);
        if (!read_line(rd, &line)) break;
        if (is_skip_line(line.data, line.len)) continue;
        bed_split_fields(line.data, line.len, &fields);
        have = 1;
        break;
    }

    /* A file with no feature lines is a valid empty result: schema is the
       minimal BED3 (chrom, start, end) and the scan yields zero rows. A data
       line with fewer than three fields is malformed. */
    int n_cols = have ? (int)fields.n : 3;
    if (have && n_cols < 3) {
        gbuf_free(&line);
        fv_free(&fields);
        rd->close_fn(rd);
        vectra_error("malformed BED: line has %d columns, need at least 3 "
                     "(chrom, start, end): %s", n_cols, path);
    }

    rd->seek_fn(rd, data_start);
    gbuf_free(&line);
    fv_free(&fields);

    /* Build the fixed BED schema for this column count. */
    char   **names = (char **)malloc((size_t)n_cols * sizeof(char *));
    VecType *types = (VecType *)malloc((size_t)n_cols * sizeof(VecType));
    if (!names || !types) vectra_error("alloc failed for BED schema");
    char namebuf[16];
    char **owned = (char **)malloc((size_t)n_cols * sizeof(char *));
    if (!owned) vectra_error("alloc failed for BED schema names");
    for (int c = 0; c < n_cols; c++) {
        const char *nm = bed_col_name(c, namebuf);
        size_t len = strlen(nm);
        owned[c] = (char *)malloc(len + 1);
        if (!owned[c]) vectra_error("alloc failed for BED col name");
        memcpy(owned[c], nm, len + 1);
        names[c] = owned[c];
        types[c] = bed_col_type(c);
    }
    VecSchema schema = vec_schema_create(n_cols, names, types); /* deep-copies */
    for (int c = 0; c < n_cols; c++) free(owned[c]);
    free(owned);
    free(names);
    free(types);

    BedScanNode *sn = (BedScanNode *)calloc(1, sizeof(BedScanNode));
    if (!sn) vectra_error("alloc failed for BedScanNode");

    sn->reader = rd;
    sn->n_cols = n_cols;
    sn->batch_size = batch_size > 0 ? batch_size : 65536;
    sn->quiet = quiet;
    sn->path = (char *)malloc(strlen(path) + 1);
    if (!sn->path) vectra_error("alloc failed for path");
    memcpy(sn->path, path, strlen(path) + 1);
    sn->records_emitted = 0;
    sn->exhausted = 0;
    sn->logged = 0;

    sn->base.output_schema = schema;
    sn->base.next_batch = bed_scan_next_batch;
    sn->base.free_node = bed_scan_free;
    sn->base.kind = "BedScanNode";
    sn->base.row_count_hint = -1;

    return sn;
}
