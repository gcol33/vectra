#include "fasta_scan.h"
#include "byte_reader.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "error.h"
#include "grow.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>

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

static void gbuf_append(GBuf *g, const char *s, int64_t n) {
    if (n <= 0) return;
    vec_grow_to((void **)&g->data, &g->cap, g->len + n, sizeof(char), "GBuf");
    memcpy(g->data + g->len, s, (size_t)n);
    g->len += n;
}

/* Null-terminate and return pointer (valid until next push/append/clear). */
static const char *gbuf_str(GBuf *g) {
    gbuf_push(g, '\0');
    g->len--; /* don't count terminator in logical length */
    return g->data;
}

/* ------------------------------------------------------------------ */
/*  Line reading                                                       */
/* ------------------------------------------------------------------ */

/* Read one line into g (trailing CR/LF stripped). Returns 1 if a line was
   read (possibly empty), 0 at EOF before any byte. */
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

/* ------------------------------------------------------------------ */
/*  Record parsing                                                     */
/* ------------------------------------------------------------------ */

/* Read the next FASTA record. hdr receives the header line without the
   leading '>'; seq receives the concatenated sequence (blank lines
   dropped). Returns 1 on a record, 0 at clean EOF. Errors on malformed
   input. `scratch` is a caller-owned reused line buffer. */
static int fasta_next(FastaScanNode *sn, GBuf *scratch, GBuf *hdr, GBuf *seq) {
    ByteReader *rd = sn->reader;

    gbuf_clear(hdr);
    if (sn->pending_header) {
        gbuf_append(hdr, sn->pending_header,
                    (int64_t)strlen(sn->pending_header));
        free(sn->pending_header);
        sn->pending_header = NULL;
    } else {
        /* Read the next header line, skipping leading blank lines. */
        int found = 0;
        while (read_line(rd, scratch)) {
            if (scratch->len == 0) continue; /* blank line */
            if (scratch->data[0] != '>')
                vectra_error("malformed FASTA: expected '>' at record start, "
                             "got '%c' in %s", scratch->data[0], sn->path);
            gbuf_append(hdr, scratch->data + 1, scratch->len - 1);
            found = 1;
            break;
        }
        if (!found) return 0; /* clean EOF */
    }

    /* Read sequence lines until the next '>' header or EOF. */
    gbuf_clear(seq);
    while (read_line(rd, scratch)) {
        if (scratch->len == 0) continue; /* blank line between records */
        if (scratch->data[0] == '>') {
            /* Belongs to the next record; stash it. */
            sn->pending_header = (char *)malloc((size_t)scratch->len);
            if (!sn->pending_header) vectra_error("alloc failed for header");
            memcpy(sn->pending_header, scratch->data + 1,
                   (size_t)(scratch->len - 1));
            sn->pending_header[scratch->len - 1] = '\0';
            break;
        }
        gbuf_append(seq, scratch->data, scratch->len);
    }
    return 1;
}

/* Read the next FASTQ record (strict 4-line form). hdr/seq/qual receive the
   header (no '@'), sequence, and quality. Returns 1 on a record, 0 at clean
   EOF. A record cut short mid-way, or a seq/qual length mismatch, is a loud
   error rather than a silent drop. `l1`/`l3` are reused scratch buffers. */
static int fastq_next(FastaScanNode *sn, GBuf *l1, GBuf *l3,
                      GBuf *hdr, GBuf *seq, GBuf *qual) {
    ByteReader *rd = sn->reader;

    /* Line 1: header. Skip leading blank lines before the first record. */
    int have = 0;
    while (read_line(rd, l1)) {
        if (l1->len == 0) continue;
        have = 1;
        break;
    }
    if (!have) return 0; /* clean EOF */
    if (l1->data[0] != '@')
        vectra_error("malformed FASTQ: expected '@' at record start, "
                     "got '%c' in %s", l1->data[0], sn->path);
    gbuf_clear(hdr);
    gbuf_append(hdr, l1->data + 1, l1->len - 1);

    /* Line 2: sequence. */
    if (!read_line(rd, seq))
        vectra_error("truncated FASTQ record: missing sequence line in %s",
                     sn->path);

    /* Line 3: separator, must start with '+'. */
    if (!read_line(rd, l3))
        vectra_error("truncated FASTQ record: missing '+' separator in %s",
                     sn->path);
    if (l3->len == 0 || l3->data[0] != '+')
        vectra_error("malformed FASTQ: expected '+' separator in %s", sn->path);

    /* Line 4: quality; must match the sequence length. */
    if (!read_line(rd, qual))
        vectra_error("truncated FASTQ record: missing quality line in %s",
                     sn->path);
    if (qual->len != seq->len)
        vectra_error("FASTQ record: sequence/quality length mismatch "
                     "(%" PRId64 " vs %" PRId64 ") in %s",
                     seq->len, qual->len, sn->path);
    return 1;
}

/* ------------------------------------------------------------------ */
/*  Header id/desc split                                               */
/* ------------------------------------------------------------------ */

/* Split a header string at the first run of whitespace: id is everything up
   to it, desc is the remainder (left-trimmed). id_len / desc_ptr / desc_len
   are set as views into the caller's buffer. desc is "" when no whitespace. */
static void split_header(const char *hdr, int64_t hlen,
                         int64_t *id_len,
                         const char **desc_ptr, int64_t *desc_len) {
    int64_t i = 0;
    while (i < hlen && hdr[i] != ' ' && hdr[i] != '\t') i++;
    *id_len = i;
    while (i < hlen && (hdr[i] == ' ' || hdr[i] == '\t')) i++;
    *desc_ptr = hdr + i;
    *desc_len = hlen - i;
}

/* ------------------------------------------------------------------ */
/*  Column building                                                    */
/* ------------------------------------------------------------------ */

/* Build a VEC_STRING column from n (ptr,len) slices. Every row is valid;
   an empty slice is an empty string, not NA. Two-pass: total length, then
   one allocation filled by offset. */
static VecArray build_string_col(char **vals, int64_t *lens, int64_t n) {
    VecArray arr = vec_array_alloc(VEC_STRING, n);

    int64_t total = 0;
    for (int64_t r = 0; r < n; r++) total += lens[r];

    free(arr.buf.str.data); /* free the 1-byte placeholder */
    arr.buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
    if (!arr.buf.str.data) vectra_error("alloc failed for string data");
    arr.buf.str.data_len = total;

    int64_t off = 0;
    for (int64_t r = 0; r < n; r++) {
        arr.buf.str.offsets[r] = off;
        vec_array_set_valid(&arr, r);
        if (lens[r] > 0) {
            memcpy(arr.buf.str.data + off, vals[r], (size_t)lens[r]);
            off += lens[r];
        }
    }
    arr.buf.str.offsets[n] = off;
    return arr;
}

/* ------------------------------------------------------------------ */
/*  Batch reading                                                      */
/* ------------------------------------------------------------------ */

static VecBatch *fasta_read_batch(FastaScanNode *sn, int *eof_out) {
    int64_t cap = sn->batch_size < 1024 ? sn->batch_size : 1024;
    if (cap < 1) cap = 1;

    /* Per-record raw strings; id/desc are split from the header at fill. */
    char  **hdrs = (char **)malloc((size_t)cap * sizeof(char *));
    int64_t *hlens = (int64_t *)malloc((size_t)cap * sizeof(int64_t));
    char  **seqs = (char **)malloc((size_t)cap * sizeof(char *));
    int64_t *slens = (int64_t *)malloc((size_t)cap * sizeof(int64_t));
    char  **quals = sn->is_fastq
                    ? (char **)malloc((size_t)cap * sizeof(char *)) : NULL;
    int64_t *qlens = sn->is_fastq
                     ? (int64_t *)malloc((size_t)cap * sizeof(int64_t)) : NULL;
    if (!hdrs || !hlens || !seqs || !slens ||
        (sn->is_fastq && (!quals || !qlens)))
        vectra_error("alloc failed for FASTA/FASTQ batch");

    GBuf s1, s2, hdr, seq, qual;
    gbuf_init(&s1); gbuf_init(&s2);
    gbuf_init(&hdr); gbuf_init(&seq); gbuf_init(&qual);

    int64_t n = 0;
    int eof = 0;
    while (n < sn->batch_size) {
        int got = sn->is_fastq
                  ? fastq_next(sn, &s1, &s2, &hdr, &seq, &qual)
                  : fasta_next(sn, &s1, &hdr, &seq);
        if (!got) { eof = 1; break; }

        if (n >= cap) {
            cap *= 2;
            hdrs  = (char **)realloc(hdrs,  (size_t)cap * sizeof(char *));
            hlens = (int64_t *)realloc(hlens, (size_t)cap * sizeof(int64_t));
            seqs  = (char **)realloc(seqs,  (size_t)cap * sizeof(char *));
            slens = (int64_t *)realloc(slens, (size_t)cap * sizeof(int64_t));
            if (sn->is_fastq) {
                quals = (char **)realloc(quals, (size_t)cap * sizeof(char *));
                qlens = (int64_t *)realloc(qlens, (size_t)cap * sizeof(int64_t));
            }
            if (!hdrs || !hlens || !seqs || !slens ||
                (sn->is_fastq && (!quals || !qlens)))
                vectra_error("realloc failed for FASTA/FASTQ batch");
        }

        hlens[n] = hdr.len;
        hdrs[n]  = (char *)malloc((size_t)(hdr.len > 0 ? hdr.len : 1));
        memcpy(hdrs[n], hdr.data, (size_t)hdr.len);

        slens[n] = seq.len;
        seqs[n]  = (char *)malloc((size_t)(seq.len > 0 ? seq.len : 1));
        memcpy(seqs[n], seq.data, (size_t)seq.len);

        if (sn->is_fastq) {
            qlens[n] = qual.len;
            quals[n] = (char *)malloc((size_t)(qual.len > 0 ? qual.len : 1));
            memcpy(quals[n], qual.data, (size_t)qual.len);
        }
        n++;
    }

    gbuf_free(&s1); gbuf_free(&s2);
    gbuf_free(&hdr); gbuf_free(&seq); gbuf_free(&qual);

    *eof_out = eof;

    if (n == 0) {
        free(hdrs); free(hlens); free(seqs); free(slens);
        free(quals); free(qlens);
        return NULL;
    }

    /* Split headers into id / desc slices (views into hdrs[r]). */
    char  **ids = (char **)malloc((size_t)n * sizeof(char *));
    int64_t *idlens = (int64_t *)malloc((size_t)n * sizeof(int64_t));
    char  **descs = (char **)malloc((size_t)n * sizeof(char *));
    int64_t *desclens = (int64_t *)malloc((size_t)n * sizeof(int64_t));
    if (!ids || !idlens || !descs || !desclens)
        vectra_error("alloc failed for id/desc split");
    for (int64_t r = 0; r < n; r++) {
        int64_t idl, dl;
        const char *dptr;
        split_header(hdrs[r], hlens[r], &idl, &dptr, &dl);
        ids[r] = hdrs[r];
        idlens[r] = idl;
        descs[r] = (char *)dptr;
        desclens[r] = dl;
    }

    int n_cols = sn->is_fastq ? 4 : 3;
    VecBatch *batch = vec_batch_alloc(n_cols, n);
    for (int c = 0; c < n_cols; c++) {
        const char *nm = sn->base.output_schema.col_names[c];
        size_t nm_len = strlen(nm);
        batch->col_names[c] = (char *)malloc(nm_len + 1);
        memcpy(batch->col_names[c], nm, nm_len + 1);
    }

    batch->columns[0] = build_string_col(ids, idlens, n);      /* id   */
    batch->columns[1] = build_string_col(descs, desclens, n);  /* desc */
    batch->columns[2] = build_string_col(seqs, slens, n);      /* seq  */
    if (sn->is_fastq)
        batch->columns[3] = build_string_col(quals, qlens, n); /* qual */

    for (int64_t r = 0; r < n; r++) {
        free(hdrs[r]);
        free(seqs[r]);
        if (sn->is_fastq) free(quals[r]);
    }
    free(hdrs); free(hlens); free(seqs); free(slens);
    free(quals); free(qlens);
    free(ids); free(idlens); free(descs); free(desclens);

    sn->records_emitted += n;
    return batch;
}

/* ------------------------------------------------------------------ */
/*  VecNode vtable                                                     */
/* ------------------------------------------------------------------ */

static void fasta_log_total(FastaScanNode *sn) {
    if (sn->quiet || sn->logged) return;
    sn->logged = 1;
    REprintf("vectra: scanned %" PRId64 " records from %s '%s'\n",
             sn->records_emitted, sn->is_fastq ? "FASTQ" : "FASTA", sn->path);
}

static VecBatch *fasta_scan_next_batch(VecNode *self) {
    FastaScanNode *sn = (FastaScanNode *)self;
    if (sn->exhausted) return NULL;

    int eof = 0;
    VecBatch *batch = fasta_read_batch(sn, &eof);
    if (eof) {
        sn->exhausted = 1;
        fasta_log_total(sn);
    }
    return batch; /* NULL when the final read produced no records */
}

static void fasta_scan_free(VecNode *self) {
    FastaScanNode *sn = (FastaScanNode *)self;
    if (sn->reader) sn->reader->close_fn(sn->reader);
    free(sn->pending_header);
    free(sn->path);
    vec_schema_free(&sn->base.output_schema);
    free(sn);
}

/* ------------------------------------------------------------------ */
/*  Constructor                                                        */
/* ------------------------------------------------------------------ */

FastaScanNode *fasta_scan_node_create(const char *path, int64_t batch_size,
                                      int is_fastq, int quiet) {
    ByteReader *rd = byte_reader_open(path);
    if (!rd)
        vectra_error("cannot open %s file: %s",
                     is_fastq ? "FASTQ" : "FASTA", path);

    int n_cols = is_fastq ? 4 : 3;
    const char *fa_names[3] = { "id", "desc", "seq" };
    const char *fq_names[4] = { "id", "desc", "seq", "qual" };
    const char **src = is_fastq ? fq_names : fa_names;

    char   **names = (char **)malloc((size_t)n_cols * sizeof(char *));
    VecType *types = (VecType *)malloc((size_t)n_cols * sizeof(VecType));
    if (!names || !types) vectra_error("alloc failed for FASTA schema");
    for (int i = 0; i < n_cols; i++) {
        names[i] = (char *)src[i]; /* schema deep-copies */
        types[i] = VEC_STRING;
    }
    VecSchema schema = vec_schema_create(n_cols, names, types);
    free(names);
    free(types);

    FastaScanNode *sn = (FastaScanNode *)calloc(1, sizeof(FastaScanNode));
    if (!sn) vectra_error("alloc failed for FastaScanNode");

    sn->reader = rd;
    sn->is_fastq = is_fastq;
    sn->batch_size = batch_size > 0 ? batch_size : 65536;
    sn->quiet = quiet;
    sn->path = (char *)malloc(strlen(path) + 1);
    if (!sn->path) vectra_error("alloc failed for path");
    memcpy(sn->path, path, strlen(path) + 1);
    sn->pending_header = NULL;
    sn->records_emitted = 0;
    sn->exhausted = 0;
    sn->logged = 0;

    sn->base.output_schema = schema;
    sn->base.next_batch = fasta_scan_next_batch;
    sn->base.free_node = fasta_scan_free;
    sn->base.kind = is_fastq ? "FastqScanNode" : "FastaScanNode";
    sn->base.row_count_hint = -1;

    return sn;
}
