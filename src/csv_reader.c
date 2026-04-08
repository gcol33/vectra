#include "csv_reader.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include "miniz/miniz.h"

/* ------------------------------------------------------------------ */
/*  Plain FILE* reader                                                 */
/* ------------------------------------------------------------------ */

typedef struct {
    CsvReader base;
    FILE     *fp;
} FileReader;

static int file_getc(CsvReader *r) {
    return fgetc(((FileReader *)r)->fp);
}

static int file_ungetc(CsvReader *r, int c) {
    return ungetc(c, ((FileReader *)r)->fp);
}

static int64_t file_tell(CsvReader *r) {
    return (int64_t)ftell(((FileReader *)r)->fp);
}

static int file_seek(CsvReader *r, int64_t offset) {
    return fseek(((FileReader *)r)->fp, (long)offset, SEEK_SET);
}

static void file_close(CsvReader *r) {
    FileReader *fr = (FileReader *)r;
    if (fr->fp) fclose(fr->fp);
    free(fr);
}

static CsvReader *file_reader_open(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return NULL;

    FileReader *fr = (FileReader *)calloc(1, sizeof(FileReader));
    if (!fr) { fclose(fp); return NULL; }

    fr->fp = fp;
    fr->base.getc_fn   = file_getc;
    fr->base.ungetc_fn = file_ungetc;
    fr->base.tell_fn   = file_tell;
    fr->base.seek_fn   = file_seek;
    fr->base.close_fn  = file_close;
    return &fr->base;
}

/* ------------------------------------------------------------------ */
/*  Memory-cursor reader (used by the gz path)                         */
/* ------------------------------------------------------------------ */
/*
 * The gzip path used to wrap zlib's gzFile so getc/ungetc/tell/seek
 * could stream straight off the compressed file. miniz has no gzFile
 * equivalent, but the only seek pattern in csv_scan.c is "tell once
 * after the header, read forward for type inference, seek back to that
 * mark", so we don't need streaming inflate at all. We decompress the
 * whole .gz into memory at open time and expose the result as a cursor.
 *
 * Memory cost: decompressed source bytes during the scan. The scan
 * itself materialises the data into typed VecArray columns, which is
 * already several times larger than the raw source, so this is not the
 * bottleneck.
 */

typedef struct {
    CsvReader base;
    uint8_t  *buf;   /* malloc'd via miniz, freed in mem_close via mz_free */
    size_t    len;
    size_t    pos;
} MemReader;

static int mem_getc(CsvReader *r) {
    MemReader *m = (MemReader *)r;
    if (m->pos >= m->len) return EOF;
    return (int)m->buf[m->pos++];
}

static int mem_ungetc(CsvReader *r, int c) {
    /* csv_scan only ever pushes back the byte it just read, so we can
       implement this as a pure cursor decrement. The 'c' argument is
       ignored on purpose: the original byte still lives at buf[pos-1]. */
    MemReader *m = (MemReader *)r;
    if (m->pos == 0) return EOF;
    m->pos--;
    return c;
}

static int64_t mem_tell(CsvReader *r) {
    return (int64_t)((MemReader *)r)->pos;
}

static int mem_seek(CsvReader *r, int64_t offset) {
    MemReader *m = (MemReader *)r;
    if (offset < 0 || (size_t)offset > m->len) return -1;
    m->pos = (size_t)offset;
    return 0;
}

static void mem_close(CsvReader *r) {
    MemReader *m = (MemReader *)r;
    if (m->buf) mz_free(m->buf);
    free(m);
}

/* Slurp a whole file into a malloc'd buffer. Caller frees with free(). */
static int read_whole_file(const char *path, uint8_t **out_data, size_t *out_len) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return -1;
    if (fseek(fp, 0, SEEK_END) != 0) { fclose(fp); return -1; }
    long sz = ftell(fp);
    if (sz < 0) { fclose(fp); return -1; }
    if (fseek(fp, 0, SEEK_SET) != 0) { fclose(fp); return -1; }

    uint8_t *buf = (uint8_t *)malloc((size_t)sz);
    if (!buf) { fclose(fp); return -1; }

    size_t got = fread(buf, 1, (size_t)sz, fp);
    fclose(fp);
    if (got != (size_t)sz) { free(buf); return -1; }

    *out_data = buf;
    *out_len = (size_t)sz;
    return 0;
}

/* Parse the gzip header (RFC 1952). On success, *body_out points into
   the input buffer at the start of the raw deflate stream and
   *body_len_out is the length of that stream (excluding the 8-byte
   trailer of CRC32 + ISIZE). */
static int gzip_parse_header(const uint8_t *data, size_t len,
                             const uint8_t **body_out, size_t *body_len_out) {
    if (len < 18) return -1;                /* 10 hdr + min body + 8 trailer */
    if (data[0] != 0x1F || data[1] != 0x8B) return -1;
    if (data[2] != 8) return -1;            /* CM == DEFLATE */

    uint8_t flg = data[3];
    size_t pos = 10;                        /* fixed header size */

    if (flg & 0x04) {                       /* FEXTRA */
        if (pos + 2 > len) return -1;
        size_t xlen = (size_t)data[pos] | ((size_t)data[pos + 1] << 8);
        pos += 2 + xlen;
        if (pos > len) return -1;
    }
    if (flg & 0x08) {                       /* FNAME (zero-terminated) */
        while (pos < len && data[pos] != 0) pos++;
        if (pos >= len) return -1;
        pos++;
    }
    if (flg & 0x10) {                       /* FCOMMENT (zero-terminated) */
        while (pos < len && data[pos] != 0) pos++;
        if (pos >= len) return -1;
        pos++;
    }
    if (flg & 0x02) {                       /* FHCRC (2 bytes) */
        if (pos + 2 > len) return -1;
        pos += 2;
    }

    if (pos + 8 > len) return -1;           /* trailer must fit */
    *body_out = data + pos;
    *body_len_out = len - pos - 8;
    return 0;
}

static CsvReader *gz_reader_open(const char *path) {
    uint8_t *file_buf = NULL;
    size_t   file_len = 0;
    if (read_whole_file(path, &file_buf, &file_len) != 0) return NULL;

    const uint8_t *body = NULL;
    size_t body_len = 0;
    if (gzip_parse_header(file_buf, file_len, &body, &body_len) != 0) {
        free(file_buf);
        return NULL;
    }

    /* tinfl_decompress_mem_to_heap with flags=0 expects a raw deflate
       stream (no zlib header, no checksum), which is exactly what sits
       inside a gzip file body. */
    size_t out_len = 0;
    void *decomp = tinfl_decompress_mem_to_heap(body, body_len, &out_len, 0);
    free(file_buf);
    if (!decomp) return NULL;

    MemReader *mr = (MemReader *)calloc(1, sizeof(MemReader));
    if (!mr) { mz_free(decomp); return NULL; }

    mr->buf = (uint8_t *)decomp;
    mr->len = out_len;
    mr->pos = 0;
    mr->base.getc_fn   = mem_getc;
    mr->base.ungetc_fn = mem_ungetc;
    mr->base.tell_fn   = mem_tell;
    mr->base.seek_fn   = mem_seek;
    mr->base.close_fn  = mem_close;
    return &mr->base;
}

/* ------------------------------------------------------------------ */
/*  Public constructor: detect .gz extension                           */
/* ------------------------------------------------------------------ */

CsvReader *csv_reader_open(const char *path) {
    size_t len = strlen(path);
    if (len >= 3 && strcmp(path + len - 3, ".gz") == 0)
        return gz_reader_open(path);
    return file_reader_open(path);
}
