#include "csv_reader.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <zlib.h>

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
/*  gzip reader (zlib gzFile)                                          */
/* ------------------------------------------------------------------ */

typedef struct {
    CsvReader base;
    gzFile    gz;
} GzReader;

static int gz_getc(CsvReader *r) {
    return gzgetc(((GzReader *)r)->gz);
}

static int gz_ungetc(CsvReader *r, int c) {
    return gzungetc(c, ((GzReader *)r)->gz);
}

static int64_t gz_tell(CsvReader *r) {
    return (int64_t)gztell(((GzReader *)r)->gz);
}

static int gz_seek(CsvReader *r, int64_t offset) {
    z_off_t res = gzseek(((GzReader *)r)->gz, (z_off_t)offset, SEEK_SET);
    return (res == -1) ? -1 : 0;
}

static void gz_close(CsvReader *r) {
    GzReader *gr = (GzReader *)r;
    if (gr->gz) gzclose(gr->gz);
    free(gr);
}

static CsvReader *gz_reader_open(const char *path) {
    gzFile gz = gzopen(path, "rb");
    if (!gz) return NULL;

    GzReader *gr = (GzReader *)calloc(1, sizeof(GzReader));
    if (!gr) { gzclose(gz); return NULL; }

    gr->gz = gz;
    gr->base.getc_fn   = gz_getc;
    gr->base.ungetc_fn = gz_ungetc;
    gr->base.tell_fn   = gz_tell;
    gr->base.seek_fn   = gz_seek;
    gr->base.close_fn  = gz_close;
    return &gr->base;
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
