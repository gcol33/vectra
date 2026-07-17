#include "byte_reader.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include "miniz/miniz.h"

/* 64-bit file offsets: a plain CSV or the compressed .gz can both exceed 2 GB,
   where 32-bit ftell/fseek wrap. The scanners only ever seek to small marks, but
   the size query and any large file must use the wide variants. */
#if defined(_WIN32)
  #define VZ_FSEEK64(fp, off) _fseeki64((fp), (int64_t)(off), SEEK_SET)
  #define VZ_FTELL64(fp)      _ftelli64(fp)
#else
  #define VZ_FSEEK64(fp, off) fseeko((fp), (off_t)(off), SEEK_SET)
  #define VZ_FTELL64(fp)      ftello(fp)
#endif

/* ------------------------------------------------------------------ */
/*  Plain FILE* reader                                                 */
/* ------------------------------------------------------------------ */

typedef struct {
    ByteReader base;
    FILE      *fp;
} FileReader;

static int file_getc(ByteReader *r) {
    return fgetc(((FileReader *)r)->fp);
}

static int file_ungetc(ByteReader *r, int c) {
    return ungetc(c, ((FileReader *)r)->fp);
}

static int64_t file_tell(ByteReader *r) {
    return (int64_t)VZ_FTELL64(((FileReader *)r)->fp);
}

static int file_seek(ByteReader *r, int64_t offset) {
    return VZ_FSEEK64(((FileReader *)r)->fp, offset);
}

static int file_error(ByteReader *r) {
    return ferror(((FileReader *)r)->fp);
}

static void file_close(ByteReader *r) {
    FileReader *fr = (FileReader *)r;
    if (fr->fp) fclose(fr->fp);
    free(fr);
}

static ByteReader *file_reader_open(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return NULL;

    FileReader *fr = (FileReader *)calloc(1, sizeof(FileReader));
    if (!fr) { fclose(fp); return NULL; }

    fr->fp = fp;
    fr->base.getc_fn   = file_getc;
    fr->base.ungetc_fn = file_ungetc;
    fr->base.tell_fn   = file_tell;
    fr->base.seek_fn   = file_seek;
    fr->base.error_fn  = file_error;
    fr->base.close_fn  = file_close;
    return &fr->base;
}

/* ------------------------------------------------------------------ */
/*  Streaming gzip reader                                              */
/* ------------------------------------------------------------------ */
/*
 * The gz path used to slurp the whole file into RAM and inflate it whole into
 * a second buffer, which caps the readable size at available memory (and the
 * size query used a 32-bit ftell, so a >2 GB .gz failed to open at all on
 * Windows). Instead we stream: raw deflate is fed through miniz's tinfl coroutine
 * into a 32 KB wrapping window (which doubles as the LZ dictionary), and getc
 * serves bytes out of that window, pumping more only when it drains. Peak memory
 * is the window plus one compressed-input block, independent of file size.
 *
 * A .gz may hold several concatenated gzip members (bgzip, `cat a.gz b.gz`).
 * When a member's deflate stream ends we consume its 8-byte trailer, and if
 * another member follows we parse its header and re-init the inflator, so the
 * whole file reads as one continuous byte stream rather than stopping at the
 * first member.
 *
 * The scanners seek backward only to a small mark near the start (header +
 * type-inference rewind). We serve a backward seek by re-inflating from the
 * first member's body start and discarding forward to the target -- cheap
 * because the target is tiny -- and a forward seek by discarding forward.
 *
 * getc returns EOF at both a clean end and a hard decode error; a truncated or
 * corrupt stream sets `error`, which gz_error reports so the scan can fail
 * loudly instead of silently returning a short read.
 */

#define GZ_IN_CHUNK 65536      /* compressed input block */

typedef struct {
    ByteReader base;
    FILE      *fp;
    int64_t    body_start;          /* compressed offset of the first member's body */
    tinfl_decompressor inflator;
    uint8_t    in_buf[GZ_IN_CHUNK];
    size_t     in_pos, in_len;
    int        in_eof;
    uint8_t    dict[TINFL_LZ_DICT_SIZE];   /* 32 KB output ring == LZ dictionary */
    size_t     write_ofs;           /* next write position in the ring */
    size_t     read_ofs;            /* next unread byte in the ring */
    size_t     avail;               /* produced-but-unconsumed bytes (<= ring) */
    uint64_t   abs_pos;             /* absolute decompressed position (consumer) */
    int        done;                /* inflate finished or errored */
    int        error;               /* hard error */
} GzReader;

/* Pull up to n raw compressed bytes from the buffered input into dst.
   Returns bytes actually read (< n only at end of file). */
static size_t gz_raw_read(GzReader *g, uint8_t *dst, size_t n) {
    size_t got = 0;
    while (got < n) {
        if (g->in_pos >= g->in_len) {
            g->in_len = fread(g->in_buf, 1, sizeof(g->in_buf), g->fp);
            g->in_pos = 0;
            if (g->in_len == 0) { g->in_eof = 1; break; }
        }
        size_t take = g->in_len - g->in_pos;
        if (take > n - got) take = n - got;
        memcpy(dst + got, g->in_buf + g->in_pos, take);
        g->in_pos += take;
        got += take;
    }
    return got;
}

static int gz_read_u8(GzReader *g, uint8_t *out) {
    return gz_raw_read(g, out, 1) == 1;
}

/* Consume, from the buffered compressed input, (optionally a preceding member
   trailer, then) a gzip header (RFC 1952), and re-init the inflator for that
   member's deflate body. The header is parsed field by field so no fixed size
   cap applies. Leaves the input cursor at the first body byte.
   Returns 1 if a member started, 0 at a clean end (nothing more to read), -1 on
   a malformed or truncated header. *hdr_len (optional) receives the header byte
   count, used only for the first member's body_start. */
static int gz_begin_member(GzReader *g, int expect_trailer, int64_t *hdr_len) {
    if (expect_trailer) {
        uint8_t trailer[8];
        if (gz_raw_read(g, trailer, sizeof trailer) != sizeof trailer)
            return 0;                        /* trailer then EOF: clean end */
    }

    uint8_t fixed[10];
    size_t got = gz_raw_read(g, fixed, sizeof fixed);
    if (got == 0) return 0;                  /* nothing after the trailer: done */
    int magic_ok = (got >= 2 && fixed[0] == 0x1F && fixed[1] == 0x8B);
    if (!magic_ok)
        return expect_trailer ? 0 : -1;      /* trailing garbage vs bad first header */
    if (got < 10 || fixed[2] != 8) return -1;/* gzip magic but truncated/not DEFLATE */

    int64_t n = 10;
    uint8_t flg = fixed[3];
    uint8_t b, b2;
    if (flg & 0x04) {                        /* FEXTRA: 2-byte length + payload */
        if (!gz_read_u8(g, &b) || !gz_read_u8(g, &b2)) return -1;
        size_t xlen = (size_t)b | ((size_t)b2 << 8);
        n += 2;
        for (size_t i = 0; i < xlen; i++) { if (!gz_read_u8(g, &b)) return -1; n++; }
    }
    if (flg & 0x08) {                        /* FNAME: NUL-terminated */
        do { if (!gz_read_u8(g, &b)) return -1; n++; } while (b != 0);
    }
    if (flg & 0x10) {                        /* FCOMMENT: NUL-terminated */
        do { if (!gz_read_u8(g, &b)) return -1; n++; } while (b != 0);
    }
    if (flg & 0x02) {                        /* FHCRC: 2 bytes */
        if (!gz_read_u8(g, &b) || !gz_read_u8(g, &b2)) return -1;
        n += 2;
    }

    if (hdr_len) *hdr_len = n;
    tinfl_init(&g->inflator);
    return 1;
}

/* Inflate more into the ring; return bytes produced this call (0 at end). */
static size_t gz_pump(GzReader *g) {
    while (!g->done) {
        if (g->in_pos >= g->in_len && !g->in_eof) {
            g->in_len = fread(g->in_buf, 1, sizeof(g->in_buf), g->fp);
            g->in_pos = 0;
            if (g->in_len == 0) g->in_eof = 1;
        }
        size_t in_bytes  = g->in_len - g->in_pos;
        size_t out_bytes = TINFL_LZ_DICT_SIZE - g->write_ofs;   /* to ring end */
        mz_uint32 flags  = g->in_eof ? 0 : TINFL_FLAG_HAS_MORE_INPUT;
        tinfl_status st = tinfl_decompress(
            &g->inflator,
            (const mz_uint8 *)(g->in_buf + g->in_pos), &in_bytes,
            (mz_uint8 *)g->dict, (mz_uint8 *)(g->dict + g->write_ofs), &out_bytes,
            flags);
        g->in_pos   += in_bytes;
        g->write_ofs = (g->write_ofs + out_bytes) & (TINFL_LZ_DICT_SIZE - 1);
        g->avail    += out_bytes;

        if (st < TINFL_STATUS_DONE) { g->done = 1; g->error = 1; return out_bytes; }
        if (st == TINFL_STATUS_DONE) {
            /* End of this member's deflate stream: follow a concatenated next
               member if one is present, otherwise the file is done. */
            int more = gz_begin_member(g, 1, NULL);
            if (more < 0) { g->done = 1; g->error = 1; return out_bytes; }
            if (more == 0) { g->done = 1; return out_bytes; }
            if (out_bytes > 0) return out_bytes;   /* new member inflates next call */
            continue;                              /* keep inflating the new member */
        }
        if (out_bytes > 0) return out_bytes;
        /* no output: needs more input (loop to refill) or truncated at EOF */
        if (st == TINFL_STATUS_NEEDS_MORE_INPUT && g->in_eof) {
            g->done = 1; g->error = 1; return 0;
        }
    }
    return 0;
}

static int gz_getc(ByteReader *r) {
    GzReader *g = (GzReader *)r;
    if (g->avail == 0) {
        gz_pump(g);
        if (g->avail == 0) return EOF;
    }
    int c = g->dict[g->read_ofs];
    g->read_ofs = (g->read_ofs + 1) & (TINFL_LZ_DICT_SIZE - 1);
    g->avail--;
    g->abs_pos++;
    return c;
}

static int gz_ungetc(ByteReader *r, int c) {
    /* The scanners only ever push back the byte they just read, so this is a
       pure cursor decrement: the original byte still lives at read_ofs-1 in the
       ring, so the `c` argument is ignored on purpose. */
    GzReader *g = (GzReader *)r;
    if (g->abs_pos == 0) return EOF;
    g->read_ofs = (g->read_ofs - 1) & (TINFL_LZ_DICT_SIZE - 1);
    g->avail++;
    g->abs_pos--;
    return c;
}

static int64_t gz_tell(ByteReader *r) {
    return (int64_t)((GzReader *)r)->abs_pos;
}

static int gz_error(ByteReader *r) {
    return ((GzReader *)r)->error;
}

static int gz_reset(GzReader *g) {
    if (VZ_FSEEK64(g->fp, g->body_start) != 0) return -1;
    tinfl_init(&g->inflator);
    g->in_pos = g->in_len = 0;
    g->in_eof = 0;
    g->write_ofs = g->read_ofs = 0;
    g->avail = 0;
    g->abs_pos = 0;
    g->done = 0;
    g->error = 0;
    return 0;
}

static int gz_seek(ByteReader *r, int64_t offset) {
    GzReader *g = (GzReader *)r;
    if (offset < 0) return -1;
    uint64_t target = (uint64_t)offset;
    if (target < g->abs_pos && gz_reset(g) != 0) return -1;
    while (g->abs_pos < target) {
        if (gz_getc(r) == EOF) return -1;
    }
    return 0;
}

static void gz_close(ByteReader *r) {
    GzReader *g = (GzReader *)r;
    if (g->fp) fclose(g->fp);
    free(g);
}

static ByteReader *gz_reader_open(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return NULL;

    GzReader *g = (GzReader *)calloc(1, sizeof(GzReader));
    if (!g) { fclose(fp); return NULL; }
    g->fp = fp;

    /* Parse the first member's header straight off the buffered input (cursor
       starts at file offset 0), so its length is the body's file offset. */
    int64_t hdr_len = 0;
    if (gz_begin_member(g, 0, &hdr_len) != 1) { fclose(fp); free(g); return NULL; }
    g->body_start = hdr_len;

    g->base.getc_fn   = gz_getc;
    g->base.ungetc_fn = gz_ungetc;
    g->base.tell_fn   = gz_tell;
    g->base.seek_fn   = gz_seek;
    g->base.error_fn  = gz_error;
    g->base.close_fn  = gz_close;
    return &g->base;
}

/* ------------------------------------------------------------------ */
/*  Public constructor: detect .gz extension                           */
/* ------------------------------------------------------------------ */

ByteReader *byte_reader_open(const char *path) {
    size_t len = strlen(path);
    if (len >= 3 && strcmp(path + len - 3, ".gz") == 0)
        return gz_reader_open(path);
    return file_reader_open(path);
}
