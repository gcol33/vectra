#include "parquet_scan.h"
#include "parquet_meta.h"
#include "parquet_column.h"
#include "parquet_codec.h"
#include "scan.h"
#include "expr.h"
#include "schema.h"
#include "array.h"
#include "batch.h"
#include "error.h"
#include "vec_omp.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <math.h>

/* 64-bit file offsets: Parquet files routinely exceed 2 GB. */
#if defined(_WIN32)
  #define PQ_FSEEK64(fp, off, wh) _fseeki64((fp), (int64_t)(off), (wh))
  #define PQ_FTELL64(fp)          _ftelli64(fp)
#else
  #define PQ_FSEEK64(fp, off, wh) fseeko((fp), (off_t)(off), (wh))
  #define PQ_FTELL64(fp)          ftello(fp)
#endif

#define PQ_MAX_SCHEMA_DEPTH 64

typedef struct {
    char       *path;
    int64_t     file_size;
    PqFileMeta  meta;
    PqLeaf     *leaves;
    int         n_leaves;
    int        *map;          /* dataset column -> leaf index in this file */
} PqFile;

typedef struct {
    VecNode    base;
    PqFile    *files;
    int        n_files;
    VecSchema  full;          /* every column the dataset exposes */
    int       *out_cols;      /* output column -> dataset column */
    int        n_out;
    VecExpr   *predicate;     /* owned pruning copy of the filter(s) above */
    int64_t    batch_size;
    char      *list_sep;
    int        file_i, rg_i;  /* next row group to consider */
    int        rg_open;
    int64_t    rg_left;
    PqCursor  *cursors;       /* one per output column while a group is open */
    int        started;
} ParquetScanNode;

static char *pq_strdup(const char *s) {
    if (!s) return NULL;
    size_t n = strlen(s) + 1;
    char *r = (char *)malloc(n);
    if (r) memcpy(r, s, n);
    return r;
}

/* ------------------------------------------------------------------ */
/*  Schema -> leaves                                                   */
/* ------------------------------------------------------------------ */

static int elem_is_list(const PqSchemaElem *e) {
    return e->logical == PQ_LT_LIST || e->converted_type == PQ_CT_LIST;
}

static int elem_is_map(const PqSchemaElem *e) {
    return e->logical == PQ_LT_MAP || e->converted_type == PQ_CT_MAP;
}

/* How a leaf's physical values map onto a vectra type. */
static void leaf_set_conv(PqLeaf *L, const PqSchemaElem *e) {
    int lt = e->logical, ct = e->converted_type;
    int is_dec = lt == PQ_LT_DECIMAL || ct == PQ_CT_DECIMAL;
    int32_t scale = lt == PQ_LT_DECIMAL ? e->lt_scale : e->scale;
    double dec_div = pow(10.0, (double)(scale > 0 && scale < 80 ? scale : 0));

    int unit = 0;       /* time unit for TIME / TIMESTAMP */
    int is_ts = 0, is_time = 0, utc = 1;
    if (lt == PQ_LT_TIMESTAMP) { is_ts = 1; unit = e->lt_unit; utc = e->lt_utc; }
    else if (ct == PQ_CT_TIMESTAMP_MILLIS) { is_ts = 1; unit = PQ_UNIT_MILLIS; }
    else if (ct == PQ_CT_TIMESTAMP_MICROS) { is_ts = 1; unit = PQ_UNIT_MICROS; }
    if (lt == PQ_LT_TIME) { is_time = 1; unit = e->lt_unit; }
    else if (ct == PQ_CT_TIME_MILLIS) { is_time = 1; unit = PQ_UNIT_MILLIS; }
    else if (ct == PQ_CT_TIME_MICROS) { is_time = 1; unit = PQ_UNIT_MICROS; }
    double unit_div = unit == PQ_UNIT_MILLIS ? 1e3
                    : unit == PQ_UNIT_MICROS ? 1e6
                    : unit == PQ_UNIT_NANOS  ? 1e9 : 1.0;

    L->div = 1.0;
    L->annotation = NULL;
    switch (L->phys) {
    case PQ_BOOLEAN:
        L->conv = PQC_BOOL; L->out_type = VEC_BOOL;
        break;
    case PQ_INT32:
        if (lt == PQ_LT_DATE || ct == PQ_CT_DATE) {
            L->conv = PQC_INT_DIV; L->out_type = VEC_DOUBLE;
            L->annotation = pq_strdup("Date");
        } else if (is_dec) {
            L->conv = PQC_INT_DIV; L->out_type = VEC_DOUBLE; L->div = dec_div;
        } else if (is_time) {
            L->conv = PQC_INT_DIV; L->out_type = VEC_DOUBLE; L->div = unit_div;
        } else if ((lt == PQ_LT_INTEGER && !e->lt_signed && e->lt_bit_width == 32) ||
                   ct == PQ_CT_UINT_32) {
            L->conv = PQC_U32; L->out_type = VEC_INT64;
        } else {
            L->conv = PQC_I32; L->out_type = VEC_INT32;
        }
        break;
    case PQ_INT64:
        if (is_ts) {
            L->conv = PQC_INT_DIV; L->out_type = VEC_DOUBLE; L->div = unit_div;
            L->annotation = pq_strdup(utc ? "POSIXct|UTC" : "POSIXct|");
        } else if (is_time) {
            L->conv = PQC_INT_DIV; L->out_type = VEC_DOUBLE; L->div = unit_div;
        } else if (is_dec) {
            L->conv = PQC_INT_DIV; L->out_type = VEC_DOUBLE; L->div = dec_div;
        } else if ((lt == PQ_LT_INTEGER && !e->lt_signed) || ct == PQ_CT_UINT_64) {
            L->conv = PQC_U64; L->out_type = VEC_DOUBLE;
        } else {
            L->conv = PQC_I64; L->out_type = VEC_INT64;
        }
        break;
    case PQ_INT96:
        L->conv = PQC_INT96; L->out_type = VEC_DOUBLE;
        L->annotation = pq_strdup("POSIXct|UTC");
        break;
    case PQ_FLOAT:
        L->conv = PQC_FLOAT; L->out_type = VEC_DOUBLE;
        break;
    case PQ_DOUBLE:
        L->conv = PQC_DOUBLE; L->out_type = VEC_DOUBLE;
        break;
    case PQ_BYTE_ARRAY:
        if (is_dec) { L->conv = PQC_DEC_BYTES; L->out_type = VEC_DOUBLE; L->div = dec_div; }
        else { L->conv = PQC_STRING; L->out_type = VEC_STRING; }
        break;
    case PQ_FIXED_LEN_BYTE_ARRAY:
        if (L->type_length <= 0) {
            L->unsupported = pq_strdup("FIXED_LEN_BYTE_ARRAY without a length");
            L->conv = PQC_STRING; L->out_type = VEC_STRING;
        } else if (is_dec) {
            L->conv = PQC_DEC_BYTES; L->out_type = VEC_DOUBLE; L->div = dec_div;
        } else if (lt == PQ_LT_FLOAT16 && L->type_length == 2) {
            L->conv = PQC_FLOAT16; L->out_type = VEC_DOUBLE;
        } else if (lt == PQ_LT_UUID && L->type_length == 16) {
            L->conv = PQC_UUID; L->out_type = VEC_STRING;
        } else {
            L->conv = PQC_STRING; L->out_type = VEC_STRING;
        }
        break;
    default: {
        char msg[96];
        snprintf(msg, sizeof(msg), "unknown physical type %d", L->phys);
        L->unsupported = pq_strdup(msg);
        L->conv = PQC_STRING; L->out_type = VEC_STRING;
    }
    }
}

typedef struct {
    PqLeaf *v;
    int     n, cap;
} LeafVec;

/* Build the leaf for the primitive at the end of path[0..depth]. */
static int make_leaf(const PqSchemaElem *const *path, int depth, int ordinal,
                     LeafVec *out) {
    if (out->n == out->cap) {
        int cap = out->cap ? out->cap * 2 : 16;
        PqLeaf *v = (PqLeaf *)realloc(out->v, (size_t)cap * sizeof(PqLeaf));
        if (!v) return -1;
        out->v = v;
        out->cap = cap;
    }
    PqLeaf *L = &out->v[out->n];
    memset(L, 0, sizeof(*L));
    L->leaf = ordinal;

    int def = 0, rep = 0;
    L->list_null_def = L->list_elem_def = -1;
    for (int i = 0; i <= depth; i++) {
        int r = path[i]->repetition;
        if (r == PQ_OPTIONAL) def++;
        else if (r == PQ_REPEATED) {
            if (rep == 0) { L->list_null_def = def; L->list_elem_def = def + 1; }
            def++;
            rep++;
        }
    }
    L->max_def = def;
    L->max_rep = rep;

    /* Column name: the path, less the wrapper groups the LIST and MAP layouts
       insert (the repeated group, and a LIST's single element wrapper). */
    uint8_t skip[PQ_MAX_SCHEMA_DEPTH + 1];
    memset(skip, 0, sizeof(skip));
    for (int i = 0; i < depth; i++) {
        const PqSchemaElem *nx = path[i + 1];
        if (nx->repetition != PQ_REPEATED) continue;
        if (elem_is_list(path[i])) {
            skip[i + 1] = 1;
            if (nx->num_children == 1 && i + 2 <= depth) skip[i + 2] = 1;
        } else if (elem_is_map(path[i]) ||
                   nx->converted_type == PQ_CT_MAP_KEY_VALUE) {
            skip[i + 1] = 1;
        }
    }
    size_t len = 1;
    for (int i = 0; i <= depth; i++)
        if (!skip[i]) len += strlen(path[i]->name) + 1;
    L->name = (char *)malloc(len);
    if (!L->name) return -1;
    L->name[0] = '\0';
    size_t pos = 0;
    for (int i = 0; i <= depth; i++) {
        if (skip[i]) continue;
        size_t k = strlen(path[i]->name);
        if (pos) L->name[pos++] = '.';
        memcpy(L->name + pos, path[i]->name, k);
        pos += k;
    }
    L->name[pos] = '\0';

    const PqSchemaElem *e = path[depth];
    L->phys = e->type;
    L->type_length = e->type_length;
    leaf_set_conv(L, e);

    if (rep > 1) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "nested lists (repetition depth %d) are not supported", rep);
        free(L->unsupported);
        L->unsupported = pq_strdup(msg);
    }
    if (rep > 0) {
        /* A list column's elements are joined into one string per row. */
        L->out_type = VEC_STRING;
        free(L->annotation);
        L->annotation = NULL;
    }
    L->stats_ok = rep == 0 && !L->unsupported;
    out->n++;
    return 0;
}

static int walk_schema(const PqFileMeta *m, int *idx, int depth,
                       const PqSchemaElem **path, int *ordinal, LeafVec *out,
                       char *err, size_t errlen) {
    if (depth > PQ_MAX_SCHEMA_DEPTH) {
        snprintf(err, errlen, "Parquet schema nests deeper than %d levels",
                 PQ_MAX_SCHEMA_DEPTH);
        return -1;
    }
    if (*idx >= m->n_schema) {
        snprintf(err, errlen, "Parquet schema is truncated");
        return -1;
    }
    const PqSchemaElem *e = &m->schema[(*idx)++];
    path[depth] = e;
    if (e->num_children < 0) {
        snprintf(err, errlen, "Parquet schema has a negative child count");
        return -1;
    }
    if (e->num_children == 0) {
        if (e->type < 0) return 0;          /* empty group: no column */
        if (make_leaf(path, depth, (*ordinal)++, out)) {
            snprintf(err, errlen, "out of memory");
            return -1;
        }
        return 0;
    }
    for (int k = 0; k < e->num_children; k++)
        if (walk_schema(m, idx, depth + 1, path, ordinal, out, err, errlen))
            return -1;
    return 0;
}

static void leaves_free(PqLeaf *v, int n) {
    if (!v) return;
    for (int i = 0; i < n; i++) {
        free(v[i].name);
        free(v[i].annotation);
        free(v[i].unsupported);
    }
    free(v);
}

static int build_leaves(PqFile *f, char *err, size_t errlen) {
    const PqFileMeta *m = &f->meta;
    const PqSchemaElem *path[PQ_MAX_SCHEMA_DEPTH + 1];
    LeafVec lv = { NULL, 0, 0 };
    int idx = 1, ordinal = 0;
    int nroot = m->schema[0].num_children;
    if (nroot < 0) {
        snprintf(err, errlen, "Parquet schema has a negative child count");
        return -1;
    }
    for (int k = 0; k < nroot; k++) {
        if (walk_schema(m, &idx, 0, path, &ordinal, &lv, err, errlen)) {
            leaves_free(lv.v, lv.n);
            return -1;
        }
    }
    if (lv.n == 0) {
        leaves_free(lv.v, lv.n);
        snprintf(err, errlen, "Parquet file has no columns");
        return -1;
    }
    /* Column names identify columns to every verb, so disambiguate the rare
       collision (a field named like a flattened struct path). */
    for (int i = 1; i < lv.n; i++) {
        for (int j = 0; j < i; j++) {
            if (strcmp(lv.v[i].name, lv.v[j].name) != 0) continue;
            size_t n = strlen(lv.v[i].name) + 16;
            char *nm = (char *)malloc(n);
            if (!nm) break;
            snprintf(nm, n, "%s_%d", lv.v[i].name, i + 1);
            free(lv.v[i].name);
            lv.v[i].name = nm;
            j = -1;                         /* recheck the new name */
        }
    }
    f->leaves = lv.v;
    f->n_leaves = lv.n;
    for (int r = 0; r < m->n_rgs; r++) {
        if (m->rgs[r].n_cols != lv.n) {
            snprintf(err, errlen,
                     "row group %d has %d column chunks, the schema %d leaves",
                     r, m->rgs[r].n_cols, lv.n);
            return -1;
        }
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Files                                                              */
/* ------------------------------------------------------------------ */

static void pq_file_free(PqFile *f) {
    if (!f) return;
    free(f->path);
    pq_meta_free(&f->meta);
    leaves_free(f->leaves, f->n_leaves);
    free(f->map);
    memset(f, 0, sizeof(*f));
}

static int pq_file_open(const char *path, PqFile *f, char *err, size_t errlen) {
    memset(f, 0, sizeof(*f));
    f->path = pq_strdup(path);
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        snprintf(err, errlen, "cannot open file");
        return -1;
    }
    uint8_t tail[8], head[4];
    int64_t size = -1;
    if (PQ_FSEEK64(fp, 0, SEEK_END) == 0) size = (int64_t)PQ_FTELL64(fp);
    if (size < 12 || PQ_FSEEK64(fp, 0, SEEK_SET) != 0 ||
        fread(head, 1, 4, fp) != 4 || PQ_FSEEK64(fp, size - 8, SEEK_SET) != 0 ||
        fread(tail, 1, 8, fp) != 8 || memcmp(head, "PAR1", 4) != 0 ||
        memcmp(tail + 4, "PAR1", 4) != 0) {
        fclose(fp);
        snprintf(err, errlen, "not a Parquet file (missing PAR1 magic)");
        return -1;
    }
    uint32_t flen = (uint32_t)tail[0] | ((uint32_t)tail[1] << 8) |
                    ((uint32_t)tail[2] << 16) | ((uint32_t)tail[3] << 24);
    if ((int64_t)flen > size - 12) {
        fclose(fp);
        snprintf(err, errlen, "Parquet footer length exceeds the file");
        return -1;
    }
    uint8_t *footer = (uint8_t *)malloc(flen ? flen : 1);
    if (!footer || PQ_FSEEK64(fp, size - 8 - (int64_t)flen, SEEK_SET) != 0 ||
        fread(footer, 1, flen, fp) != flen) {
        free(footer);
        fclose(fp);
        snprintf(err, errlen, "cannot read Parquet footer");
        return -1;
    }
    fclose(fp);
    f->file_size = size;
    if (pq_meta_decode(footer, flen, &f->meta, err, errlen)) return -1;
    return build_leaves(f, err, errlen);
}

/* Match a later file's columns to the dataset's by name. */
static int map_file(const PqFile *first, PqFile *f, char *err, size_t errlen) {
    f->map = (int *)malloc((size_t)first->n_leaves * sizeof(int));
    if (!f->map) { snprintf(err, errlen, "out of memory"); return -1; }
    for (int j = 0; j < first->n_leaves; j++) {
        const PqLeaf *a = &first->leaves[j];
        int found = -1;
        for (int l = 0; l < f->n_leaves; l++)
            if (strcmp(f->leaves[l].name, a->name) == 0) { found = l; break; }
        if (found < 0) {
            snprintf(err, errlen, "column '%s' is missing (present in '%s')",
                     a->name, first->path);
            return -1;
        }
        const PqLeaf *b = &f->leaves[found];
        const char *aa = a->annotation ? a->annotation : "";
        const char *ba = b->annotation ? b->annotation : "";
        if (a->out_type != b->out_type || strcmp(aa, ba) != 0 ||
            (a->max_rep > 0) != (b->max_rep > 0)) {
            snprintf(err, errlen, "column '%s' is %s%s%s%s here but %s%s%s%s in '%s'",
                     a->name, vec_type_name(b->out_type), *ba ? " (" : "", ba,
                     *ba ? ")" : "", vec_type_name(a->out_type),
                     *aa ? " (" : "", aa, *aa ? ")" : "", first->path);
            return -1;
        }
        f->map[j] = found;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Row-group statistics                                               */
/* ------------------------------------------------------------------ */

/* First 8 bytes as a big-endian integer, padded with `pad`: the same prefix
   key the .vtr zone maps compare strings by. */
static uint64_t pack_prefix(const uint8_t *s, uint32_t len, uint8_t pad) {
    uint64_t r = 0;
    for (uint32_t i = 0; i < 8; i++) r = (r << 8) | (i < len ? s[i] : pad);
    return r;
}

static void rg_stats(const ParquetScanNode *pn, const PqFile *f,
                     const PqRowGroup *rg, Vtr1ColStat *st) {
    for (int j = 0; j < pn->full.n_cols; j++) {
        memset(&st[j], 0, sizeof(st[j]));
        const PqLeaf *L = &f->leaves[f->map[j]];
        const PqStats *s = &rg->cols[f->map[j]].stats;
        if (!L->stats_ok || !rg->cols[f->map[j]].has_meta) continue;
        if (s->null_count >= 0) st[j].null_count = (uint64_t)s->null_count;
        if (!s->min || !s->max) continue;
        /* The deprecated min/max fields compare signed, which misorders
           unsigned integers and non-ASCII bytes. */
        int signed_order_ok = s->min_is_new ||
            (L->conv != PQC_U32 && L->conv != PQC_U64 && L->conv != PQC_STRING);
        if (!signed_order_ok) continue;
        VecType t = pn->full.col_types[j];
        if (vec_type_is_int(t)) {
            if (pq_stat_to_int64(L, s->min, s->min_len, &st[j].i64.min) &&
                pq_stat_to_int64(L, s->max, s->max_len, &st[j].i64.max))
                st[j].has_stats = 1;
        } else if (t == VEC_DOUBLE) {
            if (pq_stat_to_double(L, s->min, s->min_len, &st[j].dbl.min) &&
                pq_stat_to_double(L, s->max, s->max_len, &st[j].dbl.max))
                st[j].has_stats = 1;
        } else if (t == VEC_STRING && L->conv == PQC_STRING &&
                   L->phys == PQ_BYTE_ARRAY) {
            st[j].i64.min = (int64_t)pack_prefix(s->min, s->min_len, 0x00);
            st[j].i64.max = (int64_t)pack_prefix(s->max, s->max_len, 0xFF);
            st[j].has_stats = 1;
        }
    }
}

/* ------------------------------------------------------------------ */
/*  Node                                                               */
/* ------------------------------------------------------------------ */

static void close_rg(ParquetScanNode *pn) {
    if (pn->cursors)
        for (int k = 0; k < pn->n_out; k++) pq_cursor_free(&pn->cursors[k]);
    pn->rg_open = 0;
    pn->rg_left = 0;
}

static void pq_raise(ParquetScanNode *pn, const PqFile *f, const char *msg) {
    char buf[1024];
    snprintf(buf, sizeof(buf), "tbl_parquet: %s: %s", f ? f->path : "", msg);
    close_rg(pn);
    vectra_error("%s", buf);
}

/* Open the next row group that can hold matching rows: read the compressed
   chunk of every output column. Returns 0 when there are none left. */
static int open_next_rg(ParquetScanNode *pn) {
    Vtr1ColStat *st = NULL;
    while (pn->file_i < pn->n_files) {
        PqFile *f = &pn->files[pn->file_i];
        if (pn->rg_i >= f->meta.n_rgs) {
            pn->file_i++;
            pn->rg_i = 0;
            continue;
        }
        const PqRowGroup *rg = &f->meta.rgs[pn->rg_i++];
        if (rg->num_rows <= 0) continue;
        if (pn->predicate) {
            if (!st) {
                st = (Vtr1ColStat *)calloc((size_t)pn->full.n_cols, sizeof(*st));
                if (!st) vectra_error("out of memory");
            }
            rg_stats(pn, f, rg, st);
            if (!predicate_might_match(pn->predicate, st, &pn->full,
                                       rg->num_rows))
                continue;
        }
        free(st);
        st = NULL;

        char err[512];
        err[0] = '\0';
        FILE *fp = fopen(f->path, "rb");
        if (!fp) pq_raise(pn, f, "cannot open file");
        for (int k = 0; k < pn->n_out && !err[0]; k++) {
            const PqLeaf *L = &f->leaves[f->map[pn->out_cols[k]]];
            const PqColumnChunk *cc = &rg->cols[L->leaf];
            if (L->unsupported) {
                pq_cursor_init(&pn->cursors[k], L, 0, NULL, 0, pn->list_sep);
                continue;
            }
            if (!cc->has_meta) {
                snprintf(err, sizeof(err), "column '%s' has no chunk metadata", L->name);
                break;
            }
            if (cc->external) {
                snprintf(err, sizeof(err),
                         "column '%s' stores its data in another file, which is not supported",
                         L->name);
                break;
            }
            if (!pq_codec_supported(cc->codec)) {
                snprintf(err, sizeof(err),
                         "column '%s' uses %s compression, which is not supported",
                         L->name, pq_codec_name(cc->codec));
                break;
            }
            int64_t start = cc->data_page_offset;
            if (cc->dict_page_offset > 0 && cc->dict_page_offset < start)
                start = cc->dict_page_offset;
            int64_t len = cc->total_compressed_size;
            if (start < 4 || len <= 0 || start > f->file_size - 8 ||
                len > f->file_size - 8 - start) {
                snprintf(err, sizeof(err), "column '%s' chunk lies outside the file",
                         L->name);
                break;
            }
            uint8_t *buf = (uint8_t *)malloc((size_t)len);
            if (!buf) {
                snprintf(err, sizeof(err), "out of memory reading column '%s'", L->name);
                break;
            }
            if (PQ_FSEEK64(fp, start, SEEK_SET) != 0 ||
                fread(buf, 1, (size_t)len, fp) != (size_t)len) {
                free(buf);
                snprintf(err, sizeof(err), "cannot read column '%s'", L->name);
                break;
            }
            pq_cursor_init(&pn->cursors[k], L, cc->codec, buf, (size_t)len,
                           pn->list_sep);
        }
        fclose(fp);
        pn->rg_open = 1;
        pn->rg_left = rg->num_rows;
        if (err[0]) pq_raise(pn, f, err);
        return 1;
    }
    free(st);
    return 0;
}

static VecBatch *pq_next_batch(VecNode *self) {
    ParquetScanNode *pn = (ParquetScanNode *)self;
    pn->started = 1;
    for (;;) {
        if (!pn->rg_open && !open_next_rg(pn)) return NULL;
        if (pn->rg_left <= 0) { close_rg(pn); continue; }

        int64_t n = pn->rg_left < pn->batch_size ? pn->rg_left : pn->batch_size;
        const VecSchema *os = &pn->base.output_schema;
        VecBatch *b = vec_batch_alloc(pn->n_out, n);
        vec_batch_set_names(b, os->col_names);
        for (int k = 0; k < pn->n_out; k++)
            b->columns[k] = vec_array_alloc(os->col_types[k], n);

        int n_out = pn->n_out;
        int *failed = (int *)calloc((size_t)(n_out > 0 ? n_out : 1), sizeof(int));
        if (!failed) { vec_batch_free(b); vectra_error("out of memory"); }
        PqCursor *cur = pn->cursors;
        #ifdef _OPENMP
        #pragma omp parallel for schedule(dynamic, 1) \
            if (n_out > 1 && n * n_out >= VEC_OMP_THRESHOLD)
        #endif
        for (int k = 0; k < n_out; k++)
            failed[k] = pq_cursor_read(&cur[k], n, &b->columns[k]) != 0;

        for (int k = 0; k < n_out; k++) {
            if (failed[k]) {
                char msg[256];
                snprintf(msg, sizeof(msg), "%s", cur[k].err);
                free(failed);
                vec_batch_free(b);
                pq_raise(pn, &pn->files[pn->file_i], msg);
            }
        }
        free(failed);
        pn->rg_left -= n;
        return b;
    }
}

static int64_t pq_static_rows(const VecNode *self) {
    const ParquetScanNode *pn = (const ParquetScanNode *)self;
    if (pn->predicate || pn->started) return -1;
    int64_t total = 0;
    for (int f = 0; f < pn->n_files; f++) {
        const PqFileMeta *m = &pn->files[f].meta;
        for (int r = 0; r < m->n_rgs; r++)
            if (m->rgs[r].num_rows > 0) total += m->rgs[r].num_rows;
    }
    return total;
}

static void pq_node_free(VecNode *self) {
    ParquetScanNode *pn = (ParquetScanNode *)self;
    close_rg(pn);
    free(pn->cursors);
    for (int f = 0; f < pn->n_files; f++) pq_file_free(&pn->files[f]);
    free(pn->files);
    free(pn->out_cols);
    free(pn->list_sep);
    vec_expr_free(pn->predicate);
    vec_schema_free(&pn->full);
    vec_schema_free(&pn->base.output_schema);
    free(pn);
}

/* Output schema = the dataset columns named by out_cols, annotations kept. */
static void rebuild_output_schema(ParquetScanNode *pn) {
    vec_schema_free(&pn->base.output_schema);
    char **names = (char **)malloc((size_t)pn->n_out * sizeof(char *));
    VecType *types = (VecType *)malloc((size_t)pn->n_out * sizeof(VecType));
    if (!names || !types) {
        free(names);
        free(types);
        vectra_error("out of memory");
    }
    for (int k = 0; k < pn->n_out; k++) {
        names[k] = pn->full.col_names[pn->out_cols[k]];
        types[k] = pn->full.col_types[pn->out_cols[k]];
    }
    pn->base.output_schema = vec_schema_create(pn->n_out, names, types);
    free(names);
    free(types);
    for (int k = 0; k < pn->n_out; k++) {
        const char *a = pn->full.col_annotations[pn->out_cols[k]];
        if (a) pn->base.output_schema.col_annotations[k] = pq_strdup(a);
    }
}

VecNode *parquet_scan_node_create(int n_paths, const char **paths,
                                  int64_t batch_size, const char *list_sep) {
    if (n_paths < 1) vectra_error("tbl_parquet: no files given");
    ParquetScanNode *pn = (ParquetScanNode *)calloc(1, sizeof(ParquetScanNode));
    if (!pn) vectra_error("out of memory");
    pn->files = (PqFile *)calloc((size_t)n_paths, sizeof(PqFile));
    if (!pn->files) { free(pn); vectra_error("out of memory"); }
    pn->n_files = n_paths;
    pn->batch_size = batch_size > 0 ? batch_size : 65536;
    pn->list_sep = pq_strdup(list_sep ? list_sep : ";");

    char err[512];
    for (int i = 0; i < n_paths; i++) {
        err[0] = '\0';
        int rc = pq_file_open(paths[i], &pn->files[i], err, sizeof(err));
        if (rc == 0 && i == 0) {
            PqFile *f0 = &pn->files[0];
            f0->map = (int *)malloc((size_t)f0->n_leaves * sizeof(int));
            if (!f0->map) { rc = -1; snprintf(err, sizeof(err), "out of memory"); }
            else for (int j = 0; j < f0->n_leaves; j++) f0->map[j] = j;
        } else if (rc == 0) {
            rc = map_file(&pn->files[0], &pn->files[i], err, sizeof(err));
        }
        if (rc) {
            char msg[1024];
            snprintf(msg, sizeof(msg), "tbl_parquet: %s: %s", paths[i], err);
            pq_node_free(&pn->base);
            vectra_error("%s", msg);
        }
    }

    const PqFile *f0 = &pn->files[0];
    int nc = f0->n_leaves;
    char **names = (char **)malloc((size_t)nc * sizeof(char *));
    VecType *types = (VecType *)malloc((size_t)nc * sizeof(VecType));
    pn->out_cols = (int *)malloc((size_t)nc * sizeof(int));
    pn->cursors = (PqCursor *)calloc((size_t)nc, sizeof(PqCursor));
    if (!names || !types || !pn->out_cols || !pn->cursors) {
        free(names);
        free(types);
        pq_node_free(&pn->base);
        vectra_error("out of memory");
    }
    for (int j = 0; j < nc; j++) {
        names[j] = f0->leaves[j].name;
        types[j] = f0->leaves[j].out_type;
        pn->out_cols[j] = j;
    }
    pn->full = vec_schema_create(nc, names, types);
    free(names);
    free(types);
    for (int j = 0; j < nc; j++)
        if (f0->leaves[j].annotation)
            pn->full.col_annotations[j] = pq_strdup(f0->leaves[j].annotation);
    pn->n_out = nc;
    rebuild_output_schema(pn);

    pn->base.next_batch = pq_next_batch;
    pn->base.free_node = pq_node_free;
    pn->base.static_rows = pq_static_rows;
    pn->base.kind = "ParquetScanNode";
    pn->base.row_count_hint = pq_static_rows(&pn->base);
    return &pn->base;
}

void parquet_scan_prune(VecNode *node, const uint8_t *needed, int n_needed) {
    ParquetScanNode *pn = (ParquetScanNode *)node;
    if (pn->started) return;
    int keep = 0;
    for (int k = 0; k < pn->n_out && k < n_needed; k++) keep += needed[k] != 0;
    if (keep == pn->n_out) return;

    int *cols = (int *)malloc((size_t)(keep ? keep : 1) * sizeof(int));
    if (!cols) vectra_error("out of memory");
    int m = 0;
    for (int k = 0; k < pn->n_out && k < n_needed; k++)
        if (needed[k]) cols[m++] = pn->out_cols[k];
    if (m == 0) {
        /* Nothing is read by name (a bare row count): keep the cheapest
           column, a flat fixed-width one when there is one. */
        int best = pn->out_cols[0];
        const PqFile *f0 = &pn->files[0];
        for (int k = 0; k < pn->n_out; k++) {
            const PqLeaf *L = &f0->leaves[pn->out_cols[k]];
            if (L->max_rep == 0 && !L->unsupported && L->out_type != VEC_STRING) {
                best = pn->out_cols[k];
                break;
            }
        }
        cols[m++] = best;
    }
    free(pn->out_cols);
    pn->out_cols = cols;
    pn->n_out = m;
    rebuild_output_schema(pn);
}

VecExpr **parquet_scan_predicate_slot(VecNode *node) {
    ParquetScanNode *pn = (ParquetScanNode *)node;
    return pn->started ? NULL : &pn->predicate;
}

int parquet_scan_describe(const VecNode *node, char *buf, int bufsize) {
    const ParquetScanNode *pn = (const ParquetScanNode *)node;
    int n;
    if (pn->n_out < pn->full.n_cols)
        n = snprintf(buf, (size_t)bufsize, "streaming parquet, %d/%d cols (pruned)",
                     pn->n_out, pn->full.n_cols);
    else
        n = snprintf(buf, (size_t)bufsize, "streaming parquet, %d cols", pn->n_out);
    if (n < 0 || n >= bufsize) return n < 0 ? 0 : bufsize - 1;
    if (pn->n_files > 1) {
        int k = snprintf(buf + n, (size_t)(bufsize - n), ", %d files", pn->n_files);
        if (k > 0) n = n + k < bufsize ? n + k : bufsize - 1;
    }
    if (pn->predicate && n < bufsize - 1) {
        int k = snprintf(buf + n, (size_t)(bufsize - n), ", predicate pushdown");
        if (k > 0) n = n + k < bufsize ? n + k : bufsize - 1;
    }
    return n;
}
