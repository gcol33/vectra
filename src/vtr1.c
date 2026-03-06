#include "vtr1.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/* --- Helpers for little-endian I/O --- */

static void write_u8(FILE *fp, uint8_t v)   { fwrite(&v, 1, 1, fp); }
static void write_u16(FILE *fp, uint16_t v) { fwrite(&v, 2, 1, fp); }
static void write_u32(FILE *fp, uint32_t v) { fwrite(&v, 4, 1, fp); }
static void write_u64(FILE *fp, uint64_t v) { fwrite(&v, 8, 1, fp); }

static uint8_t read_u8(FILE *fp) {
    uint8_t v = 0;
    if (fread(&v, 1, 1, fp) != 1) vectra_error("unexpected end of file");
    return v;
}
static uint16_t read_u16(FILE *fp) {
    uint16_t v = 0;
    if (fread(&v, 2, 1, fp) != 1) vectra_error("unexpected end of file");
    return v;
}
static uint32_t read_u32(FILE *fp) {
    uint32_t v = 0;
    if (fread(&v, 4, 1, fp) != 1) vectra_error("unexpected end of file");
    return v;
}
static uint64_t read_u64(FILE *fp) {
    uint64_t v = 0;
    if (fread(&v, 8, 1, fp) != 1) vectra_error("unexpected end of file");
    return v;
}

/* --- Write --- */

void vtr1_write_header(FILE *fp, const VecSchema *schema, uint32_t n_rowgroups) {
    /* Magic */
    fwrite("VTR1", 1, 4, fp);

    /* Always write version 3 (annotations + stats) */
    (void)0; /* annotations checked below for compat, but v3 always written */
    write_u16(fp, (uint16_t)3);
    /* n_cols */
    write_u16(fp, (uint16_t)schema->n_cols);
    /* Column definitions (v3 always includes annotations) */
    for (int i = 0; i < schema->n_cols; i++) {
        uint16_t name_len = (uint16_t)strlen(schema->col_names[i]);
        write_u16(fp, name_len);
        fwrite(schema->col_names[i], 1, name_len, fp);
        write_u8(fp, (uint8_t)schema->col_types[i]);
        /* annotation string (length-prefixed, 0 = none) */
        const char *ann = (schema->col_annotations)
                          ? schema->col_annotations[i] : NULL;
        uint16_t ann_len = ann ? (uint16_t)strlen(ann) : 0;
        write_u16(fp, ann_len);
        if (ann_len > 0) fwrite(ann, 1, ann_len, fp);
    }
    /* n_rowgroups */
    write_u32(fp, n_rowgroups);
}

void vtr1_write_rowgroup(FILE *fp, const VecBatch *batch) {
    /* n_rows */
    write_u64(fp, (uint64_t)batch->n_rows);

    /* v3: per-column statistics */
    for (int c = 0; c < batch->n_cols; c++) {
        const VecArray *col = &batch->columns[c];
        if (col->type == VEC_STRING || batch->n_rows == 0) {
            write_u8(fp, 0); /* no stats */
            write_u64(fp, 0); /* min placeholder */
            write_u64(fp, 0); /* max placeholder */
        } else if (col->type == VEC_INT64) {
            int64_t mn = INT64_MAX, mx = INT64_MIN;
            int found = 0;
            for (int64_t i = 0; i < batch->n_rows; i++) {
                if (!vec_array_is_valid(col, i)) continue;
                if (!found || col->buf.i64[i] < mn) mn = col->buf.i64[i];
                if (!found || col->buf.i64[i] > mx) mx = col->buf.i64[i];
                found = 1;
            }
            write_u8(fp, found ? (uint8_t)1 : (uint8_t)0);
            write_u64(fp, found ? (uint64_t)mn : 0);
            write_u64(fp, found ? (uint64_t)mx : 0);
        } else if (col->type == VEC_DOUBLE) {
            double mn = 0, mx = 0;
            int found = 0;
            for (int64_t i = 0; i < batch->n_rows; i++) {
                if (!vec_array_is_valid(col, i)) continue;
                if (!found || col->buf.dbl[i] < mn) mn = col->buf.dbl[i];
                if (!found || col->buf.dbl[i] > mx) mx = col->buf.dbl[i];
                found = 1;
            }
            write_u8(fp, found ? (uint8_t)1 : (uint8_t)0);
            uint64_t mn_bits, mx_bits;
            memcpy(&mn_bits, &mn, 8);
            memcpy(&mx_bits, &mx, 8);
            write_u64(fp, found ? mn_bits : 0);
            write_u64(fp, found ? mx_bits : 0);
        } else if (col->type == VEC_BOOL) {
            uint8_t has_false = 0, has_true = 0;
            for (int64_t i = 0; i < batch->n_rows; i++) {
                if (!vec_array_is_valid(col, i)) continue;
                if (col->buf.bln[i]) has_true = 1; else has_false = 1;
            }
            write_u8(fp, 1);
            /* Store min=has_false, max=has_true in the 8-byte slots */
            write_u64(fp, (uint64_t)has_false);
            write_u64(fp, (uint64_t)has_true);
        }
    }

    /* Column data */
    for (int c = 0; c < batch->n_cols; c++) {
        const VecArray *col = &batch->columns[c];
        int64_t vbytes = vec_validity_bytes(batch->n_rows);

        /* Validity bitmap */
        fwrite(col->validity, 1, (size_t)vbytes, fp);

        /* Data */
        switch (col->type) {
        case VEC_INT64:
            fwrite(col->buf.i64, sizeof(int64_t), (size_t)batch->n_rows, fp);
            break;
        case VEC_DOUBLE:
            fwrite(col->buf.dbl, sizeof(double), (size_t)batch->n_rows, fp);
            break;
        case VEC_BOOL:
            fwrite(col->buf.bln, 1, (size_t)batch->n_rows, fp);
            break;
        case VEC_STRING:
            fwrite(col->buf.str.offsets, sizeof(int64_t),
                   (size_t)(batch->n_rows + 1), fp);
            write_u64(fp, (uint64_t)col->buf.str.data_len);
            fwrite(col->buf.str.data, 1, (size_t)col->buf.str.data_len, fp);
            break;
        }
    }
}

void vtr1_write(const char *path, const VecBatch *batch) {
    FILE *fp = fopen(path, "wb");
    if (!fp) vectra_error("cannot open file for writing: %s", path);

    VecSchema schema;
    memset(&schema, 0, sizeof(schema));
    schema.n_cols = batch->n_cols;
    schema.col_names = batch->col_names;
    schema.col_types = (VecType *)malloc((size_t)batch->n_cols * sizeof(VecType));
    if (!schema.col_types) { fclose(fp); vectra_error("alloc failed"); }
    for (int i = 0; i < batch->n_cols; i++)
        schema.col_types[i] = batch->columns[i].type;

    vtr1_write_header(fp, &schema, 1);
    vtr1_write_rowgroup(fp, batch);

    free(schema.col_types);
    fclose(fp);
}

/* --- Read --- */

Vtr1File *vtr1_open(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) vectra_error("cannot open file: %s", path);

    /* Magic */
    char magic[4];
    if (fread(magic, 1, 4, fp) != 4 || memcmp(magic, "VTR1", 4) != 0) {
        fclose(fp);
        vectra_error("not a valid .vtr file (bad magic): %s", path);
    }

    Vtr1File *file = (Vtr1File *)calloc(1, sizeof(Vtr1File));
    if (!file) { fclose(fp); vectra_error("alloc failed"); }
    file->fp = fp;

    /* Version */
    file->header.version = read_u16(fp);
    if (file->header.version < 1 || file->header.version > 3) {
        uint16_t ver = file->header.version;
        fclose(fp); free(file);
        vectra_error("unsupported .vtr version: %u", ver);
    }

    /* Schema */
    uint16_t n_cols = read_u16(fp);
    char **names = (char **)calloc(n_cols, sizeof(char *));
    VecType *types = (VecType *)calloc(n_cols, sizeof(VecType));
    char **annotations = (char **)calloc(n_cols, sizeof(char *));
    if ((!names || !types || !annotations) && n_cols > 0) {
        fclose(fp); free(file);
        vectra_error("alloc failed reading schema");
    }

    for (int i = 0; i < n_cols; i++) {
        uint16_t name_len = read_u16(fp);
        names[i] = (char *)malloc(name_len + 1);
        if (!names[i]) vectra_error("alloc failed");
        if (fread(names[i], 1, name_len, fp) != name_len)
            vectra_error("unexpected end of file reading column name");
        names[i][name_len] = '\0';
        types[i] = (VecType)read_u8(fp);
        /* v2: read annotation */
        if (file->header.version >= 2) {
            uint16_t ann_len = read_u16(fp);
            if (ann_len > 0) {
                annotations[i] = (char *)malloc(ann_len + 1);
                if (fread(annotations[i], 1, ann_len, fp) != ann_len)
                    vectra_error("unexpected end of file reading annotation");
                annotations[i][ann_len] = '\0';
            }
        }
    }

    file->header.schema = vec_schema_create(n_cols, names, types);
    /* Copy annotations into schema */
    for (int i = 0; i < n_cols; i++) {
        file->header.schema.col_annotations[i] = annotations[i];
        /* annotations[i] is now owned by schema, don't free */
        free(names[i]);
    }
    free(names);
    free(types);
    free(annotations);

    /* n_rowgroups */
    file->header.n_rowgroups = read_u32(fp);

    /* Build row group index by scanning through the file */
    file->rowgroups = (Vtr1RowGroup *)calloc(file->header.n_rowgroups,
                                              sizeof(Vtr1RowGroup));
    if (!file->rowgroups && file->header.n_rowgroups > 0)
        vectra_error("alloc failed for rowgroup index");

    int n_schema_cols = file->header.schema.n_cols;

    for (uint32_t rg = 0; rg < file->header.n_rowgroups; rg++) {
        file->rowgroups[rg].file_offset = ftell(fp);
        file->rowgroups[rg].col_stats = NULL;
        uint64_t n_rows = read_u64(fp);
        file->rowgroups[rg].n_rows = (int64_t)n_rows;

        /* v3: read per-column statistics */
        if (file->header.version >= 3) {
            Vtr1ColStat *stats = (Vtr1ColStat *)calloc(
                (size_t)n_schema_cols, sizeof(Vtr1ColStat));
            for (int c = 0; c < n_schema_cols; c++) {
                stats[c].has_stats = read_u8(fp);
                uint64_t val1 = read_u64(fp);
                uint64_t val2 = read_u64(fp);
                if (stats[c].has_stats) {
                    VecType t = file->header.schema.col_types[c];
                    if (t == VEC_INT64) {
                        stats[c].i64.min = (int64_t)val1;
                        stats[c].i64.max = (int64_t)val2;
                    } else if (t == VEC_DOUBLE) {
                        memcpy(&stats[c].dbl.min, &val1, 8);
                        memcpy(&stats[c].dbl.max, &val2, 8);
                    } else if (t == VEC_BOOL) {
                        stats[c].bln.min = (uint8_t)val1;
                        stats[c].bln.max = (uint8_t)val2;
                    }
                }
            }
            file->rowgroups[rg].col_stats = stats;
        }

        /* Skip column data */
        for (int c = 0; c < n_schema_cols; c++) {
            VecType t = file->header.schema.col_types[c];
            int64_t vbytes = vec_validity_bytes((int64_t)n_rows);
            /* Skip validity */
            fseek(fp, (long)vbytes, SEEK_CUR);

            /* Skip data */
            switch (t) {
            case VEC_INT64:
                fseek(fp, (long)(n_rows * 8), SEEK_CUR);
                break;
            case VEC_DOUBLE:
                fseek(fp, (long)(n_rows * 8), SEEK_CUR);
                break;
            case VEC_BOOL:
                fseek(fp, (long)n_rows, SEEK_CUR);
                break;
            case VEC_STRING: {
                /* Skip offsets */
                fseek(fp, (long)((n_rows + 1) * 8), SEEK_CUR);
                /* Read data_len, skip data */
                uint64_t data_len = read_u64(fp);
                fseek(fp, (long)data_len, SEEK_CUR);
                break;
            }
            }
        }
    }

    return file;
}

VecBatch *vtr1_read_rowgroup(Vtr1File *file, uint32_t rg_idx,
                             const int *col_mask) {
    if (rg_idx >= file->header.n_rowgroups)
        vectra_error("row group index out of range: %u >= %u",
                     rg_idx, file->header.n_rowgroups);

    const VecSchema *schema = &file->header.schema;
    int64_t n_rows = file->rowgroups[rg_idx].n_rows;

    /* Count selected columns */
    int n_selected = 0;
    for (int i = 0; i < schema->n_cols; i++)
        if (col_mask[i]) n_selected++;

    VecBatch *batch = vec_batch_alloc(n_selected, n_rows);

    /* Seek to row group start, skip n_rows field + v3 stats */
    long data_offset = (long)file->rowgroups[rg_idx].file_offset + 8;
    if (file->header.version >= 3) {
        /* Each column has 1 byte has_stats + 8 byte min + 8 byte max = 17 bytes */
        data_offset += (long)schema->n_cols * 17;
    }
    fseek(file->fp, data_offset, SEEK_SET);

    int out_col = 0;
    for (int c = 0; c < schema->n_cols; c++) {
        VecType t = schema->col_types[c];
        int64_t vbytes = vec_validity_bytes(n_rows);

        if (col_mask[c]) {
            /* Read this column */
            VecArray arr = vec_array_alloc(t, n_rows);
            if (fread(arr.validity, 1, (size_t)vbytes, file->fp) != (size_t)vbytes)
                vectra_error("unexpected end of file reading validity bitmap");

            switch (t) {
            case VEC_INT64:
                if (fread(arr.buf.i64, sizeof(int64_t), (size_t)n_rows, file->fp) != (size_t)n_rows)
                    vectra_error("unexpected end of file reading int64 data");
                break;
            case VEC_DOUBLE:
                if (fread(arr.buf.dbl, sizeof(double), (size_t)n_rows, file->fp) != (size_t)n_rows)
                    vectra_error("unexpected end of file reading double data");
                break;
            case VEC_BOOL:
                if (fread(arr.buf.bln, 1, (size_t)n_rows, file->fp) != (size_t)n_rows)
                    vectra_error("unexpected end of file reading bool data");
                break;
            case VEC_STRING: {
                if (fread(arr.buf.str.offsets, sizeof(int64_t),
                      (size_t)(n_rows + 1), file->fp) != (size_t)(n_rows + 1))
                    vectra_error("unexpected end of file reading string offsets");
                uint64_t data_len = read_u64(file->fp);
                arr.buf.str.data_len = (int64_t)data_len;
                arr.buf.str.data = (char *)malloc((size_t)data_len);
                if (!arr.buf.str.data && data_len > 0)
                    vectra_error("alloc failed for string data");
                if (fread(arr.buf.str.data, 1, (size_t)data_len, file->fp) != (size_t)data_len)
                    vectra_error("unexpected end of file reading string data");
                break;
            }
            }

            batch->columns[out_col] = arr;
            batch->col_names[out_col] = (char *)malloc(
                strlen(schema->col_names[c]) + 1);
            strcpy(batch->col_names[out_col], schema->col_names[c]);
            out_col++;
        } else {
            /* Skip this column */
            fseek(file->fp, (long)vbytes, SEEK_CUR);
            switch (t) {
            case VEC_INT64:  fseek(file->fp, (long)(n_rows * 8), SEEK_CUR); break;
            case VEC_DOUBLE: fseek(file->fp, (long)(n_rows * 8), SEEK_CUR); break;
            case VEC_BOOL:   fseek(file->fp, (long)n_rows, SEEK_CUR); break;
            case VEC_STRING: {
                fseek(file->fp, (long)((n_rows + 1) * 8), SEEK_CUR);
                uint64_t data_len = read_u64(file->fp);
                fseek(file->fp, (long)data_len, SEEK_CUR);
                break;
            }
            }
        }
    }

    return batch;
}

void vtr1_close(Vtr1File *file) {
    if (!file) return;
    if (file->fp) fclose(file->fp);
    vec_schema_free(&file->header.schema);
    if (file->rowgroups) {
        for (uint32_t rg = 0; rg < file->header.n_rowgroups; rg++)
            free(file->rowgroups[rg].col_stats);
        free(file->rowgroups);
    }
    free(file);
}
