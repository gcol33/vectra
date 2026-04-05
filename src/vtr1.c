#include "vtr1.h"
#include "vec_omp.h"
#include "vtr_codec.h"
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

/* Pack the first 8 bytes of a string into a big-endian uint64 for
   lexicographic comparison via zone maps.  Short strings are padded:
   - pad = 0x00 for min values (smallest possible completion)
   - pad = 0xFF for max values (largest possible completion) */
static uint64_t pack_str_prefix(const char *s, int64_t len, uint8_t pad) {
    uint64_t r = 0;
    for (int i = 0; i < 8; i++) {
        uint8_t b = (i < len) ? (uint8_t)s[i] : pad;
        r = (r << 8) | b;
    }
    return r;
}

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

    /* Always write version 4 (annotations + stats + columnar encoding) */
    write_u16(fp, (uint16_t)4);
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
        if (batch->n_rows == 0) {
            write_u8(fp, 0); /* no stats */
            write_u64(fp, 0); /* min placeholder */
            write_u64(fp, 0); /* max placeholder */
        } else if (col->type == VEC_STRING) {
            /* String zone maps: pack first 8 bytes of lex min/max as big-endian
               uint64.  Min padded with 0x00, max padded with 0xFF so comparisons
               are conservative (never skip a batch that could match). */
            const int64_t *offs = col->buf.str.offsets;
            const char    *data = col->buf.str.data;
            int found = 0;
            uint64_t mn = 0, mx = 0;
            for (int64_t i = 0; i < batch->n_rows; i++) {
                if (!vec_array_is_valid(col, i)) continue;
                int64_t slen = offs[i + 1] - offs[i];
                const char *s = data + offs[i];
                uint64_t lo = pack_str_prefix(s, slen, 0x00);
                uint64_t hi = pack_str_prefix(s, slen, 0xFF);
                if (!found) {
                    mn = lo;
                    mx = hi;
                    found = 1;
                } else {
                    if (lo < mn) mn = lo;
                    if (hi > mx) mx = hi;
                }
            }
            write_u8(fp, found ? (uint8_t)1 : (uint8_t)0);
            uint64_t mn_store = found ? mn : 0;
            uint64_t mx_store = found ? mx : 0;
            write_u64(fp, mn_store);
            write_u64(fp, mx_store);
        } else if (col->type == VEC_INT64) {
            int64_t mn = INT64_MAX, mx = INT64_MIN;
            int found = 0;
            #pragma omp parallel for if(batch->n_rows > VEC_OMP_THRESHOLD) schedule(static) reduction(min:mn) reduction(max:mx) reduction(|:found)
            for (int64_t i = 0; i < batch->n_rows; i++) {
                if (!vec_array_is_valid(col, i)) continue;
                if (col->buf.i64[i] < mn) mn = col->buf.i64[i];
                if (col->buf.i64[i] > mx) mx = col->buf.i64[i];
                found = 1;
            }
            write_u8(fp, found ? (uint8_t)1 : (uint8_t)0);
            write_u64(fp, found ? (uint64_t)mn : 0);
            write_u64(fp, found ? (uint64_t)mx : 0);
        } else if (col->type == VEC_DOUBLE) {
            double mn = HUGE_VAL, mx = -HUGE_VAL;
            int found = 0;
            #pragma omp parallel for if(batch->n_rows > VEC_OMP_THRESHOLD) schedule(static) reduction(min:mn) reduction(max:mx) reduction(|:found)
            for (int64_t i = 0; i < batch->n_rows; i++) {
                if (!vec_array_is_valid(col, i)) continue;
                if (col->buf.dbl[i] < mn) mn = col->buf.dbl[i];
                if (col->buf.dbl[i] > mx) mx = col->buf.dbl[i];
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

    /* Column data (v4: encoded + compressed) */
    for (int c = 0; c < batch->n_cols; c++) {
        const VecArray *col = &batch->columns[c];
        int64_t vbytes = vec_validity_bytes(batch->n_rows);

        /* Validity bitmap — always written plain */
        fwrite(col->validity, 1, (size_t)vbytes, fp);

        /* Encode + compress the column data */
        VtrEncodedCol enc = vtr_encode_column(col, batch->n_rows);

        /* Column chunk header: encoding(1) + compression(1) + data_size(4) + uncompressed_size(4) */
        write_u8(fp, enc.encoding);
        write_u8(fp, enc.compression);
        write_u32(fp, enc.data_size);
        write_u32(fp, enc.uncompressed_size);

        /* Encoded data */
        if (enc.data_size > 0)
            fwrite(enc.data, 1, (size_t)enc.data_size, fp);

        free(enc.data);
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
    if (file->header.version < 1 || file->header.version > 4) {
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
                    if (t == VEC_INT64 || t == VEC_STRING) {
                        /* For strings: packed big-endian prefix stored as int64 */
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
            int64_t vbytes = vec_validity_bytes((int64_t)n_rows);
            /* Skip validity bitmap */
            fseek(fp, (long)vbytes, SEEK_CUR);

            if (file->header.version >= 4) {
                /* v4: read chunk header to get encoded data size, then skip */
                /* encoding(1) + compression(1) + data_size(4) + uncompressed_size(4) */
                read_u8(fp);  /* encoding */
                read_u8(fp);  /* compression */
                uint32_t data_size = read_u32(fp);
                read_u32(fp); /* uncompressed_size */
                fseek(fp, (long)data_size, SEEK_CUR);
            } else {
                /* v1-v3: raw data, size depends on type */
                VecType t = file->header.schema.col_types[c];
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
                    fseek(fp, (long)((n_rows + 1) * 8), SEEK_CUR);
                    uint64_t data_len = read_u64(fp);
                    fseek(fp, (long)data_len, SEEK_CUR);
                    break;
                }
                }
            }
        }
    }

    /* Detect sorted columns: check if row group stats are monotonically
       ordered (max[i] <= min[i+1]) for each column with stats. */
    file->col_sorted = NULL;
    if (file->header.version >= 3 && file->header.n_rowgroups > 1) {
        file->col_sorted = (uint8_t *)calloc((size_t)n_schema_cols, 1);
        if (file->col_sorted) {
            for (int c = 0; c < n_schema_cols; c++) {
                VecType t = file->header.schema.col_types[c];
                int sorted = 1;
                for (uint32_t rg = 0; rg + 1 < file->header.n_rowgroups; rg++) {
                    Vtr1ColStat *sa = file->rowgroups[rg].col_stats;
                    Vtr1ColStat *sb = file->rowgroups[rg + 1].col_stats;
                    if (!sa || !sb || !sa[c].has_stats || !sb[c].has_stats) {
                        sorted = 0;
                        break;
                    }
                    if (t == VEC_INT64 || t == VEC_STRING) {
                        /* For strings: packed prefix stored as int64, compare as uint64 */
                        if ((uint64_t)sa[c].i64.max > (uint64_t)sb[c].i64.min) {
                            sorted = 0;
                            break;
                        }
                    } else if (t == VEC_DOUBLE) {
                        if (sa[c].dbl.max > sb[c].dbl.min) {
                            sorted = 0;
                            break;
                        }
                    } else {
                        sorted = 0;
                        break;
                    }
                }
                file->col_sorted[c] = (uint8_t)sorted;
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

    int is_v4 = (file->header.version >= 4);

    /* Scratch buffer helpers — grow but never shrink within a file handle */
    Vtr1Scratch *se = &file->scratch_enc;
    Vtr1Scratch *sd = &file->scratch_dec;

    int out_col = 0;
    for (int c = 0; c < schema->n_cols; c++) {
        VecType t = schema->col_types[c];
        int64_t vbytes = vec_validity_bytes(n_rows);

        if (col_mask[c]) {
            if (is_v4) {
                /* v4: read validity, then decode encoded chunk */
                VecArray arr;
                memset(&arr, 0, sizeof(arr));
                arr.type = t;
                arr.length = n_rows;
                arr.owns_data = 1;
                arr.validity = (uint8_t *)calloc((size_t)(vbytes > 0 ? vbytes : 1), 1);
                if (!arr.validity) vectra_error("alloc failed");
                if (vbytes > 0 && fread(arr.validity, 1, (size_t)vbytes, file->fp) != (size_t)vbytes)
                    vectra_error("unexpected end of file reading validity bitmap");

                /* Read chunk header in one fread (10 bytes) */
                uint8_t hdr[10];
                if (fread(hdr, 1, 10, file->fp) != 10)
                    vectra_error("unexpected end of file reading chunk header");
                uint8_t encoding = hdr[0];
                uint8_t compression = hdr[1];
                uint32_t data_size, uncompressed_size;
                memcpy(&data_size, hdr + 2, 4);
                memcpy(&uncompressed_size, hdr + 6, 4);

                /* Decode column data */
                if (data_size > 0) {
                    int is_fixed = (t == VEC_INT64 || t == VEC_DOUBLE || t == VEC_BOOL);
                    size_t elem_size = (t == VEC_BOOL) ? 1 : 8;

                    if (encoding == VTR_ENC_PLAIN && compression == VTR_COMP_NONE && is_fixed) {
                        /* Direct fread: PLAIN+NONE fixed-width — read straight
                           into final buffer, zero intermediate copies. */
                        uint8_t *dst = (uint8_t *)malloc((size_t)n_rows * elem_size);
                        if (!dst) vectra_error("alloc failed");
                        if (fread(dst, 1, (size_t)data_size, file->fp) != (size_t)data_size)
                            vectra_error("unexpected end of file reading column data");
                        if (t == VEC_INT64) arr.buf.i64 = (int64_t *)dst;
                        else if (t == VEC_DOUBLE) arr.buf.dbl = (double *)dst;
                        else arr.buf.bln = dst;

                    } else if (encoding == VTR_ENC_PLAIN && compression == VTR_COMP_LZ_VTR && is_fixed) {
                        /* Fused decompress: PLAIN+LZ fixed-width — read compressed
                           into scratch, decompress directly into final buffer. */
                        if ((size_t)data_size > se->capacity) {
                            free(se->data);
                            se->capacity = (size_t)data_size;
                            se->data = (uint8_t *)malloc(se->capacity);
                            if (!se->data) vectra_error("alloc failed");
                        }
                        if (fread(se->data, 1, (size_t)data_size, file->fp) != (size_t)data_size)
                            vectra_error("unexpected end of file reading encoded column data");
                        uint8_t *dst = (uint8_t *)malloc((size_t)n_rows * elem_size);
                        if (!dst) vectra_error("alloc failed");
                        vtr_lz_decompress_into(dst, uncompressed_size,
                                               se->data, data_size);
                        if (t == VEC_INT64) arr.buf.i64 = (int64_t *)dst;
                        else if (t == VEC_DOUBLE) arr.buf.dbl = (double *)dst;
                        else arr.buf.bln = dst;

                    } else {
                        /* General path: read into scratch, decompress if needed, decode */
                        if ((size_t)data_size > se->capacity) {
                            free(se->data);
                            se->capacity = (size_t)data_size;
                            se->data = (uint8_t *)malloc(se->capacity);
                            if (!se->data) vectra_error("alloc failed");
                        }
                        if (fread(se->data, 1, (size_t)data_size, file->fp) != (size_t)data_size)
                            vectra_error("unexpected end of file reading encoded column data");

                        if (compression == VTR_COMP_LZ_VTR) {
                            if ((size_t)uncompressed_size > sd->capacity) {
                                free(sd->data);
                                sd->capacity = (size_t)uncompressed_size;
                                sd->data = (uint8_t *)malloc(sd->capacity);
                                if (!sd->data) vectra_error("alloc failed");
                            }
                            vtr_lz_decompress_into(sd->data, uncompressed_size,
                                                   se->data, data_size);
                            vtr_decode_column_raw(&arr, n_rows, encoding,
                                                 sd->data, uncompressed_size);
                        } else {
                            vtr_decode_column_raw(&arr, n_rows, encoding,
                                                 se->data, data_size);
                        }
                    }
                }

                batch->columns[out_col] = arr;
            } else {
                /* v1-v3: read raw column data */
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
                    free(arr.buf.str.data);
                    arr.buf.str.data = (char *)malloc((size_t)(data_len > 0 ? data_len : 1));
                    if (!arr.buf.str.data)
                        vectra_error("alloc failed for string data");
                    if (data_len > 0 && fread(arr.buf.str.data, 1, (size_t)data_len, file->fp) != (size_t)data_len)
                        vectra_error("unexpected end of file reading string data");
                    break;
                }
                }
                batch->columns[out_col] = arr;
            }

            batch->col_names[out_col] = (char *)malloc(
                strlen(schema->col_names[c]) + 1);
            strcpy(batch->col_names[out_col], schema->col_names[c]);
            out_col++;
        } else {
            /* Skip this column */
            fseek(file->fp, (long)vbytes, SEEK_CUR);
            if (is_v4) {
                /* Skip chunk header (10 bytes) + data */
                uint8_t shdr[10];
                if (fread(shdr, 1, 10, file->fp) != 10)
                    vectra_error("unexpected end of file skipping chunk header");
                uint32_t skip_size;
                memcpy(&skip_size, shdr + 2, 4);
                fseek(file->fp, (long)skip_size, SEEK_CUR);
            } else {
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
    }

    return batch;
}

/* Read a single row group using a caller-provided FILE* and scratch buffers.
   This is the thread-safe core used by both sequential and parallel readers. */
static VecBatch *read_rg_with_fp(Vtr1File *file, uint32_t rg_idx,
                                  const int *col_mask, FILE *fp,
                                  Vtr1Scratch *se, Vtr1Scratch *sd) {
    const VecSchema *schema = &file->header.schema;
    int64_t n_rows = file->rowgroups[rg_idx].n_rows;

    int n_selected = 0;
    for (int i = 0; i < schema->n_cols; i++)
        if (col_mask[i]) n_selected++;

    VecBatch *batch = vec_batch_alloc(n_selected, n_rows);

    long data_offset = (long)file->rowgroups[rg_idx].file_offset + 8;
    if (file->header.version >= 3)
        data_offset += (long)schema->n_cols * 17;
    fseek(fp, data_offset, SEEK_SET);

    int is_v4 = (file->header.version >= 4);
    int out_col = 0;

    for (int c = 0; c < schema->n_cols; c++) {
        VecType t = schema->col_types[c];
        int64_t vbytes = vec_validity_bytes(n_rows);

        if (col_mask[c]) {
            if (is_v4) {
                VecArray arr;
                memset(&arr, 0, sizeof(arr));
                arr.type = t;
                arr.length = n_rows;
                arr.owns_data = 1;
                arr.validity = (uint8_t *)calloc((size_t)(vbytes > 0 ? vbytes : 1), 1);
                if (!arr.validity) vectra_error("alloc failed");
                if (vbytes > 0 && fread(arr.validity, 1, (size_t)vbytes, fp) != (size_t)vbytes)
                    vectra_error("unexpected end of file reading validity bitmap");

                uint8_t hdr[10];
                if (fread(hdr, 1, 10, fp) != 10)
                    vectra_error("unexpected end of file reading chunk header");
                uint8_t encoding = hdr[0];
                uint8_t compression = hdr[1];
                uint32_t data_size, uncompressed_size;
                memcpy(&data_size, hdr + 2, 4);
                memcpy(&uncompressed_size, hdr + 6, 4);

                if (data_size > 0) {
                    int is_fixed = (t == VEC_INT64 || t == VEC_DOUBLE || t == VEC_BOOL);
                    size_t elem_size = (t == VEC_BOOL) ? 1 : 8;

                    if (encoding == VTR_ENC_PLAIN && compression == VTR_COMP_NONE && is_fixed) {
                        uint8_t *dst = (uint8_t *)malloc((size_t)n_rows * elem_size);
                        if (!dst) vectra_error("alloc failed");
                        if (fread(dst, 1, (size_t)data_size, fp) != (size_t)data_size)
                            vectra_error("unexpected end of file reading column data");
                        if (t == VEC_INT64) arr.buf.i64 = (int64_t *)dst;
                        else if (t == VEC_DOUBLE) arr.buf.dbl = (double *)dst;
                        else arr.buf.bln = dst;
                    } else if (encoding == VTR_ENC_PLAIN && compression == VTR_COMP_LZ_VTR && is_fixed) {
                        if ((size_t)data_size > se->capacity) {
                            free(se->data);
                            se->capacity = (size_t)data_size;
                            se->data = (uint8_t *)malloc(se->capacity);
                            if (!se->data) vectra_error("alloc failed");
                        }
                        if (fread(se->data, 1, (size_t)data_size, fp) != (size_t)data_size)
                            vectra_error("unexpected end of file reading encoded column data");
                        uint8_t *dst = (uint8_t *)malloc((size_t)n_rows * elem_size);
                        if (!dst) vectra_error("alloc failed");
                        vtr_lz_decompress_into(dst, uncompressed_size, se->data, data_size);
                        if (t == VEC_INT64) arr.buf.i64 = (int64_t *)dst;
                        else if (t == VEC_DOUBLE) arr.buf.dbl = (double *)dst;
                        else arr.buf.bln = dst;
                    } else {
                        if ((size_t)data_size > se->capacity) {
                            free(se->data);
                            se->capacity = (size_t)data_size;
                            se->data = (uint8_t *)malloc(se->capacity);
                            if (!se->data) vectra_error("alloc failed");
                        }
                        if (fread(se->data, 1, (size_t)data_size, fp) != (size_t)data_size)
                            vectra_error("unexpected end of file reading encoded column data");
                        if (compression == VTR_COMP_LZ_VTR) {
                            if ((size_t)uncompressed_size > sd->capacity) {
                                free(sd->data);
                                sd->capacity = (size_t)uncompressed_size;
                                sd->data = (uint8_t *)malloc(sd->capacity);
                                if (!sd->data) vectra_error("alloc failed");
                            }
                            vtr_lz_decompress_into(sd->data, uncompressed_size,
                                                   se->data, data_size);
                            vtr_decode_column_raw(&arr, n_rows, encoding,
                                                 sd->data, uncompressed_size);
                        } else {
                            vtr_decode_column_raw(&arr, n_rows, encoding,
                                                 se->data, data_size);
                        }
                    }
                }
                batch->columns[out_col] = arr;
            } else {
                /* v1-v3: read raw column data */
                VecArray arr = vec_array_alloc(t, n_rows);
                if (fread(arr.validity, 1, (size_t)vbytes, fp) != (size_t)vbytes)
                    vectra_error("unexpected end of file reading validity bitmap");
                switch (t) {
                case VEC_INT64:
                    if (fread(arr.buf.i64, sizeof(int64_t), (size_t)n_rows, fp) != (size_t)n_rows)
                        vectra_error("unexpected end of file reading int64 data");
                    break;
                case VEC_DOUBLE:
                    if (fread(arr.buf.dbl, sizeof(double), (size_t)n_rows, fp) != (size_t)n_rows)
                        vectra_error("unexpected end of file reading double data");
                    break;
                case VEC_BOOL:
                    if (fread(arr.buf.bln, 1, (size_t)n_rows, fp) != (size_t)n_rows)
                        vectra_error("unexpected end of file reading bool data");
                    break;
                case VEC_STRING: {
                    if (fread(arr.buf.str.offsets, sizeof(int64_t),
                          (size_t)(n_rows + 1), fp) != (size_t)(n_rows + 1))
                        vectra_error("unexpected end of file reading string offsets");
                    uint64_t data_len = 0;
                    if (fread(&data_len, 8, 1, fp) != 1)
                        vectra_error("unexpected end of file");
                    arr.buf.str.data_len = (int64_t)data_len;
                    free(arr.buf.str.data);
                    arr.buf.str.data = (char *)malloc((size_t)(data_len > 0 ? data_len : 1));
                    if (!arr.buf.str.data) vectra_error("alloc failed");
                    if (data_len > 0 && fread(arr.buf.str.data, 1, (size_t)data_len, fp) != (size_t)data_len)
                        vectra_error("unexpected end of file reading string data");
                    break;
                }
                }
                batch->columns[out_col] = arr;
            }
            batch->col_names[out_col] = (char *)malloc(strlen(schema->col_names[c]) + 1);
            strcpy(batch->col_names[out_col], schema->col_names[c]);
            out_col++;
        } else {
            fseek(fp, (long)vbytes, SEEK_CUR);
            if (is_v4) {
                uint8_t shdr[10];
                if (fread(shdr, 1, 10, fp) != 10)
                    vectra_error("unexpected end of file skipping chunk header");
                uint32_t skip_size;
                memcpy(&skip_size, shdr + 2, 4);
                fseek(fp, (long)skip_size, SEEK_CUR);
            } else {
                switch (t) {
                case VEC_INT64:  fseek(fp, (long)(n_rows * 8), SEEK_CUR); break;
                case VEC_DOUBLE: fseek(fp, (long)(n_rows * 8), SEEK_CUR); break;
                case VEC_BOOL:   fseek(fp, (long)n_rows, SEEK_CUR); break;
                case VEC_STRING: {
                    fseek(fp, (long)((n_rows + 1) * 8), SEEK_CUR);
                    uint64_t data_len = 0;
                    if (fread(&data_len, 8, 1, fp) != 1)
                        vectra_error("unexpected end of file");
                    fseek(fp, (long)data_len, SEEK_CUR);
                    break;
                }
                }
            }
        }
    }
    return batch;
}

VecBatch **vtr1_read_parallel(Vtr1File *file, const int *col_mask,
                              const char *path, uint32_t *out_count) {
    uint32_t n_rgs = file->header.n_rowgroups;
    *out_count = n_rgs;

    VecBatch **batches = (VecBatch **)calloc(n_rgs, sizeof(VecBatch *));
    if (!batches) vectra_error("alloc failed for parallel read");

    #pragma omp parallel
    {
        /* Thread-local file handle and scratch buffers */
        FILE *fp = fopen(path, "rb");
        if (!fp) vectra_error("parallel read: cannot open file: %s", path);
        Vtr1Scratch se = {0}, sd = {0};

        #pragma omp for schedule(dynamic)
        for (uint32_t rg = 0; rg < n_rgs; rg++) {
            batches[rg] = read_rg_with_fp(file, rg, col_mask, fp, &se, &sd);
        }

        fclose(fp);
        free(se.data);
        free(sd.data);
    }

    return batches;
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
    free(file->col_sorted);
    free(file->scratch_enc.data);
    free(file->scratch_dec.data);
    free(file);
}
