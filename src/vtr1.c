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

    /* Write version 5 if narrow int types present, else version 4 */
    int need_v5 = 0;
    for (int i = 0; i < schema->n_cols; i++) {
        if (schema->col_types[i] == VEC_INT8 ||
            schema->col_types[i] == VEC_INT16 ||
            schema->col_types[i] == VEC_INT32) {
            need_v5 = 1;
            break;
        }
    }
    write_u16(fp, need_v5 ? (uint16_t)5 : (uint16_t)4);
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

void vtr1_write_rowgroup_qs(FILE *fp, const VecBatch *batch, int comp_level,
                            const VtrQuantizeSpec *qspecs,
                            const VtrSpatialSpec *sspecs) {
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
            write_u64(fp, found ? mn : 0);
            write_u64(fp, found ? mx : 0);
        } else if (vec_type_is_int(col->type)) {
            int64_t mn = INT64_MAX, mx = INT64_MIN;
            int found = 0;
            #pragma omp parallel for if(batch->n_rows > VEC_OMP_THRESHOLD) schedule(static) reduction(min:mn) reduction(max:mx) reduction(|:found)
            for (int64_t i = 0; i < batch->n_rows; i++) {
                if (!vec_array_is_valid(col, i)) continue;
                int64_t v = vec_array_get_int(col, i);
                if (v < mn) mn = v;
                if (v > mx) mx = v;
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
            write_u64(fp, (uint64_t)has_false);
            write_u64(fp, (uint64_t)has_true);
        }
    }

    /* Column data (v4: encoded + compressed)
     * Phase 1: Encode all columns in parallel (CPU-bound)
     * Phase 2: Write encoded data sequentially (I/O-bound) */
    int n_cols = batch->n_cols;
    VtrEncodedCol *encodings = (VtrEncodedCol *)malloc((size_t)n_cols * sizeof(VtrEncodedCol));
    if (!encodings) vectra_error("alloc failed for encodings");
    const VtrQuantizeSpec **qs_ptrs = (const VtrQuantizeSpec **)malloc((size_t)n_cols * sizeof(void *));
    if (!qs_ptrs) { free(encodings); vectra_error("alloc failed"); }

    for (int c = 0; c < n_cols; c++)
        qs_ptrs[c] = (qspecs && qspecs[c].enabled) ? &qspecs[c] : NULL;

    #pragma omp parallel for if(n_cols > 1 && comp_level > 0) schedule(dynamic)
    for (int c = 0; c < n_cols; c++) {
        const VtrSpatialSpec *ss = (sspecs && sspecs[c].enabled) ? &sspecs[c] : NULL;
        encodings[c] = vtr_encode_column_qs(&batch->columns[c], batch->n_rows,
                                             comp_level, qs_ptrs[c], ss);
    }

    /* Phase 2: Sequential write */
    for (int c = 0; c < n_cols; c++) {
        const VecArray *col = &batch->columns[c];
        int64_t vbytes = vec_validity_bytes(batch->n_rows);
        VtrEncodedCol *enc = &encodings[c];

        /* Validity bitmap — always written plain */
        fwrite(col->validity, 1, (size_t)vbytes, fp);

        /* Column chunk header: encoding(1) + compression(1) + data_size(4) + uncompressed_size(4) */
        write_u8(fp, enc->encoding);
        write_u8(fp, enc->compression);
        write_u32(fp, enc->data_size);
        write_u32(fp, enc->uncompressed_size);

        /* Quantize metadata: scale(8) + offset(8) + target_type(1) = 17 bytes */
        if (enc->encoding == VTR_ENC_QUANTIZE) {
            fwrite(&enc->quantize_scale, 8, 1, fp);
            fwrite(&enc->quantize_offset, 8, 1, fp);
            write_u8(fp, enc->quantize_target_type);
        }

        /* Spatial metadata */
        if (enc->encoding == VTR_ENC_SPATIAL) {
            write_u8(fp, enc->spatial_predictor);
            write_u32(fp, enc->spatial_nx);
            write_u32(fp, enc->spatial_ny);

            int has_q = (qs_ptrs[c] != NULL) ? 1 : 0;
            write_u8(fp, (uint8_t)has_q);
            if (has_q) {
                fwrite(&enc->quantize_scale, 8, 1, fp);
                fwrite(&enc->quantize_offset, 8, 1, fp);
                write_u8(fp, enc->quantize_target_type);
            }

            write_u16(fp, enc->spatial_tile_size);
            write_u32(fp, enc->spatial_n_tiles);
            if (enc->spatial_n_tiles > 0 && enc->spatial_coeffs) {
                fwrite(enc->spatial_coeffs, sizeof(int32_t),
                       (size_t)enc->spatial_n_tiles * 3, fp);
            }
            free(enc->spatial_coeffs);
        }

        /* Encoded data */
        if (enc->data_size > 0)
            fwrite(enc->data, 1, (size_t)enc->data_size, fp);

        free(enc->data);
    }
    free(encodings);
    free(qs_ptrs);
}

void vtr1_write_rowgroup_q(FILE *fp, const VecBatch *batch, int comp_level,
                           const VtrQuantizeSpec *qspecs) {
    vtr1_write_rowgroup_qs(fp, batch, comp_level, qspecs, NULL);
}

void vtr1_write_rowgroup(FILE *fp, const VecBatch *batch, int comp_level) {
    vtr1_write_rowgroup_qs(fp, batch, comp_level, NULL, NULL);
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
    vtr1_write_rowgroup(fp, batch, VTR_COMPRESS_FAST);

    free(schema.col_types);
    fclose(fp);
}

/* --- Read --- */

Vtr1File *vtr1_open(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) vectra_error("cannot open file: %s", path);
    setvbuf(fp, NULL, _IOFBF, 256 * 1024); /* 256KB read buffer */

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
    if (file->header.version < 1 || file->header.version > 5) {
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
                    if (vec_type_is_int(t) || t == VEC_STRING) {
                        /* Ints + strings: stored as int64/packed prefix */
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
                uint8_t enc_tag = read_u8(fp);  /* encoding */
                read_u8(fp);  /* compression */
                uint32_t data_size = read_u32(fp);
                read_u32(fp); /* uncompressed_size */
                /* Quantize encoding has 17 extra bytes of metadata */
                if (enc_tag == VTR_ENC_QUANTIZE)
                    fseek(fp, 17, SEEK_CUR);
                /* Spatial encoding has variable-size metadata */
                if (enc_tag == VTR_ENC_SPATIAL) {
                    read_u8(fp);  /* predictor */
                    read_u32(fp); /* nx */
                    read_u32(fp); /* ny */
                    uint8_t has_q = read_u8(fp);
                    if (has_q) fseek(fp, 17, SEEK_CUR); /* scale+offset+type */
                    read_u16(fp); /* tile_size */
                    uint32_t nt = read_u32(fp); /* n_tiles */
                    if (nt > 0) fseek(fp, (long)(nt * 3 * 4), SEEK_CUR); /* coeffs */
                }
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
                default: break; /* narrow int types impossible in v1-v3 */
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
                    if (vec_type_is_int(t) || t == VEC_STRING) {
                        /* Ints / strings: packed prefix stored as int64, compare as uint64 */
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
    return vtr1_read_rowgroup_ex(file, rg_idx, col_mask, NULL);
}

VecBatch *vtr1_read_rowgroup_ex(Vtr1File *file, uint32_t rg_idx,
                                const int *col_mask, void **direct_bufs) {
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

                /* Read quantize metadata if present */
                double q_scale = 0, q_offset = 0;
                uint8_t q_target = 0;
                if (encoding == VTR_ENC_QUANTIZE) {
                    if (fread(&q_scale, 8, 1, file->fp) != 1 ||
                        fread(&q_offset, 8, 1, file->fp) != 1)
                        vectra_error("unexpected end of file reading quantize metadata");
                    q_target = read_u8(file->fp);
                }

                /* Read spatial metadata if present */
                uint8_t sp_predictor = 0;
                uint32_t sp_nx = 0, sp_ny = 0, sp_n_tiles = 0;
                uint16_t sp_tile_size = 0;
                int sp_has_q = 0;
                double sp_q_scale = 0, sp_q_offset = 0;
                int32_t *sp_coeffs = NULL;
                if (encoding == VTR_ENC_SPATIAL) {
                    sp_predictor = read_u8(file->fp);
                    sp_nx = read_u32(file->fp);
                    sp_ny = read_u32(file->fp);
                    sp_has_q = read_u8(file->fp);
                    if (sp_has_q) {
                        if (fread(&sp_q_scale, 8, 1, file->fp) != 1 ||
                            fread(&sp_q_offset, 8, 1, file->fp) != 1)
                            vectra_error("unexpected end of file reading spatial quantize metadata");
                        (void)read_u8(file->fp); /* q_target type — reserved for future use */
                    }
                    sp_tile_size = read_u16(file->fp);
                    sp_n_tiles = read_u32(file->fp);
                    if (sp_n_tiles > 0) {
                        sp_coeffs = (int32_t *)malloc((size_t)sp_n_tiles * 3 * sizeof(int32_t));
                        if (!sp_coeffs) vectra_error("alloc failed for spatial coefficients");
                        if (fread(sp_coeffs, sizeof(int32_t), (size_t)sp_n_tiles * 3, file->fp) != (size_t)sp_n_tiles * 3)
                            vectra_error("unexpected end of file reading spatial coefficients");
                    }
                }

                /* Decode column data */
                if (data_size > 0) {
                    int is_fixed = vec_type_is_fixed(t);
                    size_t elem_size = (size_t)vec_type_elem_size(t);

                    if (encoding == VTR_ENC_SPATIAL) {
                        /* Spatial path: decompress residuals (int64), apply inverse predictor,
                           then optionally dequantize to float64 */
                        uint8_t *raw_data = NULL;
                        if (compression == VTR_COMP_NONE) {
                            raw_data = (uint8_t *)malloc((size_t)data_size);
                            if (!raw_data) vectra_error("alloc failed");
                            if (fread(raw_data, 1, (size_t)data_size, file->fp) != (size_t)data_size)
                                vectra_error("unexpected end of file reading spatial data");
                        } else {
                            if ((size_t)data_size > se->capacity) {
                                free(se->data);
                                se->capacity = (size_t)data_size;
                                se->data = (uint8_t *)malloc(se->capacity);
                                if (!se->data) vectra_error("alloc failed");
                            }
                            if (fread(se->data, 1, (size_t)data_size, file->fp) != (size_t)data_size)
                                vectra_error("unexpected end of file reading spatial data");
                            raw_data = (uint8_t *)malloc((size_t)uncompressed_size);
                            if (!raw_data) vectra_error("alloc failed");
                            if (compression == VTR_COMP_SHUFFLE_LZ2) {
                                vtr_lz2_decompress_into(raw_data, uncompressed_size, se->data, data_size);
                                vtr_byte_unshuffle(raw_data, uncompressed_size / 8, 8); /* int64 = 8 bytes */
                            } else {
                                free(raw_data); free(sp_coeffs);
                                vectra_error("unknown compression tag: 0x%02x", compression);
                            }
                        }

                        /* Residuals are stored as PLAIN int64 (no inner encoding) */
                        int64_t *res_i64 = (int64_t *)malloc((size_t)n_rows * sizeof(int64_t));
                        if (!res_i64) { free(raw_data); free(sp_coeffs); vectra_error("alloc failed"); }
                        memcpy(res_i64, raw_data, (size_t)n_rows * sizeof(int64_t));
                        free(raw_data);

                        /* Apply inverse spatial predictor */
                        int64_t *values = (int64_t *)malloc((size_t)n_rows * sizeof(int64_t));
                        if (!values) { free(res_i64); free(sp_coeffs); vectra_error("alloc failed"); }
                        vtr_spatial_decode(values, res_i64, n_rows,
                                           sp_nx, sp_ny, sp_predictor,
                                           sp_tile_size, sp_coeffs);
                        free(res_i64);
                        free(sp_coeffs);

                        if (sp_has_q) {
                            /* Dequantize int64 → float64 */
                            arr.type = VEC_DOUBLE;
                            arr.buf.dbl = (double *)malloc((size_t)n_rows * sizeof(double));
                            if (!arr.buf.dbl) { free(values); vectra_error("alloc failed"); }
                            vtr_spatial_dequantize(arr.buf.dbl, values, n_rows,
                                                   arr.validity, sp_q_scale, sp_q_offset);
                            free(values);
                        } else {
                            /* Return as int64 */
                            arr.buf.i64 = values;
                        }

                    } else if (encoding == VTR_ENC_QUANTIZE) {
                        /* Quantize path: decompress narrow int, dequantize to float64 */
                        uint8_t q_es = vec_type_elem_size((VecType)q_target);
                        uint8_t *int_buf = NULL;

                        if (compression == VTR_COMP_NONE) {
                            int_buf = (uint8_t *)malloc((size_t)data_size);
                            if (!int_buf) vectra_error("alloc failed");
                            if (fread(int_buf, 1, (size_t)data_size, file->fp) != (size_t)data_size)
                                vectra_error("unexpected end of file reading quantized data");
                        } else {
                            /* Read compressed into scratch_enc, decompress */
                            if ((size_t)data_size > se->capacity) {
                                free(se->data);
                                se->capacity = (size_t)data_size;
                                se->data = (uint8_t *)malloc(se->capacity);
                                if (!se->data) vectra_error("alloc failed");
                            }
                            if (fread(se->data, 1, (size_t)data_size, file->fp) != (size_t)data_size)
                                vectra_error("unexpected end of file reading quantized data");
                            int_buf = (uint8_t *)malloc((size_t)uncompressed_size);
                            if (!int_buf) vectra_error("alloc failed");
                            if (compression == VTR_COMP_SHUFFLE_LZ2) {
                                vtr_lz2_decompress_into(int_buf, uncompressed_size,
                                                        se->data, data_size);
                                if (q_es > 1)
                                    vtr_byte_unshuffle(int_buf, uncompressed_size / q_es, q_es);
                            } else {
                                free(int_buf);
                                vectra_error("unknown compression tag: 0x%02x", compression);
                            }
                        }

                        /* Dequantize narrow int → float64. The output is always
                           VEC_DOUBLE, so honor a caller-supplied direct buffer
                           by writing dequantized values straight into it. */
                        arr.type = VEC_DOUBLE;
                        int q_borrowed = (direct_bufs && direct_bufs[out_col]);
                        if (q_borrowed) {
                            arr.buf.dbl = (double *)direct_bufs[out_col];
                            arr.owns_data    = 0;
                            arr.data_borrowed = 1;
                        } else {
                            arr.buf.dbl = (double *)malloc((size_t)n_rows * sizeof(double));
                            if (!arr.buf.dbl) { free(int_buf); vectra_error("alloc failed"); }
                        }
                        vtr_dequantize(arr.buf.dbl, int_buf, n_rows, arr.validity,
                                       q_scale, q_offset, (VecType)q_target);
                        free(int_buf);

                    } else if (encoding == VTR_ENC_PLAIN && compression == VTR_COMP_NONE && is_fixed) {
                        /* Direct fread: PLAIN+NONE fixed-width — read straight
                           into final buffer, zero intermediate copies.
                           If caller provided a direct_buf, use it (zero-copy). */
                        uint8_t *dst;
                        int borrowed = (direct_bufs && direct_bufs[out_col]);
                        if (borrowed) {
                            dst = (uint8_t *)direct_bufs[out_col];
                        } else {
                            dst = (uint8_t *)malloc((size_t)n_rows * elem_size);
                            if (!dst) vectra_error("alloc failed");
                        }
                        if (fread(dst, 1, (size_t)data_size, file->fp) != (size_t)data_size)
                            vectra_error("unexpected end of file reading column data");
                        if (borrowed) {
                            arr.owns_data    = 0;
                            arr.data_borrowed = 1;
                        }
                        switch (t) {
                        case VEC_INT64:  arr.buf.i64 = (int64_t *)dst; break;
                        case VEC_INT32:  arr.buf.i32 = (int32_t *)dst; break;
                        case VEC_INT16:  arr.buf.i16 = (int16_t *)dst; break;
                        case VEC_INT8:   arr.buf.i8  = (int8_t *)dst;  break;
                        case VEC_DOUBLE: arr.buf.dbl = (double *)dst;  break;
                        default:         arr.buf.bln = dst;            break;
                        }

                    } else if (encoding == VTR_ENC_PLAIN && is_fixed &&
                               compression == VTR_COMP_SHUFFLE_LZ2) {
                        /* Fused path: PLAIN+SHUFFLE — decompress into scratch_dec,
                           then unshuffle directly into final buffer (no temp alloc). */
                        if ((size_t)data_size > se->capacity) {
                            free(se->data);
                            se->capacity = (size_t)data_size;
                            se->data = (uint8_t *)malloc(se->capacity);
                            if (!se->data) vectra_error("alloc failed");
                        }
                        if (fread(se->data, 1, (size_t)data_size, file->fp) != (size_t)data_size)
                            vectra_error("unexpected end of file reading encoded column data");

                        if ((size_t)uncompressed_size > sd->capacity) {
                            free(sd->data);
                            sd->capacity = (size_t)uncompressed_size;
                            sd->data = (uint8_t *)malloc(sd->capacity);
                            if (!sd->data) vectra_error("alloc failed");
                        }
                        vtr_lz2_decompress_into(sd->data, uncompressed_size, se->data, data_size);
                        /* Unshuffle from scratch_dec directly into final buffer.
                           If a direct buffer was supplied by the caller, unshuffle
                           straight into it (zero-copy). */
                        uint8_t *dst;
                        int borrowed = (direct_bufs && direct_bufs[out_col]);
                        if (borrowed) {
                            dst = (uint8_t *)direct_bufs[out_col];
                        } else {
                            dst = (uint8_t *)malloc((size_t)n_rows * elem_size);
                            if (!dst) vectra_error("alloc failed");
                        }
                        uint8_t es_val = (uint8_t)elem_size;
                        vtr_byte_unshuffle_to(dst, sd->data, uncompressed_size / es_val, es_val);
                        if (borrowed) {
                            arr.owns_data    = 0;
                            arr.data_borrowed = 1;
                        }
                        switch (t) {
                        case VEC_INT64:  arr.buf.i64 = (int64_t *)dst; break;
                        case VEC_INT32:  arr.buf.i32 = (int32_t *)dst; break;
                        case VEC_INT16:  arr.buf.i16 = (int16_t *)dst; break;
                        case VEC_INT8:   arr.buf.i8  = (int8_t *)dst;  break;
                        case VEC_DOUBLE: arr.buf.dbl = (double *)dst;  break;
                        default:         arr.buf.bln = dst;            break;
                        }

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

                        const uint8_t *decoded_src;
                        uint32_t decoded_size;
                        if (compression != VTR_COMP_NONE) {
                            /* Decompress into scratch_dec */
                            if ((size_t)uncompressed_size > sd->capacity) {
                                free(sd->data);
                                sd->capacity = (size_t)uncompressed_size;
                                sd->data = (uint8_t *)malloc(sd->capacity);
                                if (!sd->data) vectra_error("alloc failed");
                            }
                            if (compression == VTR_COMP_SHUFFLE_LZ2) {
                                vtr_lz2_decompress_into(sd->data, uncompressed_size,
                                                        se->data, data_size);
                                uint8_t es = vtr_shuffle_elem_size(t, encoding);
                                if (es > 0) vtr_byte_unshuffle(sd->data, uncompressed_size / es, es);
                            } else {
                                vectra_error("unknown compression tag: 0x%02x", compression);
                            }
                            decoded_src = sd->data;
                            decoded_size = uncompressed_size;
                        } else {
                            decoded_src = se->data;
                            decoded_size = data_size;
                        }

                        /* If the caller asked for a direct buffer, try to decode
                           straight into it. The codec answers 1 if it handled
                           the (encoding, type) pair (PLAIN/DELTA/DIFF for the
                           supported numeric types), 0 if we have to fall back. */
                        int decoded_into_direct = 0;
                        if (direct_bufs && direct_bufs[out_col] && is_fixed) {
                            if (vtr_decode_column_raw_into(t, n_rows, encoding,
                                                            decoded_src, decoded_size,
                                                            direct_bufs[out_col])) {
                                uint8_t *dst = (uint8_t *)direct_bufs[out_col];
                                arr.owns_data    = 0;
                                arr.data_borrowed = 1;
                                switch (t) {
                                case VEC_INT64:  arr.buf.i64 = (int64_t *)dst; break;
                                case VEC_INT32:  arr.buf.i32 = (int32_t *)dst; break;
                                case VEC_INT16:  arr.buf.i16 = (int16_t *)dst; break;
                                case VEC_INT8:   arr.buf.i8  = (int8_t *)dst;  break;
                                case VEC_DOUBLE: arr.buf.dbl = (double *)dst;  break;
                                default:         arr.buf.bln = dst;            break;
                                }
                                decoded_into_direct = 1;
                            }
                        }
                        /* String-defer fast path: if the caller passed the
                         * VTR_STRING_DICT_DEFER sentinel for a DICTIONARY-
                         * encoded string column, parse the chunk into an
                         * owned blob instead of materializing the flat
                         * string buffer. collect.c will then build the
                         * STRSXP via a CHARSXP table + RLE walk. The
                         * VecArray stays valid (empty offsets/data, length
                         * and validity intact) so normal lifetime rules
                         * apply. */
                        if (!decoded_into_direct && t == VEC_STRING &&
                            encoding == VTR_ENC_DICTIONARY &&
                            direct_bufs && direct_bufs[out_col] == VTR_STRING_DICT_DEFER) {
                            VtrDictBlob *blob = vtr_dict_parse_to_blob(
                                decoded_src, decoded_size);
                            if (blob) {
                                arr.str_dict = blob;
                                /* Minimal placeholder str buffers so the
                                 * VecArray is well-formed. Matches what
                                 * vec_array_alloc(VEC_STRING, n_rows) would
                                 * produce for an empty column. */
                                arr.buf.str.offsets = (int64_t *)calloc(
                                    (size_t)(n_rows + 1), sizeof(int64_t));
                                if (!arr.buf.str.offsets)
                                    vectra_error("alloc failed for dict-defer offsets");
                                arr.buf.str.data = (char *)malloc(1);
                                arr.buf.str.data_len = 0;
                                decoded_into_direct = 1; /* reuse the flag */
                            }
                        }
                        if (!decoded_into_direct) {
                            vtr_decode_column_raw(&arr, n_rows, encoding,
                                                  decoded_src, decoded_size);
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
                default:
                    vectra_error("unexpected narrow int type in v1-v3 file");
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
                /* Skip chunk header (10 bytes) + quantize metadata (17 if present) + data */
                uint8_t shdr[10];
                if (fread(shdr, 1, 10, file->fp) != 10)
                    vectra_error("unexpected end of file skipping chunk header");
                uint32_t skip_size;
                memcpy(&skip_size, shdr + 2, 4);
                if (shdr[0] == VTR_ENC_QUANTIZE)
                    fseek(file->fp, 17, SEEK_CUR);
                if (shdr[0] == VTR_ENC_SPATIAL) {
                    read_u8(file->fp);  /* predictor */
                    read_u32(file->fp); /* nx */
                    read_u32(file->fp); /* ny */
                    uint8_t sq = read_u8(file->fp);
                    if (sq) fseek(file->fp, 17, SEEK_CUR);
                    read_u16(file->fp); /* tile_size */
                    uint32_t snt = read_u32(file->fp);
                    if (snt > 0) fseek(file->fp, (long)(snt * 3 * 4), SEEK_CUR);
                }
                fseek(file->fp, (long)skip_size, SEEK_CUR);
            } else {
                switch (t) {
                case VEC_INT64:  fseek(file->fp, (long)(n_rows * 8), SEEK_CUR); break;
                case VEC_DOUBLE: fseek(file->fp, (long)(n_rows * 8), SEEK_CUR); break;
                case VEC_BOOL:   fseek(file->fp, (long)n_rows, SEEK_CUR); break;
                default: break; /* narrow int types impossible in v1-v3 */
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
   This is the thread-safe core used by both sequential and parallel readers.
   If direct_bufs is non-NULL, columns whose direct_bufs[out_col] is non-NULL
   are decoded directly into the caller's buffer (zero-copy). The hot paths
   (PLAIN+NONE+fixed and PLAIN+SHUFFLE_LZ2+fixed) honor this; other paths
   ignore direct_bufs and allocate normally. */
static VecBatch *read_rg_with_fp(Vtr1File *file, uint32_t rg_idx,
                                  const int *col_mask, FILE *fp,
                                  Vtr1Scratch *se, Vtr1Scratch *sd,
                                  void **direct_bufs) {
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

                /* Read quantize metadata if present */
                double q_scale = 0, q_offset = 0;
                uint8_t q_target = 0;
                if (encoding == VTR_ENC_QUANTIZE) {
                    if (fread(&q_scale, 8, 1, fp) != 1 ||
                        fread(&q_offset, 8, 1, fp) != 1)
                        vectra_error("unexpected end of file reading quantize metadata");
                    q_target = read_u8(fp);
                }

                /* Read spatial metadata if present */
                uint8_t sp_predictor2 = 0;
                uint32_t sp_nx2 = 0, sp_ny2 = 0, sp_n_tiles2 = 0;
                uint16_t sp_tile_size2 = 0;
                int sp_has_q2 = 0;
                double sp_q_scale2 = 0, sp_q_offset2 = 0;
                int32_t *sp_coeffs2 = NULL;
                if (encoding == VTR_ENC_SPATIAL) {
                    sp_predictor2 = read_u8(fp);
                    sp_nx2 = read_u32(fp);
                    sp_ny2 = read_u32(fp);
                    sp_has_q2 = read_u8(fp);
                    if (sp_has_q2) {
                        if (fread(&sp_q_scale2, 8, 1, fp) != 1 ||
                            fread(&sp_q_offset2, 8, 1, fp) != 1)
                            vectra_error("unexpected end of file reading spatial quantize metadata");
                        (void)read_u8(fp); /* q_target type — reserved for future use */
                    }
                    sp_tile_size2 = read_u16(fp);
                    sp_n_tiles2 = read_u32(fp);
                    if (sp_n_tiles2 > 0) {
                        sp_coeffs2 = (int32_t *)malloc((size_t)sp_n_tiles2 * 3 * sizeof(int32_t));
                        if (!sp_coeffs2) vectra_error("alloc failed");
                        if (fread(sp_coeffs2, sizeof(int32_t), (size_t)sp_n_tiles2 * 3, fp) != (size_t)sp_n_tiles2 * 3)
                            vectra_error("unexpected end of file reading spatial coefficients");
                    }
                }

                if (data_size > 0) {
                    int is_fixed = vec_type_is_fixed(t);
                    size_t elem_size = (size_t)vec_type_elem_size(t);

                    if (encoding == VTR_ENC_SPATIAL) {
                        /* Spatial path (parallel reader) */
                        uint8_t *raw_data = NULL;
                        if (compression == VTR_COMP_NONE) {
                            raw_data = (uint8_t *)malloc((size_t)data_size);
                            if (!raw_data) vectra_error("alloc failed");
                            if (fread(raw_data, 1, (size_t)data_size, fp) != (size_t)data_size)
                                vectra_error("unexpected end of file reading spatial data");
                        } else {
                            if ((size_t)data_size > se->capacity) {
                                free(se->data);
                                se->capacity = (size_t)data_size;
                                se->data = (uint8_t *)malloc(se->capacity);
                                if (!se->data) vectra_error("alloc failed");
                            }
                            if (fread(se->data, 1, (size_t)data_size, fp) != (size_t)data_size)
                                vectra_error("unexpected end of file reading spatial data");
                            raw_data = (uint8_t *)malloc((size_t)uncompressed_size);
                            if (!raw_data) vectra_error("alloc failed");
                            if (compression == VTR_COMP_SHUFFLE_LZ2) {
                                vtr_lz2_decompress_into(raw_data, uncompressed_size, se->data, data_size);
                                vtr_byte_unshuffle(raw_data, uncompressed_size / 8, 8);
                            } else {
                                free(raw_data); free(sp_coeffs2);
                                vectra_error("unknown compression tag: 0x%02x", compression);
                            }
                        }
                        int64_t *values = (int64_t *)malloc((size_t)n_rows * sizeof(int64_t));
                        if (!values) { free(raw_data); free(sp_coeffs2); vectra_error("alloc failed"); }
                        vtr_spatial_decode(values, (const int64_t *)raw_data, n_rows,
                                           sp_nx2, sp_ny2, sp_predictor2,
                                           sp_tile_size2, sp_coeffs2);
                        free(raw_data);
                        free(sp_coeffs2);
                        if (sp_has_q2) {
                            arr.type = VEC_DOUBLE;
                            arr.buf.dbl = (double *)malloc((size_t)n_rows * sizeof(double));
                            if (!arr.buf.dbl) { free(values); vectra_error("alloc failed"); }
                            vtr_spatial_dequantize(arr.buf.dbl, values, n_rows,
                                                   arr.validity, sp_q_scale2, sp_q_offset2);
                            free(values);
                        } else {
                            arr.buf.i64 = values;
                        }

                    } else if (encoding == VTR_ENC_QUANTIZE) {
                        uint8_t q_es = vec_type_elem_size((VecType)q_target);
                        uint8_t *int_buf = NULL;

                        if (compression == VTR_COMP_NONE) {
                            int_buf = (uint8_t *)malloc((size_t)data_size);
                            if (!int_buf) vectra_error("alloc failed");
                            if (fread(int_buf, 1, (size_t)data_size, fp) != (size_t)data_size)
                                vectra_error("unexpected end of file reading quantized data");
                        } else {
                            if ((size_t)data_size > se->capacity) {
                                free(se->data);
                                se->capacity = (size_t)data_size;
                                se->data = (uint8_t *)malloc(se->capacity);
                                if (!se->data) vectra_error("alloc failed");
                            }
                            if (fread(se->data, 1, (size_t)data_size, fp) != (size_t)data_size)
                                vectra_error("unexpected end of file reading quantized data");
                            int_buf = (uint8_t *)malloc((size_t)uncompressed_size);
                            if (!int_buf) vectra_error("alloc failed");
                            if (compression == VTR_COMP_SHUFFLE_LZ2) {
                                vtr_lz2_decompress_into(int_buf, uncompressed_size,
                                                        se->data, data_size);
                                if (q_es > 1)
                                    vtr_byte_unshuffle(int_buf, uncompressed_size / q_es, q_es);
                            } else {
                                free(int_buf);
                                vectra_error("unknown compression tag: 0x%02x", compression);
                            }
                        }

                        arr.type = VEC_DOUBLE;
                        int q_borrowed = (direct_bufs && direct_bufs[out_col]);
                        if (q_borrowed) {
                            arr.buf.dbl = (double *)direct_bufs[out_col];
                            arr.owns_data    = 0;
                            arr.data_borrowed = 1;
                        } else {
                            arr.buf.dbl = (double *)malloc((size_t)n_rows * sizeof(double));
                            if (!arr.buf.dbl) { free(int_buf); vectra_error("alloc failed"); }
                        }
                        vtr_dequantize(arr.buf.dbl, int_buf, n_rows, arr.validity,
                                       q_scale, q_offset, (VecType)q_target);
                        free(int_buf);

                    } else if (encoding == VTR_ENC_PLAIN && compression == VTR_COMP_NONE && is_fixed) {
                        uint8_t *dst;
                        int borrowed = (direct_bufs && direct_bufs[out_col]);
                        if (borrowed) {
                            dst = (uint8_t *)direct_bufs[out_col];
                        } else {
                            dst = (uint8_t *)malloc((size_t)n_rows * elem_size);
                            if (!dst) vectra_error("alloc failed");
                        }
                        if (fread(dst, 1, (size_t)data_size, fp) != (size_t)data_size)
                            vectra_error("unexpected end of file reading column data");
                        if (borrowed) {
                            arr.owns_data    = 0;
                            arr.data_borrowed = 1;
                        }
                        switch (t) {
                        case VEC_INT64:  arr.buf.i64 = (int64_t *)dst; break;
                        case VEC_INT32:  arr.buf.i32 = (int32_t *)dst; break;
                        case VEC_INT16:  arr.buf.i16 = (int16_t *)dst; break;
                        case VEC_INT8:   arr.buf.i8  = (int8_t *)dst;  break;
                        case VEC_DOUBLE: arr.buf.dbl = (double *)dst;  break;
                        default:         arr.buf.bln = dst;            break;
                        }
                    } else if (encoding == VTR_ENC_PLAIN && is_fixed &&
                               compression == VTR_COMP_SHUFFLE_LZ2) {
                        /* Fused path: decompress into scratch, unshuffle into final.
                           If a direct buffer was provided, unshuffle straight into
                           it — the caller has already allocated the destination
                           (typically the R vector's REAL/INTEGER storage). */
                        if ((size_t)data_size > se->capacity) {
                            free(se->data);
                            se->capacity = (size_t)data_size;
                            se->data = (uint8_t *)malloc(se->capacity);
                            if (!se->data) vectra_error("alloc failed");
                        }
                        if (fread(se->data, 1, (size_t)data_size, fp) != (size_t)data_size)
                            vectra_error("unexpected end of file reading encoded column data");
                        if ((size_t)uncompressed_size > sd->capacity) {
                            free(sd->data);
                            sd->capacity = (size_t)uncompressed_size;
                            sd->data = (uint8_t *)malloc(sd->capacity);
                            if (!sd->data) vectra_error("alloc failed");
                        }
                        vtr_lz2_decompress_into(sd->data, uncompressed_size, se->data, data_size);
                        uint8_t *dst;
                        int borrowed = (direct_bufs && direct_bufs[out_col]);
                        if (borrowed) {
                            dst = (uint8_t *)direct_bufs[out_col];
                        } else {
                            dst = (uint8_t *)malloc((size_t)n_rows * elem_size);
                            if (!dst) vectra_error("alloc failed");
                        }
                        uint8_t es_val = (uint8_t)elem_size;
                        vtr_byte_unshuffle_to(dst, sd->data, uncompressed_size / es_val, es_val);
                        if (borrowed) {
                            arr.owns_data    = 0;
                            arr.data_borrowed = 1;
                        }
                        switch (t) {
                        case VEC_INT64:  arr.buf.i64 = (int64_t *)dst; break;
                        case VEC_INT32:  arr.buf.i32 = (int32_t *)dst; break;
                        case VEC_INT16:  arr.buf.i16 = (int16_t *)dst; break;
                        case VEC_INT8:   arr.buf.i8  = (int8_t *)dst;  break;
                        case VEC_DOUBLE: arr.buf.dbl = (double *)dst;  break;
                        default:         arr.buf.bln = dst;            break;
                        }
                    } else {
                        if ((size_t)data_size > se->capacity) {
                            free(se->data);
                            se->capacity = (size_t)data_size;
                            se->data = (uint8_t *)malloc(se->capacity);
                            if (!se->data) vectra_error("alloc failed");
                        }
                        if (fread(se->data, 1, (size_t)data_size, fp) != (size_t)data_size)
                            vectra_error("unexpected end of file reading encoded column data");
                        const uint8_t *decoded_src;
                        uint32_t decoded_size;
                        if (compression != VTR_COMP_NONE) {
                            if ((size_t)uncompressed_size > sd->capacity) {
                                free(sd->data);
                                sd->capacity = (size_t)uncompressed_size;
                                sd->data = (uint8_t *)malloc(sd->capacity);
                                if (!sd->data) vectra_error("alloc failed");
                            }
                            if (compression == VTR_COMP_SHUFFLE_LZ2) {
                                vtr_lz2_decompress_into(sd->data, uncompressed_size,
                                                        se->data, data_size);
                                uint8_t es = vtr_shuffle_elem_size(t, encoding);
                                if (es > 0) vtr_byte_unshuffle(sd->data, uncompressed_size / es, es);
                            } else {
                                vectra_error("unknown compression tag: 0x%02x", compression);
                            }
                            decoded_src = sd->data;
                            decoded_size = uncompressed_size;
                        } else {
                            decoded_src = se->data;
                            decoded_size = data_size;
                        }

                        int decoded_into_direct = 0;
                        if (direct_bufs && direct_bufs[out_col] && is_fixed) {
                            if (vtr_decode_column_raw_into(t, n_rows, encoding,
                                                            decoded_src, decoded_size,
                                                            direct_bufs[out_col])) {
                                uint8_t *dst = (uint8_t *)direct_bufs[out_col];
                                arr.owns_data    = 0;
                                arr.data_borrowed = 1;
                                switch (t) {
                                case VEC_INT64:  arr.buf.i64 = (int64_t *)dst; break;
                                case VEC_INT32:  arr.buf.i32 = (int32_t *)dst; break;
                                case VEC_INT16:  arr.buf.i16 = (int16_t *)dst; break;
                                case VEC_INT8:   arr.buf.i8  = (int8_t *)dst;  break;
                                case VEC_DOUBLE: arr.buf.dbl = (double *)dst;  break;
                                default:         arr.buf.bln = dst;            break;
                                }
                                decoded_into_direct = 1;
                            }
                        }
                        /* String-defer fast path: see matching comment in
                         * vtr1_read_rowgroup_ex. */
                        if (!decoded_into_direct && t == VEC_STRING &&
                            encoding == VTR_ENC_DICTIONARY &&
                            direct_bufs && direct_bufs[out_col] == VTR_STRING_DICT_DEFER) {
                            VtrDictBlob *blob = vtr_dict_parse_to_blob(
                                decoded_src, decoded_size);
                            if (blob) {
                                arr.str_dict = blob;
                                arr.buf.str.offsets = (int64_t *)calloc(
                                    (size_t)(n_rows + 1), sizeof(int64_t));
                                if (!arr.buf.str.offsets)
                                    vectra_error("alloc failed for dict-defer offsets");
                                arr.buf.str.data = (char *)malloc(1);
                                arr.buf.str.data_len = 0;
                                decoded_into_direct = 1;
                            }
                        }
                        if (!decoded_into_direct) {
                            vtr_decode_column_raw(&arr, n_rows, encoding,
                                                  decoded_src, decoded_size);
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
                default:
                    vectra_error("unexpected narrow int type in v1-v3 file");
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
                if (shdr[0] == VTR_ENC_QUANTIZE)
                    fseek(fp, 17, SEEK_CUR);
                if (shdr[0] == VTR_ENC_SPATIAL) {
                    read_u8(fp);  /* predictor */
                    read_u32(fp); /* nx */
                    read_u32(fp); /* ny */
                    uint8_t sq2 = read_u8(fp);
                    if (sq2) fseek(fp, 17, SEEK_CUR);
                    read_u16(fp); /* tile_size */
                    uint32_t snt2 = read_u32(fp);
                    if (snt2 > 0) fseek(fp, (long)(snt2 * 3 * 4), SEEK_CUR);
                }
                fseek(fp, (long)skip_size, SEEK_CUR);
            } else {
                switch (t) {
                case VEC_INT64:  fseek(fp, (long)(n_rows * 8), SEEK_CUR); break;
                case VEC_DOUBLE: fseek(fp, (long)(n_rows * 8), SEEK_CUR); break;
                case VEC_BOOL:   fseek(fp, (long)n_rows, SEEK_CUR); break;
                default:
                    vectra_error("unexpected narrow int type in v1-v3 file");
                    break;
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
    return vtr1_read_parallel_into(file, col_mask, path, NULL, NULL, 0,
                                   out_count);
}

/* Parallel reader with optional zero-copy direct buffers.
   col_bases (length n_out_cols) gives the base address of each output column;
   col_elem_sizes gives the element size in bytes per output column.
   For row group rg with cumulative row offset = sum_{r<rg} n_rows[r], thread
   passes direct_bufs[i] = col_bases[i] + offset * col_elem_sizes[i] to the
   per-RG decoder. Pass NULL col_bases for the no-direct fallback. */
VecBatch **vtr1_read_parallel_into(Vtr1File *file, const int *col_mask,
                                   const char *path,
                                   void **col_bases,
                                   const size_t *col_elem_sizes,
                                   int n_out_cols,
                                   uint32_t *out_count) {
    uint32_t n_rgs = file->header.n_rowgroups;
    *out_count = n_rgs;

    VecBatch **batches = (VecBatch **)calloc(n_rgs, sizeof(VecBatch *));
    if (!batches) vectra_error("alloc failed for parallel read");

    /* Pre-compute cumulative row offsets per row group so each thread can
       jump straight to its output slot without contention. */
    int64_t *rg_offsets = NULL;
    if (col_bases) {
        rg_offsets = (int64_t *)malloc((size_t)n_rgs * sizeof(int64_t));
        if (!rg_offsets) vectra_error("alloc failed for rg_offsets");
        int64_t cum = 0;
        for (uint32_t rg = 0; rg < n_rgs; rg++) {
            rg_offsets[rg] = cum;
            cum += file->rowgroups[rg].n_rows;
        }
    }

    #pragma omp parallel
    {
        /* Thread-local file handle and scratch buffers */
        FILE *fp = fopen(path, "rb");
        if (!fp) vectra_error("parallel read: cannot open file: %s", path);
        setvbuf(fp, NULL, _IOFBF, 256 * 1024);
        Vtr1Scratch se = {0}, sd = {0};

        /* Thread-local direct_bufs scratch (per-RG, since the offset varies) */
        void **thread_bufs = NULL;
        if (col_bases) {
            thread_bufs = (void **)malloc((size_t)n_out_cols * sizeof(void *));
            if (!thread_bufs) vectra_error("alloc failed for thread_bufs");
        }

        #pragma omp for schedule(dynamic)
        for (uint32_t rg = 0; rg < n_rgs; rg++) {
            void **bufs = NULL;
            if (col_bases) {
                int64_t off = rg_offsets[rg];
                for (int i = 0; i < n_out_cols; i++) {
                    if (col_bases[i]) {
                        thread_bufs[i] = (uint8_t *)col_bases[i]
                                       + (size_t)off * col_elem_sizes[i];
                    } else {
                        thread_bufs[i] = NULL;
                    }
                }
                bufs = thread_bufs;
            }
            batches[rg] = read_rg_with_fp(file, rg, col_mask, fp, &se, &sd,
                                          bufs);
        }

        free(thread_bufs);
        fclose(fp);
        free(se.data);
        free(sd.data);
    }

    free(rg_offsets);
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
