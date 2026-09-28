#include "parquet_meta.h"
#include "parquet_thrift.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static char *pq_strdup_n(const uint8_t *s, uint32_t n) {
    char *r = (char *)malloc((size_t)n + 1);
    if (!r) return NULL;
    if (n) memcpy(r, s, n);
    r[n] = '\0';
    return r;
}

/* ---- LogicalType ---- */

static void decode_time_unit(PqThrift *t, int *unit) {
    int16_t last = 0, id;
    int ft;
    while (pqt_field(t, &last, &id, &ft)) {
        if (ft == PQT_STRUCT && id >= 1 && id <= 3) *unit = id;
        pqt_skip(t, ft);
    }
}

static void decode_logical(PqThrift *t, PqSchemaElem *e) {
    int16_t last = 0, id;
    int ft;
    while (pqt_field(t, &last, &id, &ft)) {
        if (ft != PQT_STRUCT) { pqt_skip(t, ft); continue; }
        e->logical = id;
        int16_t l2 = 0, id2;
        int ft2;
        switch (id) {
        case PQ_LT_DECIMAL:
            while (pqt_field(t, &l2, &id2, &ft2)) {
                if (id2 == 1) e->lt_scale = (int32_t)pqt_int(t, ft2);
                else if (id2 == 2) e->lt_precision = (int32_t)pqt_int(t, ft2);
                else pqt_skip(t, ft2);
            }
            break;
        case PQ_LT_TIME:
        case PQ_LT_TIMESTAMP:
            while (pqt_field(t, &l2, &id2, &ft2)) {
                if (id2 == 1 && (ft2 == PQT_TRUE || ft2 == PQT_FALSE))
                    e->lt_utc = (ft2 == PQT_TRUE);
                else if (id2 == 2 && ft2 == PQT_STRUCT)
                    decode_time_unit(t, &e->lt_unit);
                else pqt_skip(t, ft2);
            }
            break;
        case PQ_LT_INTEGER:
            while (pqt_field(t, &l2, &id2, &ft2)) {
                if (id2 == 1) e->lt_bit_width = (int)pqt_int(t, ft2);
                else if (id2 == 2 && (ft2 == PQT_TRUE || ft2 == PQT_FALSE))
                    e->lt_signed = (ft2 == PQT_TRUE);
                else pqt_skip(t, ft2);
            }
            break;
        default:
            pqt_skip(t, ft);
        }
    }
}

/* ---- SchemaElement ---- */

static void decode_schema_elem(PqThrift *t, PqSchemaElem *e) {
    e->type = -1;
    e->repetition = -1;
    e->converted_type = -1;
    e->lt_signed = 1;
    int16_t last = 0, id;
    int ft;
    while (pqt_field(t, &last, &id, &ft)) {
        switch (id) {
        case 1: e->type = (int)pqt_int(t, ft); break;
        case 2: e->type_length = (int32_t)pqt_int(t, ft); break;
        case 3: e->repetition = (int)pqt_int(t, ft); break;
        case 4:
            if (ft == PQT_BINARY) {
                uint32_t n;
                const uint8_t *s = pqt_binary(t, &n);
                if (s && !e->name) e->name = pq_strdup_n(s, n);
                if (!e->name) t->err = 1;
            } else { pqt_skip(t, ft); t->err = 1; }
            break;
        case 5: e->num_children = (int32_t)pqt_int(t, ft); break;
        case 6: e->converted_type = (int)pqt_int(t, ft); break;
        case 7: e->scale = (int32_t)pqt_int(t, ft); break;
        case 8: e->precision = (int32_t)pqt_int(t, ft); break;
        case 10:
            if (ft == PQT_STRUCT) decode_logical(t, e);
            else pqt_skip(t, ft);
            break;
        default: pqt_skip(t, ft);
        }
    }
}

/* ---- Statistics ---- */

static void decode_stats(PqThrift *t, PqStats *s) {
    const uint8_t *omin = NULL, *omax = NULL, *nmin = NULL, *nmax = NULL;
    uint32_t omin_l = 0, omax_l = 0, nmin_l = 0, nmax_l = 0;
    s->null_count = -1;
    int16_t last = 0, id;
    int ft;
    while (pqt_field(t, &last, &id, &ft)) {
        if (ft == PQT_BINARY && id >= 1 && id <= 6 && id != 3 && id != 4) {
            uint32_t n;
            const uint8_t *b = pqt_binary(t, &n);
            if (id == 1) { omax = b; omax_l = n; }
            else if (id == 2) { omin = b; omin_l = n; }
            else if (id == 5) { nmax = b; nmax_l = n; }
            else { nmin = b; nmin_l = n; }
        } else if (id == 3) {
            s->null_count = pqt_int(t, ft);
        } else {
            pqt_skip(t, ft);
        }
    }
    if (nmin && nmax) {
        s->min = nmin; s->min_len = nmin_l;
        s->max = nmax; s->max_len = nmax_l;
        s->min_is_new = 1;
    } else if (omin && omax) {
        s->min = omin; s->min_len = omin_l;
        s->max = omax; s->max_len = omax_l;
        s->min_is_new = 0;
    }
}

/* ---- ColumnMetaData / ColumnChunk ---- */

static void decode_col_meta(PqThrift *t, PqColumnChunk *c) {
    c->has_meta = 1;
    c->dict_page_offset = -1;
    int16_t last = 0, id;
    int ft;
    while (pqt_field(t, &last, &id, &ft)) {
        switch (id) {
        case 1: c->type = (int)pqt_int(t, ft); break;
        case 3:
            if (ft == PQT_LIST && !c->path) {
                int et;
                uint32_t n = pqt_list(t, &et);
                if (et != PQT_BINARY) { t->err = 1; break; }
                c->path = (char **)calloc(n ? n : 1, sizeof(char *));
                if (!c->path) { t->err = 1; break; }
                c->n_path = (int)n;
                for (uint32_t i = 0; i < n && !t->err; i++) {
                    uint32_t l;
                    const uint8_t *s = pqt_binary(t, &l);
                    if (s) c->path[i] = pq_strdup_n(s, l);
                    if (!c->path[i]) t->err = 1;
                }
            } else pqt_skip(t, ft);
            break;
        case 4: c->codec = (int)pqt_int(t, ft); break;
        case 5: c->num_values = pqt_int(t, ft); break;
        case 7: c->total_compressed_size = pqt_int(t, ft); break;
        case 9: c->data_page_offset = pqt_int(t, ft); break;
        case 11: c->dict_page_offset = pqt_int(t, ft); break;
        case 12:
            if (ft == PQT_STRUCT) decode_stats(t, &c->stats);
            else pqt_skip(t, ft);
            break;
        default: pqt_skip(t, ft);
        }
    }
}

static void decode_col_chunk(PqThrift *t, PqColumnChunk *c) {
    c->stats.null_count = -1;
    int16_t last = 0, id;
    int ft;
    while (pqt_field(t, &last, &id, &ft)) {
        if (id == 1 && ft == PQT_BINARY) {
            uint32_t n;
            (void)pqt_binary(t, &n);
            c->external = 1;
        } else if (id == 3 && ft == PQT_STRUCT) {
            decode_col_meta(t, c);
        } else {
            pqt_skip(t, ft);
        }
    }
}

static void decode_row_group(PqThrift *t, PqRowGroup *rg) {
    int16_t last = 0, id;
    int ft;
    while (pqt_field(t, &last, &id, &ft)) {
        if (id == 1 && ft == PQT_LIST && !rg->cols) {
            int et;
            uint32_t n = pqt_list(t, &et);
            if (et != PQT_STRUCT) { t->err = 1; break; }
            rg->cols = (PqColumnChunk *)calloc(n ? n : 1, sizeof(PqColumnChunk));
            if (!rg->cols) { t->err = 1; break; }
            rg->n_cols = (int)n;
            for (uint32_t i = 0; i < n && !t->err; i++)
                decode_col_chunk(t, &rg->cols[i]);
        } else if (id == 3) {
            rg->num_rows = pqt_int(t, ft);
        } else {
            pqt_skip(t, ft);
        }
    }
}

int pq_meta_decode(uint8_t *footer, size_t len, PqFileMeta *m,
                   char *err, size_t errlen) {
    memset(m, 0, sizeof(*m));
    m->footer = footer;
    PqThrift t;
    pqt_init(&t, footer, len);
    int16_t last = 0, id;
    int ft;
    while (pqt_field(&t, &last, &id, &ft)) {
        if (id == 2 && ft == PQT_LIST && !m->schema) {
            int et;
            uint32_t n = pqt_list(&t, &et);
            if (et != PQT_STRUCT) { t.err = 1; break; }
            m->schema = (PqSchemaElem *)calloc(n ? n : 1, sizeof(PqSchemaElem));
            if (!m->schema) { t.err = 1; break; }
            m->n_schema = (int)n;
            for (uint32_t i = 0; i < n && !t.err; i++)
                decode_schema_elem(&t, &m->schema[i]);
        } else if (id == 3) {
            m->num_rows = pqt_int(&t, ft);
        } else if (id == 4 && ft == PQT_LIST && !m->rgs) {
            int et;
            uint32_t n = pqt_list(&t, &et);
            if (et != PQT_STRUCT) { t.err = 1; break; }
            m->rgs = (PqRowGroup *)calloc(n ? n : 1, sizeof(PqRowGroup));
            if (!m->rgs) { t.err = 1; break; }
            m->n_rgs = (int)n;
            for (uint32_t i = 0; i < n && !t.err; i++)
                decode_row_group(&t, &m->rgs[i]);
        } else {
            pqt_skip(&t, ft);
        }
    }
    if (t.err) {
        snprintf(err, errlen, "malformed Parquet footer");
        return -1;
    }
    if (!m->schema || m->n_schema < 1) {
        snprintf(err, errlen, "Parquet footer has no schema");
        return -1;
    }
    for (int i = 0; i < m->n_schema; i++) {
        if (!m->schema[i].name) {
            snprintf(err, errlen, "Parquet schema element %d has no name", i);
            return -1;
        }
    }
    return 0;
}

void pq_meta_free(PqFileMeta *m) {
    if (!m) return;
    if (m->schema) {
        for (int i = 0; i < m->n_schema; i++) free(m->schema[i].name);
        free(m->schema);
    }
    if (m->rgs) {
        for (int r = 0; r < m->n_rgs; r++) {
            PqRowGroup *rg = &m->rgs[r];
            if (!rg->cols) continue;
            for (int c = 0; c < rg->n_cols; c++) {
                PqColumnChunk *cc = &rg->cols[c];
                if (cc->path) {
                    for (int k = 0; k < cc->n_path; k++) free(cc->path[k]);
                    free(cc->path);
                }
            }
            free(rg->cols);
        }
        free(m->rgs);
    }
    free(m->footer);
    memset(m, 0, sizeof(*m));
}

/* ---- PageHeader ---- */

static void decode_data_page_v1(PqThrift *t, PqPageHeader *h) {
    h->has_data = 1;
    int16_t last = 0, id;
    int ft;
    while (pqt_field(t, &last, &id, &ft)) {
        switch (id) {
        case 1: h->num_values = (int32_t)pqt_int(t, ft); break;
        case 2: h->encoding = (int)pqt_int(t, ft); break;
        case 3: h->def_encoding = (int)pqt_int(t, ft); break;
        case 4: h->rep_encoding = (int)pqt_int(t, ft); break;
        default: pqt_skip(t, ft);
        }
    }
}

static void decode_dict_page(PqThrift *t, PqPageHeader *h) {
    h->has_dict = 1;
    int16_t last = 0, id;
    int ft;
    while (pqt_field(t, &last, &id, &ft)) {
        switch (id) {
        case 1: h->num_values = (int32_t)pqt_int(t, ft); break;
        case 2: h->encoding = (int)pqt_int(t, ft); break;
        default: pqt_skip(t, ft);
        }
    }
}

static void decode_data_page_v2(PqThrift *t, PqPageHeader *h) {
    h->has_v2 = 1;
    h->is_compressed = 1;
    int16_t last = 0, id;
    int ft;
    while (pqt_field(t, &last, &id, &ft)) {
        switch (id) {
        case 1: h->num_values = (int32_t)pqt_int(t, ft); break;
        case 2: h->num_nulls = (int32_t)pqt_int(t, ft); break;
        case 3: h->num_rows = (int32_t)pqt_int(t, ft); break;
        case 4: h->encoding = (int)pqt_int(t, ft); break;
        case 5: h->def_len = (int32_t)pqt_int(t, ft); break;
        case 6: h->rep_len = (int32_t)pqt_int(t, ft); break;
        case 7:
            if (ft == PQT_TRUE || ft == PQT_FALSE) h->is_compressed = (ft == PQT_TRUE);
            else pqt_skip(t, ft);
            break;
        default: pqt_skip(t, ft);
        }
    }
}

int64_t pq_page_header_decode(const uint8_t *p, size_t n, PqPageHeader *h) {
    memset(h, 0, sizeof(*h));
    h->type = -1;
    PqThrift t;
    pqt_init(&t, p, n);
    int16_t last = 0, id;
    int ft;
    while (pqt_field(&t, &last, &id, &ft)) {
        switch (id) {
        case 1: h->type = (int)pqt_int(&t, ft); break;
        case 2: h->uncompressed_size = (int32_t)pqt_int(&t, ft); break;
        case 3: h->compressed_size = (int32_t)pqt_int(&t, ft); break;
        case 5:
            if (ft == PQT_STRUCT) decode_data_page_v1(&t, h);
            else pqt_skip(&t, ft);
            break;
        case 7:
            if (ft == PQT_STRUCT) decode_dict_page(&t, h);
            else pqt_skip(&t, ft);
            break;
        case 8:
            if (ft == PQT_STRUCT) decode_data_page_v2(&t, h);
            else pqt_skip(&t, ft);
            break;
        default: pqt_skip(&t, ft);
        }
    }
    if (t.err) return -1;
    if (h->uncompressed_size < 0 || h->compressed_size < 0) return -1;
    if (h->num_values < 0 || h->def_len < 0 || h->rep_len < 0 ||
        h->num_nulls < 0 || h->num_rows < 0) return -1;
    return (int64_t)(t.p - p);
}
