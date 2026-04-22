#include "expr.h"
#include "array.h"
#include "scalar_ops.h"
#include "coerce.h"
#include "error.h"
#include "string_distance.h"
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <regex.h>
#include "vec_omp.h"

/* Expand regex backreferences (\1..\9) in replacement string.
   Writes expanded replacement to dst, returns bytes written. */
static inline int64_t backref_write(char *dst, const char *rep, int64_t rlen,
                                     const regmatch_t *matches, int max_groups,
                                     const char *cursor) {
    int64_t w = 0;
    for (int64_t j = 0; j < rlen; ) {
        if (rep[j] == '\\' && j + 1 < rlen && rep[j+1] >= '1' && rep[j+1] <= '9') {
            int gi = rep[j+1] - '0';
            if (gi < max_groups && matches[gi].rm_so >= 0) {
                int64_t gl = matches[gi].rm_eo - matches[gi].rm_so;
                memcpy(dst + w, cursor + matches[gi].rm_so, (size_t)gl);
                w += gl;
            }
            j += 2;
        } else { dst[w++] = rep[j++]; }
    }
    return w;
}

/* Compute expanded replacement length for backreferences. */
static inline int64_t backref_len(const char *rep, int64_t rlen,
                                   const regmatch_t *matches, int max_groups) {
    int64_t len = 0;
    for (int64_t j = 0; j < rlen; ) {
        if (rep[j] == '\\' && j + 1 < rlen && rep[j+1] >= '1' && rep[j+1] <= '9') {
            int gi = rep[j+1] - '0';
            if (gi < max_groups && matches[gi].rm_so >= 0)
                len += matches[gi].rm_eo - matches[gi].rm_so;
            j += 2;
        } else { len++; j++; }
    }
    return len;
}

/* Thin wrappers around shared implementations in string_distance.h,
   preserving the call-site names used throughout this file. */
static int64_t levenshtein_distance(const char *s, int64_t len_s,
                                     const char *t, int64_t len_t,
                                     int64_t max_dist) {
    return strdist_levenshtein(s, len_s, t, len_t, max_dist);
}

static int64_t dl_distance(const char *s, int64_t len_s,
                            const char *t, int64_t len_t,
                            int64_t max_dist) {
    return strdist_dl(s, len_s, t, len_t, max_dist);
}

static double jaro_winkler_sim(const char *s, int64_t len_s,
                                const char *t, int64_t len_t) {
    return strdist_jaro_winkler(s, len_s, t, len_t);
}

VecArray *vec_expr_eval_string(VecExprKind op, const VecExpr *expr,
                                const VecBatch *batch) {
    switch (op) {
    case EXPR_NCHAR: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING)
            vectra_error("nchar: argument must be string");
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_INT64, s->length);
        for (int64_t i = 0; i < s->length; i++) {
            if (!vec_array_is_valid(s, i)) {
                vec_array_set_null(out, i);
            } else {
                vec_array_set_valid(out, i);
                out->buf.i64[i] = s->buf.str.offsets[i + 1] -
                                   s->buf.str.offsets[i];
            }
        }
        vec_array_free(s); free(s);
        return out;
    }
    case EXPR_SUBSTR: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        VecArray *start_a = vec_expr_eval(expr->left, batch);
        VecArray *stop_a = vec_expr_eval(expr->right, batch);
        if (s->type != VEC_STRING)
            vectra_error("substr: first argument must be string");
        int64_t n = s->length;

        /* First pass: compute total output length */
        int64_t total_len = 0;
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(s, i) ||
                !vec_array_is_valid(start_a, i) ||
                !vec_array_is_valid(stop_a, i))
                continue;
            int64_t slen = s->buf.str.offsets[i + 1] - s->buf.str.offsets[i];
            int64_t st = (start_a->type == VEC_DOUBLE)
                         ? (int64_t)start_a->buf.dbl[i]
                         : start_a->buf.i64[i];
            int64_t sp = (stop_a->type == VEC_DOUBLE)
                         ? (int64_t)stop_a->buf.dbl[i]
                         : stop_a->buf.i64[i];
            st = st - 1; /* R is 1-based */
            if (st < 0) st = 0;
            if (sp > slen) sp = slen;
            if (sp > st) total_len += sp - st;
        }

        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_STRING, n);
        free(out->buf.str.data);  /* free 1-byte from vec_array_alloc */
        out->buf.str.data = (char *)malloc((size_t)(total_len > 0 ? total_len : 1));
        out->buf.str.data_len = total_len;

        int64_t offset = 0;
        for (int64_t i = 0; i < n; i++) {
            out->buf.str.offsets[i] = offset;
            if (!vec_array_is_valid(s, i) ||
                !vec_array_is_valid(start_a, i) ||
                !vec_array_is_valid(stop_a, i)) {
                vec_array_set_null(out, i);
                continue;
            }
            vec_array_set_valid(out, i);
            int64_t so = s->buf.str.offsets[i];
            int64_t slen = s->buf.str.offsets[i + 1] - so;
            int64_t st = (start_a->type == VEC_DOUBLE)
                         ? (int64_t)start_a->buf.dbl[i]
                         : start_a->buf.i64[i];
            int64_t sp = (stop_a->type == VEC_DOUBLE)
                         ? (int64_t)stop_a->buf.dbl[i]
                         : stop_a->buf.i64[i];
            st = st - 1;
            if (st < 0) st = 0;
            if (sp > slen) sp = slen;
            int64_t sub_len = (sp > st) ? sp - st : 0;
            if (sub_len > 0) {
                memcpy(out->buf.str.data + offset,
                       s->buf.str.data + so + st, (size_t)sub_len);
                offset += sub_len;
            }
        }
        out->buf.str.offsets[n] = offset;

        vec_array_free(s); free(s);
        vec_array_free(start_a); free(start_a);
        vec_array_free(stop_a); free(stop_a);
        return out;
    }
    case EXPR_GREPL: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING)
            vectra_error("grepl: argument must be string");
        const char *pattern = expr->lit_str;
        int64_t pat_len = (int64_t)strlen(pattern);
        int64_t n = s->length;

        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_BOOL, n);

        if (!expr->fixed) {
            /* Regex mode — parallel with per-thread compiled regex */
            regex_t re_check;
            if (regcomp(&re_check, pattern, REG_EXTENDED | REG_NOSUB) != 0)
                vectra_error("grepl: invalid regex pattern: %s", pattern);
            regfree(&re_check);
            #pragma omp parallel if(n > 1000)
            {
                regex_t re_local;
                regcomp(&re_local, pattern, REG_EXTENDED | REG_NOSUB);
                int64_t tl_cap = 256;
                char *tl_buf = (char *)malloc((size_t)tl_cap);
                #pragma omp for schedule(dynamic, 64)
                for (int64_t i = 0; i < n; i++) {
                    if (!vec_array_is_valid(s, i)) { vec_array_set_null(out, i); continue; }
                    vec_array_set_valid(out, i);
                    int64_t so = s->buf.str.offsets[i];
                    int64_t slen = s->buf.str.offsets[i + 1] - so;
                    if (slen + 1 > tl_cap) {
                        tl_cap = slen + 1;
                        tl_buf = (char *)realloc(tl_buf, (size_t)tl_cap);
                    }
                    memcpy(tl_buf, s->buf.str.data + so, (size_t)slen);
                    tl_buf[slen] = '\0';
                    out->buf.bln[i] = (uint8_t)(regexec(&re_local, tl_buf, 0, NULL, 0) == 0);
                }
                free(tl_buf);
                regfree(&re_local);
            }
        } else {
            /* Fixed substring match */
            for (int64_t i = 0; i < n; i++) {
                if (!vec_array_is_valid(s, i)) { vec_array_set_null(out, i); continue; }
                vec_array_set_valid(out, i);
                int64_t so = s->buf.str.offsets[i];
                int64_t slen = s->buf.str.offsets[i + 1] - so;
                int found = 0;
                if (pat_len <= slen) {
                    for (int64_t j = 0; j <= slen - pat_len; j++) {
                        if (memcmp(s->buf.str.data + so + j, pattern, (size_t)pat_len) == 0)
                            { found = 1; break; }
                    }
                }
                out->buf.bln[i] = (uint8_t)found;
            }
        }

        vec_array_free(s); free(s);
        return out;
    }
    case EXPR_TOLOWER: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING) vectra_error("tolower: argument must be string");
        VecArray *out = vec_coerce(s, VEC_STRING);
        for (int64_t i = 0; i < out->buf.str.data_len; i++)
            out->buf.str.data[i] = (char)tolower((unsigned char)out->buf.str.data[i]);
        vec_array_free(s); free(s);
        return out;
    }
    case EXPR_TOUPPER: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING) vectra_error("toupper: argument must be string");
        VecArray *out = vec_coerce(s, VEC_STRING);
        for (int64_t i = 0; i < out->buf.str.data_len; i++)
            out->buf.str.data[i] = (char)toupper((unsigned char)out->buf.str.data[i]);
        vec_array_free(s); free(s);
        return out;
    }
    case EXPR_TRIMWS: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING) vectra_error("trimws: argument must be string");
        int64_t n = s->length;
        /* First pass: compute trimmed lengths */
        int64_t total = 0;
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(s, i)) continue;
            int64_t so = s->buf.str.offsets[i], eo = s->buf.str.offsets[i + 1];
            const char *p = s->buf.str.data + so;
            int64_t len = eo - so;
            int64_t start = 0, end = len;
            while (start < end && (p[start] == ' ' || p[start] == '\t' || p[start] == '\n' || p[start] == '\r')) start++;
            while (end > start && (p[end - 1] == ' ' || p[end - 1] == '\t' || p[end - 1] == '\n' || p[end - 1] == '\r')) end--;
            total += end - start;
        }
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_STRING, n);
        free(out->buf.str.data);
        out->buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
        out->buf.str.data_len = total;
        int64_t off = 0;
        for (int64_t i = 0; i < n; i++) {
            out->buf.str.offsets[i] = off;
            if (!vec_array_is_valid(s, i)) { vec_array_set_null(out, i); continue; }
            vec_array_set_valid(out, i);
            int64_t so = s->buf.str.offsets[i], eo = s->buf.str.offsets[i + 1];
            const char *p = s->buf.str.data + so;
            int64_t len = eo - so;
            int64_t start = 0, end = len;
            while (start < end && (p[start] == ' ' || p[start] == '\t' || p[start] == '\n' || p[start] == '\r')) start++;
            while (end > start && (p[end - 1] == ' ' || p[end - 1] == '\t' || p[end - 1] == '\n' || p[end - 1] == '\r')) end--;
            int64_t tlen = end - start;
            if (tlen > 0) memcpy(out->buf.str.data + off, p + start, (size_t)tlen);
            off += tlen;
        }
        out->buf.str.offsets[n] = off;
        vec_array_free(s); free(s);
        return out;
    }
    case EXPR_IN: {
        VecArray *o = vec_expr_eval(expr->operand, batch);
        int64_t n = o->length;
        int64_t ns = expr->n_set;
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_BOOL, n);

        /* For large sets, build a hash set for O(1) lookup */
        if (ns > 16) {
            /* FNV-1a hash set with open addressing */
            int64_t ht_cap = 1;
            while (ht_cap < ns * 2) ht_cap <<= 1;
            int64_t mask = ht_cap - 1;
            uint64_t *ht_hashes = (uint64_t *)calloc((size_t)ht_cap, sizeof(uint64_t));
            int8_t *ht_used = (int8_t *)calloc((size_t)ht_cap, sizeof(int8_t));
            int64_t *ht_idx = (int64_t *)malloc((size_t)ht_cap * sizeof(int64_t));
            /* Insert set values */
            for (int64_t j = 0; j < ns; j++) {
                uint64_t h;
                if (o->type == VEC_DOUBLE) {
                    double v = expr->set_dbl[j];
                    memcpy(&h, &v, sizeof(h));
                    h = h * 0x00000100000001B3ULL ^ 0xcbf29ce484222325ULL;
                } else if (o->type == VEC_INT64) {
                    int64_t v = expr->set_i64[j];
                    memcpy(&h, &v, sizeof(h));
                    h = h * 0x00000100000001B3ULL ^ 0xcbf29ce484222325ULL;
                } else {
                    const char *s = expr->set_str[j];
                    h = 0xcbf29ce484222325ULL;
                    for (const char *p = s; *p; p++)
                        h = (h ^ (uint8_t)*p) * 0x00000100000001B3ULL;
                }
                int64_t slot = (int64_t)(h & (uint64_t)mask);
                while (ht_used[slot]) slot = (slot + 1) & mask;
                ht_hashes[slot] = h;
                ht_used[slot] = 1;
                ht_idx[slot] = j;
            }
            for (int64_t i = 0; i < n; i++) {
                if (!vec_array_is_valid(o, i)) { vec_array_set_null(out, i); continue; }
                vec_array_set_valid(out, i);
                uint64_t h;
                if (o->type == VEC_DOUBLE) {
                    double v = o->buf.dbl[i];
                    memcpy(&h, &v, sizeof(h));
                    h = h * 0x00000100000001B3ULL ^ 0xcbf29ce484222325ULL;
                } else if (o->type == VEC_INT64) {
                    int64_t v = o->buf.i64[i];
                    memcpy(&h, &v, sizeof(h));
                    h = h * 0x00000100000001B3ULL ^ 0xcbf29ce484222325ULL;
                } else {
                    int64_t so2 = o->buf.str.offsets[i], eo2 = o->buf.str.offsets[i + 1];
                    h = 0xcbf29ce484222325ULL;
                    for (int64_t k = so2; k < eo2; k++)
                        h = (h ^ (uint8_t)o->buf.str.data[k]) * 0x00000100000001B3ULL;
                }
                int found = 0;
                int64_t slot = (int64_t)(h & (uint64_t)mask);
                while (ht_used[slot]) {
                    if (ht_hashes[slot] == h) {
                        int64_t j = ht_idx[slot];
                        /* Verify equality */
                        if (o->type == VEC_DOUBLE) {
                            if (o->buf.dbl[i] == expr->set_dbl[j]) { found = 1; break; }
                        } else if (o->type == VEC_INT64) {
                            if (o->buf.i64[i] == expr->set_i64[j]) { found = 1; break; }
                        } else {
                            int64_t so2 = o->buf.str.offsets[i], eo2 = o->buf.str.offsets[i + 1];
                            int64_t slen2 = eo2 - so2;
                            int64_t clen = (int64_t)strlen(expr->set_str[j]);
                            if (slen2 == clen && memcmp(o->buf.str.data + so2, expr->set_str[j], (size_t)slen2) == 0)
                                { found = 1; break; }
                        }
                    }
                    slot = (slot + 1) & mask;
                }
                out->buf.bln[i] = (uint8_t)found;
            }
            free(ht_hashes); free(ht_used); free(ht_idx);
        } else {
            /* Small set: linear scan */
            for (int64_t i = 0; i < n; i++) {
                if (!vec_array_is_valid(o, i)) { vec_array_set_null(out, i); continue; }
                vec_array_set_valid(out, i);
                int found = 0;
                if (o->type == VEC_DOUBLE) {
                    double v = o->buf.dbl[i];
                    for (int64_t j = 0; j < ns; j++)
                        if (v == expr->set_dbl[j]) { found = 1; break; }
                } else if (o->type == VEC_INT64) {
                    int64_t v = o->buf.i64[i];
                    for (int64_t j = 0; j < ns; j++)
                        if (v == expr->set_i64[j]) { found = 1; break; }
                } else if (o->type == VEC_STRING) {
                    int64_t so2 = o->buf.str.offsets[i], eo2 = o->buf.str.offsets[i + 1];
                    int64_t slen2 = eo2 - so2;
                    for (int64_t j = 0; j < ns; j++) {
                        int64_t clen = (int64_t)strlen(expr->set_str[j]);
                        if (slen2 == clen && memcmp(o->buf.str.data + so2, expr->set_str[j], (size_t)slen2) == 0)
                            { found = 1; break; }
                    }
                }
                out->buf.bln[i] = (uint8_t)found;
            }
        }
        vec_array_free(o); free(o);
        return out;
    }
    case EXPR_PASTE0: {
        VecArray *a = vec_expr_eval(expr->left, batch);
        VecArray *b = vec_expr_eval(expr->right, batch);
        if (a->type != VEC_STRING) vectra_error("paste0: first argument must be string");
        if (b->type != VEC_STRING) vectra_error("paste0: second argument must be string");
        int64_t n = a->length;
        /* First pass: compute total length */
        int64_t total = 0;
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(a, i) || !vec_array_is_valid(b, i)) continue;
            total += (a->buf.str.offsets[i+1] - a->buf.str.offsets[i])
                   + (b->buf.str.offsets[i+1] - b->buf.str.offsets[i]);
        }
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_STRING, n);
        free(out->buf.str.data);
        out->buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
        out->buf.str.data_len = total;
        int64_t off = 0;
        for (int64_t i = 0; i < n; i++) {
            out->buf.str.offsets[i] = off;
            if (!vec_array_is_valid(a, i) || !vec_array_is_valid(b, i)) {
                vec_array_set_null(out, i); continue;
            }
            vec_array_set_valid(out, i);
            int64_t sa = a->buf.str.offsets[i], la = a->buf.str.offsets[i+1] - sa;
            int64_t sb = b->buf.str.offsets[i], lb = b->buf.str.offsets[i+1] - sb;
            if (la > 0) memcpy(out->buf.str.data + off, a->buf.str.data + sa, (size_t)la);
            off += la;
            if (lb > 0) memcpy(out->buf.str.data + off, b->buf.str.data + sb, (size_t)lb);
            off += lb;
        }
        out->buf.str.offsets[n] = off;
        vec_array_free(a); free(a);
        vec_array_free(b); free(b);
        return out;
    }
    case EXPR_STARTSWITH: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING) vectra_error("startsWith: argument must be string");
        const char *prefix = expr->lit_str;
        int64_t plen = (int64_t)strlen(prefix);
        int64_t n = s->length;
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_BOOL, n);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(s, i)) { vec_array_set_null(out, i); continue; }
            vec_array_set_valid(out, i);
            int64_t so = s->buf.str.offsets[i];
            int64_t slen = s->buf.str.offsets[i+1] - so;
            out->buf.bln[i] = (uint8_t)(slen >= plen && memcmp(s->buf.str.data + so, prefix, (size_t)plen) == 0);
        }
        vec_array_free(s); free(s);
        return out;
    }
    case EXPR_ENDSWITH: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING) vectra_error("endsWith: argument must be string");
        const char *suffix = expr->lit_str;
        int64_t xlen = (int64_t)strlen(suffix);
        int64_t n = s->length;
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_BOOL, n);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(s, i)) { vec_array_set_null(out, i); continue; }
            vec_array_set_valid(out, i);
            int64_t so = s->buf.str.offsets[i];
            int64_t slen = s->buf.str.offsets[i+1] - so;
            out->buf.bln[i] = (uint8_t)(slen >= xlen && memcmp(s->buf.str.data + so + slen - xlen, suffix, (size_t)xlen) == 0);
        }
        vec_array_free(s); free(s);
        return out;
    }
    case EXPR_GSUB:
    case EXPR_SUB: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING) vectra_error("gsub/sub: argument must be string");
        const char *pat = expr->gsub_pattern;
        const char *rep = expr->gsub_replacement;
        int64_t rlen = (int64_t)strlen(rep);
        int64_t n = s->length;
        int only_first = (op == EXPR_SUB);

        if (!expr->fixed) {
            /* Regex mode: compile regex, two-pass replacement with backreference support */
            #define MAX_GROUPS 10
            regex_t re;
            if (regcomp(&re, pat, REG_EXTENDED) != 0)
                vectra_error("gsub/sub: invalid regex: %s", pat);

            /* Pass 1: compute output lengths (reusable buffer) */
            int64_t *out_lens = (int64_t *)malloc((size_t)n * sizeof(int64_t));
            int64_t total = 0;
            int64_t tb_cap = 256;
            char *tb = (char *)malloc((size_t)tb_cap);
            for (int64_t i = 0; i < n; i++) {
                out_lens[i] = 0;
                if (!vec_array_is_valid(s, i)) continue;
                int64_t so = s->buf.str.offsets[i];
                int64_t slen = s->buf.str.offsets[i+1] - so;
                if (slen + 1 > tb_cap) { tb_cap = slen + 1; tb = (char *)realloc(tb, (size_t)tb_cap); }
                memcpy(tb, s->buf.str.data + so, (size_t)slen);
                tb[slen] = '\0';
                regmatch_t matches[MAX_GROUPS];
                const char *cursor = tb;
                int64_t olen = 0;
                int replaced = 0;
                while (regexec(&re, cursor, MAX_GROUPS, matches, 0) == 0 && matches[0].rm_so != matches[0].rm_eo) {
                    int64_t exp_len = backref_len(rep, rlen, matches, MAX_GROUPS);
                    olen += matches[0].rm_so + exp_len;
                    cursor += matches[0].rm_eo;
                    replaced = 1;
                    if (only_first) { olen += (int64_t)strlen(cursor); break; }
                }
                if (!only_first || !replaced) olen += (int64_t)strlen(cursor);
                if (!replaced) olen = slen;
                out_lens[i] = olen;
                total += olen;
            }
            free(tb);
            VecArray *out = (VecArray *)malloc(sizeof(VecArray));
            *out = vec_array_alloc(VEC_STRING, n);
            free(out->buf.str.data);
            out->buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
            out->buf.str.data_len = total;
            /* Pass 2: fill with backreference expansion (reusable buffer) */
            int64_t off = 0;
            tb_cap = 256;
            tb = (char *)malloc((size_t)tb_cap);
            for (int64_t i = 0; i < n; i++) {
                out->buf.str.offsets[i] = off;
                if (!vec_array_is_valid(s, i)) { vec_array_set_null(out, i); continue; }
                vec_array_set_valid(out, i);
                int64_t so = s->buf.str.offsets[i];
                int64_t slen = s->buf.str.offsets[i+1] - so;
                if (slen + 1 > tb_cap) { tb_cap = slen + 1; tb = (char *)realloc(tb, (size_t)tb_cap); }
                memcpy(tb, s->buf.str.data + so, (size_t)slen);
                tb[slen] = '\0';
                regmatch_t matches[MAX_GROUPS];
                const char *cursor = tb;
                int replaced = 0;
                while (regexec(&re, cursor, MAX_GROUPS, matches, 0) == 0 && matches[0].rm_so != matches[0].rm_eo) {
                    if (matches[0].rm_so > 0) { memcpy(out->buf.str.data + off, cursor, (size_t)matches[0].rm_so); off += matches[0].rm_so; }
                    off += backref_write(out->buf.str.data + off, rep, rlen, matches, MAX_GROUPS, cursor);
                    cursor += matches[0].rm_eo;
                    replaced = 1;
                    if (only_first) {
                        int64_t rem = (int64_t)strlen(cursor);
                        if (rem > 0) { memcpy(out->buf.str.data + off, cursor, (size_t)rem); off += rem; }
                        cursor += rem;
                        break;
                    }
                }
                if (!only_first || !replaced) {
                    int64_t rem = (int64_t)strlen(cursor);
                    if (rem > 0) { memcpy(out->buf.str.data + off, cursor, (size_t)rem); off += rem; }
                }
            }
            free(tb);
            out->buf.str.offsets[n] = off;
            free(out_lens);
            regfree(&re);
            #undef MAX_GROUPS
            vec_array_free(s); free(s);
            return out;
        }

        /* Fixed string replacement (original code) */
        int64_t plen = (int64_t)strlen(pat);
        int64_t total = 0;
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(s, i)) continue;
            int64_t so = s->buf.str.offsets[i];
            int64_t slen = s->buf.str.offsets[i+1] - so;
            if (plen == 0) { total += slen; continue; }
            int64_t out_len = 0, j = 0;
            int replaced = 0;
            while (j <= slen - plen) {
                if (memcmp(s->buf.str.data + so + j, pat, (size_t)plen) == 0 && !(only_first && replaced)) {
                    out_len += rlen;
                    j += plen;
                    replaced = 1;
                    if (only_first) { out_len += slen - j; break; }
                } else { out_len++; j++; }
            }
            if (!only_first || !replaced) out_len += slen - j;
            total += out_len;
        }
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_STRING, n);
        free(out->buf.str.data);
        out->buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
        out->buf.str.data_len = total;
        int64_t off = 0;
        for (int64_t i = 0; i < n; i++) {
            out->buf.str.offsets[i] = off;
            if (!vec_array_is_valid(s, i)) { vec_array_set_null(out, i); continue; }
            vec_array_set_valid(out, i);
            int64_t so = s->buf.str.offsets[i];
            int64_t slen = s->buf.str.offsets[i+1] - so;
            if (plen == 0) {
                if (slen > 0) memcpy(out->buf.str.data + off, s->buf.str.data + so, (size_t)slen);
                off += slen;
                continue;
            }
            int64_t j = 0;
            int replaced = 0;
            while (j <= slen - plen) {
                if (memcmp(s->buf.str.data + so + j, pat, (size_t)plen) == 0 && !(only_first && replaced)) {
                    if (rlen > 0) memcpy(out->buf.str.data + off, rep, (size_t)rlen);
                    off += rlen;
                    j += plen;
                    replaced = 1;
                    if (only_first) {
                        int64_t rem = slen - j;
                        if (rem > 0) memcpy(out->buf.str.data + off, s->buf.str.data + so + j, (size_t)rem);
                        off += rem;
                        j = slen;
                        break;
                    }
                } else {
                    out->buf.str.data[off++] = s->buf.str.data[so + j];
                    j++;
                }
            }
            if (j < slen) {
                int64_t rem = slen - j;
                memcpy(out->buf.str.data + off, s->buf.str.data + so + j, (size_t)rem);
                off += rem;
            }
        }
        out->buf.str.offsets[n] = off;
        vec_array_free(s); free(s);
        return out;
    }
    case EXPR_LEVENSHTEIN:
    case EXPR_LEVENSHTEIN_NORM: {
        /* operand = first string arg, left/right or lit_str = second arg */
        VecArray *a = vec_expr_eval(expr->operand, batch);
        if (a->type != VEC_STRING) vectra_error("levenshtein: first argument must be string");
        int64_t n = a->length;
        int is_norm = (op == EXPR_LEVENSHTEIN_NORM);
        int64_t md = expr->max_dist;

        /* Second argument: literal string (lit_str) or column (left) */
        VecArray *b = NULL;
        const char *pat = NULL;
        int64_t pat_len = 0;
        if (expr->left) {
            b = vec_expr_eval(expr->left, batch);
            if (b->type != VEC_STRING) vectra_error("levenshtein: second argument must be string");
        } else {
            pat = expr->lit_str;
            pat_len = (int64_t)strlen(pat);
        }

        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        if (is_norm) {
            *out = vec_array_alloc(VEC_DOUBLE, n);
        } else {
            *out = vec_array_alloc(VEC_INT64, n);
        }

        #pragma omp parallel for schedule(dynamic, 64) if(n > 1000)
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(a, i) || (b && !vec_array_is_valid(b, i))) {
                vec_array_set_null(out, i);
                continue;
            }
            vec_array_set_valid(out, i);
            int64_t sa = a->buf.str.offsets[i];
            int64_t la = a->buf.str.offsets[i + 1] - sa;
            const char *s2;
            int64_t l2;
            if (b) {
                int64_t sb = b->buf.str.offsets[i];
                l2 = b->buf.str.offsets[i + 1] - sb;
                s2 = b->buf.str.data + sb;
            } else {
                s2 = pat;
                l2 = pat_len;
            }
            int64_t dist = levenshtein_distance(a->buf.str.data + sa, la, s2, l2, md);
            if (is_norm) {
                int64_t max_len = (la > l2) ? la : l2;
                out->buf.dbl[i] = (max_len == 0) ? 0.0 : (double)dist / (double)max_len;
            } else {
                out->buf.i64[i] = dist;
            }
        }
        vec_array_free(a); free(a);
        if (b) { vec_array_free(b); free(b); }
        return out;
    }
    case EXPR_DL_DIST:
    case EXPR_DL_DIST_NORM: {
        VecArray *a = vec_expr_eval(expr->operand, batch);
        if (a->type != VEC_STRING) vectra_error("dl_dist: first argument must be string");
        int64_t n = a->length;
        int is_norm = (op == EXPR_DL_DIST_NORM);
        int64_t md = expr->max_dist;

        VecArray *b = NULL;
        const char *pat = NULL;
        int64_t pat_len = 0;
        if (expr->left) {
            b = vec_expr_eval(expr->left, batch);
            if (b->type != VEC_STRING) vectra_error("dl_dist: second argument must be string");
        } else {
            pat = expr->lit_str;
            pat_len = (int64_t)strlen(pat);
        }

        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        if (is_norm) {
            *out = vec_array_alloc(VEC_DOUBLE, n);
        } else {
            *out = vec_array_alloc(VEC_INT64, n);
        }

        #pragma omp parallel for schedule(dynamic, 64) if(n > 1000)
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(a, i) || (b && !vec_array_is_valid(b, i))) {
                vec_array_set_null(out, i);
                continue;
            }
            vec_array_set_valid(out, i);
            int64_t sa = a->buf.str.offsets[i];
            int64_t la = a->buf.str.offsets[i + 1] - sa;
            const char *s2;
            int64_t l2;
            if (b) {
                int64_t sb = b->buf.str.offsets[i];
                l2 = b->buf.str.offsets[i + 1] - sb;
                s2 = b->buf.str.data + sb;
            } else {
                s2 = pat;
                l2 = pat_len;
            }
            int64_t dist = dl_distance(a->buf.str.data + sa, la, s2, l2, md);
            if (is_norm) {
                int64_t max_len = (la > l2) ? la : l2;
                out->buf.dbl[i] = (max_len == 0) ? 0.0 : (double)dist / (double)max_len;
            } else {
                out->buf.i64[i] = dist;
            }
        }
        vec_array_free(a); free(a);
        if (b) { vec_array_free(b); free(b); }
        return out;
    }
    case EXPR_JARO_WINKLER: {
        VecArray *a = vec_expr_eval(expr->operand, batch);
        if (a->type != VEC_STRING) vectra_error("jaro_winkler: first argument must be string");
        int64_t n = a->length;

        VecArray *b = NULL;
        const char *pat = NULL;
        int64_t pat_len = 0;
        if (expr->left) {
            b = vec_expr_eval(expr->left, batch);
            if (b->type != VEC_STRING) vectra_error("jaro_winkler: second argument must be string");
        } else {
            pat = expr->lit_str;
            pat_len = (int64_t)strlen(pat);
        }

        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_DOUBLE, n);

        #pragma omp parallel for schedule(dynamic, 64) if(n > 1000)
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(a, i) || (b && !vec_array_is_valid(b, i))) {
                vec_array_set_null(out, i);
                continue;
            }
            vec_array_set_valid(out, i);
            int64_t sa = a->buf.str.offsets[i];
            int64_t la = a->buf.str.offsets[i + 1] - sa;
            const char *s2;
            int64_t l2;
            if (b) {
                int64_t sb = b->buf.str.offsets[i];
                l2 = b->buf.str.offsets[i + 1] - sb;
                s2 = b->buf.str.data + sb;
            } else {
                s2 = pat;
                l2 = pat_len;
            }
            out->buf.dbl[i] = jaro_winkler_sim(a->buf.str.data + sa, la, s2, l2);
        }
        vec_array_free(a); free(a);
        if (b) { vec_array_free(b); free(b); }
        return out;
    }
    case EXPR_PASTE: {
        /* N-ary paste with optional separator */
        int64_t nc = expr->n_children;
        int64_t n = batch->n_rows;
        const char *sep = expr->paste_sep;
        int64_t sep_len = sep ? (int64_t)strlen(sep) : 0;

        /* Evaluate all children and coerce to string */
        VecArray **args = (VecArray **)malloc((size_t)nc * sizeof(VecArray *));
        for (int64_t c = 0; c < nc; c++) {
            VecArray *raw = vec_expr_eval(expr->children[c], batch);
            if (raw->type != VEC_STRING) {
                args[c] = vec_coerce(raw, VEC_STRING);
                vec_array_free(raw); free(raw);
            } else {
                args[c] = raw;
            }
        }

        /* Pass 1: compute total output length */
        int64_t total = 0;
        for (int64_t i = 0; i < n; i++) {
            int any_na = 0;
            for (int64_t c = 0; c < nc; c++) {
                if (!vec_array_is_valid(args[c], i)) { any_na = 1; break; }
            }
            if (any_na) continue;
            for (int64_t c = 0; c < nc; c++) {
                total += args[c]->buf.str.offsets[i+1] - args[c]->buf.str.offsets[i];
                if (c < nc - 1) total += sep_len;
            }
        }

        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_STRING, n);
        free(out->buf.str.data);
        out->buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
        out->buf.str.data_len = total;

        /* Pass 2: fill */
        int64_t off = 0;
        for (int64_t i = 0; i < n; i++) {
            out->buf.str.offsets[i] = off;
            int any_na = 0;
            for (int64_t c = 0; c < nc; c++) {
                if (!vec_array_is_valid(args[c], i)) { any_na = 1; break; }
            }
            if (any_na) { vec_array_set_null(out, i); continue; }
            vec_array_set_valid(out, i);
            for (int64_t c = 0; c < nc; c++) {
                int64_t s = args[c]->buf.str.offsets[i];
                int64_t l = args[c]->buf.str.offsets[i+1] - s;
                if (l > 0) memcpy(out->buf.str.data + off, args[c]->buf.str.data + s, (size_t)l);
                off += l;
                if (c < nc - 1 && sep_len > 0) {
                    memcpy(out->buf.str.data + off, sep, (size_t)sep_len);
                    off += sep_len;
                }
            }
        }
        out->buf.str.offsets[n] = off;
        for (int64_t c = 0; c < nc; c++) { vec_array_free(args[c]); free(args[c]); }
        free(args);
        return out;
    }
    case EXPR_STR_EXTRACT: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING) vectra_error("str_extract: argument must be string");
        const char *pattern = expr->lit_str;
        int64_t n = s->length;
        regex_t re;
        if (regcomp(&re, pattern, REG_EXTENDED) != 0)
            vectra_error("str_extract: invalid regex: %s", pattern);
        int has_groups = (re.re_nsub > 0);
        int nmatch = has_groups ? 2 : 1;
        regmatch_t *matches = (regmatch_t *)malloc((size_t)nmatch * sizeof(regmatch_t));
        /* Pass 1: compute output lengths (reusable buffer) */
        int64_t total = 0;
        int64_t *out_lens = (int64_t *)malloc((size_t)n * sizeof(int64_t));
        int64_t tb_cap = 256;
        char *tb = (char *)malloc((size_t)tb_cap);
        for (int64_t i = 0; i < n; i++) {
            out_lens[i] = -1;
            if (!vec_array_is_valid(s, i)) continue;
            int64_t so = s->buf.str.offsets[i];
            int64_t slen = s->buf.str.offsets[i+1] - so;
            if (slen + 1 > tb_cap) { tb_cap = slen + 1; tb = (char *)realloc(tb, (size_t)tb_cap); }
            memcpy(tb, s->buf.str.data + so, (size_t)slen);
            tb[slen] = '\0';
            if (regexec(&re, tb, (size_t)nmatch, matches, 0) == 0) {
                int mi = has_groups ? 1 : 0;
                if (matches[mi].rm_so >= 0) {
                    out_lens[i] = matches[mi].rm_eo - matches[mi].rm_so;
                    total += out_lens[i];
                }
            }
        }
        free(tb);
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_STRING, n);
        free(out->buf.str.data);
        out->buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
        out->buf.str.data_len = total;
        /* Pass 2: fill (reusable buffer) */
        int64_t off = 0;
        tb_cap = 256;
        tb = (char *)malloc((size_t)tb_cap);
        for (int64_t i = 0; i < n; i++) {
            out->buf.str.offsets[i] = off;
            if (!vec_array_is_valid(s, i) || out_lens[i] < 0) {
                vec_array_set_null(out, i); continue;
            }
            vec_array_set_valid(out, i);
            int64_t so = s->buf.str.offsets[i];
            int64_t slen = s->buf.str.offsets[i+1] - so;
            if (slen + 1 > tb_cap) { tb_cap = slen + 1; tb = (char *)realloc(tb, (size_t)tb_cap); }
            memcpy(tb, s->buf.str.data + so, (size_t)slen);
            tb[slen] = '\0';
            regexec(&re, tb, (size_t)nmatch, matches, 0);
            int mi = has_groups ? 1 : 0;
            int64_t mlen = matches[mi].rm_eo - matches[mi].rm_so;
            if (mlen > 0) memcpy(out->buf.str.data + off, tb + matches[mi].rm_so, (size_t)mlen);
            off += mlen;
        }
        free(tb);
        out->buf.str.offsets[n] = off;
        free(out_lens); free(matches);
        regfree(&re);
        vec_array_free(s); free(s);
        return out;
    }
    default:
        vectra_error("vec_expr_eval_string: unhandled op %d", (int)op);
        return NULL;
    }
}
