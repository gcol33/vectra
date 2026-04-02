#include "expr.h"
#include "array.h"
#include "scalar_ops.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/* Wagner-Fischer Levenshtein distance with single-row buffer.
   If max_dist >= 0, returns max_dist + 1 as soon as the minimum
   possible distance exceeds the bound (early termination). */
static int64_t levenshtein_distance(const char *s, int64_t len_s,
                                     const char *t, int64_t len_t,
                                     int64_t max_dist) {
    if (len_s == 0) return len_t;
    if (len_t == 0) return len_s;

    /* Use shorter string as column to minimize memory */
    if (len_s > len_t) {
        const char *tmp_s = s; s = t; t = tmp_s;
        int64_t tmp_l = len_s; len_s = len_t; len_t = tmp_l;
    }

    /* Quick lower-bound check: length difference alone exceeds max_dist */
    if (max_dist >= 0 && (len_t - len_s) > max_dist)
        return max_dist + 1;

    int64_t *prev = (int64_t *)malloc((size_t)(len_s + 1) * sizeof(int64_t));
    for (int64_t i = 0; i <= len_s; i++) prev[i] = i;

    for (int64_t j = 1; j <= len_t; j++) {
        int64_t prev_diag = prev[0];
        prev[0] = j;
        int64_t row_min = prev[0];
        for (int64_t i = 1; i <= len_s; i++) {
            int64_t cost = (s[i-1] == t[j-1]) ? 0 : 1;
            int64_t val = prev[i-1] + 1;               /* delete */
            if (prev[i] + 1 < val) val = prev[i] + 1;  /* insert */
            int64_t diag = prev_diag + cost;             /* substitute */
            if (diag < val) val = diag;
            prev_diag = prev[i];
            prev[i] = val;
            if (val < row_min) row_min = val;
        }
        /* Early termination: if every cell in this row exceeds max_dist,
           the final result can only grow. */
        if (max_dist >= 0 && row_min > max_dist) {
            free(prev);
            return max_dist + 1;
        }
    }
    int64_t result = prev[len_s];
    free(prev);
    return result;
}

/* Optimal String Alignment (restricted Damerau-Levenshtein) distance.
   Adds transposition as a primitive operation (cost 1) on top of
   insert/delete/substitute. Uses two-row buffer: O(min(m,n)) space.
   Supports optional max_dist early termination. */
static int64_t dl_distance(const char *s, int64_t len_s,
                            const char *t, int64_t len_t,
                            int64_t max_dist) {
    if (len_s == 0) return len_t;
    if (len_t == 0) return len_s;

    /* Use shorter string as column */
    if (len_s > len_t) {
        const char *tmp_s = s; s = t; t = tmp_s;
        int64_t tmp_l = len_s; len_s = len_t; len_t = tmp_l;
    }

    if (max_dist >= 0 && (len_t - len_s) > max_dist)
        return max_dist + 1;

    /* Need two previous rows for transposition check */
    int64_t *prev2 = (int64_t *)malloc((size_t)(len_s + 1) * sizeof(int64_t));
    int64_t *prev  = (int64_t *)malloc((size_t)(len_s + 1) * sizeof(int64_t));
    int64_t *curr  = (int64_t *)malloc((size_t)(len_s + 1) * sizeof(int64_t));

    for (int64_t i = 0; i <= len_s; i++) prev[i] = i;

    for (int64_t j = 1; j <= len_t; j++) {
        curr[0] = j;
        int64_t row_min = curr[0];
        for (int64_t i = 1; i <= len_s; i++) {
            int64_t cost = (s[i-1] == t[j-1]) ? 0 : 1;
            int64_t val = prev[i-1] + cost;                 /* substitute */
            if (prev[i] + 1 < val) val = prev[i] + 1;      /* insert */
            if (curr[i-1] + 1 < val) val = curr[i-1] + 1;  /* delete */
            /* Transposition: swap of two adjacent characters */
            if (i > 1 && j > 1 && s[i-1] == t[j-2] && s[i-2] == t[j-1]) {
                int64_t trans = prev2[i-2] + cost;
                if (trans < val) val = trans;
            }
            curr[i] = val;
            if (val < row_min) row_min = val;
        }
        if (max_dist >= 0 && row_min > max_dist) {
            free(prev2); free(prev); free(curr);
            return max_dist + 1;
        }
        /* Rotate rows: prev2 <- prev, prev <- curr, curr <- prev2 */
        int64_t *tmp = prev2;
        prev2 = prev;
        prev = curr;
        curr = tmp;
    }
    int64_t result = prev[len_s];
    free(prev2); free(prev); free(curr);
    return result;
}

/* Jaro-Winkler similarity score (0.0 = completely different, 1.0 = identical).
   Jaro base + Winkler prefix bonus (up to 4 chars, p = 0.1). */
static double jaro_winkler_sim(const char *s, int64_t len_s,
                                const char *t, int64_t len_t) {
    if (len_s == 0 && len_t == 0) return 1.0;
    if (len_s == 0 || len_t == 0) return 0.0;

    int64_t match_window = (len_s > len_t ? len_s : len_t) / 2 - 1;
    if (match_window < 0) match_window = 0;

    /* Stack-allocate flags for typical name lengths, heap for long strings */
    uint8_t s_stack[256], t_stack[256];
    uint8_t *s_matched, *t_matched;
    int heap_alloc = 0;
    if (len_s <= 256 && len_t <= 256) {
        s_matched = s_stack;
        t_matched = t_stack;
    } else {
        s_matched = (uint8_t *)malloc((size_t)len_s);
        t_matched = (uint8_t *)malloc((size_t)len_t);
        heap_alloc = 1;
    }
    memset(s_matched, 0, (size_t)len_s);
    memset(t_matched, 0, (size_t)len_t);

    int64_t matches = 0;
    int64_t transpositions = 0;

    /* Count matches */
    for (int64_t i = 0; i < len_s; i++) {
        int64_t lo = (i - match_window > 0) ? (i - match_window) : 0;
        int64_t hi = (i + match_window + 1 < len_t) ? (i + match_window + 1) : len_t;
        for (int64_t j = lo; j < hi; j++) {
            if (!t_matched[j] && s[i] == t[j]) {
                s_matched[i] = 1;
                t_matched[j] = 1;
                matches++;
                break;
            }
        }
    }

    if (matches == 0) {
        if (heap_alloc) { free(s_matched); free(t_matched); }
        return 0.0;
    }

    /* Count transpositions */
    int64_t k = 0;
    for (int64_t i = 0; i < len_s; i++) {
        if (!s_matched[i]) continue;
        while (!t_matched[k]) k++;
        if (s[i] != t[k]) transpositions++;
        k++;
    }

    if (heap_alloc) { free(s_matched); free(t_matched); }

    double m = (double)matches;
    double jaro = (m / (double)len_s + m / (double)len_t +
                   (m - (double)(transpositions / 2)) / m) / 3.0;

    /* Winkler prefix bonus: up to 4 shared prefix chars, p = 0.1 */
    int64_t prefix = 0;
    int64_t max_prefix = (len_s < len_t ? len_s : len_t);
    if (max_prefix > 4) max_prefix = 4;
    for (int64_t i = 0; i < max_prefix; i++) {
        if (s[i] == t[i]) prefix++;
        else break;
    }

    return jaro + (double)prefix * 0.1 * (1.0 - jaro);
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

        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(s, i)) {
                vec_array_set_null(out, i);
                continue;
            }
            vec_array_set_valid(out, i);
            int64_t so = s->buf.str.offsets[i];
            int64_t slen = s->buf.str.offsets[i + 1] - so;
            int found = 0;
            if (pat_len <= slen) {
                for (int64_t j = 0; j <= slen - pat_len; j++) {
                    if (memcmp(s->buf.str.data + so + j, pattern,
                               (size_t)pat_len) == 0) {
                        found = 1;
                        break;
                    }
                }
            }
            out->buf.bln[i] = (uint8_t)found;
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
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_BOOL, n);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(o, i)) { vec_array_set_null(out, i); continue; }
            vec_array_set_valid(out, i);
            int found = 0;
            if (o->type == VEC_DOUBLE) {
                double v = o->buf.dbl[i];
                for (int64_t j = 0; j < expr->n_set; j++) {
                    if (v == expr->set_dbl[j]) { found = 1; break; }
                }
            } else if (o->type == VEC_INT64) {
                int64_t v = o->buf.i64[i];
                for (int64_t j = 0; j < expr->n_set; j++) {
                    if (v == expr->set_i64[j]) { found = 1; break; }
                }
            } else if (o->type == VEC_STRING) {
                int64_t so = o->buf.str.offsets[i], eo = o->buf.str.offsets[i + 1];
                int64_t slen = eo - so;
                for (int64_t j = 0; j < expr->n_set; j++) {
                    int64_t clen = (int64_t)strlen(expr->set_str[j]);
                    if (slen == clen && memcmp(o->buf.str.data + so, expr->set_str[j], (size_t)slen) == 0) {
                        found = 1; break;
                    }
                }
            }
            out->buf.bln[i] = (uint8_t)found;
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
        int64_t plen = (int64_t)strlen(pat);
        int64_t rlen = (int64_t)strlen(rep);
        int64_t n = s->length;
        int only_first = (op == EXPR_SUB);
        /* First pass: compute output sizes */
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
            /* Copy remaining chars after last match check */
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
    default:
        vectra_error("vec_expr_eval_string: unhandled op %d", (int)op);
        return NULL;
    }
}
