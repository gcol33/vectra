#ifndef VECTRA_STRING_DISTANCE_H
#define VECTRA_STRING_DISTANCE_H

/*
 * Shared string distance/similarity functions used by both the expression
 * evaluator (expr_string.c) and fuzzy join (fuzzy_join.c).
 *
 * All functions are static inline so each translation unit gets its own
 * copy without linker symbol conflicts, while the compiler can inline
 * at the call site.
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/*  Levenshtein distance                                               */
/* ------------------------------------------------------------------ */

/* Classic Levenshtein edit distance (insert/delete/substitute).
   Uses single-row buffer: O(min(m,n)) space.
   max_dist < 0 disables early termination; otherwise returns
   max_dist + 1 when the true distance is known to exceed max_dist. */
static inline int64_t strdist_levenshtein(const char *s, int64_t len_s,
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
    if (!prev) return max_dist + 1;
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

/* ------------------------------------------------------------------ */
/*  Optimal String Alignment (restricted Damerau-Levenshtein) distance */
/* ------------------------------------------------------------------ */

/* Adds transposition as a primitive operation (cost 1) on top of
   insert/delete/substitute. Uses three-row buffer: O(min(m,n)) space.
   max_dist < 0 disables early termination. */
static inline int64_t strdist_dl(const char *s, int64_t len_s,
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
    if (!prev2 || !prev || !curr) {
        free(prev2); free(prev); free(curr);
        return max_dist + 1;
    }

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

/* ------------------------------------------------------------------ */
/*  Jaro-Winkler similarity                                            */
/* ------------------------------------------------------------------ */

/* Jaro-Winkler similarity score (0.0 = completely different, 1.0 = identical).
   Jaro base + Winkler prefix bonus (up to 4 chars, p = 0.1). */
static inline double strdist_jaro_winkler(const char *s, int64_t len_s,
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

#endif /* VECTRA_STRING_DISTANCE_H */
