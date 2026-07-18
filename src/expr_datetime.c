#include "expr.h"
#include "array.h"
#include "scalar_ops.h"
#include "coerce.h"
#include "hash.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

/* Civil-date arithmetic (Howard Hinnant's public-domain algorithms), exact for
   the full range including negative (pre-1970) days. Used instead of the C
   library gmtime/timegm, which reject a negative time_t on Windows (gmtime_s
   returns EINVAL and leaves the struct at -1, silently producing year 1899). */
static void civil_from_days(int64_t z, int *yy, int *mm, int *dd) {
    z += 719468;
    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    int64_t doe = z - era * 146097;                 /* [0, 146096] */
    int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int64_t y   = yoe + era * 400;
    int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    int64_t mp  = (5 * doy + 2) / 153;              /* [0, 11] */
    int64_t d   = doy - (153 * mp + 2) / 5 + 1;     /* [1, 31] */
    int64_t m   = mp < 10 ? mp + 3 : mp - 9;        /* [1, 12] */
    *yy = (int)(y + (m <= 2));
    *mm = (int)m;
    *dd = (int)d;
}

static int64_t days_from_civil(int y, int m, int d) {
    int64_t yy  = y - (m <= 2);
    int64_t era = (yy >= 0 ? yy : yy - 399) / 400;
    int64_t yoe = yy - era * 400;
    int64_t doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

/* Split epoch seconds (possibly negative, UTC) into calendar fields. */
static void civil_from_secs(double secs, int *Y, int *Mo, int *D,
                            int *h, int *mi, int *s) {
    int64_t days = (int64_t)floor(secs / 86400.0);
    int64_t rem  = (int64_t)floor(secs - (double)days * 86400.0);  /* [0,86400) */
    if (rem < 0) { rem += 86400; days -= 1; }
    if (rem >= 86400) { rem -= 86400; days += 1; }
    civil_from_days(days, Y, Mo, D);
    *h  = (int)(rem / 3600);
    *mi = (int)((rem % 3600) / 60);
    *s  = (int)(rem % 60);
}

static int date_is_leap(int y) {
    return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
}

static int date_days_in_month(int y, int m) {
    static const int dim[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
    if (m == 2 && date_is_leap(y)) return 29;
    return dim[m - 1];
}

/* Decide whether a datetime operand's numeric value is in days (Date) or
   seconds (POSIXct). Prefer the parse-time scale resolved from the column's
   schema annotation; fall back to a magnitude guess only for a computed
   operand whose class was lost. */
static int datetime_is_date(char date_scale, double val) {
    if (date_scale == 'D') return 1;
    if (date_scale == 'T') return 0;
    return fabs(val) < 200000.0;
}

VecArray *vec_expr_eval_extended(VecExprKind op, const VecExpr *expr,
                                  const VecBatch *batch) {
    switch (op) {
    case EXPR_PMIN:
    case EXPR_PMAX: {
        VecArray *l = vec_expr_eval(expr->left, batch);
        VecArray *r = vec_expr_eval(expr->right, batch);
        /* Coerce both to double */
        if (l->type == VEC_INT64) { VecArray *t = vec_coerce(l, VEC_DOUBLE); vec_array_free(l); free(l); l = t; }
        if (r->type == VEC_INT64) { VecArray *t = vec_coerce(r, VEC_DOUBLE); vec_array_free(r); free(r); r = t; }
        int64_t n = l->length;
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_DOUBLE, n);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(l, i) || !vec_array_is_valid(r, i)) { vec_array_set_null(out, i); continue; }
            vec_array_set_valid(out, i);
            double lv = l->buf.dbl[i], rv = r->buf.dbl[i];
            out->buf.dbl[i] = (op == EXPR_PMIN) ? (lv < rv ? lv : rv) : (lv > rv ? lv : rv);
        }
        vec_array_free(l); free(l);
        vec_array_free(r); free(r);
        return out;
    }
    case EXPR_DATE_PART: {
        VecArray *o = vec_expr_eval(expr->operand, batch);
        /* Coerce to double if needed */
        if (o->type == VEC_INT64) {
            VecArray *t = vec_coerce(o, VEC_DOUBLE);
            vec_array_free(o); free(o); o = t;
        }
        int64_t n = o->length;
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_DOUBLE, n);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(o, i)) { vec_array_set_null(out, i); continue; }
            vec_array_set_valid(out, i);
            double val = o->buf.dbl[i];
            double secs = datetime_is_date(expr->date_scale, val)
                          ? val * 86400.0 : val;
            int Y, Mo, D, hh, mi, ss;
            civil_from_secs(secs, &Y, &Mo, &D, &hh, &mi, &ss);
            switch (expr->date_part) {
            case 'Y': out->buf.dbl[i] = (double)Y;  break;
            case 'M': out->buf.dbl[i] = (double)Mo; break;
            case 'D': out->buf.dbl[i] = (double)D;  break;
            case 'h': out->buf.dbl[i] = (double)hh; break;
            case 'm': out->buf.dbl[i] = (double)mi; break;
            case 's': out->buf.dbl[i] = (double)ss; break;
            default: vectra_error("unknown date part: %c", expr->date_part);
            }
        }
        vec_array_free(o); free(o);
        return out;
    }
    case EXPR_AS_DATE: {
        VecArray *s = vec_expr_eval(expr->operand, batch);
        if (s->type != VEC_STRING) vectra_error("as.Date: argument must be string");
        int64_t n = s->length;
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_DOUBLE, n);
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(s, i)) { vec_array_set_null(out, i); continue; }
            int64_t so = s->buf.str.offsets[i];
            int64_t slen = s->buf.str.offsets[i + 1] - so;
            /* Parse YYYY-MM-DD format */
            if (slen < 10) { vec_array_set_null(out, i); continue; }
            const char *p = s->buf.str.data + so;
            /* Require the exact YYYY-MM-DD shape with digit fields and '-'
               separators; validate the day against the actual month length so
               "2021-02-30" is rejected (returns NA, as base R does) rather than
               normalized to a bogus date. */
            int ok = (p[4] == '-' && p[7] == '-');
            for (int j = 0; ok && j < 10; j++)
                if (j != 4 && j != 7 && (p[j] < '0' || p[j] > '9')) ok = 0;
            if (!ok) { vec_array_set_null(out, i); continue; }
            int year = (p[0]-'0')*1000 + (p[1]-'0')*100 + (p[2]-'0')*10 + (p[3]-'0');
            int mon  = (p[5]-'0')*10 + (p[6]-'0');
            int mday = (p[8]-'0')*10 + (p[9]-'0');
            if (mon < 1 || mon > 12 || mday < 1 ||
                mday > date_days_in_month(year, mon)) {
                vec_array_set_null(out, i);
                continue;
            }
            vec_array_set_valid(out, i);
            out->buf.dbl[i] = (double)days_from_civil(year, mon, mday);
        }
        vec_array_free(s); free(s);
        return out;
    }
    case EXPR_FLOOR_TIME: {
        /* Truncate an epoch value to a calendar grid. The input is a Date
           (days since epoch) or POSIXct (seconds since epoch), detected by
           magnitude like EXPR_DATE_PART; the result is returned in the same
           scale so it stays a valid Date / POSIXct value. */
        VecArray *o = vec_expr_eval(expr->operand, batch);
        if (o->type == VEC_INT64) {
            VecArray *t = vec_coerce(o, VEC_DOUBLE);
            vec_array_free(o); free(o); o = t;
        }
        int64_t n = expr->lit_i64;
        if (n < 1) n = 1;
        char unit = expr->date_part;

        /* seconds in a fixed-width unit (0 for calendar units) */
        double unit_secs = 0.0;
        switch (unit) {
        case 's': unit_secs = 1.0; break;
        case 'n': unit_secs = 60.0; break;
        case 'h': unit_secs = 3600.0; break;
        case 'd': unit_secs = 86400.0; break;
        case 'w': unit_secs = 604800.0; break;
        case 'M': case 'q': case 'y': break;  /* calendar */
        default: vectra_error("floor_time: unknown unit '%c'", unit);
        }

        int64_t len = o->length;
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_DOUBLE, len);

        for (int64_t i = 0; i < len; i++) {
            if (!vec_array_is_valid(o, i)) { vec_array_set_null(out, i); continue; }
            vec_array_set_valid(out, i);
            double val = o->buf.dbl[i];
            int is_date = datetime_is_date(expr->date_scale, val);
            double secs = is_date ? val * 86400.0 : val;
            double floored;

            if (unit_secs > 0.0) {
                double bucket = (double)n * unit_secs;
                floored = floor(secs / bucket) * bucket;
            } else {
                int Y, Mo, D, hh, mi, ss;
                civil_from_secs(secs, &Y, &Mo, &D, &hh, &mi, &ss);
                if (unit == 'y') {
                    int fy = (int)floor((double)(Y - 1970) / (double)n) * n + 1970;
                    Y = fy; Mo = 1; D = 1;
                } else if (unit == 'q') {
                    Mo = ((Mo - 1) / 3) * 3 + 1; D = 1;
                } else { /* month */
                    int total = Y * 12 + (Mo - 1);
                    int fm = (int)floor((double)total / (double)n) * n;
                    Y = fm / 12; Mo = fm % 12 + 1; D = 1;
                }
                floored = (double)days_from_civil(Y, Mo, D) * 86400.0;
            }

            out->buf.dbl[i] = is_date ? floored / 86400.0 : floored;
        }
        vec_array_free(o); free(o);
        return out;
    }
    case EXPR_IF_ELSE: {
        VecArray *cond = vec_expr_eval(expr->cond, batch);
        VecArray *then_v = vec_expr_eval(expr->then_expr, batch);
        VecArray *else_v = vec_expr_eval(expr->else_expr, batch);
        /* Coerce then/else to common type (string > double > int64 > bool) */
        VecType common = (then_v->type == VEC_STRING || else_v->type == VEC_STRING) ? VEC_STRING :
                         (then_v->type == VEC_DOUBLE || else_v->type == VEC_DOUBLE) ? VEC_DOUBLE : then_v->type;
        if (then_v->type != common) { VecArray *t2 = vec_coerce(then_v, common); vec_array_free(then_v); free(then_v); then_v = t2; }
        if (else_v->type != common) { VecArray *e2 = vec_coerce(else_v, common); vec_array_free(else_v); free(else_v); else_v = e2; }
        int64_t n = cond->length;
        VecArray *out;
        if (common == VEC_STRING) {
            /* String if_else: compute total length first */
            int64_t total = 0;
            for (int64_t i = 0; i < n; i++) {
                if (!vec_array_is_valid(cond, i)) continue;
                const VecArray *src = cond->buf.bln[i] ? then_v : else_v;
                if (vec_array_is_valid(src, i))
                    total += src->buf.str.offsets[i + 1] - src->buf.str.offsets[i];
            }
            out = (VecArray *)malloc(sizeof(VecArray));
            *out = vec_array_alloc(VEC_STRING, n);
            free(out->buf.str.data);
            out->buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
            out->buf.str.data_len = total;
            int64_t off = 0;
            for (int64_t i = 0; i < n; i++) {
                out->buf.str.offsets[i] = off;
                if (!vec_array_is_valid(cond, i)) { vec_array_set_null(out, i); continue; }
                const VecArray *src = cond->buf.bln[i] ? then_v : else_v;
                if (!vec_array_is_valid(src, i)) { vec_array_set_null(out, i); continue; }
                vec_array_set_valid(out, i);
                int64_t s = src->buf.str.offsets[i], e = src->buf.str.offsets[i + 1];
                int64_t slen = e - s;
                if (slen > 0) memcpy(out->buf.str.data + off, src->buf.str.data + s, (size_t)slen);
                off += slen;
            }
            out->buf.str.offsets[n] = off;
        } else {
            out = (VecArray *)malloc(sizeof(VecArray));
            *out = vec_array_alloc(common, n);
            for (int64_t i = 0; i < n; i++) {
                if (!vec_array_is_valid(cond, i)) { vec_array_set_null(out, i); continue; }
                const VecArray *src = cond->buf.bln[i] ? then_v : else_v;
                if (!vec_array_is_valid(src, i)) { vec_array_set_null(out, i); continue; }
                vec_array_set_valid(out, i);
                switch (common) {
                case VEC_DOUBLE: out->buf.dbl[i] = src->buf.dbl[i]; break;
                case VEC_INT64:  out->buf.i64[i] = src->buf.i64[i]; break;
                case VEC_BOOL:   out->buf.bln[i] = src->buf.bln[i]; break;
                default: break;
                }
            }
        }
        vec_array_free(cond); free(cond);
        vec_array_free(then_v); free(then_v);
        vec_array_free(else_v); free(else_v);
        return out;
    }
    case EXPR_RESOLVE: {
        /* resolve(fk_col, pk_col, value_col)
           Build hash map from pk_col values -> row indices,
           then for each row look up fk_col value to get the matched row's value_col. */
        VecArray *fk = vec_expr_eval(expr->operand, batch);
        VecArray *pk = vec_expr_eval(expr->left, batch);
        VecArray *val = vec_expr_eval(expr->right, batch);
        /* Coerce the two key columns to a common type so hashing and equality
           compare like with like. A differing fk/pk type would otherwise read
           one type's bytes through the other and could silently false-match. */
        if (fk->type != pk->type) {
            VecType kt = vec_common_type(fk->type, pk->type);
            if (fk->type != kt) { VecArray *t = vec_coerce(fk, kt); vec_array_free(fk); free(fk); fk = t; }
            if (pk->type != kt) { VecArray *t = vec_coerce(pk, kt); vec_array_free(pk); free(pk); pk = t; }
        }
        int64_t n = fk->length;

        /* Build hash map: pk_value -> row index.
           Open-addressing hash table, stores row index per slot. */
        int64_t ht_cap = 1;
        while (ht_cap < n * 2) ht_cap *= 2;  /* load factor < 50% */
        int64_t *ht_rows = (int64_t *)malloc((size_t)ht_cap * sizeof(int64_t));
        uint64_t *ht_hashes = (uint64_t *)malloc((size_t)ht_cap * sizeof(uint64_t));
        memset(ht_rows, -1, (size_t)ht_cap * sizeof(int64_t));

        /* Insert pk values */
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(pk, i)) continue;
            uint64_t h = vec_hash_value(pk, i);
            int64_t mask = ht_cap - 1;
            int64_t idx = (int64_t)(h & (uint64_t)mask);
            for (;;) {
                if (ht_rows[idx] == -1) {
                    ht_rows[idx] = i;
                    ht_hashes[idx] = h;
                    break;
                }
                /* On collision with same hash, check equality; first match wins */
                if (ht_hashes[idx] == h && vec_val_equal(pk, i, pk, ht_rows[idx])) break;
                idx = (idx + 1) & mask;
            }
        }

        /* Produce output by looking up each fk value */
        VecArray *out;
        if (val->type == VEC_STRING) {
            /* String output: two passes */
            int64_t total = 0;
            for (int64_t i = 0; i < n; i++) {
                if (!vec_array_is_valid(fk, i)) continue;
                uint64_t h = vec_hash_value(fk, i);
                int64_t mask = ht_cap - 1;
                int64_t idx = (int64_t)(h & (uint64_t)mask);
                int64_t matched = -1;
                while (ht_rows[idx] != -1) {
                    if (ht_hashes[idx] == h && vec_val_equal(fk, i, pk, ht_rows[idx])) {
                        matched = ht_rows[idx];
                        break;
                    }
                    idx = (idx + 1) & mask;
                }
                if (matched >= 0 && vec_array_is_valid(val, matched))
                    total += val->buf.str.offsets[matched + 1] - val->buf.str.offsets[matched];
            }
            out = (VecArray *)malloc(sizeof(VecArray));
            *out = vec_array_alloc(VEC_STRING, n);
            free(out->buf.str.data);
            out->buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
            out->buf.str.data_len = total;
            int64_t off = 0;
            for (int64_t i = 0; i < n; i++) {
                out->buf.str.offsets[i] = off;
                if (!vec_array_is_valid(fk, i)) { vec_array_set_null(out, i); continue; }
                uint64_t h = vec_hash_value(fk, i);
                int64_t mask = ht_cap - 1;
                int64_t idx = (int64_t)(h & (uint64_t)mask);
                int64_t matched = -1;
                while (ht_rows[idx] != -1) {
                    if (ht_hashes[idx] == h && vec_val_equal(fk, i, pk, ht_rows[idx])) {
                        matched = ht_rows[idx];
                        break;
                    }
                    idx = (idx + 1) & mask;
                }
                if (matched < 0 || !vec_array_is_valid(val, matched)) {
                    vec_array_set_null(out, i);
                } else {
                    vec_array_set_valid(out, i);
                    int64_t vs = val->buf.str.offsets[matched];
                    int64_t vlen = val->buf.str.offsets[matched + 1] - vs;
                    if (vlen > 0) memcpy(out->buf.str.data + off, val->buf.str.data + vs, (size_t)vlen);
                    off += vlen;
                }
            }
            out->buf.str.offsets[n] = off;
        } else {
            /* Non-string output: single pass */
            out = (VecArray *)malloc(sizeof(VecArray));
            *out = vec_array_alloc(val->type, n);
            for (int64_t i = 0; i < n; i++) {
                if (!vec_array_is_valid(fk, i)) { vec_array_set_null(out, i); continue; }
                uint64_t h = vec_hash_value(fk, i);
                int64_t mask = ht_cap - 1;
                int64_t idx = (int64_t)(h & (uint64_t)mask);
                int64_t matched = -1;
                while (ht_rows[idx] != -1) {
                    if (ht_hashes[idx] == h && vec_val_equal(fk, i, pk, ht_rows[idx])) {
                        matched = ht_rows[idx];
                        break;
                    }
                    idx = (idx + 1) & mask;
                }
                if (matched < 0 || !vec_array_is_valid(val, matched)) {
                    vec_array_set_null(out, i);
                } else {
                    vec_array_set_valid(out, i);
                    switch (val->type) {
                    case VEC_INT64:  out->buf.i64[i] = val->buf.i64[matched]; break;
                    case VEC_DOUBLE: out->buf.dbl[i] = val->buf.dbl[matched]; break;
                    case VEC_BOOL:   out->buf.bln[i] = val->buf.bln[matched]; break;
                    default: break;
                    }
                }
            }
        }

        free(ht_rows);
        free(ht_hashes);
        vec_array_free(fk); free(fk);
        vec_array_free(pk); free(pk);
        vec_array_free(val); free(val);
        return out;
    }
    case EXPR_PROPAGATE: {
        /* propagate(parent_fk, pk, seed_expr)
           Evaluate seed_expr to get initial values, build pk->row_index map,
           then iteratively fill NAs by looking up parent row's value. */
        VecArray *parent_fk = vec_expr_eval(expr->operand, batch);
        VecArray *pk = vec_expr_eval(expr->left, batch);
        VecArray *seed = vec_expr_eval(expr->right, batch);
        /* Coerce the two key columns to a common type (see resolve) so a
           differing fk/pk type cannot silently false-match via a byte reinterpret. */
        if (parent_fk->type != pk->type) {
            VecType kt = vec_common_type(parent_fk->type, pk->type);
            if (parent_fk->type != kt) { VecArray *t = vec_coerce(parent_fk, kt); vec_array_free(parent_fk); free(parent_fk); parent_fk = t; }
            if (pk->type != kt) { VecArray *t = vec_coerce(pk, kt); vec_array_free(pk); free(pk); pk = t; }
        }
        int64_t n = parent_fk->length;

        /* Build hash map: pk_value -> row index */
        int64_t ht_cap = 1;
        while (ht_cap < n * 2) ht_cap *= 2;
        int64_t *ht_rows = (int64_t *)malloc((size_t)ht_cap * sizeof(int64_t));
        uint64_t *ht_hashes = (uint64_t *)malloc((size_t)ht_cap * sizeof(uint64_t));
        memset(ht_rows, -1, (size_t)ht_cap * sizeof(int64_t));

        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(pk, i)) continue;
            uint64_t h = vec_hash_value(pk, i);
            int64_t mask = ht_cap - 1;
            int64_t idx = (int64_t)(h & (uint64_t)mask);
            for (;;) {
                if (ht_rows[idx] == -1) {
                    ht_rows[idx] = i;
                    ht_hashes[idx] = h;
                    break;
                }
                if (ht_hashes[idx] == h && vec_val_equal(pk, i, pk, ht_rows[idx])) break;
                idx = (idx + 1) & mask;
            }
        }

        /* Helper to look up a row by fk value */
        #define PROPAGATE_LOOKUP(fk_arr, row) do { \
            matched = -1; \
            if (vec_array_is_valid(fk_arr, row)) { \
                uint64_t h = vec_hash_value(fk_arr, row); \
                int64_t mask = ht_cap - 1; \
                int64_t idx = (int64_t)(h & (uint64_t)mask); \
                while (ht_rows[idx] != -1) { \
                    if (ht_hashes[idx] == h && vec_val_equal(fk_arr, row, pk, ht_rows[idx])) { \
                        matched = ht_rows[idx]; \
                        break; \
                    } \
                    idx = (idx + 1) & mask; \
                } \
            } \
        } while (0)

        /* Iteratively propagate: copy value from parent row to child row.
           The result IS the seed array, modified in-place. Each pass resolves
           one more level of the hierarchy and a resolved row is never cleared,
           so progress is monotonic: the loop converges (breaks on !changed) in
           at most the chain depth, bounded above by n. Capping at n resolves an
           arbitrarily deep chain within a batch; a cycle simply stops making
           progress and breaks. */
        int64_t max_iter = n;
        for (int64_t iter = 0; iter < max_iter; iter++) {
            int changed = 0;
            if (seed->type == VEC_STRING) {
                /* String propagation: need to rebuild buffer each iteration.
                   Collect values to copy, then rebuild. */
                /* First: count how many NAs can be resolved */
                int64_t n_resolve = 0;
                for (int64_t i = 0; i < n; i++) {
                    if (vec_array_is_valid(seed, i)) continue;
                    int64_t matched;
                    PROPAGATE_LOOKUP(parent_fk, i);
                    if (matched >= 0 && vec_array_is_valid(seed, matched))
                        n_resolve++;
                }
                if (n_resolve == 0) break;

                /* Build new string array with resolved values */
                int64_t total = 0;
                for (int64_t i = 0; i < n; i++) {
                    if (vec_array_is_valid(seed, i)) {
                        total += seed->buf.str.offsets[i + 1] - seed->buf.str.offsets[i];
                    } else {
                        int64_t matched;
                        PROPAGATE_LOOKUP(parent_fk, i);
                        if (matched >= 0 && vec_array_is_valid(seed, matched))
                            total += seed->buf.str.offsets[matched + 1] - seed->buf.str.offsets[matched];
                    }
                }
                VecArray new_arr = vec_array_alloc(VEC_STRING, n);
                free(new_arr.buf.str.data);
                new_arr.buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
                new_arr.buf.str.data_len = total;
                int64_t off = 0;
                for (int64_t i = 0; i < n; i++) {
                    new_arr.buf.str.offsets[i] = off;
                    if (vec_array_is_valid(seed, i)) {
                        vec_array_set_valid(&new_arr, i);
                        int64_t s = seed->buf.str.offsets[i];
                        int64_t slen = seed->buf.str.offsets[i + 1] - s;
                        if (slen > 0) memcpy(new_arr.buf.str.data + off, seed->buf.str.data + s, (size_t)slen);
                        off += slen;
                    } else {
                        int64_t matched;
                        PROPAGATE_LOOKUP(parent_fk, i);
                        if (matched >= 0 && vec_array_is_valid(seed, matched)) {
                            vec_array_set_valid(&new_arr, i);
                            int64_t s = seed->buf.str.offsets[matched];
                            int64_t slen = seed->buf.str.offsets[matched + 1] - s;
                            if (slen > 0) memcpy(new_arr.buf.str.data + off, seed->buf.str.data + s, (size_t)slen);
                            off += slen;
                            changed = 1;
                        } else {
                            vec_array_set_null(&new_arr, i);
                        }
                    }
                }
                new_arr.buf.str.offsets[n] = off;
                vec_array_free(seed);
                *seed = new_arr;
            } else {
                /* Non-string types: propagate in-place */
                for (int64_t i = 0; i < n; i++) {
                    if (vec_array_is_valid(seed, i)) continue;
                    int64_t matched;
                    PROPAGATE_LOOKUP(parent_fk, i);
                    if (matched < 0 || !vec_array_is_valid(seed, matched)) continue;
                    vec_array_set_valid(seed, i);
                    switch (seed->type) {
                    case VEC_INT64:  seed->buf.i64[i] = seed->buf.i64[matched]; break;
                    case VEC_DOUBLE: seed->buf.dbl[i] = seed->buf.dbl[matched]; break;
                    case VEC_BOOL:   seed->buf.bln[i] = seed->buf.bln[matched]; break;
                    default: break;
                    }
                    changed = 1;
                }
            }
            if (!changed) break;
        }
        #undef PROPAGATE_LOOKUP

        free(ht_rows);
        free(ht_hashes);
        vec_array_free(parent_fk); free(parent_fk);
        vec_array_free(pk); free(pk);

        /* seed is the result — transfer it to a heap-allocated VecArray */
        VecArray *out = (VecArray *)malloc(sizeof(VecArray));
        *out = *seed;
        free(seed);
        return out;
    }
    default:
        vectra_error("vec_expr_eval_extended: unhandled op %d", (int)op);
        return NULL;
    }
}
