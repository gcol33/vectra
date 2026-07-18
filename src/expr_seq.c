/* Scalar biological-sequence expressions: seq_* ops inside
 * mutate()/filter()/summarise().
 *
 * A DNA / RNA / protein sequence rides through the engine as an ordinary ASCII
 * string in a VEC_STRING column -- the same "self-describing blob per cell"
 * shape geometry (hex-WKB) and embeddings (hex float32) use. These expressions
 * decode that column one row at a time and compute a measure, a transformed
 * sequence, or an edit distance straight off it, with no round-trip through
 * Biostrings. They slot into the evaluator exactly like the string and geometry
 * ops: vec_expr_eval dispatches EXPR_SEQ here, the seq_fn discriminator picks
 * the operation, and the result is a plain VecArray (int64, double, or string)
 * that flows on through the rest of the pipeline.
 *
 *   measures (seq -> number)   seq_length ('L' -> int64),
 *                              seq_gc ('G' -> double GC fraction)
 *   transforms (seq -> seq)    seq_revcomp ('r'), seq_complement ('c'),
 *                              seq_reverse ('v'), seq_transcribe ('t' DNA<->RNA),
 *                              seq_translate ('p' -> protein),
 *                              seq_subseq ('s', start + width)
 *   distance (seq, seq -> int) seq_dist ('d' -> int64 edit distance)
 *
 * Complement uses the DNA alphabet (A<->T) with the IUPAC ambiguity codes
 * mapped to their complements; a U in the input is treated as an A-partner and
 * complemented to A (transcribe first for an RNA-alphabet complement).
 * Translation uses the standard genetic code (NCBI transl_table 1); a codon
 * carrying any non-ACGTU base yields 'X', a stop codon '*'.
 *
 * Per-cell contract (matching st_* / embeddings): a missing (NA) cell yields NA;
 * a non-sequence or unexpected character is processed as-is (passed through by
 * complement, translated to 'X'), never raised as an error. A whole column of
 * the wrong *type* is a usage error.
 *
 * Threading: the heavy per-character fill is parallelized over rows with the
 * standard vec_omp discipline (never #include <omp.h>). Output offsets are
 * assigned serially first, so each row writes into a disjoint slice of the
 * output buffer and the parallel fill is race-free.
 */

#include "expr.h"
#include "array.h"
#include "error.h"
#include "string_distance.h"
#include "seq_util.h"
#include "vec_omp.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* Below this row count the parallel fill is not worth the thread setup. */
#define SEQ_PAR_THRESHOLD 1024
/* Multiple of 8 so per-thread output-validity bytes never overlap. Validity is
   written serially in pass 1, but keeping the chunk byte-aligned is harmless
   and matches the other expr kernels. */
#define SEQ_CHUNK 64

static inline const char *seq_ptr(const VecArray *a, int64_t row) {
    return a->buf.str.data + a->buf.str.offsets[row];
}
static inline int64_t seq_str_len(const VecArray *a, int64_t row) {
    return a->buf.str.offsets[row + 1] - a->buf.str.offsets[row];
}

/* DNA complement with IUPAC ambiguity codes, case preserved. U is treated as an
   A-partner (complemented to A); gaps and unknown characters pass through. */
static inline char seq_comp_base(char b) {
    switch (b) {
    case 'A': return 'T'; case 'a': return 't';
    case 'T': return 'A'; case 't': return 'a';
    case 'U': return 'A'; case 'u': return 'a';
    case 'G': return 'C'; case 'g': return 'c';
    case 'C': return 'G'; case 'c': return 'g';
    case 'R': return 'Y'; case 'r': return 'y';  /* A|G  <-> C|T */
    case 'Y': return 'R'; case 'y': return 'r';
    case 'S': return 'S'; case 's': return 's';  /* G|C  self */
    case 'W': return 'W'; case 'w': return 'w';  /* A|T  self */
    case 'K': return 'M'; case 'k': return 'm';  /* G|T  <-> A|C */
    case 'M': return 'K'; case 'm': return 'k';
    case 'B': return 'V'; case 'b': return 'v';  /* C|G|T <-> A|C|G */
    case 'V': return 'B'; case 'v': return 'b';
    case 'D': return 'H'; case 'd': return 'h';  /* A|G|T <-> A|C|T */
    case 'H': return 'D'; case 'h': return 'd';
    case 'N': return 'N'; case 'n': return 'n';
    default:  return b;
    }
}

/* Swap T<->U (case preserved), leaving everything else. A DNA sequence becomes
   RNA and an RNA sequence becomes DNA. */
static inline char seq_transcribe_base(char b) {
    switch (b) {
    case 'T': return 'U'; case 't': return 'u';
    case 'U': return 'T'; case 'u': return 't';
    default:  return b;
    }
}

/* Standard genetic code (NCBI transl_table 1), indexed (a<<4)|(b<<2)|c with the
   2-bit base codes above. */
static const char SEQ_STD_CODE[65] =
    "KNKNTTTTRSRSIIMIQHQHPPPPRRRRLLLLEDEDAAAAGGGGVVVV*Y*YSSSS*CWCLFLF";

static inline char seq_codon_aa(char a, char b, char c) {
    int x = seq_base2bit(a), y = seq_base2bit(b), z = seq_base2bit(c);
    if (x < 0 || y < 0 || z < 0) return 'X';
    return SEQ_STD_CODE[(x << 4) | (y << 2) | z];
}

VecType vec_expr_seq_result_type(char seq_fn) {
    switch (seq_fn) {
    case 'L': case 'd': return VEC_INT64;
    case 'G':           return VEC_DOUBLE;
    default:            return VEC_STRING;  /* r c v t p s */
    }
}

/* ------------------------------------------------------------------ measures */

/* seq_length ('L' -> int64) and seq_gc ('G' -> double). */
static VecArray *seq_eval_measure(const VecExpr *expr, const VecBatch *batch) {
    char fn = expr->seq_fn;
    VecArray *s = vec_expr_eval(expr->operand, batch);
    if (s->type != VEC_STRING)
        vectra_error("seq_%s: argument must be a sequence string column",
                     fn == 'L' ? "length" : "gc");
    int64_t n = s->length;

    VecType rt = (fn == 'L') ? VEC_INT64 : VEC_DOUBLE;
    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    *out = vec_array_alloc(rt, n);

    int do_par = (n > SEQ_PAR_THRESHOLD);
#ifdef _OPENMP
    #pragma omp parallel for if(do_par) schedule(dynamic, SEQ_CHUNK)
#endif
    for (int64_t i = 0; i < n; i++) {
        if (!vec_array_is_valid(s, i)) { vec_array_set_null(out, i); continue; }
        int64_t L = seq_str_len(s, i);
        if (fn == 'L') {
            out->buf.i64[i] = L;
            vec_array_set_valid(out, i);
        } else {
            if (L <= 0) { vec_array_set_null(out, i); continue; }
            const char *p = seq_ptr(s, i);
            int64_t gc = 0;
            for (int64_t k = 0; k < L; k++) {
                char c = p[k];
                if (c == 'G' || c == 'C' || c == 'g' || c == 'c') gc++;
            }
            out->buf.dbl[i] = (double)gc / (double)L;
            vec_array_set_valid(out, i);
        }
    }

    vec_array_free(s); free(s);
    return out;
}

/* ---------------------------------------------------------------- transforms */

/* Length of the output sequence for row i (input length L). */
static inline int64_t seq_out_len(char fn, int64_t L,
                                  int64_t start1, int64_t width) {
    switch (fn) {
    case 'p':  return L / 3;                 /* complete codons only */
    case 's': {                              /* subseq: 1-based start, width */
        int64_t st = start1 - 1; if (st < 0) st = 0;
        if (st > L) st = L;
        int64_t w = width; if (w < 0) w = 0;
        int64_t end = st + w; if (end > L) end = L;
        return end - st;
    }
    default:   return L;                     /* r c v t: same length */
    }
}

/* seq_revcomp / seq_complement / seq_reverse / seq_transcribe / seq_translate /
   seq_subseq -- all produce a string column, so share one two-pass skeleton. */
static VecArray *seq_eval_transform(const VecExpr *expr, const VecBatch *batch) {
    char fn = expr->seq_fn;
    VecArray *s = vec_expr_eval(expr->operand, batch);
    if (s->type != VEC_STRING)
        vectra_error("seq transform: argument must be a sequence string column");
    int64_t n = s->length;

    /* subseq: start + width operands (columns or broadcast literals). */
    VecArray *start_a = NULL, *width_a = NULL;
    if (fn == 's') {
        start_a = vec_expr_eval(expr->left, batch);
        width_a = vec_expr_eval(expr->right, batch);
    }

    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    *out = vec_array_alloc(VEC_STRING, n);

    /* Pass 1 (serial): per-row output length, offsets, validity, total. */
    int64_t total = 0;
    for (int64_t i = 0; i < n; i++) {
        out->buf.str.offsets[i] = total;
        int valid = vec_array_is_valid(s, i);
        int64_t start1 = 1, width = 0;
        if (valid && fn == 's') {
            if (!vec_array_is_valid(start_a, i) || !vec_array_is_valid(width_a, i)) {
                valid = 0;
            } else {
                start1 = vec_array_get_int(start_a, i);
                if (start_a->type == VEC_DOUBLE) start1 = vec_d2i_saturate(start_a->buf.dbl[i]);
                width = vec_array_get_int(width_a, i);
                if (width_a->type == VEC_DOUBLE) width = vec_d2i_saturate(width_a->buf.dbl[i]);
            }
        }
        if (!valid) { vec_array_set_null(out, i); continue; }
        vec_array_set_valid(out, i);
        total += seq_out_len(fn, seq_str_len(s, i), start1, width);
    }
    out->buf.str.offsets[n] = total;

    free(out->buf.str.data);  /* 1-byte placeholder from vec_array_alloc */
    out->buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
    out->buf.str.data_len = total;
    char *data = out->buf.str.data;

    /* Pass 2 (parallel): fill each row's disjoint slice. */
    int do_par = (n > SEQ_PAR_THRESHOLD);
#ifdef _OPENMP
    #pragma omp parallel for if(do_par) schedule(dynamic, SEQ_CHUNK)
#endif
    for (int64_t i = 0; i < n; i++) {
        if (!vec_array_is_valid(out, i)) continue;
        char *dst = data + out->buf.str.offsets[i];
        const char *src = seq_ptr(s, i);
        int64_t L = seq_str_len(s, i);
        switch (fn) {
        case 'c':
            for (int64_t k = 0; k < L; k++) dst[k] = seq_comp_base(src[k]);
            break;
        case 'r':  /* reverse complement */
            for (int64_t k = 0; k < L; k++) dst[k] = seq_comp_base(src[L - 1 - k]);
            break;
        case 'v':  /* reverse */
            for (int64_t k = 0; k < L; k++) dst[k] = src[L - 1 - k];
            break;
        case 't':  /* transcribe */
            for (int64_t k = 0; k < L; k++) dst[k] = seq_transcribe_base(src[k]);
            break;
        case 'p': {  /* translate, frame 1 */
            int64_t ncod = L / 3;
            for (int64_t j = 0; j < ncod; j++)
                dst[j] = seq_codon_aa(src[3*j], src[3*j+1], src[3*j+2]);
            break;
        }
        case 's': {  /* subseq */
            int64_t start1 = vec_array_get_int(start_a, i);
            if (start_a->type == VEC_DOUBLE) start1 = vec_d2i_saturate(start_a->buf.dbl[i]);
            int64_t st = start1 - 1; if (st < 0) st = 0; if (st > L) st = L;
            int64_t olen = out->buf.str.offsets[i + 1] - out->buf.str.offsets[i];
            if (olen > 0) memcpy(dst, src + st, (size_t)olen);
            break;
        }
        default: break;
        }
    }

    vec_array_free(s); free(s);
    if (start_a) { vec_array_free(start_a); free(start_a); }
    if (width_a) { vec_array_free(width_a); free(width_a); }
    return out;
}

/* ------------------------------------------------------------------ distance */

/* Hamming distance: -1 (NA) if lengths differ. */
static inline int64_t seq_hamming(const char *s, int64_t ls,
                                  const char *t, int64_t lt) {
    if (ls != lt) return -1;
    int64_t d = 0;
    for (int64_t i = 0; i < ls; i++) if (s[i] != t[i]) d++;
    return d;
}

/* seq_dist ('d' -> int64). Method on expr->op: 'l' Levenshtein (default),
   'd' Damerau-Levenshtein, 'h' Hamming. Second sequence is a column on
   expr->left or a constant on expr->lit_str. */
static VecArray *seq_eval_dist(const VecExpr *expr, const VecBatch *batch) {
    VecArray *a = vec_expr_eval(expr->operand, batch);
    if (a->type != VEC_STRING)
        vectra_error("seq_dist: first argument must be a sequence string column");
    int64_t n = a->length;

    VecArray *b = NULL;
    const char *lit = expr->lit_str;
    int64_t lit_len = lit ? (int64_t)strlen(lit) : 0;
    if (expr->left) {
        b = vec_expr_eval(expr->left, batch);
        if (b->type != VEC_STRING)
            vectra_error("seq_dist: second argument must be a sequence string column");
    } else if (!lit) {
        vectra_error("seq_dist: missing reference sequence");
    }

    char method = expr->op ? expr->op : 'l';

    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    *out = vec_array_alloc(VEC_INT64, n);

    int do_par = (n > SEQ_PAR_THRESHOLD);
#ifdef _OPENMP
    #pragma omp parallel for if(do_par) schedule(dynamic, SEQ_CHUNK)
#endif
    for (int64_t i = 0; i < n; i++) {
        if (!vec_array_is_valid(a, i)) { vec_array_set_null(out, i); continue; }
        const char *bp; int64_t bl;
        if (b) {
            if (!vec_array_is_valid(b, i)) { vec_array_set_null(out, i); continue; }
            bp = seq_ptr(b, i); bl = seq_str_len(b, i);
        } else {
            bp = lit; bl = lit_len;
        }
        const char *ap = seq_ptr(a, i);
        int64_t al = seq_str_len(a, i);
        int64_t d;
        switch (method) {
        case 'd': d = strdist_dl(ap, al, bp, bl, -1); break;
        case 'h': d = seq_hamming(ap, al, bp, bl);    break;
        default:  d = strdist_levenshtein(ap, al, bp, bl, -1); break;
        }
        if (d < 0) { vec_array_set_null(out, i); continue; }
        out->buf.i64[i] = d;
        vec_array_set_valid(out, i);
    }

    vec_array_free(a); free(a);
    if (b) { vec_array_free(b); free(b); }
    return out;
}

VecArray *vec_expr_eval_seq(const VecExpr *expr, const VecBatch *batch) {
    switch (expr->seq_fn) {
    case 'L': case 'G':
        return seq_eval_measure(expr, batch);
    case 'd':
        return seq_eval_dist(expr, batch);
    default:  /* r c v t p s */
        return seq_eval_transform(expr, batch);
    }
}
