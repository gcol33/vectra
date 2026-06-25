/*
 * src/core/decode_profile.h
 *
 * Cycle-level profiling for LZ_STREAMS decode hot path. Flag-gated
 * via --profile CLI (tdc_dp_force_enable) or TDC_DECODE_PROFILE=1 env;
 * zero cost when disabled (compiler can hoist the check through the
 * branch predictor, and the hot helpers early-return).
 *
 * State is in a single TU (decode_profile.c) — this header only
 * declares externs. Per-TU `static inline` would give each caller a
 * private copy of the counters and the dump would see only the
 * bench-side state, not the lz_streams-side counters.
 *
 * Usage:
 *
 *   tdc_dp_reset();
 *   ... decode ...
 *   tdc_dp_dump("LZ_STREAMS");
 *
 * Per section, wraps:
 *   uint64_t t0 = tdc_dp_rdtsc();
 *   <section>
 *   tdc_dp_add(TDC_DP_MATCH, t0);
 */

#ifndef TDC_CORE_DECODE_PROFILE_H
#define TDC_CORE_DECODE_PROFILE_H

#include <stdint.h>

#if defined(_MSC_VER)
#  include <intrin.h>
#endif

enum {
    TDC_DP_SYMBOL = 0,   /* symbol reconstruct (ll, ml, off, repcode)
                          * fused path: full reconstruct per seq.
                          * reconstruct path: prefetch + addr-walk only;
                          * the pre-pass is TDC_DP_SYMPRE. */
    TDC_DP_LIT,          /* literal copy */
    TDC_DP_MATCH,        /* match copy */
    TDC_DP_OTHER,        /* bookkeeping / bounds checks */
    TDC_DP_SYMPRE,       /* symbol reconstruct pre-pass
                          * (lzs_reconstruct_symbols[_split]) */
    TDC_DP_STREAMDEC,    /* per-stream Huffman/FSE decode of ll/ml/off */
    TDC_DP_NSEC
};

/* Backing state and enable-flag live in decode_profile.c. */
extern uint64_t tdc_dp_cycles[TDC_DP_NSEC];
extern uint64_t tdc_dp_off_hist[8];
extern uint64_t tdc_dp_n_seqs_total;
extern uint64_t tdc_dp_n_decode_calls;
extern int      tdc_dp_flag;  /* -1 resolve-from-env, 0 off, 1 on */

int  tdc_dp_enabled(void);
void tdc_dp_force_enable(void);
void tdc_dp_reset(void);
void tdc_dp_dump(const char *label);

/* Hot-path profile hooks are compile-time gated. Define TDC_DECODE_PROFILE
 * at build time to enable per-section cycle accounting. Default OFF so
 * release builds pay zero overhead. The histogram and seq counters stay
 * live so --profile can still report structural data (seq counts, offsets)
 * when enabled at runtime, without per-iteration rdtsc. */
#ifdef TDC_DECODE_PROFILE
static inline uint64_t tdc_dp_rdtsc(void) {
#if defined(_MSC_VER)
    return (uint64_t)__rdtsc();
#elif defined(__x86_64__) || defined(__i386__)
    uint32_t lo, hi;
    __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
#else
    return 0;
#endif
}

static inline void tdc_dp_add(int sec, uint64_t t0) {
    if (!tdc_dp_enabled()) return;
    uint64_t t1 = tdc_dp_rdtsc();
    tdc_dp_cycles[sec] += (t1 - t0);
}
#else
static inline uint64_t tdc_dp_rdtsc(void) { return 0; }
static inline void tdc_dp_add(int sec, uint64_t t0) { (void)sec; (void)t0; }
#endif

static inline int tdc_dp_off_bucket(uint32_t mo) {
    if (mo <= 3u)     return 0;
    if (mo <= 7u)     return 1;
    if (mo <= 15u)    return 2;
    if (mo <= 31u)    return 3;
    if (mo <= 127u)   return 4;
    if (mo <= 1023u)  return 5;
    if (mo <= 16383u) return 6;
    return 7;
}

static inline void tdc_dp_count_offset(uint32_t mo) {
    if (!tdc_dp_enabled()) return;
    tdc_dp_off_hist[tdc_dp_off_bucket(mo)]++;
}

static inline void tdc_dp_count_seqs(uint32_t n_seqs) {
    if (!tdc_dp_enabled()) return;
    tdc_dp_n_seqs_total   += n_seqs;
    tdc_dp_n_decode_calls++;
}

#endif /* TDC_CORE_DECODE_PROFILE_H */
