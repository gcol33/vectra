/*
 * src/core/decode_profile.c
 *
 * Single-TU backing for decode_profile.h. Keeps the counters and
 * enable-flag unique across the whole link unit so inline callers
 * in lz_streams.c and bench_throughput.c share one state.
 */

#include "decode_profile.h"
#include "log.h"

#include <stdlib.h>

uint64_t tdc_dp_cycles[TDC_DP_NSEC];
uint64_t tdc_dp_off_hist[8];
uint64_t tdc_dp_n_seqs_total;
uint64_t tdc_dp_n_decode_calls;
int      tdc_dp_flag = -1;

int tdc_dp_enabled(void) {
    if (tdc_dp_flag < 0) {
        const char *e = getenv("TDC_DECODE_PROFILE");
        tdc_dp_flag = (e && e[0] && e[0] != '0') ? 1 : 0;
    }
    return tdc_dp_flag;
}

void tdc_dp_force_enable(void) {
    tdc_dp_flag = 1;
}

void tdc_dp_reset(void) {
    for (int i = 0; i < TDC_DP_NSEC; i++) tdc_dp_cycles[i] = 0;
    for (int i = 0; i < 8; i++)           tdc_dp_off_hist[i] = 0;
    tdc_dp_n_seqs_total   = 0;
    tdc_dp_n_decode_calls = 0;
}

void tdc_dp_dump(const char *label) {
    if (!tdc_dp_enabled()) return;
    uint64_t total = 0;
    for (int i = 0; i < TDC_DP_NSEC; i++) total += tdc_dp_cycles[i];
    if (total == 0) return;
    static const char *names[TDC_DP_NSEC] = {
        "symbol", "literal", "match", "other", "sympre", "streamdec"
    };
    TDC_LOG("[decode-profile %s] %llu calls, %llu seqs, %llu cycles total\n",
        label,
        (unsigned long long)tdc_dp_n_decode_calls,
        (unsigned long long)tdc_dp_n_seqs_total,
        (unsigned long long)total);
    for (int i = 0; i < TDC_DP_NSEC; i++) {
        double pct = 100.0 * (double)tdc_dp_cycles[i] / (double)total;
        double cps = tdc_dp_n_seqs_total
            ? (double)tdc_dp_cycles[i] / (double)tdc_dp_n_seqs_total
            : 0.0;
        TDC_LOG("  %-8s %14llu cyc  %5.1f%%  %7.2f cyc/seq\n",
                names[i],
                (unsigned long long)tdc_dp_cycles[i],
                pct, cps);
    }
    uint64_t hsum = 0;
    for (int i = 0; i < 8; i++) hsum += tdc_dp_off_hist[i];
    if (hsum > 0) {
        static const char *labels[8] = {
            "1-3", "4-7", "8-15", "16-31",
            "32-127", "128-1K", "1K-16K", ">16K"
        };
        TDC_LOG("  offset histogram (%llu offsets):\n",
                (unsigned long long)hsum);
        for (int i = 0; i < 8; i++) {
            double pct = 100.0 * (double)tdc_dp_off_hist[i] / (double)hsum;
            TDC_LOG("    %-8s %14llu  %5.1f%%\n",
                    labels[i],
                    (unsigned long long)tdc_dp_off_hist[i],
                    pct);
        }
    }
    TDC_LOG_FLUSH();
}
