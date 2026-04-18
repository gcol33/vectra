#ifndef VECTRA_VTR1_H
#define VECTRA_VTR1_H

/*
 * vtr1.h — shared column-statistics type for row-group predicate pruning.
 *
 * The reader in vtr1_tdc.c populates Vtr1ColStat per column per row group;
 * scan.c's scan_evaluate_row_group consumes it to skip unreachable groups.
 * Kept in its own header so scan.c does not pull in the full reader API.
 */

#include "types.h"
#include <stdint.h>

/* Per-column per-rowgroup statistics. */
typedef struct {
    uint8_t has_stats;    /* 0 for string cols or no data */
    union {
        struct { int64_t min, max; } i64;
        struct { double min, max; } dbl;
        struct { uint8_t min, max; } bln;
    };
    /* Number of NA rows in the row group. Populated by the tdc reader
     * from tdc_column_stats.null_count. */
    uint64_t null_count;
} Vtr1ColStat;

#endif /* VECTRA_VTR1_H */
