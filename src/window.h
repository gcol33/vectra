#ifndef VECTRA_WINDOW_H
#define VECTRA_WINDOW_H

#include "types.h"
#include <stdint.h>

typedef enum {
    WIN_LAG,
    WIN_LEAD,
    WIN_ROW_NUMBER,
    WIN_RANK,
    WIN_DENSE_RANK,
    WIN_CUMSUM,
    WIN_CUMMEAN,
    WIN_CUMMIN,
    WIN_CUMMAX,
    WIN_NTILE,
    WIN_PERCENT_RANK,
    WIN_CUME_DIST,
    WIN_ROLL_SUM,    /* time-based trailing rolling aggregates over `window` */
    WIN_ROLL_MEAN,
    WIN_ROLL_MIN,
    WIN_ROLL_MAX,
    WIN_ROLL_N
} WinKind;

typedef struct {
    char      *output_name;
    WinKind    kind;
    char      *input_col;    /* order/value column; NULL for unordered row_number */
    int        offset;       /* for lag/lead: n positions */
    double     default_val;  /* for lag/lead: fill value */
    int        has_default;
    int        desc;         /* row_number/rank: descending order when nonzero */
    char      *order_col;    /* roll_*: datetime column defining the window */
    double     window;       /* roll_*: trailing window span in seconds */
} WinSpec;

typedef struct {
    VecNode   base;
    VecNode  *child;
    int       n_keys;
    char    **key_names;
    int       n_wins;
    WinSpec  *win_specs;
    int       done;

    /* Spill-safe streaming path (grouped windows with a temp dir). When set,
       `child` is a sort node keyed on the group columns plus a trailing row-id,
       so each group arrives contiguous and in original within-group order; the
       node materializes one group at a time instead of the whole table. */
    int       streaming;
    int      *key_idx;     /* group key column indices in child schema */
    int       rowid_idx;   /* row-id column index in child schema (-1 if none) */
    VecBatch *hold_batch;  /* current sorted batch being consumed */
    int64_t   hold_pos;    /* logical cursor into hold_batch */
    int64_t   hold_n;      /* logical rows in hold_batch */

    /* Cumulative streaming path (ungrouped windows whose every spec is a
       forward-cumulative aggregate: cumsum/cummean/cummin/cummax or an
       unordered row_number). Each output batch is computed from one child
       batch plus O(1) running state, so peak memory is one batch. */
    int       cum_mode;
    void     *cum_state;   /* WinCumState[n_wins]; internal to window.c */
} WindowNode;

/* Returns the top of a small node pipeline. For grouped windows with a
   `temp_dir`, that is a (row-id -> sort -> window -> restore-sort -> drop
   row-id) chain whose peak memory is one group, not the whole table; for
   ungrouped windows (or a NULL temp_dir) it is a plain in-memory window node.
   Either way the returned node's output schema is child columns + window
   columns, in original row order. */
VecNode *window_node_create(VecNode *child,
                            int n_keys, char **key_names,
                            int n_wins, WinSpec *win_specs,
                            const char *temp_dir);

#endif /* VECTRA_WINDOW_H */
