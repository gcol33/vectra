#ifndef VECTRA_WINDOW_H
#define VECTRA_WINDOW_H

#include "types.h"
#include <stdint.h>

typedef enum {
    WIN_LAG,
    WIN_LEAD,
    WIN_ROW_NUMBER,
    WIN_RANK,
    WIN_AVG_RANK,    /* base::rank ties.method = "average" (double result) */
    WIN_DENSE_RANK,
    WIN_N,           /* partition/group size repeated per row (dplyr n() in mutate) */
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

    /* Ordered single-partition streaming path (ungrouped windows). Every spec
       shares one stream ordering: either the child's natural arrival order, or
       a global sort inserted below this node (by a value column for the rank
       family, by a time column for rolling). Each output batch is computed
       from one child batch plus bounded per-spec running state, so peak memory
       is one batch (plus the pre-sort's own spill-safe buffering). This
       subsumes the cumulative aggregates, the rank family, ntile, lag, and the
       rolling aggregates. */
    int       ostream;
    void     *run_state;   /* WinRunState[n_wins]; internal to window.c */
    int64_t   total_n;     /* partition row count for ntile/percent_rank/
                              cume_dist; -1 until known */
    void     *count_src;   /* SortNode* to read total_n from, or NULL */
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
