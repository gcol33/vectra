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
} WindowNode;

WindowNode *window_node_create(VecNode *child,
                               int n_keys, char **key_names,
                               int n_wins, WinSpec *win_specs);

#endif /* VECTRA_WINDOW_H */
