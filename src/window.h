#ifndef VECTRA_WINDOW_H
#define VECTRA_WINDOW_H

#include "types.h"
#include <stdint.h>

typedef enum {
    WIN_LAG,
    WIN_LEAD,
    WIN_ROW_NUMBER,
    WIN_CUMSUM,
    WIN_CUMMEAN,
    WIN_CUMMIN,
    WIN_CUMMAX
} WinKind;

typedef struct {
    char      *output_name;
    WinKind    kind;
    char      *input_col;    /* NULL for row_number */
    int        offset;       /* for lag/lead: n positions */
    double     default_val;  /* for lag/lead: fill value */
    int        has_default;
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
