#include "window.h"
#include "hash.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "builder.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* Compare two values in a VecArray. Returns <0, 0, or >0.
   NAs sort last (greater than any non-NA value). */
static int vec_compare_values(const VecArray *arr, int64_t a, int64_t b) {
    int a_valid = vec_array_is_valid(arr, a);
    int b_valid = vec_array_is_valid(arr, b);
    if (!a_valid && !b_valid) return 0;
    if (!a_valid) return 1;   /* NA > non-NA */
    if (!b_valid) return -1;
    switch (arr->type) {
    case VEC_INT64: {
        int64_t va = arr->buf.i64[a], vb = arr->buf.i64[b];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    case VEC_DOUBLE: {
        double va = arr->buf.dbl[a], vb = arr->buf.dbl[b];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    case VEC_STRING: {
        int64_t sa = arr->buf.str.offsets[a], ea = arr->buf.str.offsets[a + 1];
        int64_t sb = arr->buf.str.offsets[b], eb = arr->buf.str.offsets[b + 1];
        int64_t la = ea - sa, lb = eb - sb;
        int64_t mn = la < lb ? la : lb;
        int cmp = memcmp(arr->buf.str.data + sa, arr->buf.str.data + sb, (size_t)mn);
        if (cmp != 0) return cmp;
        return (la < lb) ? -1 : (la > lb) ? 1 : 0;
    }
    default:
        return 0;
    }
}

/* Apply a window kernel over a contiguous segment [start, end) */
static VecArray win_eval_segment(WinKind kind, const VecArray *input,
                                 int64_t start, int64_t end, int64_t n_total,
                                 int offset, double default_val, int has_default,
                                 VecArray *result) {
    (void)n_total;
    int64_t seg_len = end - start;

    switch (kind) {
    case WIN_LAG:
        for (int64_t i = start; i < end; i++) {
            int64_t src = i - offset;
            if (src < start || src >= end) {
                if (has_default) {
                    vec_array_set_valid(result, i);
                    result->buf.dbl[i] = default_val;
                } else {
                    vec_array_set_null(result, i);
                }
            } else if (!vec_array_is_valid(input, src)) {
                vec_array_set_null(result, i);
            } else {
                vec_array_set_valid(result, i);
                result->buf.dbl[i] = (input->type == VEC_DOUBLE) ?
                    input->buf.dbl[src] : (double)input->buf.i64[src];
            }
        }
        break;

    case WIN_LEAD:
        for (int64_t i = start; i < end; i++) {
            int64_t src = i + offset;
            if (src < start || src >= end) {
                if (has_default) {
                    vec_array_set_valid(result, i);
                    result->buf.dbl[i] = default_val;
                } else {
                    vec_array_set_null(result, i);
                }
            } else if (!vec_array_is_valid(input, src)) {
                vec_array_set_null(result, i);
            } else {
                vec_array_set_valid(result, i);
                result->buf.dbl[i] = (input->type == VEC_DOUBLE) ?
                    input->buf.dbl[src] : (double)input->buf.i64[src];
            }
        }
        break;

    case WIN_ROW_NUMBER:
        for (int64_t i = start; i < end; i++) {
            vec_array_set_valid(result, i);
            result->buf.dbl[i] = (double)(i - start + 1);
        }
        break;

    case WIN_RANK:
        /* min_rank: rank = 1 + count of values strictly less than current */
        for (int64_t i = start; i < end; i++) {
            int64_t r = 1;
            for (int64_t j = start; j < end; j++) {
                if (j != i && vec_compare_values(input, j, i) < 0) r++;
            }
            vec_array_set_valid(result, i);
            result->buf.dbl[i] = (double)r;
        }
        break;

    case WIN_DENSE_RANK:
        /* dense_rank: rank = 1 + count of distinct values strictly less than current */
        for (int64_t i = start; i < end; i++) {
            int64_t r = 1;
            for (int64_t j = start; j < end; j++) {
                if (vec_compare_values(input, j, i) < 0) {
                    /* Check if this value is distinct from all previous "less than" values */
                    int is_dup = 0;
                    for (int64_t k = start; k < j; k++) {
                        if (vec_compare_values(input, k, i) < 0 &&
                            vec_compare_values(input, k, j) == 0) {
                            is_dup = 1;
                            break;
                        }
                    }
                    if (!is_dup) r++;
                }
            }
            vec_array_set_valid(result, i);
            result->buf.dbl[i] = (double)r;
        }
        break;

    case WIN_CUMSUM: {
        double acc = 0.0;
        for (int64_t i = start; i < end; i++) {
            if (!vec_array_is_valid(input, i)) {
                vec_array_set_null(result, i);
                /* Once NA is seen, rest is NA (R semantics) */
                for (int64_t j = i + 1; j < end; j++)
                    vec_array_set_null(result, j);
                break;
            }
            double v = (input->type == VEC_DOUBLE) ?
                input->buf.dbl[i] : (double)input->buf.i64[i];
            acc += v;
            vec_array_set_valid(result, i);
            result->buf.dbl[i] = acc;
        }
        break;
    }

    case WIN_CUMMEAN: {
        double acc = 0.0;
        int64_t cnt = 0;
        for (int64_t i = start; i < end; i++) {
            if (!vec_array_is_valid(input, i)) {
                vec_array_set_null(result, i);
                for (int64_t j = i + 1; j < end; j++)
                    vec_array_set_null(result, j);
                break;
            }
            double v = (input->type == VEC_DOUBLE) ?
                input->buf.dbl[i] : (double)input->buf.i64[i];
            acc += v;
            cnt++;
            vec_array_set_valid(result, i);
            result->buf.dbl[i] = acc / (double)cnt;
        }
        break;
    }

    case WIN_CUMMIN: {
        double cur_min = INFINITY;
        for (int64_t i = start; i < end; i++) {
            if (!vec_array_is_valid(input, i)) {
                vec_array_set_null(result, i);
                for (int64_t j = i + 1; j < end; j++)
                    vec_array_set_null(result, j);
                break;
            }
            double v = (input->type == VEC_DOUBLE) ?
                input->buf.dbl[i] : (double)input->buf.i64[i];
            if (v < cur_min) cur_min = v;
            vec_array_set_valid(result, i);
            result->buf.dbl[i] = cur_min;
        }
        break;
    }

    case WIN_CUMMAX: {
        double cur_max = -INFINITY;
        for (int64_t i = start; i < end; i++) {
            if (!vec_array_is_valid(input, i)) {
                vec_array_set_null(result, i);
                for (int64_t j = i + 1; j < end; j++)
                    vec_array_set_null(result, j);
                break;
            }
            double v = (input->type == VEC_DOUBLE) ?
                input->buf.dbl[i] : (double)input->buf.i64[i];
            if (v > cur_max) cur_max = v;
            vec_array_set_valid(result, i);
            result->buf.dbl[i] = cur_max;
        }
        break;
    }
    }

    (void)seg_len;
    return *result;
}

static VecBatch *window_next_batch(VecNode *self) {
    WindowNode *wn = (WindowNode *)self;
    if (wn->done) return NULL;
    wn->done = 1;

    const VecSchema *cschema = &wn->child->output_schema;

    /* 1) Materialize all child batches */
    int n_cols = cschema->n_cols;
    VecArrayBuilder *builders = (VecArrayBuilder *)calloc(
        (size_t)n_cols, sizeof(VecArrayBuilder));
    for (int c = 0; c < n_cols; c++)
        builders[c] = vec_builder_init(cschema->col_types[c]);

    VecBatch *batch;
    while ((batch = wn->child->next_batch(wn->child)) != NULL) {
        if (!batch->sel) {
            for (int c = 0; c < n_cols; c++)
                vec_builder_append_array(&builders[c], &batch->columns[c]);
        } else {
            int64_t n_logical = vec_batch_logical_rows(batch);
            for (int c = 0; c < n_cols; c++)
                vec_builder_reserve(&builders[c], n_logical);
            for (int64_t li = 0; li < n_logical; li++) {
                int64_t pi = vec_batch_physical_row(batch, li);
                for (int c = 0; c < n_cols; c++)
                    vec_builder_append_one(&builders[c],
                                           &batch->columns[c], pi);
            }
        }
        vec_batch_free(batch);
    }

    int64_t n_rows = builders[0].length;
    VecArray *cols = (VecArray *)malloc((size_t)n_cols * sizeof(VecArray));
    for (int c = 0; c < n_cols; c++)
        cols[c] = vec_builder_finish(&builders[c]);
    free(builders);

    /* 2) Evaluate window functions */
    if (wn->n_keys > 0) {
        /* Grouped: find key column indices */
        int *key_idx = (int *)malloc((size_t)wn->n_keys * sizeof(int));
        for (int k = 0; k < wn->n_keys; k++) {
            key_idx[k] = vec_schema_find_col(cschema, wn->key_names[k]);
            if (key_idx[k] < 0)
                vectra_error("window: group column not found: %s",
                             wn->key_names[k]);
        }

        /* Assign group IDs via hash table */
        int64_t *group_ids = (int64_t *)malloc((size_t)n_rows * sizeof(int64_t));
        VecHashTable ht = vec_ht_create(64);

        /* Key arena via builders (supports all types including strings) */
        VecArrayBuilder *arena_builders = (VecArrayBuilder *)calloc(
            (size_t)wn->n_keys, sizeof(VecArrayBuilder));
        for (int k = 0; k < wn->n_keys; k++)
            arena_builders[k] = vec_builder_init(cols[key_idx[k]].type);

        /* Temporary arena arrays for hash lookups (rebuilt after each insert) */
        VecArray *arena = (VecArray *)calloc((size_t)wn->n_keys, sizeof(VecArray));
        int64_t arena_len = 0;

        /* Build key_cols array for hash lookups */
        VecArray *key_cols = (VecArray *)malloc(
            (size_t)wn->n_keys * sizeof(VecArray));
        for (int k = 0; k < wn->n_keys; k++)
            key_cols[k] = cols[key_idx[k]];

        for (int64_t r = 0; r < n_rows; r++) {
            uint64_t h = 0;
            for (int k = 0; k < wn->n_keys; k++) {
                uint64_t kh = vec_hash_value(&key_cols[k], r);
                h = (k == 0) ? kh : vec_hash_combine(h, kh);
            }

            int was_new = 0;
            int64_t gid = vec_ht_find_or_insert(
                &ht, h, key_cols, wn->n_keys, r,
                arena, arena_len, &was_new);

            if (was_new) {
                for (int k = 0; k < wn->n_keys; k++)
                    vec_builder_append_one(&arena_builders[k],
                                           &cols[key_idx[k]], r);
                arena_len++;
                /* Rebuild arena arrays from builders for next lookup */
                for (int k = 0; k < wn->n_keys; k++) {
                    if (arena[k].validity) vec_array_free(&arena[k]);
                    arena[k] = vec_builder_finish(&arena_builders[k]);
                    /* Re-init builder and re-append everything */
                    arena_builders[k] = vec_builder_init(cols[key_idx[k]].type);
                    vec_builder_append_array(&arena_builders[k], &arena[k]);
                }
            }
            group_ids[r] = gid;
        }

        int64_t n_groups = ht.n_groups;

        /* Build segments: collect row ranges per group
           Since rows may not be contiguous by group, collect indices per group */
        /* For simplicity: process window functions per-row with group awareness.
           Build per-group row lists. */
        int64_t **grp_rows = (int64_t **)calloc((size_t)n_groups, sizeof(int64_t *));
        int64_t *grp_lens = (int64_t *)calloc((size_t)n_groups, sizeof(int64_t));
        int64_t *grp_caps = (int64_t *)calloc((size_t)n_groups, sizeof(int64_t));

        for (int64_t r = 0; r < n_rows; r++) {
            int64_t g = group_ids[r];
            if (grp_lens[g] >= grp_caps[g]) {
                int64_t nc = grp_caps[g] == 0 ? 16 : grp_caps[g] * 2;
                grp_rows[g] = (int64_t *)realloc(grp_rows[g],
                    (size_t)nc * sizeof(int64_t));
                grp_caps[g] = nc;
            }
            grp_rows[g][grp_lens[g]++] = r;
        }

        /* Evaluate window functions per group */
        int out_ncols = wn->base.output_schema.n_cols;
        VecBatch *result = vec_batch_alloc(out_ncols, n_rows);

        /* Copy pass-through columns */
        for (int c = 0; c < n_cols; c++) {
            VecArray *copy = vec_coerce(&cols[c], cols[c].type);
            result->columns[c] = *copy;
            free(copy);
            const char *nm = cschema->col_names[c];
            result->col_names[c] = (char *)malloc(strlen(nm) + 1);
            strcpy(result->col_names[c], nm);
        }

        /* Evaluate window expressions */
        for (int w = 0; w < wn->n_wins; w++) {
            WinSpec *ws = &wn->win_specs[w];
            int in_col = -1;
            if (ws->input_col) {
                in_col = vec_schema_find_col(cschema, ws->input_col);
                if (in_col < 0)
                    vectra_error("window: column not found: %s", ws->input_col);
            }

            VecArray out = vec_array_alloc(VEC_DOUBLE, n_rows);

            for (int64_t g = 0; g < n_groups; g++) {
                int64_t glen = grp_lens[g];
                int64_t *rows = grp_rows[g];

                switch (ws->kind) {
                case WIN_ROW_NUMBER:
                    for (int64_t j = 0; j < glen; j++) {
                        vec_array_set_valid(&out, rows[j]);
                        out.buf.dbl[rows[j]] = (double)(j + 1);
                    }
                    break;

                case WIN_RANK:
                    for (int64_t j = 0; j < glen; j++) {
                        int64_t r = 1;
                        for (int64_t k = 0; k < glen; k++) {
                            if (k != j && vec_compare_values(&cols[in_col], rows[k], rows[j]) < 0)
                                r++;
                        }
                        vec_array_set_valid(&out, rows[j]);
                        out.buf.dbl[rows[j]] = (double)r;
                    }
                    break;

                case WIN_DENSE_RANK:
                    for (int64_t j = 0; j < glen; j++) {
                        int64_t r = 1;
                        for (int64_t k = 0; k < glen; k++) {
                            if (vec_compare_values(&cols[in_col], rows[k], rows[j]) < 0) {
                                int is_dup = 0;
                                for (int64_t m = 0; m < k; m++) {
                                    if (vec_compare_values(&cols[in_col], rows[m], rows[j]) < 0 &&
                                        vec_compare_values(&cols[in_col], rows[m], rows[k]) == 0) {
                                        is_dup = 1;
                                        break;
                                    }
                                }
                                if (!is_dup) r++;
                            }
                        }
                        vec_array_set_valid(&out, rows[j]);
                        out.buf.dbl[rows[j]] = (double)r;
                    }
                    break;

                case WIN_LAG:
                    for (int64_t j = 0; j < glen; j++) {
                        int64_t src_j = j - ws->offset;
                        if (src_j < 0 || src_j >= glen) {
                            if (ws->has_default) {
                                vec_array_set_valid(&out, rows[j]);
                                out.buf.dbl[rows[j]] = ws->default_val;
                            } else {
                                vec_array_set_null(&out, rows[j]);
                            }
                        } else {
                            int64_t src_row = rows[src_j];
                            if (!vec_array_is_valid(&cols[in_col], src_row)) {
                                vec_array_set_null(&out, rows[j]);
                            } else {
                                vec_array_set_valid(&out, rows[j]);
                                out.buf.dbl[rows[j]] = (cols[in_col].type == VEC_DOUBLE) ?
                                    cols[in_col].buf.dbl[src_row] :
                                    (double)cols[in_col].buf.i64[src_row];
                            }
                        }
                    }
                    break;

                case WIN_LEAD:
                    for (int64_t j = 0; j < glen; j++) {
                        int64_t src_j = j + ws->offset;
                        if (src_j < 0 || src_j >= glen) {
                            if (ws->has_default) {
                                vec_array_set_valid(&out, rows[j]);
                                out.buf.dbl[rows[j]] = ws->default_val;
                            } else {
                                vec_array_set_null(&out, rows[j]);
                            }
                        } else {
                            int64_t src_row = rows[src_j];
                            if (!vec_array_is_valid(&cols[in_col], src_row)) {
                                vec_array_set_null(&out, rows[j]);
                            } else {
                                vec_array_set_valid(&out, rows[j]);
                                out.buf.dbl[rows[j]] = (cols[in_col].type == VEC_DOUBLE) ?
                                    cols[in_col].buf.dbl[src_row] :
                                    (double)cols[in_col].buf.i64[src_row];
                            }
                        }
                    }
                    break;

                case WIN_CUMSUM: {
                    double acc = 0.0;
                    int poisoned = 0;
                    for (int64_t j = 0; j < glen; j++) {
                        int64_t ri = rows[j];
                        if (poisoned || !vec_array_is_valid(&cols[in_col], ri)) {
                            vec_array_set_null(&out, ri);
                            poisoned = 1;
                        } else {
                            double v = (cols[in_col].type == VEC_DOUBLE) ?
                                cols[in_col].buf.dbl[ri] : (double)cols[in_col].buf.i64[ri];
                            acc += v;
                            vec_array_set_valid(&out, ri);
                            out.buf.dbl[ri] = acc;
                        }
                    }
                    break;
                }

                case WIN_CUMMEAN: {
                    double acc = 0.0;
                    int64_t cnt = 0;
                    int poisoned = 0;
                    for (int64_t j = 0; j < glen; j++) {
                        int64_t ri = rows[j];
                        if (poisoned || !vec_array_is_valid(&cols[in_col], ri)) {
                            vec_array_set_null(&out, ri);
                            poisoned = 1;
                        } else {
                            double v = (cols[in_col].type == VEC_DOUBLE) ?
                                cols[in_col].buf.dbl[ri] : (double)cols[in_col].buf.i64[ri];
                            acc += v;
                            cnt++;
                            vec_array_set_valid(&out, ri);
                            out.buf.dbl[ri] = acc / (double)cnt;
                        }
                    }
                    break;
                }

                case WIN_CUMMIN: {
                    double cur = INFINITY;
                    int poisoned = 0;
                    for (int64_t j = 0; j < glen; j++) {
                        int64_t ri = rows[j];
                        if (poisoned || !vec_array_is_valid(&cols[in_col], ri)) {
                            vec_array_set_null(&out, ri);
                            poisoned = 1;
                        } else {
                            double v = (cols[in_col].type == VEC_DOUBLE) ?
                                cols[in_col].buf.dbl[ri] : (double)cols[in_col].buf.i64[ri];
                            if (v < cur) cur = v;
                            vec_array_set_valid(&out, ri);
                            out.buf.dbl[ri] = cur;
                        }
                    }
                    break;
                }

                case WIN_CUMMAX: {
                    double cur = -INFINITY;
                    int poisoned = 0;
                    for (int64_t j = 0; j < glen; j++) {
                        int64_t ri = rows[j];
                        if (poisoned || !vec_array_is_valid(&cols[in_col], ri)) {
                            vec_array_set_null(&out, ri);
                            poisoned = 1;
                        } else {
                            double v = (cols[in_col].type == VEC_DOUBLE) ?
                                cols[in_col].buf.dbl[ri] : (double)cols[in_col].buf.i64[ri];
                            if (v > cur) cur = v;
                            vec_array_set_valid(&out, ri);
                            out.buf.dbl[ri] = cur;
                        }
                    }
                    break;
                }
                }
            }

            result->columns[n_cols + w] = out;
            result->col_names[n_cols + w] = (char *)malloc(
                strlen(ws->output_name) + 1);
            strcpy(result->col_names[n_cols + w], ws->output_name);
        }

        /* Cleanup */
        for (int64_t g = 0; g < n_groups; g++) free(grp_rows[g]);
        free(grp_rows);
        free(grp_lens);
        free(grp_caps);
        free(group_ids);
        for (int k = 0; k < wn->n_keys; k++) {
            vec_array_free(&arena[k]);
            vec_builder_free(&arena_builders[k]);
        }
        free(arena);
        free(arena_builders);
        free(key_cols);
        free(key_idx);
        vec_ht_free(&ht);
        for (int c = 0; c < n_cols; c++) vec_array_free(&cols[c]);
        free(cols);

        return result;
    }

    /* Ungrouped path: single segment over entire data */
    int out_ncols = wn->base.output_schema.n_cols;
    VecBatch *result = vec_batch_alloc(out_ncols, n_rows);

    for (int c = 0; c < n_cols; c++) {
        VecArray *copy = vec_coerce(&cols[c], cols[c].type);
        result->columns[c] = *copy;
        free(copy);
        const char *nm = cschema->col_names[c];
        result->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(result->col_names[c], nm);
    }

    for (int w = 0; w < wn->n_wins; w++) {
        WinSpec *ws = &wn->win_specs[w];
        int in_col = -1;
        if (ws->input_col) {
            in_col = vec_schema_find_col(cschema, ws->input_col);
            if (in_col < 0)
                vectra_error("window: column not found: %s", ws->input_col);
        }

        VecArray out = vec_array_alloc(VEC_DOUBLE, n_rows);
        win_eval_segment(ws->kind, in_col >= 0 ? &cols[in_col] : NULL,
                         0, n_rows, n_rows,
                         ws->offset, ws->default_val, ws->has_default,
                         &out);
        result->columns[n_cols + w] = out;
        result->col_names[n_cols + w] = (char *)malloc(
            strlen(ws->output_name) + 1);
        strcpy(result->col_names[n_cols + w], ws->output_name);
    }

    for (int c = 0; c < n_cols; c++) vec_array_free(&cols[c]);
    free(cols);

    return result;
}

static void window_free(VecNode *self) {
    WindowNode *wn = (WindowNode *)self;
    wn->child->free_node(wn->child);
    for (int k = 0; k < wn->n_keys; k++) free(wn->key_names[k]);
    free(wn->key_names);
    for (int w = 0; w < wn->n_wins; w++) {
        free(wn->win_specs[w].output_name);
        free(wn->win_specs[w].input_col);
    }
    free(wn->win_specs);
    vec_schema_free(&wn->base.output_schema);
    free(wn);
}

WindowNode *window_node_create(VecNode *child,
                               int n_keys, char **key_names,
                               int n_wins, WinSpec *win_specs) {
    WindowNode *wn = (WindowNode *)calloc(1, sizeof(WindowNode));
    if (!wn) vectra_error("alloc failed for WindowNode");
    wn->child = child;
    wn->n_keys = n_keys;
    wn->key_names = key_names;
    wn->n_wins = n_wins;
    wn->win_specs = win_specs;
    wn->done = 0;

    /* Output schema: child schema + window columns (all double) */
    const VecSchema *cs = &child->output_schema;
    int out_n = cs->n_cols + n_wins;
    char **names = (char **)malloc((size_t)out_n * sizeof(char *));
    VecType *types = (VecType *)malloc((size_t)out_n * sizeof(VecType));
    for (int i = 0; i < cs->n_cols; i++) {
        names[i] = cs->col_names[i];
        types[i] = cs->col_types[i];
    }
    for (int w = 0; w < n_wins; w++) {
        names[cs->n_cols + w] = win_specs[w].output_name;
        types[cs->n_cols + w] = VEC_DOUBLE;
    }

    wn->base.output_schema = vec_schema_create(out_n, names, types);
    free(names);
    free(types);

    wn->base.next_batch = window_next_batch;
    wn->base.kind = "WindowNode";
    wn->base.free_node = window_free;

    return wn;
}
