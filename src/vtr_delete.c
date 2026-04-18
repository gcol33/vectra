#include "vtr_delete.h"
#include "error.h"
#include "vtr_atomic_rename.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* --- Tombstone I/O --- */

TombstoneSet *tombstone_load(const char *del_path) {
    FILE *fp = fopen(del_path, "r");
    if (!fp) return NULL; /* no .del file = no deletions */

    /* First pass: count lines */
    int64_t count = 0;
    int c;
    int last_was_newline = 1;
    while ((c = fgetc(fp)) != EOF) {
        if (c == '\n') {
            last_was_newline = 1;
        } else if (last_was_newline) {
            count++;
            last_was_newline = 0;
        }
    }
    /* Handle file that doesn't end with newline */
    if (!last_was_newline) count++;

    if (count == 0) {
        fclose(fp);
        return NULL;
    }

    rewind(fp);

    TombstoneSet *ts = (TombstoneSet *)malloc(sizeof(TombstoneSet));
    if (!ts) { fclose(fp); vectra_error("tombstone_load: alloc failed"); }
    ts->rows = (int64_t *)malloc((size_t)count * sizeof(int64_t));
    if (!ts->rows) { free(ts); fclose(fp); vectra_error("tombstone_load: alloc failed for rows"); }
    ts->n = 0;

    /* Second pass: read values */
    char line[32];
    while (fgets(line, sizeof(line), fp)) {
        /* Skip blank lines */
        char *end = line;
        while (*end == ' ' || *end == '\t') end++;
        if (*end == '\r' || *end == '\n' || *end == '\0') continue;
        int64_t v = (int64_t)strtoll(line, NULL, 10);
        ts->rows[ts->n++] = v;
    }
    fclose(fp);

    if (ts->n == 0) {
        free(ts->rows);
        free(ts);
        return NULL;
    }

    return ts;
}

void tombstone_free(TombstoneSet *ts) {
    if (!ts) return;
    free(ts->rows);
    free(ts);
}

int tombstone_is_deleted(const TombstoneSet *ts, int64_t row) {
    if (!ts || ts->n == 0) return 0;
    /* Binary search (rows are sorted after tombstone_write) */
    int64_t lo = 0, hi = ts->n - 1;
    while (lo <= hi) {
        int64_t mid = lo + (hi - lo) / 2;
        if (ts->rows[mid] == row) return 1;
        if (ts->rows[mid] < row) lo = mid + 1;
        else hi = mid - 1;
    }
    return 0;
}

/* qsort comparator for int64_t */
static int cmp_i64(const void *a, const void *b) {
    int64_t x = *(const int64_t *)a;
    int64_t y = *(const int64_t *)b;
    return (x > y) - (x < y);
}

void tombstone_write(const char *del_path, const int64_t *rows, int64_t n_rows) {
    if (n_rows <= 0) return;

    /* Load existing entries */
    TombstoneSet *existing = tombstone_load(del_path);
    int64_t existing_n = existing ? existing->n : 0;

    /* Merge: allocate union buffer */
    int64_t total = existing_n + n_rows;
    int64_t *merged = (int64_t *)malloc((size_t)total * sizeof(int64_t));
    if (!merged) {
        tombstone_free(existing);
        vectra_error("tombstone_write: alloc failed");
    }

    if (existing_n > 0)
        memcpy(merged, existing->rows, (size_t)existing_n * sizeof(int64_t));
    memcpy(merged + existing_n, rows, (size_t)n_rows * sizeof(int64_t));
    tombstone_free(existing);

    /* Sort */
    qsort(merged, (size_t)total, sizeof(int64_t), cmp_i64);

    /* Deduplicate */
    int64_t unique_n = 0;
    for (int64_t i = 0; i < total; i++) {
        if (unique_n == 0 || merged[i] != merged[unique_n - 1])
            merged[unique_n++] = merged[i];
    }

    /* Write atomically: temp file then rename */
    size_t del_len = strlen(del_path);
    char *tmp_path = (char *)malloc(del_len + 10);
    if (!tmp_path) { free(merged); vectra_error("tombstone_write: alloc failed for tmp path"); }
    memcpy(tmp_path, del_path, del_len);
    memcpy(tmp_path + del_len, ".~writing", 10);

    FILE *fp = fopen(tmp_path, "w");
    if (!fp) {
        char err_copy[512];
        snprintf(err_copy, sizeof(err_copy),
                 "tombstone_write: cannot open temp file: %s", tmp_path);
        free(merged);
        free(tmp_path);
        vectra_error("%s", err_copy);
    }

    for (int64_t i = 0; i < unique_n; i++) {
        /* Use PRId64 manually for portability */
        char buf[32];
        int len = snprintf(buf, sizeof(buf), "%lld\n", (long long)merged[i]);
        if (len > 0) fwrite(buf, 1, (size_t)len, fp);
    }
    fclose(fp);
    free(merged);

    if (vtr_atomic_replace(tmp_path, del_path) != 0) {
        remove(tmp_path);
        free(tmp_path);
        vectra_error("tombstone_write: failed to rename temp file to: %s", del_path);
    }
    free(tmp_path);
}

/* --- .Call bridge --- */

SEXP C_delete_vtr(SEXP path_sexp, SEXP row_indices) {
    const char *vtr_path = CHAR(STRING_ELT(path_sexp, 0));

    /* Build .del path: vtr_path + ".del" */
    size_t vpath_len = strlen(vtr_path);
    char *del_path = (char *)malloc(vpath_len + 5);
    if (!del_path) vectra_error("C_delete_vtr: alloc failed");
    memcpy(del_path, vtr_path, vpath_len);
    memcpy(del_path + vpath_len, ".del", 5); /* includes '\0' */

    /* Convert R integer or double vector to int64_t array */
    R_xlen_t n = XLENGTH(row_indices);
    int64_t *rows = (int64_t *)malloc((size_t)(n > 0 ? n : 1) * sizeof(int64_t));
    if (!rows) { free(del_path); vectra_error("C_delete_vtr: alloc failed for rows"); }

    if (TYPEOF(row_indices) == REALSXP) {
        double *dp = REAL(row_indices);
        for (R_xlen_t i = 0; i < n; i++)
            rows[i] = (int64_t)dp[i];
    } else {
        int *ip = INTEGER(row_indices);
        for (R_xlen_t i = 0; i < n; i++)
            rows[i] = (int64_t)ip[i];
    }

    tombstone_write(del_path, rows, (int64_t)n);

    free(rows);
    free(del_path);
    return R_NilValue;
}
