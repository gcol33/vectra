#ifndef VECTRA_SQLITE_FORMAT_H
#define VECTRA_SQLITE_FORMAT_H

#include <stdint.h>

/* ------------------------------------------------------------------ */
/*  Minimal SQLite file format reader/writer.                          */
/*  No SQL parser — just table scan and table creation.                */
/* ------------------------------------------------------------------ */

/* Column value types (matches SQLite fundamental types) */
#define SQLFMT_NULL    0
#define SQLFMT_INTEGER 1
#define SQLFMT_FLOAT   2
#define SQLFMT_TEXT    3

#define SQLFMT_MAX_COLS 256

/* ------------------------------------------------------------------ */
/*  Reader                                                             */
/* ------------------------------------------------------------------ */

typedef struct SqlfmtReader SqlfmtReader;

/* Open a SQLite database and prepare to scan a table.
   Returns 0 on success, -1 on error. */
int sqlfmt_reader_open(const char *path, const char *table,
                        SqlfmtReader **out);

int sqlfmt_reader_ncols(SqlfmtReader *r);
const char *sqlfmt_reader_colname(SqlfmtReader *r, int col);
const char *sqlfmt_reader_coltype(SqlfmtReader *r, int col);

/* Advance to next row. Returns 1 if a row is available, 0 if done. */
int sqlfmt_reader_step(SqlfmtReader *r);

/* Access current row values (valid after step() returns 1) */
int     sqlfmt_reader_col_type(SqlfmtReader *r, int col);
int64_t sqlfmt_reader_int64(SqlfmtReader *r, int col);
double  sqlfmt_reader_double(SqlfmtReader *r, int col);
const char *sqlfmt_reader_text(SqlfmtReader *r, int col);
int     sqlfmt_reader_bytes(SqlfmtReader *r, int col);

const char *sqlfmt_reader_errmsg(SqlfmtReader *r);
void sqlfmt_reader_close(SqlfmtReader *r);

/* ------------------------------------------------------------------ */
/*  Writer                                                             */
/* ------------------------------------------------------------------ */

typedef struct SqlfmtWriter SqlfmtWriter;

/* Create a new SQLite database with a single table.
   col_types: "INTEGER", "REAL", "TEXT", etc.
   Returns 0 on success, -1 on error. */
int sqlfmt_writer_create(const char *path, const char *table,
                          int n_cols, const char **col_names,
                          const char **col_types,
                          SqlfmtWriter **out);

/* Bind values for the current row */
void sqlfmt_writer_bind_null(SqlfmtWriter *w, int col);
void sqlfmt_writer_bind_int64(SqlfmtWriter *w, int col, int64_t val);
void sqlfmt_writer_bind_double(SqlfmtWriter *w, int col, double val);
void sqlfmt_writer_bind_text(SqlfmtWriter *w, int col,
                              const char *text, int len);

/* Write the current row. Returns 0 on success. */
int sqlfmt_writer_insert(SqlfmtWriter *w);

const char *sqlfmt_writer_errmsg(SqlfmtWriter *w);

/* Finalize: build B-tree structure and close. */
void sqlfmt_writer_close(SqlfmtWriter *w);

#endif /* VECTRA_SQLITE_FORMAT_H */
