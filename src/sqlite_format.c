#include "sqlite_format.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

/* ================================================================== */
/*  Part 1: Utilities — big-endian I/O, varint codec                   */
/* ================================================================== */

static uint16_t read_be16(const uint8_t *p) {
    return (uint16_t)((p[0] << 8) | p[1]);
}

static uint32_t read_be32(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8)  | (uint32_t)p[3];
}

static void write_be16(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)(v);
}

static void write_be32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)(v);
}

static int64_t read_be_signed(const uint8_t *p, int n) {
    int64_t v = 0;
    /* Sign-extend from the first byte */
    if (n > 0 && (p[0] & 0x80))
        v = -1; /* all 1s */
    for (int i = 0; i < n; i++)
        v = (v << 8) | p[i];
    return v;
}

/* SQLite varint: 1-9 bytes, high bit = continuation, 9th byte uses all 8 bits */
static int read_varint(const uint8_t *p, int64_t *out) {
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++) {
        v = (v << 7) | (p[i] & 0x7F);
        if ((p[i] & 0x80) == 0) {
            *out = (int64_t)v;
            return i + 1;
        }
    }
    /* 9th byte: all 8 bits are data */
    v = (v << 8) | p[8];
    *out = (int64_t)v;
    return 9;
}

static int write_varint(uint8_t *p, int64_t val) {
    uint64_t v = (uint64_t)val;

    if (v <= 0x7F) {
        p[0] = (uint8_t)v;
        return 1;
    }

    /* Collect 7-bit groups in reverse order */
    uint8_t buf[9];
    int n = 0;
    while (v > 0x7F && n < 8) {
        buf[n++] = (uint8_t)(v & 0x7F);
        v >>= 7;
    }
    buf[n++] = (uint8_t)(v & 0x7F);

    /* Write in forward order: all but last get high bit set */
    for (int i = 0; i < n; i++) {
        if (i < n - 1)
            p[i] = buf[n - 1 - i] | 0x80;
        else
            p[i] = buf[0];
    }
    return n;
}

/* ================================================================== */
/*  Part 2: Reader internals                                           */
/* ================================================================== */

#define MAX_BTREE_DEPTH 20
#define PAGE1_HEADER_OFFSET 100 /* B-tree header on page 1 starts at 100 */

typedef struct {
    uint32_t page_no;
    int      cell_idx;     /* next cell/child to process */
    int      n_cells;
    int      is_leaf;
    uint32_t right_child;  /* interior pages only */
    uint8_t *page_buf;     /* page_size bytes */
    int      hdr_offset;   /* 100 for page 1, 0 otherwise */
} BtreeLevel;

/* Per-column info for current row */
typedef struct {
    int     serial_type;
    int64_t offset;   /* byte offset in record data */
    int64_t length;   /* byte length of value */
} ColInfo;

struct SqlfmtReader {
    FILE     *fp;
    uint32_t  page_size;
    uint32_t  n_pages;
    uint16_t  reserved;

    /* Table schema */
    int  n_cols;
    char *col_names[SQLFMT_MAX_COLS];
    char *col_types[SQLFMT_MAX_COLS];
    uint32_t root_page;

    /* B-tree cursor */
    BtreeLevel stack[MAX_BTREE_DEPTH];
    int depth;  /* -1 = done */
    int started;

    /* Current row */
    uint8_t *record_buf;
    int64_t  record_cap;
    int64_t  record_len;
    ColInfo  cols[SQLFMT_MAX_COLS];
    int      cur_n_cols; /* columns in current record */

    char errmsg[256];
};

static int read_page(SqlfmtReader *r, uint32_t page_no, uint8_t *buf) {
    int64_t offset = (int64_t)(page_no - 1) * r->page_size;
    if (fseek(r->fp, (long)offset, SEEK_SET) != 0) return -1;
    if (fread(buf, 1, r->page_size, r->fp) != r->page_size) return -1;
    return 0;
}

/* Initialize a B-tree level from a page */
static int init_level(SqlfmtReader *r, int lvl, uint32_t page_no) {
    BtreeLevel *L = &r->stack[lvl];
    L->page_no = page_no;
    L->cell_idx = 0;
    L->hdr_offset = (page_no == 1) ? PAGE1_HEADER_OFFSET : 0;

    if (!L->page_buf) {
        L->page_buf = (uint8_t *)malloc(r->page_size);
        if (!L->page_buf) return -1;
    }
    if (read_page(r, page_no, L->page_buf) != 0) return -1;

    uint8_t *hdr = L->page_buf + L->hdr_offset;
    uint8_t ptype = hdr[0];
    L->is_leaf = (ptype == 0x0D);
    L->n_cells = read_be16(hdr + 3);
    L->right_child = L->is_leaf ? 0 : read_be32(hdr + 8);
    return 0;
}

/* Get byte offset of cell pointer i within the page */
static uint16_t cell_ptr(BtreeLevel *L, int i) {
    int ptr_offset = L->hdr_offset + (L->is_leaf ? 8 : 12) + 2 * i;
    return read_be16(L->page_buf + ptr_offset);
}

/* Serial type → byte length of stored value */
static int64_t serial_type_len(int st) {
    if (st <= 4) {
        static const int64_t lens[] = {0, 1, 2, 3, 4};
        return lens[st];
    }
    if (st == 5) return 6;
    if (st == 6 || st == 7) return 8;
    if (st == 8 || st == 9) return 0;
    if (st >= 12 && (st & 1) == 0) return (st - 12) / 2; /* blob */
    if (st >= 13 && (st & 1) == 1) return (st - 13) / 2; /* text */
    return 0;
}

/* Read a cell payload, handling overflow pages.
   cell_data points to the start of the payload (after rowid varint for leaf cells).
   total_payload = declared payload size.
   Writes into r->record_buf (reallocs if needed). */
static int read_payload(SqlfmtReader *r, const uint8_t *cell_data,
                         int64_t total_payload, int64_t local_size,
                         uint32_t overflow_page) {
    if (total_payload > r->record_cap) {
        r->record_cap = total_payload + 256;
        r->record_buf = (uint8_t *)realloc(r->record_buf, (size_t)r->record_cap);
        if (!r->record_buf) return -1;
    }
    r->record_len = total_payload;

    /* Copy local portion */
    memcpy(r->record_buf, cell_data, (size_t)local_size);

    /* Read overflow pages if any */
    int64_t copied = local_size;
    uint32_t ovfl = overflow_page;
    uint32_t usable = r->page_size - r->reserved;

    uint8_t *ovfl_buf = NULL;
    if (ovfl != 0) {
        ovfl_buf = (uint8_t *)malloc(r->page_size);
        if (!ovfl_buf) return -1;
    }

    while (copied < total_payload && ovfl != 0) {
        if (read_page(r, ovfl, ovfl_buf) != 0) { free(ovfl_buf); return -1; }
        uint32_t next = read_be32(ovfl_buf);
        int64_t avail = (int64_t)(usable - 4);
        int64_t need = total_payload - copied;
        int64_t take = need < avail ? need : avail;
        memcpy(r->record_buf + copied, ovfl_buf + 4, (size_t)take);
        copied += take;
        ovfl = next;
    }

    free(ovfl_buf);
    return 0;
}

/* Parse the record format in r->record_buf into r->cols[].
   Record: header_size(varint) serial_types... data... */
static int parse_record(SqlfmtReader *r) {
    const uint8_t *p = r->record_buf;
    int64_t hdr_size;
    int off = read_varint(p, &hdr_size);

    int ncol = 0;
    int64_t data_offset = hdr_size;
    while (off < hdr_size && ncol < SQLFMT_MAX_COLS) {
        int64_t st;
        off += read_varint(p + off, &st);
        r->cols[ncol].serial_type = (int)st;
        r->cols[ncol].offset = data_offset;
        r->cols[ncol].length = serial_type_len((int)st);
        data_offset += r->cols[ncol].length;
        ncol++;
    }
    r->cur_n_cols = ncol;
    return 0;
}

/* Read next leaf cell. Returns 1 if got a row, 0 if page exhausted. */
static int read_leaf_cell(SqlfmtReader *r) {
    BtreeLevel *L = &r->stack[r->depth];
    if (L->cell_idx >= L->n_cells) return 0;

    uint16_t cp = cell_ptr(L, L->cell_idx);
    const uint8_t *cell = L->page_buf + cp;
    int off = 0;

    /* Payload size (varint) */
    int64_t payload_size;
    off += read_varint(cell + off, &payload_size);

    /* Rowid (varint) */
    int64_t rowid;
    off += read_varint(cell + off, &rowid);
    (void)rowid;

    /* Compute local payload size and overflow page */
    uint32_t usable = r->page_size - r->reserved;
    int64_t X = (int64_t)usable - 35;
    int64_t local_size;
    uint32_t overflow_page = 0;

    if (payload_size <= X) {
        local_size = payload_size;
    } else {
        int64_t M = (((int64_t)usable - 12) * 32 / 255) - 23;
        local_size = M;
        if (local_size > payload_size) local_size = payload_size;
        /* Overflow page number follows the local payload */
        overflow_page = read_be32(cell + off + (int)local_size);
    }

    if (read_payload(r, cell + off, payload_size, local_size, overflow_page) != 0)
        return -1;
    if (parse_record(r) != 0)
        return -1;

    L->cell_idx++;
    return 1;
}

/* Advance B-tree cursor to next row. Returns 1=row, 0=done, -1=error. */
static int btree_next(SqlfmtReader *r) {
    while (r->depth >= 0) {
        BtreeLevel *L = &r->stack[r->depth];

        if (L->is_leaf) {
            int rc = read_leaf_cell(r);
            if (rc == 1) return 1;
            if (rc < 0) return -1;
            /* Leaf exhausted, pop */
            r->depth--;
            continue;
        }

        /* Interior page: cell_idx tracks which child to visit next.
           Children: cell[0].left, cell[1].left, ..., cell[n-1].left, right_child
           That's n_cells + 1 children. cell_idx counts which one is next. */
        if (L->cell_idx <= L->n_cells) {
            uint32_t child;
            if (L->cell_idx < L->n_cells) {
                uint16_t cp = cell_ptr(L, L->cell_idx);
                child = read_be32(L->page_buf + cp);
            } else {
                child = L->right_child;
            }
            L->cell_idx++;

            /* Push child */
            r->depth++;
            if (r->depth >= MAX_BTREE_DEPTH) return -1;
            if (init_level(r, r->depth, child) != 0) return -1;
            continue;
        }

        /* All children visited, pop */
        r->depth--;
    }
    return 0; /* done */
}

/* ================================================================== */
/*  Part 3: Schema parsing — extract table info from sqlite_master     */
/* ================================================================== */

/* Minimal CREATE TABLE parser.
   Input: "CREATE TABLE "tbl" ("col1" TYPE1, "col2" TYPE2, ...)"
   Extracts column names and declared types. */
static int parse_create_table(const char *sql, int max_cols,
                               char **names, char **types, int *out_ncols) {
    /* Find opening parenthesis */
    const char *p = strchr(sql, '(');
    if (!p) return -1;
    p++; /* skip '(' */

    int ncol = 0;
    while (*p && *p != ')' && ncol < max_cols) {
        /* Skip whitespace */
        while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
        if (*p == ')') break;

        /* Read column name (possibly quoted) */
        char name_buf[256];
        int ni = 0;
        if (*p == '"' || *p == '`' || *p == '[') {
            char close = (*p == '[') ? ']' : *p;
            p++;
            while (*p && *p != close && ni < 255)
                name_buf[ni++] = *p++;
            if (*p == close) p++;
        } else {
            while (*p && *p != ' ' && *p != ',' && *p != ')' && ni < 255)
                name_buf[ni++] = *p++;
        }
        name_buf[ni] = '\0';

        /* Skip whitespace */
        while (*p == ' ' || *p == '\t') p++;

        /* Read type (until comma, closing paren, or constraint keywords) */
        char type_buf[256];
        int ti = 0;
        /* Collect type tokens until we hit a comma or closing paren.
           Stop at known constraint keywords too. */
        int paren_depth = 0;
        while (*p && ti < 255) {
            if (*p == '(') { paren_depth++; type_buf[ti++] = *p++; continue; }
            if (*p == ')' && paren_depth > 0) { paren_depth--; type_buf[ti++] = *p++; continue; }
            if ((*p == ',' || *p == ')') && paren_depth == 0) break;
            type_buf[ti++] = *p++;
        }
        type_buf[ti] = '\0';

        /* Trim trailing whitespace from type */
        while (ti > 0 && (type_buf[ti-1] == ' ' || type_buf[ti-1] == '\t'))
            type_buf[--ti] = '\0';

        /* Store */
        names[ncol] = (char *)malloc(strlen(name_buf) + 1);
        strcpy(names[ncol], name_buf);
        types[ncol] = (char *)malloc(strlen(type_buf) + 1);
        strcpy(types[ncol], type_buf);
        ncol++;

        /* Skip comma */
        if (*p == ',') p++;
    }

    *out_ncols = ncol;
    return 0;
}

/* Scan sqlite_master (page 1) to find a table by name.
   Sets r->root_page, r->n_cols, r->col_names[], r->col_types[]. */
static int find_table(SqlfmtReader *r, const char *table_name) {
    /* Save current cursor state; we'll scan page 1's B-tree temporarily.
       Page 1 is the sqlite_master table with schema:
       (type TEXT, name TEXT, tbl_name TEXT, rootpage INTEGER, sql TEXT) */

    /* Initialize cursor on page 1 */
    r->depth = 0;
    if (init_level(r, 0, 1) != 0) {
        snprintf(r->errmsg, 256, "cannot read page 1");
        return -1;
    }

    int found = 0;
    while (!found) {
        int rc = btree_next(r);
        if (rc == 0) break;
        if (rc < 0) {
            snprintf(r->errmsg, 256, "error scanning sqlite_master");
            return -1;
        }

        /* sqlite_master has 5 columns: type, name, tbl_name, rootpage, sql */
        if (r->cur_n_cols < 5) continue;

        /* Column 0 (type): must be "table" */
        if (r->cols[0].serial_type < 13) continue; /* not text */
        int64_t type_len = r->cols[0].length;
        const char *type_str = (const char *)(r->record_buf + r->cols[0].offset);
        if (type_len != 5 || memcmp(type_str, "table", 5) != 0) continue;

        /* Column 1 (name): must match table_name */
        if (r->cols[1].serial_type < 13) continue;
        int64_t name_len = r->cols[1].length;
        const char *name_str = (const char *)(r->record_buf + r->cols[1].offset);
        if ((int64_t)strlen(table_name) != name_len) continue;
        if (memcmp(name_str, table_name, (size_t)name_len) != 0) continue;

        /* Column 3 (rootpage): integer */
        int st3 = r->cols[3].serial_type;
        int64_t rootpage = 0;
        if (st3 >= 1 && st3 <= 6) {
            rootpage = read_be_signed(
                r->record_buf + r->cols[3].offset,
                (int)r->cols[3].length);
        } else if (st3 == 8) {
            rootpage = 0;
        } else if (st3 == 9) {
            rootpage = 1;
        }
        r->root_page = (uint32_t)rootpage;

        /* Column 4 (sql): parse CREATE TABLE */
        if (r->cols[4].serial_type < 13) continue;
        int64_t sql_len = r->cols[4].length;
        char *sql = (char *)malloc((size_t)(sql_len + 1));
        memcpy(sql, r->record_buf + r->cols[4].offset, (size_t)sql_len);
        sql[sql_len] = '\0';

        if (parse_create_table(sql, SQLFMT_MAX_COLS,
                                r->col_names, r->col_types,
                                &r->n_cols) != 0) {
            free(sql);
            snprintf(r->errmsg, 256, "cannot parse CREATE TABLE");
            return -1;
        }
        free(sql);
        found = 1;
    }

    if (!found) {
        snprintf(r->errmsg, 256, "table not found: %s", table_name);
        return -1;
    }

    return 0;
}

/* ================================================================== */
/*  Part 4: Reader public API                                          */
/* ================================================================== */

int sqlfmt_reader_open(const char *path, const char *table,
                        SqlfmtReader **out) {
    SqlfmtReader *r = (SqlfmtReader *)calloc(1, sizeof(SqlfmtReader));
    if (!r) return -1;

    r->fp = fopen(path, "rb");
    if (!r->fp) {
        snprintf(r->errmsg, 256, "cannot open: %s", path);
        *out = r;
        return -1;
    }

    /* Read database header (first 100 bytes) */
    uint8_t hdr[100];
    if (fread(hdr, 1, 100, r->fp) != 100) {
        snprintf(r->errmsg, 256, "cannot read header");
        *out = r;
        return -1;
    }

    /* Verify magic */
    if (memcmp(hdr, "SQLite format 3\000", 16) != 0) {
        snprintf(r->errmsg, 256, "not a SQLite database");
        *out = r;
        return -1;
    }

    /* Page size: 2 bytes at offset 16. Value of 1 means 65536. */
    uint16_t raw_ps = read_be16(hdr + 16);
    r->page_size = (raw_ps == 1) ? 65536 : (uint32_t)raw_ps;
    r->reserved = hdr[20];
    r->n_pages = read_be32(hdr + 28);

    /* Only support UTF-8 */
    uint32_t encoding = read_be32(hdr + 56);
    if (encoding != 1) {
        snprintf(r->errmsg, 256, "only UTF-8 databases supported (got %u)",
                 encoding);
        *out = r;
        return -1;
    }

    /* Allocate record buffer */
    r->record_cap = 4096;
    r->record_buf = (uint8_t *)malloc((size_t)r->record_cap);
    if (!r->record_buf) { *out = r; return -1; }

    /* Find table in sqlite_master */
    if (find_table(r, table) != 0) {
        *out = r;
        return -1;
    }

    /* Initialize cursor on the data table's root page */
    r->depth = 0;
    r->started = 0;
    if (init_level(r, 0, r->root_page) != 0) {
        snprintf(r->errmsg, 256, "cannot read root page %u", r->root_page);
        *out = r;
        return -1;
    }

    *out = r;
    return 0;
}

int sqlfmt_reader_ncols(SqlfmtReader *r) { return r->n_cols; }

const char *sqlfmt_reader_colname(SqlfmtReader *r, int col) {
    return (col >= 0 && col < r->n_cols) ? r->col_names[col] : "";
}

const char *sqlfmt_reader_coltype(SqlfmtReader *r, int col) {
    return (col >= 0 && col < r->n_cols) ? r->col_types[col] : "";
}

int sqlfmt_reader_step(SqlfmtReader *r) {
    return btree_next(r);
}

int sqlfmt_reader_col_type(SqlfmtReader *r, int col) {
    if (col < 0 || col >= r->cur_n_cols) return SQLFMT_NULL;
    int st = r->cols[col].serial_type;
    if (st == 0) return SQLFMT_NULL;
    if (st >= 1 && st <= 6) return SQLFMT_INTEGER;
    if (st == 7) return SQLFMT_FLOAT;
    if (st == 8 || st == 9) return SQLFMT_INTEGER;
    if (st >= 13 && (st & 1) == 1) return SQLFMT_TEXT;
    return SQLFMT_TEXT; /* blob → text fallback */
}

int64_t sqlfmt_reader_int64(SqlfmtReader *r, int col) {
    if (col < 0 || col >= r->cur_n_cols) return 0;
    int st = r->cols[col].serial_type;
    if (st == 8) return 0;
    if (st == 9) return 1;
    if (st >= 1 && st <= 6) {
        return read_be_signed(r->record_buf + r->cols[col].offset,
                              (int)r->cols[col].length);
    }
    return 0;
}

double sqlfmt_reader_double(SqlfmtReader *r, int col) {
    if (col < 0 || col >= r->cur_n_cols) return 0.0;
    int st = r->cols[col].serial_type;
    if (st == 7) {
        /* IEEE 754 big-endian */
        uint64_t bits = 0;
        const uint8_t *p = r->record_buf + r->cols[col].offset;
        for (int i = 0; i < 8; i++)
            bits = (bits << 8) | p[i];
        double d;
        memcpy(&d, &bits, 8);
        return d;
    }
    /* Integer types → convert */
    return (double)sqlfmt_reader_int64(r, col);
}

const char *sqlfmt_reader_text(SqlfmtReader *r, int col) {
    if (col < 0 || col >= r->cur_n_cols) return "";
    int st = r->cols[col].serial_type;
    if (st < 13 || (st & 1) != 1) return "";
    int64_t off = r->cols[col].offset;
    int64_t len = r->cols[col].length;
    /* Copy to a separate buffer to avoid clobbering adjacent record data
       with the null terminator */
    static char text_buf[65536];
    if (len >= (int64_t)sizeof(text_buf)) len = sizeof(text_buf) - 1;
    memcpy(text_buf, r->record_buf + off, (size_t)len);
    text_buf[len] = '\0';
    return text_buf;
}

int sqlfmt_reader_bytes(SqlfmtReader *r, int col) {
    if (col < 0 || col >= r->cur_n_cols) return 0;
    return (int)r->cols[col].length;
}

const char *sqlfmt_reader_errmsg(SqlfmtReader *r) {
    return r->errmsg;
}

void sqlfmt_reader_close(SqlfmtReader *r) {
    if (!r) return;
    if (r->fp) fclose(r->fp);
    for (int i = 0; i < MAX_BTREE_DEPTH; i++)
        free(r->stack[i].page_buf);
    free(r->record_buf);
    for (int i = 0; i < r->n_cols; i++) {
        free(r->col_names[i]);
        free(r->col_types[i]);
    }
    free(r);
}

/* ================================================================== */
/*  Part 5: Writer internals                                           */
/* ================================================================== */

#define WRITER_PAGE_SIZE 4096

typedef struct {
    uint8_t *data;
    int64_t  len;
    int64_t  cap;
} GrowBuf;

static void gbuf_init_w(GrowBuf *g, int64_t cap) {
    g->data = (uint8_t *)malloc((size_t)cap);
    g->len = 0;
    g->cap = cap;
}

static void gbuf_ensure(GrowBuf *g, int64_t need) {
    if (g->len + need > g->cap) {
        while (g->cap < g->len + need) g->cap *= 2;
        g->data = (uint8_t *)realloc(g->data, (size_t)g->cap);
    }
}

/* Bound value for a column */
typedef struct {
    int type; /* SQLFMT_NULL, SQLFMT_INTEGER, SQLFMT_FLOAT, SQLFMT_TEXT */
    int64_t i64;
    double  dbl;
    char   *text;
    int     text_len;
} BoundVal;

struct SqlfmtWriter {
    FILE *fp;
    char *table_name;
    int   n_cols;
    char *col_names[SQLFMT_MAX_COLS];
    char *col_types[SQLFMT_MAX_COLS];

    BoundVal cur_row[SQLFMT_MAX_COLS];

    /* Current leaf page being built */
    uint8_t cur_page[WRITER_PAGE_SIZE];
    int     cur_n_cells;
    int     cur_content_end;   /* offset where content area starts (grows down) */
    int     cur_ptr_end;       /* offset past the last cell pointer (grows up) */

    /* Completed leaf pages */
    struct {
        uint32_t page_no;
        int64_t  max_rowid;
    } *leaves;
    int n_leaves;
    int leaves_cap;

    int64_t next_rowid;
    uint32_t next_page_no; /* next page to write */

    char errmsg[256];
};

/* Build a SQLite record from bound values.
   Returns the record in buf, length in *out_len. */
static int build_record(SqlfmtWriter *w, GrowBuf *buf) {
    buf->len = 0;

    /* First pass: compute serial types and header size */
    int serial_types[SQLFMT_MAX_COLS];
    uint8_t hdr_varints[SQLFMT_MAX_COLS * 9]; /* max varint bytes per type */
    int hdr_varint_len = 0;

    for (int c = 0; c < w->n_cols; c++) {
        BoundVal *v = &w->cur_row[c];
        int st;
        switch (v->type) {
        case SQLFMT_NULL:    st = 0; break;
        case SQLFMT_INTEGER: {
            int64_t val = v->i64;
            if (val == 0) st = 8;
            else if (val == 1) st = 9;
            else if (val >= -128 && val <= 127) st = 1;
            else if (val >= -32768 && val <= 32767) st = 2;
            else if (val >= -8388608 && val <= 8388607) st = 3;
            else if (val >= -2147483648LL && val <= 2147483647LL) st = 4;
            else if (val >= -140737488355328LL && val <= 140737488355327LL) st = 5;
            else st = 6;
            break;
        }
        case SQLFMT_FLOAT:   st = 7; break;
        case SQLFMT_TEXT:    st = 13 + 2 * v->text_len; break;
        default:             st = 0; break;
        }
        serial_types[c] = st;
        hdr_varint_len += write_varint(hdr_varints + hdr_varint_len, st);
    }

    /* Header size varint (includes itself) */
    /* Try encoding header size; it might be 1 or 2 bytes */
    int hdr_size = hdr_varint_len + 1; /* assume 1-byte size varint */
    if (hdr_size > 127) hdr_size = hdr_varint_len + 2;

    /* Write header */
    gbuf_ensure(buf, hdr_size + w->n_cols * 9 + 1024);
    buf->len += write_varint(buf->data + buf->len, hdr_size);
    memcpy(buf->data + buf->len, hdr_varints, (size_t)hdr_varint_len);
    buf->len += hdr_varint_len;

    /* Write data */
    for (int c = 0; c < w->n_cols; c++) {
        BoundVal *v = &w->cur_row[c];
        int st = serial_types[c];

        if (st == 0 || st == 8 || st == 9) continue; /* zero-length */

        gbuf_ensure(buf, buf->len + 8 + (v->type == SQLFMT_TEXT ? v->text_len : 0));

        if (st >= 1 && st <= 6) {
            int n = (int)serial_type_len(st);
            /* Write big-endian integer */
            int64_t val = v->i64;
            for (int i = n - 1; i >= 0; i--) {
                buf->data[buf->len + i] = (uint8_t)(val & 0xFF);
                val >>= 8;
            }
            buf->len += n;
        } else if (st == 7) {
            /* float64, big-endian IEEE 754 */
            uint64_t bits;
            memcpy(&bits, &v->dbl, 8);
            uint8_t *p = buf->data + buf->len;
            for (int i = 7; i >= 0; i--) {
                p[i] = (uint8_t)(bits & 0xFF);
                bits >>= 8;
            }
            buf->len += 8;
        } else if (st >= 13 && (st & 1) == 1) {
            /* text */
            memcpy(buf->data + buf->len, v->text, (size_t)v->text_len);
            buf->len += v->text_len;
        }
    }

    return 0;
}

static void flush_leaf_page(SqlfmtWriter *w, int64_t max_rowid) {
    /* Write the current page to disk */
    uint8_t *pg = w->cur_page;

    /* Update leaf page header */
    pg[0] = 0x0D; /* table B-tree leaf */
    write_be16(pg + 1, 0); /* first freeblock */
    write_be16(pg + 3, (uint16_t)w->cur_n_cells);
    write_be16(pg + 5, (uint16_t)w->cur_content_end);
    pg[7] = 0; /* fragmented free bytes */

    int64_t offset = (int64_t)(w->next_page_no - 1) * WRITER_PAGE_SIZE;
    fseek(w->fp, (long)offset, SEEK_SET);
    fwrite(pg, 1, WRITER_PAGE_SIZE, w->fp);

    /* Track this leaf */
    if (w->n_leaves >= w->leaves_cap) {
        w->leaves_cap = w->leaves_cap ? w->leaves_cap * 2 : 64;
        w->leaves = realloc(w->leaves,
                             (size_t)w->leaves_cap * sizeof(w->leaves[0]));
    }
    w->leaves[w->n_leaves].page_no = w->next_page_no;
    w->leaves[w->n_leaves].max_rowid = max_rowid;
    w->n_leaves++;

    w->next_page_no++;
}

static void init_leaf_page(SqlfmtWriter *w) {
    memset(w->cur_page, 0, WRITER_PAGE_SIZE);
    w->cur_n_cells = 0;
    w->cur_content_end = WRITER_PAGE_SIZE;
    w->cur_ptr_end = 8; /* leaf header is 8 bytes */
}

/* ================================================================== */
/*  Part 6: Writer public API                                          */
/* ================================================================== */

int sqlfmt_writer_create(const char *path, const char *table,
                          int n_cols, const char **col_names,
                          const char **col_types, SqlfmtWriter **out) {
    SqlfmtWriter *w = (SqlfmtWriter *)calloc(1, sizeof(SqlfmtWriter));
    if (!w) return -1;

    w->fp = fopen(path, "w+b");
    if (!w->fp) {
        snprintf(w->errmsg, 256, "cannot create: %s", path);
        *out = w;
        return -1;
    }

    w->n_cols = n_cols;
    w->table_name = (char *)malloc(strlen(table) + 1);
    strcpy(w->table_name, table);

    for (int i = 0; i < n_cols; i++) {
        w->col_names[i] = (char *)malloc(strlen(col_names[i]) + 1);
        strcpy(w->col_names[i], col_names[i]);
        w->col_types[i] = (char *)malloc(strlen(col_types[i]) + 1);
        strcpy(w->col_types[i], col_types[i]);
    }

    w->next_rowid = 1;
    w->next_page_no = 2; /* page 1 is reserved for schema */
    w->n_leaves = 0;
    w->leaves_cap = 0;
    w->leaves = NULL;

    /* Write placeholder page 1 (will be overwritten at close) */
    uint8_t zeros[WRITER_PAGE_SIZE];
    memset(zeros, 0, WRITER_PAGE_SIZE);
    fwrite(zeros, 1, WRITER_PAGE_SIZE, w->fp);

    /* Initialize first data leaf page */
    init_leaf_page(w);

    *out = w;
    return 0;
}

void sqlfmt_writer_bind_null(SqlfmtWriter *w, int col) {
    if (col >= 0 && col < w->n_cols) w->cur_row[col].type = SQLFMT_NULL;
}

void sqlfmt_writer_bind_int64(SqlfmtWriter *w, int col, int64_t val) {
    if (col >= 0 && col < w->n_cols) {
        w->cur_row[col].type = SQLFMT_INTEGER;
        w->cur_row[col].i64 = val;
    }
}

void sqlfmt_writer_bind_double(SqlfmtWriter *w, int col, double val) {
    if (col >= 0 && col < w->n_cols) {
        w->cur_row[col].type = SQLFMT_FLOAT;
        w->cur_row[col].dbl = val;
    }
}

void sqlfmt_writer_bind_text(SqlfmtWriter *w, int col,
                              const char *text, int len) {
    if (col >= 0 && col < w->n_cols) {
        w->cur_row[col].type = SQLFMT_TEXT;
        /* Free previous text if any */
        free(w->cur_row[col].text);
        w->cur_row[col].text = (char *)malloc((size_t)(len + 1));
        memcpy(w->cur_row[col].text, text, (size_t)len);
        w->cur_row[col].text[len] = '\0';
        w->cur_row[col].text_len = len;
    }
}

int sqlfmt_writer_insert(SqlfmtWriter *w) {
    GrowBuf rec;
    gbuf_init_w(&rec, 512);
    build_record(w, &rec);

    int64_t rowid = w->next_rowid++;

    /* Build cell: payload_size(varint) + rowid(varint) + record */
    uint8_t cell_hdr[18];
    int hdr_len = 0;
    hdr_len += write_varint(cell_hdr + hdr_len, rec.len);
    hdr_len += write_varint(cell_hdr + hdr_len, rowid);

    int cell_total = hdr_len + (int)rec.len;

    /* Check if cell fits in current page */
    int avail = w->cur_content_end - w->cur_ptr_end - 2; /* -2 for cell ptr */
    if (avail < cell_total) {
        /* Flush current page */
        flush_leaf_page(w, rowid - 1);
        init_leaf_page(w);
    }

    /* Write cell content at end of page (growing down) */
    w->cur_content_end -= cell_total;
    memcpy(w->cur_page + w->cur_content_end, cell_hdr, (size_t)hdr_len);
    memcpy(w->cur_page + w->cur_content_end + hdr_len,
           rec.data, (size_t)rec.len);

    /* Write cell pointer */
    write_be16(w->cur_page + w->cur_ptr_end,
               (uint16_t)w->cur_content_end);
    w->cur_ptr_end += 2;
    w->cur_n_cells++;

    /* Free bound text values */
    for (int c = 0; c < w->n_cols; c++) {
        if (w->cur_row[c].type == SQLFMT_TEXT) {
            free(w->cur_row[c].text);
            w->cur_row[c].text = NULL;
        }
    }

    free(rec.data);
    return 0;
}

const char *sqlfmt_writer_errmsg(SqlfmtWriter *w) {
    return w->errmsg;
}

void sqlfmt_writer_close(SqlfmtWriter *w) {
    if (!w) return;

    /* Flush remaining data if any */
    if (w->cur_n_cells > 0) {
        flush_leaf_page(w, w->next_rowid - 1);
    }

    /* Determine root page for data table */
    uint32_t data_root;

    if (w->n_leaves == 0) {
        /* Empty table: root is an empty leaf page */
        init_leaf_page(w);
        flush_leaf_page(w, 0);
        data_root = w->next_page_no - 1;
    } else if (w->n_leaves == 1) {
        data_root = w->leaves[0].page_no;
    } else {
        /* Build interior page(s) pointing to leaf pages.
           For simplicity, build a single interior page (supports up to
           ~500 leaves with 4096-byte pages = millions of rows). */
        uint8_t ipage[WRITER_PAGE_SIZE];
        memset(ipage, 0, WRITER_PAGE_SIZE);

        data_root = w->next_page_no;

        /* Interior page header: 12 bytes */
        ipage[0] = 0x05; /* table B-tree interior */
        /* right_child = last leaf page */
        write_be32(ipage + 8, w->leaves[w->n_leaves - 1].page_no);

        int n_interior_cells = w->n_leaves - 1;
        write_be16(ipage + 3, (uint16_t)n_interior_cells);

        int ptr_off = 12; /* cell pointers start after 12-byte header */
        int content_end = WRITER_PAGE_SIZE;

        for (int i = 0; i < n_interior_cells; i++) {
            /* Interior cell: 4-byte child page + rowid varint */
            uint8_t cell[13];
            int clen = 0;
            write_be32(cell, w->leaves[i].page_no);
            clen = 4;
            clen += write_varint(cell + 4, w->leaves[i].max_rowid);

            content_end -= clen;
            memcpy(ipage + content_end, cell, (size_t)clen);
            write_be16(ipage + ptr_off, (uint16_t)content_end);
            ptr_off += 2;
        }

        write_be16(ipage + 5, (uint16_t)content_end); /* content area start */

        int64_t offset = (int64_t)(data_root - 1) * WRITER_PAGE_SIZE;
        fseek(w->fp, (long)offset, SEEK_SET);
        fwrite(ipage, 1, WRITER_PAGE_SIZE, w->fp);
        w->next_page_no++;
    }

    /* Build page 1: database header + sqlite_master leaf page */
    uint8_t page1[WRITER_PAGE_SIZE];
    memset(page1, 0, WRITER_PAGE_SIZE);

    /* Database header (first 100 bytes) */
    memcpy(page1, "SQLite format 3\000", 16);
    write_be16(page1 + 16, WRITER_PAGE_SIZE); /* page size */
    page1[18] = 1;  /* file format write version */
    page1[19] = 1;  /* file format read version */
    page1[20] = 0;  /* reserved space */
    page1[21] = 64; /* max embedded payload fraction */
    page1[22] = 32; /* min embedded payload fraction */
    page1[23] = 32; /* leaf payload fraction */
    write_be32(page1 + 24, 1); /* file change counter */
    write_be32(page1 + 28, w->next_page_no - 1); /* database size in pages */
    write_be32(page1 + 44, 4); /* schema format number */
    write_be32(page1 + 56, 1); /* text encoding: UTF-8 */
    write_be32(page1 + 96, 3047000); /* SQLite version number (3.47.0) */

    /* Build CREATE TABLE SQL string */
    char create_sql[4096];
    int pos = snprintf(create_sql, sizeof(create_sql),
                       "CREATE TABLE \"%s\" (", w->table_name);
    for (int c = 0; c < w->n_cols && pos < 4000; c++) {
        if (c > 0) pos += snprintf(create_sql + pos,
                                    sizeof(create_sql) - (size_t)pos, ", ");
        pos += snprintf(create_sql + pos, sizeof(create_sql) - (size_t)pos,
                        "\"%s\" %s", w->col_names[c], w->col_types[c]);
    }
    snprintf(create_sql + pos, sizeof(create_sql) - (size_t)pos, ")");

    /* Build sqlite_master record:
       (type="table", name=table_name, tbl_name=table_name,
        rootpage=data_root, sql=create_sql) */
    GrowBuf schema_rec;
    gbuf_init_w(&schema_rec, 1024);

    /* Compute serial types */
    int type_len = 5; /* "table" */
    int name_len = (int)strlen(w->table_name);
    int sql_len = (int)strlen(create_sql);

    int st_type = 13 + 2 * type_len;
    int st_name = 13 + 2 * name_len;
    int st_tblname = 13 + 2 * name_len;
    int st_rootpage;
    if (data_root == 0) st_rootpage = 8;
    else if (data_root == 1) st_rootpage = 9;
    else if (data_root <= 127) st_rootpage = 1;
    else if (data_root <= 32767) st_rootpage = 2;
    else st_rootpage = 4;
    int st_sql = 13 + 2 * sql_len;

    /* Header */
    uint8_t hdr_buf[64];
    int hdr_len = 0;
    hdr_len++; /* reserve 1 byte for header size (usually fits in 1) */
    hdr_len += write_varint(hdr_buf + hdr_len, st_type);
    hdr_len += write_varint(hdr_buf + hdr_len, st_name);
    hdr_len += write_varint(hdr_buf + hdr_len, st_tblname);
    hdr_len += write_varint(hdr_buf + hdr_len, st_rootpage);
    hdr_len += write_varint(hdr_buf + hdr_len, st_sql);
    hdr_buf[0] = (uint8_t)hdr_len; /* header size (including this byte) */

    gbuf_ensure(&schema_rec, hdr_len + type_len + name_len * 2 +
                8 + sql_len + 64);
    memcpy(schema_rec.data, hdr_buf, (size_t)hdr_len);
    schema_rec.len = hdr_len;

    /* Data: type */
    memcpy(schema_rec.data + schema_rec.len, "table", 5);
    schema_rec.len += 5;

    /* Data: name */
    memcpy(schema_rec.data + schema_rec.len, w->table_name, (size_t)name_len);
    schema_rec.len += name_len;

    /* Data: tbl_name */
    memcpy(schema_rec.data + schema_rec.len, w->table_name, (size_t)name_len);
    schema_rec.len += name_len;

    /* Data: rootpage */
    if (st_rootpage == 8 || st_rootpage == 9) {
        /* zero-length */
    } else {
        int n = (int)serial_type_len(st_rootpage);
        int64_t val = (int64_t)data_root;
        for (int i = n - 1; i >= 0; i--) {
            schema_rec.data[schema_rec.len + i] = (uint8_t)(val & 0xFF);
            val >>= 8;
        }
        schema_rec.len += n;
    }

    /* Data: sql */
    memcpy(schema_rec.data + schema_rec.len, create_sql, (size_t)sql_len);
    schema_rec.len += sql_len;

    /* Build cell for sqlite_master: payload_size + rowid(=1) + record */
    uint8_t cell_hdr[18];
    int cell_hdr_len = 0;
    cell_hdr_len += write_varint(cell_hdr, schema_rec.len);
    cell_hdr_len += write_varint(cell_hdr + cell_hdr_len, 1); /* rowid */

    int cell_total = cell_hdr_len + (int)schema_rec.len;

    /* Place the cell on page 1 (B-tree header at offset 100) */
    int p1_content_end = WRITER_PAGE_SIZE - cell_total;
    memcpy(page1 + p1_content_end, cell_hdr, (size_t)cell_hdr_len);
    memcpy(page1 + p1_content_end + cell_hdr_len,
           schema_rec.data, (size_t)schema_rec.len);

    /* Page 1 B-tree header at offset 100 */
    page1[100] = 0x0D; /* leaf table B-tree */
    write_be16(page1 + 101, 0); /* first freeblock */
    write_be16(page1 + 103, 1); /* 1 cell */
    write_be16(page1 + 105, (uint16_t)p1_content_end);
    page1[107] = 0; /* fragmented bytes */
    /* Cell pointer at offset 108 */
    write_be16(page1 + 108, (uint16_t)p1_content_end);

    /* Write page 1 */
    fseek(w->fp, 0, SEEK_SET);
    fwrite(page1, 1, WRITER_PAGE_SIZE, w->fp);

    free(schema_rec.data);

    /* Clean up */
    fclose(w->fp);
    free(w->table_name);
    for (int i = 0; i < w->n_cols; i++) {
        free(w->col_names[i]);
        free(w->col_types[i]);
    }
    free(w->leaves);
    free(w);
}
