# vectra Extension: String Distance Functions (IMPLEMENTED)

## Motivation

taxify (offline multi-backend taxonomic name matching) will use vectra
as its query engine to stream large Darwin Core backbone files (WFO
~120MB, COL larger). The matching pipeline is:

1.  Load backbone via
    [`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md)
    / [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md)
2.  Clean + normalize names (existing string ops: `tolower`, `trimws`,
    `gsub`)
3.  Exact match via hash join on canonical name (existing `inner_join`)
4.  **For unmatched names: fuzzy match against genus-filtered
    candidates** — this needs string distance

Step 4 currently requires
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) to
R then [`adist()`](https://rdrr.io/r/utils/adist.html) in base R. Adding
Levenshtein distance as a C-level expression keeps the entire pipeline
in the engine, avoids materializing intermediate results, and lets us
exploit selection vectors for zero-copy candidate filtering.

## New Expression Nodes

### 1. `EXPR_LEVENSHTEIN` — Levenshtein edit distance

**R surface:**

``` r

# In mutate/filter context
backbone |>
  filter(genus == "Festuca") |>
  mutate(dist = levenshtein(epithet, "rubra")) |>
  filter(dist <= 2)
```

**Signature:** `levenshtein(x, pattern)` → `int64` - `x`: column
reference (VEC_STRING) - `pattern`: string literal or column reference -
Returns: edit distance as int64 (0 = exact match) - NA propagation: if
either input is NA, result is NA

**C implementation:**

Add to `VecExprKind` enum in `expr.h`:

``` c
EXPR_LEVENSHTEIN,  /* levenshtein(x, pattern) -> int64 */
```

Add to `VecExpr` struct — reuses existing `operand` (the column) +
`lit_str` (the pattern when literal). For column-vs-column comparison,
reuses `left`/`right`.

The algorithm is standard Wagner-Fischer with a single-row buffer
(O(min(m,n)) space):

``` c
static int64_t levenshtein_distance(const char *s, int64_t len_s,
                                     const char *t, int64_t len_t) {
    if (len_s == 0) return len_t;
    if (len_t == 0) return len_s;

    /* Use shorter string as column to minimize memory */
    if (len_s > len_t) {
        const char *tmp_s = s; s = t; t = tmp_s;
        int64_t tmp_l = len_s; len_s = len_t; len_t = tmp_l;
    }

    int64_t *prev = (int64_t *)malloc((len_s + 1) * sizeof(int64_t));
    for (int64_t i = 0; i <= len_s; i++) prev[i] = i;

    for (int64_t j = 1; j <= len_t; j++) {
        int64_t prev_diag = prev[0];
        prev[0] = j;
        for (int64_t i = 1; i <= len_s; i++) {
            int64_t cost = (s[i-1] == t[j-1]) ? 0 : 1;
            int64_t val = prev[i-1] + 1;              /* delete */
            if (prev[i] + 1 < val) val = prev[i] + 1; /* insert */
            int64_t diag = prev_diag + cost;            /* substitute */
            if (diag < val) val = diag;
            prev_diag = prev[i];
            prev[i] = val;
        }
    }
    int64_t result = prev[len_s];
    free(prev);
    return result;
}
```

### 2. `EXPR_LEVENSHTEIN_NORM` — Normalized Levenshtein (0.0–1.0)

**R surface:**

``` r

backbone |>
  mutate(dist = levenshtein_norm(epithet, "rubra")) |>
  filter(dist <= 0.1)
```

**Signature:** `levenshtein_norm(x, pattern)` → `double` - Returns:
`levenshtein(x, pattern) / max(nchar(x), nchar(pattern))` - Range: 0.0
(identical) to 1.0 (completely different) - Edge case: both empty
strings → 0.0

This matches WFO.match()’s `Fuzzy.dist` threshold convention.

### 3. `EXPR_SOUNDEX` — Soundex phonetic code (optional, lower priority)

**R surface:**

``` r

backbone |>
  mutate(code = soundex(epithet)) |>
  filter(code == soundex("pratensis"))
```

**Signature:** `soundex(x)` → `string` (always 4 characters: letter + 3
digits)

Lower priority — Levenshtein covers most fuzzy matching needs. Soundex
is useful for severely misspelled names but can wait for a later
release.

## R Layer Changes

### `R/expr.R` — Expression serializer

Add recognition for the new functions in the NSE walker:

``` r

# In the switch on function name:
"levenshtein" = {
    list(kind = "levenshtein", operand = serialize_expr(args[[1]]),
         pattern = serialize_expr(args[[2]]))
}
"levenshtein_norm" = {
    list(kind = "levenshtein_norm", operand = serialize_expr(args[[1]]),
         pattern = serialize_expr(args[[2]]))
}
```

### `R/verbs.R` — No changes needed

The functions work inside
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) and
[`filter()`](https://gillescolling.com/vectra/reference/filter.md) like
any other expression — no new verb required.

### `src/r_bridge.c` — Deserializer

Add cases in the expression deserializer to construct `EXPR_LEVENSHTEIN`
/ `EXPR_LEVENSHTEIN_NORM` nodes from the R list representation.

## Performance Considerations

### Early termination

For `filter(levenshtein(x, "rubra") <= k)`, we can add a `max_dist`
parameter to the C function that aborts computation once the running
minimum exceeds `k`. This turns O(mn) into O(km) for rejecting distant
strings early.

**R surface for bounded variant:**

``` r

# Explicit bound (avoids computing full distance for clearly-wrong candidates)
filter(levenshtein(epithet, "rubra", max_dist = 2) <= 2)
```

When `max_dist` is set, the function returns `max_dist + 1` for any
string whose true distance exceeds the bound, without computing the
exact value.

### Candidate pre-filtering

The taxify pipeline will filter by genus before fuzzy matching on
epithet, so the distance computation only runs on ~100–1000 candidates
per unmatched name, not the full backbone. This makes even the naive
O(mn) algorithm fast enough.

### Memory

The single-row Wagner-Fischer buffer uses O(min(m,n)) memory. For
typical epithets (5–20 chars), this is ~160 bytes. Negligible.

## Testing

``` r

test_that("levenshtein exact match is 0", {
  df <- data.frame(name = c("rubra", "alba", "rubra"))
  res <- df |> write_vtr(f <- tempfile()) ; tbl(f) |>
    mutate(d = levenshtein(name, "rubra")) |> collect()
  expect_equal(res$d, c(0, 4, 0))
})

test_that("levenshtein handles NA", {
  df <- data.frame(name = c("rubra", NA, "alba"))
  res <- df |> write_vtr(f <- tempfile()) ; tbl(f) |>
    mutate(d = levenshtein(name, "rubra")) |> collect()
  expect_equal(res$d, c(0, NA, 4))
})

test_that("levenshtein_norm in 0-1 range", {
  df <- data.frame(name = c("rubra", "rubrum", "pratensis"))
  res <- df |> write_vtr(f <- tempfile()) ; tbl(f) |>
    mutate(d = levenshtein_norm(name, "rubra")) |> collect()
  expect_true(all(res$d >= 0 & res$d <= 1))
  expect_equal(res$d[1], 0)  # exact match
})

test_that("levenshtein with max_dist early termination", {
  df <- data.frame(name = c("rubra", "pratensis", "rubrum"))
  res <- df |> write_vtr(f <- tempfile()) ; tbl(f) |>
    mutate(d = levenshtein(name, "rubra", max_dist = 2)) |> collect()
  expect_equal(res$d[1], 0)       # exact
  expect_equal(res$d[2], 3)       # max_dist + 1 (capped)
  expect_equal(res$d[3], 2)       # within bound, exact distance
})

test_that("levenshtein column vs column", {
  df <- data.frame(a = c("rubra", "alba"), b = c("rubrum", "alba"))
  res <- df |> write_vtr(f <- tempfile()) ; tbl(f) |>
    mutate(d = levenshtein(a, b)) |> collect()
  expect_equal(res$d, c(2, 0))
})
```

## Files to Modify

| File | Change |
|----|----|
| `src/expr.h` | Add `EXPR_LEVENSHTEIN`, `EXPR_LEVENSHTEIN_NORM` to enum; add `int64_t max_dist` field to `VecExpr` |
| `src/expr.c` | Implement `levenshtein_distance()`, evaluation cases for both new expr kinds |
| `src/r_bridge.c` | Deserialize `"levenshtein"` and `"levenshtein_norm"` from R list to C expr nodes |
| `R/expr.R` | Recognize `levenshtein()`, `levenshtein_norm()` in NSE walker |
| `tests/testthat/test-levenshtein.R` | New test file |
| `man/levenshtein.Rd` | Documentation (via roxygen in a new `R/levenshtein.R` with `@usage` examples) |
