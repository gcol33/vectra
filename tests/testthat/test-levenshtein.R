# --- levenshtein distance ---

test_that("levenshtein exact match is 0", {
  df <- data.frame(name = c("rubra", "alba", "rubra"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(d = levenshtein(name, "rubra")) |> collect()
  expect_equal(result$d, c(0, 3, 0))
})

test_that("levenshtein handles NA", {
  df <- data.frame(name = c("rubra", NA, "alba"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(d = levenshtein(name, "rubra")) |> collect()
  expect_equal(result$d, c(0, NA, 3))
})

test_that("levenshtein known distances", {
  df <- data.frame(name = c("kitten", "saturday", ""))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(d = levenshtein(name, "sitting")) |> collect()
  expect_equal(result$d, c(3, 6, 7))
})

test_that("levenshtein column vs column", {
  df <- data.frame(a = c("rubra", "alba"), b = c("rubrum", "alba"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(d = levenshtein(a, b)) |> collect()
  expect_equal(result$d, c(2, 0))
})

test_that("levenshtein with max_dist early termination", {
  df <- data.frame(name = c("rubra", "pratensis", "rubrum"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    mutate(d = levenshtein(name, "rubra", max_dist = 2)) |> collect()
  expect_equal(result$d[1], 0)  # exact match
  expect_equal(result$d[2], 3)  # capped at max_dist + 1
  expect_equal(result$d[3], 2)  # within bound
})

test_that("levenshtein works in filter", {
  df <- data.frame(name = c("rubra", "pratensis", "rubrum", "alba"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(levenshtein(name, "rubra") <= 2) |> collect()
  expect_equal(nrow(result), 2)
  expect_true(all(result$name %in% c("rubra", "rubrum")))
})

# --- levenshtein_norm ---

test_that("levenshtein_norm in 0-1 range", {
  df <- data.frame(name = c("rubra", "rubrum", "pratensis"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    mutate(d = levenshtein_norm(name, "rubra")) |> collect()
  expect_true(all(result$d >= 0 & result$d <= 1))
  expect_equal(result$d[1], 0)  # exact match
})

test_that("levenshtein_norm both empty is 0", {
  df <- data.frame(name = c("", "abc"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    mutate(d = levenshtein_norm(name, "")) |> collect()
  expect_equal(result$d[1], 0)   # both empty -> 0
  expect_equal(result$d[2], 1)   # "abc" vs "" -> 3/3 = 1.0
})

test_that("levenshtein_norm handles NA", {
  df <- data.frame(name = c("rubra", NA))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    mutate(d = levenshtein_norm(name, "rubra")) |> collect()
  expect_equal(result$d[1], 0)
  expect_true(is.na(result$d[2]))
})

# --- Damerau-Levenshtein distance ---

test_that("dl_dist counts transpositions as 1", {
  # "ab" -> "ba" is 1 transposition (DL) vs 2 ops (plain Levenshtein)
  df <- data.frame(name = c("ab", "abc", "pratesnsis"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  res_dl <- tbl(f) |> mutate(d = dl_dist(name, "ba")) |> collect()
  res_lv <- tbl(f) |> mutate(d = levenshtein(name, "ba")) |> collect()
  expect_equal(res_dl$d[1], 1)  # transposition: cost 1
  expect_equal(res_lv$d[1], 2)  # no transposition op: cost 2
})

test_that("dl_dist exact match is 0", {
  df <- data.frame(name = c("rubra", "alba"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(d = dl_dist(name, "rubra")) |> collect()
  expect_equal(result$d[1], 0)
})

test_that("dl_dist handles NA", {
  df <- data.frame(name = c("rubra", NA))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(d = dl_dist(name, "rubra")) |> collect()
  expect_equal(result$d[1], 0)
  expect_true(is.na(result$d[2]))
})

test_that("dl_dist column vs column", {
  df <- data.frame(a = c("abc", "alba"), b = c("bac", "alba"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(d = dl_dist(a, b)) |> collect()
  expect_equal(result$d, c(1, 0))  # "abc"->"bac" = 1 transposition
})

test_that("dl_dist with max_dist early termination", {
  df <- data.frame(name = c("rubra", "pratensis", "rubrum"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    mutate(d = dl_dist(name, "rubra", max_dist = 2)) |> collect()
  expect_equal(result$d[1], 0)
  expect_equal(result$d[2], 3)  # capped
})

test_that("dl_dist_norm in 0-1 range", {
  df <- data.frame(name = c("rubra", "rubrum", "ba"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    mutate(d = dl_dist_norm(name, "rubra")) |> collect()
  expect_true(all(result$d >= 0 & result$d <= 1))
  expect_equal(result$d[1], 0)
})

test_that("dl_dist works in filter", {
  df <- data.frame(name = c("rubra", "rубра", "pratensis", "rubmra"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> filter(dl_dist(name, "rubra") <= 1) |> collect()
  expect_true("rubra" %in% result$name)
})

# --- Jaro-Winkler similarity ---

test_that("jaro_winkler identical strings are 1.0", {
  df <- data.frame(name = c("rubra", "pratensis"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    mutate(jw = jaro_winkler(name, "rubra")) |> collect()
  expect_equal(result$jw[1], 1.0)
  expect_true(result$jw[2] < 1.0)
})

test_that("jaro_winkler in 0-1 range", {
  df <- data.frame(name = c("rubra", "rubrum", "alba", "pratensis", ""))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    mutate(jw = jaro_winkler(name, "rubra")) |> collect()
  expect_true(all(result$jw >= 0 & result$jw <= 1))
})

test_that("jaro_winkler prefix bonus rewards shared prefix", {
  # "rubrum" shares 4-char prefix "rubr" with "rubra" -> high JW
  # "xubra" shares 0-char prefix -> lower JW even though edit distance is 1
  df <- data.frame(name = c("rubrum", "xubra"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    mutate(jw = jaro_winkler(name, "rubra")) |> collect()
  expect_true(result$jw[1] > result$jw[2])
})

test_that("jaro_winkler handles NA", {
  df <- data.frame(name = c("rubra", NA))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    mutate(jw = jaro_winkler(name, "rubra")) |> collect()
  expect_equal(result$jw[1], 1.0)
  expect_true(is.na(result$jw[2]))
})

test_that("jaro_winkler both empty is 1.0", {
  df <- data.frame(name = c("", "abc"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    mutate(jw = jaro_winkler(name, "")) |> collect()
  expect_equal(result$jw[1], 1.0)   # both empty -> identical
  expect_equal(result$jw[2], 0.0)   # "abc" vs "" -> no matches
})

test_that("jaro_winkler column vs column", {
  df <- data.frame(a = c("rubra", "alba"), b = c("rubra", "pratensis"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |> mutate(jw = jaro_winkler(a, b)) |> collect()
  expect_equal(result$jw[1], 1.0)
  expect_true(result$jw[2] < 1.0)
})

test_that("jaro_winkler works in filter", {
  df <- data.frame(name = c("rubra", "rubrum", "pratensis", "alba"))
  f <- tempfile(fileext = ".vtr")
  on.exit(unlink(f))
  write_vtr(df, f)
  result <- tbl(f) |>
    filter(jaro_winkler(name, "rubra") >= 0.85) |> collect()
  expect_true("rubra" %in% result$name)
  expect_true("rubrum" %in% result$name)
})
