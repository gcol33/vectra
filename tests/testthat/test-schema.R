# Helper to create a star-schema test setup
make_star_schema <- function() {
  f_obs <- tempfile(fileext = ".vtr")
  f_sp  <- tempfile(fileext = ".vtr")
  f_ct  <- tempfile(fileext = ".vtr")

  write_vtr(data.frame(
    sp_id   = c(1, 2, 3, 4),
    ct_code = c("AT", "DE", "FR", "XX"),
    value   = c(10, 20, 30, 40)
  ), f_obs)

  write_vtr(data.frame(
    sp_id = c(1, 2, 3),
    name  = c("Oak", "Beech", "Pine")
  ), f_sp)

  write_vtr(data.frame(
    ct_code = c("AT", "DE", "FR"),
    gdp     = c(400, 3800, 2700)
  ), f_ct)

  list(f_obs = f_obs, f_sp = f_sp, f_ct = f_ct)
}

# --- link() ---

test_that("link() validates inputs", {
  f <- tempfile(fileext = ".vtr")
  write_vtr(data.frame(x = 1), f)
  on.exit(unlink(f))

  expect_error(link(123, tbl(f)), "key must be")
  expect_error(link(character(0), tbl(f)), "key must be")
  expect_error(link("x", data.frame()), "must be a vectra_node")
})

test_that("link() creates a vectra_link object", {
  f <- tempfile(fileext = ".vtr")
  write_vtr(data.frame(x = 1), f)
  on.exit(unlink(f))

  lnk <- link("x", tbl(f))
  expect_s3_class(lnk, "vectra_link")
  expect_equal(lnk$key, "x")
})

# --- vtr_schema() ---

test_that("vtr_schema() validates inputs", {
  f <- tempfile(fileext = ".vtr")
  write_vtr(data.frame(x = 1), f)
  on.exit(unlink(f))

  expect_error(vtr_schema(fact = data.frame()), "must be a vectra_node")
  expect_error(vtr_schema(fact = tbl(f)), "at least one dimension")
  expect_error(vtr_schema(fact = tbl(f), link("x", tbl(f))),
               "must be named")
  expect_error(vtr_schema(fact = tbl(f), dim1 = "not_a_link"),
               "must be a vectra_link")
})

test_that("vtr_schema() creates a vectra_schema object", {
  ft <- make_star_schema()
  on.exit(unlink(c(ft$f_obs, ft$f_sp, ft$f_ct)))

  s <- vtr_schema(
    fact    = tbl(ft$f_obs),
    species = link("sp_id", tbl(ft$f_sp)),
    country = link("ct_code", tbl(ft$f_ct))
  )
  expect_s3_class(s, "vectra_schema")
  expect_equal(names(s$dims), c("species", "country"))
})

test_that("print.vectra_schema produces output", {
  ft <- make_star_schema()
  on.exit(unlink(c(ft$f_obs, ft$f_sp, ft$f_ct)))

  s <- vtr_schema(
    fact    = tbl(ft$f_obs),
    species = link("sp_id", tbl(ft$f_sp))
  )
  out <- capture.output(print(s))
  expect_true(any(grepl("vectra schema", out)))
  expect_true(any(grepl("species", out)))
})

# --- lookup() ---

test_that("lookup() resolves fact columns by bare name", {
  ft <- make_star_schema()
  on.exit(unlink(c(ft$f_obs, ft$f_sp, ft$f_ct)))

  s <- vtr_schema(
    fact    = tbl(ft$f_obs),
    species = link("sp_id", tbl(ft$f_sp))
  )
  result <- lookup(s, value, .report = FALSE) |> collect()
  expect_equal(result$value, c(10, 20, 30, 40))
})

test_that("lookup() resolves dimension columns via dim$col syntax", {
  ft <- make_star_schema()
  on.exit(unlink(c(ft$f_obs, ft$f_sp, ft$f_ct)))

  s <- vtr_schema(
    fact    = tbl(ft$f_obs),
    species = link("sp_id", tbl(ft$f_sp)),
    country = link("ct_code", tbl(ft$f_ct))
  )
  result <- lookup(s, value, species$name, country$gdp, .report = FALSE) |>
    collect()
  expect_equal(ncol(result), 3)
  expect_equal(names(result), c("value", "name", "gdp"))
  # Row 4 has sp_id=4 which doesn't match species (left join -> NA)
  expect_equal(result$name[1:3], c("Oak", "Beech", "Pine"))
  expect_true(is.na(result$name[4]))
  # Row 4 has ct_code="XX" which doesn't match country
  expect_equal(result$gdp[1:3], c(400, 3800, 2700))
  expect_true(is.na(result$gdp[4]))
})

test_that("lookup() with inner join drops unmatched rows", {
  ft <- make_star_schema()
  on.exit(unlink(c(ft$f_obs, ft$f_sp, ft$f_ct)))

  s <- vtr_schema(
    fact    = tbl(ft$f_obs),
    species = link("sp_id", tbl(ft$f_sp))
  )
  result <- lookup(s, value, species$name, .join = "inner",
                   .report = FALSE) |> collect()
  expect_equal(nrow(result), 3)
  expect_false(any(is.na(result$name)))
})

test_that("lookup() only joins needed dimensions", {
  ft <- make_star_schema()
  on.exit(unlink(c(ft$f_obs, ft$f_sp, ft$f_ct)))

  s <- vtr_schema(
    fact    = tbl(ft$f_obs),
    species = link("sp_id", tbl(ft$f_sp)),
    country = link("ct_code", tbl(ft$f_ct))
  )
  # Only request species columns - country should not be joined
  result <- lookup(s, value, species$name, .report = FALSE) |> collect()
  expect_equal(names(result), c("value", "name"))
})

test_that("lookup() reports unmatched keys", {
  ft <- make_star_schema()
  on.exit(unlink(c(ft$f_obs, ft$f_sp, ft$f_ct)))

  s <- vtr_schema(
    fact    = tbl(ft$f_obs),
    species = link("sp_id", tbl(ft$f_sp)),
    country = link("ct_code", tbl(ft$f_ct))
  )
  msgs <- capture.output(
    result <- lookup(s, value, species$name, country$gdp, .report = TRUE) |>
      collect(),
    type = "message"
  )
  # species: sp_id=4 is unmatched
  expect_true(any(grepl("species.*1/4.*unmatched", msgs)))
  # country: ct_code="XX" is unmatched
  expect_true(any(grepl("country.*1/4.*unmatched", msgs)))
})

test_that("lookup() reports all matched when no mismatches", {
  f_obs <- tempfile(fileext = ".vtr")
  f_sp  <- tempfile(fileext = ".vtr")
  write_vtr(data.frame(sp_id = c(1, 2), value = c(10, 20)), f_obs)
  write_vtr(data.frame(sp_id = c(1, 2), name = c("A", "B")), f_sp)
  on.exit(unlink(c(f_obs, f_sp)))

  s <- vtr_schema(
    fact    = tbl(f_obs),
    species = link("sp_id", tbl(f_sp))
  )
  msgs <- capture.output(
    result <- lookup(s, value, species$name) |> collect(),
    type = "message"
  )
  expect_true(any(grepl("all 2 keys matched", msgs)))
})

test_that("lookup() errors on unknown dimension", {
  ft <- make_star_schema()
  on.exit(unlink(c(ft$f_obs, ft$f_sp, ft$f_ct)))

  s <- vtr_schema(
    fact    = tbl(ft$f_obs),
    species = link("sp_id", tbl(ft$f_sp))
  )
  expect_error(lookup(s, bogus$col, .report = FALSE), "dimension 'bogus'")
})

test_that("lookup() errors with no column references", {
  ft <- make_star_schema()
  on.exit(unlink(c(ft$f_obs, ft$f_sp, ft$f_ct)))

  s <- vtr_schema(
    fact    = tbl(ft$f_obs),
    species = link("sp_id", tbl(ft$f_sp))
  )
  expect_error(lookup(s, .report = FALSE), "at least one column")
})

test_that("lookup() works with named keys (different column names)", {
  f_obs <- tempfile(fileext = ".vtr")
  f_sp  <- tempfile(fileext = ".vtr")
  write_vtr(data.frame(species_id = c(1, 2), value = c(10, 20)), f_obs)
  write_vtr(data.frame(sp_id = c(1, 2), name = c("A", "B")), f_sp)
  on.exit(unlink(c(f_obs, f_sp)))

  s <- vtr_schema(
    fact    = tbl(f_obs),
    species = link(c("species_id" = "sp_id"), tbl(f_sp))
  )
  result <- lookup(s, value, species$name, .report = FALSE) |> collect()
  expect_equal(result$name, c("A", "B"))
})

test_that("schema can be reused for multiple lookups", {
  ft <- make_star_schema()
  on.exit(unlink(c(ft$f_obs, ft$f_sp, ft$f_ct)))

  s <- vtr_schema(
    fact    = tbl(ft$f_obs),
    species = link("sp_id", tbl(ft$f_sp)),
    country = link("ct_code", tbl(ft$f_ct))
  )

  r1 <- lookup(s, value, species$name, .report = FALSE) |> collect()
  r2 <- lookup(s, value, country$gdp, .report = FALSE) |> collect()
  r3 <- lookup(s, species$name, country$gdp, .report = FALSE) |> collect()

  expect_equal(nrow(r1), 4)
  expect_equal(nrow(r2), 4)
  expect_equal(nrow(r3), 4)
  expect_equal(names(r1), c("value", "name"))
  expect_equal(names(r2), c("value", "gdp"))
  expect_equal(names(r3), c("name", "gdp"))
})
