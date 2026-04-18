## Resubmission

This is a resubmission of vectra after the 0.3.2 submission was rejected
with three reviewer-flagged items. All three have been addressed:

* Function names in the DESCRIPTION are written with `()` — `filter()`,
  `select()`, `mutate()`, `group_by()`, `summarise()`, `n()`, `sum()`,
  `mean()`, `min()`, `max()`, `sd()`, `first()`, `last()`.
* vectra does not implement a published method, so no `<doi:...>` /
  `<ISBN:...>` reference has been added.
* No example uses `\dontrun{}`; long-running examples use `\donttest{}`.

This release (0.5.0) also rewrites the compression backend on top of
`tdc`, a standalone C11 compression library vendored into `src/tdc/`.
The `.vtr` on-disk format has changed as part of that rewire —
intentional, since vectra has not yet been accepted on CRAN.

## Test environments

* local Windows 11, R 4.5.2 (GCC 14.3.0 via Rtools)

## R CMD check results

0 errors, 0 warnings, 1 note.

The sole note is "New submission"; the package is not yet on CRAN.
