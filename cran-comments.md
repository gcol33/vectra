## Resubmission (hotfix)

This upload replaces the vectra 0.14.0 submitted on 2026-10-25, which is
pending manual inspection. It fixes one bug found by our sanitizer CI after
that upload: on a deliberately corrupted file (a test in
test-append-size.R), the new decoder read the record header's dimensions
before validating it, so a garbage rank byte indexed past a 3-element array
and the element count overflowed (UBSAN: "index 3 out of bounds", "signed
integer overflow" in src/vtr_codec_tdc.c). The header is now validated
first. Valid files were never affected; nothing else changed. I kept the
version at 0.14.0 because the previous upload was not accepted.

## Submission

This is a feature update, 0.13.0 having been published on 2026-09-30. It is
submitted now because taxify, vectra's only reverse dependency, needs it to
answer a CRAN request: on 2026-10-25 Prof. Ripley reported that taxify had
left 4.4 GB of backbone data in `~/.local/share/R/taxify` on his check server
and scheduled taxify for archival on 2026-11-02. Part of taxify's fix is to
make the data it downloads much smaller, and that depends on this release.

The main change is the on-disk format of `.vtr` files. String columns were
stored with their dictionary uncompressed; in the new format the dictionary is
entropy-coded like the rest of the column. On the World Flora Online backbone
that taxify downloads (1.66 million rows), the file goes from 739 MiB to
118 MiB and reads back in 9 s instead of 19 s.

Files of the previous format are still read, and can still be appended to.
Files written by this version cannot be read by earlier versions; NEWS.md
says so. For taxify this is handled on the data side: the stores published
for taxify < 0.7.0 stay in the previous format under their existing names, and
taxify 0.7.0 (to be submitted after this release) reads the new ones.

NEWS.md lists every change.

## Test environments

* Local: Windows 11, R 4.6.0, `R CMD check --as-cran` on the built tarball
* win-builder: R-devel, R-release

## R CMD check results

* Local (--as-cran): 0 errors | 0 warnings | 1 note
* win-builder R-devel (2026-10-24 r90762): Status 1 NOTE, check time 575 s
* win-builder R-release (4.6.1): Status 1 NOTE, check time 530 s

The note is from the incoming feasibility check, counting the updates in the
past six months. This release is submitted because of the CRAN request above.

## Reverse dependencies

taxify imports vectra. `R CMD check --as-cran` of taxify 0.6.0 (the CRAN
version) against this release passes, with its test suite in full; the one
flag is the incoming check's "Insufficient package version", from checking the
version already on CRAN. The check left the user data directory empty.
