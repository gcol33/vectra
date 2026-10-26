## Submission

This release fixes the problems in the 'Additional issues' (clang-ASAN,
gcc-ASAN, gcc-UBSAN, M1-SAN) of vectra 0.14.0, which CRAN asked to correct
before 2026-11-16. It is a hotfix: 0.14.0 was submitted under time pressure,
so that taxify 0.7.0 (its only reverse dependency) can follow before taxify's
archival date of 2026-11-02, and I missed that our sanitizer CI had flagged
the bug. This release fixes that bug and changes nothing else.

All reports are one bug, at src/vtr_codec_tdc.c:618. On a deliberately
corrupted file (a test in test-append-size.R), the decoder read the record
header's dimensions before validating the header, so a garbage rank byte
indexed past the 3-element dimension array (stack-buffer-overflow, "index 3
out of bounds") and the element count overflowed ("signed integer overflow").
The header is now validated first, in all three decoders that read it. Valid
files were never affected. I am aware of the request to reduce the update
frequency.

## Test environments

* Local: Windows 11, R 4.6.0, full test suite
* GitHub Actions: Linux gcc ASAN + UBSAN and clang UBSAN (incl. function),
  clean
* win-builder: R-release
* Local: Windows 11, R-devel (2026-09-29 r90598 ucrt), R CMD check --as-cran.
  The win-builder R-devel queue did not move on 2026-10-26 from 17:38 to
  past 21:30 (server time), for every package in it, so R-devel was checked
  locally.

## R CMD check results

* win-builder R-release (4.6.1): 0 errors | 0 warnings | 1 note
* Local R-devel (--as-cran): Status OK

The note is "Days since last update: 0" / "Number of updates in past 6
months: 13", explained above.

## Reverse dependencies

taxify imports vectra; the fix touches only the reading of corrupted records.
