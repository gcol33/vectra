## Submission

This is a hotfix for vectra 0.14.0, accepted on 2026-10-26. 0.14.0 was
submitted under time pressure, so that taxify 0.7.0 (its only reverse
dependency) can follow before taxify's archival date of 2026-11-02, and I
missed that our sanitizer CI had flagged a bug in it. This release fixes that
bug and changes nothing else.

On a deliberately corrupted file (a test in test-append-size.R), the new
decoder read the record header's dimensions before validating the header, so
a garbage rank byte indexed past a 3-element array and the element count
overflowed (UBSAN: "index 3 out of bounds", "signed integer overflow" in
src/vtr_codec_tdc.c). The header is now validated first. Valid files were
never affected. The CRAN gcc-UBSAN and clang-UBSAN checks would report it, so
I am fixing it before they do. I am aware of the request to reduce the update
frequency.

## Test environments

* Local: Windows 11, R 4.6.0, full test suite
* GitHub Actions: Linux ASAN + UBSAN and clang UBSAN (incl. function), clean
* win-builder: R-devel, R-release

## R CMD check results

* win-builder R-devel: <pending>
* win-builder R-release: <pending>

## Reverse dependencies

taxify imports vectra; the fix touches only the reading of corrupted records.
