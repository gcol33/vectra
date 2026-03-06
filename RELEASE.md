# Release Checklist

## Pre-release validation

- [ ] `devtools::test()` — all tests pass (465+)
- [ ] `devtools::check(args = "--no-manual")` — 0 errors, 0 warnings, 0 notes
- [ ] CI fully green (R-CMD-check across Linux/Windows/macOS)
- [ ] ASAN/UBSAN job green
- [ ] Benchmark smoke job green
- [ ] Full benchmark run completed and baseline saved

## Documentation

- [ ] README reviewed top to bottom — no stale claims
- [ ] `vignette("engine")` renders cleanly
- [ ] All exported functions have `@examples`
- [ ] pkgdown builds without errors
- [ ] pkgdown site reviewed as a first-time reader
- [ ] NEWS.md updated with all changes since last release
- [ ] Version in DESCRIPTION matches NEWS.md header

## Code quality

- [ ] No accidental debug-only behavior in release build
- [ ] Error messages are clear and include expected vs actual values
- [ ] No TODO/FIXME left in shipped code paths
- [ ] No false claims in docs (verify every feature listed is implemented)

## Package metadata

- [ ] DESCRIPTION: version, title, description accurate
- [ ] DESCRIPTION: URLs and BugReports correct
- [ ] LICENSE and LICENSE.md present and consistent
- [ ] NAMESPACE: all exports intentional, no missing imports

## Release

- [ ] Tag: `git tag v0.2.1-rc1`
- [ ] Push tag: `git push origin v0.2.1-rc1`
- [ ] GitHub release created with release notes from NEWS.md
- [ ] pkgdown site deployed

## Post-release

- [ ] Install from GitHub on a clean R library: `pak::pak("gcol33/vectra")`
- [ ] Run quick smoke test on the installed package
- [ ] Share with 2-3 technically strong users for feedback
