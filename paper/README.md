# vectra — R Journal manuscript (first draft)

This folder holds the source of the R Journal article
"vectra: Larger-than-Memory Tabular and Spatial Analysis in R" and
everything needed to reproduce its numbers.

```
paper/
├── vectra.Rmd              main article (rjtools::rjournal_article)
├── vectra.bib              references
├── sections/               child documents: evaluation, case study, discussion
├── scripts/
│   ├── parse_runs.R        harness output (runs.jsonl) -> one row per run
│   ├── export_runs.R       writes data/bench_runs.csv
│   ├── results.R           loads data/, defines the helpers used inline in the text
│   ├── figures.R           benchmark figures -> figures/
│   ├── case_facts.R        case-study numbers -> data/case_*.{csv,rds}
│   └── fig_case.R          case-study figure
├── data/                   measured results used by the text (CSV/RDS)
├── figures/                generated figures (PDF for print, PNG for HTML)
├── bench/                  benchmark harness
└── casestudy/              GBIF case-study scripts, one per stage
```

## Rendering

```r
# from this folder
rmarkdown::render("vectra.Rmd")      # PDF and HTML via rjtools
```

The text reads its numbers from `data/`, so rendering does not rerun any
benchmark. `scripts/figures.R` and `scripts/fig_case.R` regenerate the figures
from the same files.

## Rerunning the benchmarks

The harness needs Linux (WSL2 works): it drops the page cache before every run
(`/proc/sys/vm/drop_caches`, needs root), measures peak memory with GNU
`time -f %M`, and reads `/proc/self/io`.

```sh
cd bench
export BENCH_DIR=$PWD            # data/, results/, work/ are created here
Rscript gen.R 10                 # 10 x 1e7-row chunks: .vtr, Parquet, fst (~15 GB)
Rscript gen_dim.R                # build-side tables for the join workload
Rscript make_abl.R               # single 5e7-row store (+ indexed copy) for the ablation
./grid.sh                        # full grid; appends to results/runs.jsonl
Rscript ../scripts/export_runs.R # -> ../data/bench_runs.csv
```

All tools are limited to two threads (`OMP_NUM_THREADS`, `arrow::set_cpu_count()`,
`SET threads`, `setDTthreads()`); edit `threads` in `bench_one.R` to change that.
The spatial workload needs `eu_bbox.rds` (Natural Earth 1:10m countries clipped
to 10°W–30°E, 36°N–70°N), built by `casestudy/prep_geo.R`.

## Rerunning the case study

`casestudy/00_download.R` selects European Tracheophyta records from the GBIF
snapshot of 1 September 2026 (doi:10.15468/dl.e9fsnq) on the GBIF AWS open-data
bucket with DuckDB's httpfs extension. For a citable, versioned input, replace
this step with a GBIF download (e.g. `rgbif::occ_download(format =
"SIMPLE_PARQUET")`) and cite its DOI. Then run the stages in order, each in a
fresh process:

```sh
cd casestudy
for s in 01_ingest 02_clean 03_cells 04_occupancy 05_models; do ./run_stage.sh $s.R; done
Rscript ../scripts/case_facts.R
```
