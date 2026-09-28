# Single 5e7-row store for the optimiser ablation, plus a copy with a .vtri index on k7.
suppressMessages(library(vectra))
dir.create("data/abl", FALSE)
f <- sprintf("data/vtr/chunk%02d.vtr", 1:5)
do.call(bind_rows, lapply(f, tbl)) |> write_vtr("data/abl/f5e7.vtr")
file.copy("data/abl/f5e7.vtr", "data/abl/f5e7_idx.vtr", overwrite = TRUE)
create_index("data/abl/f5e7_idx.vtr", "k7")
