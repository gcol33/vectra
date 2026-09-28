source("/home/claude/paper/scripts/parse_runs.R")
x <- parse_runs("/home/claude/bench/results/runs.jsonl")
write.csv(x, "/home/claude/paper/data/bench_runs.csv", row.names = FALSE)
cat(nrow(x), "runs\n")
