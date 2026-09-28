#!/bin/bash
# Full benchmark grid; each configuration run 3 times in fresh processes,
# repetitions interleaved across tools so drift affects all tools alike.
cd /home/claude/bench
export TIMEOUT=1800
T5="vectra vectra1g arrow duckdb datatable"
for rep in 1 2 3; do
  for p in zm_sorted zm_random cols_1 cols_all idx_none idx_vtri mut_before mut_after; do ./run.sh vectra abl $p $rep; done
  for n in 1e7 2e7 5e7 1e8; do for t in $T5; do ./run.sh $t scan $n $rep; done; done
  for g in 1e3 1e5 1e6 1e7; do for t in $T5; do ./run.sh $t groups $g $rep; done; done
  for m in 1e3 1e5 1e6 1e7 5e7; do for t in $T5; do ./run.sh $t join $m $rep; done; done
  for s in 0.001 0.01 0.1 0.5; do for t in vectra arrow duckdb; do ./run.sh $t collect $s $rep; ./run.sh $t sink $s $rep; done; done
  for n in 1e6 1e7 3e7 5e7; do for t in vectra sf duckdb; do ./run.sh $t pip $n $rep; done; done
  for n in 1e7 5e7; do for w in model_offload model_rebuild model_glm; do ./run.sh vectra $w $n $rep; done; done
  for n in 1e7 5e7 1e8; do for t in $T5; do ./run.sh $t sort $n $rep; done; done
done
echo GRID_DONE
