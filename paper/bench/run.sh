#!/bin/bash
# run.sh <tool> <workload> <param> <rep> [extra env]
# Fresh process, cold page cache, GNU time for wall/peak RSS, du poller for temp disk.
tool=$1; wl=$2; param=$3; rep=$4
id="${tool}_${wl}_${param}_${rep}"
cfg="${tool}_${wl}_${param}"
if [ "$rep" -gt 1 ]; then
  if grep -qx "$cfg" /home/claude/bench/skip.txt 2>/dev/null || grep -q "\"id\":\"${cfg}_1\".*\"exit\":\"EXIT 137\"" /home/claude/bench/results/runs.jsonl; then
    echo "$id SKIPPED"; exit 0
  fi
  if [ "$(python3 /home/claude/bench/skipcheck.py $cfg $rep)" = "skip" ]; then echo "$id SKIPPED-LONG"; exit 0; fi
fi
work=/home/claude/bench/work/$id; rm -rf "$work"; mkdir -p "$work/tmp" "$work/out"
sync; echo 3 > /proc/sys/vm/drop_caches
cd /home/claude/bench
( TMPDIR=$work/tmp /usr/bin/time -f "TIME %e %M" -o $work/time.txt \
    timeout ${TIMEOUT:-3600} Rscript bench_one.R $tool $wl $param $work/out > $work/stdout.txt 2> $work/stderr.txt; echo "EXIT $?" > $work/exit.txt ) &
pid=$!
maxtmp=0
while kill -0 $pid 2>/dev/null; do
  s=$(du -sb $work/tmp 2>/dev/null | cut -f1); [ -n "$s" ] && [ "$s" -gt "$maxtmp" ] && maxtmp=$s
  sleep 0.5
done
wait $pid
res=$(grep '^RESULT' $work/stdout.txt | sed 's/^RESULT //')
t=$(grep '^TIME' $work/time.txt | tail -1)
ex=$(cat $work/exit.txt)
outsz=$(du -sb $work/out | cut -f1)
killed=$(grep -c -i "killed\|cannot allocate\|bad_alloc\|Out of Memory" $work/stderr.txt $work/time.txt 2>/dev/null | awk -F: '{s+=$2} END {print s}')
echo "{\"id\":\"$id\",\"rep\":$rep,\"time\":\"$t\",\"exit\":\"$ex\",\"maxtmp\":$maxtmp,\"outsz\":$outsz,\"oomhint\":$killed,\"res\":${res:-null}}" >> results/runs.jsonl
grep -v "duckdb_storage\|shared_home\|re-downloaded\|Secrets\|removed when\|RtmpvE\|assumes that they\|are planar\|s2) switched" $work/stderr.txt | tail -c 1500 > results/stderr_$id.txt
rm -rf "$work"
echo "$id $t $ex tmp=$maxtmp"
