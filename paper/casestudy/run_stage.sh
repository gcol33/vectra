#!/bin/bash
# run_stage.sh <script.R>: fresh process, cold cache, wall time, peak RSS, temp disk
s=$1; name=$(basename $s .R)
tmp=/home/claude/cs/tmp_$name; rm -rf $tmp; mkdir -p $tmp
sync; echo 3 > /proc/sys/vm/drop_caches
cd /home/claude/cs
( TMPDIR=$tmp OMP_NUM_THREADS=2 /usr/bin/time -f "TIME %e %M" -o time_$name.txt Rscript $s > log_$name.txt 2>&1; echo $? > exit_$name.txt ) &
pid=$!; maxtmp=0
while kill -0 $pid 2>/dev/null; do s2=$(du -sb $tmp | cut -f1); [ "$s2" -gt "$maxtmp" ] && maxtmp=$s2; sleep 1; done
wait $pid
echo "{\"stage\":\"$name\",\"time\":\"$(tail -1 time_$name.txt)\",\"exit\":$(cat exit_$name.txt),\"maxtmp\":$maxtmp}" >> stages.jsonl
rm -rf $tmp
tail -1 stages.jsonl
