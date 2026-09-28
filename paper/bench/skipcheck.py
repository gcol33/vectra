import json,sys
cfg, rep = sys.argv[1], int(sys.argv[2])
for l in open('/home/claude/bench/results/runs.jsonl'):
    d = json.loads(l)
    if d['id'] == cfg + '_1':
        t = d['time'].split()
        if rep >= 3 and len(t) > 1 and float(t[1]) > 150: print('skip'); sys.exit()
print('run')
