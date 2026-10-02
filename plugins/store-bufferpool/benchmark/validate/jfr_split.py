#!/usr/bin/env python3
"""Split the search-thread JFR samples of jfr_profile.py recordings into main query / look-ahead build / planner advance.
Usage: validate/jfr_split.py MODE,REC.jfr,RUNS,MEDIAN_MS [...]   (needs `jfr` on PATH)
build = under DocValuesPrefetch.queryMatches; advance = under DocValuesPrefetch$Planner; main = the rest.
"""
import json,subprocess,sys,collections
def cats(path):
    out=subprocess.run(['jfr','print','--json','--stack-depth','200','--events','jdk.ExecutionSample',path],capture_output=True,text=True).stdout
    ev=json.loads(out)['recording']['events']
    c=collections.Counter(); pts=collections.Counter(); n=0
    for e in ev:
        st=e['values'].get('stackTrace')
        if not st: continue
        fr=[f['method']['type']['name']+'.'+f['method']['name'] for f in st['frames']]
        th=e['values']['sampledThread']['javaName'] or ''
        if 'search' not in th: continue
        n+=1
        s=' '.join(fr)
        if 'DocValuesPrefetch.queryMatches' in s: k='build'
        elif 'DocValuesPrefetch$Planner' in s: k='advance'
        elif 'DocValuesPrefetch.planner' in s: k='advance'
        else: k='main'
        c[k]+=1
        if 'DocIdSetBuilder' in s or 'BKDReader' in s or 'BKDPointTree' in s: pts[k]+=1
    return n,c,pts
for p in sys.argv[1:]:
    mode,path,runs,med=p.split(',')
    n,c,pts=cats(path)
    print(mode, f"samples={n}", ' '.join(f"{k}={c[k]/n*100:.1f}%(~{c[k]/n*float(med):.1f}ms, points {pts[k]/n*100:.1f}%)" for k in ('main','build','advance')))
