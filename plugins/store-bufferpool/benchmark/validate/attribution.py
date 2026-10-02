#!/usr/bin/env python3
"""Attribution table from one bench_aggs.py result file: median ratio of each mode pair with a bootstrap 95% CI
(`*` = CI excludes 0). Usage: validate/attribution.py results/aggs_YYYYMMDD_HHMMSS.json
"""
import json,random,statistics as st,sys
d=json.load(open(sys.argv[1]))['runs'][0]['results']
R={(x['query'],x['mode'],x['variant']):x['took_ms'] for x in d}
qs=[]; [qs.append(x['query']) for x in d if x['query'] not in qs]
random.seed(1)
def ci(a,b):
    m=st.median(b)/st.median(a)-1
    bs=sorted(st.median(random.choices(b,k=len(b)))/st.median(random.choices(a,k=len(a)))-1 for _ in range(2000))
    lo,hi=bs[50],bs[1949]
    sig='*' if lo>0 or hi<0 else ' '
    return f"{m*100:+4.0f}%{sig}"
pairs=[('vecdec','pf'),('pf','pfs'),('pf','pfl'),('pfl','pfsl'),('pfs','pfsl'),('pf','pfsl'),('vecdec','pfsl')]
print('mode query'.ljust(22),' '.join(f"{b}/{a}".rjust(12) for a,b in pairs))
for mode in ('cold','warm'):
  for q in qs:
    print(f"{mode} {q}".ljust(22),' '.join(ci(R[(q,mode,a)],R[(q,mode,b)]).rjust(12) for a,b in pairs))
