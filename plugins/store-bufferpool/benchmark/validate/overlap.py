"""Traces one cold query per prefetch mode and reports, from block-load completion times, how many .doc loads were in
flight over the query, per clause and in total, and how long the query thread spent between consecutive loads."""
import json, sys, time, urllib.request
H = "http://localhost:9200"
IDX = "postings_poc_v3_60500000_42_f246f5c2"
def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(H + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read() or b"{}")
layout = json.loads(open("/tmp/layout.txt").read()[open("/tmp/layout.txt").read().index("{"):])["layout"]["tag"]  # DualNav .doc = baseline .doc body, header 1 byte shorter
terms = sys.argv[1:]
B = 131072
blocks_of = {t: set(range((layout[t]["doc_start"] - 1) // B, (layout[t]["doc_end"] - 2) // B + 1)) for t in terms}
req("POST", "/_bufferpool/dual_nav/_mode?mode=nav")
for name, pf in [("nav, no prefetch", "blocks=0&aligned=false"), ("pf1 byte budget", "blocks=1&aligned=false"),
                 ("pf1 node-aligned", "blocks=1&aligned=true")]:
    req("POST", f"/_bufferpool/disjunction_prefetch?{pf}")
    time.sleep(0.2)
    req("POST", "/_bufferpool/cache/_clear")
    req("POST", "/_bufferpool/trace/_start")
    body = {"size": 0, "track_total_hits": True,
            "query": {"bool": {"should": [{"term": {"tag_dual": t}} for t in terms], "minimum_should_match": 1}}}
    took = req("POST", f"/{IDX}/_search?request_cache=false", body)["took"]
    ev = [e for e in req("POST", "/_bufferpool/trace/_stop")["events"] if e["file"].endswith(".doc") and e["size"] != -1]
    ends = [e["micros"] / 1000 for e in ev]            # load finished (after the 4 ms delay)
    iv = [(t - 4.0, t) for t in ends]                  # each load occupies ~4 ms before it finishes
    t0, t1 = min(a for a, _ in iv), max(b for _, b in iv)
    step = 0.05
    samples = [sum(1 for a, b in iv if a <= t0 + k * step < b) for k in range(int((t1 - t0) / step))]
    busy = [s for s in samples if s > 0]
    per_term = {}
    for t in terms:
        mine = [(e["micros"] / 1000 - 4.0, e["micros"] / 1000) for e in ev if e["offset"] // B in blocks_of[t]]
        if mine:
            s2 = [sum(1 for a, b in mine if a <= x < b) for x in [t0 + k * step for k in range(int((t1 - t0) / step))]]
            s2 = [s for s in s2 if s]
            per_term[t] = f"{len(mine)} loads, max {max(s2)}, avg {sum(s2)/len(s2):.1f} in flight"
    print(f"{name:18} took={took}ms  .doc loads={len(ev)} (prefetch {sum(e['prefetch'] for e in ev)})  "
          f"load span={t1 - t0:.0f}ms  in flight: max {max(samples)}, avg while busy {sum(busy)/len(busy):.1f}  "
          f"threads={len({e['thread'] for e in ev})}")
    for t, s in per_term.items():
        print(f"{'':20}{t}: {s}")
