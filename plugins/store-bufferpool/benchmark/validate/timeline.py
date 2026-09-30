import json, sys, time, urllib.request
H = "http://localhost:9200"; IDX = "postings_poc_v3_60500000_42_f246f5c2"
def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(H + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read() or b"{}")
pf, terms = sys.argv[1], sys.argv[2:]
req("POST", "/_bufferpool/dual_nav/_mode?mode=nav"); req("POST", f"/_bufferpool/disjunction_prefetch?{pf}")
time.sleep(0.2); req("POST", "/_bufferpool/cache/_clear"); req("POST", "/_bufferpool/trace/_start")
body = {"size": 0, "track_total_hits": True, "query": {"bool": {"should": [{"term": {"tag_dual": t}} for t in terms], "minimum_should_match": 1}}}
took = req("POST", f"/{IDX}/_search?request_cache=false", body)["took"]
ev = sorted(req("POST", "/_bufferpool/trace/_stop")["events"], key=lambda e: e["micros"])
print("took", took, "search threads:", sorted({e["thread"] for e in ev if "search" in e["thread"]}))
t0 = ev[0]["micros"]
for e in ev:
    if e["file"].endswith(".doc"):
        kind = f"WAIT {e['waited_micros']/1000:.1f}ms" if e["size"] == -1 else ("prefetch load" if e["prefetch"] else "demand load")
        print(f"{(e['micros']-t0)/1000:7.1f}ms block {e['block']:3} {kind:16} {e['thread'][-18:]:18} {e['search']}")
