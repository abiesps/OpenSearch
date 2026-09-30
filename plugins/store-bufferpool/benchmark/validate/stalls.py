"""Per prefetch mode, one cold traced query: how long the query thread was blocked on .doc blocks (sum of read waits),
how many reads blocked, and whether requests were issued early enough (time from prefetch load finishing to the read)."""
import json, sys, time, urllib.request
H = "http://localhost:9200"
IDX = "postings_poc_v3_60500000_42_f246f5c2"
def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(H + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read() or b"{}")
terms = sys.argv[1:]
req("PUT", "/_cluster/settings", {"transient": {"bufferpool.simulated_load_latency": "4ms"}})
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
    ev = [e for e in req("POST", "/_bufferpool/trace/_stop")["events"] if e["file"].endswith(".doc")]
    waits = [e for e in ev if e["size"] == -1 and "search" in e["thread"]]
    loads = [e for e in ev if e["size"] != -1]
    stall = sum(e["waited_micros"] for e in waits) / 1000
    print(f"{name:18} took={took:4}ms  .doc loads={len(loads)}  reads that blocked={len(waits):3}  "
          f"query-thread stall={stall:6.1f}ms  not stalled={took - stall:6.1f}ms  "
          f"avg stall per blocked read={stall / max(1, len(waits)):.2f}ms")
