"""One cold traced top-k query per top-k prefetch mode: loads per file (demand / by prefetch), query-thread stall, took."""
import collections, json, sys, time, urllib.request
H = "http://localhost:9200"
IDX = "topk_v2_30700000_42_f246f5c2"
def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(H + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read() or b"{}")
terms = sys.argv[1:] or ["t50", "t20"]
req("PUT", "/_cluster/settings", {"transient": {"bufferpool.simulated_load_latency": "4ms"}})
for name, field, mode, pf in [("dual_nav", "body_dual", "nav", "norms_blocks=0"),
                              ("dual_nav norms2", "body_dual", "nav", "norms_blocks=2&filter=false"),
                              ("dual_nav norms2 filter", "body_dual", "nav", "norms_blocks=2&filter=true"),
                              ("baseline norms2", "body", "doc", "norms_blocks=2&filter=false"),
                              ("dual_nav doc1", "body_dual", "nav", "norms_blocks=0&doc_blocks=1"),
                              ("dual_nav norms2 doc1", "body_dual", "nav", "norms_blocks=2&filter=false&doc_blocks=1")]:
    req("POST", f"/_bufferpool/dual_nav/_mode?mode={mode}")
    req("POST", f"/_bufferpool/topk_prefetch?{pf}" + ("" if "doc_blocks" in pf else "&doc_blocks=0"))
    time.sleep(0.2)
    req("POST", "/_bufferpool/cache/_clear"); req("POST", "/_bufferpool/trace/_start")
    body = {"size": 10, "track_total_hits": False, "stored_fields": "_none_",
            "query": {"bool": {"should": [{"term": {field: t}} for t in terms]}}}
    r = req("POST", f"/{IDX}/_search?request_cache=false", body)
    time.sleep(0.3)
    ev = req("POST", "/_bufferpool/trace/_stop")["events"]
    loads = [e for e in ev if e["size"] != -1]
    waits = [e for e in ev if e["size"] == -1 and "search" in e["thread"]]
    per = collections.defaultdict(lambda: [0, 0])
    for e in loads:
        per[e["file"].rsplit(".", 1)[-1]][1 if e["prefetch"] else 0] += 1
    stall = sum(e["waited_micros"] for e in waits) / 1000
    top = [round(h["_score"], 4) for h in r["hits"]["hits"]][:3]
    print(f"{name:24} took={r['took']:4}ms stall={stall:6.1f}ms loads(demand,prefetch)={dict(per)} top={top}")
req("POST", "/_bufferpool/topk_prefetch?norms_blocks=0&doc_blocks=0"); req("POST", "/_bufferpool/dual_nav/_mode?mode=doc")
