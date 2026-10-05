"""For each variant: wait for an idle prefetch pool, clear the cache, check it is empty, run one cold query,
then check (right away and 1 s later) that no prefetch work or block load happens after the query returned."""
import json, sys, time, urllib.request
H = "http://localhost:9200"
IDX = "postings_poc_v3_60500000_42_f246f5c2"
def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(H + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read() or b"{}")
def pool():
    """(active, queue, items run, dropped). With the prefetch scheduler the thread pool runs workers, not items: read
    prefetch_scheduler.items_started (items a worker ran) and pending (queued plus running) instead."""
    n = next(iter(req("GET", "/_nodes/stats/thread_pool")["nodes"].values()))
    p = n["thread_pool"]["bufferpool_prefetch"]
    s = req("GET", "/_bufferpool/stats").get("prefetch_scheduler")
    if s is None:
        return p["active"], p["queue"], p["completed"], p["rejected"]
    return p["active"], s["pending"], s["items_started"], sum(s["dropped"].values())
def doc_io():
    s = req("GET", "/_bufferpool/stats")
    f = s["files"]["Lucene104DualNav_0.doc"] if FIELD == "tag_dual" else s["files"]["Lucene104Baseline_0.doc"]
    return s["cached_blocks"], f["loads"], f["prefetch_loads"], f["prefetch_requests"]
def wait_idle():
    t0 = time.time()
    while True:
        a, q, c, r = pool()
        if a == 0 and q == 0:
            return time.time() - t0
        time.sleep(0.01)
req("PUT", "/_cluster/settings", {"transient": {"bufferpool.simulated_load_latency": "4ms"}})
terms = sys.argv[1:] or ["d50", "d10"]
variants = [("baseline", "tag", "doc", "blocks=0&aligned=false"), ("nav", "tag_dual", "nav", "blocks=0&aligned=false"),
            ("pf1", "tag_dual", "nav", "blocks=1&aligned=false"), ("pfa1", "tag_dual", "nav", "blocks=1&aligned=true")]
print("query:", " OR ".join(terms))
for rep in range(2):
    for name, FIELD, mode, pf in variants:
        req("POST", f"/_bufferpool/dual_nav/_mode?mode={mode}")
        req("POST", f"/_bufferpool/disjunction_prefetch?{pf}")
        waited = wait_idle()
        req("POST", "/_bufferpool/cache/_clear")
        cached_after_clear = req("GET", "/_bufferpool/stats")["cached_blocks"]
        req("POST", "/_bufferpool/stats/_reset")
        c0 = pool()[2]
        body = {"size": 0, "track_total_hits": True,
                "query": {"bool": {"should": [{"term": {FIELD: t}} for t in terms], "minimum_should_match": 1}}}
        t = time.perf_counter()
        r = req("POST", f"/{IDX}/_search?request_cache=false", body)
        wall = (time.perf_counter() - t) * 1000
        a0, q0, c1, _ = pool()
        io0 = doc_io()
        time.sleep(1.0)
        a1, q1, c2, rej = pool()
        io1 = doc_io()
        print(f"{name:8} took={r['took']:4}ms wall={wall:5.0f}ms hits={r['hits']['total']['value']} "
              f"| before: idle-wait={waited*1000:.0f}ms cached_after_clear={cached_after_clear} "
              f"| at return: pool active={a0} queue={q0} doc loads(demand,prefetch)={io0[1]},{io0[2]} "
              f"| +1s: pool active={a1} queue={q1} doc loads={io1[1]},{io1[2]} cached={io1[0]} "
              f"| tasks run for this query={c2 - c0} rejected_total={rej}")
