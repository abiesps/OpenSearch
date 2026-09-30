"""Top-k sanity: which bulk scorer runs, and cold IOs per file of top-k vs exhaustive, per query, on the baseline field."""
import collections, json, sys, urllib.request
H = "http://localhost:9200"
IDX = sys.argv[1]
FIELD = sys.argv[2] if len(sys.argv) > 2 else "body"
def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(H + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read() or b"{}")
req("PUT", "/_cluster/settings", {"transient": {"bufferpool.simulated_load_latency": "4ms"}})
stats = req("GET", f"/{IDX}/_stats/docs,store")["indices"][IDX]["primaries"]
print("docs", stats["docs"]["count"], "store MiB", stats["store"]["size_in_bytes"] // 2**20)
for terms, k in [(["t50", "t20"], 10), (["t50", "t5"], 10), (["t50", "t1"], 10), (["t20", "t5", "t1"], 10), (["t5", "t1", "t01"], 10)]:
    should = [{"term": {FIELD: t}} for t in terms]
    row = []
    for mode, body in [("topk", {"size": k, "track_total_hits": False, "stored_fields": "_none_", "query": {"bool": {"should": should}}}),
                       ("full", {"size": 0, "track_total_hits": True, "query": {"bool": {"should": should}}})]:
        req("POST", "/_bufferpool/cache/_clear"); req("POST", "/_bufferpool/stats/_reset")
        req("POST", "/_bufferpool/trace/_start")
        r = req("POST", f"/{IDX}/_search?request_cache=false", body)
        ev = [e for e in req("POST", "/_bufferpool/trace/_stop")["events"] if e["size"] != -1]
        by_file = collections.Counter(e["file"].split("_", 2)[-1] if "Lucene104" in e["file"] else e["file"].rsplit(".", 1)[-1] for e in ev)
        scorers = collections.Counter(e["search"] for e in ev if e["search"])
        top = [h["_score"] for h in r["hits"]["hits"]][:3]
        row.append(f"{mode}: took={r['took']}ms loads={len(ev)} {dict(by_file)} scorer={scorers.most_common(2)} top={top}")
    print(" OR ".join(terms)); [print("   ", x) for x in row]
