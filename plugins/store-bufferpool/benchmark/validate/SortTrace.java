
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilterNumericDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectExperiments;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FilterDocIdSetIterator;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.comparators.ComparatorExperiments;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Runs bench_aggs.py sort queries (sort_ORDER:SEL:WINDOW[:SIZE][:nt|:tt], SEL s50/s10/s1/all; not "bare" and not
 * all:all, which OpenSearch answers with its approximation framework) directly in Lucene on the benchmark index, built
 * the way OpenSearch builds them (ConstantScore(bool filter [term sel, IndexOrDocValuesQuery(LongPoint range, doc-values
 * range)]), SortedNumericSortField with the MAX selector for desc and MIN for asc, TopFieldCollectorManager with a total
 * hits threshold of 10,000, 1 for track_total_hits false, or Integer.MAX_VALUE for true, each leaf scored in
 * OpenSearch's CancellableBulkScorer chunks), and reports how the points of the sort field are used: every getPointTree
 * call with its caller (the sort comparator's competitive iterator or the range query), which of them are
 * intersections, the estimates the comparator makes before deciding to intersect, and every leaf each intersection
 * reads, as visitDocIDs (the cell is inside the range: doc IDs only) or visitDocValues (the cell crosses: doc IDs and
 * values). Leaf ordinals are in value order on either tree class: the stock tree by getLeafBlockFP, the split tree
 * (Lucene90Split points) by its leaf ID. Also the leaves that hold the returned hits and how many docs reached the
 * collector, and the counter J: the nextDoc/advance calls on the comparator's competitive iterator whose result skips
 * at least one whole doc-values node of the sort field (some node lies strictly between the previous doc and the
 * result; intoBitSet calls are forwarded and their skipped nodes counted from the bits they set). Node boundaries come
 * from NumericDocValues.nextPrefetchNodeDoc(doc, NODE_BYTES) on a separate doc-values instance. The counting wrapper
 * forwards intoBitSet, docIDRunEnd and prefetchAhead, so the replay takes the same code paths as an unwrapped run.
 * Output: JSON lines, one per (query, field), after one "layout" line per field (nodes, minNodeDocs). A short side by
 * side summary per query goes to stderr. Opens the index read-only.
 *
 * <p>Run (Java 21+): java -cp 'DISTRO/lib/*' validate/SortTrace.java SHARD_INDEX_DIR [SPEC[,SPEC...]] [OUT.jsonl]
 * [--field=F | --fields=F1,F2] [--node-bytes=131072] [--k1] [--k2=N] [--k3] [--k4=off|fallback|first]
 * [--check-twin[=BASE,TWIN]]
 * <ul>
 *   <li>--field/--fields: the sort and range field(s); default @timestamp. With two fields every query runs on each.
 *   <li>--k1 (competitive bounds = the query range, needs SortField.setCompetitiveBounds), --k2=N
 *       (ComparatorExperiments.setSampleDocs), --k3 (CollectExperiments.setCompetitiveRunCap), --k4=MODE
 *       (ComparatorExperiments.setSkipperMode): Lucene switches set for the replay.
 *   <li>--check-twin: compares BASE and TWIN (default @timestamp,@timestamp_split) over all docs: the doc values per
 *       doc, the point values per doc, PointValues size, getDocCount, min and max. Exits 1 on any difference.
 * </ul>
 */
public class SortTrace {
    static final long START_MS = 1788220800000L; // bench_aggs.START_MS
    static final long DAY = 86_400_000L;

    // per query state
    static final List<String> calls = new ArrayList<>(); // getPointTree callers, by call id
    static final List<int[]> visits = new ArrayList<>(); // {call id, leaf ordinal, 0 = doc IDs (inside) / 1 = values (crosses)}
    static final List<Integer> estimates = new ArrayList<>(); // per call id: estimates started at the root of that tree
    static final TreeMap<String, long[]> dvReads = new TreeMap<>(); // sort field doc values: creator -> {advanceExact, nextDoc+advance}
    static final long[] jCounts = new long[4]; // {J, nextDoc+advance calls, intoBitSet calls, nodes skipped in total}
    static String traced; // the field of the current query
    static final Map<String, FieldState> fieldStates = new HashMap<>();
    // per time run of the traced field: {setBottom calls, comparator update attempts (estimates on the comparator's
    // tree, or installs when it has no tree), attempts outside a comparator callback (K2 trailing), comparator
    // intersections, bound writes (setBottom/threshold calls after which the comparator's competitive bound
    // changed), delivered docs}
    static final int RUN_STATS = 6;
    static long[][] runStats = new long[0][RUN_STATS];
    static int curDoc = -1; // last doc the comparator saw (copy/compareBottom/compareTop)
    static int inCallback; // > 0 while a comparator callback (setBottom, setHitsThresholdReached, setScorer) runs

    static void runCount(int doc, int stat, long n) {
        final FieldState fs = fieldStates.get(traced);
        if (fs == null || runStats.length == 0) return;
        runStats[fs.runOf(Math.max(doc, 0))][stat] += n;
    }

    /** Per sort field: leaf ordinal of every doc, how to read the leaf ordinal of a tree, and the doc-values nodes. */
    static final class FieldState {
        final String name;
        final int[] docLeaf;
        int numLeaves;
        long[] leafFps; // stock tree: leaf FPs in order
        Method leafKey; // stock: getLeafBlockFP, split: leafID
        boolean split;
        int[] nodeStarts; // first doc of every doc-values node, ascending
        int[] runStarts; // first doc of every time run (the value jumps by more than a minute), ascending

        final List<Long> runMin = new ArrayList<>(), runMax = new ArrayList<>(); // value range of every time run

        int runOf(int doc) {
            final int i = Arrays.binarySearch(runStarts, doc);
            return i >= 0 ? i : -i - 2;
        }

        FieldState(String name, int maxDoc) {
            this.name = name;
            this.docLeaf = new int[maxDoc];
        }

        int leafOrd(PointValues.PointTree leafTree) throws IOException {
            try {
                if (split) {
                    return (int) leafKey.invoke(leafTree);
                }
                final long fp = (long) leafKey.invoke(leafTree);
                final int i = Arrays.binarySearch(leafFps, fp);
                if (i < 0) throw new IllegalStateException("unknown leaf fp " + fp);
                return i;
            } catch (ReflectiveOperationException e) {
                throw new IOException(e);
            }
        }

        /** Index of the node holding doc ({@code -1} for doc -1). */
        int nodeOf(int doc) {
            if (doc < 0) return -1;
            final int i = Arrays.binarySearch(nodeStarts, doc);
            return i >= 0 ? i : -i - 2;
        }
    }

    public static void main(String[] args) throws Exception {
        final List<String> positional = new ArrayList<>();
        List<String> fields = List.of("@timestamp");
        long nodeBytes = 131_072;
        boolean k1 = false;
        String checkTwin = null;
        for (String a : args) {
            if (a.startsWith("--field=")) fields = List.of(a.substring("--field=".length()));
            else if (a.startsWith("--fields=")) fields = Arrays.asList(a.substring("--fields=".length()).split(","));
            else if (a.startsWith("--node-bytes=")) nodeBytes = Long.parseLong(a.substring("--node-bytes=".length()));
            else if (a.equals("--k1")) k1 = true;
            else if (a.startsWith("--k2=")) ComparatorExperiments.setSampleDocs(Integer.parseInt(a.substring("--k2=".length())));
            else if (a.equals("--k3")) CollectExperiments.setCompetitiveRunCap(true);
            else if (a.startsWith("--k4=")) ComparatorExperiments.setSkipperMode(
                ComparatorExperiments.SkipperMode.valueOf(a.substring("--k4=".length()).toUpperCase(Locale.ROOT))
            );
            else if (a.equals("--check-twin")) checkTwin = "@timestamp,@timestamp_split";
            else if (a.startsWith("--check-twin=")) checkTwin = a.substring("--check-twin=".length());
            else if (a.startsWith("--")) throw new IllegalArgumentException("unknown flag " + a);
            else positional.add(a);
        }
        try (Directory d = new NIOFSDirectory(Path.of(positional.get(0))); DirectoryReader raw = DirectoryReader.open(d)) {
            if (raw.leaves().size() != 1) throw new IllegalStateException("expected one segment, found " + raw.leaves().size());
            final LeafReader leaf = raw.leaves().get(0).reader();
            if (checkTwin != null) {
                final String[] f = checkTwin.split(",");
                System.exit(checkTwin(leaf, f[0], f[1]) ? 0 : 1);
            }
            final PrintStream out = positional.size() > 2 ? new PrintStream(positional.get(2)) : System.out;
            for (String field : fields) {
                final FieldState fs = new FieldState(field, leaf.maxDoc());
                indexLeaves(leaf.getPointValues(field), fs);
                nodeStarts(leaf, fs, nodeBytes);
                fieldStates.put(field, fs);
                printLayout(out, fs, nodeBytes);
            }
            if (positional.size() < 2) return;
            final DirectoryReader reader = new FilterDirectoryReader(raw, new FilterDirectoryReader.SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader r) {
                    return new CountingLeafReader(r);
                }
            }) {
                @Override
                protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public CacheHelper getReaderCacheHelper() {
                    return null;
                }
            };
            final IndexSearcher searcher = new ChunkedSearcher(reader);
            searcher.setQueryCache(null);
            for (String spec : positional.get(1).split(",")) {
                final StringBuilder summary = new StringBuilder(spec);
                for (String field : fields) {
                    traced = field;
                    for (int rep = 0; rep < 2; rep++) { // the first run warms the JIT; report the second (same result)
                        calls.clear();
                        visits.clear();
                        estimates.clear();
                        dvReads.clear();
                        Arrays.fill(jCounts, 0);
                        runStats = new long[fieldStates.get(field).runStarts.length][RUN_STATS];
                        curDoc = -1;
                        inCallback = 0;
                        final String[] p = spec.split(":");
                        final boolean desc = p[0].equals("sort_desc");
                        final String sel = p[1], window = p[2];
                        int size = 10;
                        int threshold = 10_000;
                        for (int i = 3; i < p.length; i++) {
                            if (p[i].equals("nt")) threshold = 1;
                            else if (p[i].equals("tt")) threshold = Integer.MAX_VALUE;
                            else size = Integer.parseInt(p[i]);
                        }
                        final Query q = query(field, sel, window);
                        final SortedNumericSortField sortField = new TracingSortField(
                            field,
                            desc,
                            desc ? SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN
                        );
                        if (k1 && window.equals("all") == false) {
                            final long[] r = range(window);
                            try {
                                sortField.getClass()
                                    .getMethod("setCompetitiveBounds", long.class, long.class)
                                    .invoke(sortField, r[0], r[1]);
                            } catch (NoSuchMethodException e) {
                                throw new IllegalStateException("--k1 needs SortedNumericSortField.setCompetitiveBounds (FEAT-007)", e);
                            }
                        }
                        final long t0 = System.nanoTime();
                        final TopFieldDocs top = searcher.search(
                            q,
                            new TopFieldCollectorManager(new Sort(sortField), size, null, threshold)
                        );
                        final long nanos = System.nanoTime() - t0;
                        if (rep == 0) continue;
                        print(out, spec, field, q, top, nanos);
                        int inter = 0;
                        for (String c : calls)
                            if (c.startsWith("intersect<")) inter++;
                        int vd = 0, vv = 0;
                        for (int[] v : visits)
                            if (v[2] == 1) vv++;
                            else vd++;
                        summary.append(
                            String.format(
                                Locale.ROOT,
                                " | %s: calls %d, intersections %d, estimates %s, leaves %d ids + %d values, J %d, per run "
                                    + "[setBottom, attempts, trailing, intersects, bound writes, delivered] %s",
                                field,
                                calls.size(),
                                inter,
                                estimates,
                                vd,
                                vv,
                                jCounts[0],
                                Arrays.deepToString(runStats)
                            )
                        );
                    }
                }
                System.err.println(summary);
            }
        }
    }

    static long[] range(String window) {
        final long lo = START_MS + (window.equals("7d") ? 0 : 3 * DAY);
        final long hi = START_MS + (window.equals("7d") ? 7 * DAY : 4 * DAY) - 1; // "lt"
        return new long[] { lo, hi };
    }

    static Query query(String field, String sel, String window) {
        Query range = null;
        if (window.equals("all") == false) {
            final long[] r = range(window);
            range = new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery(field, r[0], r[1]),
                SortedNumericDocValuesField.newSlowRangeQuery(field, r[0], r[1])
            );
        }
        final BooleanQuery.Builder b = new BooleanQuery.Builder();
        if (sel.equals("all") == false) b.add(new TermQuery(new Term("sel", sel)), BooleanClause.Occur.FILTER);
        if (range != null) b.add(range, BooleanClause.Occur.FILTER);
        if (sel.equals("all") && range == null) return new MatchAllDocsQuery();
        return new ConstantScoreQuery(b.build());
    }

    static void indexLeaves(PointValues pv, FieldState fs) throws Exception {
        final PointValues.PointTree tree = pv.getPointTree();
        fs.split = tree.getClass().getName().endsWith("SplitBKDPointTree");
        fs.leafKey = tree.getClass().getDeclaredMethod(fs.split ? "leafID" : "getLeafBlockFP");
        fs.leafKey.setAccessible(true);
        final List<Long> fps = new ArrayList<>();
        walk(tree, fps, fs);
        fs.numLeaves = fps.size();
        if (fs.split) {
            for (int i = 0; i < fps.size(); i++) {
                if (fps.get(i) != i) throw new IllegalStateException(fs.name + ": leaf IDs out of order at " + i);
            }
        } else {
            fs.leafFps = fps.stream().mapToLong(Long::longValue).toArray();
            for (int i = 1; i < fs.leafFps.length; i++) {
                if (fs.leafFps[i] <= fs.leafFps[i - 1]) throw new IllegalStateException(fs.name + ": leaves out of file order");
            }
        }
    }

    static void walk(PointValues.PointTree tree, List<Long> keys, FieldState fs) throws Exception {
        if (tree.moveToChild()) {
            do {
                walk(tree, keys, fs);
            } while (tree.moveToSibling());
            tree.moveToParent();
            return;
        }
        final int ord = keys.size();
        keys.add(((Number) fs.leafKey.invoke(tree)).longValue());
        tree.visitDocIDs(new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {
                fs.docLeaf[docID] = ord;
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                fs.docLeaf[docID] = ord;
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                return PointValues.Relation.CELL_INSIDE_QUERY;
            }
        });
    }

    /** First doc of every doc-values node of nodeBytes bytes, from nextPrefetchNodeDoc. */
    static void nodeStarts(LeafReader leaf, FieldState fs, long nodeBytes) throws IOException {
        final NumericDocValues dv = DocValues.unwrapSingleton(leaf.getSortedNumericDocValues(fs.name));
        if (dv == null) throw new IllegalStateException(fs.name + ": doc values are not single-valued");
        final List<Integer> starts = new ArrayList<>();
        starts.add(0);
        int doc = 0;
        while (true) {
            final int next = dv.nextPrefetchNodeDoc(doc, nodeBytes);
            if (next < 0) throw new IllegalStateException(fs.name + ": nextPrefetchNodeDoc is not supported");
            if (next == DocIdSetIterator.NO_MORE_DOCS || next >= leaf.maxDoc()) break;
            if (next <= doc) throw new IllegalStateException(fs.name + ": nextPrefetchNodeDoc(" + doc + ") = " + next);
            starts.add(next);
            doc = next;
        }
        fs.nodeStarts = starts.stream().mapToInt(Integer::intValue).toArray();
        // time runs: a new run starts where the value jumps (either way) by more than a minute from the previous doc
        final NumericDocValues values = DocValues.unwrapSingleton(leaf.getSortedNumericDocValues(fs.name));
        final List<Integer> runs = new ArrayList<>();
        runs.add(0);
        long prev = Long.MIN_VALUE;
        for (int d = values.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = values.nextDoc()) {
            final long v = values.longValue();
            if (prev != Long.MIN_VALUE && Math.abs(v - prev) > 60_000L) runs.add(d);
            if (runs.size() > fs.runMin.size()) {
                fs.runMin.add(v);
                fs.runMax.add(v);
            }
            final int r = runs.size() - 1;
            fs.runMin.set(r, Math.min(fs.runMin.get(r), v));
            fs.runMax.set(r, Math.max(fs.runMax.get(r), v));
            prev = v;
        }
        fs.runStarts = runs.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * The node layout of a field: minNodeDocs is the smallest number of docs of a whole node, i.e. of every node but the
     * first and the last, which are cut by the start and the end of the field's values in the file (both reported).
     */
    static void printLayout(PrintStream out, FieldState fs, long nodeBytes) {
        final int[] s = fs.nodeStarts;
        final int maxDoc = fs.docLeaf.length;
        final int[] docs = new int[s.length];
        for (int i = 0; i < s.length; i++) {
            docs[i] = (i + 1 < s.length ? s[i + 1] : maxDoc) - s[i];
        }
        int min = Integer.MAX_VALUE, max = 0, minAll = Integer.MAX_VALUE;
        for (int i = 0; i < s.length; i++) {
            minAll = Math.min(minAll, docs[i]);
            if (i > 0 && i + 1 < s.length) {
                min = Math.min(min, docs[i]);
                max = Math.max(max, docs[i]);
            }
        }
        if (s.length < 3) {
            min = minAll;
            max = Math.max(docs[0], docs[s.length - 1]);
        }
        out.printf(
            Locale.ROOT,
            "{\"layout\": \"%s\", \"tree\": \"%s\", \"leaves\": %d, \"node_bytes\": %d, \"nodes\": %d, \"min_node_docs\": %d, "
                + "\"max_node_docs\": %d, \"first_node_docs\": %d, \"last_node_docs\": %d, \"min_node_docs_incl_edges\": %d, "
                + "\"run_starts\": %s, \"run_hours\": %s}%n",
            fs.name,
            fs.split ? "split" : "stock",
            fs.numLeaves,
            nodeBytes,
            s.length,
            min,
            max,
            docs[0],
            docs[s.length - 1],
            minAll,
            Arrays.toString(fs.runStarts),
            runHours(fs)
        );
        out.flush();
    }

    /** Value range of every time run in hours since START_MS, [[min, max], ...]. */
    static String runHours(FieldState fs) {
        final StringBuilder sb = new StringBuilder("[");
        for (int r = 0; r < fs.runMin.size(); r++) {
            sb.append(r == 0 ? "" : ", ")
                .append(
                    String.format(Locale.ROOT, "[%.2f, %.2f]", (fs.runMin.get(r) - START_MS) / 3.6e6, (fs.runMax.get(r) - START_MS) / 3.6e6)
                );
        }
        return sb.append(']').toString();
    }

    /** --check-twin: per-doc doc values and point values, size, docCount, min and max of base and twin. */
    static boolean checkTwin(LeafReader leaf, String base, String twin) throws IOException {
        boolean ok = true;
        final int maxDoc = leaf.maxDoc();
        // doc values, doc by doc
        final SortedNumericDocValues a = leaf.getSortedNumericDocValues(base), b = leaf.getSortedNumericDocValues(twin);
        long dvDocs = 0, dvDiff = 0;
        String firstDvDiff = null;
        for (int doc = 0; doc < maxDoc; doc++) {
            final boolean ha = a.advanceExact(doc), hb = b.advanceExact(doc);
            boolean same = ha == hb;
            if (same && ha) {
                dvDocs++;
                same = a.docValueCount() == b.docValueCount();
                for (int i = 0; same && i < a.docValueCount(); i++) {
                    same = a.nextValue() == b.nextValue();
                }
            }
            if (same == false) {
                dvDiff++;
                if (firstDvDiff == null) firstDvDiff = "doc " + doc;
            }
        }
        // point values, doc by doc (single-valued: one point per doc)
        final PointValues pa = leaf.getPointValues(base), pb = leaf.getPointValues(twin);
        final long[] values = new long[maxDoc];
        final FixedBitSet seen = new FixedBitSet(maxDoc);
        final long[] multi = new long[1];
        pa.intersect(new AllPoints() {
            @Override
            public void visit(int docID, byte[] packedValue) {
                if (seen.getAndSet(docID)) multi[0]++;
                values[docID] = NumericUtils.sortableBytesToLong(packedValue, 0);
            }
        });
        final FixedBitSet seenTwin = new FixedBitSet(maxDoc);
        final long[] pointDiff = new long[1];
        final String[] firstPointDiff = new String[1];
        pb.intersect(new AllPoints() {
            @Override
            public void visit(int docID, byte[] packedValue) {
                if (seenTwin.getAndSet(docID)) multi[0]++;
                final long v = NumericUtils.sortableBytesToLong(packedValue, 0);
                if (seen.get(docID) == false || values[docID] != v) {
                    pointDiff[0]++;
                    if (firstPointDiff[0] == null) firstPointDiff[0] = "doc " + docID + ": " + values[docID] + " vs " + v;
                }
            }
        });
        if (seen.cardinality() != seenTwin.cardinality()) pointDiff[0]++;
        final boolean sameStats = pa.size() == pb.size()
            && pa.getDocCount() == pb.getDocCount()
            && Arrays.equals(pa.getMinPackedValue(), pb.getMinPackedValue())
            && Arrays.equals(pa.getMaxPackedValue(), pb.getMaxPackedValue());
        ok = dvDiff == 0 && pointDiff[0] == 0 && multi[0] == 0 && sameStats;
        System.out.printf(
            Locale.ROOT,
            "{\"check_twin\": \"%s\", \"base\": \"%s\", \"twin\": \"%s\", \"max_doc\": %d, \"doc_values_docs\": %d, "
                + "\"doc_values_differences\": %d, \"first_doc_values_difference\": %s, \"point_docs\": [%d, %d], "
                + "\"point_differences\": %d, \"first_point_difference\": %s, \"multi_valued_points\": %d, "
                + "\"size\": [%d, %d], \"doc_count\": [%d, %d], \"min\": [%d, %d], \"max\": [%d, %d], "
                + "\"base_tree\": \"%s\", \"twin_tree\": \"%s\"}%n",
            ok ? "pass" : "FAIL",
            base,
            twin,
            maxDoc,
            dvDocs,
            dvDiff,
            firstDvDiff == null ? "null" : "\"" + firstDvDiff + "\"",
            seen.cardinality(),
            seenTwin.cardinality(),
            pointDiff[0],
            firstPointDiff[0] == null ? "null" : "\"" + firstPointDiff[0] + "\"",
            multi[0],
            pa.size(),
            pb.size(),
            pa.getDocCount(),
            pb.getDocCount(),
            NumericUtils.sortableBytesToLong(pa.getMinPackedValue(), 0),
            NumericUtils.sortableBytesToLong(pb.getMinPackedValue(), 0),
            NumericUtils.sortableBytesToLong(pa.getMaxPackedValue(), 0),
            NumericUtils.sortableBytesToLong(pb.getMaxPackedValue(), 0),
            pa.getPointTree().getClass().getSimpleName(),
            pb.getPointTree().getClass().getSimpleName()
        );
        return ok;
    }

    /** Visits every point with its value. */
    abstract static class AllPoints implements PointValues.IntersectVisitor {
        @Override
        public void visit(int docID) {
            throw new AssertionError("every cell crosses");
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }
    }

    static void print(PrintStream out, String spec, String field, Query q, TopFieldDocs top, long nanos) {
        final int[] docLeaf = fieldStates.get(field).docLeaf;
        final StringBuilder sb = new StringBuilder();
        sb.append(
            String.format(
                Locale.ROOT,
                "{\"query\": \"%s\", \"field\": \"%s\", \"lucene_query\": \"%s\", \"millis\": %.1f, \"collected\": %d, \"relation\": \"%s\"",
                spec,
                field,
                q.toString().replace("\"", "'"),
                nanos / 1e6,
                top.totalHits.value(),
                top.totalHits.relation()
            )
        );
        sb.append(", \"calls\": [");
        for (int i = 0; i < calls.size(); i++) {
            sb.append(i == 0 ? "" : ", ").append('"').append(calls.get(i)).append('"');
        }
        sb.append("], \"estimates\": ").append(estimates);
        sb.append(
            String.format(
                Locale.ROOT,
                ", \"j\": %d, \"competitive_moves\": %d, \"competitive_into_bitset\": %d, \"nodes_skipped\": %d",
                jCounts[0],
                jCounts[1],
                jCounts[2],
                jCounts[3]
            )
        );
        sb.append(", \"run_starts\": ").append(Arrays.toString(fieldStates.get(field).runStarts));
        sb.append(", \"runs\": ").append(Arrays.deepToString(runStats));
        sb.append(", \"dv_reads\": {");
        int k = 0;
        for (var e : dvReads.entrySet()) {
            sb.append(k++ == 0 ? "" : ", ").append('"').append(e.getKey()).append("\": ").append(Arrays.toString(e.getValue()));
        }
        sb.append('}');
        sb.append(", \"visits\": [");
        for (int i = 0; i < visits.size(); i++) {
            final int[] v = visits.get(i);
            sb.append(i == 0 ? "" : ",").append('[').append(v[0]).append(',').append(v[1]).append(',').append(v[2]).append(']');
        }
        sb.append("], \"hits\": [");
        for (int i = 0; i < top.scoreDocs.length; i++) {
            final ScoreDoc sd = top.scoreDocs[i];
            sb.append(i == 0 ? "" : ",")
                .append('[')
                .append(sd.doc)
                .append(',')
                .append(((FieldDoc) sd).fields[0])
                .append(',')
                .append(docLeaf[sd.doc])
                .append(']');
        }
        sb.append("]}");
        out.println(sb);
        out.flush();
    }

    /**
     * Scores each leaf in growing doc ID chunks, 4,096 doubling up to 1,048,576, like OpenSearch's CancellableBulkScorer
     * (ContextIndexSearcher wraps every bulk scorer in it when low-level cancellation is on, the default). The chunking
     * matters for sort: a bulk scorer re-reads the collector's competitive iterator only at the start of each call.
     */
    static final class ChunkedSearcher extends IndexSearcher {
        ChunkedSearcher(DirectoryReader reader) {
            super(reader);
        }

        @Override
        protected void searchLeaf(LeafReaderContext ctx, int minDocId, int maxDocId, Weight weight, Collector collector)
            throws IOException {
            final LeafCollector leafCollector;
            try {
                leafCollector = new JCountingLeafCollector(collector.getLeafCollector(ctx), fieldStates.get(traced));
            } catch (CollectionTerminatedException e) {
                return;
            }
            final ScorerSupplier supplier = weight.scorerSupplier(ctx);
            if (supplier != null) {
                supplier.setTopLevelScoringClause();
                final BulkScorer scorer = supplier.bulkScorer();
                try {
                    int min = minDocId, interval = 1 << 12;
                    while (min < maxDocId) {
                        min = scorer.score(leafCollector, ctx.reader().getLiveDocs(), min, (int) Math.min((long) min + interval, maxDocId));
                        interval = Math.min(interval << 1, 1 << 20);
                    }
                } catch (CollectionTerminatedException e) {
                    // done with this leaf
                }
            }
            leafCollector.finish();
        }
    }

    /**
     * The OpenSearch sort field (LONG, MIN for asc, MAX for desc) whose comparator is wrapped to count, per time run,
     * setBottom calls and bound writes (the leaf comparator's competitive bound read by reflection after each callback)
     * and to track the doc the comparator last saw. Everything is forwarded, so the comparator works as unwrapped.
     */
    static final class TracingSortField extends SortedNumericSortField {
        TracingSortField(String field, boolean reverse, SortedNumericSelector.Type selector) {
            super(field, SortField.Type.LONG, reverse, selector);
        }

        @Override
        @SuppressWarnings("unchecked")
        public FieldComparator<?> getComparator(int numHits, Pruning pruning) {
            final FieldComparator<Object> in = (FieldComparator<Object>) super.getComparator(numHits, pruning);
            return new FieldComparator<Object>() {
                @Override
                public int compare(int slot1, int slot2) {
                    return in.compare(slot1, slot2);
                }

                @Override
                public void setTopValue(Object value) {
                    in.setTopValue(value);
                }

                @Override
                public Object value(int slot) {
                    return in.value(slot);
                }

                @Override
                public int compareValues(Object first, Object second) {
                    return in.compareValues(first, second);
                }

                @Override
                public void setSingleSort() {
                    in.setSingleSort();
                }

                @Override
                public void disableSkipping() {
                    in.disableSkipping();
                }

                @Override
                public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                    return new TracingLeafComparator(in.getLeafComparator(context));
                }
            };
        }
    }

    static final class TracingLeafComparator implements LeafFieldComparator {
        final LeafFieldComparator in;
        final Object builder; // NumericComparator.CompetitiveDISIBuilder, or null
        final java.lang.reflect.Field minField, maxField;
        long lastMin = Long.MIN_VALUE, lastMax = Long.MAX_VALUE;

        TracingLeafComparator(LeafFieldComparator in) {
            this.in = in;
            Object b = null;
            java.lang.reflect.Field mn = null, mx = null;
            try {
                final java.lang.reflect.Field f = field(in.getClass(), "competitiveDISIBuilder");
                b = f == null ? null : f.get(in);
                if (b != null) {
                    mn = field(b.getClass(), "minValueAsLong");
                    mx = field(b.getClass(), "maxValueAsLong");
                    lastMin = mn.getLong(b);
                    lastMax = mx.getLong(b);
                }
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
            this.builder = b;
            this.minField = mn;
            this.maxField = mx;
        }

        static java.lang.reflect.Field field(Class<?> c, String name) {
            for (; c != null; c = c.getSuperclass()) {
                try {
                    final java.lang.reflect.Field f = c.getDeclaredField(name);
                    f.setAccessible(true);
                    return f;
                } catch (NoSuchFieldException e) {
                    // look in the superclass
                }
            }
            return null;
        }

        private void boundCheck() {
            if (builder == null) return;
            try {
                final long mn = minField.getLong(builder), mx = maxField.getLong(builder);
                if (mn != lastMin || mx != lastMax) {
                    runCount(curDoc, 4, 1);
                    lastMin = mn;
                    lastMax = mx;
                }
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void setBottom(int slot) throws IOException {
            runCount(curDoc, 0, 1);
            inCallback++;
            try {
                in.setBottom(slot);
            } finally {
                inCallback--;
            }
            boundCheck();
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            curDoc = doc;
            return in.compareBottom(doc);
        }

        @Override
        public int compareTop(int doc) throws IOException {
            curDoc = doc;
            return in.compareTop(doc);
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            curDoc = doc;
            in.copy(slot, doc);
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            inCallback++;
            try {
                in.setScorer(scorer);
            } finally {
                inCallback--;
            }
            boundCheck();
        }

        @Override
        public DocIdSetIterator competitiveIterator() throws IOException {
            return in.competitiveIterator();
        }

        @Override
        public void setHitsThresholdReached() throws IOException {
            inCallback++;
            try {
                in.setHitsThresholdReached();
            } finally {
                inCallback--;
            }
            boundCheck();
        }
    }

    /** Forwards everything; wraps the competitive iterator (the same wrapper for the same iterator) to count J. */
    static final class JCountingLeafCollector implements LeafCollector {
        final LeafCollector in;
        final FieldState fs;
        DocIdSetIterator wrappedOf;
        DocIdSetIterator wrapper;

        JCountingLeafCollector(LeafCollector in, FieldState fs) {
            this.in = in;
            this.fs = fs;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            in.setScorer(scorer);
        }

        @Override
        public void collect(int doc) throws IOException {
            runCount(doc, 5, 1);
            in.collect(doc);
        }

        @Override
        public void collect(DocIdStream stream) throws IOException {
            // TopFieldCollector's leaf collectors take streams doc by doc (LeafCollector default), so this is the same path
            stream.forEach(doc -> {
                runCount(doc, 5, 1);
                in.collect(doc);
            });
        }

        @Override
        public void collectRange(int min, int max) throws IOException {
            final FieldState f = fieldStates.get(traced);
            for (int r = f.runOf(min); r < f.runStarts.length && f.runStarts[r] < max; r++) {
                final int from = Math.max(min, f.runStarts[r]);
                final int to = r + 1 < f.runStarts.length ? Math.min(max, f.runStarts[r + 1]) : max;
                if (to > from) runStats[r][5] += to - from;
            }
            in.collectRange(min, max);
        }

        @Override
        public DocIdSetIterator competitiveIterator() throws IOException {
            final DocIdSetIterator it = in.competitiveIterator();
            if (it == null) return null;
            if (it != wrappedOf) {
                wrappedOf = it;
                wrapper = new JCountingIterator(it, fs);
            }
            return wrapper;
        }

        @Override
        public void finish() throws IOException {
            in.finish();
        }
    }

    /** Counts nextDoc/advance results that skip a whole doc-values node (J), and the nodes intoBitSet skips. */
    static final class JCountingIterator extends FilterDocIdSetIterator {
        final FieldState fs;

        JCountingIterator(DocIdSetIterator in, FieldState fs) {
            super(in);
            this.fs = fs;
        }

        private int moved(int from, int to) {
            jCounts[1]++;
            if (to != NO_MORE_DOCS) {
                final int skipped = fs.nodeOf(to) - fs.nodeOf(from) - 1;
                if (skipped > 0) {
                    jCounts[0]++;
                    jCounts[3] += skipped;
                }
            }
            return to;
        }

        @Override
        public int nextDoc() throws IOException {
            final int from = in.docID();
            return moved(from, in.nextDoc());
        }

        @Override
        public int advance(int target) throws IOException {
            final int from = in.docID();
            return moved(from, in.advance(target));
        }

        @Override
        public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            final int from = in.docID();
            in.intoBitSet(upTo, bitSet, offset);
            jCounts[2]++;
            // the docs it delivered are the bits set in [from, upTo); a jump between two of them over a whole node is J
            int prev = from;
            final int end = Math.min(upTo, in.docID()) - offset;
            int bit = Math.max(0, from - offset);
            while (bit < end && bit < bitSet.length()) {
                final int next = bitSet.nextSetBit(bit, Math.min(end, bitSet.length()));
                if (next == DocIdSetIterator.NO_MORE_DOCS) break;
                final int doc = next + offset;
                final int skipped = fs.nodeOf(doc) - fs.nodeOf(prev) - 1;
                if (skipped > 0) {
                    jCounts[0]++;
                    jCounts[3] += skipped;
                }
                prev = doc;
                bit = next + 1;
            }
        }

        @Override
        public int docIDRunEnd() throws IOException {
            return in.docIDRunEnd();
        }
    }

    static final class CountingLeafReader extends FilterLeafReader {
        CountingLeafReader(LeafReader in) {
            super(in);
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            final PointValues pv = in.getPointValues(field);
            return pv == null || field.equals(traced) == false ? pv : new CountingPointValues(pv, fieldStates.get(field));
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            final SortedNumericDocValues dv = in.getSortedNumericDocValues(field);
            final NumericDocValues single = DocValues.unwrapSingleton(dv);
            if (dv == null || field.equals(traced) == false || single == null) return dv;
            final String creator = StackWalker.getInstance()
                .walk(
                    frames -> frames.map(f -> f.getClassName().substring(f.getClassName().lastIndexOf('.') + 1) + "." + f.getMethodName())
                        .filter(
                            n -> n.startsWith("SortTrace") == false
                                && n.startsWith("DocValues.") == false
                                && n.startsWith("FilterLeafReader") == false
                        )
                        .findFirst()
                        .orElse("?")
                );
            final long[] counts = dvReads.computeIfAbsent(creator, c -> new long[2]);
            return DocValues.singleton(new FilterNumericDocValues(single) {
                @Override
                public boolean advanceExact(int target) throws IOException {
                    counts[0]++;
                    return in.advanceExact(target);
                }

                @Override
                public int nextDoc() throws IOException {
                    counts[1]++;
                    return in.nextDoc();
                }

                @Override
                public int advance(int target) throws IOException {
                    counts[1]++;
                    return in.advance(target);
                }
            });
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return null;
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }

    static final class CountingPointValues extends PointValues {
        final PointValues in;
        final FieldState fs;

        CountingPointValues(PointValues in, FieldState fs) {
            this.in = in;
            this.fs = fs;
        }

        @Override
        public PointTree getPointTree() throws IOException {
            // caller: the innermost frame outside this tool; "intersect" when PointValues.intersect asked for the tree
            final String caller = StackWalker.getInstance().walk(frames -> {
                final List<String> names = frames.map(
                    f -> f.getClassName().substring(f.getClassName().lastIndexOf('.') + 1) + "." + f.getMethodName()
                ).filter(n -> n.startsWith("SortTrace") == false).limit(2).toList();
                return names.get(0).equals("PointValues.intersect") ? "intersect<" + names.get(1) : names.get(0);
            });
            final int id = calls.size();
            calls.add(caller);
            estimates.add(0);
            final boolean comparator = caller.contains("NumericComparator");
            if (comparator && caller.startsWith("intersect<")) runCount(curDoc, 3, 1);
            final CountingTree tree = new CountingTree(in.getPointTree(), id, fs);
            tree.comparator = comparator && caller.startsWith("intersect<") == false;
            return tree;
        }

        @Override
        public byte[] getMinPackedValue() throws IOException {
            return in.getMinPackedValue();
        }

        @Override
        public byte[] getMaxPackedValue() throws IOException {
            return in.getMaxPackedValue();
        }

        @Override
        public int getNumDimensions() throws IOException {
            return in.getNumDimensions();
        }

        @Override
        public int getNumIndexDimensions() throws IOException {
            return in.getNumIndexDimensions();
        }

        @Override
        public int getBytesPerDimension() throws IOException {
            return in.getBytesPerDimension();
        }

        @Override
        public long size() {
            return in.size();
        }

        @Override
        public int getDocCount() {
            return in.getDocCount();
        }
    }

    /** Delegates; records every leaf read and, at the root, every bounds read (one per estimate on a cached tree). */
    static final class CountingTree implements PointValues.PointTree {
        final PointValues.PointTree in;
        final int call;
        final FieldState fs;
        int depth;
        boolean comparator; // the comparator's estimate tree: every root bounds read is one update attempt

        CountingTree(PointValues.PointTree in, int call, FieldState fs) {
            this.in = in;
            this.call = call;
            this.fs = fs;
        }

        @Override
        public PointValues.PointTree clone() {
            final CountingTree c = new CountingTree(in.clone(), call, fs);
            c.depth = depth;
            c.comparator = comparator;
            return c;
        }

        @Override
        public boolean moveToChild() throws IOException {
            final boolean moved = in.moveToChild();
            if (moved) depth++;
            return moved;
        }

        @Override
        public boolean moveToSibling() throws IOException {
            return in.moveToSibling();
        }

        @Override
        public boolean moveToParent() throws IOException {
            final boolean moved = in.moveToParent();
            if (moved) depth--;
            return moved;
        }

        @Override
        public byte[] getMinPackedValue() {
            if (depth == 0) {
                estimates.set(call, estimates.get(call) + 1);
                if (comparator) {
                    runCount(curDoc, 1, 1);
                    if (inCallback == 0) runCount(curDoc, 2, 1);
                }
            }
            return in.getMinPackedValue();
        }

        @Override
        public byte[] getMaxPackedValue() {
            return in.getMaxPackedValue();
        }

        @Override
        public long size() {
            return in.size();
        }

        @Override
        public void visitDocIDs(PointValues.IntersectVisitor visitor) throws IOException {
            perLeaf(in.clone(), visitor, false);
        }

        @Override
        public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
            perLeaf(in.clone(), visitor, true);
        }

        @Override
        public void prefetchIntersect(PointValues.IntersectVisitor visitor) throws IOException {
            in.prefetchIntersect(visitor);
        }

        private void perLeaf(PointValues.PointTree t, PointValues.IntersectVisitor visitor, boolean values) throws IOException {
            if (t.moveToChild()) {
                do {
                    perLeaf(t, visitor, values);
                } while (t.moveToSibling());
                t.moveToParent();
                return;
            }
            visits.add(new int[] { call, fs.leafOrd(t), values ? 1 : 0 });
            if (values) {
                t.visitDocValues(visitor);
            } else {
                t.visitDocIDs(visitor);
            }
        }
    }
}
