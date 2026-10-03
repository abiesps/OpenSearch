
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
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;

/**
 * Runs bench_aggs.py sort queries (sort_ORDER:SEL:WINDOW[:SIZE][:nt], SEL s50/s10/s1/all; not "bare" and not
 * all:all, which OpenSearch answers with its approximation framework) directly in Lucene on the benchmark index, built
 * the way OpenSearch builds them (ConstantScore(bool filter [term sel, IndexOrDocValuesQuery(LongPoint range, doc-values
 * range)]), SortedNumericSortField on @timestamp with the MAX selector for desc and MIN for asc, TopFieldCollectorManager
 * with a total hits threshold of 10,000, or 1 for track_total_hits false, each leaf scored in OpenSearch's
 * CancellableBulkScorer chunks), and reports how the points of @timestamp are
 * used: every getPointTree call with its caller (the sort comparator's competitive iterator or the range query), which
 * of them are intersections, the estimates the comparator makes before deciding to intersect, and every leaf each
 * intersection reads, as visitDocIDs (the cell is inside the range: doc IDs only) or visitDocValues (the cell crosses:
 * doc IDs and values). Also the leaves that hold the returned hits and how many docs reached the collector.
 * Output: JSON lines, one per query. Opens the index read-only.
 *
 * <p>Run (Java 21+): java -cp 'DISTRO/lib/*' validate/SortTrace.java SHARD_INDEX_DIR SPEC[,SPEC...] [OUT.jsonl]
 */
public class SortTrace {
    static final long START_MS = 1788220800000L; // bench_aggs.START_MS
    static final long DAY = 86_400_000L;
    static final String FIELD = "@timestamp";

    // per query state
    static final List<String> calls = new ArrayList<>(); // getPointTree callers, by call id
    static final List<int[]> visits = new ArrayList<>(); // {call id, leaf ordinal, 0 = doc IDs (inside) / 1 = values (crosses)}
    static final List<Integer> estimates = new ArrayList<>(); // per call id: estimates started at the root of that tree
    static final TreeMap<String, long[]> dvReads = new TreeMap<>(); // @timestamp doc values: creator -> {advanceExact, nextDoc+advance}
    static long[] leafFps;
    static Method fpMethod;

    public static void main(String[] args) throws Exception {
        final PrintStream out = args.length > 2 ? new PrintStream(args[2]) : System.out;
        try (Directory d = new NIOFSDirectory(Path.of(args[0])); DirectoryReader raw = DirectoryReader.open(d)) {
            final LeafReader leaf = raw.leaves().get(0).reader();
            final int[] docLeaf = new int[leaf.maxDoc()];
            indexLeaves(leaf.getPointValues(FIELD), docLeaf);
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
            for (String spec : args[1].split(",")) {
                for (int rep = 0; rep < 2; rep++) { // the first run warms the JIT; report the second (same result)
                    calls.clear();
                    visits.clear();
                    estimates.clear();
                    dvReads.clear();
                    final String[] p = spec.split(":");
                    final boolean desc = p[0].equals("sort_desc");
                    final String sel = p[1], window = p[2];
                    int size = 10;
                    int threshold = 10_000;
                    for (int i = 3; i < p.length; i++) {
                        if (p[i].equals("nt")) threshold = 1;
                        else size = Integer.parseInt(p[i]);
                    }
                    final Query q = query(sel, window);
                    final Sort sort = new Sort(
                        new SortedNumericSortField(
                            FIELD,
                            SortField.Type.LONG,
                            desc,
                            desc ? SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN
                        )
                    );
                    final long t0 = System.nanoTime();
                    final TopFieldDocs top = searcher.search(q, new TopFieldCollectorManager(sort, size, null, threshold));
                    final long nanos = System.nanoTime() - t0;
                    if (rep == 0) continue;
                    print(out, spec, q, top, nanos, docLeaf);
                }
            }
        }
    }

    static Query query(String sel, String window) {
        Query range = null;
        if (window.equals("all") == false) {
            final long lo = START_MS + (window.equals("7d") ? 0 : 3 * DAY);
            final long hi = START_MS + (window.equals("7d") ? 7 * DAY : 4 * DAY) - 1; // "lt"
            range = new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery(FIELD, lo, hi),
                SortedNumericDocValuesField.newSlowRangeQuery(FIELD, lo, hi)
            );
        }
        final BooleanQuery.Builder b = new BooleanQuery.Builder();
        if (sel.equals("all") == false) b.add(new TermQuery(new Term("sel", sel)), BooleanClause.Occur.FILTER);
        if (range != null) b.add(range, BooleanClause.Occur.FILTER);
        if (sel.equals("all") && range == null) return new MatchAllDocsQuery();
        return new ConstantScoreQuery(b.build());
    }

    static void indexLeaves(PointValues pv, int[] docLeaf) throws Exception {
        final PointValues.PointTree tree = pv.getPointTree();
        fpMethod = tree.getClass().getDeclaredMethod("getLeafBlockFP");
        fpMethod.setAccessible(true);
        final List<Long> fps = new ArrayList<>();
        walk(tree, fps, docLeaf);
        leafFps = fps.stream().mapToLong(Long::longValue).toArray();
    }

    static void walk(PointValues.PointTree tree, List<Long> fps, int[] docLeaf) throws Exception {
        if (tree.moveToChild()) {
            do {
                walk(tree, fps, docLeaf);
            } while (tree.moveToSibling());
            tree.moveToParent();
            return;
        }
        final int ord = fps.size();
        fps.add((long) fpMethod.invoke(tree));
        tree.visitDocIDs(new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {
                docLeaf[docID] = ord;
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                docLeaf[docID] = ord;
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                return PointValues.Relation.CELL_INSIDE_QUERY;
            }
        });
    }

    static int leafOrd(PointValues.PointTree leafTree) throws IOException {
        try {
            final long fp = (long) fpMethod.invoke(leafTree);
            final int i = Arrays.binarySearch(leafFps, fp);
            if (i < 0) throw new IllegalStateException("unknown leaf fp " + fp);
            return i;
        } catch (ReflectiveOperationException e) {
            throw new IOException(e);
        }
    }

    static void print(PrintStream out, String spec, Query q, TopFieldDocs top, long nanos, int[] docLeaf) {
        final StringBuilder sb = new StringBuilder();
        sb.append(
            String.format(
                Locale.ROOT,
                "{\"query\": \"%s\", \"lucene_query\": \"%s\", \"millis\": %.1f, \"collected\": %d, \"relation\": \"%s\"",
                spec,
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
                leafCollector = collector.getLeafCollector(ctx);
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

    static final class CountingLeafReader extends FilterLeafReader {
        CountingLeafReader(LeafReader in) {
            super(in);
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            final PointValues pv = in.getPointValues(field);
            return pv == null || field.equals(FIELD) == false ? pv : new CountingPointValues(pv);
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            final SortedNumericDocValues dv = in.getSortedNumericDocValues(field);
            final NumericDocValues single = DocValues.unwrapSingleton(dv);
            if (dv == null || field.equals(FIELD) == false || single == null) return dv;
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

        CountingPointValues(PointValues in) {
            this.in = in;
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
            return new CountingTree(in.getPointTree(), id);
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
        int depth;

        CountingTree(PointValues.PointTree in, int call) {
            this.in = in;
            this.call = call;
        }

        @Override
        public PointValues.PointTree clone() {
            final CountingTree c = new CountingTree(in.clone(), call);
            c.depth = depth;
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
            if (depth == 0) estimates.set(call, estimates.get(call) + 1);
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

        private void perLeaf(PointValues.PointTree t, PointValues.IntersectVisitor visitor, boolean values) throws IOException {
            if (t.moveToChild()) {
                do {
                    perLeaf(t, visitor, values);
                } while (t.moveToSibling());
                t.moveToParent();
                return;
            }
            visits.add(new int[] { call, leafOrd(t), values ? 1 : 0 });
            if (values) {
                t.visitDocValues(visitor);
            } else {
                t.visitDocIDs(visitor);
            }
        }
    }
}
