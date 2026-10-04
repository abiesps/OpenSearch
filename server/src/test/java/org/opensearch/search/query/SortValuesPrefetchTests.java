/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilterNumericDocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapsingTopDocsCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.DocValuesPrefetch;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.ScrollContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.Profilers;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Checks experiment E ({@link SortValuesPrefetch}): every doc reaches the comparator once, in order, in its arrival form
 * (split only at the planner's triggers); every requested node is read later, at most one node ahead; nothing is
 * planned while the data is cached; sorted searches return the same hits; and the wrap happens only when the top-field
 * collector is the only collector of the search.
 */
public class SortValuesPrefetchTests extends OpenSearchTestCase {

    @Override
    public void tearDown() throws Exception {
        SortIoExperiments.resetForTest();
        DocValuesPrefetch.setEnabled(false);
        DocValuesPrefetch.setRunAhead(false);
        DocValuesPrefetch.setRunAheadGate(false);
        DocValuesPrefetch.setRunAheadBypass(false);
        DocValuesPrefetch.setShareLookahead(false);
        DocValuesPrefetch.setLeapfrogLookahead(false);
        super.tearDown();
    }

    /** Nodes of {@code nodeDocs} doc IDs; logs requests and value reads (every collected doc reads its value). */
    private static final class FakeField implements DocValuesPrefetch.Field {
        final int nodeDocs;
        final int maxDoc;
        /** Values of docs below it are cached. */
        int loadedUpTo;
        final List<Integer> requested = new ArrayList<>();
        final Set<Integer> readNodes = new HashSet<>();
        int lastCollected = -1;

        FakeField(int nodeDocs, int maxDoc) {
            this.nodeDocs = nodeDocs;
            this.maxDoc = maxDoc;
        }

        @Override
        public int nextNodeDoc(int doc, long nodeBytes) {
            final long next = ((long) doc / nodeDocs + 1) * nodeDocs;
            return next >= maxDoc ? DocIdSetIterator.NO_MORE_DOCS : (int) next;
        }

        @Override
        public void prefetch(int doc, long nodeBytes) {
            assertTrue("requested doc " + doc + " was collected already", doc > lastCollected);
            int ahead = 0;
            for (int r : requested) {
                if (r > lastCollected) {
                    ahead++;
                }
            }
            // the doc at the trigger is collected right after this request: at most it is still unread
            assertTrue("more than one node requested ahead of collection", ahead <= 1);
            requested.add(doc);
        }

        @Override
        public boolean isLoaded(int doc, long nodeBytes) {
            return doc < loadedUpTo;
        }

        void read(int doc) {
            assertTrue("docs in order: " + doc + " after " + lastCollected, doc > lastCollected);
            lastCollected = doc;
            readNodes.add(doc / nodeDocs);
        }
    }

    /** Matches of a window, backed by a 4,096-bit set (as Lucene's windows are). */
    private static final class WindowStream extends DocIdStream {
        private final FixedBitSet bits;
        private final int base;
        private int upTo;

        WindowStream(FixedBitSet bits, int base) {
            this.bits = bits;
            this.base = base;
            this.upTo = base;
        }

        private int max() {
            return base + bits.length();
        }

        @Override
        public boolean mayHaveRemaining() {
            return upTo < max();
        }

        @Override
        public void forEach(int upTo, CheckedIntConsumer<IOException> consumer) throws IOException {
            upTo = Math.min(upTo, max());
            if (upTo > this.upTo) {
                bits.forEach(this.upTo - base, upTo - base, base, consumer);
                this.upTo = upTo;
            }
        }

        @Override
        public int count(int upTo) {
            upTo = Math.min(upTo, max());
            if (upTo > this.upTo) {
                int c = bits.cardinality(this.upTo - base, upTo - base);
                this.upTo = upTo;
                return c;
            }
            return 0;
        }

        @Override
        public int intoArray(int upTo, int[] array) {
            upTo = Math.min(upTo, max());
            if (upTo > this.upTo) {
                int c = bits.intoArray(this.upTo - base, upTo - base, base, array);
                if (c == array.length) {
                    upTo = array[array.length - 1] + 1;
                }
                this.upTo = upTo;
                return c;
            }
            return 0;
        }
    }

    /** A comparator-like leaf collector: reads each doc's value, never advances the planner itself. */
    private static final class Recorder implements LeafCollector {
        final FakeField field;
        final List<Integer> delivered = new ArrayList<>();
        final List<Integer> perDoc = new ArrayList<>();
        final DocIdSetIterator competitive = DocIdSetIterator.all(1);
        Scorable scorer;
        int finished;
        int expected = -1;
        boolean inBulk;

        Recorder(FakeField field) {
            this.field = field;
        }

        @Override
        public void setScorer(Scorable scorer) {
            this.scorer = scorer;
        }

        @Override
        public void collect(int doc) {
            if (inBulk == false) {
                perDoc.add(doc);
            }
            field.read(doc);
            delivered.add(doc);
        }

        @Override
        public void collect(DocIdStream stream) throws IOException {
            inBulk = true;
            try {
                if (randomBoolean()) {
                    stream.forEach(this::collect);
                } else {
                    int[] buf = new int[randomIntBetween(1, 3000)];
                    for (int n = stream.intoArray(buf); n > 0; n = stream.intoArray(buf)) {
                        for (int i = 0; i < n; i++) {
                            collect(buf[i]);
                        }
                    }
                }
            } finally {
                inBulk = false;
            }
        }

        @Override
        public void collectRange(int min, int max) {
            assertTrue(min < max);
            inBulk = true;
            try {
                for (int doc = min; doc < max; doc++) {
                    collect(doc);
                }
            } finally {
                inBulk = false;
            }
        }

        @Override
        public DocIdSetIterator competitiveIterator() {
            return competitive;
        }

        @Override
        public void finish() {
            finished++;
            if (expected >= 0) {
                assertEquals("finish after every buffered doc was delivered", expected, delivered.size());
            }
        }
    }

    private static FixedBitSet randomMatches(int maxDoc) {
        FixedBitSet m = new FixedBitSet(maxDoc);
        int doc = 0;
        while (doc < maxDoc) {
            int len = randomIntBetween(1, 50_000);
            int end = Math.min(maxDoc, doc + len);
            switch (randomIntBetween(0, 4)) {
                case 0 -> {
                } // gap
                case 1 -> m.set(doc, end); // dense run
                default -> {
                    int every = randomIntBetween(1, 200);
                    for (int d = doc; d < end; d += randomIntBetween(1, every)) {
                        m.set(d);
                    }
                }
            }
            doc = end;
        }
        return m;
    }

    /** Feeds {@code matches} like a bulk scorer: windows, single docs and fully matching ranges, in doc ID order. */
    private static List<Integer> feed(FixedBitSet matches, int maxDoc, LeafCollector in) throws IOException {
        final List<Integer> perDocIn = new ArrayList<>();
        int doc = 0;
        while (doc < maxDoc) {
            int next = matches.nextSetBit(doc);
            if (next == DocIdSetIterator.NO_MORE_DOCS) {
                break;
            }
            switch (randomIntBetween(0, 2)) {
                case 0 -> {
                    int end = Math.min(maxDoc, next + randomIntBetween(1, 4096));
                    FixedBitSet window = new FixedBitSet(4096);
                    for (int d = next; d < end; d++) {
                        if (matches.get(d)) {
                            window.set(d - next);
                        }
                    }
                    in.collect(new WindowStream(window, next));
                    doc = end;
                }
                case 1 -> {
                    perDocIn.add(next);
                    in.collect(next);
                    doc = next + 1;
                }
                default -> {
                    int clear = matches.nextClearBit(next);
                    int end = clear == DocIdSetIterator.NO_MORE_DOCS ? maxDoc : clear;
                    in.collectRange(next, end);
                    doc = end;
                }
            }
        }
        return perDocIn;
    }

    private static void assertDelivered(FixedBitSet matches, List<Integer> delivered) {
        int i = 0;
        for (int d = matches.length() == 0 ? DocIdSetIterator.NO_MORE_DOCS : matches.nextSetBit(0); d != DocIdSetIterator.NO_MORE_DOCS; d =
            d + 1 >= matches.length() ? DocIdSetIterator.NO_MORE_DOCS : matches.nextSetBit(d + 1)) {
            assertTrue("missing doc " + d, i < delivered.size());
            assertEquals(d, (int) delivered.get(i++));
        }
        assertEquals(i, delivered.size());
    }

    public void testDelivery() throws IOException {
        DocValuesPrefetch.resetCounters();
        for (int iter = 0; iter < 20; iter++) {
            final int maxDoc = randomIntBetween(1, 1_500_000);
            final FixedBitSet matches = randomMatches(maxDoc);
            final FakeField field = new FakeField(randomIntBetween(500, 200_000), maxDoc);
            field.loadedUpTo = randomFrom(0, randomIntBetween(0, maxDoc), maxDoc + 1);
            final int lag = randomFrom(SortIoExperiments.MIN_SORT_PREFETCH_DOCS, 1 << 17);
            final DocValuesPrefetch.RunAhead ra = DocValuesPrefetch.sortRunAhead(lag);
            final DocValuesPrefetch.Planner planner = DocValuesPrefetch.sortPlanner(field, ra, DocValuesPrefetch.ALL_MATCHES, 4096);
            final Recorder out = new Recorder(field);
            out.expected = matches.cardinality();
            final LeafCollector in = ra.wrapLeafCollector(out, planner);
            final Scorable scorer = new Scorable() {
                @Override
                public float score() {
                    return 0;
                }
            };
            in.setScorer(scorer);
            assertSame(scorer, out.scorer);
            assertSame(out.competitive, in.competitiveIterator());
            final List<Integer> perDocIn = feed(matches, maxDoc, in);
            in.finish();
            assertEquals(1, out.finished);
            assertDelivered(matches, out.delivered);
            // a single doc stays a single doc; bulk arrivals are split only at triggers
            assertTrue("docs handed over one at a time are collected one at a time", out.perDoc.containsAll(perDocIn));
            int lastNode = -1;
            for (int req : field.requested) {
                assertTrue("requested doc " + req + " is a match", matches.get(req));
                assertTrue("requested node " + req / field.nodeDocs + " is read", field.readNodes.contains(req / field.nodeDocs));
                assertTrue("requested doc " + req + " is not cached", req >= field.loadedUpTo);
                int node = req / field.nodeDocs;
                assertTrue("one request per node, in order", node > lastNode);
                lastNode = node;
            }
            if (field.loadedUpTo > maxDoc) {
                assertTrue("nothing requested while everything is cached", field.requested.isEmpty());
            }
        }
        assertEquals("aggregation counters untouched", 0, DocValuesPrefetch.requests());
        assertEquals(0, DocValuesPrefetch.planners());
    }

    /** N8: the first uncached node is reached inside a range or a stream; the rest of that arrival is buffered. */
    public void testSwitchInsideAnArrival() throws IOException {
        for (boolean range : new boolean[] { true, false }) {
            final int maxDoc = 100_000;
            final FakeField field = new FakeField(1000, maxDoc);
            field.loadedUpTo = 3000;
            final DocValuesPrefetch.RunAhead ra = DocValuesPrefetch.sortRunAhead(SortIoExperiments.MIN_SORT_PREFETCH_DOCS);
            final DocValuesPrefetch.Planner planner = DocValuesPrefetch.sortPlanner(field, ra, DocValuesPrefetch.ALL_MATCHES, 4096);
            final Recorder out = new Recorder(field);
            out.expected = maxDoc;
            final LeafCollector in = ra.wrapLeafCollector(out, planner);
            if (range) {
                in.collectRange(0, maxDoc);
            } else {
                FixedBitSet all = new FixedBitSet(maxDoc);
                all.set(0, maxDoc);
                in.collect(new WindowStream(all, 0));
            }
            // the rest of the arrival waits in the buffer (lag doc IDs behind the scorer)
            assertTrue(out.delivered.size() >= 3000);
            assertTrue(out.delivered.size() < maxDoc);
            in.finish();
            final FixedBitSet expected = new FixedBitSet(maxDoc);
            expected.set(0, maxDoc);
            assertDelivered(expected, out.delivered);
            assertFalse(field.requested.isEmpty());
            assertEquals("the first request is the first doc of the node after the uncached one", 4000, (int) field.requested.get(0));
        }
    }

    /** Sorted searches return the same hits, sort values and totals with and without the wrapper. */
    public void testSameTopHits() throws IOException {
        SortIoExperiments.setSortPrefetchNodeBytes(4096);
        DocValuesPrefetch.resetCounters();
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec());
            iwc.setMaxBufferedDocs(randomIntBetween(5_000, 100_000));
            final int numDocs = randomIntBetween(20_000, 120_000);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                long ts = randomLongBetween(0, 1L << 40);
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    // mostly increasing timestamps with jitter and a few runs out of order, like log data
                    ts += randomIntBetween(0, 1000);
                    final long value = randomInt(50) == 0 ? randomLongBetween(0, 1L << 40) : ts;
                    doc.add(new SortedNumericDocValuesField("ts", value));
                    doc.add(new LongPoint("ts", value));
                    doc.add(new StringField("cat", "c" + randomInt(4), Field.Store.NO));
                    w.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                for (int q = 0; q < 10; q++) {
                    final Sort sort = new Sort(new SortedNumericSortField("ts", SortField.Type.LONG, randomBoolean()));
                    final Query query = randomQuery();
                    final int size = randomFrom(1, 10, 100, 500);
                    final int threshold = randomFrom(1, 1000, 10_000, Integer.MAX_VALUE);
                    FieldDoc after = null;
                    if (randomBoolean()) {
                        TopFieldDocs first = searcher.search(query, new TopFieldCollectorManager(sort, size, null, threshold));
                        if (first.scoreDocs.length > 0) {
                            after = (FieldDoc) first.scoreDocs[first.scoreDocs.length - 1];
                        }
                    }
                    final TopFieldDocs expected = searcher.search(query, new TopFieldCollectorManager(sort, size, after, threshold));
                    final TopFieldCollectorManager manager = new TopFieldCollectorManager(sort, size, after, threshold);
                    final TopFieldDocs actual = searcher.search(query, new CollectorManager<Collector, TopFieldDocs>() {
                        final List<TopFieldCollector> collectors = new ArrayList<>();

                        @Override
                        public Collector newCollector() throws IOException {
                            TopFieldCollector c = manager.newCollector();
                            collectors.add(c);
                            return SortValuesPrefetch.wrap(c, sort);
                        }

                        @Override
                        public TopFieldDocs reduce(Collection<Collector> collected) throws IOException {
                            return manager.reduce(collectors);
                        }
                    });
                    assertSameTopDocs(expected, actual);
                }
            }
        }
        assertTrue("sort planners were created", DocValuesPrefetch.sortPlanners() > 0);
        assertTrue("sort planners requested nodes", DocValuesPrefetch.sortRequests() > 0);
        assertEquals("aggregation counters untouched", 0, DocValuesPrefetch.requests());
        assertEquals(0, DocValuesPrefetch.planners());
    }

    private static Query randomQuery() {
        final long a = randomLongBetween(0, 1L << 40);
        final long b = randomLongBetween(a, a + (1L << 38));
        return switch (randomInt(3)) {
            case 0 -> new MatchAllDocsQuery();
            case 1 -> LongPoint.newRangeQuery("ts", a, b);
            case 2 -> new TermQuery(new Term("cat", "c" + randomInt(4)));
            default -> new BooleanQuery.Builder().add(LongPoint.newRangeQuery("ts", a, Long.MAX_VALUE), BooleanClause.Occur.FILTER)
                .add(new TermQuery(new Term("cat", "c" + randomInt(4))), BooleanClause.Occur.FILTER)
                .build();
        };
    }

    /**
     * Same hits and sort values. Totals: equal when exact; when the threshold was passed (a lower bound) both are lower
     * bounds above it, and the prefetch may count stale extra docs that the comparator rejects (OpenSearch caps both at
     * the threshold).
     */
    private static void assertSameTopDocs(TopFieldDocs expected, TopFieldDocs actual) {
        assertEquals(expected.totalHits.relation(), actual.totalHits.relation());
        if (expected.totalHits.relation() == TotalHits.Relation.EQUAL_TO) {
            assertEquals(expected.totalHits.value(), actual.totalHits.value());
        } else {
            assertTrue(actual.totalHits.value() >= expected.totalHits.value());
        }
        assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
        for (int i = 0; i < expected.scoreDocs.length; i++) {
            assertEquals(expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
            assertArrayEquals(((FieldDoc) expected.scoreDocs[i]).fields, ((FieldDoc) actual.scoreDocs[i]).fields);
        }
    }

    /** A collector whose leaf collector is a fixed instance, to check that a leaf is left unwrapped. */
    private static final class FixedLeaf implements Collector {
        final LeafCollector leaf = new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {}

            @Override
            public void collect(int doc) {}
        };

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) {
            return leaf;
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    /** Leaves that cannot be planned keep the comparator's leaf collector unchanged. */
    public void testLeafEligibility() throws IOException {
        final Sort sort = new Sort(new SortedNumericSortField("ts", SortField.Type.LONG));
        final int numDocs = 5000;
        // index sort on the field: unwrapped; otherwise wrapped
        for (boolean indexSorted : new boolean[] { false, true }) {
            try (Directory dir = newDirectory()) {
                final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec());
                if (indexSorted) {
                    iwc.setIndexSort(sort);
                }
                try (IndexWriter w = new IndexWriter(dir, iwc)) {
                    for (int i = 0; i < numDocs; i++) {
                        Document doc = new Document();
                        doc.add(new SortedNumericDocValuesField("ts", i));
                        doc.add(new SortedNumericDocValuesField("multi", i));
                        doc.add(new SortedNumericDocValuesField("multi", i + 1));
                        doc.add(new SortedDocValuesField("keyword", new org.apache.lucene.util.BytesRef("k" + i)));
                        doc.add(new NumericDocValuesField("num", i));
                        w.addDocument(doc);
                    }
                    w.forceMerge(1);
                }
                try (IndexReader reader = DirectoryReader.open(dir)) {
                    final LeafReaderContext ctx = reader.leaves().get(0);
                    final FixedLeaf delegate = new FixedLeaf();
                    final LeafCollector leaf = SortValuesPrefetch.wrap(delegate, sort).getLeafCollector(ctx);
                    if (indexSorted) {
                        assertSame("index-sorted leaf", delegate.leaf, leaf);
                    } else {
                        assertNotSame("planned leaf", delegate.leaf, leaf);
                    }
                    final Sort num = new Sort(new SortField("num", SortField.Type.LONG));
                    assertNotSame("numeric field", delegate.leaf, SortValuesPrefetch.wrap(delegate, num).getLeafCollector(ctx));
                    for (String unplanned : new String[] { "multi", "keyword", "missing" }) {
                        final Sort s = new Sort(new SortedNumericSortField(unplanned, SortField.Type.LONG));
                        assertSame(unplanned, delegate.leaf, SortValuesPrefetch.wrap(delegate, s).getLeafCollector(ctx));
                    }
                }
                if (indexSorted == false) {
                    // a reader whose values do not support node planning
                    try (IndexReader reader = new NoPlanningReader(DirectoryReader.open(dir))) {
                        final FixedLeaf delegate = new FixedLeaf();
                        assertSame(delegate.leaf, SortValuesPrefetch.wrap(delegate, sort).getLeafCollector(reader.leaves().get(0)));
                    }
                }
            }
        }
    }

    /** Hides node planning of the doc values. */
    private static final class NoPlanningReader extends FilterDirectoryReader {
        NoPlanningReader(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new FilterLeafReader(reader) {
                        @Override
                        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
                            final SortedNumericDocValues values = super.getSortedNumericDocValues(field);
                            final NumericDocValues single = values == null
                                ? null
                                : org.apache.lucene.index.DocValues.unwrapSingleton(values);
                            if (single == null) {
                                return values;
                            }
                            return org.apache.lucene.index.DocValues.singleton(new FilterNumericDocValues(single) {
                                @Override
                                public int nextPrefetchNodeDoc(int doc, long nodeBytes) {
                                    return -1;
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
                    };
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new NoPlanningReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }

    private SearchContext eligibleContext(IndexReader reader) {
        final SearchContext context = mock(SearchContext.class);
        final ContextIndexSearcher searcher = mock(ContextIndexSearcher.class);
        when(searcher.getIndexReader()).thenReturn(reader);
        when(context.searcher()).thenReturn(searcher);
        when(context.query()).thenReturn(new MatchAllDocsQuery());
        when(context.size()).thenReturn(10);
        when(context.from()).thenReturn(0);
        when(context.trackTotalHitsUpTo()).thenReturn(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);
        when(context.sort()).thenReturn(
            new SortAndFormats(
                new Sort(new SortedNumericSortField("ts", SortField.Type.LONG, true)),
                new DocValueFormat[] { DocValueFormat.RAW }
            )
        );
        return context;
    }

    private static boolean wrapped(SearchContext context, boolean hasFilterCollector) throws IOException {
        return TopDocsCollectorContext.createTopDocsCollectorContext(context, hasFilterCollector)
            .create(null) instanceof SortValuesPrefetch;
    }

    /** The wrap happens only when the top-field collector would be the only collector of a non-concurrent search. */
    public void testSearchEligibility() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("ts", 1));
                w.addDocument(doc);
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                SortIoExperiments.setSortPrefetch(true);
                assertTrue("eligible", wrapped(eligibleContext(reader), false));

                // decoupling (H4): every aggregation switch on, sort switch off -> no wrap
                SortIoExperiments.setSortPrefetch(false);
                DocValuesPrefetch.setEnabled(true);
                DocValuesPrefetch.setRunAhead(true);
                DocValuesPrefetch.setRunAheadGate(true);
                DocValuesPrefetch.setRunAheadBypass(true);
                DocValuesPrefetch.setShareLookahead(true);
                DocValuesPrefetch.setLeapfrogLookahead(true);
                assertFalse("sort_prefetch off", wrapped(eligibleContext(reader), false));
                DocValuesPrefetch.setEnabled(false);
                DocValuesPrefetch.setRunAhead(false);
                SortIoExperiments.setSortPrefetch(true);

                // post_filter, min_score and terminate_after are filter collectors
                assertFalse("filter collector", wrapped(eligibleContext(reader), true));

                SearchContext c = eligibleContext(reader);
                final SearchContextAggregations aggs = mock(SearchContextAggregations.class);
                when(c.aggregations()).thenReturn(aggs);
                assertFalse("aggregations", wrapped(c, false));

                c = eligibleContext(reader);
                final Profilers profilers = new Profilers(mock(ContextIndexSearcher.class), false);
                when(c.getProfilers()).thenReturn(profilers);
                assertFalse("profile", wrapped(c, false));

                c = eligibleContext(reader);
                when(c.trackScores()).thenReturn(true);
                assertFalse("track_scores", wrapped(c, false));

                c = eligibleContext(reader);
                when(c.shouldUseConcurrentSearch()).thenReturn(true);
                assertFalse("concurrent search", wrapped(c, false));

                c = eligibleContext(reader);
                @SuppressWarnings("unchecked")
                final CollectorManager<? extends Collector, ReduceableSearchResult> plugin = mock(CollectorManager.class);
                final Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> managers = Map.of(
                    SortValuesPrefetchTests.class,
                    plugin
                );
                when(c.queryCollectorManagers()).thenReturn(managers);
                assertFalse("plugin query collector manager", wrapped(c, false));

                c = eligibleContext(reader);
                when(c.sort()).thenReturn(
                    new SortAndFormats(
                        new Sort(new SortedNumericSortField("ts", SortField.Type.LONG), new SortField("other", SortField.Type.LONG)),
                        new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW }
                    )
                );
                assertFalse("two sort fields", wrapped(c, false));

                c = eligibleContext(reader);
                when(c.sort()).thenReturn(
                    new SortAndFormats(
                        new Sort(new SortedNumericSortField("ts", SortField.Type.INT)),
                        new DocValueFormat[] { DocValueFormat.RAW }
                    )
                );
                assertFalse("int sort", wrapped(c, false));

                c = eligibleContext(reader);
                when(c.sort()).thenReturn(null);
                assertFalse("score sort", wrapped(c, false));

                c = eligibleContext(reader);
                when(c.sort()).thenReturn(
                    new SortAndFormats(new Sort(new SortField("ts", SortField.Type.LONG)), new DocValueFormat[] { DocValueFormat.RAW })
                );
                assertTrue("long SortField", wrapped(c, false));

                c = eligibleContext(reader);
                when(c.scrollContext()).thenReturn(new ScrollContext());
                assertFalse("scroll", wrapped(c, false));

                c = eligibleContext(reader);
                final CollapseContext collapse = mock(CollapseContext.class);
                when(collapse.createTopDocs(any(), anyInt(), any())).thenAnswer(
                    inv -> CollapsingTopDocsCollector.createNumeric(
                        "ts",
                        mock(MappedFieldType.class),
                        inv.getArgument(0),
                        inv.getArgument(1)
                    )
                );
                when(c.collapse()).thenReturn(collapse);
                assertFalse("collapse", wrapped(c, false));
            }
        }
    }

    /** Counters (N2): aggregation planners count only in the aggregation counters. */
    public void testAggregationPlannerCounters() throws IOException {
        DocValuesPrefetch.resetCounters();
        DocValuesPrefetch.setEnabled(true);
        final FakeField field = new FakeField(1000, 10_000);
        final DocValuesPrefetch.Matches all = target -> target;
        assertNotNull(DocValuesPrefetch.planner(field, all, DocValuesPrefetch.ALL_MATCHES));
        assertEquals(1, DocValuesPrefetch.planners());
        assertEquals(1, DocValuesPrefetch.requests());
        assertEquals(0, DocValuesPrefetch.sortPlanners());
        assertEquals(0, DocValuesPrefetch.sortRequests());
        assertEquals(Arrays.asList(0), field.requested);
    }
}
