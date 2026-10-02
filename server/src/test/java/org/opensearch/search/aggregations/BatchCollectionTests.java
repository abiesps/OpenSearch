/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 * Aggregation results must not depend on {@link BatchCollection}: the same searches run with it off and on.
 */
public class BatchCollectionTests extends AggregatorTestCase {

    private static final String TS = "ts";
    private static final String V = "v";
    private static final String TAG = "tag";
    private static final String SVC = "svc";

    private final MappedFieldType tsType = new DateFieldMapper.DateFieldType(TS);
    private final MappedFieldType vType = new NumberFieldMapper.NumberFieldType(V, NumberFieldMapper.NumberType.LONG);
    private final MappedFieldType svcType = new KeywordFieldMapper.KeywordFieldType(SVC);

    /** Whether {@link #run} turns doc-values prefetch on together with batch collection. */
    private boolean prefetch;
    /** Makes {@link #run} use the run-ahead buffer. */
    private boolean forceRunAhead;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        prefetch = randomBoolean();
    }

    @Override
    public void tearDown() throws Exception {
        BatchCollection.setEnabled(false);
        DocValuesPrefetch.setEnabled(false);
        DocValuesPrefetch.setShareLookahead(false);
        DocValuesPrefetch.setLeapfrogLookahead(false);
        DocValuesPrefetch.setRunAhead(false);
        DocValuesPrefetch.setRunAheadGate(false);
        super.tearDown();
    }

    /**
     * One segment: ts increases with doc ID (indexed with a skipper, so date_histogram uses its skip-list collector),
     * v is present in all docs or in most of them, and two dense tags, so a conjunction of the tags runs with Lucene's
     * dense conjunction scorer and collects 4,096-doc windows as DocIdStreams.
     */
    private Directory index(int numDocs, boolean allHaveV, long maxV) throws IOException {
        return index(numDocs, allHaveV, maxV, 1);
    }

    /**
     * With {@code runs > 1}, doc IDs hold {@code runs} time runs in a shuffled order (as segment merges leave a log
     * index): ts increases within a run and jumps between runs, so some skipper intervals span two runs.
     */
    private Directory index(int numDocs, boolean allHaveV, long maxV, int runs) throws IOException {
        Directory dir = newDirectory();
        // the default codec, so doc values use Lucene90 (node planning and bulk reads)
        IndexWriterConfig config = newIndexWriterConfig().setCodec(TestUtil.getDefaultCodec())
            // merges adjacent segments only, so the merged segment keeps ts in doc ID order (a random merge policy can
            // merge segments out of order, and then no skipper interval falls in one bucket)
            .setMergePolicy(newLogMergePolicy());
        try (IndexWriter w = new IndexWriter(dir, config)) {
            long ts = 1_700_000_000_000L;
            final List<Integer> shuffled = new ArrayList<>();
            for (int r = 0; r < runs; r++) {
                shuffled.add(r);
            }
            Collections.shuffle(shuffled, random());
            final int runDocs = (numDocs + runs - 1) / runs;
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                if (runs > 1 && i % runDocs == 0) {
                    // each run covers its own stretch of time, about 100 ms per doc
                    ts = 1_700_000_000_000L + (long) shuffled.get(i / runDocs) * runDocs * 120L;
                }
                // about 100 ms apart: a 4,096-doc skipper interval spans about 7 minutes
                ts += randomIntBetween(0, 200);
                doc.add(SortedNumericDocValuesField.indexedField(TS, ts));
                if (allHaveV || randomIntBetween(0, 9) > 0) {
                    doc.add(new SortedNumericDocValuesField(V, randomLongBetween(-maxV, maxV)));
                }
                // 50 services, skewed like the benchmark corpus
                int svc = Math.min(49, (int) Math.floor(Math.pow(50, randomDouble())) - 1);
                doc.add(new SortedSetDocValuesField(SVC, new BytesRef("svc" + svc)));
                if (randomIntBetween(0, 9) < 6) {
                    doc.add(new StringField(TAG, "a", Field.Store.NO));
                }
                if (randomIntBetween(0, 9) < 7) {
                    doc.add(new StringField(TAG, "b", Field.Store.NO));
                }
                w.addDocument(doc);
            }
            w.forceMerge(1);
        }
        return dir;
    }

    private static Query denseConjunction() {
        return new BooleanQuery.Builder().add(new TermQuery(new Term(TAG, "a")), BooleanClause.Occur.FILTER)
            .add(new TermQuery(new Term(TAG, "b")), BooleanClause.Occur.FILTER)
            .build();
    }

    private <A extends InternalAggregation> A run(IndexSearcher searcher, Query query, AggregationBuilder agg, boolean batch)
        throws IOException {
        BatchCollection.setEnabled(batch);
        DocValuesPrefetch.setEnabled(batch && prefetch);
        DocValuesPrefetch.setNodeBytes(1L << randomIntBetween(10, 17));
        DocValuesPrefetch.setShareLookahead(randomBoolean());
        DocValuesPrefetch.setLeapfrogLookahead(randomBoolean());
        DocValuesPrefetch.setRunAhead(forceRunAhead || randomBoolean());
        DocValuesPrefetch.setRunAheadGate(randomBoolean());
        DocValuesPrefetch.setRunAheadDocs(randomFrom(4096, 8192, 65_536, 1 << 17));
        try {
            return searchAndReduce(searcher, query, agg, false, tsType, vType, svcType);
        } finally {
            BatchCollection.setEnabled(false);
            DocValuesPrefetch.setEnabled(false);
        }
    }

    private static List<Object> histogramResult(InternalDateHistogram histogram) {
        List<Object> out = new ArrayList<>();
        for (Histogram.Bucket b : histogram.getBuckets()) {
            InternalAvg avg = b.getAggregations().get("avg");
            out.add(List.of(b.getKey(), b.getDocCount(), avg.getValue()));
        }
        return out;
    }

    private static List<Object> termsResult(StringTerms terms) {
        List<Object> out = new ArrayList<>();
        for (Terms.Bucket b : terms.getBuckets()) {
            InternalAvg avg = b.getAggregations().get("avg");
            out.add(List.of(b.getKeyAsString(), b.getDocCount(), avg.getValue()));
        }
        return out;
    }

    private void assertSameResults(boolean allHaveV, long maxV) throws IOException {
        // 40 to 100 minutes of docs: at least two 30m buckets
        int numDocs = randomIntBetween(24_000, 60_000);
        try (Directory dir = index(numDocs, allHaveV, maxV); IndexReader reader = DirectoryReader.open(dir)) {
            // a plain searcher without a query cache, so the conjunction runs with Lucene's dense conjunction scorer
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setQueryCache(null);
            for (Query query : new Query[] { denseConjunction(), new MatchAllDocsQuery() }) {
                AvgAggregationBuilder avg = new AvgAggregationBuilder("avg").field(V);
                InternalAvg stockAvg = run(searcher, query, avg, false);
                BatchCollection.resetCounters();
                DocValuesPrefetch.resetCounters();
                InternalAvg batchAvg = run(searcher, query, avg, true);
                assertPrefetched(allHaveV);
                assertEquals(query + " avg", stockAvg.getValue(), batchAvg.getValue(), 0d);
                if (query instanceof BooleanQuery && allHaveV) {
                    // the dense conjunction collects DocIdStreams, so the top-level avg reads them in bulk
                    assertTrue("bulk chunks read", BatchCollection.bulkChunks() > 0);
                }

                String interval = randomFrom("1m", "10m", "30m");
                DateHistogramAggregationBuilder histogram = new DateHistogramAggregationBuilder("h").field(TS)
                    .fixedInterval(new DateHistogramInterval(interval))
                    .subAggregation(new AvgAggregationBuilder("avg").field(V));
                InternalDateHistogram stock = run(searcher, query, histogram, false);
                BatchCollection.resetCounters();
                DocValuesPrefetch.resetCounters();
                InternalDateHistogram batch = run(searcher, query, histogram, true);
                assertPrefetched(true);
                assertTrue(query + " histogram has buckets", stock.getBuckets().size() > 1);
                assertTrue(query + " histogram", Objects.equals(histogramResult(stock), histogramResult(batch)));
                if (query instanceof BooleanQuery && interval.equals("30m")) {
                    // most 4,096-doc intervals fit in one 30m bucket: those runs go to the avg sub-aggregation as streams
                    assertTrue("stream runs", BatchCollection.streamRuns() > 0);
                }

                for (Aggregator.SubAggCollectionMode mode : Aggregator.SubAggCollectionMode.values()) {
                    TermsAggregationBuilder terms = new TermsAggregationBuilder("t").field(SVC)
                        .size(randomIntBetween(3, 60))
                        .collectMode(mode)
                        .subAggregation(new AvgAggregationBuilder("avg").field(V));
                    StringTerms stockTerms = run(searcher, query, terms, false);
                    BatchCollection.resetCounters();
                    DocValuesPrefetch.resetCounters();
                    StringTerms batchTerms = run(searcher, query, terms, true);
                    assertPrefetched(true);
                    assertTrue(query + " " + mode + " terms has buckets", stockTerms.getBuckets().size() > 1);
                    assertTrue(query + " " + mode + " terms", Objects.equals(termsResult(stockTerms), termsResult(batchTerms)));
                    if (query instanceof BooleanQuery) {
                        // ordinals read in bulk per DocIdStream chunk
                        assertTrue(mode + " bulk ordinal chunks", BatchCollection.bulkChunks() > 0);
                    }
                }
            }
        }
    }

    /** With prefetch on, planners were created and requested nodes; without it, none. */
    private void assertPrefetched(boolean expectPlanners) {
        if (prefetch && expectPlanners) {
            assertTrue("planners", DocValuesPrefetch.planners() > 0);
            assertTrue("requests", DocValuesPrefetch.requests() > 0);
            if (DocValuesPrefetch.isRunAhead()) {
                // planners took their matches from the run-ahead buffer (or the replay ring), not a second scorer
                assertTrue("run-ahead used", DocValuesPrefetch.runAheadLeaves() + DocValuesPrefetch.runAheadReplays() > 0);
                assertEquals("no look-ahead scorer", 0, DocValuesPrefetch.lookaheads());
            }
        } else if (prefetch == false) {
            assertEquals(0, DocValuesPrefetch.planners());
        }
    }

    /**
     * Shuffled time runs (the benchmark corpus layout): date_histogram with and without a sub-aggregation, with the
     * run-ahead buffer, against stock.
     */
    public void testShuffledTimeRuns() throws IOException {
        prefetch = true;
        int numDocs = randomIntBetween(150_000, 400_000);
        try (Directory dir = index(numDocs, true, 100_000, randomIntBetween(2, 7)); IndexReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setQueryCache(null);
            for (Query query : new Query[] { denseConjunction(), new TermQuery(new Term(TAG, "a")), new MatchAllDocsQuery() }) {
                for (boolean withSub : new boolean[] { false, true }) {
                    DateHistogramAggregationBuilder histogram = new DateHistogramAggregationBuilder("h").field(TS)
                        .fixedInterval(new DateHistogramInterval(randomFrom("1m", "10m", "30m")));
                    if (withSub) {
                        histogram.subAggregation(new AvgAggregationBuilder("avg").field(V));
                    }
                    InternalDateHistogram stock = run(searcher, query, histogram, false);
                    DocValuesPrefetch.resetCounters();
                    forceRunAhead = true;
                    InternalDateHistogram batch;
                    try {
                        batch = run(searcher, query, histogram, true);
                    } finally {
                        forceRunAhead = false;
                    }
                    assertTrue("run-ahead used", DocValuesPrefetch.runAheadLeaves() > 0);
                    assertEquals(query + " sub=" + withSub, countsResult(stock), countsResult(batch));
                }
            }
        }
    }

    /**
     * date_histogram's skip-list collector must not trust {@link DocIdStream#mayHaveRemaining()}: Lucene's window streams
     * are backed by 4,096-bit sets and report remaining docs up to the end of the bit set, past the end of a shorter
     * window, and the next window may start there. Checked against the same histogram without the skip list
     * (hard bounds turn it off), with stock collection and with batch collection plus run-ahead.
     */
    public void testPaddedWindowStreams() throws IOException {
        int numDocs = randomIntBetween(150_000, 300_000);
        try (Directory dir = index(numDocs, true, 100_000, randomIntBetween(2, 7)); IndexReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setQueryCache(null);
            for (Query inner : new Query[] { new MatchAllDocsQuery(), new TermQuery(new Term(TAG, "a")) }) {
                Query query = new PaddedWindowsQuery(inner, randomLong());
                String interval = randomFrom("1m", "10m", "30m");
                DateHistogramAggregationBuilder skipList = new DateHistogramAggregationBuilder("h").field(TS)
                    .fixedInterval(new DateHistogramInterval(interval));
                DateHistogramAggregationBuilder noSkipList = new DateHistogramAggregationBuilder("h").field(TS)
                    .fixedInterval(new DateHistogramInterval(interval))
                    .hardBounds(new LongBounds(0L, Long.MAX_VALUE));
                prefetch = false;
                List<Object> expected = countsResult(run(searcher, query, noSkipList, false));
                assertEquals(inner + " stock", expected, countsResult(run(searcher, query, skipList, false)));
                assertEquals(inner + " batch", expected, countsResult(run(searcher, query, skipList, true)));
                prefetch = true;
                forceRunAhead = true;
                try {
                    assertEquals(inner + " run-ahead", expected, countsResult(run(searcher, query, skipList, true)));
                } finally {
                    forceRunAhead = false;
                }
            }
        }
    }

    /** Collects the matches of {@code in} as window streams of random length whose bit sets are 4,096 bits long. */
    private static final class PaddedWindowsQuery extends Query {
        private final Query in;
        private final long seed;

        PaddedWindowsQuery(Query in, long seed) {
            this.in = in;
            this.seed = seed;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            Weight weight = in.createWeight(searcher, scoreMode, boost);
            return new FilterWeight(this, weight) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext ctx) throws IOException {
                    ScorerSupplier supplier = weight.scorerSupplier(ctx);
                    if (supplier == null) {
                        return null;
                    }
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            return supplier.get(leadCost);
                        }

                        @Override
                        public BulkScorer bulkScorer() throws IOException {
                            return padded(supplier.get(Long.MAX_VALUE).iterator(), ctx.reader().maxDoc(), new Random(seed));
                        }

                        @Override
                        public long cost() {
                            return supplier.cost();
                        }
                    };
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false;
                }
            };
        }

        private static BulkScorer padded(DocIdSetIterator it, int maxDoc, Random random) {
            return new BulkScorer() {
                @Override
                public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                    collector.setScorer(new Scorable() {
                        @Override
                        public float score() {
                            return 1f;
                        }
                    });
                    max = Math.min(max, maxDoc);
                    int doc = it.docID() < min ? it.advance(min) : it.docID();
                    FixedBitSet bits = new FixedBitSet(4096);
                    while (doc < max) {
                        int base = doc;
                        int end = Math.min(max, base + 1 + random.nextInt(4096));
                        bits.clear();
                        for (; doc < end; doc = it.nextDoc()) {
                            if (acceptDocs == null || acceptDocs.get(doc)) {
                                bits.set(doc - base);
                            }
                        }
                        collector.collect(new PaddedStream(bits, base));
                    }
                    return doc;
                }

                @Override
                public long cost() {
                    return it.cost();
                }
            };
        }

        @Override
        public String toString(String field) {
            return "padded(" + in.toString(field) + ")";
        }

        @Override
        public void visit(QueryVisitor visitor) {
            in.visit(visitor);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof PaddedWindowsQuery p && p.in.equals(in) && p.seed == seed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(in, seed);
        }
    }

    /** A window's matches over a 4,096-bit set, reporting remaining docs up to the end of the bit set. */
    private static final class PaddedStream extends DocIdStream {
        private final FixedBitSet bits;
        private final int base;
        private int upTo;

        PaddedStream(FixedBitSet bits, int base) {
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

    private static List<Object> countsResult(InternalDateHistogram histogram) {
        List<Object> out = new ArrayList<>();
        for (Histogram.Bucket b : histogram.getBuckets()) {
            InternalAvg avg = b.getAggregations().get("avg");
            out.add(List.of(b.getKey(), b.getDocCount(), avg == null ? "" : avg.getValue()));
        }
        return out;
    }

    public void testAllDocsHaveValues() throws IOException {
        assertSameResults(true, 100_000);
    }

    public void testSomeDocsMissValues() throws IOException {
        assertSameResults(false, 100_000);
    }

    /** Values too large for an exact chunk sum fall back to adding each value; results must still match. */
    public void testLargeValues() throws IOException {
        assertSameResults(true, 1L << 50);
    }
}
