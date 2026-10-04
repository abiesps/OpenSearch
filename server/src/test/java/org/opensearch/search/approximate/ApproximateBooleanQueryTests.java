/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene90.Lucene90PointsReader;
import org.apache.lucene.codecs.lucene90.Lucene90PointsWriter;
import org.apache.lucene.codecs.lucene90.Lucene90SplitPointsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPointsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.bkd.BKDWriter;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.SortIoExperiments;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.lucene.document.LongPoint.pack;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * D-a and D-b: approximation of a {@code bool} with a range on the sort field. Every case runs on the stock points
 * field {@link #TS} and on its twin {@link #TS_SPLIT} written in the split points format, and compares the hits (doc
 * IDs, sort values) and the total hits with the plain {@code bool} run on a stock searcher.
 */
public class ApproximateBooleanQueryTests extends OpenSearchTestCase {

    static final String TS = "ts";
    static final String TS_SPLIT = "ts_split";
    static final String SEL = "sel";
    static final String[] FIELDS = { TS, TS_SPLIT };

    @After
    public void resetSwitches() {
        SortIoExperiments.resetForTest();
    }

    /** The stock format with another number of points per leaf. */
    private static final class StockPointsFormat extends PointsFormat {
        private final int maxPointsInLeafNode;

        StockPointsFormat(int maxPointsInLeafNode) {
            this.maxPointsInLeafNode = maxPointsInLeafNode;
        }

        @Override
        public PointsWriter fieldsWriter(SegmentWriteState writeState) throws IOException {
            return new Lucene90PointsWriter(writeState, maxPointsInLeafNode, BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP);
        }

        @Override
        public PointsReader fieldsReader(SegmentReadState readState) throws IOException {
            return new Lucene90PointsReader(readState);
        }
    }

    /**
     * A codec named like the fork's {@code Lucene104SplitPointsCodec} (so segments read back through SPI by the per-field
     * attributes) that writes {@link #TS_SPLIT} in the split format and every other field in the stock format, both
     * with {@code leafSize} points per leaf.
     */
    static FilterCodec twinCodec(int leafSize) {
        final PointsFormat stock = new StockPointsFormat(leafSize);
        final PointsFormat split = new Lucene90SplitPointsFormat(leafSize);
        final PointsFormat perField = new PerFieldPointsFormat() {
            @Override
            public PointsFormat getPointsFormatForField(FieldInfo field) {
                return TS_SPLIT.equals(field.name) ? split : stock;
            }

            @Override
            protected String getFormatName(PointsFormat format) {
                return format == stock ? STOCK_FORMAT_NAME : super.getFormatName(format);
            }
        };
        return new FilterCodec("Lucene104SplitPoints", new Lucene104Codec()) {
            @Override
            public PointsFormat pointsFormat() {
                return perField;
            }
        };
    }

    /** A test index: values[i] on both time fields, doc i has sel:s if selected[i]. */
    static final class TestIndex implements AutoCloseable {
        final Directory dir;
        final DirectoryReader reader;

        TestIndex(long[] values, boolean[] selected, double deleteRatio, int leafSize, int segments) throws IOException {
            dir = newDirectory();
            IndexWriterConfig iwc = new IndexWriterConfig().setCodec(twinCodec(leafSize));
            iwc.setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                int perSegment = Math.max(1, (values.length + segments - 1) / segments);
                for (int i = 0; i < values.length; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                    for (String f : FIELDS) {
                        doc.add(new LongPoint(f, values[i]));
                        doc.add(SortedNumericDocValuesField.indexedField(f, values[i]));
                    }
                    if (selected[i]) {
                        doc.add(new StringField(SEL, "s", Field.Store.NO));
                    }
                    w.addDocument(doc);
                    if ((i + 1) % perSegment == 0) {
                        w.flush();
                    }
                }
                if (deleteRatio > 0) {
                    for (int i = 0; i < values.length; i++) {
                        if (random().nextDouble() < deleteRatio) {
                            w.deleteDocuments(new Term("id", Integer.toString(i)));
                        }
                    }
                }
                w.commit();
            }
            reader = DirectoryReader.open(dir);
        }

        @Override
        public void close() throws IOException {
            reader.close();
            dir.close();
        }
    }

    /** The range as DateFieldMapper builds it. */
    static ApproximateScoreQuery range(String field, long l, long u) {
        return new ApproximateScoreQuery(
            new IndexOrDocValuesQuery(LongPoint.newRangeQuery(field, l, u), SortedNumericDocValuesField.newSlowRangeQuery(field, l, u)),
            new ApproximatePointRangeQuery(field, pack(l).bytes, pack(u).bytes, 1, ApproximatePointRangeQuery.LONG_FORMAT)
        );
    }

    /** One search request: sort order, size, track_total_hits (null = default, -1 = false), search_after value. */
    record Request(String field, SortOrder order, int size, int tth, Long searchAfter) {
        Sort sort() {
            return new Sort(new SortedNumericSortField(field, SortField.Type.LONG, order == SortOrder.DESC));
        }

        FieldDoc after() {
            return searchAfter == null ? null : new FieldDoc(Integer.MAX_VALUE, Float.NaN, new Object[] { searchAfter });
        }

        int threshold() {
            return tth == SearchContext.TRACK_TOTAL_HITS_DISABLED ? 1 : tth;
        }

        SearchContext context() {
            SearchContext context = mock(SearchContext.class);
            ShardSearchRequest request = mock(ShardSearchRequest.class);
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.sort(new FieldSortBuilder(field).order(order));
            source.size(size);
            source.terminateAfter(SearchContext.DEFAULT_TERMINATE_AFTER);
            if (searchAfter != null) {
                source.searchAfter(new Object[] { searchAfter });
            }
            when(context.aggregations()).thenReturn(null);
            when(context.trackTotalHitsUpTo()).thenReturn(tth);
            when(context.trackScores()).thenReturn(false);
            when(context.from()).thenReturn(0);
            when(context.size()).thenReturn(size);
            when(context.request()).thenReturn(request);
            when(request.source()).thenReturn(source);
            QueryShardContext shardContext = mock(QueryShardContext.class);
            when(shardContext.fieldMapper(field)).thenReturn(
                new NumberFieldMapper.NumberFieldType(field, NumberFieldMapper.NumberType.LONG)
            );
            when(context.getQueryShardContext()).thenReturn(shardContext);
            return context;
        }
    }

    static ContextIndexSearcher contextSearcher(DirectoryReader reader, SearchContext context) throws IOException {
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            null,
            IndexSearcher.getDefaultQueryCachingPolicy(),
            false,
            null,
            context
        );
    }

    static TopFieldDocs run(DirectoryReader reader, Query query, Request r) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        return searcher.search(query, new TopFieldCollectorManager(r.sort(), r.size(), r.after(), r.threshold()));
    }

    /** The total hits as the response reports them: capped at track_total_hits, none when it is false. */
    static String reported(TotalHits total, Request r) {
        if (r.tth() == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
            return "none";
        }
        if (total.value() > r.tth()) {
            return r.tth() + " " + TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        }
        return total.value() + " " + total.relation();
    }

    static void assertSameTopDocs(String msg, TopFieldDocs expected, TopFieldDocs actual, Request r) {
        assertEquals(msg + " total", reported(expected.totalHits, r), reported(actual.totalHits, r));
        assertEquals(msg + " hits", expected.scoreDocs.length, actual.scoreDocs.length);
        for (int i = 0; i < expected.scoreDocs.length; i++) {
            FieldDoc e = (FieldDoc) expected.scoreDocs[i];
            FieldDoc a = (FieldDoc) actual.scoreDocs[i];
            assertEquals(msg + " doc at " + i, e.doc, a.doc);
            assertArrayEquals(msg + " sort values at " + i, e.fields, a.fields);
        }
    }

    /** Random values: runs of equal values, about half the docs share a value with a neighbour. */
    static long[] randomValues(int numDocs, long maxValue) {
        long[] values = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            values[i] = randomLongBetween(0, maxValue);
        }
        if (randomBoolean()) {
            Arrays.sort(values); // time-clustered, like an append-only log
        }
        return values;
    }

    static boolean[] randomSelection(int numDocs, double ratio) {
        boolean[] selected = new boolean[numDocs];
        for (int i = 0; i < numDocs; i++) {
            selected[i] = random().nextDouble() < ratio;
        }
        return selected;
    }

    static int randomTth() {
        return randomFrom(
            SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO,
            SearchContext.TRACK_TOTAL_HITS_DISABLED,
            randomIntBetween(50, 2000)
        );
    }

    // ---------------------------------------------------------------- D-a

    private void assertSingleClause(BooleanClause.Occur occur, boolean withSearchAfter) throws IOException {
        int numDocs = randomIntBetween(3_000, 20_000);
        long maxValue = randomFrom(numDocs / 8L, numDocs * 4L);
        long[] values = randomValues(numDocs, maxValue);
        try (
            TestIndex index = new TestIndex(
                values,
                randomSelection(numDocs, 0.5),
                randomFrom(0.0, 0.05),
                randomFrom(16, 64, 512),
                randomIntBetween(1, 3)
            )
        ) {
            for (String field : FIELDS) {
                for (SortOrder order : SortOrder.values()) {
                    long l = randomLongBetween(0, maxValue / 2);
                    long u = randomLongBetween(l, maxValue);
                    Long after = withSearchAfter ? randomLongBetween(l, u) : null;
                    Request r = new Request(field, order, randomFrom(10, 50, 500), randomTth(), after);
                    SortIoExperiments.setApproxSingle(true);
                    ApproximateScoreQuery clause = range(field, l, u);
                    Query bool = new BooleanQuery.Builder().add(clause, occur).build();
                    Query rewritten = contextSearcher(index.reader, r.context()).rewrite(bool);
                    boolean applies = after == null && index.reader.hasDeletions() == false;
                    if (applies == false) {
                        // search_after or deletions: the bool keeps its stock form and the clause gets no context
                        assertNull(clause.resolvedQuery);
                    } else if (occur == BooleanClause.Occur.FILTER) {
                        assertTrue(rewritten.toString(), rewritten instanceof BoostQuery);
                        assertEquals(0f, ((BoostQuery) rewritten).getBoost(), 0f);
                        assertSame(clause, ((ConstantScoreQuery) ((BoostQuery) rewritten).getQuery()).getQuery());
                    } else {
                        assertSame(clause, rewritten);
                    }
                    if (applies) {
                        assertSame(
                            "D-a must resolve the clause to the approximation",
                            clause.getApproximationQuery(),
                            clause.resolvedQuery
                        );
                    }
                    TopFieldDocs actual = run(index.reader, rewritten, r);
                    Query plain = new BooleanQuery.Builder().add(range(field, l, u), occur).build();
                    TopFieldDocs expected = run(index.reader, plain, r);
                    assertSameTopDocs(field + " " + r + " [" + l + ", " + u + "]", expected, actual, r);
                }
            }
        }
    }

    public void testSingleFilterClause() throws IOException {
        assertSingleClause(BooleanClause.Occur.FILTER, false);
    }

    public void testSingleMustClause() throws IOException {
        assertSingleClause(BooleanClause.Occur.MUST, false);
    }

    public void testSingleClauseSearchAfter() throws IOException {
        assertSingleClause(randomFrom(BooleanClause.Occur.FILTER, BooleanClause.Occur.MUST), true);
    }

    /**
     * 64 docs, 16 per leaf, value of doc i = i / 11, desc, size 10, track_total_hits false (budget 11): the walk stops
     * after the last leaf (docs 48-63, values 4 and 5), but the 10th hit is value 4 with the lowest doc ID, 44, in the
     * leaf to its left. D-a must return it; the stock approximation alone does not.
     */
    public void testSingleClauseDescTieAtTheCut() throws IOException {
        long[] values = new long[64];
        for (int i = 0; i < values.length; i++) {
            values[i] = i / 11;
        }
        try (TestIndex index = new TestIndex(values, randomSelection(64, 0.5), 0, 16, 1)) {
            for (String field : FIELDS) {
                Request r = new Request(field, SortOrder.DESC, 10, SearchContext.TRACK_TOTAL_HITS_DISABLED, null);
                Query plain = new BooleanQuery.Builder().add(range(field, 0, 100), BooleanClause.Occur.FILTER).build();
                TopFieldDocs expected = run(index.reader, plain, r);
                assertEquals(44, expected.scoreDocs[9].doc);

                SortIoExperiments.setApproxSingle(true);
                ApproximateScoreQuery clause = range(field, 0, 100);
                Query rewritten = contextSearcher(index.reader, r.context()).rewrite(
                    new BooleanQuery.Builder().add(clause, BooleanClause.Occur.FILTER).build()
                );
                assertTrue(((ApproximatePointRangeQuery) clause.getApproximationQuery()).isIncludeTies());
                assertSameTopDocs(field, expected, run(index.reader, rewritten, r), r);

                // the stock approximation (top-level range, no tie pass) misses doc 44
                ApproximateScoreQuery top = range(field, 0, 100);
                Query stockApprox = contextSearcher(index.reader, r.context()).rewrite(top);
                assertSame(top.getApproximationQuery(), top.resolvedQuery);
                assertNotEquals(44, run(index.reader, stockApprox, r).scoreDocs[9].doc);
            }
        }
    }

    public void testSingleClauseNotApproximableIsUnchanged() throws IOException {
        long[] values = randomValues(2_000, 500);
        try (TestIndex index = new TestIndex(values, randomSelection(2_000, 0.5), 0, 64, 1)) {
            SortIoExperiments.setApproxSingle(true);
            // track_total_hits: true: canApproximate is false, so the bool stays as stock rewrites it
            Request r = new Request(TS, SortOrder.DESC, 10, SearchContext.TRACK_TOTAL_HITS_ACCURATE, null);
            ApproximateScoreQuery clause = range(TS, 10, 400);
            Query bool = new BooleanQuery.Builder().add(clause, BooleanClause.Occur.FILTER).build();
            contextSearcher(index.reader, r.context()).rewrite(bool);
            assertNull(clause.resolvedQuery);
            // switch off: the clause is never given the context
            SortIoExperiments.setApproxSingle(false);
            Request r2 = new Request(TS, SortOrder.DESC, 10, SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO, null);
            ApproximateScoreQuery clause2 = range(TS, 10, 400);
            contextSearcher(index.reader, r2.context()).rewrite(
                new BooleanQuery.Builder().add(clause2, BooleanClause.Occur.FILTER).build()
            );
            assertNull(clause2.resolvedQuery);
            // two clauses, a SHOULD clause, or minimum_should_match: not D-a
            SortIoExperiments.setApproxSingle(true);
            ApproximateScoreQuery clause3 = range(TS, 10, 400);
            contextSearcher(index.reader, r2.context()).rewrite(
                new BooleanQuery.Builder().add(clause3, BooleanClause.Occur.FILTER)
                    .add(new TermQuery(new Term(SEL, "s")), BooleanClause.Occur.FILTER)
                    .build()
            );
            assertNull(clause3.resolvedQuery);
            ApproximateScoreQuery clause4 = range(TS, 10, 400);
            contextSearcher(index.reader, r2.context()).rewrite(
                new BooleanQuery.Builder().add(clause4, BooleanClause.Occur.SHOULD).build()
            );
            assertNull(clause4.resolvedQuery);
        }
    }
}
