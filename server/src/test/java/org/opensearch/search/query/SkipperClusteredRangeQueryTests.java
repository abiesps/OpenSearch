/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.IntToLongFunction;

public class SkipperClusteredRangeQueryTests extends OpenSearchTestCase {

    private static final String FIELD = "ts";

    private static Query query(long l, long u) {
        Query dv = SortedNumericDocValuesField.newSlowRangeQuery(FIELD, l, u);
        return new SkipperClusteredRangeQuery(FIELD, l, u, new IndexOrDocValuesQuery(LongPoint.newRangeQuery(FIELD, l, u), dv), dv);
    }

    private static Query stock(long l, long u) {
        return new IndexOrDocValuesQuery(LongPoint.newRangeQuery(FIELD, l, u), SortedNumericDocValuesField.newSlowRangeQuery(FIELD, l, u));
    }

    /** One segment of numDocs docs, the value of doc i is value.applyAsLong(i); skip index on the doc values. */
    private static Directory index(int numDocs, IntToLongFunction value, boolean skipIndex) throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec());
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                long v = value.applyAsLong(i);
                Document doc = new Document();
                doc.add(new LongPoint(FIELD, v));
                doc.add(skipIndex ? SortedNumericDocValuesField.indexedField(FIELD, v) : new SortedNumericDocValuesField(FIELD, v));
                w.addDocument(doc);
            }
            w.forceMerge(1);
        }
        return dir;
    }

    private static long[] shuffled(int numDocs) {
        List<Long> values = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            values.add((long) i);
        }
        Collections.shuffle(values, random());
        long[] out = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            out[i] = values.get(i);
        }
        return out;
    }

    private static SkipperClusteredRangeQuery.SkipperClusteredWeight weight(IndexSearcher searcher, Query q) throws IOException {
        Query rewritten = searcher.rewrite(q);
        assertTrue(rewritten.toString(), rewritten instanceof SkipperClusteredRangeQuery);
        return (SkipperClusteredRangeQuery.SkipperClusteredWeight) rewritten.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
    }

    private static void assertSameDocs(IndexSearcher searcher, long l, long u) throws IOException {
        Sort sort = new Sort(new SortedNumericSortField(FIELD, SortField.Type.LONG, randomBoolean()));
        int n = searcher.getIndexReader().maxDoc();
        TopDocs expected = searcher.search(stock(l, u), n, sort);
        TopDocs actual = searcher.search(query(l, u), n, sort);
        assertEquals(expected.totalHits, actual.totalHits);
        assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
        for (int i = 0; i < expected.scoreDocs.length; i++) {
            ScoreDoc e = expected.scoreDocs[i];
            ScoreDoc a = actual.scoreDocs[i];
            assertEquals(e.doc, a.doc);
        }
        assertEquals(searcher.count(stock(l, u)), searcher.count(query(l, u)));
    }

    public void testClusteredSegmentUsesDocValues() throws IOException {
        int numDocs = randomIntBetween(40_000, 120_000);
        int perValue = randomIntBetween(1, 4);
        try (Directory dir = index(numDocs, i -> i / perValue, true); DirectoryReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = newSearcher(reader, false, false);
            searcher.setQueryCache(null);
            long max = (numDocs - 1) / perValue;
            long l = randomLongBetween(0, max);
            long u = randomLongBetween(l, max);
            Query q = query(l, u);
            SkipperClusteredRangeQuery.SkipperClusteredWeight w = weight(searcher, q);
            LeafReaderContext leaf = reader.leaves().get(0);
            assertTrue("clustered values must use doc values", w.clusteredEstimate(leaf) >= 0);
            assertSameDocs(searcher, l, u);
        }
    }

    public void testShuffledSegmentUsesPoints() throws IOException {
        int numDocs = randomIntBetween(200_000, 260_000);
        long[] values = shuffled(numDocs);
        try (Directory dir = index(numDocs, i -> values[i], true); DirectoryReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = newSearcher(reader, false, false);
            searcher.setQueryCache(null);
            long l = randomLongBetween(0, numDocs / 2);
            long u = l + randomLongBetween(numDocs / 10, numDocs / 2);
            SkipperClusteredRangeQuery.SkipperClusteredWeight w = weight(searcher, query(l, u));
            assertEquals(-1, w.clusteredEstimate(reader.leaves().get(0)));
            assertSameDocs(searcher, l, u);
        }
    }

    public void testNoSkipperUsesPoints() throws IOException {
        int numDocs = randomIntBetween(1_000, 20_000);
        try (Directory dir = index(numDocs, i -> i, false); DirectoryReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = newSearcher(reader, false, false);
            searcher.setQueryCache(null);
            long l = randomLongBetween(1, numDocs / 2);
            long u = randomLongBetween(l, numDocs - 2);
            SkipperClusteredRangeQuery.SkipperClusteredWeight w = weight(searcher, query(l, u));
            assertEquals(-1, w.clusteredEstimate(reader.leaves().get(0)));
            assertSameDocs(searcher, l, u);
        }
    }

    /** Counts the intervals the clustering guard reads. */
    private static final class CountingSkipper extends DocValuesSkipper {
        final DocValuesSkipper in;
        int advances;

        CountingSkipper(DocValuesSkipper in) {
            this.in = in;
        }

        @Override
        public void advance(int target) throws IOException {
            advances++;
            in.advance(target);
        }

        @Override
        public int numLevels() {
            return in.numLevels();
        }

        @Override
        public int minDocID(int level) {
            return in.minDocID(level);
        }

        @Override
        public int maxDocID(int level) {
            return in.maxDocID(level);
        }

        @Override
        public long minValue(int level) {
            return in.minValue(level);
        }

        @Override
        public long maxValue(int level) {
            return in.maxValue(level);
        }

        @Override
        public int docCount(int level) {
            return in.docCount(level);
        }

        @Override
        public long minValue() {
            return in.minValue();
        }

        @Override
        public long maxValue() {
            return in.maxValue();
        }

        @Override
        public int docCount() {
            return in.docCount();
        }
    }

    public void testGuardStopsEarlyOnShuffledSegment() throws IOException {
        int numDocs = randomIntBetween(300_000, 400_000);
        long[] values = shuffled(numDocs);
        try (Directory dir = index(numDocs, i -> values[i], true); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            long l = randomLongBetween(0, numDocs / 4);
            long u = l + randomLongBetween(1, numDocs / 2);
            CountingSkipper skipper = new CountingSkipper(leaf.reader().getDocValuesSkipper(FIELD));
            assertEquals(-1, SkipperClusteredRangeQuery.clusteredEstimate(skipper, l, u, numDocs));
            // every level-1 interval (32,768 docs) is MAYBE; the limit is max(65,536, numDocs / 8) <= 50,000 docs, so
            // the walk stops at the third level-1 interval, long before the end of the segment
            int level1Intervals = (numDocs + 32_767) / 32_768;
            assertTrue("advances " + skipper.advances + " of " + level1Intervals, skipper.advances <= 3);

            // a range over every value is clustered with every doc estimated, from the global bounds alone
            CountingSkipper all = new CountingSkipper(leaf.reader().getDocValuesSkipper(FIELD));
            assertEquals(numDocs, SkipperClusteredRangeQuery.clusteredEstimate(all, -1, numDocs, numDocs));
            assertEquals(0, all.advances);
        }
    }

    public void testEstimateOnClusteredSegment() throws IOException {
        int numDocs = randomIntBetween(100_000, 200_000);
        try (Directory dir = index(numDocs, i -> i, true); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = reader.leaves().get(0);
            long l = randomLongBetween(0, numDocs - 1);
            long u = randomLongBetween(l, numDocs - 1);
            long estimate = SkipperClusteredRangeQuery.clusteredEstimate(leaf.reader().getDocValuesSkipper(FIELD), l, u, numDocs);
            // the estimate counts the YES and MAYBE intervals: at least the matches, at most matches + 2 level-1 intervals
            long matches = u - l + 1;
            assertTrue(estimate + " vs " + matches, estimate >= matches && estimate <= matches + 2 * 32_768);
            // a range outside the values is clustered with no matches
            assertEquals(
                0,
                SkipperClusteredRangeQuery.clusteredEstimate(leaf.reader().getDocValuesSkipper(FIELD), numDocs, numDocs + 5, numDocs)
            );
        }
    }

    public void testRewriteToMatchAllAndNone() throws IOException {
        int numDocs = randomIntBetween(100, 2_000);
        try (Directory dir = index(numDocs, i -> i, true); DirectoryReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = newSearcher(reader, false, false);
            Query all = searcher.rewrite(query(Long.MIN_VALUE, Long.MAX_VALUE));
            assertEquals(searcher.rewrite(stock(Long.MIN_VALUE, Long.MAX_VALUE)).getClass(), all.getClass());
            assertEquals(MatchAllDocsQuery.class, all.getClass());
            Query none = searcher.rewrite(query(numDocs + 10, numDocs + 20));
            assertEquals(MatchNoDocsQuery.class, none.getClass());
        }
    }

    public void testEqualsHashCodeToString() {
        Query a = query(10, 20);
        Query b = query(10, 20);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, query(11, 20));
        assertNotEquals(a, query(10, 21));
        Query dv = SortedNumericDocValuesField.newSlowRangeQuery(FIELD, 10, 20);
        Query iod = stock(10, 20);
        assertNotEquals(a, new SkipperClusteredRangeQuery("other", 10, 20, iod, dv));
        assertNotEquals(a, new SkipperClusteredRangeQuery(FIELD, 10, 20, stock(10, 21), dv));
        assertNotEquals(
            a,
            new SkipperClusteredRangeQuery(FIELD, 10, 20, iod, SortedNumericDocValuesField.newSlowRangeQuery(FIELD, 10, 21))
        );
        assertEquals("SkipperClustered(" + iod.toString(FIELD) + ")", a.toString(FIELD));
    }
}
