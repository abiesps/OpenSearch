/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.histogram;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.opensearch.index.mapper.DateFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration and property-based tests for DateHistogramAggregator bulk collection.
 * Validates that the bulk collect(DocIdStream, long) path produces correct results.
 *
 * Validates: Requirements 1.1, 1.3, 1.4, 11.1, 11.3
 */
public class DateHistogramBulkCollectionTests extends DateHistogramAggregatorTestCase {

    // ========================================================================
    // Task 9.1: Integration test — deterministic bucket count verification
    // Requirements: 11.1
    // ========================================================================

    /**
     * Index documents with known timestamp values across multiple segments,
     * execute a date_histogram aggregation, and verify bucket counts match
     * expected values. This exercises the bulk collection path since the
     * single-valued LeafBucketCollector overrides collect(DocIdStream, long).
     */
    public void testBulkCollectionKnownTimestamps() throws IOException {
        // Known timestamps grouped by minute:
        //   2024-01-15T10:00:xx -> 3 docs
        //   2024-01-15T10:01:xx -> 2 docs
        //   2024-01-15T10:02:xx -> 1 doc
        //   2024-01-15T10:05:xx -> 4 docs (gap at minutes 3 and 4)
        List<String> timestamps = Arrays.asList(
            "2024-01-15T10:00:10.000Z",
            "2024-01-15T10:00:30.000Z",
            "2024-01-15T10:00:55.000Z",
            "2024-01-15T10:01:15.000Z",
            "2024-01-15T10:01:45.000Z",
            "2024-01-15T10:02:20.000Z",
            "2024-01-15T10:05:01.000Z",
            "2024-01-15T10:05:12.000Z",
            "2024-01-15T10:05:33.000Z",
            "2024-01-15T10:05:59.000Z"
        );

        DateFieldMapper.DateFieldType fieldType = aggregableDateFieldType(false, true);

        try (org.apache.lucene.store.Directory directory = newDirectory()) {
            // Use multiple segments to exercise bulk collection across segments
            IndexWriterConfig config = newIndexWriterConfig();
            config.setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                // Segment 1: first 5 docs
                for (int i = 0; i < 5; i++) {
                    Document doc = new Document();
                    long instant = fieldType.parse(timestamps.get(i));
                    doc.add(new SortedNumericDocValuesField(AGGREGABLE_DATE, instant));
                    doc.add(new LongPoint(AGGREGABLE_DATE, instant));
                    writer.addDocument(doc);
                }
                writer.flush();

                // Segment 2: remaining 5 docs
                for (int i = 5; i < timestamps.size(); i++) {
                    Document doc = new Document();
                    long instant = fieldType.parse(timestamps.get(i));
                    doc.add(new SortedNumericDocValuesField(AGGREGABLE_DATE, instant));
                    doc.add(new LongPoint(AGGREGABLE_DATE, instant));
                    writer.addDocument(doc);
                }
                writer.flush();
            }

            try (IndexReader reader = DirectoryReader.open(directory)) {
                // Verify we have multiple segments
                assertTrue("Expected multiple segments", reader.leaves().size() >= 2);

                IndexSearcher searcher = newSearcher(reader, true, true);

                DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("test_agg");
                aggBuilder.calendarInterval(DateHistogramInterval.MINUTE)
                    .field(AGGREGABLE_DATE)
                    .minDocCount(1L);

                InternalDateHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);

                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals("Expected 4 non-empty minute buckets", 4, buckets.size());

                // Verify each bucket
                assertEquals("2024-01-15T10:00:00.000Z", buckets.get(0).getKeyAsString());
                assertEquals(3, buckets.get(0).getDocCount());

                assertEquals("2024-01-15T10:01:00.000Z", buckets.get(1).getKeyAsString());
                assertEquals(2, buckets.get(1).getDocCount());

                assertEquals("2024-01-15T10:02:00.000Z", buckets.get(2).getKeyAsString());
                assertEquals(1, buckets.get(2).getDocCount());

                assertEquals("2024-01-15T10:05:00.000Z", buckets.get(3).getKeyAsString());
                assertEquals(4, buckets.get(3).getDocCount());
            }
        }
    }

    /**
     * Test with a larger dataset across multiple segments to ensure the bulk
     * collection path handles batches correctly (buffer size is 4096).
     */
    public void testBulkCollectionLargeDataset() throws IOException {
        DateFieldMapper.DateFieldType fieldType = aggregableDateFieldType(false, true);

        // Create 500 documents spread across 5 days
        // Day 1: 100 docs, Day 2: 100 docs, Day 3: 100 docs, Day 4: 100 docs, Day 5: 100 docs
        long baseMillis = fieldType.parse("2024-01-01T00:00:00.000Z");
        long dayMillis = 24L * 60 * 60 * 1000;

        try (org.apache.lucene.store.Directory directory = newDirectory()) {
            IndexWriterConfig config = newIndexWriterConfig();
            config.setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (int day = 0; day < 5; day++) {
                    for (int i = 0; i < 100; i++) {
                        Document doc = new Document();
                        // Spread within the day (each doc offset by i seconds)
                        long instant = baseMillis + day * dayMillis + i * 1000L;
                        doc.add(new SortedNumericDocValuesField(AGGREGABLE_DATE, instant));
                        doc.add(new LongPoint(AGGREGABLE_DATE, instant));
                        writer.addDocument(doc);
                    }
                    // Flush after each day to create separate segments
                    writer.flush();
                }
            }

            try (IndexReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = newSearcher(reader, true, true);

                DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("test_agg");
                aggBuilder.calendarInterval(DateHistogramInterval.DAY)
                    .field(AGGREGABLE_DATE)
                    .minDocCount(1L);

                InternalDateHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);

                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals("Expected 5 day buckets", 5, buckets.size());

                for (int day = 0; day < 5; day++) {
                    assertEquals(
                        "Day " + day + " should have 100 docs",
                        100,
                        buckets.get(day).getDocCount()
                    );
                }
            }
        }
    }

    // ========================================================================
    // Task 9.2: Property test — Aggregation-level bulk collection equivalence
    // Feature: docvalues-prefetch-bulk-collection, Property 3: Aggregation-level bulk collection equivalence
    // Validates: Requirements 1.1, 1.3, 1.4, 11.1, 11.3
    //
    // For 100+ iterations: generate random documents with single-valued timestamps
    // (dense, random encoding), run date_histogram aggregation via the bulk
    // collect(DocIdStream, long) path (which is the default for single-valued fields),
    // and verify bucket counts and keys match independently computed expected values.
    //
    // Since the bulk path is always used when DocIdStream is available for single-valued
    // fields, we verify correctness by computing expected bucket counts independently
    // from the raw timestamp values and comparing against the aggregation output.
    // ========================================================================

    /**
     * Property 3: Aggregation-level bulk collection equivalence.
     *
     * For each iteration, generates random documents with single-valued timestamps,
     * runs a date_histogram aggregation (which exercises the bulk collection path),
     * and verifies bucket counts and keys match expected values computed independently.
     *
     * Validates: Requirements 1.1, 1.3, 1.4, 11.1, 11.3
     */
    public void testAggregationBulkCollectionEquivalence() throws IOException {
        int iterations = atLeast(100);
        for (int iter = 0; iter < iterations; iter++) {
            doTestAggregationBulkCollectionEquivalenceIteration(iter);
        }
    }

    private void doTestAggregationBulkCollectionEquivalenceIteration(int iter) throws IOException {
        DateFieldMapper.DateFieldType fieldType = aggregableDateFieldType(false, true);

        // Generate random parameters
        int numDocs = randomIntBetween(10, 500);
        // Use hour interval for a good balance of bucket granularity
        long intervalMillis = 60L * 60 * 1000; // 1 hour in millis

        // Random base time within a reasonable range
        long baseMillis = fieldType.parse("2020-01-01T00:00:00.000Z");
        // Spread docs across 1-48 hours
        int numHours = randomIntBetween(1, 48);
        long spreadMillis = numHours * intervalMillis;

        // Generate random timestamps and compute expected buckets independently
        long[] timestamps = new long[numDocs];
        Map<Long, Integer> expectedBuckets = new HashMap<>();

        for (int i = 0; i < numDocs; i++) {
            // Random offset within the spread
            long offset = randomLongBetween(0, spreadMillis - 1);
            long ts = baseMillis + offset;
            timestamps[i] = ts;

            // Compute the expected bucket key (floor to hour boundary)
            long bucketKey = (ts / intervalMillis) * intervalMillis;
            expectedBuckets.merge(bucketKey, 1, Integer::sum);
        }

        try (org.apache.lucene.store.Directory directory = newDirectory()) {
            // Randomly decide number of segments (1-4)
            int numSegments = randomIntBetween(1, 4);
            IndexWriterConfig config = newIndexWriterConfig();
            config.setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                int docsPerSegment = numDocs / numSegments;
                int docIndex = 0;
                for (int seg = 0; seg < numSegments; seg++) {
                    int segEnd = (seg == numSegments - 1) ? numDocs : docIndex + docsPerSegment;
                    for (; docIndex < segEnd; docIndex++) {
                        Document doc = new Document();
                        doc.add(new SortedNumericDocValuesField(AGGREGABLE_DATE, timestamps[docIndex]));
                        doc.add(new LongPoint(AGGREGABLE_DATE, timestamps[docIndex]));
                        writer.addDocument(doc);
                    }
                    if (seg < numSegments - 1) {
                        writer.flush();
                    }
                }
            }

            try (IndexReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = newSearcher(reader, true, true);

                DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("test_agg");
                aggBuilder.fixedInterval(new DateHistogramInterval("1h"))
                    .field(AGGREGABLE_DATE)
                    .minDocCount(1L);

                InternalDateHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);

                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();

                // Verify total doc count matches
                long totalDocCount = buckets.stream().mapToLong(Histogram.Bucket::getDocCount).sum();
                assertEquals(
                    "Iteration " + iter + ": total doc count mismatch",
                    numDocs,
                    totalDocCount
                );

                // Verify number of buckets matches
                assertEquals(
                    "Iteration " + iter + ": bucket count mismatch",
                    expectedBuckets.size(),
                    buckets.size()
                );

                // Verify each bucket's key and doc count
                for (Histogram.Bucket bucket : buckets) {
                    // The key is returned as a ZonedDateTime, get millis
                    long keyMillis;
                    if (bucket.getKey() instanceof Number) {
                        keyMillis = ((Number) bucket.getKey()).longValue();
                    } else {
                        // ZonedDateTime
                        keyMillis = ((java.time.ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli();
                    }

                    Integer expectedCount = expectedBuckets.get(keyMillis);
                    assertNotNull(
                        "Iteration " + iter + ": unexpected bucket key " + bucket.getKeyAsString(),
                        expectedCount
                    );
                    assertEquals(
                        "Iteration " + iter + ": doc count mismatch for bucket " + bucket.getKeyAsString(),
                        expectedCount.intValue(),
                        (int) bucket.getDocCount()
                    );
                }
            }
        }
    }

}
