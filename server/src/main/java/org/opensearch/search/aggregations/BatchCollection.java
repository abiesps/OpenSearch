/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.search.DocIdStream;
import org.opensearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;

/**
 * Experiment switch and helpers for batch (vectorized) aggregation collection. Off by default (stock behavior); read
 * when a leaf collector is created.
 * <p>
 * When on, aggregators that support it consume a {@link DocIdStream} in chunks of {@link #CHUNK} doc IDs, read their
 * values with one bulk call per chunk ({@link NumericDocValues#longValues}) and reduce the chunk in a tight loop, and
 * bucket aggregators hand runs of docs that fall in one bucket to their sub-aggregators as a stream instead of one
 * doc at a time.
 *
 * @opensearch.internal
 */
public final class BatchCollection {

    /** Doc IDs per bulk read. */
    public static final int CHUNK = 1024;

    private static volatile boolean enabled;
    private static final LongAdder bulkChunks = new LongAdder();
    private static final LongAdder streamRuns = new LongAdder();
    private static final LongAdder perDocStreams = new LongAdder();

    private BatchCollection() {}

    /** Counts a chunk of doc IDs read with one bulk value call. */
    public static void countBulkChunk() {
        bulkChunks.increment();
    }

    /** Counts a single-bucket run handed to a sub-aggregation as a stream. */
    public static void countStreamRun() {
        streamRuns.increment();
    }

    /** Counts a DocIdStream that a batch-capable collector consumed one doc at a time (diagnostics). */
    public static void countPerDocStream() {
        perDocStreams.increment();
    }

    /** DocIdStreams consumed one doc at a time by batch-capable collectors since the last {@link #resetCounters()}. */
    public static long perDocStreams() {
        return perDocStreams.sum();
    }

    /** Chunks read with one bulk value call since the last {@link #resetCounters()}. */
    public static long bulkChunks() {
        return bulkChunks.sum();
    }

    /** Single-bucket runs handed to sub-aggregations as streams since the last {@link #resetCounters()}. */
    public static long streamRuns() {
        return streamRuns.sum();
    }

    /** Sets the counters to zero. */
    public static void resetCounters() {
        bulkChunks.reset();
        streamRuns.reset();
        perDocStreams.reset();
    }

    /** Turns batch collection on or off for leaf collectors created from now on. */
    public static void setEnabled(boolean on) {
        enabled = on;
    }

    /** Returns whether batch collection is on. */
    public static boolean isEnabled() {
        return enabled;
    }

    /**
     * The values of a non-floating-point numeric field (integers, dates, booleans) as a single-valued
     * {@link NumericDocValues}, whose long values are exactly the values the double view returns, or {@code null} when
     * the source is not such a field in this segment (floating point, unsigned long, scripts, missing-value wrappers,
     * multi-valued).
     */
    public static NumericDocValues exactLongs(ValuesSource.Numeric valuesSource, LeafReaderContext ctx) throws IOException {
        if (valuesSource.getClass() != ValuesSource.Numeric.FieldData.class
            || valuesSource.isFloatingPoint()
            || valuesSource.isBigInteger()) {
            return null;
        }
        return DocValues.unwrapSingleton(valuesSource.longValues(ctx));
    }

    /** Whether every doc in {@code docs[0..n)} (sorted) has a value; positions {@code values} on {@code docs[0]}. */
    public static boolean allHaveValues(NumericDocValues values, int[] docs, int n) throws IOException {
        return n > 0 && values.advanceExact(docs[0]) && values.docIDRunEnd() > docs[n - 1];
    }

    /**
     * View of a {@link DocIdStream} limited to doc IDs below {@code upTo}, which counts the doc IDs it hands out. The
     * consumer must consume the view fully; {@link #drainAndCount()} consumes what is left and returns the total.
     */
    public static final class UpTo extends DocIdStream {
        private DocIdStream in;
        private int upTo;
        private int count;

        /** Resets the view to the doc IDs of {@code in} below {@code upTo}, with a count of zero. */
        public UpTo reset(DocIdStream in, int upTo) {
            this.in = in;
            this.upTo = upTo;
            this.count = 0;
            return this;
        }

        @Override
        public void forEach(int upTo, CheckedIntConsumer<IOException> consumer) throws IOException {
            in.forEach(Math.min(upTo, this.upTo), doc -> {
                count++;
                consumer.accept(doc);
            });
        }

        @Override
        public int count(int upTo) throws IOException {
            int n = in.count(Math.min(upTo, this.upTo));
            count += n;
            return n;
        }

        @Override
        public int intoArray(int upTo, int[] array) {
            int n = in.intoArray(Math.min(upTo, this.upTo), array);
            count += n;
            return n;
        }

        @Override
        public boolean mayHaveRemaining() {
            return in.mayHaveRemaining();
        }

        /** Consumes the doc IDs left below the limit and returns how many doc IDs the view handed out in total. */
        public int drainAndCount() throws IOException {
            count(upTo);
            return count;
        }
    }
}
