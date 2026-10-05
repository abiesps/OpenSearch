/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;

/**
 * A latency histogram in microseconds that many threads record into: an HdrHistogram {@link ConcurrentHistogram} with
 * {@value #SIGNIFICANT_DIGITS} significant digits (a recorded value is reported within 1 %), from 1 to
 * {@value #MAX_MICROS} microseconds; larger values are recorded as {@value #MAX_MICROS}. A 10 % shift (for example 20,000
 * to 22,000 microseconds) is 10 times the resolution. {@link #reset()} swaps in an empty histogram, so recording never
 * waits for a reset, and {@link #snapshot()} reads a copy.
 */
final class LatencyHistogram {

    /** Largest recorded value: 60 s. */
    static final long MAX_MICROS = 60_000_000L;
    static final int SIGNIFICANT_DIGITS = 2;

    private volatile ConcurrentHistogram histogram = newHistogram();

    private static ConcurrentHistogram newHistogram() {
        return new ConcurrentHistogram(1, MAX_MICROS, SIGNIFICANT_DIGITS);
    }

    void recordNanos(long nanos) {
        recordMicros(TimeUnit.NANOSECONDS.toMicros(nanos));
    }

    void recordMicros(long micros) {
        histogram.recordValue(Math.min(Math.max(micros, 0), MAX_MICROS));
    }

    void reset() {
        histogram = newHistogram();
    }

    Snapshot snapshot() {
        return new Snapshot(histogram.copy());
    }

    /** A copy of the recorded values. */
    static final class Snapshot {
        private final Histogram histogram;

        Snapshot(Histogram histogram) {
            this.histogram = histogram;
        }

        long count() {
            return histogram.getTotalCount();
        }

        long median() {
            return valueAt(50);
        }

        long percentile90() {
            return valueAt(90);
        }

        long percentile99() {
            return valueAt(99);
        }

        long max() {
            return count() == 0 ? 0 : histogram.getMaxValue();
        }

        private long valueAt(double percentile) {
            return count() == 0 ? 0 : histogram.getValueAtPercentile(percentile);
        }

        /**
         * The non-empty buckets in increasing order, each as {upper value in microseconds, count}. The difference of two
         * snapshots' buckets is the histogram of the values recorded between them.
         */
        List<long[]> buckets() {
            final List<long[]> buckets = new ArrayList<>();
            for (HistogramIterationValue v : histogram.recordedValues()) {
                buckets.add(new long[] { v.getValueIteratedTo(), v.getCountAddedInThisIterationStep() });
            }
            return buckets;
        }
    }
}
