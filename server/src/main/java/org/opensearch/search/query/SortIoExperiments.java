/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

/**
 * Experiment switches for the IO of field-sorted queries. All are off by default (stock behavior). Settings are
 * process-wide and read when a query is rewritten or a leaf collector is created.
 *
 * @opensearch.internal
 */
public final class SortIoExperiments {

    /** Smallest accepted value of {@link #setSortPrefetchDocs(int)}, and its default. */
    public static final int MIN_SORT_PREFETCH_DOCS = 1 << 16;
    /** Largest accepted value of {@link #setSortPrefetchDocs(int)}. */
    public static final int MAX_SORT_PREFETCH_DOCS = 1 << 22;
    /** Default of {@link #setSortPrefetchNodeBytes(long)}. */
    public static final long DEFAULT_SORT_PREFETCH_NODE_BYTES = 128 * 1024;

    private static volatile boolean approxSingle;
    private static volatile boolean approxBool;
    private static volatile boolean skipperRange;
    private static volatile boolean sortPrefetch;
    private static volatile int sortPrefetchDocs = MIN_SORT_PREFETCH_DOCS;
    private static volatile long sortPrefetchNodeBytes = DEFAULT_SORT_PREFETCH_NODE_BYTES;
    private static volatile boolean clamp;

    private SortIoExperiments() {}

    /** Sets whether a {@code bool} query with a single required range clause on the sort field is approximated (D-a). */
    public static void setApproxSingle(boolean on) {
        approxSingle = on;
    }

    /** Returns whether a {@code bool} query with a single range clause on the sort field is approximated. */
    public static boolean isApproxSingle() {
        return approxSingle;
    }

    /** Sets whether a {@code bool} query with a range clause on the sort field and other clauses is approximated (D-b). */
    public static void setApproxBool(boolean on) {
        approxBool = on;
    }

    /** Returns whether a {@code bool} query with a range clause on the sort field and other clauses is approximated. */
    public static boolean isApproxBool() {
        return approxBool;
    }

    /** Sets whether a range on the sort field is answered from the doc-values skipper instead of the points index (C). */
    public static void setSkipperRange(boolean on) {
        skipperRange = on;
    }

    /** Returns whether a range on the sort field is answered from the doc-values skipper. */
    public static boolean isSkipperRange() {
        return skipperRange;
    }

    /** Sets whether the sort comparator prefetches the doc-values nodes it will read (E). */
    public static void setSortPrefetch(boolean on) {
        sortPrefetch = on;
    }

    /** Returns whether the sort comparator prefetches the doc-values nodes it will read. */
    public static boolean isSortPrefetch() {
        return sortPrefetch;
    }

    /**
     * Sets how many doc IDs ahead of collection the sort comparator looks for the next node to prefetch.
     *
     * @throws IllegalArgumentException if {@code docs} is not in 65,536..4,194,304
     */
    public static void setSortPrefetchDocs(int docs) {
        if (docs < MIN_SORT_PREFETCH_DOCS || docs > MAX_SORT_PREFETCH_DOCS) {
            throw new IllegalArgumentException(
                "sort prefetch docs must be in " + MIN_SORT_PREFETCH_DOCS + ".." + MAX_SORT_PREFETCH_DOCS + ", got " + docs
            );
        }
        sortPrefetchDocs = docs;
    }

    /** Returns how many doc IDs ahead of collection the sort comparator looks. */
    public static int sortPrefetchDocs() {
        return sortPrefetchDocs;
    }

    /**
     * Sets the size of one storage node in bytes, e.g. the cache block size.
     *
     * @throws IllegalArgumentException if {@code bytes} is not a positive power of two
     */
    public static void setSortPrefetchNodeBytes(long bytes) {
        if (bytes <= 0 || Long.bitCount(bytes) != 1) {
            throw new IllegalArgumentException("sort prefetch node bytes must be a positive power of two, got " + bytes);
        }
        sortPrefetchNodeBytes = bytes;
    }

    /** Returns the size of one storage node in bytes. */
    public static long sortPrefetchNodeBytes() {
        return sortPrefetchNodeBytes;
    }

    /** Sets whether the sort comparator's competitive range is clamped to the query's range on the sort field (K1). */
    public static void setClamp(boolean on) {
        clamp = on;
    }

    /** Returns whether the sort comparator's competitive range is clamped to the query's range. */
    public static boolean isClamp() {
        return clamp;
    }

    /** Sets every switch back to its default. For tests. */
    public static void resetForTest() {
        approxSingle = false;
        approxBool = false;
        skipperRange = false;
        sortPrefetch = false;
        sortPrefetchDocs = MIN_SORT_PREFETCH_DOCS;
        sortPrefetchNodeBytes = DEFAULT_SORT_PREFETCH_NODE_BYTES;
        clamp = false;
    }
}
