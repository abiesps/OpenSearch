/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;

/**
 * Experiment: non-speculative doc-values prefetch for aggregations. Off by default; read when a leaf collector is
 * created.
 * <p>
 * A {@link Planner} keeps one storage node (cache block of {@link #nodeBytes()} bytes) of a field requested ahead of the
 * node being read, doc-ID aligned: when collection reaches a doc in a new node, it asks the field for the first doc of
 * the following node ({@code nextPrefetchNodeDoc}), finds the next doc at or after it whose value will be read, and
 * requests that doc's node. The proof that a doc will be read is the search's own query: each planner advances a second
 * scorer of the query (the look-ahead iterator) to find the next match, and a {@link ReadFilter} can narrow matches to
 * the docs whose values the collector reads (for example, the date_histogram skip-list collector reads no values in a
 * skipper interval that falls in one bucket). So every requested node holds a value the collector will read.
 *
 * @opensearch.internal
 */
public final class DocValuesPrefetch {

    private static volatile boolean enabled;
    private static volatile long nodeBytes = 128 * 1024;
    private static final LongAdder planners = new LongAdder();
    private static final LongAdder requests = new LongAdder();

    private DocValuesPrefetch() {}

    /** Turns the planner on or off for leaf collectors created from now on. */
    public static void setEnabled(boolean on) {
        enabled = on;
    }

    /** Returns whether planners are created. */
    public static boolean isEnabled() {
        return enabled;
    }

    /** Sets the node size, e.g. the cache block size. */
    public static void setNodeBytes(long bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be > 0, got " + bytes);
        }
        nodeBytes = bytes;
    }

    /** Returns the node size. */
    public static long nodeBytes() {
        return nodeBytes;
    }

    /** Planners created since the last {@link #resetCounters()}. */
    public static long planners() {
        return planners.sum();
    }

    /** Node requests (one doc's node each) since the last {@link #resetCounters()}. */
    public static long requests() {
        return requests.sum();
    }

    /** Sets the counters to zero. */
    public static void resetCounters() {
        planners.reset();
        requests.reset();
    }

    /**
     * The docs of a segment that match the search's query, as a new iterator that is only advanced (a second scorer of
     * the query), skipping deleted docs.
     */
    public static DocIdSetIterator queryMatches(SearchContext context, LeafReaderContext ctx) throws IOException {
        final Query query = context.searcher().rewrite(context.query());
        final Weight weight = context.searcher().createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
        final Scorer scorer = weight.scorer(ctx);
        if (scorer == null) {
            return DocIdSetIterator.empty();
        }
        final TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
        final DocIdSetIterator matches = twoPhase == null ? scorer.iterator() : TwoPhaseIterator.asDocIdSetIterator(twoPhase);
        final Bits live = ctx.reader().getLiveDocs();
        if (live == null) {
            return matches;
        }
        return new FilteredDocIdSetIterator(matches) {
            @Override
            protected boolean match(int doc) {
                return live.get(doc);
            }
        };
    }

    /** A field whose stored values can be mapped to nodes. */
    public interface Field {
        /** First doc whose value starts in a later node than {@code doc}'s, NO_MORE_DOCS, or -1 if unsupported. */
        int nextNodeDoc(int doc, long nodeBytes) throws IOException;

        /** Requests the node(s) holding {@code doc}'s value. */
        void prefetch(int doc, long nodeBytes) throws IOException;
    }

    /** Planning view of a numeric field, or null if its codec does not support node planning. */
    public static Field of(NumericDocValues values) throws IOException {
        if (values == null || values.nextPrefetchNodeDoc(0, nodeBytes) < 0) {
            return null;
        }
        return new Field() {
            @Override
            public int nextNodeDoc(int doc, long nodeBytes) throws IOException {
                return values.nextPrefetchNodeDoc(doc, nodeBytes);
            }

            @Override
            public void prefetch(int doc, long nodeBytes) throws IOException {
                values.prefetchNodes(doc, doc + 1, nodeBytes);
            }
        };
    }

    /** Planning view of the ordinals of a sorted field, or null if unsupported. */
    public static Field of(SortedDocValues values) throws IOException {
        if (values == null || values.nextPrefetchNodeDoc(0, nodeBytes) < 0) {
            return null;
        }
        return new Field() {
            @Override
            public int nextNodeDoc(int doc, long nodeBytes) throws IOException {
                return values.nextPrefetchNodeDoc(doc, nodeBytes);
            }

            @Override
            public void prefetch(int doc, long nodeBytes) throws IOException {
                values.prefetchNodes(doc, doc + 1, nodeBytes);
            }
        };
    }

    /** Narrows query matches to the docs whose values the collector reads. */
    public interface ReadFilter {
        /** The first doc at or after {@code target} that matches and whose value is read, or NO_MORE_DOCS. */
        int nextRead(int target, DocIdSetIterator matches) throws IOException;
    }

    /** Every match is read. */
    public static final ReadFilter ALL_MATCHES = DocValuesPrefetch::advanceTo;

    /** The first doc of {@code it} at or after {@code target}; never moves the iterator backwards. */
    public static int advanceTo(int target, DocIdSetIterator it) throws IOException {
        final int doc = it.docID();
        return doc >= target ? doc : it.advance(target);
    }

    /**
     * Creates a planner and requests the node of the first doc that will be read, or returns null when planning is
     * off or the field does not support it.
     */
    public static Planner planner(Field field, DocIdSetIterator matches, ReadFilter filter) throws IOException {
        if (enabled == false || field == null || matches == null) {
            return null;
        }
        final Planner p = new Planner(field, matches, filter, nodeBytes);
        planners.increment();
        p.start();
        return p;
    }

    /** Keeps one node of a field requested ahead of the node being read. */
    public static final class Planner {
        private final Field field;
        private final DocIdSetIterator matches;
        private final ReadFilter filter;
        private final long nodeBytes;
        /** When a read doc reaches it, the doc is in a node not planned from yet: request the following one. */
        private int trigger;

        private Planner(Field field, DocIdSetIterator matches, ReadFilter filter, long nodeBytes) {
            this.field = field;
            this.matches = matches;
            this.filter = filter;
            this.nodeBytes = nodeBytes;
        }

        private void start() throws IOException {
            final int first = filter.nextRead(0, matches);
            if (first == DocIdSetIterator.NO_MORE_DOCS) {
                trigger = DocIdSetIterator.NO_MORE_DOCS;
            } else {
                request(first);
                trigger = first;
            }
        }

        private void request(int doc) throws IOException {
            field.prefetch(doc, nodeBytes);
            requests.increment();
        }

        /**
         * Called with a doc whose value is about to be read (or the last doc of a batch about to be read). If it is in a
         * node not planned from yet, requests the node of the next doc that will be read after that node.
         */
        public void advance(int doc) throws IOException {
            if (doc < trigger) {
                return;
            }
            final int nodeEnd = field.nextNodeDoc(doc, nodeBytes);
            if (nodeEnd == DocIdSetIterator.NO_MORE_DOCS || nodeEnd < 0) {
                trigger = DocIdSetIterator.NO_MORE_DOCS;
                return;
            }
            final int next = filter.nextRead(nodeEnd, matches);
            if (next == DocIdSetIterator.NO_MORE_DOCS) {
                trigger = DocIdSetIterator.NO_MORE_DOCS;
                return;
            }
            request(next);
            trigger = nodeEnd;
        }
    }
}
