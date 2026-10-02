/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Checks the run-ahead buffer and the replay ring of {@link DocValuesPrefetch}: every match reaches the collector once and
 * in order, and the planner requests exactly the first match of every node that holds a match, before that doc is
 * collected (one node ahead, never a doc that is not read).
 */
public class DocValuesRunAheadTests extends OpenSearchTestCase {

    @Override
    public void tearDown() throws Exception {
        DocValuesPrefetch.setEnabled(false);
        super.tearDown();
    }

    /** A node of {@code nodeDocs} doc IDs; records the requested docs and how many docs were collected before. */
    private static final class FakeField implements DocValuesPrefetch.Field {
        final int nodeDocs;
        final int maxDoc;
        final List<Integer> requested = new ArrayList<>();
        final List<Integer> collectedBefore = new ArrayList<>();
        int collected;

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
            requested.add(doc);
            collectedBefore.add(collected);
        }
    }

    /** Matches of a window, backed by a 4,096-bit set that may be longer than the window (as Lucene's windows are). */
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

    public void testRunAhead() throws IOException {
        DocValuesPrefetch.setEnabled(true);
        for (int iter = 0; iter < 20; iter++) {
            final int maxDoc = randomIntBetween(1, 1_500_000);
            final FixedBitSet matches = randomMatches(maxDoc);
            final int lag = randomFrom(4096, 10_000, 65_536, 1 << 17);
            final DocValuesPrefetch.RunAhead ra = new DocValuesPrefetch.RunAhead(lag, randomBoolean());
            final FakeField field = new FakeField(randomIntBetween(500, 200_000), maxDoc);
            final DocValuesPrefetch.Planner planner = DocValuesPrefetch.planner(field, ra, DocValuesPrefetch.ALL_MATCHES);
            final List<Integer> delivered = new ArrayList<>();
            // docs the scorer handed over one at a time, and docs the collector got one at a time (not in a stream)
            final List<Integer> perDocIn = new ArrayList<>();
            final List<Integer> perDocOut = new ArrayList<>();
            final boolean[] inStream = new boolean[1];
            final LeafBucketCollector out = new LeafBucketCollector() {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    assertEquals(0, owningBucketOrd);
                    if (inStream[0] == false) {
                        perDocOut.add(doc);
                    }
                    planner.advance(doc);
                    delivered.add(doc);
                    field.collected++;
                }

                @Override
                public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
                    inStream[0] = true;
                    try {
                        collectStream(stream, owningBucketOrd);
                    } finally {
                        inStream[0] = false;
                    }
                }

                @Override
                public void collectRange(int min, int max) throws IOException {
                    assertTrue(min < max);
                    inStream[0] = true;
                    try {
                        for (int doc = min; doc < max; doc++) {
                            collect(doc, 0);
                        }
                    } finally {
                        inStream[0] = false;
                    }
                }

                private void collectStream(DocIdStream stream, long owningBucketOrd) throws IOException {
                    if (randomBoolean()) {
                        stream.forEach(doc -> collect(doc, owningBucketOrd));
                    } else {
                        int[] buf = new int[randomIntBetween(1, 3000)];
                        for (int n = stream.intoArray(buf); n > 0; n = stream.intoArray(buf)) {
                            for (int i = 0; i < n; i++) {
                                collect(buf[i], owningBucketOrd);
                            }
                        }
                    }
                }
            };
            final LeafBucketCollector in = ra.wrap(out);
            // feed like a bulk scorer: windows, single docs and fully matching ranges, in doc ID order
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
                        in.collect(new WindowStream(window, next), 0);
                        doc = end;
                    }
                    case 1 -> {
                        perDocIn.add(next);
                        in.collect(next, 0);
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
            in.finish();
            assertDelivered(matches, delivered);
            assertEquals("docs handed over one at a time are collected one at a time", perDocIn, perDocOut);
            assertPlanned(matches, field);
        }
    }

    public void testReplay() throws IOException {
        DocValuesPrefetch.setEnabled(true);
        for (int iter = 0; iter < 20; iter++) {
            final int maxDoc = randomIntBetween(1, 1_500_000);
            final FixedBitSet matches = randomMatches(maxDoc);
            final DocValuesPrefetch.Replay replay = new DocValuesPrefetch.Replay(randomFrom(4096, 65_536, 1 << 17));
            final FakeField field = new FakeField(randomIntBetween(500, 200_000), maxDoc);
            final DocValuesPrefetch.Planner planner = DocValuesPrefetch.planner(field, replay, DocValuesPrefetch.ALL_MATCHES);
            assertTrue(replay.hasPlanners());
            final List<Integer> delivered = new ArrayList<>();
            final LeafBucketCollector out = new LeafBucketCollector() {
                @Override
                public void collect(int doc, long owningBucketOrd) {
                    throw new AssertionError("batch expected");
                }

                @Override
                public void collectBatch(int[] docs, long[] buckets, int count) throws IOException {
                    planner.advance(docs[0]);
                    planner.advance(docs[count - 1]);
                    for (int i = 0; i < count; i++) {
                        assertEquals(docs[i] % 7, buckets[i]);
                        delivered.add(docs[i]);
                        field.collected++;
                    }
                }
            };
            for (int d = matches.nextSetBit(0); d != DocIdSetIterator.NO_MORE_DOCS; d = d + 1 >= maxDoc
                ? DocIdSetIterator.NO_MORE_DOCS
                : matches.nextSetBit(d + 1)) {
                replay.add(d, d % 7, out);
            }
            replay.finish(out);
            assertDelivered(matches, delivered);
            // batches advance the planner only at their first and last doc: a node may be skipped inside a batch,
            // but every request is still the first match of a node, in order, before it is collected
            assertRequestsAreFirstMatches(matches, field);
        }
    }

    private static void assertDelivered(FixedBitSet matches, List<Integer> delivered) {
        int i = 0;
        for (int d = matches.length() == 0 ? DocIdSetIterator.NO_MORE_DOCS : matches.nextSetBit(0); d != DocIdSetIterator.NO_MORE_DOCS; d =
            d + 1 >= matches.length() ? DocIdSetIterator.NO_MORE_DOCS : matches.nextSetBit(d + 1)) {
            assertTrue("missing doc " + d, i < delivered.size());
            assertEquals((int) delivered.get(i++), d);
        }
        assertEquals(i, delivered.size());
    }

    /** The first match of every node that holds a match, in order. */
    private static List<Integer> firstMatches(FixedBitSet matches, FakeField field) {
        List<Integer> out = new ArrayList<>();
        int d = matches.length() == 0 ? DocIdSetIterator.NO_MORE_DOCS : matches.nextSetBit(0);
        while (d != DocIdSetIterator.NO_MORE_DOCS) {
            out.add(d);
            int nodeEnd = field.nextNodeDoc(d, 0);
            d = nodeEnd == DocIdSetIterator.NO_MORE_DOCS ? nodeEnd : matches.nextSetBit(nodeEnd);
        }
        return out;
    }

    private static void assertPlanned(FixedBitSet matches, FakeField field) {
        assertEquals(firstMatches(matches, field), field.requested);
        assertRequestedBeforeCollected(matches, field);
    }

    private static void assertRequestsAreFirstMatches(FixedBitSet matches, FakeField field) {
        List<Integer> first = firstMatches(matches, field);
        int j = 0;
        for (int doc : field.requested) {
            while (j < first.size() && first.get(j) < doc) {
                j++;
            }
            assertTrue("requested doc " + doc + " is not the first match of its node", j < first.size() && first.get(j) == doc);
            j++;
        }
        if (first.isEmpty() == false) {
            assertEquals(first.get(0), field.requested.get(0));
        }
        assertRequestedBeforeCollected(matches, field);
    }

    private static void assertRequestedBeforeCollected(FixedBitSet matches, FakeField field) {
        for (int k = 0; k < field.requested.size(); k++) {
            int doc = field.requested.get(k);
            int rank = doc == 0 ? 0 : matches.cardinality(0, doc);
            assertTrue("doc " + doc + " requested after it was collected", field.collectedBefore.get(k) <= rank);
        }
    }
}
