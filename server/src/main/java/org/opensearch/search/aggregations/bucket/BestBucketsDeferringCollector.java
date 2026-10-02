/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.LongHash;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BatchCollection;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.aggregations.DocValuesPrefetch;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.MultiBucketCollector;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A specialization of {@link DeferringBucketCollector} that collects all
 * matches and then is able to replay a given subset of buckets which represent
 * the survivors from a pruning process performed by the aggregator that owns
 * this collector.
 *
 * @opensearch.internal
 */
public class BestBucketsDeferringCollector extends DeferringBucketCollector {
    /**
     * Entry in the bucket collector
     *
     * @opensearch.internal
     */
    static class Entry {
        final LeafReaderContext context;
        final PackedLongValues docDeltas;
        final PackedLongValues buckets;

        Entry(LeafReaderContext context, PackedLongValues docDeltas, PackedLongValues buckets) {
            this.context = Objects.requireNonNull(context);
            this.docDeltas = Objects.requireNonNull(docDeltas);
            this.buckets = Objects.requireNonNull(buckets);
        }
    }

    protected List<Entry> entries = new ArrayList<>();
    protected BucketCollector collector;
    protected final SearchContext searchContext;
    protected final boolean isGlobal;
    protected LeafReaderContext context;
    protected PackedLongValues.Builder docDeltasBuilder;
    protected PackedLongValues.Builder bucketsBuilder;
    protected long maxBucket = -1;
    protected boolean finished;
    protected LongHash selectedBuckets;

    /**
     * Sole constructor.
     * @param context The search context
     * @param isGlobal Whether this collector visits all documents (global context)
     */
    public BestBucketsDeferringCollector(SearchContext context, boolean isGlobal) {
        this.searchContext = context;
        this.isGlobal = isGlobal;
        // a postCollection call is not made by the IndexSearcher when there are no segments.
        // In this case init the collector as finished.
        this.finished = context.searcher().getLeafContexts().isEmpty();
    }

    @Override
    public ScoreMode scoreMode() {
        if (collector == null) {
            throw new IllegalStateException();
        }
        return collector.scoreMode();
    }

    /** Set the deferred collectors. */
    @Override
    public void setDeferredCollector(Iterable<BucketCollector> deferredCollectors) {
        this.collector = MultiBucketCollector.wrap(deferredCollectors);
    }

    private void finishLeaf() {
        if (context != null) {
            assert docDeltasBuilder != null && bucketsBuilder != null;
            entries.add(new Entry(context, docDeltasBuilder.build(), bucketsBuilder.build()));
            context = null;
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
        finishLeaf();

        context = null;
        // allocates the builder lazily in case this segment doesn't contain any match
        docDeltasBuilder = null;
        bucketsBuilder = null;

        return new LeafBucketCollector() {
            int lastDoc = 0;

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (context == null) {
                    context = ctx;
                    docDeltasBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
                    bucketsBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
                }
                docDeltasBuilder.add(doc - lastDoc);
                bucketsBuilder.add(bucket);
                lastDoc = doc;
                maxBucket = Math.max(maxBucket, bucket);
            }

            @Override
            public void collectBatch(int[] docs, long[] buckets, int count) throws IOException {
                if (count == 0) {
                    return;
                }
                if (context == null) {
                    context = ctx;
                    docDeltasBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
                    bucketsBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
                }
                int last = lastDoc;
                long max = maxBucket;
                for (int i = 0; i < count; i++) {
                    docDeltasBuilder.add(docs[i] - last);
                    bucketsBuilder.add(buckets[i]);
                    last = docs[i];
                    max = Math.max(max, buckets[i]);
                }
                lastDoc = last;
                maxBucket = max;
            }
        };
    }

    @Override
    public void preCollection() throws IOException {
        collector.preCollection();
    }

    @Override
    public void postCollection() throws IOException {
        assert searchContext.searcher().getLeafContexts().isEmpty() || finished != true;
        finishLeaf();
        finished = true;
    }

    /**
     * Replay the wrapped collector, but only on a selection of buckets.
     */
    @Override
    public void prepareSelectedBuckets(long... selectedBuckets) throws IOException {
        if (finished == false) {
            throw new IllegalStateException("Cannot replay yet, collection is not finished: postCollect() has not been called");
        }
        if (this.selectedBuckets != null) {
            throw new IllegalStateException("Already been replayed");
        }

        this.selectedBuckets = new LongHash(selectedBuckets.length, BigArrays.NON_RECYCLING_INSTANCE);
        for (long ord : selectedBuckets) {
            this.selectedBuckets.add(ord);
        }

        boolean needsScores = scoreMode().needsScores();
        Weight weight = null;
        if (needsScores) {
            Query query = isGlobal ? new MatchAllDocsQuery() : searchContext.query();
            weight = searchContext.searcher().createWeight(searchContext.searcher().rewrite(query), ScoreMode.COMPLETE, 1f);
        }

        for (Entry entry : entries) {
            assert entry.docDeltas.size() > 0 : "segment should have at least one document to replay, got 0";
            try {
                // run-ahead doc-values prefetch: planners of the replayed collectors see the replayed docs ahead
                final DocValuesPrefetch.Replay replay = needsScores ? null : DocValuesPrefetch.beginReplay();
                final LeafBucketCollector leafCollector;
                try {
                    leafCollector = collector.getLeafCollector(entry.context);
                } finally {
                    DocValuesPrefetch.clear(replay);
                }
                DocIdSetIterator scoreIt = null;
                if (needsScores) {
                    Scorer scorer = weight.scorer(entry.context);
                    // We don't need to check if the scorer is null
                    // since we are sure that there are documents to replay (entry.docDeltas it not empty).
                    scoreIt = scorer.iterator();
                    leafCollector.setScorer(scorer);
                }
                final PackedLongValues.Iterator docDeltaIterator = entry.docDeltas.iterator();
                final PackedLongValues.Iterator buckets = entry.buckets.iterator();
                if (needsScores == false && BatchCollection.isEnabled() && maxBucket < MAX_REBASE_TABLE) {
                    replayBatch(
                        leafCollector,
                        entry.docDeltas.size(),
                        docDeltaIterator,
                        buckets,
                        replay != null && replay.hasPlanners() ? replay : null
                    );
                    continue;
                }
                int doc = 0;
                for (long i = 0, end = entry.docDeltas.size(); i < end; ++i) {
                    doc += (int) docDeltaIterator.next();
                    final long bucket = buckets.next();
                    final long rebasedBucket = this.selectedBuckets.find(bucket);
                    if (rebasedBucket != -1) {
                        if (needsScores) {
                            if (scoreIt.docID() < doc) {
                                scoreIt.advance(doc);
                            }
                            // aggregations should only be replayed on matching documents
                            assert scoreIt.docID() == doc;
                        }
                        leafCollector.collect(doc, rebasedBucket);
                    }
                }
            } catch (CollectionTerminatedException e) {
                // collection was terminated prematurely
                // continue with the following leaf
            }
        }
        collector.postCollection();
    }

    /** Batch replay builds a dense rebase table of bucket ordinals up to this size. */
    private static final long MAX_REBASE_TABLE = 1 << 20;
    private long[] rebaseTable;

    /**
     * Replays one segment in chunks of {@link BatchCollection#CHUNK} docs with
     * {@link LeafBucketCollector#collectBatch}, and rebases buckets with a dense table instead of a hash lookup per doc.
     */
    /**
     * {@link #replayBatch} through a run-ahead ring ({@link DocValuesPrefetch.Replay}): same loop, but the chunks are the
     * ring's, so planners of the replayed collectors see the docs ahead of collection. Its own method, so the JIT compiles
     * the loop as tightly as the plain one.
     */
    static void replayAhead(
        LeafBucketCollector leafCollector,
        long size,
        PackedLongValues.Iterator docDeltaIterator,
        PackedLongValues.Iterator buckets,
        long[] rebase,
        DocValuesPrefetch.Replay replay
    ) throws IOException {
        long start = 0;
        int doc = 0;
        if (replay.isPassThrough()) {
            start = replayPassThrough(leafCollector, size, docDeltaIterator, buckets, rebase, replay);
            if (start == size) {
                return;
            }
            // left pass-through right after a full chunk: its last doc is the last one decoded
            doc = replay.resumeDoc();
        }
        replayRing(leafCollector, start, size, doc, docDeltaIterator, buckets, rebase, replay);
    }

    /**
     * The plain {@link #replayBatch} loop, through {@link DocValuesPrefetch.Replay#passThrough}: while the data is cached
     * the ring is not used. Returns the number of recorded docs consumed: {@code size}, or fewer if a planner left
     * pass-through, then the rest goes through the ring.
     */
    private static long replayPassThrough(
        LeafBucketCollector leafCollector,
        long size,
        PackedLongValues.Iterator docDeltaIterator,
        PackedLongValues.Iterator buckets,
        long[] rebase,
        DocValuesPrefetch.Replay replay
    ) throws IOException {
        final int[] docs = new int[BatchCollection.CHUNK];
        final long[] rebased = new long[BatchCollection.CHUNK];
        int doc = 0;
        int n = 0;
        for (long i = 0; i < size; ++i) {
            doc += (int) docDeltaIterator.next();
            final long rebasedBucket = rebase[(int) buckets.next()];
            if (rebasedBucket != -1) {
                docs[n] = doc;
                rebased[n++] = rebasedBucket;
                if (n == docs.length) {
                    replay.passThrough(docs, rebased, n, leafCollector);
                    n = 0;
                    if (replay.isPassThrough() == false) {
                        return i + 1;
                    }
                }
            }
        }
        replay.passThrough(docs, rebased, n, leafCollector);
        return size;
    }

    /** Replays recorded docs {@code start} to {@code size} - 1 through the ring; {@code doc} is the doc before them. */
    private static void replayRing(
        LeafBucketCollector leafCollector,
        long start,
        long size,
        int doc,
        PackedLongValues.Iterator docDeltaIterator,
        PackedLongValues.Iterator buckets,
        long[] rebase,
        DocValuesPrefetch.Replay replay
    ) throws IOException {
        int[] docs = replay.docsToFill();
        long[] rebased = replay.bucketsToFill();
        int n = 0;
        for (long i = start; i < size; ++i) {
            doc += (int) docDeltaIterator.next();
            final long rebasedBucket = rebase[(int) buckets.next()];
            if (rebasedBucket != -1) {
                docs[n] = doc;
                rebased[n++] = rebasedBucket;
                if (n == docs.length) {
                    replay.sealFull(leafCollector);
                    docs = replay.docsToFill();
                    rebased = replay.bucketsToFill();
                    n = 0;
                }
            }
        }
        replay.finish(n, leafCollector);
    }

    private void replayBatch(
        LeafBucketCollector leafCollector,
        long size,
        PackedLongValues.Iterator docDeltaIterator,
        PackedLongValues.Iterator buckets,
        DocValuesPrefetch.Replay replay
    ) throws IOException {
        if (rebaseTable == null) {
            rebaseTable = new long[Math.toIntExact(maxBucket + 1)];
            for (int b = 0; b < rebaseTable.length; b++) {
                rebaseTable[b] = selectedBuckets.find(b);
            }
        }
        final long[] rebase = rebaseTable;
        if (replay != null) {
            replayAhead(leafCollector, size, docDeltaIterator, buckets, rebase, replay);
            return;
        }
        final int[] docs = new int[BatchCollection.CHUNK];
        final long[] rebased = new long[BatchCollection.CHUNK];
        int doc = 0;
        int n = 0;
        for (long i = 0; i < size; ++i) {
            doc += (int) docDeltaIterator.next();
            final long rebasedBucket = rebase[(int) buckets.next()];
            if (rebasedBucket != -1) {
                docs[n] = doc;
                rebased[n++] = rebasedBucket;
                if (n == docs.length) {
                    leafCollector.collectBatch(docs, rebased, n);
                    n = 0;
                }
            }
        }
        if (n > 0) {
            leafCollector.collectBatch(docs, rebased, n);
        }
    }

    /**
     * Wrap the provided aggregator so that it behaves (almost) as if it had
     * been collected directly.
     */
    @Override
    public Aggregator wrap(final Aggregator in) {

        return new WrappedAggregator(in) {
            @Override
            public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
                if (selectedBuckets == null) {
                    throw new IllegalStateException("Collection has not been replayed yet.");
                }
                long[] rebasedOrds = new long[owningBucketOrds.length];
                for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                    rebasedOrds[ordIdx] = selectedBuckets.find(owningBucketOrds[ordIdx]);
                    if (rebasedOrds[ordIdx] == -1) {
                        throw new IllegalStateException("Cannot build for a bucket which has not been collected");
                    }
                }
                return in.buildAggregations(rebasedOrds);
            }
        };
    }

}
