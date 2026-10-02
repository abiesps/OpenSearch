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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.aggregations.DocValuesPrefetch;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.when;

public class BestBucketsDeferringCollectorTests extends AggregatorTestCase {

    public void testReplay() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        int numDocs = randomIntBetween(1, 128);
        int maxNumValues = randomInt(16);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("field", String.valueOf(randomInt(maxNumValues)), Field.Store.NO));
            indexWriter.addDocument(document);
        }

        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        TermQuery termQuery = new TermQuery(new Term("field", String.valueOf(randomInt(maxNumValues))));
        Query rewrittenQuery = indexSearcher.rewrite(termQuery);
        TopDocs topDocs = indexSearcher.search(termQuery, numDocs);

        SearchContext searchContext = createSearchContext(indexSearcher, createIndexSettings(), rewrittenQuery, null);
        when(searchContext.query()).thenReturn(rewrittenQuery);
        BestBucketsDeferringCollector collector = new BestBucketsDeferringCollector(searchContext, false) {
            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE;
            }
        };
        Set<Integer> deferredCollectedDocIds = new HashSet<>();
        collector.setDeferredCollector(Collections.singleton(bla(deferredCollectedDocIds)));
        collector.preCollection();
        indexSearcher.search(termQuery, collector);
        collector.postCollection();
        collector.prepareSelectedBuckets(0);

        assertEquals(topDocs.scoreDocs.length, deferredCollectedDocIds.size());
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            assertTrue("expected docid [" + scoreDoc.doc + "] is missing", deferredCollectedDocIds.contains(scoreDoc.doc));
        }

        topDocs = indexSearcher.search(new MatchAllDocsQuery(), numDocs);
        collector = new BestBucketsDeferringCollector(searchContext, true);
        deferredCollectedDocIds = new HashSet<>();
        collector.setDeferredCollector(Collections.singleton(bla(deferredCollectedDocIds)));
        collector.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), collector);
        collector.postCollection();
        collector.prepareSelectedBuckets(0);

        assertEquals(topDocs.scoreDocs.length, deferredCollectedDocIds.size());
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            assertTrue("expected docid [" + scoreDoc.doc + "] is missing", deferredCollectedDocIds.contains(scoreDoc.doc));
        }
        indexReader.close();
        directory.close();
    }

    private BucketCollector bla(Set<Integer> docIds) {
        return new BucketCollector() {
            @Override
            public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
                return new LeafBucketCollector() {
                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        docIds.add(ctx.docBase + doc);
                    }
                };
            }

            @Override
            public void preCollection() throws IOException {

            }

            @Override
            public void postCollection() throws IOException {

            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }
        };
    }

    /**
     * Run-ahead replay in pass-through: docs go straight to the collector while the read nodes are cached, then (at the
     * first read node that is not) the rest of the segment goes through the ring. Every selected doc is replayed once,
     * in order, with its rebased bucket, and every request is a read doc, one per node, before it is collected.
     */
    public void testReplayAheadPassThrough() throws IOException {
        DocValuesPrefetch.setEnabled(true);
        DocValuesPrefetch.setRunAhead(true);
        DocValuesPrefetch.setRunAheadBypass(true);
        try {
            for (int iter = 0; iter < 20; iter++) {
                DocValuesPrefetch.setRunAheadDocs(randomFrom(4096, 65_536, 1 << 17));
                final int maxDoc = randomIntBetween(1, 1_000_000);
                final int numBuckets = randomIntBetween(1, 20);
                final long[] rebase = new long[numBuckets];
                for (int b = 0; b < numBuckets; b++) {
                    rebase[b] = randomBoolean() ? -1 : randomIntBetween(0, 100);
                }
                final PackedLongValues.Builder deltas = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
                final PackedLongValues.Builder bucketsBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
                final List<Integer> expectedDocs = new ArrayList<>();
                final List<Long> expectedBuckets = new ArrayList<>();
                final int every = randomIntBetween(1, 50);
                int last = 0;
                for (int doc = randomIntBetween(0, every); doc < maxDoc; doc += randomIntBetween(1, every)) {
                    final int bucket = randomIntBetween(0, numBuckets - 1);
                    deltas.add(doc - last);
                    bucketsBuilder.add(bucket);
                    last = doc;
                    if (rebase[bucket] != -1) {
                        expectedDocs.add(doc);
                        expectedBuckets.add(rebase[bucket]);
                    }
                }
                final PackedLongValues docDeltas = deltas.build();
                final PackedLongValues buckets = bucketsBuilder.build();
                final int nodeDocs = randomIntBetween(500, 200_000);
                final int loadedUpTo = randomFrom(0, randomIntBetween(0, maxDoc), maxDoc + 1);
                final List<Integer> requested = new ArrayList<>();
                final List<Integer> collectedBefore = new ArrayList<>();
                final List<Integer> delivered = new ArrayList<>();
                final List<Long> deliveredBuckets = new ArrayList<>();
                final DocValuesPrefetch.Field field = new DocValuesPrefetch.Field() {
                    @Override
                    public int nextNodeDoc(int doc, long nodeBytes) {
                        final long next = ((long) doc / nodeDocs + 1) * nodeDocs;
                        return next >= maxDoc ? DocIdSetIterator.NO_MORE_DOCS : (int) next;
                    }

                    @Override
                    public void prefetch(int doc, long nodeBytes) {
                        requested.add(doc);
                        collectedBefore.add(delivered.size());
                    }

                    @Override
                    public boolean isLoaded(int doc, long nodeBytes) {
                        return doc < loadedUpTo;
                    }
                };
                final DocValuesPrefetch.Replay replay = DocValuesPrefetch.beginReplay();
                assertNotNull(replay);
                final DocValuesPrefetch.Planner planner;
                try {
                    planner = DocValuesPrefetch.planner(field, replay, DocValuesPrefetch.ALL_MATCHES);
                } finally {
                    DocValuesPrefetch.clear(replay);
                }
                assertTrue(replay.isPassThrough());
                final LeafBucketCollector out = new LeafBucketCollector() {
                    @Override
                    public void collect(int doc, long owningBucketOrd) {
                        throw new AssertionError("batch expected");
                    }

                    @Override
                    public void collectBatch(int[] docs, long[] rebased, int count) throws IOException {
                        planner.advance(docs[0]);
                        planner.advance(docs[count - 1]);
                        for (int i = 0; i < count; i++) {
                            delivered.add(docs[i]);
                            deliveredBuckets.add(rebased[i]);
                        }
                    }
                };
                BestBucketsDeferringCollector.replayAhead(out, docDeltas.size(), docDeltas.iterator(), buckets.iterator(), rebase, replay);
                assertEquals(expectedDocs, delivered);
                assertEquals(expectedBuckets, deliveredBuckets);
                if (loadedUpTo > maxDoc) {
                    assertTrue("pass-through to the end", replay.isPassThrough());
                    assertTrue("nothing requested while cached", requested.isEmpty());
                }
                int lastNode = -1;
                for (int k = 0; k < requested.size(); k++) {
                    final int doc = requested.get(k);
                    final int rank = Collections.binarySearch(expectedDocs, doc);
                    assertTrue("requested doc " + doc + " is replayed", rank >= 0);
                    assertTrue("one request per node, in order", doc / nodeDocs > lastNode);
                    lastNode = doc / nodeDocs;
                    assertTrue("doc " + doc + " requested after it was collected", collectedBefore.get(k) <= rank);
                }
            }
        } finally {
            DocValuesPrefetch.setEnabled(false);
            DocValuesPrefetch.setRunAhead(false);
            DocValuesPrefetch.setRunAheadBypass(false);
        }
    }
}
