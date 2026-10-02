/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/** The look-ahead iterator must return exactly the query's matches, with any combination of its options. */
public class DocValuesPrefetchTests extends OpenSearchTestCase {

    @Override
    public void tearDown() throws Exception {
        DocValuesPrefetch.setShareLookahead(false);
        DocValuesPrefetch.setLeapfrogLookahead(false);
        super.tearDown();
    }

    private static Query[] queries(int maxDoc) {
        Query a = new TermQuery(new Term("tag", "a"));
        Query b = new TermQuery(new Term("tag", "b"));
        int lo = maxDoc / 3;
        Query range = LongPoint.newRangeQuery("p", lo, lo + maxDoc / 50);
        Query wide = LongPoint.newRangeQuery("p", 0, maxDoc);
        return new Query[] {
            and(a, b),
            and(a, range),
            and(range, a, b),
            and(a, wide),
            new ConstantScoreQuery(and(b, range)),
            range,
            a,
            new MatchAllDocsQuery() };
    }

    private static Query and(Query... clauses) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Query q : clauses) {
            builder.add(q, BooleanClause.Occur.FILTER);
        }
        return builder.build();
    }

    private static FixedBitSet expected(IndexSearcher searcher, Query query, LeafReaderContext ctx) throws IOException {
        FixedBitSet bits = new FixedBitSet(ctx.reader().maxDoc());
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        Scorer scorer = weight.scorer(ctx);
        if (scorer != null) {
            DocIdSetIterator it = scorer.iterator();
            for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                bits.set(doc);
            }
        }
        return bits;
    }

    public void testSameMatchesInEveryMode() throws IOException {
        try (Directory dir = newDirectory()) {
            int maxDoc = randomIntBetween(20_000, 80_000);
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(TestUtil.getDefaultCodec()))) {
                for (int i = 0; i < maxDoc; i++) {
                    Document doc = new Document();
                    doc.add(new LongPoint("p", i));
                    if (randomIntBetween(0, 9) < 5) {
                        doc.add(new StringField("tag", "a", Field.Store.NO));
                    }
                    if (randomIntBetween(0, 9) < 1) {
                        doc.add(new StringField("tag", "b", Field.Store.NO));
                    }
                    w.addDocument(doc);
                }
                w.forceMerge(1);
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                LeafReaderContext ctx = reader.leaves().get(0);
                for (boolean share : new boolean[] { false, true }) {
                    for (boolean leapfrog : new boolean[] { false, true }) {
                        DocValuesPrefetch.setShareLookahead(share);
                        DocValuesPrefetch.setLeapfrogLookahead(leapfrog);
                        DocValuesPrefetch.resetCounters();
                        Object search = new Object();
                        for (Query query : queries(maxDoc)) {
                            FixedBitSet want = expected(searcher, query, ctx);
                            // twice per search: the second look-ahead may reuse the first one's shared work
                            for (int round = 0; round < 2; round++) {
                                DocIdSetIterator it = DocValuesPrefetch.queryMatches(search, searcher, query, ctx);
                                // random advances, as the planners do, and every match in between with nextDoc
                                int doc = -1;
                                while (true) {
                                    int target = doc + 1 + (randomBoolean() ? 0 : randomIntBetween(0, 3000));
                                    int got = target >= maxDoc ? DocIdSetIterator.NO_MORE_DOCS : it.advance(target);
                                    int exp = target >= maxDoc ? DocIdSetIterator.NO_MORE_DOCS : want.nextSetBit(target);
                                    if (exp == DocIdSetIterator.NO_MORE_DOCS || exp >= maxDoc) {
                                        exp = DocIdSetIterator.NO_MORE_DOCS;
                                    }
                                    String desc = query + " share=" + share + " leapfrog=" + leapfrog + " target=" + target;
                                    assertEquals(desc, exp, got);
                                    if (got == DocIdSetIterator.NO_MORE_DOCS) {
                                        break;
                                    }
                                    doc = got;
                                }
                            }
                        }
                        if (leapfrog) {
                            assertTrue("leapfrog conjunctions built", DocValuesPrefetch.leapfrogs() > 0);
                        } else {
                            assertEquals(0, DocValuesPrefetch.leapfrogs());
                        }
                        if (share) {
                            assertTrue("shared range clauses reused", DocValuesPrefetch.sharedHits() > 0);
                        } else {
                            assertEquals(0, DocValuesPrefetch.sharedHits());
                        }
                        assertEquals(share ? 1 : 0, DocValuesPrefetch.sharedSearches());
                        DocValuesPrefetch.release(search);
                        assertEquals("released searches hold nothing", 0, DocValuesPrefetch.sharedSearches());
                    }
                }
            }
        }
    }
}
