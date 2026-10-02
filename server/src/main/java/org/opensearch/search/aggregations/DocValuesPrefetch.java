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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterDocIdSetIterator;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private static volatile boolean shareLookahead;
    private static volatile boolean leapfrogLookahead;
    private static volatile long nodeBytes = 128 * 1024;
    private static final LongAdder planners = new LongAdder();
    private static final LongAdder requests = new LongAdder();
    private static final LongAdder lookaheads = new LongAdder();
    private static final LongAdder leapfrogs = new LongAdder();
    private static final LongAdder sharedHits = new LongAdder();
    private static final LongAdder sharedMisses = new LongAdder();
    /**
     * Per search: a searcher over the same reader that caches the look-ahead's non-term clauses per segment. An entry
     * lives until its search is released ({@link #release}); a search context releases its entry when it closes.
     */
    private static final Map<Object, IndexSearcher> SHARED = new ConcurrentHashMap<>();

    private DocValuesPrefetch() {}

    /** Turns the planner on or off for leaf collectors created from now on. */
    public static void setEnabled(boolean on) {
        enabled = on;
    }

    /** Returns whether planners are created. */
    public static boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets whether the planners of one search share the look-ahead's expensive per-segment work: every clause of the query
     * that is not a term query (for example a points range, whose scorer intersects the BKD tree) is evaluated once per
     * segment and cached for the other planners of the same search, instead of once per planner.
     */
    public static void setShareLookahead(boolean on) {
        shareLookahead = on;
    }

    /** Returns whether look-ahead clauses are shared. */
    public static boolean isShareLookahead() {
        return shareLookahead;
    }

    /**
     * Sets whether a look-ahead over a pure conjunction is built as a leapfrog conjunction of its clauses: bit-set clauses
     * are advanced with {@code nextSetBit} instead of being tested doc by doc while another clause is iterated (Lucene's
     * bit-set conjunction walks its lead clause with {@code nextDoc}).
     */
    public static void setLeapfrogLookahead(boolean on) {
        leapfrogLookahead = on;
    }

    /** Returns whether look-aheads leapfrog. */
    public static boolean isLeapfrogLookahead() {
        return leapfrogLookahead;
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

    /** Look-ahead iterators built since the last {@link #resetCounters()}. */
    public static long lookaheads() {
        return lookaheads.sum();
    }

    /** Look-aheads built as a leapfrog conjunction since the last {@link #resetCounters()}. */
    public static long leapfrogs() {
        return leapfrogs.sum();
    }

    /** Look-ahead clauses served from the shared per-segment cache since the last {@link #resetCounters()}. */
    public static long sharedHits() {
        return sharedHits.sum();
    }

    /** Look-ahead clauses evaluated and put in the shared cache since the last {@link #resetCounters()}. */
    public static long sharedMisses() {
        return sharedMisses.sum();
    }

    /** Sets the counters to zero. */
    public static void resetCounters() {
        planners.reset();
        requests.reset();
        lookaheads.reset();
        leapfrogs.reset();
        sharedHits.reset();
        sharedMisses.reset();
    }

    private static IndexSearcher lookaheadSearcher(Object search, IndexSearcher base) {
        if (shareLookahead == false) {
            return base;
        }
        return SHARED.computeIfAbsent(search, c -> {
            final IndexSearcher searcher = new IndexSearcher(base.getIndexReader());
            searcher.setQueryCache(new SearchQueryCache());
            searcher.setQueryCachingPolicy(new QueryCachingPolicy() {
                @Override
                public void onUse(Query query) {}

                @Override
                public boolean shouldCache(Query query) {
                    // a term query's iterator is its postings: caching it would read them all up front
                    return (query instanceof TermQuery) == false && (query instanceof BooleanQuery) == false;
                }
            });
            return searcher;
        });
    }

    /** Drops the shared look-ahead work of {@code search}; later look-aheads with the same key start over. */
    public static void release(Object search) {
        SHARED.remove(search);
    }

    /** Searches holding shared look-ahead work (should be 0 when no search runs). */
    public static int sharedSearches() {
        return SHARED.size();
    }

    /**
     * Caches the docs of a clause per segment for one search. Unlike {@code LRUQueryCache} it registers no listener on
     * the segment readers, so nothing outlives the search once {@link #release} drops it.
     */
    private static final class SearchQueryCache implements QueryCache {
        private record Key(Query query, Object leaf) {
        }

        private final Map<Key, DocIdSet> sets = new ConcurrentHashMap<>();

        @Override
        public Weight doCache(Weight weight, QueryCachingPolicy policy) {
            try {
                if (policy.shouldCache(weight.getQuery()) == false) {
                    return weight;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return new ConstantScoreWeight(weight.getQuery(), 1f) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext ctx) throws IOException {
                    final DocIdSet set = docs(weight, ctx);
                    final DocIdSetIterator probe = set.iterator();
                    if (probe == null) {
                        return null;
                    }
                    final long cost = probe.cost();
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            return new ConstantScoreScorer(score(), ScoreMode.COMPLETE_NO_SCORES, set.iterator());
                        }

                        @Override
                        public long cost() {
                            return cost;
                        }
                    };
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false;
                }
            };
        }

        private DocIdSet docs(Weight weight, LeafReaderContext ctx) throws IOException {
            final Key key = new Key(weight.getQuery(), ctx.id());
            DocIdSet set = sets.get(key);
            if (set != null) {
                sharedHits.increment();
                return set;
            }
            sharedMisses.increment();
            set = build(weight, ctx);
            final DocIdSet raced = sets.putIfAbsent(key, set);
            return raced == null ? set : raced;
        }

        private static DocIdSet build(Weight weight, LeafReaderContext ctx) throws IOException {
            final ScorerSupplier supplier = weight.scorerSupplier(ctx);
            if (supplier == null) {
                return DocIdSet.EMPTY;
            }
            // lead cost "unbounded": the clause's own index structure, never a doc-values scan
            final DocIdSetIterator it = supplier.get(Long.MAX_VALUE).iterator();
            final int maxDoc = ctx.reader().maxDoc();
            if (it.cost() >= maxDoc >>> 7) {
                final FixedBitSet bits = new FixedBitSet(maxDoc);
                it.nextDoc();
                it.intoBitSet(DocIdSetIterator.NO_MORE_DOCS, bits, 0);
                return new BitDocIdSet(bits);
            }
            final RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(maxDoc);
            for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                builder.add(doc);
            }
            return builder.build();
        }
    }

    /** The FILTER / MUST clauses of a pure conjunction (no SHOULD, no MUST_NOT), unwrapping constant-score wrappers. */
    private static List<Query> conjunctionClauses(Query query) {
        while (true) {
            if (query instanceof ConstantScoreQuery csq) {
                query = csq.getQuery();
            } else if (query instanceof BoostQuery bq) {
                query = bq.getQuery();
            } else {
                break;
            }
        }
        if (query instanceof BooleanQuery == false) {
            return null;
        }
        final BooleanQuery bool = (BooleanQuery) query;
        final List<Query> clauses = new ArrayList<>();
        for (BooleanClause c : bool.clauses()) {
            if (c.occur() != BooleanClause.Occur.FILTER && c.occur() != BooleanClause.Occur.MUST) {
                return null;
            }
            clauses.add(c.query());
        }
        return clauses.size() >= 2 ? clauses : null;
    }

    /** Leapfrog conjunction of the clauses' iterators, or null when a clause is two-phase. */
    private static DocIdSetIterator leapfrog(IndexSearcher searcher, List<Query> clauses, LeafReaderContext ctx) throws IOException {
        final List<DocIdSetIterator> iterators = new ArrayList<>();
        for (Query clause : clauses) {
            final Weight weight = searcher.createWeight(searcher.rewrite(clause), ScoreMode.COMPLETE_NO_SCORES, 1f);
            final ScorerSupplier supplier = weight.scorerSupplier(ctx);
            if (supplier == null) {
                return DocIdSetIterator.empty();
            }
            // lead cost "unbounded": the clause's own index structure, never a doc-values scan, which would read values
            final Scorer scorer = supplier.get(Long.MAX_VALUE);
            if (scorer.twoPhaseIterator() != null) {
                return null;
            }
            final DocIdSetIterator it = scorer.iterator();
            // hide the bit-set type, so the conjunction advances it with nextSetBit instead of testing every lead doc
            iterators.add(it instanceof BitSetIterator ? new FilterDocIdSetIterator(it) : it);
        }
        return ConjunctionUtils.intersectIterators(iterators);
    }

    /**
     * The docs of a segment that match the search's query, as a new iterator that is only advanced (a second scorer of
     * the query), skipping deleted docs.
     */
    public static DocIdSetIterator queryMatches(SearchContext context, LeafReaderContext ctx) throws IOException {
        if (shareLookahead && SHARED.containsKey(context) == false) {
            // registered before the entry exists, so a search that closes never leaves one behind
            context.addReleasable(() -> release(context));
        }
        return queryMatches(context, context.searcher(), context.query(), ctx);
    }

    /**
     * Same as {@link #queryMatches(SearchContext, LeafReaderContext)}, for {@code query} on {@code base}; look-aheads
     * with the same {@code search} key share their per-segment work when {@link #isShareLookahead()}.
     */
    public static DocIdSetIterator queryMatches(Object search, IndexSearcher base, Query original, LeafReaderContext ctx)
        throws IOException {
        lookaheads.increment();
        final IndexSearcher searcher = lookaheadSearcher(search, base);
        final Query query = searcher.rewrite(original);
        DocIdSetIterator matches = null;
        if (leapfrogLookahead) {
            final List<Query> clauses = conjunctionClauses(query);
            if (clauses != null) {
                matches = leapfrog(searcher, clauses, ctx);
                if (matches != null) {
                    leapfrogs.increment();
                }
            }
        }
        if (matches == null) {
            final Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
            final Scorer scorer = weight.scorer(ctx);
            if (scorer == null) {
                return DocIdSetIterator.empty();
            }
            final TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
            matches = twoPhase == null ? scorer.iterator() : TwoPhaseIterator.asDocIdSetIterator(twoPhase);
        }
        final Bits live = ctx.reader().getLiveDocs();
        if (live == null) {
            return matches;
        }
        final DocIdSetIterator all = matches;
        return new FilteredDocIdSetIterator(all) {
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
