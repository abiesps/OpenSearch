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
import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.FilterDocIdSetIterator;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Scorable;
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
import java.util.Arrays;
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
    private static volatile boolean runAhead;
    private static volatile int runAheadDocs = 1 << 17;
    private static volatile boolean runAheadGate;
    private static volatile boolean runAheadBypass;
    private static final LongAdder runAheadSwitches = new LongAdder();
    private static final LongAdder runAheadLeaves = new LongAdder();
    private static final LongAdder runAheadReplays = new LongAdder();
    /** The run-ahead buffer of the leaf collector tree being built on this thread, see {@link #beginLeaf()}. */
    private static final ThreadLocal<Ahead> CURRENT = new ThreadLocal<>();
    private static final LongAdder planners = new LongAdder();
    private static final LongAdder requests = new LongAdder();
    private static final LongAdder lookaheads = new LongAdder();
    private static final LongAdder leapfrogs = new LongAdder();
    private static final LongAdder sharedHits = new LongAdder();
    private static final LongAdder sharedMisses = new LongAdder();
    /** Sort planners ({@link #sortPlanner}) and their node requests, counted apart from the aggregation planners. */
    private static final LongAdder sortPlanners = new LongAdder();
    private static final LongAdder sortRequests = new LongAdder();
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

    /**
     * Sets whether planners take their proof of a read from the main scorer's own matches (a run-ahead buffer between the
     * scorer and the aggregation collectors) instead of a second scorer of the query.
     */
    public static void setRunAhead(boolean on) {
        runAhead = on;
    }

    /** Returns whether planners use the run-ahead buffer. */
    public static boolean isRunAhead() {
        return runAhead;
    }

    /**
     * Sets whether the run-ahead buffer holds collection before a planner's next requested doc until the read after
     * that doc's node is known (up to the buffer's capacity), so a field read far apart (date_histogram reads values
     * only in skipper intervals that span two buckets) still gets its request one node ahead in time.
     */
    public static void setRunAheadGate(boolean on) {
        runAheadGate = on;
    }

    /**
     * Sets whether the run-ahead buffer starts as a pass-through: docs go straight to the collectors (no copy, no delay)
     * while every node a planner's collector reads from is cached. At the first read node that is not cached the buffer
     * starts buffering, and stays so for the leaf.
     */
    public static void setRunAheadBypass(boolean on) {
        runAheadBypass = on;
    }

    /** Returns whether the run-ahead buffer passes docs through while the data is cached. */
    public static boolean isRunAheadBypass() {
        return runAheadBypass;
    }

    /** Leaves (and replays) whose run-ahead buffer started buffering after passing docs through. */
    public static long runAheadSwitches() {
        return runAheadSwitches.sum();
    }

    /** Returns whether the run-ahead buffer gates collection on the planners. */
    public static boolean isRunAheadGate() {
        return runAheadGate;
    }

    /** Sets how many doc IDs the run-ahead buffer keeps between the scorer and the collectors. */
    public static void setRunAheadDocs(int docs) {
        if (docs < 4096) {
            throw new IllegalArgumentException("docs must be >= 4096, got " + docs);
        }
        runAheadDocs = docs;
    }

    /** Returns how many doc IDs the run-ahead buffer keeps between the scorer and the collectors. */
    public static int runAheadDocs() {
        return runAheadDocs;
    }

    /** Leaf collectors wrapped with a run-ahead buffer since the last {@link #resetCounters()}. */
    public static long runAheadLeaves() {
        return runAheadLeaves.sum();
    }

    /** Deferred replays run through a run-ahead ring since the last {@link #resetCounters()}. */
    public static long runAheadReplays() {
        return runAheadReplays.sum();
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

    /** Sort planners created since the last {@link #resetCounters()}. */
    public static long sortPlanners() {
        return sortPlanners.sum();
    }

    /** Node requests of sort planners since the last {@link #resetCounters()}. */
    public static long sortRequests() {
        return sortRequests.sum();
    }

    /** Sets the counters to zero. */
    public static void resetCounters() {
        planners.reset();
        requests.reset();
        sortPlanners.reset();
        sortRequests.reset();
        lookaheads.reset();
        leapfrogs.reset();
        sharedHits.reset();
        sharedMisses.reset();
        runAheadLeaves.reset();
        runAheadReplays.reset();
        runAheadSwitches.reset();
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
    /**
     * The matches a planner of this leaf collector takes its proof of a read from: in run-ahead mode the run-ahead buffer
     * of the leaf collector tree being built (null outside {@link #beginLeaf()} / {@link #beginReplay()}), otherwise a
     * look-ahead scorer of the query ({@link #queryMatches(SearchContext, LeafReaderContext)}).
     */
    public static Matches matches(SearchContext context, LeafReaderContext ctx) throws IOException {
        if (enabled == false) {
            return null;
        }
        if (runAhead) {
            return CURRENT.get();
        }
        final DocIdSetIterator it = queryMatches(context, ctx);
        return target -> advanceTo(target, it);
    }

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

        /** Whether the node(s) holding {@code doc}'s value are cached, so reading it needs no IO (false if unknown). */
        default boolean isLoaded(int doc, long nodeBytes) throws IOException {
            return false;
        }
    }

    /** Planning view of a numeric field, or null if its codec does not support node planning. */
    public static Field of(NumericDocValues values) throws IOException {
        return of(values, nodeBytes);
    }

    /** Same as {@link #of(NumericDocValues)}, checking support with nodes of {@code nodeBytes} bytes. */
    public static Field of(NumericDocValues values, long nodeBytes) throws IOException {
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

            @Override
            public boolean isLoaded(int doc, long nodeBytes) throws IOException {
                return values.isNodeLoaded(doc, nodeBytes);
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

            @Override
            public boolean isLoaded(int doc, long nodeBytes) throws IOException {
                return values.isNodeLoaded(doc, nodeBytes);
            }
        };
    }

    /** The query's matches as a planner sees them: the first match at or after a target, if it is known yet. */
    public interface Matches {
        /** Returned by {@link #next} when it is not known yet whether a doc at or after the target matches. */
        int UNKNOWN = -1;

        /** The first match at or after {@code target}, NO_MORE_DOCS, or {@link #UNKNOWN}. */
        int next(int target) throws IOException;

        /**
         * Right after {@link #next} returned {@link #UNKNOWN}: the doc to search from when more matches are known (no
         * doc before it, at or after the target, matches).
         */
        default int resumeFrom() {
            throw new UnsupportedOperationException();
        }
    }

    /** Narrows query matches to the docs whose values the collector reads. */
    public interface ReadFilter {
        /**
         * The first doc at or after {@code target} that matches and whose value is read, NO_MORE_DOCS, or
         * {@link Matches#UNKNOWN} when {@code matches} does not know yet.
         */
        int nextRead(int target, Matches matches) throws IOException;
    }

    /** Every match is read. */
    public static final ReadFilter ALL_MATCHES = (target, matches) -> matches.next(target);

    /** The first doc of {@code it} at or after {@code target}; never moves the iterator backwards. */
    public static int advanceTo(int target, DocIdSetIterator it) throws IOException {
        final int doc = it.docID();
        return doc >= target ? doc : it.advance(target);
    }

    /**
     * Creates a planner and requests the node of the first doc that will be read (or waits until it is known), or
     * returns null when planning is off or the field does not support it.
     */
    public static Planner planner(Field field, Matches matches, ReadFilter filter) throws IOException {
        if (enabled == false || field == null || matches == null) {
            return null;
        }
        final Planner p = new Planner(field, matches, filter, nodeBytes, requests);
        planners.increment();
        if (matches instanceof Ahead ahead) {
            p.ahead = ahead;
            ahead.planners.add(p);
        }
        p.start();
        return p;
    }

    /**
     * A run-ahead buffer for the sort comparator ({@link RunAhead#wrapLeafCollector}): no gate, pass-through while the
     * nodes read are cached, bit set allocated when buffering starts. Ignores every aggregation switch.
     */
    public static RunAhead sortRunAhead(int lag) {
        return new RunAhead(lag, false, true, true);
    }

    /**
     * Creates a planner of the sort comparator's reads over the matches of {@code ra}, or returns null when the field
     * does not support planning. Same as {@link #planner} without the {@link #isEnabled()} check, counted in
     * {@link #sortPlanners()} and {@link #sortRequests()}.
     */
    public static Planner sortPlanner(Field field, RunAhead ra, ReadFilter filter, long nodeBytes) throws IOException {
        if (field == null || ra == null) {
            return null;
        }
        final Planner p = new Planner(field, ra, filter, nodeBytes, sortRequests);
        p.entryCheck = true;
        sortPlanners.increment();
        p.ahead = ra;
        ra.planners.add(p);
        p.start();
        return p;
    }

    /** Keeps one node of a field requested ahead of the node being read. */
    public static final class Planner {
        private final Field field;
        private final Matches matches;
        private final ReadFilter filter;
        private final long nodeBytes;
        /** Counts this planner's node requests. */
        private final LongAdder requestCounter;
        private Ahead ahead;
        /** When a read doc reaches it, the doc is in a node not planned from yet: request the following one. */
        private int trigger;
        /** When >= 0, the next read is not known yet: plan from here when more matches are known. */
        private int pendingFrom = -1;
        /**
         * Searches for the next read only move forward: no read is needed below {@code cursor} (at or after the last
         * target, every doc below it was checked), and {@code found} is the first read at or after it, if known (-1
         * otherwise). Searching again from a doc the filter already passed could see a doc in an interval it skipped.
         */
        private int cursor;
        private int found = -1;
        /** First doc of the node after {@link #trigger}'s, once computed for the gate (-1: not yet). */
        private int gateTarget = -1;
        /**
         * Sort planners in pass-through: whether the node starting at {@link #trigger} was cached when collection entered
         * the node before it. The last reads of a node touch a few bytes of the next one, so by the time collection
         * reaches the trigger that node can be loaded by those reads even when the data is cold.
         */
        private boolean entryCheck;
        private boolean entryLoaded = true;

        private Planner(Field field, Matches matches, ReadFilter filter, long nodeBytes, LongAdder requestCounter) {
            this.field = field;
            this.matches = matches;
            this.filter = filter;
            this.nodeBytes = nodeBytes;
            this.requestCounter = requestCounter;
        }

        /** The doc at which {@link #advance} plans again: a read at or after it needs a call, a read before it not. */
        public int trigger() {
            return trigger;
        }

        private void start() throws IOException {
            if (ahead != null && ahead.bypass) {
                trigger = 0; // check the cache at the first read
                return;
            }
            plan(0);
        }

        /** The first read at or after {@code from}, NO_MORE_DOCS, or {@link Matches#UNKNOWN}. */
        private int find(int from) throws IOException {
            if (found != -1 && found >= from) {
                return found;
            }
            found = -1;
            cursor = Math.max(cursor, from);
            final int next = filter.nextRead(cursor, matches);
            if (next == Matches.UNKNOWN) {
                cursor = Math.max(cursor, matches.resumeFrom());
                return next;
            }
            found = next;
            return next;
        }

        /** Requests the node of the first doc at or after {@code from} that will be read, once it is known. */
        private void plan(int from) throws IOException {
            final int next = find(from);
            if (next == Matches.UNKNOWN) {
                pendingFrom = from;
                trigger = DocIdSetIterator.NO_MORE_DOCS;
                ahead.pending(cursor);
                return;
            }
            pendingFrom = -1;
            if (next == DocIdSetIterator.NO_MORE_DOCS) {
                trigger = DocIdSetIterator.NO_MORE_DOCS;
                return;
            }
            field.prefetch(next, nodeBytes);
            requestCounter.increment();
            trigger = next;
            gateTarget = -1;
        }

        /**
         * Pass-through: while each node collection reads from is cached nothing is planned. At the first read node that
         * is not cached (its read is a miss either way) the buffer starts buffering, and this planner plans from the
         * next node with the matches that arrive from now on. The node checked is the one being read, not the next one
         * by position: a field read far apart (date_histogram) never reads, and so never caches, most nodes.
         */
        private void bypassAdvance(int doc) throws IOException {
            final int nodeEnd = field.nextNodeDoc(doc, nodeBytes);
            final boolean last = nodeEnd == DocIdSetIterator.NO_MORE_DOCS || nodeEnd < 0;
            boolean loaded = field.isLoaded(doc, nodeBytes);
            if (loaded && entryCheck && entryLoaded == false && (doc == trigger || field.nextNodeDoc(trigger, nodeBytes) == nodeEnd)) {
                // this node was not cached when collection entered the previous one: the reads at the end of that node
                // (which touch a few bytes past it) loaded it
                loaded = false;
            }
            if (loaded) {
                trigger = last ? DocIdSetIterator.NO_MORE_DOCS : nodeEnd;
                if (entryCheck) {
                    entryLoaded = last || field.isLoaded(nodeEnd, nodeBytes);
                }
                return;
            }
            ahead.startBuffering();
            if (last) {
                trigger = DocIdSetIterator.NO_MORE_DOCS;
                return;
            }
            plan(nodeEnd);
        }

        /** More matches are known: plan if waiting for them. */
        private void retry() throws IOException {
            if (pendingFrom >= 0) {
                plan(pendingFrom);
            }
        }

        /**
         * Whether collection may reach {@link #trigger}: the read after the trigger's node is known (or there is none),
         * so the planner can request it as soon as collection gets there.
         */
        private boolean gateOpen() throws IOException {
            if (trigger == DocIdSetIterator.NO_MORE_DOCS) {
                return true;
            }
            if (gateTarget < 0) {
                final int nodeEnd = field.nextNodeDoc(trigger, nodeBytes);
                gateTarget = nodeEnd < 0 ? DocIdSetIterator.NO_MORE_DOCS : nodeEnd;
            }
            if (gateTarget == DocIdSetIterator.NO_MORE_DOCS) {
                return true;
            }
            return find(gateTarget) != Matches.UNKNOWN;
        }

        /**
         * Called with a doc whose value is about to be read (or the last doc of a batch about to be read). If it is in a
         * node not planned from yet, requests the node of the next doc that will be read after that node.
         */
        public void advance(int doc) throws IOException {
            if (doc < trigger) {
                return;
            }
            if (ahead != null && ahead.bypass) {
                bypassAdvance(doc);
                return;
            }
            final int nodeEnd = field.nextNodeDoc(doc, nodeBytes);
            if (nodeEnd == DocIdSetIterator.NO_MORE_DOCS || nodeEnd < 0) {
                trigger = DocIdSetIterator.NO_MORE_DOCS;
                return;
            }
            plan(nodeEnd);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Run-ahead: the main scorer's matches, seen ahead of collection
    // ---------------------------------------------------------------------------------------------------------------

    /** Matches known ahead of collection, and the planners waiting on them. */
    abstract static class Ahead implements Matches {
        final List<Planner> planners = new ArrayList<>(2);
        /** Pass-through while the data is cached, see {@link #setRunAheadBypass}. */
        boolean bypass;

        /** Leaves pass-through: docs that arrive from now on are buffered. */
        void startBuffering() {
            if (bypass) {
                bypass = false;
                runAheadSwitches.increment();
            }
        }

        /** Docs below it are known: matches are in the buffer, other docs do not match. */
        int arrived;
        boolean finished;
        /** Smallest {@code pendingFrom} of a waiting planner, or MAX_VALUE. */
        private int retryAt = Integer.MAX_VALUE;
        private int lastTarget;

        void pending(int from) {
            retryAt = Math.min(retryAt, from);
        }

        /** Smallest doc at which a waiting planner must retry, or MAX_VALUE. */
        final int retryAt() {
            return retryAt;
        }

        /** Lets waiting planners plan if matches at or after their resume doc arrived. */
        final void afterArrival() throws IOException {
            if (arrived > retryAt) {
                retryAt = Integer.MAX_VALUE;
                for (Planner p : planners) {
                    p.retry();
                }
            }
        }

        /** No more matches will arrive. */
        final void finishArrivals() throws IOException {
            finished = true;
            retryAt = 0;
            arrived = Integer.MAX_VALUE;
            afterArrival();
        }

        /** First buffered match in [target, arrived), or NO_MORE_DOCS. */
        abstract int nextBuffered(int target);

        @Override
        public final int next(int target) {
            final int doc = target < arrived ? nextBuffered(target) : DocIdSetIterator.NO_MORE_DOCS;
            if (doc != DocIdSetIterator.NO_MORE_DOCS || finished) {
                return doc;
            }
            lastTarget = target;
            return UNKNOWN;
        }

        @Override
        public final int resumeFrom() {
            return Math.max(lastTarget, arrived);
        }
    }

    /**
     * Starts building the leaf collector tree of a top-level aggregation in run-ahead mode: planners created until
     * {@link #endLeaf} take their matches from a buffer that sits between the scorer and the tree. Returns null when
     * run-ahead is off (or a tree is already being built on this thread).
     */
    public static RunAhead beginLeaf() {
        if (enabled == false || runAhead == false || CURRENT.get() != null) {
            return null;
        }
        final RunAhead ra = new RunAhead(runAheadDocs, runAheadGate, runAheadBypass);
        CURRENT.set(ra);
        return ra;
    }

    /** The tree's leaf collector: wrapped with the run-ahead buffer if a planner uses it, unchanged otherwise. */
    public static LeafBucketCollector endLeaf(RunAhead ra, LeafBucketCollector leaf) {
        if (ra == null || ra.planners.isEmpty() || leaf == LeafBucketCollector.NO_OP_COLLECTOR) {
            return leaf;
        }
        runAheadLeaves.increment();
        return ra.wrap(leaf);
    }

    /** Ends {@link #beginLeaf()} or {@link #beginReplay()}; call in a finally block. */
    public static void clear(Ahead ahead) {
        if (ahead != null && CURRENT.get() == ahead) {
            CURRENT.remove();
        }
    }

    /**
     * Buffers the scorer's matches of one leaf and hands them to the aggregation collectors {@code lag} doc IDs later,
     * so planners know the matches up to one node ahead of collection. The scorer's windows are copied by words
     * ({@link DocIdStream#intoBitSet}); no query work is repeated.
     */
    public static final class RunAhead extends Ahead {
        private static final int WINDOW = 4096;
        private final int lag;
        /** The buffer; a sort run-ahead allocates it at its first {@code startAt} (null while it passes docs through). */
        private FixedBitSet bits;
        private long[] words;
        private final int capacity;
        /** A sort run-ahead ({@link #sortRunAhead}): lazy bit set, switches not counted with the aggregation ones. */
        private final boolean sort;
        /** Doc ID of bit 0, a multiple of 64. */
        private int base;
        /** Docs below it were handed to the collectors. */
        private int delivered;
        /** Highest doc ID set in the buffer, or -1. */
        private int lastSet = -1;
        private LeafBucketCollector out;
        /** Set by {@link #wrapLeafCollector}: buffered docs go to a plain leaf collector instead of {@link #out}. */
        private PlainSink plain;
        private final BufferStream stream = new BufferStream();
        private final int[] first = new int[1];

        private final boolean gate;

        RunAhead(int lag) {
            this(lag, false);
        }

        RunAhead(int lag, boolean gate) {
            this(lag, gate, false);
        }

        RunAhead(int lag, boolean gate, boolean bypass) {
            this(lag, gate, bypass, false);
        }

        RunAhead(int lag, boolean gate, boolean bypass, boolean sort) {
            this.lag = lag;
            this.gate = gate;
            this.bypass = bypass;
            this.sort = sort;
            this.buffering = bypass == false;
            // with the gate, delivery can wait at a planner's trigger while the scorer runs further ahead
            this.capacity = ((gate ? 4 : 2) * lag + 4 * WINDOW + 63) & ~63;
            if (sort == false) {
                this.bits = new FixedBitSet(capacity);
                this.words = bits.getBits();
            }
        }

        /** The buffer's bit set, null if not allocated. For tests. */
        FixedBitSet bitSetForTest() {
            return bits;
        }

        @Override
        int nextBuffered(int target) {
            final int from = Math.max(target, base);
            final int to = Math.min(arrived, base + capacity);
            if (from >= to || lastSet < from) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            final int i = bits.nextSetBit(from - base, to - base);
            return i == DocIdSetIterator.NO_MORE_DOCS ? i : base + i;
        }

        // Arrival runs: the scorer hands docs one at a time (PER_DOC), as streams (STREAM) or as ranges of matching docs
        // (RANGE). They are handed on in the same form, so the collectors run the same code path as without the buffer.
        private static final byte STREAM = 0, PER_DOC = 1, RANGE = 2;
        private static final int RUNS = 1024;
        private final int[] runEnd = new int[RUNS];
        /** First doc of a RANGE run (every doc from it to the run's end matches). */
        private final int[] runStart = new int[RUNS];
        private final byte[] runKind = new byte[RUNS];
        private int runHead, runCount;
        /** Docs that arrived one at a time, in order. */
        private static final int RING = 1 << 14;
        private final int[] ring = new int[RING];
        private int ringHead, ringCount;

        /** Starts a run of {@code kind} unless the current run has that kind (ranges always start a run). */
        private void startRun(byte kind) throws IOException {
            if (kind != PER_DOC) {
                fastUpTo = 0;
            }
            if (runCount > 0) {
                endRun();
            }
            if (kind != RANGE && runCount > 0 && runKind[(runHead + runCount - 1) & (RUNS - 1)] == kind) {
                return;
            }
            if (runCount == RUNS) {
                deliver(arrived); // frees every closed run
            }
            final int r = (runHead + runCount) & (RUNS - 1);
            runKind[r] = kind;
            runEnd[r] = arrived;
            runStart[r] = arrived;
            runCount++;
        }

        /** The current run covers every doc below {@link #arrived}. */
        private void endRun() {
            runEnd[(runHead + runCount - 1) & (RUNS - 1)] = arrived;
        }

        /**
         * Single docs below it need no bookkeeping: no planner waits on them, no delivery is due, the buffer has room
         * (0 when the current run is not a single-doc run). The end of the current run is then updated lazily.
         */
        private int fastUpTo;

        @Override
        void pending(int from) {
            super.pending(from);
            fastUpTo = 0;
        }

        private void arriveDoc(int doc) throws IOException {
            if (doc < fastUpTo && ringCount < RING) {
                final int i = doc - base;
                words[i >> 6] |= 1L << i;
                lastSet = doc;
                arrived = doc + 1;
                ring[(ringHead + ringCount++) & (RING - 1)] = doc;
                return;
            }
            arriveDocSlow(doc);
            fastUpTo = (int) Math.min(Math.min((long) retryAt(), (long) delivered + lag + WINDOW - 1), (long) base + capacity - WINDOW);
        }

        /** Set when pass-through ends: the next arrival starts the buffer at its first doc. */
        private boolean startPending;
        /** Whether docs were buffered (so finish must hand them over). */
        private boolean buffering;

        @Override
        void startBuffering() {
            if (bypass) {
                if (sort) {
                    bypass = false;
                } else {
                    super.startBuffering();
                }
                startPending = true;
            }
        }

        /** Starts the buffer at {@code doc}: every earlier doc went straight to the collectors. */
        private void startAt(int doc) {
            if (bits == null) {
                bits = new FixedBitSet(capacity);
                words = bits.getBits();
            }
            startPending = false;
            buffering = true;
            base = doc & ~63;
            delivered = doc;
            arrived = doc;
        }

        private void arriveDocSlow(int doc) throws IOException {
            if (startPending) {
                startAt(doc);
            }
            assert bits != null : "arrival before the buffer started";
            startRun(PER_DOC);
            if (ringCount == RING) {
                endRun();
                deliver(arrived);
            }
            if (doc - base >= capacity - WINDOW) {
                makeRoom(doc);
            }
            final int i = doc - base;
            words[i >> 6] |= 1L << i;
            lastSet = doc;
            arrived = doc + 1;
            ring[(ringHead + ringCount++) & (RING - 1)] = doc;
            endRun();
            afterArrival();
            deliverReady();
        }

        private void arriveStream(DocIdStream s) throws IOException {
            // the first doc tells where the window starts, which may be far after the buffer
            if (s.intoArray(first) == 0) {
                return;
            }
            arriveStream(first[0], s);
        }

        /** The arrival of {@code doc} and then the rest of {@code s}. */
        private void arriveStream(int doc, DocIdStream s) throws IOException {
            if (startPending) {
                startAt(doc);
            }
            assert bits != null : "arrival before the buffer started";
            startRun(STREAM);
            arriveDoc0(doc);
            while (s.mayHaveRemaining()) {
                final int end = base + capacity;
                final int last = s.intoBitSet(end, bits, base);
                if (last > 0) {
                    lastSet = last - 1;
                    arrived = Math.max(arrived, last);
                }
                if (s.mayHaveRemaining() == false) {
                    break;
                }
                // matches remain at or after end, so every doc below end is known
                arrived = end;
                endRun();
                afterArrival();
                makeRoom(end);
            }
            endRun();
            afterArrival();
            deliverReady();
        }

        /** Sets {@code doc}, making room first; no delivery. */
        private void arriveDoc0(int doc) throws IOException {
            if (doc - base >= capacity - WINDOW) {
                makeRoom(doc);
            }
            final int i = doc - base;
            words[i >> 6] |= 1L << i;
            lastSet = doc;
            arrived = doc + 1;
            endRun();
        }

        private void arriveRange(int min, int max) throws IOException {
            if (startPending) {
                startAt(min);
            }
            assert bits != null : "arrival before the buffer started";
            // a new run per range: delivery hands each run on as one range
            startRun(RANGE);
            runStart[(runHead + runCount - 1) & (RUNS - 1)] = min;
            int from = min;
            while (from < max) {
                if (from - base >= capacity - WINDOW) {
                    makeRoom(from);
                }
                final int to = Math.min(max, base + capacity);
                bits.set(from - base, to - base);
                lastSet = to - 1;
                arrived = to;
                endRun();
                afterArrival();
                from = to;
            }
            deliverReady();
        }

        /** Delivers what is {@link #lag} doc IDs behind the scorer (and, with the gate, not past a closed gate). */
        private void deliverReady() throws IOException {
            int to = arrived - lag;
            if (gate) {
                for (Planner p : planners) {
                    if (p.trigger < to && p.gateOpen() == false) {
                        to = p.trigger;
                    }
                }
            }
            if (to - delivered >= WINDOW) {
                deliver(to);
            }
        }

        /** Every doc below {@code doc} is known: deliver up to {@code doc - lag} and move the buffer so it has room. */
        private void makeRoom(int doc) throws IOException {
            assert bits != null : "room made before the buffer started";
            arrived = Math.max(arrived, doc);
            endRun();
            afterArrival();
            deliver(doc - lag);
            final int newBase = delivered & ~63;
            if (newBase <= base) {
                return;
            }
            final int shiftWords = (newBase - base) >> 6;
            final int usedWords = Math.min(words.length, (Math.max(lastSet, base) - base + 64) >> 6);
            if (shiftWords >= usedWords) {
                Arrays.fill(words, 0, usedWords, 0L);
            } else {
                System.arraycopy(words, shiftWords, words, 0, usedWords - shiftWords);
                Arrays.fill(words, usedWords - shiftWords, usedWords, 0L);
            }
            base = newBase;
        }

        /** Hands the buffered matches below {@code to} to the collectors, run by run, in their arrival form. */
        private void deliver(int to) throws IOException {
            to = Math.min(to, arrived);
            while (delivered < to && runCount > 0) {
                final int r = runHead;
                final int end = Math.min(runEnd[r], to);
                if (end > delivered) {
                    if (runKind[r] == RANGE) {
                        // every buffered doc of a range run matches; the run starts at its first doc
                        final int lo = Math.max(delivered, runStart[r]);
                        final int hi = Math.min(end, lastSet + 1);
                        if (hi > lo) {
                            if (plain == null) {
                                out.collectRange(lo, hi);
                            } else {
                                plain.range(lo, hi, false);
                            }
                        }
                    } else if (runKind[r] == STREAM) {
                        final int hi = Math.min(end, lastSet + 1);
                        if (hi > delivered) {
                            stream.reset(delivered, hi);
                            // planners advanced by the collectors may look at buffered docs not delivered yet: keep
                            // delivered unchanged until the stream is consumed
                            if (plain == null) {
                                out.collect(stream, 0);
                            } else {
                                plain.stream(stream, false);
                            }
                        }
                    } else {
                        while (ringCount > 0 && ring[ringHead] < end) {
                            final int doc = ring[ringHead];
                            ringHead = (ringHead + 1) & (RING - 1);
                            ringCount--;
                            if (plain == null) {
                                out.collect(doc, 0);
                            } else {
                                plain.doc(doc);
                            }
                        }
                    }
                    delivered = end;
                }
                if (runEnd[r] <= delivered && runCount > 1) {
                    runHead = (runHead + 1) & (RUNS - 1);
                    runCount--;
                } else {
                    break;
                }
            }
            delivered = Math.max(delivered, to);
        }

        private void finishLeaf() throws IOException {
            if (buffering == false) {
                return;
            }
            fastUpTo = 0;
            if (runCount > 0) {
                endRun();
            }
            finishArrivals();
            if (runCount > 0) {
                endRun();
            }
            deliver(Integer.MAX_VALUE);
        }

        LeafBucketCollector wrap(LeafBucketCollector delegate) {
            this.out = delegate;
            return new LeafBucketCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    out.setScorer(scorer);
                }

                @Override
                public void collect(int doc) throws IOException {
                    if (bypass) {
                        out.collect(doc, 0);
                        return;
                    }
                    // the scorer's per-doc call, without the hop through collect(doc, 0)
                    arriveDoc(doc);
                }

                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (bypass || owningBucketOrd != 0) {
                        // not a top-level call: hand over everything buffered, then this doc, in order
                        finishFlush();
                        out.collect(doc, owningBucketOrd);
                        return;
                    }
                    arriveDoc(doc);
                }

                @Override
                public void collect(DocIdStream s, long owningBucketOrd) throws IOException {
                    if (bypass || owningBucketOrd != 0) {
                        finishFlush();
                        out.collect(s, owningBucketOrd);
                        return;
                    }
                    arriveStream(s);
                }

                @Override
                public void collectRange(int min, int max) throws IOException {
                    if (bypass) {
                        out.collectRange(min, max);
                        return;
                    }
                    arriveRange(min, max);
                }

                @Override
                public void finish() throws IOException {
                    finishLeaf();
                    out.finish();
                }
            };
        }

        /**
         * Wraps a plain leaf collector (the sort comparator's) that does not advance {@code p} itself: the buffer
         * advances it at its trigger, before the doc there is collected. Docs reach {@code delegate} in their arrival
         * form, split only at triggers. The leaf starts in pass-through; when {@code p} finds a read node that is not
         * cached, the rest of the leaf is buffered, starting with the rest of the current arrival.
         */
        public LeafCollector wrapLeafCollector(LeafCollector delegate, Planner p) {
            final PlainSink sink = new PlainSink(delegate, p);
            this.plain = sink;
            return new LeafCollector() {
                /** In pass-through, docs below it go straight to the delegate; MIN_VALUE once buffering. */
                private int passBelow = 0;

                /** Refreshes {@code passBelow} after the planner may have moved or left pass-through. */
                private void updatePassBelow() {
                    passBelow = bypass ? p.trigger : Integer.MIN_VALUE;
                }

                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    delegate.setScorer(scorer);
                }

                @Override
                public void collect(int doc) throws IOException {
                    if (doc < passBelow) {
                        delegate.collect(doc);
                        return;
                    }
                    if (bypass) {
                        p.advance(doc);
                        updatePassBelow();
                        if (bypass == false) {
                            arriveDoc(doc);
                            return;
                        }
                        delegate.collect(doc);
                        return;
                    }
                    arriveDoc(doc);
                }

                @Override
                public void collect(DocIdStream s) throws IOException {
                    if (bypass) {
                        final int rest = sink.stream(s, true);
                        updatePassBelow();
                        if (rest >= 0) {
                            arriveStream(rest, s);
                        }
                        return;
                    }
                    arriveStream(s);
                }

                @Override
                public void collectRange(int min, int max) throws IOException {
                    if (bypass) {
                        final int rest = sink.range(min, max, true);
                        updatePassBelow();
                        if (rest >= 0) {
                            arriveRange(rest, max);
                        }
                        return;
                    }
                    arriveRange(min, max);
                }

                @Override
                public DocIdSetIterator competitiveIterator() throws IOException {
                    return delegate.competitiveIterator();
                }

                @Override
                public void finish() throws IOException {
                    finishLeaf();
                    delegate.finish();
                }
            };
        }

        /** Hands docs to a plain leaf collector, advancing the planner at its trigger before the doc there. */
        private final class PlainSink {
            private final LeafCollector out;
            private final Planner p;
            private final LimitedStream view = new LimitedStream();
            private final int[] one = new int[1];

            PlainSink(LeafCollector out, Planner p) {
                this.out = out;
                this.p = p;
            }

            void doc(int doc) throws IOException {
                if (doc >= p.trigger) {
                    p.advance(doc);
                }
                out.collect(doc);
            }

            /**
             * Hands over {@code s}. With {@code passThrough}, returns the doc at which the planner started buffering
             * (not handed over; it and the rest of {@code s} must arrive in the buffer), else -1.
             */
            int stream(DocIdStream s, boolean passThrough) throws IOException {
                while (true) {
                    final int trigger = p.trigger;
                    if (trigger == DocIdSetIterator.NO_MORE_DOCS) {
                        out.collect(s);
                        return -1;
                    }
                    view.reset(s, trigger);
                    out.collect(view);
                    if (s.intoArray(one) == 0) {
                        return -1;
                    }
                    final int doc = one[0];
                    p.advance(doc);
                    if (passThrough && bypass == false) {
                        return doc;
                    }
                    out.collect(doc);
                }
            }

            /**
             * Hands over the range {@code [min, max)}. With {@code passThrough}, returns the doc at which the planner
             * started buffering (the range from it on is not handed over), else -1.
             */
            int range(int min, int max, boolean passThrough) throws IOException {
                while (true) {
                    final int trigger = p.trigger;
                    if (trigger >= max) {
                        out.collectRange(min, max);
                        return -1;
                    }
                    if (trigger > min) {
                        out.collectRange(min, trigger);
                        min = trigger;
                    }
                    p.advance(min);
                    if (passThrough && bypass == false) {
                        return min;
                    }
                }
            }
        }

        /** The docs of a stream below a limit, as a stream; the docs at or after the limit stay in the stream. */
        private static final class LimitedStream extends DocIdStream {
            private DocIdStream in;
            private int limit;
            private boolean exhausted;

            void reset(DocIdStream in, int limit) {
                this.in = in;
                this.limit = limit;
                this.exhausted = false;
            }

            @Override
            public void forEach(int upTo, CheckedIntConsumer<IOException> consumer) throws IOException {
                if (upTo >= limit) {
                    upTo = limit;
                    exhausted = true;
                }
                in.forEach(upTo, consumer);
            }

            @Override
            public int count(int upTo) throws IOException {
                if (upTo >= limit) {
                    upTo = limit;
                    exhausted = true;
                }
                return in.count(upTo);
            }

            @Override
            public int intoArray(int upTo, int[] array) {
                final boolean toLimit = upTo >= limit;
                final int n = in.intoArray(toLimit ? limit : upTo, array);
                if (n == 0 && toLimit) {
                    exhausted = true;
                }
                return n;
            }

            @Override
            public int intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                if (upTo >= limit) {
                    upTo = limit;
                    exhausted = true;
                }
                return in.intoBitSet(upTo, bitSet, offset);
            }

            @Override
            public boolean mayHaveRemaining() {
                return exhausted == false && in.mayHaveRemaining();
            }
        }

        private void finishFlush() throws IOException {
            if (buffering == false) {
                return;
            }
            fastUpTo = 0;
            if (runCount > 0) {
                endRun();
            }
            deliver(arrived);
        }

        /**
         * Writes the doc IDs of the set bits of a word with at least 32 set bits, without a branch per bit (as Lucene's
         * FixedBitSet does); {@code docs} must have room for one more than the word's bit count.
         */
        private static int denseWordToArray(long word, int base, int[] docs, int offset) {
            final int lWord = (int) word;
            final int hWord = (int) (word >>> 32);
            final int offset32 = offset + Integer.bitCount(lWord);
            int hOffset = offset32;
            for (int i = 0; i < 32; i++) {
                docs[offset] = base + i;
                docs[hOffset] = base + i + 32;
                offset += (lWord >>> i) & 1;
                hOffset += (hWord >>> i) & 1;
            }
            docs[offset32] = base + 32 + Integer.numberOfTrailingZeros(hWord);
            return hOffset;
        }

        /** The buffered matches in [from, to), as a stream for the collectors. */
        private final class BufferStream extends DocIdStream {
            private int upTo, max;

            void reset(int from, int to) {
                assert bits != null : "stream over a buffer that did not start";
                this.upTo = from;
                this.max = to;
            }

            @Override
            public boolean mayHaveRemaining() {
                return upTo < max;
            }

            @Override
            public void forEach(int upTo, CheckedIntConsumer<IOException> consumer) throws IOException {
                if (upTo > this.upTo) {
                    upTo = Math.min(upTo, max);
                    bits.forEach(this.upTo - base, upTo - base, base, consumer);
                    this.upTo = upTo;
                }
            }

            @Override
            public int count(int upTo) {
                if (upTo > this.upTo) {
                    upTo = Math.min(upTo, max);
                    final int count = bits.cardinality(this.upTo - base, upTo - base);
                    this.upTo = upTo;
                    return count;
                }
                return 0;
            }

            @Override
            public int intoArray(int upTo, int[] array) {
                upTo = Math.min(upTo, max);
                if (upTo <= this.upTo) {
                    return 0;
                }
                // the buffer can be sparse (a selective query): skip empty words in a tight loop, and decode dense words
                // without a branch per bit
                final int end = upTo - base;
                final int lastWord = (end - 1) >> 6;
                int wordIndex = (this.upTo - base) >> 6;
                long word = words[wordIndex] & (-1L << (this.upTo - base));
                int count = 0;
                while (true) {
                    if (wordIndex == lastWord && (end & 63) != 0) {
                        word &= (1L << end) - 1;
                    }
                    if (word != 0) {
                        final int wordBase = base + (wordIndex << 6);
                        final int bitCount = Long.bitCount(word);
                        if (array.length - count > bitCount) {
                            if (bitCount >= 32) {
                                count = denseWordToArray(word, wordBase, array, count);
                            } else {
                                do {
                                    array[count++] = wordBase + Long.numberOfTrailingZeros(word);
                                    word &= word - 1;
                                } while (word != 0);
                            }
                        } else {
                            // the array fills in this word
                            while (count < array.length) {
                                array[count++] = wordBase + Long.numberOfTrailingZeros(word);
                                word &= word - 1;
                            }
                            if (word != 0) {
                                this.upTo = wordBase + Long.numberOfTrailingZeros(word);
                            } else {
                                this.upTo = wordIndex == lastWord ? upTo : wordBase + 64;
                            }
                            return count;
                        }
                    }
                    if (++wordIndex > lastWord) {
                        this.upTo = upTo;
                        return count;
                    }
                    word = words[wordIndex];
                }
            }
        }
    }

    /**
     * Starts building the deferred (replay) leaf collector of a segment in run-ahead mode: planners created until
     * {@link #clear} take their matches from a ring of replay chunks. Returns null when run-ahead is off.
     */
    public static Replay beginReplay() {
        if (enabled == false || runAhead == false || CURRENT.get() != null) {
            return null;
        }
        final Replay r = new Replay(runAheadDocs, runAheadBypass);
        CURRENT.set(r);
        return r;
    }

    /**
     * Replays recorded docs in chunks, keeping up to {@link #CHUNKS} chunks between decoding and collection, so planners
     * of the replayed collectors know the docs ahead of collection. The chunks are the replay's own buffers: no doc is
     * decoded twice.
     */
    public static final class Replay extends Ahead {
        /**
         * Up to 128 chunks of 1,024 docs (1.5 MiB, allocated as needed): a dense replay needs about lag x density docs in
         * the ring to keep a node of lead (16 chunks gave terms s50 7d only half of the cold gain).
         */
        static final int CHUNKS = 128;
        private final int lag;
        private final int[][] docs = new int[CHUNKS][];
        private final long[][] buckets = new long[CHUNKS][];
        private final int[] counts = new int[CHUNKS];
        /** Oldest sealed chunk and number of sealed chunks; the chunk after them is being filled. */
        private int head, sealed;
        private int n;
        private int[] fillDocs;
        private long[] fillBuckets;

        Replay(int lag) {
            this(lag, false);
        }

        Replay(int lag, boolean bypass) {
            this.lag = lag;
            this.bypass = bypass;
        }

        /** Whether a planner uses this ring; if not, replay as usual. */
        public boolean hasPlanners() {
            return planners.isEmpty() == false;
        }

        /**
         * Whether docs go straight to the collectors (pass-through, see {@link #setRunAheadBypass}): the replay loop
         * then uses its own chunk and {@link #passThrough}, without the ring, until this turns false.
         */
        public boolean isPassThrough() {
            return bypass;
        }

        /**
         * Pass-through: hands {@code count} docs straight to {@code leaf}. Planners called from {@code leaf} may leave
         * pass-through ({@link #isPassThrough()} turns false); the replay loop then fills the ring from the next doc on.
         */
        public void passThrough(int[] docs, long[] buckets, int count, LeafBucketCollector leaf) throws IOException {
            if (count == 0) {
                return;
            }
            if (arrived == 0) {
                runAheadReplays.increment();
            }
            // the docs below are known: a planner that leaves pass-through waits for the docs after them
            arrived = docs[count - 1] + 1;
            leaf.collectBatch(docs, buckets, count);
        }

        /** The last doc handed over by {@link #passThrough}: the ring continues after it. */
        public int resumeDoc() {
            return arrived - 1;
        }

        private void startFill() {
            final int c = (head + sealed) % CHUNKS;
            if (docs[c] == null) {
                docs[c] = new int[BatchCollection.CHUNK];
                buckets[c] = new long[BatchCollection.CHUNK];
            }
            fillDocs = docs[c];
            fillBuckets = buckets[c];
            n = 0;
        }

        /**
         * The doc array of the chunk to fill: the replay loop writes docs (and {@link #bucketsToFill()} buckets) at
         * positions 0 to {@link BatchCollection#CHUNK} - 1 itself, then calls {@link #sealFull} or {@link #finish(int,
         * LeafBucketCollector)}. Keeps the replay loop as tight as without the ring.
         */
        public int[] docsToFill() {
            if (fillDocs == null) {
                if (arrived == 0) {
                    runAheadReplays.increment();
                }
                startFill();
            }
            return fillDocs;
        }

        /** The bucket array of the chunk to fill, see {@link #docsToFill()}. */
        public long[] bucketsToFill() {
            return fillBuckets;
        }

        /** The chunk being filled is full; may hand older chunks to {@code leaf}. */
        public void sealFull(LeafBucketCollector leaf) throws IOException {
            n = fillDocs.length;
            seal(leaf);
        }

        /** The chunk being filled holds {@code count} docs and no more will come: hands every doc to {@code leaf}. */
        public void finish(int count, LeafBucketCollector leaf) throws IOException {
            n = count;
            finish(leaf);
        }

        /** Adds one doc to replay into {@code bucket}; may hand older chunks to {@code leaf}. */
        public void add(int doc, long bucket, LeafBucketCollector leaf) throws IOException {
            if (fillDocs == null) {
                if (arrived == 0) {
                    runAheadReplays.increment();
                }
                startFill();
            }
            fillDocs[n] = doc;
            fillBuckets[n++] = bucket;
            if (n == fillDocs.length) {
                seal(leaf);
            }
        }

        private void seal(LeafBucketCollector leaf) throws IOException {
            final int c = (head + sealed) % CHUNKS;
            counts[c] = n;
            sealed++;
            arrived = fillDocs[n - 1] + 1;
            afterArrival();
            // pass-through (bypass): every sealed chunk goes to the collectors right away, as without the ring
            while (sealed > 0 && (bypass || sealed == CHUNKS - 1 || arrived - docs[head][counts[head] - 1] > lag)) {
                deliverHead(leaf);
            }
            startFill();
        }

        private void deliverHead(LeafBucketCollector leaf) throws IOException {
            // the chunk stays in the ring while it is collected: its planners may still look at it
            leaf.collectBatch(docs[head], buckets[head], counts[head]);
            head = (head + 1) % CHUNKS;
            sealed--;
        }

        /** Hands every remaining doc to {@code leaf}. */
        public void finish(LeafBucketCollector leaf) throws IOException {
            if (fillDocs == null) {
                return;
            }
            if (n > 0) {
                final int c = (head + sealed) % CHUNKS;
                counts[c] = n;
                sealed++;
                n = 0;
            }
            finishArrivals();
            while (sealed > 0) {
                deliverHead(leaf);
            }
        }

        @Override
        int nextBuffered(int target) {
            for (int k = 0; k < sealed; k++) {
                final int c = (head + k) % CHUNKS;
                final int count = counts[c];
                final int[] d = docs[c];
                if (d[count - 1] < target) {
                    continue;
                }
                int i = Arrays.binarySearch(d, 0, count, target);
                if (i < 0) {
                    i = -1 - i;
                } else {
                    while (i > 0 && d[i - 1] == target) {
                        i--;
                    }
                }
                return d[i];
            }
            return DocIdSetIterator.NO_MORE_DOCS;
        }
    }
}
