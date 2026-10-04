/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LSBRadixSorter;
import org.apache.lucene.util.packed.PackedInts;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Top-k approximation of a {@code bool} whose required clauses are a range on the primary sort field and other filters
 * (experiment D-b, switch {@code approx_bool}). Per segment it walks the range's BKD leaves from the sort end, gathers
 * candidate docs in batches sized by the selectivity of the other clauses, keeps the candidates that match every other
 * clause, and stops once it holds the budget ({@code max(from + size, track_total_hits) + 1}) plus the deleted docs. For a
 * descending sort it then adds the matching docs tied with the cut value from the leaves to the left, because the field
 * sort ranks tied docs by lower doc ID first. The result holds every match that ranks at least as well as the cut, so
 * the top hits and {@code hits.total} equal the plain {@code bool}.
 *
 * @opensearch.internal
 */
public final class ApproximateBooleanQuery extends ApproximateQuery {

    private static final double MIN_SELECTIVITY = 1.0 / 64;
    private static final double BATCH_FACTOR = 1.5;

    private final BooleanQuery original;
    private final ApproximatePointRangeQuery range;
    private final List<Query> others;
    // the original bool, rewritten; null until this query is rewritten
    private final Query fallback;

    ApproximateBooleanQuery(BooleanQuery original, ApproximatePointRangeQuery range, List<Query> others) {
        this(original, range, others, null);
    }

    private ApproximateBooleanQuery(BooleanQuery original, ApproximatePointRangeQuery range, List<Query> others, Query fallback) {
        this.original = original;
        this.range = range;
        this.others = Collections.unmodifiableList(others);
        this.fallback = fallback;
    }

    /**
     * Returns {@code query} wrapped for D-b and resolved to the approximation, or null if the query does not have the
     * shape (all clauses FILTER or MUST, at least two, minimum_should_match 0, exactly one an {@link ApproximateScoreQuery}
     * over a one-dimensional {@link ApproximatePointRangeQuery} on the primary sort field) or cannot be approximated in
     * {@code context}. With null the caller keeps the plain {@code bool}.
     */
    public static ApproximateScoreQuery approximate(BooleanQuery query, SearchContext context) {
        if (context == null || query.getMinimumNumberShouldMatch() != 0 || query.clauses().size() < 2) {
            return null;
        }
        ApproximatePointRangeQuery range = null;
        List<Query> others = new ArrayList<>();
        for (BooleanClause clause : query.clauses()) {
            if (clause.occur() != BooleanClause.Occur.FILTER && clause.occur() != BooleanClause.Occur.MUST) {
                return null;
            }
            if (range == null
                && clause.query() instanceof ApproximateScoreQuery asq
                && asq.getApproximationQuery() instanceof ApproximatePointRangeQuery r
                && r.pointRangeQuery.getNumDims() == 1
                && r.pointRangeQuery.getField().equals(primarySortField(context))) {
                range = r;
            } else {
                others.add(clause.query());
            }
        }
        if (range == null) {
            return null;
        }
        for (Query other : others) {
            if (other instanceof ApproximateScoreQuery asq
                && asq.getApproximationQuery() instanceof ApproximatePointRangeQuery r
                && r.pointRangeQuery.getField().equals(range.pointRangeQuery.getField())) {
                return null; // a second range on the sort field: not the shape
            }
        }
        ApproximateScoreQuery wrapped = new ApproximateScoreQuery(query, new ApproximateBooleanQuery(query, range, others));
        return wrapped.resolveIfApproximable(context) ? wrapped : null;
    }

    private static String primarySortField(SearchContext context) {
        if (context.request() == null || context.request().source() == null) {
            return null;
        }
        FieldSortBuilder primary = FieldSortBuilder.getPrimaryFieldSortOrNull(context.request().source());
        return primary == null ? null : primary.fieldName();
    }

    @Override
    protected boolean canApproximate(SearchContext context) {
        if (context == null || context.request() == null || context.request().source() == null) {
            return false;
        }
        // search_after first: the range's canApproximate rewrites the range for it before it returns (POC limit)
        if (context.request().source().searchAfter() != null) {
            return false;
        }
        if (context.trackScores()) {
            return false;
        }
        if (range.pointRangeQuery.getField().equals(primarySortField(context)) == false) {
            return false;
        }
        // the context checks (aggregations, track_total_hits: true, several sorts, missing, terminate_after); sets the
        // budget and the sort order
        return range.canApproximate(context);
    }

    /** The plain bool. */
    public BooleanQuery getOriginalQuery() {
        return original;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (fallback != null) {
            return this;
        }
        List<Query> rewritten = new ArrayList<>(others.size());
        for (Query q : others) {
            rewritten.add(rewriteFully(q, indexSearcher));
        }
        return new ApproximateBooleanQuery(original, range, rewritten, rewriteFully(original, indexSearcher));
    }

    private static Query rewriteFully(Query query, IndexSearcher searcher) throws IOException {
        for (Query rewritten = query.rewrite(searcher); rewritten != query; rewritten = query.rewrite(searcher)) {
            query = rewritten;
        }
        return query;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        original.visit(visitor);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (fallback == null) {
            return rewrite(searcher).createWeight(searcher, scoreMode, boost);
        }
        final Weight fallbackWeight = searcher.createWeight(fallback, scoreMode, boost);
        final Weight[] otherWeights = new Weight[others.size()];
        for (int i = 0; i < otherWeights.length; i++) {
            otherWeights[i] = searcher.createWeight(others.get(i), ScoreMode.COMPLETE_NO_SCORES, 1f);
        }
        final String field = range.pointRangeQuery.getField();
        final int bytes = range.pointRangeQuery.getBytesPerDim();
        final byte[] lower = range.pointRangeQuery.getLowerPoint();
        final byte[] upper = range.pointRangeQuery.getUpperPoint();
        final int budget = range.getSize();
        final boolean desc = range.getSortOrder() == SortOrder.DESC;
        final ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(bytes);

        return new ConstantScoreWeight(this, boost) {

            boolean inRange(byte[] value) {
                return comparator.compare(value, 0, lower, 0) >= 0 && comparator.compare(value, 0, upper, 0) <= 0;
            }

            PointValues.Relation relate(byte[] min, byte[] max) {
                if (comparator.compare(min, 0, upper, 0) > 0 || comparator.compare(max, 0, lower, 0) < 0) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                if (comparator.compare(min, 0, lower, 0) >= 0 && comparator.compare(max, 0, upper, 0) <= 0) {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final LeafReader reader = context.reader();
                final PointValues values = reader.getPointValues(field);
                if (values == null
                    || values.getNumIndexDimensions() != 1
                    || values.getBytesPerDimension() != bytes
                    || values.size() != values.getDocCount()
                    || values.size() < budget) {
                    return fallbackWeight.scorerSupplier(context);
                }
                final int maxDoc = reader.maxDoc();
                double selectivity = 1;
                for (Weight w : otherWeights) {
                    ScorerSupplier ss = w.scorerSupplier(context);
                    if (ss == null) {
                        return null; // a required clause matches nothing in this segment
                    }
                    selectivity = Math.min(selectivity, (double) ss.cost() / maxDoc);
                }
                final double sel = Math.min(1, Math.max(MIN_SELECTIVITY, selectivity));
                return new ScorerSupplier() {
                    DocIdSetIterator result;
                    long matches = -1;

                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        compute();
                        return new ConstantScoreScorer(score(), scoreMode, result);
                    }

                    @Override
                    public long cost() {
                        try {
                            compute();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                        return matches;
                    }

                    private void compute() throws IOException {
                        if (matches >= 0) {
                            return;
                        }
                        final long need = (long) budget + reader.numDeletedDocs();
                        long batch = (long) Math.ceil(need / sel * BATCH_FACTOR);
                        final DocIdSetBuilder builder = new DocIdSetBuilder(maxDoc);
                        final LeafWalk walk = new LeafWalk(values.getPointTree());
                        final Candidates candidates = new Candidates();
                        final byte[] lastMin = new byte[bytes];
                        long found = 0;
                        boolean exhausted = false;
                        while (found < need && exhausted == false) {
                            candidates.clear();
                            while (candidates.size < batch) {
                                if (walk.next() == false) {
                                    exhausted = true;
                                    break;
                                }
                                System.arraycopy(walk.current.getMinPackedValue(), 0, lastMin, 0, bytes);
                                candidates.collect(walk.current, walk.relation);
                            }
                            found += intersect(context, candidates, builder, maxDoc);
                            batch = Math.min(batch * 2, Integer.MAX_VALUE);
                        }
                        // desc: the leaves to the left can hold docs tied with the cut value v* (the last included
                        // leaf's min) with lower doc IDs, which rank first; read them while a leaf's max is v*, and go
                        // on past a leaf only if its min is v* too (whatever its matches). asc needs no tie loop: tied
                        // docs to the right have higher doc IDs and rank after the included ones.
                        if (desc && exhausted == false && found > 0 && inRange(lastMin)) {
                            while (walk.next()) {
                                PointValues.PointTree leaf = walk.current;
                                if (comparator.compare(leaf.getMaxPackedValue(), 0, lastMin, 0) != 0) {
                                    break;
                                }
                                candidates.clear();
                                candidates.collectEqual(leaf, lastMin);
                                found += intersect(context, candidates, builder, maxDoc);
                                if (comparator.compare(leaf.getMinPackedValue(), 0, lastMin, 0) != 0) {
                                    break;
                                }
                            }
                        }
                        matches = found;
                        result = builder.build().iterator();
                    }
                };
            }

            /**
             * Sorts the candidates, drops deleted docs, and adds the ones that match every other clause; a fresh scorer
             * per clause and call (a scorer supplier may be used once, and candidates of a later batch can have lower
             * doc IDs).
             */
            private long intersect(LeafReaderContext context, Candidates candidates, DocIdSetBuilder builder, int maxDoc)
                throws IOException {
                if (candidates.size == 0) {
                    return 0;
                }
                final int[] docs = candidates.docs;
                new LSBRadixSorter().sort(PackedInts.bitsRequired(maxDoc - 1), docs, candidates.size);
                final Bits live = context.reader().getLiveDocs();
                int n = 0;
                for (int i = 0; i < candidates.size; i++) {
                    if (live == null || live.get(docs[i])) {
                        docs[n++] = docs[i];
                    }
                }
                if (n == 0) {
                    return 0;
                }
                final DocIdSetIterator[] approximations = new DocIdSetIterator[otherWeights.length];
                final TwoPhaseIterator[] twoPhases = new TwoPhaseIterator[otherWeights.length];
                for (int j = 0; j < otherWeights.length; j++) {
                    ScorerSupplier ss = otherWeights[j].scorerSupplier(context);
                    if (ss == null) {
                        return 0;
                    }
                    Scorer scorer = ss.get(n);
                    twoPhases[j] = scorer.twoPhaseIterator();
                    approximations[j] = twoPhases[j] == null ? scorer.iterator() : twoPhases[j].approximation();
                }
                final DocIdSetBuilder.BulkAdder adder = builder.grow(n);
                long added = 0;
                docLoop: for (int i = 0; i < n; i++) {
                    final int doc = docs[i];
                    for (DocIdSetIterator it : approximations) {
                        int d = it.docID();
                        if (d < doc) {
                            d = it.advance(doc);
                        }
                        if (d == DocIdSetIterator.NO_MORE_DOCS) {
                            break docLoop;
                        }
                        if (d != doc) {
                            continue docLoop;
                        }
                    }
                    for (TwoPhaseIterator tp : twoPhases) {
                        if (tp != null && tp.matches() == false) {
                            continue docLoop;
                        }
                    }
                    adder.add(doc);
                    added++;
                }
                return added;
            }

            /** The leaves of the range in sort order (asc: left to right, desc: right to left), skipping outside cells. */
            final class LeafWalk {
                private final ArrayDeque<PointValues.PointTree> stack = new ArrayDeque<>();
                PointValues.PointTree current;
                PointValues.Relation relation;

                LeafWalk(PointValues.PointTree root) {
                    stack.push(root);
                }

                boolean next() throws IOException {
                    while (stack.isEmpty() == false) {
                        PointValues.PointTree node = stack.pop();
                        PointValues.Relation r = relate(node.getMinPackedValue(), node.getMaxPackedValue());
                        if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
                            continue;
                        }
                        PointValues.PointTree child = node.clone();
                        if (child.moveToChild()) {
                            PointValues.PointTree left = child.clone();
                            PointValues.PointTree right = child.moveToSibling() ? child : null;
                            // the stack pops the last push first
                            if (desc) {
                                stack.push(left);
                                if (right != null) {
                                    stack.push(right);
                                }
                            } else {
                                if (right != null) {
                                    stack.push(right);
                                }
                                stack.push(left);
                            }
                            continue;
                        }
                        current = node;
                        relation = r;
                        return true;
                    }
                    current = null;
                    return false;
                }
            }

            /** Candidate doc IDs gathered from whole leaves. */
            final class Candidates implements PointValues.IntersectVisitor {
                int[] docs = new int[1024];
                int size;
                private byte[] equalTo; // non-null: collect docs with exactly this value (tie loop)

                void clear() {
                    size = 0;
                }

                void collect(PointValues.PointTree leaf, PointValues.Relation r) throws IOException {
                    equalTo = null;
                    if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
                        leaf.visitDocIDs(this);
                    } else {
                        leaf.visitDocValues(this);
                    }
                }

                void collectEqual(PointValues.PointTree leaf, byte[] value) throws IOException {
                    equalTo = value;
                    leaf.visitDocValues(this);
                    equalTo = null;
                }

                private boolean accept(byte[] packedValue) {
                    return equalTo == null ? inRange(packedValue) : comparator.compare(packedValue, 0, equalTo, 0) == 0;
                }

                private void add(int doc) {
                    if (size == docs.length) {
                        docs = ArrayUtil.grow(docs, size + 1);
                    }
                    docs[size++] = doc;
                }

                @Override
                public void grow(int count) {
                    docs = ArrayUtil.grow(docs, size + count);
                }

                @Override
                public void visit(int docID) {
                    add(docID);
                }

                @Override
                public void visit(DocIdSetIterator iterator) throws IOException {
                    for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                        add(doc);
                    }
                }

                @Override
                public void visit(IntsRef ref) {
                    docs = ArrayUtil.grow(docs, size + ref.length);
                    System.arraycopy(ref.ints, ref.offset, docs, size, ref.length);
                    size += ref.length;
                }

                @Override
                public void visit(int docID, byte[] packedValue) {
                    if (accept(packedValue)) {
                        add(docID);
                    }
                }

                @Override
                public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                    if (accept(packedValue)) {
                        visit(iterator);
                    }
                }

                @Override
                public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                    if (equalTo == null) {
                        return relate(minPackedValue, maxPackedValue);
                    }
                    if (comparator.compare(minPackedValue, 0, equalTo, 0) > 0 || comparator.compare(maxPackedValue, 0, equalTo, 0) < 0) {
                        return PointValues.Relation.CELL_OUTSIDE_QUERY;
                    }
                    if (comparator.compare(minPackedValue, 0, equalTo, 0) == 0 && comparator.compare(maxPackedValue, 0, equalTo, 0) == 0) {
                        return PointValues.Relation.CELL_INSIDE_QUERY;
                    }
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    @Override
    public String toString(String field) {
        return "ApproximateBoolean(" + original.toString(field) + ")";
    }

    @Override
    public boolean equals(Object o) {
        return sameClassAs(o) && original.equals(((ApproximateBooleanQuery) o).original);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), original);
    }
}
