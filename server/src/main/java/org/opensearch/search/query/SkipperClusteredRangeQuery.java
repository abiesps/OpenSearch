/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A range on a numeric field with points and a doc-values skipper. On a segment whose values are clustered in doc-ID
 * order (most skipper intervals are entirely inside or outside the range), the range is answered from the doc-values
 * query: whole skipper blocks inside the range match without reading values, and only the few boundary blocks are
 * checked value by value. On every other segment the {@link IndexOrDocValuesQuery} answers it, as in stock.
 * Both sides describe the same range, so the matching docs are identical. Experiment C, switch
 * {@link SortIoExperiments#isSkipperRange()}.
 *
 * @opensearch.internal
 */
public final class SkipperClusteredRangeQuery extends Query {

    /** Fewest MAYBE docs (two level-1 skipper intervals) that a clustered segment may still have. */
    static final int MIN_MAYBE_DOCS = 65_536;

    private final String field;
    private final long lowerValue;
    private final long upperValue;
    private final Query indexOrDocValuesQuery;
    private final Query dvQuery;

    /**
     * @param field the field, indexed with points and doc values with a skip index
     * @param lowerValue lowest matching value, inclusive
     * @param upperValue highest matching value, inclusive
     * @param indexOrDocValuesQuery the stock query for the range (an {@link IndexOrDocValuesQuery})
     * @param dvQuery the doc-values query for the same range
     */
    public SkipperClusteredRangeQuery(String field, long lowerValue, long upperValue, Query indexOrDocValuesQuery, Query dvQuery) {
        this.field = Objects.requireNonNull(field);
        this.lowerValue = lowerValue;
        this.upperValue = upperValue;
        this.indexOrDocValuesQuery = Objects.requireNonNull(indexOrDocValuesQuery);
        this.dvQuery = Objects.requireNonNull(dvQuery);
    }

    public String getField() {
        return field;
    }

    public long getLowerValue() {
        return lowerValue;
    }

    public long getUpperValue() {
        return upperValue;
    }

    /** The stock query for the range, used on segments that are not clustered. */
    public Query getIndexOrDocValuesQuery() {
        return indexOrDocValuesQuery;
    }

    /** The doc-values query for the range, used on clustered segments. */
    public Query getDocValuesQuery() {
        return dvQuery;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        Query indexOrDv = indexOrDocValuesQuery.rewrite(indexSearcher);
        if (indexOrDv instanceof IndexOrDocValuesQuery == false) {
            // the range rewrote to match-all or match-none (or another stock form): use it as is
            return indexOrDv;
        }
        Query dv = dvQuery.rewrite(indexSearcher);
        if (indexOrDv != indexOrDocValuesQuery || dv != dvQuery) {
            return new SkipperClusteredRangeQuery(field, lowerValue, upperValue, indexOrDv, dv);
        }
        return this;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        indexOrDocValuesQuery.visit(visitor);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final Weight indexOrDvWeight = indexOrDocValuesQuery.createWeight(searcher, scoreMode, boost);
        final Weight dvWeight = dvQuery.createWeight(searcher, scoreMode, boost);
        return new SkipperClusteredWeight(indexOrDvWeight, dvWeight);
    }

    /** Chooses the doc-values weight on clustered segments and the stock weight on the others. */
    final class SkipperClusteredWeight extends Weight {

        private final Weight indexOrDvWeight;
        private final Weight dvWeight;
        // segment core key -> estimated matches if clustered, -1 if not
        private final Map<Object, Long> clustered = new ConcurrentHashMap<>();

        private SkipperClusteredWeight(Weight indexOrDvWeight, Weight dvWeight) {
            super(SkipperClusteredRangeQuery.this);
            this.indexOrDvWeight = indexOrDvWeight;
            this.dvWeight = dvWeight;
        }

        /** Estimated matches if the segment is clustered for the range, else -1 (also -1 without a skipper). */
        long clusteredEstimate(LeafReaderContext context) throws IOException {
            final IndexReader.CacheHelper helper = context.reader().getCoreCacheHelper();
            if (helper == null) {
                return computeEstimate(context);
            }
            Long cached = clustered.get(helper.getKey());
            if (cached == null) {
                cached = computeEstimate(context);
                clustered.put(helper.getKey(), cached);
            }
            return cached;
        }

        private long computeEstimate(LeafReaderContext context) throws IOException {
            final DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
            if (skipper == null) {
                return -1;
            }
            return SkipperClusteredRangeQuery.clusteredEstimate(skipper, lowerValue, upperValue, context.reader().maxDoc());
        }

        private Weight chosen(LeafReaderContext context) throws IOException {
            return clusteredEstimate(context) >= 0 ? dvWeight : indexOrDvWeight;
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            final long estimate = clusteredEstimate(context);
            if (estimate < 0) {
                return indexOrDvWeight.scorerSupplier(context);
            }
            final ScorerSupplier in = dvWeight.scorerSupplier(context);
            if (in == null) {
                return null;
            }
            // The doc-values iterator reports maxDoc as its cost; the skipper walk gives an upper bound of the
            // matches, which is what the points side would report, so clause ordering in conjunctions follows stock.
            return new ScorerSupplier() {
                @Override
                public Scorer get(long leadCost) throws IOException {
                    return in.get(leadCost);
                }

                @Override
                public BulkScorer bulkScorer() throws IOException {
                    return in.bulkScorer();
                }

                @Override
                public long cost() {
                    return Math.min(in.cost(), estimate);
                }

                @Override
                public void setTopLevelScoringClause() throws IOException {
                    in.setTopLevelScoringClause();
                }
            };
        }

        @Override
        public int count(LeafReaderContext context) throws IOException {
            return indexOrDvWeight.count(context);
        }

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            return chosen(context).matches(context, doc);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return chosen(context).explain(context, doc);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return indexOrDvWeight.isCacheable(ctx);
        }
    }

    private static final int NO = 0;
    private static final int YES = 1;
    private static final int MAYBE = 2;

    /**
     * Walks the skipper from its top level down and classifies each interval against {@code [lower, upper]}: NO (all
     * values outside), YES (all inside) or MAYBE. A MAYBE interval is refined down to level 1 (level 0 where no
     * higher level exists). The segment is clustered iff the MAYBE docs are at most {@code max(65,536,
     * estimatedMatches / 8)}, where estimatedMatches = the docs of the YES and MAYBE intervals. The walk stops as soon
     * as the MAYBE docs pass the limit (the limit only falls as NO docs are found, so the answer is final).
     *
     * @return estimatedMatches if clustered, else -1
     */
    static long clusteredEstimate(DocValuesSkipper skipper, long lower, long upper, int maxDoc) throws IOException {
        if (skipper.docCount() == 0 || skipper.minValue() > upper || skipper.maxValue() < lower) {
            return 0; // nothing matches: the doc-values side answers with no reads of values
        }
        long noDocs = 0;
        long maybeDocs = 0;
        int position = 0;
        skipper.advance(0);
        while (skipper.minDocID(0) != DocIdSetIterator.NO_MORE_DOCS) {
            final int levels = skipper.numLevels();
            final int floor = Math.min(1, levels - 1);
            int level = levels - 1;
            int match = classify(skipper, level, lower, upper);
            while (match == MAYBE && level > floor) {
                level--;
                match = classify(skipper, level, lower, upper);
            }
            final int end = skipper.maxDocID(level);
            final long docs = (long) end - Math.max(position, skipper.minDocID(level)) + 1;
            if (match == NO) {
                noDocs += docs;
            } else if (match == MAYBE) {
                maybeDocs += docs;
                if (maybeDocs > maybeLimit(maxDoc, noDocs)) {
                    return -1;
                }
            }
            if (end == DocIdSetIterator.NO_MORE_DOCS || end + 1 >= maxDoc) {
                break;
            }
            position = end + 1;
            skipper.advance(position);
        }
        return maybeDocs <= maybeLimit(maxDoc, noDocs) ? maxDoc - noDocs : -1;
    }

    private static long maybeLimit(int maxDoc, long noDocs) {
        return Math.max(MIN_MAYBE_DOCS, (maxDoc - noDocs) / 8);
    }

    private static int classify(DocValuesSkipper skipper, int level, long lower, long upper) {
        final long min = skipper.minValue(level);
        final long max = skipper.maxValue(level);
        if (min > upper || max < lower) {
            return NO;
        }
        if (min >= lower && max <= upper) {
            return YES;
        }
        return MAYBE;
    }

    @Override
    public String toString(String f) {
        return "SkipperClustered(" + indexOrDocValuesQuery.toString(f) + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        SkipperClusteredRangeQuery that = (SkipperClusteredRangeQuery) obj;
        return field.equals(that.field)
            && lowerValue == that.lowerValue
            && upperValue == that.upperValue
            && indexOrDocValuesQuery.equals(that.indexOrDocValuesQuery)
            && dvQuery.equals(that.dvQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, lowerValue, upperValue, indexOrDocValuesQuery, dvQuery);
    }
}
