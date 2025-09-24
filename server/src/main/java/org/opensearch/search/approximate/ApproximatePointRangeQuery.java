/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.sandbox.document.BigIntegerPoint;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.IntsRef;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumericPointEncoder;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.opensearch.search.aggregations.bucket.filterrewrite.PointTreeTraversal.ENABLE_PREFETCH;

/**
 * An approximate-able version of {@link PointRangeQuery}. It creates an instance of {@link PointRangeQuery} but short-circuits the intersect logic
 * after {@code size} is hit
 */
public class ApproximatePointRangeQuery extends ApproximateQuery {
    public static final Function<byte[], String> LONG_FORMAT = bytes -> Long.toString(LongPoint.decodeDimension(bytes, 0));
    public static final Function<byte[], String> INT_FORMAT = bytes -> Integer.toString(IntPoint.decodeDimension(bytes, 0));
    public static final Function<byte[], String> HALF_FLOAT_FORMAT = bytes -> Float.toString(HalfFloatPoint.decodeDimension(bytes, 0));
    public static final Function<byte[], String> FLOAT_FORMAT = bytes -> Float.toString(FloatPoint.decodeDimension(bytes, 0));
    public static final Function<byte[], String> DOUBLE_FORMAT = bytes -> Double.toString(DoublePoint.decodeDimension(bytes, 0));
    public static final Function<byte[], String> UNSIGNED_LONG_FORMAT = bytes -> BigIntegerPoint.decodeDimension(bytes, 0).toString();

    private static final Logger logger = LogManager.getLogger(ApproximatePointRangeQuery.class);
    private int size;
    private SortOrder sortOrder;
    public PointRangeQuery pointRangeQuery;
    private final Function<byte[], String> valueToString;

    public ApproximatePointRangeQuery(
        String field,
        byte[] lowerPoint,
        byte[] upperPoint,
        int numDims,
        Function<byte[], String> valueToString
    ) {
        this(field, lowerPoint, upperPoint, numDims, SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO, null, valueToString);
    }

    protected ApproximatePointRangeQuery(
        String field,
        byte[] lowerPoint,
        byte[] upperPoint,
        int numDims,
        int size,
        SortOrder sortOrder,
        Function<byte[], String> valueToString
    ) {
        this.size = size;
        logger.info("Value of size in query is {}", size);
        this.sortOrder = sortOrder;
        this.valueToString = valueToString;
        this.pointRangeQuery = new PointRangeQuery(field, lowerPoint, upperPoint, numDims) {
            @Override
            protected String toString(int dimension, byte[] value) {
                return valueToString.apply(value);
            }
        };
    }

    public int getSize() {
        return this.size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public SortOrder getSortOrder() {
        return this.sortOrder;
    }

    public void setSortOrder(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        return super.rewrite(indexSearcher);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        pointRangeQuery.visit(visitor);
    }

    @Override
    public final ConstantScoreWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        Weight pointRangeQueryWeight = pointRangeQuery.createWeight(searcher, scoreMode, boost);
        return new ApproximatePointWeight( this, boost, size, pointRangeQueryWeight,
            sortOrder, scoreMode, searcher);
    }

    private byte[] computeEffectiveBound(SearchContext context, boolean isLowerBound) {
        byte[] originalBound = isLowerBound ? pointRangeQuery.getLowerPoint() : pointRangeQuery.getUpperPoint();
        boolean isAscending = sortOrder == null || sortOrder.equals(SortOrder.ASC);
        if ((isLowerBound && isAscending) || (isLowerBound == false && isAscending == false)) {
            Object searchAfterValue = context.request().source().searchAfter()[0];
            MappedFieldType fieldType = context.getQueryShardContext().fieldMapper(pointRangeQuery.getField());
            if (fieldType instanceof NumericPointEncoder encoder) {
                return encoder.encodePoint(searchAfterValue, isLowerBound);
            }
        }
        return originalBound;
    }

    @Override
    public boolean canApproximate(SearchContext context) {
        if (context == null) {
            return false;
        }
        if (context.aggregations() != null) {
            return false;
        }
        // Exclude approximation when "track_total_hits": true
        if (context.trackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
            return false;
        }
        // size 0 could be set for caching
        if (context.from() + context.size() == 0) {
            this.setSize(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);
        } else {
            // We add +1 to ensure we collect at least one more document than required. This guarantees correct relation value:
            // - If we find exactly trackTotalHitsUpTo docs: relation = EQUAL_TO
            // - If we find > trackTotalHitsUpTo docs: relation = GREATER_THAN_OR_EQUAL_TO
            // With +1, we will consistently get GREATER_THAN_OR_EQUAL_TO relation.
            this.setSize(Math.max(context.from() + context.size(), context.trackTotalHitsUpTo()) + 1);
        }
        if (context.request() != null && context.request().source() != null) {
            if (context.request().source().sorts() != null && context.request().source().sorts().size() > 1) {
                return false;
            }
            FieldSortBuilder primarySortField = FieldSortBuilder.getPrimaryFieldSortOrNull(context.request().source());
            if (primarySortField != null) {
                if (!primarySortField.fieldName().equals(pointRangeQuery.getField())) {
                    return false;
                }
                if (primarySortField.missing() != null) {
                    // Cannot sort documents missing this field.
                    return false;
                }
                this.setSortOrder(primarySortField.order());
                if (context.request().source().searchAfter() != null) {
                    byte[] lower;
                    byte[] upper;
                    if (sortOrder == SortOrder.ASC) {
                        lower = computeEffectiveBound(context, true);
                        upper = pointRangeQuery.getUpperPoint();
                    } else {
                        lower = pointRangeQuery.getLowerPoint();
                        upper = computeEffectiveBound(context, false);
                    }
                    this.pointRangeQuery = new PointRangeQuery(pointRangeQuery.getField(), lower, upper, pointRangeQuery.getNumDims()) {
                        @Override
                        protected String toString(int dimension, byte[] value) {
                            return valueToString.apply(value);
                        }
                    };
                }
            }
            return context.request().source().terminateAfter() == SearchContext.DEFAULT_TERMINATE_AFTER;
        }
        return true;
    }

    @Override
    public final int hashCode() {
        return pointRangeQuery.hashCode();
    }

    @Override
    public final boolean equals(Object o) {
        return sameClassAs(o) && equalsTo(getClass().cast(o));
    }

    private boolean equalsTo(ApproximatePointRangeQuery other) {
        return Objects.equals(pointRangeQuery, other.pointRangeQuery);
    }

    @Override
    public final String toString(String field) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Approximate(");
        sb.append(pointRangeQuery.toString());
        sb.append(")");

        return sb.toString();
    }

    public int getBytesPerDim() {
        return pointRangeQuery.getBytesPerDim();
    }

    public String getField() {
        return pointRangeQuery.getField();
    }

    public int getNumDims() {
        return pointRangeQuery.getNumDims();
    }

    public byte[] getLowerPoint() {
        return pointRangeQuery.getLowerPoint();
    }

    public byte[] getUpperPoint() {
        return pointRangeQuery.getUpperPoint();
    }
}
