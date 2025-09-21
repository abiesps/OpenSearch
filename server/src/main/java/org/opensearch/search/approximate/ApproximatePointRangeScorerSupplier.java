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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.IntsRef;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.opensearch.search.aggregations.bucket.filterrewrite.PointTreeTraversal.ENABLE_PREFETCH;

public class ApproximatePointRangeScorerSupplier extends ScorerSupplier {


    private final DocIdSetBuilder resultWithPrefetching;
    private final PointValues.IntersectVisitor visitorWithPrefetching;
    private final DocIdSetBuilder result;
    private final PointRangeQuery pointRangeQuery;
    final ArrayUtil.ByteArrayComparator comparator;
    private final PointValues.PointTree pointTree;
    private final int size;

    private static final Logger logger = LogManager.getLogger(ApproximatePointRangeScorerSupplier.class);
    private final PointValues.PointTree pointTreeWithPrefetching;
    private long cost;
    private final ConstantScoreWeight constantScoreWeight;
    private final ScoreMode scoreMode;
    private final PointValues values;
    private final PointValues.IntersectVisitor visitor;

    long[] docCountWithPrefetching = { 0 };
    long[] docCount = { 0 };


    private boolean matches(byte[] packedValue) {
        for (int dim = 0; dim < pointRangeQuery.getNumDims(); dim++) {
            int offset = dim * pointRangeQuery.getBytesPerDim();
            if (comparator.compare(packedValue, offset, pointRangeQuery.getLowerPoint(), offset) < 0) {
                // Doc's value is too low, in this dimension
                return false;
            }
            if (comparator.compare(packedValue, offset, pointRangeQuery.getUpperPoint(), offset) > 0) {
                // Doc's value is too high, in this dimension
                return false;
            }
        }
        return true;
    }

    public PointValues.IntersectVisitor getPrefetchingIntersectVisitor(DocIdSetBuilder result, long[] docCount) {
        return new PointValues.IntersectVisitor() {

            DocIdSetBuilder.BulkAdder adder;
            Set<Long> matchingLeafBlocksFPsDocIds = new LinkedHashSet<>();
            Set<Long> matchingLeafBlocksFPsDocValues = new LinkedHashSet<>();

            @Override
            public void grow(int count) {
                adder = result.grow(count);
            }

            @Override
            public void visit(int docID) {
                // it is possible that size < 1024 and docCount < size but we will continue to count through all the 1024 docs
                adder.add(docID);
                //docCount[0]++;
            }

            @Override
            public void visit(DocIdSetIterator iterator) throws IOException {
                adder.add(iterator);
            }

            @Override
            public void visit(IntsRef ref) {
                adder.add(ref);
                //docCount[0] += ref.length;
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                if (matches(packedValue)) {
                    visit(docID);
                }
            }

            @Override
            public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                if (matches(packedValue)) {
                    adder.add(iterator);
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                return relate(minPackedValue, maxPackedValue);
            }

            @Override
            public void matchedLeafFpDocIds(long fp, int count) {
                matchingLeafBlocksFPsDocIds.add(fp);
                docCount[0] += count;
            };

            @Override
            public  Set<Long> matchingLeafNodesfpDocIds() {
                return matchingLeafBlocksFPsDocIds;
            }

            @Override
            public void matchedLeafFpDocValues(long fp) {
                matchingLeafBlocksFPsDocValues.add(fp);
            };

            @Override
            public  Set<Long> matchingLeafNodesfpDocValues() {
                return matchingLeafBlocksFPsDocValues;
            }
        };
    }

    public PointValues.IntersectVisitor getIntersectVisitor(
                                                            DocIdSetBuilder result,
                                                            long[] docCount) {
        return new PointValues.IntersectVisitor() {

            DocIdSetBuilder.BulkAdder adder;
            Set<Long> matchingLeafBlocksFPsDocIds = new LinkedHashSet<>();
            Set<Long> matchingLeafBlocksFPsDocValues = new LinkedHashSet<>();

            @Override
            public void grow(int count) {
                adder = result.grow(count);
            }

            @Override
            public void visit(int docID) {
                // it is possible that size < 1024 and docCount < size but we will continue to count through all the 1024 docs
                adder.add(docID);
                docCount[0]++;
            }

            @Override
            public void visit(DocIdSetIterator iterator) throws IOException {
                adder.add(iterator);
            }

            @Override
            public void visit(IntsRef ref) {
                adder.add(ref);
                docCount[0] += ref.length;
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                if (matches(packedValue)) {
                    visit(docID);
                }
            }

            @Override
            public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                if (matches(packedValue)) {
                    adder.add(iterator);
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                return relate(minPackedValue, maxPackedValue);
            }

            @Override
            public void matchedLeafFpDocIds(long fp, int count) {
                matchingLeafBlocksFPsDocIds.add(fp);
                //docCount[0] += count;
            };

            @Override
            public  Set<Long> matchingLeafNodesfpDocIds() {
                return matchingLeafBlocksFPsDocIds;
            }

            @Override
            public void matchedLeafFpDocValues(long fp) {
                matchingLeafBlocksFPsDocValues.add(fp);
            };

            @Override
            public  Set<Long> matchingLeafNodesfpDocValues() {
                return matchingLeafBlocksFPsDocValues;
            }
        };
    }

    private void intersectLeft(PointValues.PointTree pointTree, PointValues.IntersectVisitor visitor, long[] docCount)
        throws IOException {
        intersectLeft(visitor, pointTree, docCount);
        assert pointTree.moveToParent() == false;
    }

    private void intersectLeft2(PointValues.PointTree pointTree, PointValues.IntersectVisitor visitor, long[] docCount)
        throws IOException {
        intersectLeft2(visitor, pointTree, docCount);
        assert pointTree.moveToParent() == false;
    }

    public void intersectLeft2(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree, long[] docCount)
        throws IOException {
        if (docCount[0] >= size) {
            return;
        }
        PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
        if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
            return;
        }
        // Handle leaf nodes
        if (pointTree.moveToChild() == false) {
            if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
                pointTree.visitDocIDs(visitor);
            } else {
                pointTree.visitDocValues(visitor);
            }
            return;
        }
        // For CELL_INSIDE_QUERY, check if we can skip right child
        if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
            long leftSize = pointTree.size();
            long needed = size - docCount[0];

            if (leftSize >= needed) {
                intersectLeft2(visitor, pointTree, docCount);
                pointTree.moveToParent();
                return;
            }
        }
        // We need both children - now clone right
        PointValues.PointTree rightChild = null;
        if (pointTree.moveToSibling()) {
            rightChild = pointTree.clone();
            pointTree.moveToParent();
            pointTree.moveToChild();
        }
        // Process both children: left first, then right if needed
        intersectLeft2(visitor, pointTree, docCount);
        //if (rightChild != null) {
        if (docCount[0] < size && rightChild != null) {
            intersectLeft2(visitor, rightChild, docCount);
        }
        pointTree.moveToParent();
    }

    public void intersectLeft(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree, long[] docCount)
        throws IOException {
        if (docCount[0] >= size) {
            return;
        }
        PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
        if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
            return;
        }
        // Handle leaf nodes
        if (pointTree.moveToChild() == false) {
            if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
                pointTree.prefetchDocIDs(visitor);
            } else {
                pointTree.prefetchDocValues(visitor);
            }
            return;
        }
        // For CELL_INSIDE_QUERY, check if we can skip right child
        if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
            long leftSize = pointTree.size();
            long needed = size - docCount[0];

            if (leftSize >= needed) {
                // Process only left child
                intersectLeft(visitor, pointTree, docCount);
                pointTree.moveToParent();
                return;
            }
        }
        // We need both children - now clone right
        PointValues.PointTree rightChild = null;
        if (pointTree.moveToSibling()) {
            rightChild = pointTree.clone();
            pointTree.moveToParent();
            pointTree.moveToChild();
        }
        // Process both children: left first, then right if needed
        intersectLeft(visitor, pointTree, docCount);
        if (docCount[0] < size && rightChild != null) {
            intersectLeft(visitor, rightChild, docCount);
        }
        pointTree.moveToParent();
    }


    public ApproximatePointRangeScorerSupplier(PointRangeQuery pointRangeQuery,
                                               LeafReader reader, PointValues values,
                                               int size, ConstantScoreWeight constantScoreWeight,
                                               ScoreMode scoreMode) throws IOException {
        this.pointRangeQuery = pointRangeQuery;
        this.comparator = ArrayUtil.getUnsignedComparator(pointRangeQuery.getBytesPerDim());
        this.pointTree = values.getPointTree();
        this.pointTreeWithPrefetching = values.getPointTree();
        resultWithPrefetching = new DocIdSetBuilder(reader.maxDoc(), values);
        this.size = size;
        this.scoreMode = scoreMode;
        this.constantScoreWeight = constantScoreWeight;
        this.values = values;
        visitorWithPrefetching = getPrefetchingIntersectVisitor(resultWithPrefetching, docCountWithPrefetching);
        result = new DocIdSetBuilder(reader.maxDoc(), values);
        this.visitor = getIntersectVisitor(result, docCount);
        this.cost = -1;
        String name = pointTree.name();
        long st = System.currentTimeMillis();
        if (ENABLE_PREFETCH) {
            intersectLeft(pointTreeWithPrefetching, visitorWithPrefetching, docCount);
            long travelTime = System.currentTimeMillis() - st;
            logger.info("Travel time with prefetching: {} ms for {} ", travelTime, name);
        } else  {
            intersectLeft2(pointTree, visitor, docCount);
            long travelTime = System.currentTimeMillis() - st;
            logger.info("Travel time without prefetching: {} ms for {} ", travelTime, name);
        }
    }
    /**
     * Get the {@link Scorer}. This may not return {@code null} and must be called at most once.
     *
     * @param leadCost Cost of the scorer that will be used in order to lead iteration. This can be
     *                 interpreted as an upper bound of the number of times that {@link DocIdSetIterator#nextDoc},
     *                 {@link DocIdSetIterator#advance} and {@link TwoPhaseIterator#matches} will be called. Under
     *                 doubt, pass {@link Long#MAX_VALUE}, which will produce a {@link Scorer} that has good
     *                 iteration capabilities.
     */
    @Override
    public Scorer get(long leadCost) throws IOException {
        String name = pointTree.name();
        long st = System.currentTimeMillis();
        if (ENABLE_PREFETCH) {
            st = System.currentTimeMillis();
            pointTreeWithPrefetching.visitMatchingDocIDs(visitorWithPrefetching);
            pointTreeWithPrefetching.visitMatchingDocValues(visitorWithPrefetching);
            DocIdSetIterator iterator = resultWithPrefetching.build().iterator();
            long elapsed = System.currentTimeMillis() - st;
            logger.info("It took {} ms for {} with prefetching", elapsed, name);
            return new ConstantScoreScorer(this.constantScoreWeight.score(), scoreMode, iterator);
        } else  {
            DocIdSetIterator iterator = result.build().iterator();
            long elapsed = System.currentTimeMillis() - st;
            logger.info("It took {} ms for {} without prefetching", elapsed, name);
            return new ConstantScoreScorer(this.constantScoreWeight.score(), scoreMode, iterator);
        }
    }

    /**
     * Get an estimate of the {@link Scorer} that would be returned by {@link #get}. This may be a
     * costly operation, so it should only be called if necessary.
     *
     * @see DocIdSetIterator#cost
     */
    @Override
    public long cost() {
        if (cost == -1) {
            // Computing the cost may be expensive, so only do it if necessary
            cost = values.estimateDocCount(visitor);
            assert cost >= 0;
        }
        return cost;
    }

    private PointValues.Relation relate(byte[] minPackedValue, byte[] maxPackedValue) {
        boolean crosses = false;

        for (int dim = 0; dim < pointRangeQuery.getNumDims(); dim++) {
            int offset = dim * pointRangeQuery.getBytesPerDim();

            if (comparator.compare(minPackedValue, offset, pointRangeQuery.getUpperPoint(), offset) > 0
                || comparator.compare(maxPackedValue, offset, pointRangeQuery.getLowerPoint(), offset) < 0) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }

            crosses |= comparator.compare(minPackedValue, offset, pointRangeQuery.getLowerPoint(), offset) < 0
                || comparator.compare(maxPackedValue, offset, pointRangeQuery.getUpperPoint(), offset) > 0;
        }

        if (crosses) {
            return PointValues.Relation.CELL_CROSSES_QUERY;
        } else {
            return PointValues.Relation.CELL_INSIDE_QUERY;
        }
    }
}
