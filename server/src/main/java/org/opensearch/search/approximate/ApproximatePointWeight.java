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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.IntsRef;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.opensearch.search.aggregations.bucket.filterrewrite.PointTreeTraversal.ENABLE_PREFETCH;

public class ApproximatePointWeight extends ConstantScoreWeight {

    private  IndexSearcher searcher;
    private  ArrayUtil.ByteArrayComparator comparator;
    private ScoreMode scoreMode;
    private SortOrder sortOrder;
    private Weight pointRangeQueryWeight;
    private int size;
    PointRangeQuery query;
    static ExecutorService lookupExecutors = Executors.newVirtualThreadPerTaskExecutor();
    private static final Logger logger = LogManager.getLogger(ApproximatePointWeight.class);
    ConcurrentHashMap<Integer, PointValues.PointTree> pointTreeMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, PointValues.IntersectVisitor> visitorConcurrentHashMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, DocIdSetBuilder> resultMap = new ConcurrentHashMap<>();


    private ApproximatePointWeight(Query query, float score) {
        super(query, score);
    }

    protected ApproximatePointWeight(ApproximatePointRangeQuery query, float score,
                                     int size, Weight pointRangeQueryWeight, SortOrder sortOrder, ScoreMode scoreMode,
                                     IndexSearcher searcher) throws IOException {
        this(query, score);
        this.size = size;
        this.pointRangeQueryWeight = pointRangeQueryWeight;
        this.sortOrder = sortOrder;
        this.scoreMode = scoreMode;
        this.comparator = ArrayUtil.getUnsignedComparator(query.getBytesPerDim());
        this.searcher = searcher;
        logger.info("==================================================================Taking new code path or not ?======================");

        long s = System.currentTimeMillis();
        //do lookup here
        if (ENABLE_PREFETCH) {
            //do lookup
            List<LeafReaderContext> leafContexts = searcher.getLeafContexts();
            CountDownLatch latch = new CountDownLatch(leafContexts.size());
            for (LeafReaderContext context : leafContexts) {
                lookupExecutors.execute( ( ) -> {
                    try {
                        long st = System.currentTimeMillis();
                        LeafReader reader = context.reader();
                        long[] docCountWithPrefetching = { 0 };
                        long[] docCount = { 0 };
                        PointValues values = reader.getPointValues(query.getField());
                        PointValues.PointTree pointTreeWithPrefetching = values.getPointTree();
                        String name = pointTreeWithPrefetching.name();
                        DocIdSetBuilder resultsWithPrefetching = new DocIdSetBuilder(reader.maxDoc(), values);
                        PointValues.IntersectVisitor visitorWithPrefetching = getPrefetchingIntersectVisitor(resultsWithPrefetching, docCountWithPrefetching);
                        resultMap.put(context.ord, resultsWithPrefetching);
                        pointTreeMap.put(context.ord,  pointTreeWithPrefetching);
                        visitorConcurrentHashMap.put(context.ord, visitorWithPrefetching);
                        intersectLeft(pointTreeWithPrefetching, visitorWithPrefetching, docCountWithPrefetching);
                        long travelTime = System.currentTimeMillis() - st;
                        logger.info("Travel time with prefetching: {} ms for {} total number of matching leaf fp {} ", travelTime, name,
                            visitorWithPrefetching.matchingLeafNodesfpDocIds().size() + visitorWithPrefetching.matchingLeafNodesfpDocValues().size()
                        );
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            try {
                latch.await(10, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            long elapsed = System.currentTimeMillis() - s;
            logger.info("Total elapsed time for creating weight {} ms", elapsed );
        }
    }


    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        long s = System.currentTimeMillis();
        LeafReader reader = context.reader();
        long[] docCountWithPrefetching = { 0 };
        long[] docCount = { 0 };

        PointValues values = reader.getPointValues(query.getField());
        if (checkValidPointValues(values) == false) {
            return null;
        }
        // values.size(): total points indexed, In most cases: values.size() ≈ number of documents (assuming single-valued fields)
        if (this.size > values.size()) {
            return pointRangeQueryWeight.scorerSupplier(context);
        } else {
            PointValues.PointTree pointTree = values.getPointTree();
            if (sortOrder == null || sortOrder.equals(SortOrder.ASC)) {
                PointValues.PointTree pointTreeWithPrefetching = pointTreeMap.get(context.ord);
                PointValues.IntersectVisitor visitorWithPrefetching = visitorConcurrentHashMap.get(context.ord);
                DocIdSetBuilder resultsWithPrefetching = resultMap.get(context.ord);
                return new ApproximatePointRangeScorerSupplier(query, reader, values, size, this, scoreMode,
                    pointTreeWithPrefetching, visitorWithPrefetching, resultsWithPrefetching);
            } else {
                // we need to fetch size + deleted docs since the collector will prune away deleted docs resulting in fewer results
                // than expected
                final int deletedDocs = reader.numDeletedDocs();
                size += deletedDocs;
                return new ScorerSupplier() {

                    final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values);
                    final PointValues.IntersectVisitor visitor = getIntersectVisitor(result, docCount);
                    long cost = -1;

                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        intersectRight(pointTree, visitor, docCount);
                        DocIdSetIterator iterator = result.build().iterator();
                        return new ConstantScoreScorer(score(), scoreMode, iterator);
                    }

                    @Override
                    public long cost() {
                        if (cost == -1) {
                            // Computing the cost may be expensive, so only do it if necessary
                            cost = values.estimateDocCount(visitor);
                            assert cost >= 0;
                        }
                        return cost;
                    }
                };
            }
        }

    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

    @Override
    public int count(LeafReaderContext context) throws IOException {
        return pointRangeQueryWeight.count(context);
    }

    private boolean matches(byte[] packedValue) {
        for (int dim = 0; dim < query.getNumDims(); dim++) {
            int offset = dim * query.getBytesPerDim();
            if (comparator.compare(packedValue, offset, query.getLowerPoint(), offset) < 0) {
                // Doc's value is too low, in this dimension
                return false;
            }
            if (comparator.compare(packedValue, offset, query.getUpperPoint(), offset) > 0) {
                // Doc's value is too high, in this dimension
                return false;
            }
        }
        return true;
    }

    // we pull this from PointRangeQuery since it is final
    private PointValues.Relation relate(byte[] minPackedValue, byte[] maxPackedValue) {
        boolean crosses = false;

        for (int dim = 0; dim < query.getNumDims(); dim++) {
            int offset = dim * query.getBytesPerDim();

            if (comparator.compare(minPackedValue, offset, query.getUpperPoint(), offset) > 0
                || comparator.compare(maxPackedValue, offset, query.getLowerPoint(), offset) < 0) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }

            crosses |= comparator.compare(minPackedValue, offset, query.getLowerPoint(), offset) < 0
                || comparator.compare(maxPackedValue, offset, query.getUpperPoint(), offset) > 0;
        }

        if (crosses) {
            return PointValues.Relation.CELL_CROSSES_QUERY;
        } else {
            return PointValues.Relation.CELL_INSIDE_QUERY;
        }
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

    public PointValues.IntersectVisitor getIntersectVisitor(DocIdSetBuilder result, long[] docCount) {
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

    // we pull this from PointRangeQuery since it is final
    private boolean checkValidPointValues(PointValues values) throws IOException {
        if (values == null) {
            // No docs in this segment/field indexed any points
            return false;
        }

        if (values.getNumIndexDimensions() != query.getNumDims()) {
            throw new IllegalArgumentException(
                "field=\""
                    + query.getField()
                    + "\" was indexed with numIndexDimensions="
                    + values.getNumIndexDimensions()
                    + " but this query has numDims="
                    + query.getNumDims()
            );
        }
        if (query.getBytesPerDim() != values.getBytesPerDimension()) {
            throw new IllegalArgumentException(
                "field=\""
                    + query.getField()
                    + "\" was indexed with bytesPerDim="
                    + values.getBytesPerDimension()
                    + " but this query has bytesPerDim="
                    + query.getBytesPerDim()
            );
        }
        return true;
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

    private void intersectRight(PointValues.PointTree pointTree, PointValues.IntersectVisitor visitor, long[] docCount)
        throws IOException {
        intersectRight(visitor, pointTree, docCount);
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

    // custom intersect visitor to walk the left of the tree
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
        //if (rightChild != null) {
        if (docCount[0] < size && rightChild != null) {
            intersectLeft(visitor, rightChild, docCount);
        }
        pointTree.moveToParent();
    }

    public static void compareSets(Set<Long> set1, Set<Long> set2, String name) {
        // Find common elements (Intersection)
        //System.out.println("Name : " + name + " non-prefetching size " + set1.size()  + " prefetching " + set2.size());
        Set<Long> intersection = new HashSet<>(set1);
        intersection.retainAll(set2);
        // System.out.println(" Name " + name + " Common elements: " + intersection);

        // Find elements present in set1 but not in set2 (Difference)
        Set<Long> difference = new HashSet<>(set1);
        difference.removeAll(set2);
        if (!difference.isEmpty())
            System.out.println(" Name " + name + " elements in without prefetching but not in with prefetching: " + difference);

        // Find elements present in set2 but not in set1 (Difference)
        Set<Long> reverseDifference = new HashSet<>(set2);
        reverseDifference.removeAll(set1);
        if (!reverseDifference.isEmpty())
            System.out.println(" Name " + name + " elements in with prefetching but not in without prefetching: " + reverseDifference);

        // Check if both sets are equal
        boolean areEqual = set1.equals(set2);
        if (!areEqual)
            System.out.println("Name" + name + " Are both sets equal? " + areEqual);

        // Find union of both sets
        Set<Long> union = new HashSet<>(set1);
        union.addAll(set2);
        // System.out.println("Union of both sets: " + union);
    }

    // custom intersect visitor to walk the right of tree (from rightmost leaf going left)
    public void intersectRight(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree, long[] docCount)
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
                // CELL_CROSSES_QUERY
                pointTree.visitDocValues(visitor);
            }
            return;
        }
        // Internal node - get left child reference (we're at left child initially)
        PointValues.PointTree leftChild = pointTree.clone();
        // Move to right child if it exists
        boolean hasRightChild = pointTree.moveToSibling();
        // For CELL_INSIDE_QUERY, check if we can skip left child
        if (r == PointValues.Relation.CELL_INSIDE_QUERY && hasRightChild) {
            long rightSize = pointTree.size();
            long needed = size - docCount[0];
            if (rightSize >= needed) {
                // Right child has all we need - only process right
                intersectRight(visitor, pointTree, docCount);
                pointTree.moveToParent();
                return;
            }
        }
        // Process both children: right first (for DESC), then left if needed
        if (hasRightChild) {
            intersectRight(visitor, pointTree, docCount);
        }
        if (docCount[0] < size) {
            intersectRight(visitor, leftChild, docCount);
        }
        pointTree.moveToParent();
    }
}
