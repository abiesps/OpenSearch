/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.bkd.BKDReader;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.env.Environment;
import org.opensearch.search.aggregations.bucket.filterrewrite.rangecollector.RangeCollector;
import org.opensearch.search.aggregations.bucket.filterrewrite.rangecollector.SimpleRangeCollector;
import org.opensearch.search.aggregations.bucket.filterrewrite.rangecollector.SubAggRangeCollector;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Utility class for traversing a {@link PointValues.PointTree} and collecting document counts for the ranges.
 *
 * <p>The main entry point is the {@link #multiRangesTraverse} method
 *
 * <p>The class uses a {@link RangeCollector} to keep track of the active ranges and
 * determine which parts of the tree to visit. The {@link
 * PointValues.IntersectVisitor} implementation is responsible for the actual visitation and
 * document count collection.
 */
public final class PointTreeTraversal {
    private PointTreeTraversal() {}

    private static final Logger logger = LogManager.getLogger(Helper.loggerName);

    /**
     * Creates an appropriate RangeCollector based on whether sub-aggregations are needed.
     */
    static RangeCollector createCollector(
        Ranges ranges,
        BiConsumer<Integer, Integer> incrementRangeDocCount,
        int maxNumNonZeroRange,
        int activeIndex,
        Function<Integer, Long> getBucketOrd,
        FilterRewriteOptimizationContext.OptimizeResult result,
        FilterRewriteOptimizationContext.SubAggCollectorParam subAggCollectorParam
    ) {
        if (subAggCollectorParam == null) {
            return new SimpleRangeCollector(ranges, incrementRangeDocCount, maxNumNonZeroRange, activeIndex, result);
        } else {
            return new SubAggRangeCollector(
                ranges,
                incrementRangeDocCount,
                maxNumNonZeroRange,
                activeIndex,
                result,
                getBucketOrd,
                subAggCollectorParam
            );
        }
    }


    static boolean ENABLE_PREFETCH = false;
    static {

        String doubleTraversalStr = System.getenv("ENABLE_PREFETCH");
        if (doubleTraversalStr == null || doubleTraversalStr.equalsIgnoreCase("false")) {
            ENABLE_PREFETCH = false;
        } else {
            ENABLE_PREFETCH = true;
        }
    }

    static ExecutorService executors = Executors.newVirtualThreadPerTaskExecutor();
    /**
     * Traverses the given {@link PointValues.PointTree} and collects document counts for the intersecting ranges.
     *
     * @param tree      the point tree to traverse
     * @param collector the collector to use for gathering results
     * @param prefetchingRangeCollector the collector to use for gathering results
     * @return a {@link FilterRewriteOptimizationContext.OptimizeResult} object containing debug information about the traversal
     */
    static FilterRewriteOptimizationContext.OptimizeResult multiRangesTraverse(final PointValues.PointTree tree, RangeCollector collector,
                                                                               RangeCollector prefetchingRangeCollector)
        throws IOException {

        if (!ENABLE_PREFETCH) {
            logger.info("Taking normal path");
            PointValues.IntersectVisitor visitor = getIntersectVisitor(collector);
            try {
                org.opensearch.search.internal.ExitableDirectoryReader.ExitablePointTree exitablePointTree = (org.opensearch.search.internal.ExitableDirectoryReader.ExitablePointTree) tree;
                logger.info("Size of inner nodes {} ", exitablePointTree.innerNodesSize());
                long st = System.currentTimeMillis();
                intersectWithRanges2(visitor, tree, collector);
                long et = System.currentTimeMillis();
                logger.info("IntersectWithRanges traversed in {} ms for segment {} ms", (et - st), collector);
                logger.info("Total number of docs after leaf visit as per collector {} and it took {} ms ", collector.docCount(),
                    et - st);
                logger.info("Traversal tree state without prefetching {} for tree {} ", exitablePointTree.logState(), exitablePointTree);
            } catch (CollectionTerminatedException e) {
                logger.debug("Early terminate since no more range to collect");
            }
            collector.finalizePreviousRange();
            return collector.getResult();
        } else {
            logger.info("Taking prefetch path");
            PointValues.IntersectVisitor visitor = getIntersectVisitor(collector);
            try {
                org.opensearch.search.internal.ExitableDirectoryReader.ExitablePointTree exitablePointTree = (org.opensearch.search.internal.ExitableDirectoryReader.ExitablePointTree) tree;
                logger.info("Size of inner nodes {} for {} ", exitablePointTree.innerNodesSize(), exitablePointTree.name());
                long st = System.currentTimeMillis();
                intersectWithRanges(visitor, tree, collector);
                long et = System.currentTimeMillis();
                logger.info("IntersectWithRanges traversed in {} ms for segment {} ms", (et - st), exitablePointTree.name());
                Set<Long> longs = exitablePointTree.leafBlocks();
                //logger.info("Total number of docs as per collector before actual leaf visit {} ", collector.docCount());
                //logger.info("All leaf blocks that we need to prefetch {} ", longs);
//                st = System.currentTimeMillis();
//                for (Long leafBlock : longs) {
//                    exitablePointTree.prefetch(leafBlock);
//                }
//                et = System.currentTimeMillis();
//                logger.info("Time to prefetch {} leaves is {} ms for segment {}", longs.size(), (et - st), exitablePointTree.name());
                st = System.currentTimeMillis();


                /// concurrency here[?]

                CountDownLatch latch = new CountDownLatch(longs.size());
                for (Long leafBlock : longs) {
                    if (leafBlock == 0) {
                        //System.out.println("Skipping leaf block " + leafBlock);
                        //latch.countDown();
                        continue;
                    }
                    exitablePointTree.visitDocValues(visitor, leafBlock);
//                    executors.execute(() -> {
//                        org.opensearch.search.internal.ExitableDirectoryReader.ExitablePointTree clone = (org.opensearch.search.internal.ExitableDirectoryReader.ExitablePointTree) exitablePointTree.clone();
//                        PointValues.IntersectVisitor localVisitor = getIntersectVisitor(collector);
//                        try {
//                            clone.visitDocValues(localVisitor, leafBlock);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        } finally {
//                            latch.countDown();
//                        }
//                    });
                    //System.out.println("Visiting leaf block " + leafBlock);

                }
                //latch.await(10, TimeUnit.HOURS);
                et = System.currentTimeMillis();

                logger.info("Total number of docs after leaf visit as per collector {} and it took {} ms for {}  ", collector.docCount(),
                    et - st, exitablePointTree.name());
                //logger.info("Traversal tree state with prefetching {} for tree {} ", exitablePointTree.logState(), exitablePointTree);
            } catch (CollectionTerminatedException e) {
                logger.info("Early terminate since no more range to collect");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            collector.finalizePreviousRange();
            return collector.getResult();
        }
    }



    /**
     * Traverses the given {@link PointValues.PointTree} and collects document counts for the intersecting ranges.
     *
     * @param tree      the point tree to traverse
     * @param collector the collector to use for gathering results
     * @return a {@link FilterRewriteOptimizationContext.OptimizeResult} object containing debug information about the traversal
     */
    static FilterRewriteOptimizationContext.OptimizeResult multiRangesTraverse(final PointValues.PointTree tree, RangeCollector collector)
        throws IOException {
        PointValues.IntersectVisitor visitor = getIntersectVisitor(collector);
        //RangeCollector collector2
        try {
            intersectWithRanges(visitor, tree, collector);
        } catch (CollectionTerminatedException e) {
            logger.debug("Early terminate since no more range to collect");
        }
        collector.finalizePreviousRange();
        return collector.getResult();
    }

    private static void intersectWithRanges(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree, RangeCollector collector)
        throws IOException {

        org.opensearch.search.internal.ExitableDirectoryReader.ExitablePointTree exitablePointTree = (org.opensearch.search.internal.ExitableDirectoryReader.ExitablePointTree) pointTree;
       // BKDReader.BKDPointTree bkdPointTree = (BKDReader.BKDPointTree) exitablePointTree;
        PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
        //logger.info("Intersect with ranges is called for segment {} thread name {} thread id {}",
         //   collector, Thread.currentThread().getName(), Thread.currentThread().getId());
        //logger.info(exitablePointTree.logState());
        switch (r) {
            case CELL_INSIDE_QUERY:
                collector.countNode((int) pointTree.size());
                if (collector.hasSubAgg()) {
                    //System.out.println("Any subaggregations ??");
                    pointTree.visitDocIDs(visitor);
                } else {
                    //System.out.println("Collector visit inner");
                    //collector.visitInner();//what does it do ?//Just debug I can remove this//
                }
                break;
            case CELL_CROSSES_QUERY:
                if (pointTree.moveToChild()) {
                    do {
                        intersectWithRanges(visitor, pointTree, collector);
                    } while (pointTree.moveToSibling());
                    pointTree.moveToParent();
                } else {
                    //logger.info("Now visiting leaf {} ", exitablePointTree.logState());
                    exitablePointTree.resetNodeDataPosition();
                    exitablePointTree.markLeafForVisiting();
                   // pointTree.visitDocValues(visitor);//
                   // collector.visitLeaf();only for debugging.

                }
                break;
            case CELL_OUTSIDE_QUERY:
        }

    }

    private static void intersectWithRanges2(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree, RangeCollector collector)
        throws IOException {

        PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());

        switch (r) {
            case CELL_INSIDE_QUERY:
                collector.countNode((int) pointTree.size());
                if (collector.hasSubAgg()) {
                    pointTree.visitDocIDs(visitor);
                } else {
                    collector.visitInner();//what does it do ?//Just debug I can remove this//
                }
                break;
            case CELL_CROSSES_QUERY:
                if (pointTree.moveToChild()) {
                    do {
                        intersectWithRanges2(visitor, pointTree, collector);
                    } while (pointTree.moveToSibling());
                    pointTree.moveToParent();
                } else {
                     pointTree.visitDocValues(visitor);//
                     collector.visitLeaf();//only for debugging.
                }
                break;
            case CELL_OUTSIDE_QUERY:
        }

    }

    private static PointValues.IntersectVisitor getIntersectLeafCachingVisitor(RangeCollector collector) {
        return new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {
                collector.collectDocId(docID);
            }

            @Override
            public void visit(DocIdSetIterator iterator) throws IOException {
                collector.collectDocIdSet(iterator);
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
                visitPoints(packedValue, () -> {
                    collector.count();
                    if (collector.hasSubAgg()) {
                        collector.collectDocId(docID);
                    }
                });
            }

            @Override
            public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                visitPoints(packedValue, () -> {
                    // note: iterator can only iterate once
                    for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
                        collector.count();
                        if (collector.hasSubAgg()) {
                            collector.collectDocId(doc);
                        }
                    }
                });
            }

            private void visitPoints(byte[] packedValue, CheckedRunnable<IOException> collect) throws IOException {
                if (!collector.withinUpperBound(packedValue)) {
                    collector.finalizePreviousRange();
                    if (collector.iterateRangeEnd(packedValue, true)) {
                        throw new CollectionTerminatedException();
                    }
                }

                if (collector.withinRange(packedValue)) {
                    collect.run();
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                // try to find the first range that may collect values from this cell
                if (!collector.withinUpperBound(minPackedValue)) {
                    collector.finalizePreviousRange();
                    if (collector.iterateRangeEnd(minPackedValue, false)) {
                        throw new CollectionTerminatedException();
                    }
                }
                // after the loop, min < upper
                // cell could be outside [min max] lower
                if (!collector.withinLowerBound(maxPackedValue)) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                if (collector.withinRange(minPackedValue) && collector.withinRange(maxPackedValue)) {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        };
    }


    private static PointValues.IntersectVisitor getIntersectVisitor(RangeCollector collector) {
        return new PointValues.IntersectVisitor() {

            public int totalDocsVisited() {
                return collector.docCount();
            }
            @Override
            public void visit(int docID) {
                collector.collectDocId(docID);
            }

            @Override
            public void visit(DocIdSetIterator iterator) throws IOException {
                collector.collectDocIdSet(iterator);
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
                visitPoints(packedValue, () -> {
                    collector.count();
                    if (collector.hasSubAgg()) {
                        collector.collectDocId(docID);
                    }
                });
            }

            @Override
            public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                visitPoints(packedValue, () -> {
                    // note: iterator can only iterate once
                    for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
                        collector.count();
                        if (collector.hasSubAgg()) {
                            collector.collectDocId(doc);
                        }
                    }
                });
            }

            private void visitPoints(byte[] packedValue, CheckedRunnable<IOException> collect) throws IOException {
                if (!collector.withinUpperBound(packedValue)) {
                    collector.finalizePreviousRange();
                    if (collector.iterateRangeEnd(packedValue, true)) {
                        throw new CollectionTerminatedException();
                    }
                }

                if (collector.withinRange(packedValue)) {
                    collect.run();
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                // try to find the first range that may collect values from this cell
                if (!collector.withinUpperBound(minPackedValue)) {
                    collector.finalizePreviousRange();
                    if (collector.iterateRangeEnd(minPackedValue, false)) {
                        throw new CollectionTerminatedException();
                    }
                }
                // after the loop, min < upper
                // cell could be outside [min max] lower
                if (!collector.withinLowerBound(maxPackedValue)) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                if (collector.withinRange(minPackedValue) && collector.withinRange(maxPackedValue)) {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        };
    }
}
