/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The cancellation wrappers of {@link ExitableDirectoryReader} forward the leaf-prefetch hook of
 * {@link PointValues#intersect(PointValues.IntersectVisitor)}: the tree passes the caller's visitor to
 * the wrapped tree, and the visitor wrapper reports the caller's opt-in.
 */
public class ExitablePointTreePrefetchTests extends OpenSearchTestCase {

    /** Records what reaches the wrapped point tree. */
    private static final class Recorded {
        final List<PointValues.IntersectVisitor> prefetchVisitors = new ArrayList<>();
        final List<Boolean> visitedOptIns = new ArrayList<>();
    }

    public void testTreeAndVisitorForwardPrefetch() throws IOException {
        Recorded recorded = new Recorded();
        AtomicBoolean cancelled = new AtomicBoolean(false);
        try (Directory dir = newIndex(); DirectoryReader reader = wrap(DirectoryReader.open(dir), recorded, cancelled)) {
            PointValues values = reader.leaves().get(0).reader().getPointValues("f");
            assertNotNull(values);

            Visitor optIn = new Visitor(true);
            values.intersect(optIn);
            assertEquals(1, recorded.prefetchVisitors.size());
            assertSame(optIn, recorded.prefetchVisitors.get(0));
            assertEquals(100, optIn.count);
            // visitDocValues gets the cancellation wrapper, which reports the caller's opt-in
            assertFalse(recorded.visitedOptIns.isEmpty());
            assertTrue(recorded.visitedOptIns.stream().allMatch(b -> b));

            recorded.prefetchVisitors.clear();
            recorded.visitedOptIns.clear();
            Visitor noOptIn = new Visitor(false);
            values.intersect(noOptIn);
            assertTrue(recorded.prefetchVisitors.isEmpty());
            assertEquals(100, noOptIn.count);
            assertTrue(recorded.visitedOptIns.stream().noneMatch(b -> b));

            // a cancelled query stops before the prefetch reaches the wrapped tree
            PointValues.PointTree tree = values.getPointTree();
            cancelled.set(true);
            expectThrows(TaskCancelledException.class, () -> tree.prefetchIntersect(optIn));
            assertTrue(recorded.prefetchVisitors.isEmpty());
        }
    }

    private static Directory newIndex() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 100; i++) {
                Document doc = new Document();
                doc.add(new LongPoint("f", i));
                w.addDocument(doc);
            }
            w.forceMerge(1);
        }
        return dir;
    }

    private static DirectoryReader wrap(DirectoryReader in, Recorded recorded, AtomicBoolean cancelled) throws IOException {
        DirectoryReader recording = new FilterDirectoryReader(in, new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return new FilterLeafReader(reader) {
                    @Override
                    public PointValues getPointValues(String field) throws IOException {
                        PointValues values = in.getPointValues(field);
                        return values == null ? null : new RecordingPointValues(values, recorded);
                    }

                    @Override
                    public CacheHelper getCoreCacheHelper() {
                        return in.getCoreCacheHelper();
                    }

                    @Override
                    public CacheHelper getReaderCacheHelper() {
                        return in.getReaderCacheHelper();
                    }
                };
            }
        }) {
            @Override
            protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }
        };
        return new ExitableDirectoryReader(recording, new ExitableDirectoryReader.QueryCancellation() {
            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public void checkCancelled() {
                if (cancelled.get()) {
                    throw new TaskCancelledException("cancelled");
                }
            }
        });
    }

    /** Range [0, 99] over the values 0..99 with a CROSSES root, so leaves are visited with values. */
    private static final class Visitor implements PointValues.IntersectVisitor {
        final boolean optIn;
        int count;

        Visitor(boolean optIn) {
            this.optIn = optIn;
        }

        @Override
        public void visit(int docID) {
            count++;
        }

        @Override
        public void visit(int docID, byte[] packedValue) {
            count++;
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }

        @Override
        public boolean prefetchIntersect() {
            return optIn;
        }
    }

    private static final class RecordingPointValues extends PointValues {
        private final PointValues in;
        private final Recorded recorded;

        RecordingPointValues(PointValues in, Recorded recorded) {
            this.in = in;
            this.recorded = recorded;
        }

        @Override
        public PointTree getPointTree() throws IOException {
            return new RecordingTree(in.getPointTree(), recorded);
        }

        @Override
        public byte[] getMinPackedValue() throws IOException {
            return in.getMinPackedValue();
        }

        @Override
        public byte[] getMaxPackedValue() throws IOException {
            return in.getMaxPackedValue();
        }

        @Override
        public int getNumDimensions() throws IOException {
            return in.getNumDimensions();
        }

        @Override
        public int getNumIndexDimensions() throws IOException {
            return in.getNumIndexDimensions();
        }

        @Override
        public int getBytesPerDimension() throws IOException {
            return in.getBytesPerDimension();
        }

        @Override
        public long size() {
            return in.size();
        }

        @Override
        public int getDocCount() {
            return in.getDocCount();
        }
    }

    private static final class RecordingTree implements PointValues.PointTree {
        private final PointValues.PointTree in;
        private final Recorded recorded;

        RecordingTree(PointValues.PointTree in, Recorded recorded) {
            this.in = in;
            this.recorded = recorded;
        }

        @Override
        public PointValues.PointTree clone() {
            return new RecordingTree(in.clone(), recorded);
        }

        @Override
        public boolean moveToChild() throws IOException {
            return in.moveToChild();
        }

        @Override
        public boolean moveToSibling() throws IOException {
            return in.moveToSibling();
        }

        @Override
        public boolean moveToParent() throws IOException {
            return in.moveToParent();
        }

        @Override
        public byte[] getMinPackedValue() {
            return in.getMinPackedValue();
        }

        @Override
        public byte[] getMaxPackedValue() {
            return in.getMaxPackedValue();
        }

        @Override
        public long size() {
            return in.size();
        }

        @Override
        public void visitDocIDs(PointValues.IntersectVisitor visitor) throws IOException {
            in.visitDocIDs(visitor);
        }

        @Override
        public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
            recorded.visitedOptIns.add(visitor.prefetchIntersect());
            in.visitDocValues(visitor);
        }

        @Override
        public void prefetchIntersect(PointValues.IntersectVisitor visitor) throws IOException {
            recorded.prefetchVisitors.add(visitor);
            in.prefetchIntersect(visitor);
        }
    }
}
