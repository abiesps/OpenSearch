/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.index.store.OpenSearchBaseDirectoryTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;

import static org.opensearch.plugin.store.bufferpool.BlockCache.BLOCK_SIZE;

public class BufferPoolDirectoryTests extends OpenSearchBaseDirectoryTestCase {

    @Override
    protected Directory getDirectory(Path file) throws IOException {
        // a cache of a few blocks exercises eviction; prefetch runs on the calling thread
        final long maxBytes = random().nextBoolean() ? 4L * BLOCK_SIZE : 64L * BLOCK_SIZE;
        return new BufferPoolDirectory(file, FSLockFactory.getDefault(), new BlockCache(maxBytes, Runnable::run));
    }

    private static byte[] randomData(int size) {
        final byte[] data = new byte[size];
        random().nextBytes(data);
        return data;
    }

    private static void write(Directory dir, String name, byte[] data) throws IOException {
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }
    }

    public void testReadsAcrossBlockBoundaries() throws IOException {
        final byte[] data = randomData(3 * BLOCK_SIZE + random().nextInt(BLOCK_SIZE));
        final ByteBuffer expected = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        try (Directory dir = getDirectory(createTempDir())) {
            write(dir, "multi_block", data);
            try (IndexInput in = dir.openInput("multi_block", IOContext.DEFAULT)) {
                assertEquals(data.length, in.length());
                for (int iter = 0; iter < 2000; iter++) {
                    // positions clustered around block boundaries
                    final int boundary = BLOCK_SIZE * (1 + random().nextInt(3));
                    final int pos = Math.max(0, Math.min(data.length - 8, boundary - 8 + random().nextInt(16)));
                    in.seek(pos);
                    switch (random().nextInt(5)) {
                        case 0 -> assertEquals(data[pos], in.readByte());
                        case 1 -> assertEquals(expected.getShort(pos), in.readShort());
                        case 2 -> assertEquals(expected.getInt(pos), in.readInt());
                        case 3 -> assertEquals(expected.getLong(pos), in.readLong());
                        default -> {
                            final int len = Math.min(data.length - pos, random().nextInt(2 * BLOCK_SIZE));
                            final byte[] actual = new byte[len];
                            in.readBytes(actual, 0, len);
                            assertArrayEquals(Arrays.copyOfRange(data, pos, pos + len), actual);
                        }
                    }
                }
                final int sliceOffset = random().nextInt(BLOCK_SIZE);
                final RandomAccessInput slice = in.randomAccessSlice(sliceOffset, data.length - sliceOffset);
                for (int iter = 0; iter < 2000; iter++) {
                    final int p = random().nextInt(data.length - sliceOffset - 8);
                    assertEquals(expected.getLong(sliceOffset + p), slice.readLong(p));
                    assertEquals(expected.getInt(sliceOffset + p), slice.readInt(p));
                    assertEquals(expected.getShort(sliceOffset + p), slice.readShort(p));
                    assertEquals(data[sliceOffset + p], slice.readByte(p));
                }
            }
        }
    }

    public void testReplacedFileIsNotServedFromCache() throws IOException {
        final byte[] first = randomData(2 * BLOCK_SIZE + 17);
        final byte[] second = randomData(first.length);
        try (Directory dir = getDirectory(createTempDir())) {
            write(dir, "replaced", first);
            // keep a reader of the old file open across the delete, the way an old searcher would
            try (IndexInput old = dir.openInput("replaced", IOContext.DEFAULT)) {
                final byte[] read = new byte[first.length];
                old.readBytes(read, 0, read.length);
                assertArrayEquals(first, read);

                dir.deleteFile("replaced");
                write(dir, "replaced", second);

                // the old reader re-populates the cache with blocks of the old file
                old.seek(0);
                old.readBytes(read, 0, read.length);
                assertArrayEquals(first, read);

                try (IndexInput fresh = dir.openInput("replaced", IOContext.DEFAULT)) {
                    fresh.readBytes(read, 0, read.length);
                    assertArrayEquals(second, read);
                }
            }

            write(dir, "source", first);
            try (IndexInput in = dir.openInput("replaced", IOContext.DEFAULT)) {
                in.readBytes(new byte[second.length], 0, second.length);
            }
            dir.rename("source", "replaced");
            try (IndexInput in = dir.openInput("replaced", IOContext.DEFAULT)) {
                final byte[] read = new byte[first.length];
                in.readBytes(read, 0, read.length);
                assertArrayEquals(first, read);
            }
        }
    }

    public void testPrefetchLoadsBlocks() throws IOException {
        final byte[] data = randomData(4 * BLOCK_SIZE);
        final Path path = createTempDir();
        final BlockCache cache = new BlockCache(64L * BLOCK_SIZE, Runnable::run);
        try (Directory dir = new BufferPoolDirectory(path, FSLockFactory.getDefault(), cache)) {
            write(dir, "prefetched", data);
            try (IndexInput in = dir.openInput("prefetched", IOContext.DEFAULT)) {
                assertEquals(0, cache.size());
                // spans the end of block 0 and the start of block 2
                in.prefetch(BLOCK_SIZE - 1, BLOCK_SIZE + 2);
                assertEquals(3, cache.size());
                in.prefetch(0, 1);
                assertEquals(3, cache.size());
                final IndexInput slice = in.slice("slice", 3L * BLOCK_SIZE, BLOCK_SIZE);
                slice.prefetch(0, BLOCK_SIZE);
                assertEquals(4, cache.size());
                assertEquals(data[3 * BLOCK_SIZE], slice.readByte());
            }
        }
        assertEquals("closing the directory drops its blocks", 0, cache.size());
    }
}
