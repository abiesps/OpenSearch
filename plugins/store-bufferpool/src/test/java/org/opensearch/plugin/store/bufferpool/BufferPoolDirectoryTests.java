/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Constants;
import org.opensearch.index.store.OpenSearchBaseDirectoryTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

import static org.opensearch.plugin.store.bufferpool.BlockCache.DEFAULT_BLOCK_SIZE;

public class BufferPoolDirectoryTests extends OpenSearchBaseDirectoryTestCase {

    @Override
    protected Directory getDirectory(Path file) throws IOException {
        // random block and read sizes; a cache of a few blocks (possibly smaller than one read window) exercises eviction;
        // prefetch runs on the calling thread
        final int blockSize = 1 << TestUtil.nextInt(random(), 9, 17);
        final int randomReadSize = blockSize << TestUtil.nextInt(random(), 0, 3);
        final int sequentialReadSize = blockSize << TestUtil.nextInt(random(), 0, 5);
        final long maxBytes = random().nextBoolean() ? 4L * blockSize : 64L * DEFAULT_BLOCK_SIZE;
        return new BufferPoolDirectory(
            file,
            FSLockFactory.getDefault(),
            new BlockCache(maxBytes, blockSize, randomReadSize, sequentialReadSize, Runnable::run)
        );
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
        final byte[] data = randomData(3 * DEFAULT_BLOCK_SIZE + random().nextInt(DEFAULT_BLOCK_SIZE));
        final ByteBuffer expected = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        try (Directory dir = getDirectory(createTempDir())) {
            write(dir, "multi_block", data);
            try (IndexInput in = dir.openInput("multi_block", IOContext.DEFAULT)) {
                assertEquals(data.length, in.length());
                for (int iter = 0; iter < 2000; iter++) {
                    // positions clustered around block boundaries
                    final int boundary = DEFAULT_BLOCK_SIZE * (1 + random().nextInt(3));
                    final int pos = Math.max(0, Math.min(data.length - 8, boundary - 8 + random().nextInt(16)));
                    in.seek(pos);
                    switch (random().nextInt(5)) {
                        case 0 -> assertEquals(data[pos], in.readByte());
                        case 1 -> assertEquals(expected.getShort(pos), in.readShort());
                        case 2 -> assertEquals(expected.getInt(pos), in.readInt());
                        case 3 -> assertEquals(expected.getLong(pos), in.readLong());
                        default -> {
                            final int len = Math.min(data.length - pos, random().nextInt(2 * DEFAULT_BLOCK_SIZE));
                            final byte[] actual = new byte[len];
                            in.readBytes(actual, 0, len);
                            assertArrayEquals(Arrays.copyOfRange(data, pos, pos + len), actual);
                        }
                    }
                }
                final int sliceOffset = random().nextInt(DEFAULT_BLOCK_SIZE);
                final RandomAccessInput slice = in.randomAccessSlice(sliceOffset, data.length - sliceOffset);
                for (int iter = 0; iter < 2000; iter++) {
                    final int p = random().nextInt(data.length - sliceOffset - 8);
                    assertEquals(expected.getLong(sliceOffset + p), slice.readLong(p));
                    assertEquals(expected.getInt(sliceOffset + p), slice.readInt(p));
                    assertEquals(expected.getShort(sliceOffset + p), slice.readShort(p));
                    assertEquals(data[sliceOffset + p], slice.readByte(p));
                    // positional bulk read, across block boundaries
                    final int len = Math.min(data.length - sliceOffset - p, random().nextInt(2 * DEFAULT_BLOCK_SIZE));
                    final byte[] actual = new byte[len];
                    slice.readBytes(p, actual, 0, len);
                    assertArrayEquals(Arrays.copyOfRange(data, sliceOffset + p, sliceOffset + p + len), actual);
                }
            }
        }
    }

    public void testReplacedFileIsNotServedFromCache() throws IOException {
        final byte[] first = randomData(2 * DEFAULT_BLOCK_SIZE + 17);
        final byte[] second = randomData(first.length);
        try (Directory dir = getDirectory(createTempDir())) {
            write(dir, "replaced", first);
            // keep a reader of the old file open across the delete, the way an old searcher would
            try (IndexInput old = dir.openInput("replaced", IOContext.DEFAULT)) {
                final byte[] read = new byte[first.length];
                old.readBytes(read, 0, read.length);
                assertArrayEquals(first, read);

                dir.deleteFile("replaced");
                // a file system that cannot delete open files (WindowsFS) leaves the delete pending, so the name
                // cannot be re-used while the old reader is open
                assumeTrue("file system does not delete open files", dir.getPendingDeletions().isEmpty());
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
        final byte[] data = randomData(4 * DEFAULT_BLOCK_SIZE);
        final Path path = createTempDir();
        final BlockCache cache = new BlockCache(64L * DEFAULT_BLOCK_SIZE, Runnable::run);
        try (Directory dir = new BufferPoolDirectory(path, FSLockFactory.getDefault(), cache)) {
            write(dir, "prefetched", data);
            try (IndexInput in = dir.openInput("prefetched", IOContext.DEFAULT)) {
                assertEquals(0, cache.size());
                // spans the end of block 0 and the start of block 2
                in.prefetch(DEFAULT_BLOCK_SIZE - 1, DEFAULT_BLOCK_SIZE + 2);
                assertEquals(3, cache.size());
                in.prefetch(0, 1);
                assertEquals(3, cache.size());
                final IndexInput slice = in.slice("slice", 3L * DEFAULT_BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
                slice.prefetch(0, DEFAULT_BLOCK_SIZE);
                assertEquals(4, cache.size());
                assertEquals(data[3 * DEFAULT_BLOCK_SIZE], slice.readByte());
            }
        }
        assertEquals("closing the directory drops its blocks", 0, cache.size());
    }

    public void testReadSizeFollowsTheIOContext() throws IOException {
        final int block = 1024;
        final int random = 4 * block;
        final int sequential = 32 * block;
        final BlockCache cache = new BlockCache(1L << 26, block, random, sequential, Runnable::run);
        final int byDefault = Constants.DEFAULT_READADVICE == ReadAdvice.RANDOM ? random : sequential;
        final byte[] data = randomData(64 * block + 7);
        try (Directory dir = new BufferPoolDirectory(createTempDir(), FSLockFactory.getDefault(), cache)) {
            write(dir, "_0.fdt", data);
            final IOContext randomContext = IOContext.DEFAULT.withHints(FileTypeHint.DATA, DataAccessHint.RANDOM);
            final IOContext merge = IOContext.merge(new MergeInfo(10, 1000, false, 1)).withHints(DataAccessHint.RANDOM);
            try (
                BufferPoolIndexInput randomInput = (BufferPoolIndexInput) dir.openInput("_0.fdt", randomContext);
                BufferPoolIndexInput defaultInput = (BufferPoolIndexInput) dir.openInput("_0.fdt", IOContext.DEFAULT);
                BufferPoolIndexInput sequentialInput = (BufferPoolIndexInput) dir.openInput(
                    "_0.fdt",
                    IOContext.DEFAULT.withHints(DataAccessHint.SEQUENTIAL)
                );
                BufferPoolIndexInput readOnce = (BufferPoolIndexInput) dir.openInput("_0.fdt", IOContext.READONCE);
                BufferPoolIndexInput merging = (BufferPoolIndexInput) dir.openInput("_0.fdt", merge)
            ) {
                assertEquals(random, randomInput.readSize());
                assertEquals(byDefault, defaultInput.readSize());
                assertEquals(sequential, sequentialInput.readSize());
                assertEquals(sequential, readOnce.readSize());
                assertEquals("merges read sequentially whatever their hints", sequential, merging.readSize());
                // clones keep the read size; slices take the one of their context, if they have one
                assertEquals(random, randomInput.clone().readSize());
                assertEquals(random, ((BufferPoolIndexInput) randomInput.slice("s", 10, 100)).readSize());
                assertEquals(sequential, ((BufferPoolIndexInput) randomInput.slice("s", 10, 100, IOContext.READONCE)).readSize());
                assertEquals(random, ((BufferPoolIndexInput) defaultInput.slice("s", 10, 100, randomContext)).readSize());

                // a random miss reads one random window, a sequential miss one sequential window
                final BlockCache.FileStats stats = cache.statsFor("_0.fdt");
                randomInput.seek(5L * block + 3);
                assertEquals(data[5 * block + 3], randomInput.readByte());
                assertEquals(Map.of((long) random, 1L), stats.readsBySize());
                sequentialInput.seek(40L * block);
                assertEquals(data[40 * block], sequentialInput.readByte());
                assertEquals(Map.of((long) random, 1L, (long) sequential, 1L), stats.readsBySize());
                assertEquals((long) random + sequential, stats.bytesRead.sum());

                // the update applies to later reads of this input only
                final BufferPoolIndexInput before = defaultInput.clone();
                defaultInput.updateIOContext(randomContext);
                assertEquals(random, defaultInput.readSize());
                assertEquals(byDefault, before.readSize());

                // every byte reads back the same through inputs of both read sizes
                final byte[] a = new byte[data.length];
                final byte[] b = new byte[data.length];
                randomInput.seek(0);
                randomInput.readBytes(a, 0, a.length);
                sequentialInput.seek(0);
                sequentialInput.readBytes(b, 0, b.length);
                assertArrayEquals(data, a);
                assertArrayEquals(data, b);
                assertEquals(stats.bytesRead.sum(), stats.bytesLoaded.sum());
                assertEquals(cache.sizeInBytes(), stats.bytesRead.sum());
                assertEquals(data.length, cache.sizeInBytes());
            }
        }
    }
}
