/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Read windows of {@link BlockCache}: alignment, clipping, de-duplication, prefetch coalescing and their counters. */
public class BlockCacheReadWindowTests extends OpenSearchTestCase {

    private static final int BLOCK = 1024;
    private static final int RANDOM = 4 * BLOCK;
    private static final int SEQUENTIAL = 16 * BLOCK;

    private byte[] data;
    private Path file;
    private FileChannel channel;

    private void open(int length) throws IOException {
        data = new byte[length];
        random().nextBytes(data);
        file = createTempDir().resolve("_0.dvd");
        Files.write(file, data);
        channel = FileChannel.open(file, StandardOpenOption.READ);
    }

    @Override
    public void tearDown() throws Exception {
        if (channel != null) {
            channel.close();
        }
        super.tearDown();
    }

    private static BlockCache cache(int randomReadSize, int sequentialReadSize) {
        // large enough that nothing is evicted; prefetch runs on the calling thread
        return new BlockCache(1L << 26, BLOCK, randomReadSize, sequentialReadSize, Runnable::run);
    }

    private BlockKey key(int block) {
        return new BlockKey(file, 1, (long) block * BLOCK);
    }

    private ByteBuffer get(BlockCache cache, int block, int readSize) throws IOException {
        final BlockCache.FileStats stats = cache.statsFor(file.getFileName().toString());
        return cache.getOrLoad(key(block), channel, data.length, readSize, stats);
    }

    private void assertBlock(int block, ByteBuffer actual) {
        final int from = block * BLOCK;
        final int length = Math.min(BLOCK, data.length - from);
        assertEquals("block " + block + " length", length, actual.limit());
        for (int i = 0; i < length; i++) {
            assertEquals("block " + block + " byte " + i, data[from + i], actual.get(i));
        }
    }

    /** Every block of the cache holds the file's bytes. */
    private void assertCachedBlocksMatch(BlockCache cache) {
        for (int b = 0; b * BLOCK < data.length; b++) {
            if (cache.contains(file, 1, (long) b * BLOCK, 1)) {
                final BlockCache.FileStats stats = cache.statsFor(file.getFileName().toString());
                try {
                    assertBlock(b, cache.getOrLoad(key(b), channel, data.length, cache.randomReadSize(), stats));
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }
    }

    private BlockCache.FileStats stats(BlockCache cache) {
        return cache.statsFor(file.getFileName().toString());
    }

    public void testMissReadsTheAlignedWindowOfTheReadSize() throws IOException {
        open(64 * BLOCK);
        final BlockCache cache = cache(RANDOM, SEQUENTIAL);
        // block 5 is in the random window of blocks 4..7
        assertBlock(5, get(cache, 5, RANDOM));
        final BlockCache.FileStats stats = stats(cache);
        assertEquals(1, stats.reads.sum());
        assertEquals(RANDOM, stats.bytesRead.sum());
        assertEquals(1, stats.loads.sum());
        assertEquals(3, stats.readaheadLoads.sum());
        assertEquals(RANDOM, stats.bytesLoaded.sum());
        assertEquals(Map.of((long) RANDOM, 1L), stats.readsBySize());
        assertTrue(cache.contains(file, 1, 4L * BLOCK, 4));
        assertFalse(cache.contains(file, 1, 3L * BLOCK, 1));
        assertFalse(cache.contains(file, 1, 8L * BLOCK, 1));
        // the window's other blocks are hits now
        for (int b = 4; b < 8; b++) {
            assertBlock(b, get(cache, b, RANDOM));
        }
        assertEquals(1, stats.reads.sum());
        // block 20 is in the sequential window of blocks 16..31
        assertBlock(20, get(cache, 20, SEQUENTIAL));
        assertEquals(2, stats.reads.sum());
        assertEquals(RANDOM + SEQUENTIAL, stats.bytesRead.sum());
        assertEquals(Map.of((long) RANDOM, 1L, (long) SEQUENTIAL, 1L), stats.readsBySize());
        assertTrue(cache.contains(file, 1, 16L * BLOCK, 16));
        assertEquals(20, cache.size());
        assertEquals(0, cache.inFlightReads());
        assertCachedBlocksMatch(cache);
    }

    public void testWindowIsClippedAtTheEndOfTheFile() throws IOException {
        // 2 sequential windows and a partial last block: the second window holds 3 whole blocks and 100 bytes
        final int length = SEQUENTIAL + 3 * BLOCK + 100;
        open(length);
        final BlockCache cache = cache(RANDOM, SEQUENTIAL);
        assertBlock(19, get(cache, 19, SEQUENTIAL));
        final BlockCache.FileStats stats = stats(cache);
        assertEquals(1, stats.reads.sum());
        assertEquals(length - SEQUENTIAL, stats.bytesRead.sum());
        assertEquals(length - SEQUENTIAL, stats.bytesLoaded.sum());
        // a 3,172-byte read falls in the 4 KiB class
        assertEquals(Map.of(4096L, 1L), stats.readsBySize());
        assertEquals(4, cache.size());
        for (int b = 16; b < 20; b++) {
            assertBlock(b, get(cache, b, SEQUENTIAL));
        }
        // a random window at the end of the file is clipped too
        final BlockCache other = cache(RANDOM, SEQUENTIAL);
        assertBlock(17, get(other, 17, RANDOM));
        assertEquals(length - SEQUENTIAL, stats(other).bytesRead.sum());
        assertEquals(0, cache.inFlightReads());
    }

    public void testCachedBlocksAreNeitherReadAgainNorReplaced() throws IOException {
        open(32 * BLOCK);
        // random reads of one block cache blocks 2 and 5 first
        final BlockCache cache = cache(BLOCK, 8 * BLOCK);
        final ByteBuffer two = get(cache, 2, BLOCK);
        final ByteBuffer five = get(cache, 5, BLOCK);
        final BlockCache.FileStats stats = stats(cache);
        assertEquals(2, stats.reads.sum());
        // a sequential miss on block 0 reads the window 0..7 as the runs 0-1, 3-4 and 6-7, the wanted run first
        assertBlock(0, get(cache, 0, 8 * BLOCK));
        assertEquals(5, stats.reads.sum());
        assertEquals(2 * BLOCK + 6 * BLOCK, stats.bytesRead.sum());
        assertEquals(Map.of((long) BLOCK, 2L, 2L * BLOCK, 3L), stats.readsBySize());
        assertEquals(3, stats.loads.sum());
        assertEquals(5, stats.readaheadLoads.sum());
        assertEquals(8, cache.size());
        assertSame("a cached block is never replaced", two, get(cache, 2, 8 * BLOCK));
        assertSame(five, get(cache, 5, 8 * BLOCK));
        assertEquals(5, stats.reads.sum());
        assertCachedBlocksMatch(cache);
    }

    public void testConcurrentMissesOnOneWindowReadItOnce() throws Exception {
        open(64 * BLOCK);
        final BlockCache cache = cache(RANDOM, SEQUENTIAL);
        // a slow read keeps the window in flight while the other threads miss on it
        cache.setSimulatedLoadLatencyNanos(TimeUnit.MILLISECONDS.toNanos(50));
        final int threads = 8;
        final CyclicBarrier barrier = new CyclicBarrier(threads);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            // all in the sequential window of blocks 16..31
            final int block = 16 + 2 * t;
            final Thread worker = new Thread(() -> {
                try {
                    barrier.await();
                    assertBlock(block, get(cache, block, SEQUENTIAL));
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        assertNull(failure.get());
        final BlockCache.FileStats stats = stats(cache);
        assertEquals("one read for the window", 1, stats.reads.sum());
        assertEquals(SEQUENTIAL, stats.bytesRead.sum());
        assertEquals(1, stats.loads.sum());
        assertEquals(15, stats.readaheadLoads.sum());
        assertEquals(threads, stats.requests.sum());
        assertEquals(0, cache.inFlightReads());
    }

    public void testConcurrentMixedReadSizesReadEveryBlockOnce() throws Exception {
        open(256 * BLOCK + random().nextInt(BLOCK));
        final int blocks = (data.length + BLOCK - 1) / BLOCK;
        final BlockCache cache = cache(RANDOM, SEQUENTIAL);
        if (random().nextBoolean()) {
            cache.setSimulatedLoadLatencyNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        final int threads = 6;
        final CyclicBarrier barrier = new CyclicBarrier(threads);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final List<Thread> workers = new ArrayList<>();
        final long seed = random().nextLong();
        for (int t = 0; t < threads; t++) {
            final int thread = t;
            final Thread worker = new Thread(() -> {
                final java.util.Random r = new java.util.Random(seed + thread);
                try {
                    barrier.await();
                    for (int i = 0; i < 200; i++) {
                        final int block = r.nextInt(blocks);
                        switch (r.nextInt(3)) {
                            case 0 -> assertBlock(block, get(cache, block, RANDOM));
                            case 1 -> assertBlock(block, get(cache, block, SEQUENTIAL));
                            default -> cache.prefetch(
                                file,
                                1,
                                channel,
                                data.length,
                                (long) block * BLOCK,
                                Math.min(blocks - block, 1 + r.nextInt(8)),
                                stats(cache)
                            );
                        }
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        assertNull(failure.get());
        final BlockCache.FileStats stats = stats(cache);
        // nothing is evicted, so a block read twice would show as more bytes read than cached
        assertEquals(cache.sizeInBytes(), stats.bytesRead.sum());
        assertEquals(stats.bytesRead.sum(), stats.bytesLoaded.sum());
        assertEquals(cache.size(), stats.loads.sum() + stats.prefetchLoads.sum() + stats.readaheadLoads.sum());
        assertEquals(stats.reads.sum() + stats.prefetchReads.sum(), stats.readsBySize().values().stream().mapToLong(Long::longValue).sum());
        for (long size : stats.readsBySize().keySet()) {
            assertTrue("read of " + size + " bytes", size <= SEQUENTIAL);
        }
        assertEquals(0, cache.inFlightReads());
        assertCachedBlocksMatch(cache);
    }

    public void testPrefetchIsAlignedAndCoalescedIntoSequentialWindows() throws IOException {
        open(64 * BLOCK);
        final BlockCache cache = cache(RANDOM, SEQUENTIAL);
        final BlockCache.FileStats stats = stats(cache);
        // blocks 10..21 touch the sequential windows 0..15 and 16..31
        cache.prefetch(file, 1, channel, data.length, 10L * BLOCK, 12, stats);
        assertEquals(2, stats.prefetchReads.sum());
        assertEquals(0, stats.reads.sum());
        assertEquals(2L * SEQUENTIAL, stats.bytesRead.sum());
        assertEquals(12, stats.prefetchRequests.sum());
        assertEquals(12, stats.prefetchLoads.sum());
        assertEquals(20, stats.readaheadLoads.sum());
        assertEquals(Map.of((long) SEQUENTIAL, 2L), stats.readsBySize());
        assertTrue(cache.contains(file, 1, 0, 32));
        // all requested blocks cached: no read, even though the next window is missing
        cache.prefetch(file, 1, channel, data.length, 30L * BLOCK, 2, stats);
        assertEquals(2, stats.prefetchReads.sum());
        assertCachedBlocksMatch(cache);
    }

    public void testPrefetchSkipsCachedBlocksOfItsWindows() throws IOException {
        open(32 * BLOCK);
        final BlockCache cache = cache(BLOCK, SEQUENTIAL);
        final BlockCache.FileStats stats = stats(cache);
        get(cache, 3, BLOCK);
        // window 0..15 misses all blocks but 3: runs 0-2 and 4-15; window 16..31 is one run
        cache.prefetch(file, 1, channel, data.length, 0, 20, stats);
        assertEquals(1, stats.reads.sum());
        assertEquals(3, stats.prefetchReads.sum());
        assertEquals(BLOCK + 31L * BLOCK, stats.bytesRead.sum());
        assertEquals(Map.of((long) BLOCK, 1L, 4L * BLOCK, 1L, 16L * BLOCK, 2L), stats.readsBySize());
        assertEquals(19, stats.prefetchLoads.sum());
        assertEquals(12, stats.readaheadLoads.sum());
        assertCachedBlocksMatch(cache);
    }

    public void testFailedReadReleasesItsClaims() throws IOException {
        open(64 * BLOCK);
        final BlockCache cache = cache(RANDOM, SEQUENTIAL);
        final FileChannel closed = FileChannel.open(file, StandardOpenOption.READ);
        closed.close();
        expectThrows(ClosedChannelException.class, () -> cache.getOrLoad(key(5), closed, data.length, RANDOM, stats(cache)));
        assertEquals(0, cache.inFlightReads());
        assertEquals(0, cache.size());
        // a failed prefetch is dropped, and releases its claims too
        cache.prefetch(file, 1, closed, data.length, 0, 20, stats(cache));
        assertEquals(0, cache.inFlightReads());
        assertEquals(0, cache.size());
        // the blocks load again through an open channel
        assertBlock(5, get(cache, 5, RANDOM));
        assertEquals(4, cache.size());
    }

    public void testOneBlockReadsWhenReadSizesEqualTheBlockSize() throws IOException {
        open(8 * BLOCK + 10);
        final BlockCache cache = new BlockCache(1L << 26, BLOCK, Runnable::run);
        assertEquals(BLOCK, cache.randomReadSize());
        assertEquals(BLOCK, cache.sequentialReadSize());
        for (int b = 0; b <= 8; b++) {
            assertBlock(b, get(cache, b, BLOCK));
        }
        final BlockCache.FileStats stats = stats(cache);
        assertEquals(9, stats.reads.sum());
        assertEquals(9, stats.loads.sum());
        assertEquals(0, stats.readaheadLoads.sum());
        assertEquals(data.length, stats.bytesRead.sum());
        assertEquals(Map.of((long) BLOCK, 8L, 16L, 1L), stats.readsBySize());
        stats.reset();
        assertEquals(Map.of(), stats.readsBySize());
        assertEquals(0, stats.bytesRead.sum());
    }

    public void testSizeClass() {
        assertEquals(0, BlockCache.sizeClass(1));
        assertEquals(1, BlockCache.sizeClass(2));
        assertEquals(2, BlockCache.sizeClass(3));
        assertEquals(13, BlockCache.sizeClass(8192));
        assertEquals(14, BlockCache.sizeClass(8193));
        assertEquals(BlockCache.READ_SIZE_CLASSES - 1, BlockCache.sizeClass(BlockCache.MAX_READ_SIZE));
    }

    public void testInvalidReadSizesAreRejected() {
        expectThrows(IllegalArgumentException.class, () -> new BlockCache(1 << 20, BLOCK, BLOCK / 2, BLOCK, Runnable::run));
        expectThrows(IllegalArgumentException.class, () -> new BlockCache(1 << 20, BLOCK, BLOCK, 3 * BLOCK, Runnable::run));
        expectThrows(
            IllegalArgumentException.class,
            () -> new BlockCache(1 << 20, BLOCK, 2 * BlockCache.MAX_READ_SIZE, BLOCK, Runnable::run)
        );
        expectThrows(IllegalArgumentException.class, () -> new BlockCache(1 << 20, 3000, 4096, 4096, Runnable::run));
    }
}
