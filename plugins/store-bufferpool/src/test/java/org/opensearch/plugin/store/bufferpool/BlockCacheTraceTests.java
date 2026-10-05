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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

import static org.opensearch.plugin.store.bufferpool.BlockCache.DEFAULT_BLOCK_SIZE;

public class BlockCacheTraceTests extends OpenSearchTestCase {

    private static final int BLOCKS = 4;

    private Path writeFile(String name) throws IOException {
        final byte[] data = new byte[BLOCKS * DEFAULT_BLOCK_SIZE];
        random().nextBytes(data);
        final Path file = createTempDir().resolve(name);
        Files.write(file, data);
        return file;
    }

    private static BlockKey key(Path file, int block) {
        return new BlockKey(file, 1, (long) block * DEFAULT_BLOCK_SIZE);
    }

    public void testPrefetchedButUnreadBlocksAreReported() throws IOException {
        final Path file = writeFile("_0.dvd");
        // prefetch runs on the calling thread
        final BlockCache cache = new BlockCache(64L * DEFAULT_BLOCK_SIZE, Runnable::run);
        final BlockCache.FileStats stats = cache.statsFor(file.getFileName().toString());
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            final long length = channel.size();
            cache.startTrace(1000);
            cache.prefetch(file, 1, channel, length, 0, 3, stats);
            assertEquals(3, cache.currentTrace().prefetchedUnreadCount());
            cache.getOrLoad(key(file, 0), channel, length, DEFAULT_BLOCK_SIZE, stats);
            cache.getOrLoad(key(file, 2), channel, length, DEFAULT_BLOCK_SIZE, stats);
            // a block loaded by a read, not a prefetch, is never counted
            cache.getOrLoad(key(file, 3), channel, length, DEFAULT_BLOCK_SIZE, stats);
            final BlockCache.Trace trace = cache.stopTrace();
            assertEquals(1, trace.prefetchedUnreadCount());
            assertEquals(List.of("_0.dvd:1"), trace.prefetchedUnread(DEFAULT_BLOCK_SIZE, 20));
            // no Lucene frame on the requesting (test) thread
            assertEquals(Map.of("null / null", 1), trace.prefetchedUnreadByRequester());
            assertEquals(3, stats.prefetchLoads.sum());
            assertEquals(1, stats.loads.sum());
        }
    }

    public void testUnreadListIsSortedAndLimited() throws IOException {
        final Path file = writeFile("_1.kdd");
        final BlockCache cache = new BlockCache(64L * DEFAULT_BLOCK_SIZE, Runnable::run);
        final BlockCache.FileStats stats = cache.statsFor(file.getFileName().toString());
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            cache.startTrace(1000);
            cache.prefetch(file, 1, channel, channel.size(), 0, BLOCKS, stats);
            final BlockCache.Trace trace = cache.stopTrace();
            assertEquals(BLOCKS, trace.prefetchedUnreadCount());
            assertEquals(List.of("_1.kdd:0", "_1.kdd:1"), trace.prefetchedUnread(DEFAULT_BLOCK_SIZE, 2));
        }
    }

    public void testNoTrackingWithoutTrace() throws IOException {
        final Path file = writeFile("_2.dvd");
        final BlockCache cache = new BlockCache(64L * DEFAULT_BLOCK_SIZE, Runnable::run);
        final BlockCache.FileStats stats = cache.statsFor(file.getFileName().toString());
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            // prefetched before the trace starts: not tracked by it
            cache.prefetch(file, 1, channel, channel.size(), 0, 3, stats);
            assertNull(cache.currentTrace());
            cache.startTrace(1000);
            final BlockCache.Trace trace = cache.stopTrace();
            assertEquals(0, trace.prefetchedUnreadCount());
            assertEquals(List.of(), trace.prefetchedUnread(DEFAULT_BLOCK_SIZE, 20));
            assertEquals(3, stats.prefetchLoads.sum());
        }
    }

    public void testReadAheadBlocksAreReportedApartFromPrefetchedBlocks() throws IOException {
        final Path file = writeFile("_3.dvd");
        // 4 blocks of DEFAULT_BLOCK_SIZE / 4 per read window: one prefetch read of the first window brings 4 blocks
        final int block = DEFAULT_BLOCK_SIZE / 4;
        final BlockCache cache = new BlockCache(64L * DEFAULT_BLOCK_SIZE, block, block, DEFAULT_BLOCK_SIZE, Runnable::run);
        final BlockCache.FileStats stats = cache.statsFor(file.getFileName().toString());
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            final long length = channel.size();
            cache.startTrace(1000);
            cache.prefetch(file, 1, channel, length, block, 1, stats);
            BlockCache.Trace trace = cache.currentTrace();
            assertEquals("only the requested block counts as prefetched", 1, trace.prefetchedUnreadCount());
            assertEquals(3, trace.readaheadUnreadCount());
            cache.getOrLoad(new BlockKey(file, 1, 0), channel, length, block, stats);
            cache.getOrLoad(new BlockKey(file, 1, block), channel, length, block, stats);
            trace = cache.stopTrace();
            assertEquals(0, trace.prefetchedUnreadCount());
            assertEquals(2, trace.readaheadUnreadCount());
            final List<BlockCache.Event> loads = trace.events.stream().filter(e -> e.size() > 0).toList();
            assertEquals(4, loads.size());
            assertEquals(1, loads.stream().filter(e -> e.readahead() == false).count());
            assertTrue(loads.stream().allMatch(BlockCache.Event::prefetch));
            assertEquals(1, stats.prefetchReads.sum());
            assertEquals(0, stats.reads.sum());
            assertEquals(0, stats.loads.sum());
        }
    }
}
