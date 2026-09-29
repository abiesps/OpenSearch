/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.io.Channels;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Node-wide cache of fixed-size file blocks, backed by Caffeine.
 *
 * <p>Each block is a read-only, little-endian, direct {@link ByteBuffer} of {@link #BLOCK_SIZE} bytes (the last block of a
 * file is shorter). The cache is bounded by the total bytes of the cached blocks. There is no explicit memory pool and no
 * reference counting: an evicted block is freed by the garbage collector once no {@link BufferPoolIndexInput} still
 * points at it.
 *
 * <p>Concurrent misses on the same block are de-duplicated by Caffeine: one thread reads the block, the others wait for it.
 */
final class BlockCache {

    private static final Logger logger = LogManager.getLogger(BlockCache.class);

    /** log2 of the block size. */
    static final int BLOCK_SIZE_POWER = 17;
    /** Block size in bytes (128 KiB). */
    static final int BLOCK_SIZE = 1 << BLOCK_SIZE_POWER;
    /** Mask of the offset of a byte within its block. */
    static final long BLOCK_MASK = BLOCK_SIZE - 1L;

    private final Cache<BlockKey, ByteBuffer> cache;
    private final Executor prefetchExecutor;

    /**
     * @param maxBytes         upper bound of the total size of all cached blocks
     * @param prefetchExecutor executor that loads prefetched blocks in the background
     */
    BlockCache(long maxBytes, Executor prefetchExecutor) {
        this.cache = Caffeine.newBuilder()
            .maximumWeight(maxBytes)
            .weigher((BlockKey key, ByteBuffer block) -> block.capacity())
            // run cache maintenance on the calling thread so the cache owns no background threads
            .executor(Runnable::run)
            .build();
        this.prefetchExecutor = prefetchExecutor;
    }

    /**
     * Returns the block for {@code key}, reading it from {@code channel} on a miss.
     *
     * @param fileLength length of the whole file, which bounds the size of its last block
     */
    ByteBuffer getOrLoad(BlockKey key, FileChannel channel, long fileLength) throws IOException {
        try {
            return cache.get(key, k -> load(k, channel, fileLength));
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * Loads the missing blocks among {@code blockCount} blocks that start at {@code firstBlockOffset}, asynchronously.
     * Best effort: the request is dropped when the prefetch queue is full, and load failures are only logged.
     */
    void prefetch(Path file, long fileId, FileChannel channel, long fileLength, long firstBlockOffset, long blockCount) {
        final List<BlockKey> missing = new ArrayList<>();
        for (long i = 0; i < blockCount; i++) {
            BlockKey key = new BlockKey(file, fileId, firstBlockOffset + (i << BLOCK_SIZE_POWER));
            // containsKey does not count as an access, so a prefetch does not skew the eviction policy
            if (cache.asMap().containsKey(key) == false) {
                missing.add(key);
            }
        }
        if (missing.isEmpty()) {
            return;
        }
        try {
            prefetchExecutor.execute(() -> {
                for (BlockKey key : missing) {
                    try {
                        getOrLoad(key, channel, fileLength);
                    } catch (IOException | RuntimeException e) {
                        // e.g. the input was closed before the prefetch ran; a later read loads the block on demand
                        logger.debug(() -> "prefetch of block [" + key + "] failed", e);
                        return;
                    }
                }
            });
        } catch (OpenSearchRejectedExecutionException e) {
            logger.trace("prefetch queue is full, dropping prefetch of [{}] blocks of [{}]", missing.size(), file);
        }
    }

    /** Removes the blocks of one incarnation of a file. */
    void invalidateFile(Path file, long fileId, long fileLength) {
        for (long blockOffset = 0; blockOffset < fileLength; blockOffset += BLOCK_SIZE) {
            cache.invalidate(new BlockKey(file, fileId, blockOffset));
        }
    }

    /** Removes the blocks of all files under {@code directory}. Scans the whole cache. */
    void invalidateDirectory(Path directory) {
        cache.asMap().keySet().removeIf(key -> key.file().startsWith(directory));
    }

    /** Number of cached blocks. */
    long size() {
        cache.cleanUp();
        return cache.estimatedSize();
    }

    private static ByteBuffer load(BlockKey key, FileChannel channel, long fileLength) {
        final int size = (int) Math.min(BLOCK_SIZE, fileLength - key.blockOffset());
        final ByteBuffer block = ByteBuffer.allocateDirect(size);
        try {
            Channels.readFromFileChannelWithEofException(channel, key.blockOffset(), block);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        // readers only use absolute gets, so the shared buffer is never mutated and needs no position/limit reset
        return block.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
    }
}
