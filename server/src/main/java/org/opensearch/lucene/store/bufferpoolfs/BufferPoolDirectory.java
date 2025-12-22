/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lucene.store.bufferpoolfs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Provider;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import one.jasyncfio.AsyncFile;
import one.jasyncfio.OpenOption;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.lucene.store.block.RefCountedMemorySegment;
import org.opensearch.lucene.store.block_cache.BlockCache;
import org.opensearch.lucene.store.block_cache.CaffeineBlockCache;
import org.opensearch.lucene.store.block_cache.FileBlockCacheKey;
import org.opensearch.lucene.store.block_loader.BlockLoader;
import org.opensearch.lucene.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.lucene.store.cipher.EncryptionMetadataCache;
import org.opensearch.lucene.store.footer.EncryptionFooter;
import org.opensearch.lucene.store.footer.EncryptionMetadataTrailer;
import org.opensearch.lucene.store.key.KeyResolver;
import org.opensearch.lucene.store.pool.Pool;
import org.opensearch.lucene.store.read_ahead.ReadaheadContext;
import org.opensearch.lucene.store.read_ahead.ReadaheadManager;
import org.opensearch.lucene.store.read_ahead.Worker;
import org.opensearch.lucene.store.read_ahead.impl.ReadaheadManagerImpl;

import static org.opensearch.lucene.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.lucene.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;
import static org.opensearch.node.Node.eventExecutor;

/**
 * A high-performance FSDirectory implementation that combines Direct I/O operations with encryption.
 *
 * <p>This directory provides:
 * <ul>
 * <li>Direct I/O operations bypassing the OS page cache for better memory control</li>
 * <li>Block-level caching with memory segment pools for efficient memory management</li>
 * <li>Transparent encryption/decryption using OpenSSL native implementations</li>
 * <li>Read-ahead optimizations for sequential access patterns</li>
 * <li>Automatic cache invalidation on file deletion</li>
 * </ul>
 *
 * <p>The directory uses {@link BufferIOWithCaching} for output operations which encrypts
 * data before writing to disk and caches plaintext blocks for read operations. Input
 * operations use {@link CachedMemorySegmentIndexInput} with a multi-level cache hierarchy
 * including {@link BlockSlotTinyCache} for L1 caching.
 *
 * <p>Note: Some file types (segments files and .si files) fall back to the parent
 * directory implementation to avoid compatibility issues.
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "uses custom DirectIO")
public class BufferPoolDirectory extends FSDirectory {
    private static final Logger LOGGER = LogManager.getLogger(BufferPoolDirectory.class);

    private final Pool<RefCountedMemorySegment> memorySegmentPool;
    private final BlockCache<RefCountedMemorySegment> blockCache;
    private final Worker readAheadworker;
    private final Path dirPath;
    private final double minCacheMiss;
    private final boolean iouring;

    public BufferPoolDirectory(
        Path path,
        LockFactory lockFactory,
        Pool<RefCountedMemorySegment> memorySegmentPool,
        BlockCache<RefCountedMemorySegment> blockCache,
        Worker worker,
        double minCacheMiss,
        boolean iouring
    )
        throws IOException {
        super(path, lockFactory);
        this.memorySegmentPool = memorySegmentPool;
        this.blockCache = blockCache;
        this.readAheadworker = worker;
        this.dirPath = getDirectory();
        this.minCacheMiss = minCacheMiss;
        this.iouring = iouring;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);

        Path file = dirPath.resolve(name);
        if (iouring) {
            CompletableFuture<AsyncFile> asyncFileCompletableFuture = AsyncFile.open(file, eventExecutor,
                OpenOption.DIRECT);
            AsyncFile asyncFile = null;
            try {
                asyncFile = asyncFileCompletableFuture.get();
                // Calculate content length with OSEF validation
                //virtual threads will block here
                long contentLength = asyncFile.size().get();
                asyncFile.close().get();//close file after getting size
                //Disable read aheads
                ReadaheadManager readAheadManager = new ReadaheadManagerImpl(readAheadworker);
                ReadaheadContext readAheadContext = readAheadManager.register(file, contentLength);
                BlockSlotTinyCache pinRegistry = new BlockSlotTinyCache(blockCache, file);

                return CachedMemorySegmentIndexInput
                    .newInstance(
                        "CachedMemorySegmentIndexInput(path=\"" + file + "\")",
                        file,
                        contentLength,
                        blockCache,
                        readAheadManager,
                        readAheadContext,
                        pinRegistry,
                        minCacheMiss,
                        iouring
                    );
            } catch (Exception e) {
                if (asyncFile != null) {
                    try {
                        asyncFile.close().get();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    } catch (ExecutionException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
            return null;
        } else {
            long rawFileSize = Files.size(file);
            if (rawFileSize == 0) {
                throw new IOException("Cannot open empty file with DirectIO: " + file);
            }

            // Calculate content length with OSEF validation
            long contentLength = rawFileSize;

            ReadaheadManager readAheadManager = new ReadaheadManagerImpl(readAheadworker);
            ReadaheadContext readAheadContext = readAheadManager.register(file, contentLength);
            BlockSlotTinyCache pinRegistry = new BlockSlotTinyCache(blockCache, file);

            return CachedMemorySegmentIndexInput
                .newInstance(
                    "CachedMemorySegmentIndexInput(path=\"" + file + "\")",
                    file,
                    contentLength,
                    blockCache,
                    readAheadManager,
                    readAheadContext,
                    pinRegistry,
                    minCacheMiss,
                    iouring
                );
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return super.createOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return super.createTempOutput(prefix, suffix, context);
    }

    // only close resources owned by this directory type.
    // the actual directory is closed only once (see HybridCryptoDirectory.java)
    @Override
    @SuppressWarnings("ConvertToTryWithResources")
    public synchronized void close() throws IOException {
        readAheadworker.close();
       // encryptionMetadataCache.invalidateDirectory();

        // Invalidate all cache entries for this directory to prevent memory leaks
        // when the shard/index is closed or deleted
        if (blockCache != null) {
            blockCache.invalidateByPathPrefix(dirPath);
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        Path file = dirPath.resolve(name);

        // Cancel any pending async read-ahead operations for this file FIRST
        // to prevent race where read-ahead tries to load blocks from deleted/replaced file
        readAheadworker.cancel(file);

        if (blockCache != null) {
            try {
                long fileSize = Files.size(file);
                if (fileSize > 0) {
                    final int totalBlocks = (int) ((fileSize + CACHE_BLOCK_SIZE - 1) >>> CACHE_BLOCK_SIZE_POWER);
                    for (int i = 0; i < totalBlocks; i++) {
                        final long blockOffset = (long) i << CACHE_BLOCK_SIZE_POWER;
                        FileBlockCacheKey key = new FileBlockCacheKey(file, blockOffset);
                        blockCache.invalidate(key);
                    }
                }
            } catch (IOException e) {
                // Fall back to path-based invalidation if file size unavailable
                LOGGER.warn("Failed to get file size for clearing cache for deleting shard", e);
            }
        }
        super.deleteFile(name);
        //encryptionMetadataCache.invalidateFile(EncryptionMetadataCache.normalizePath(file));
    }

    private void logCacheAndPoolStats() {
        try {

            if (blockCache instanceof CaffeineBlockCache) {
                String cacheStats = ((CaffeineBlockCache<?, ?>) blockCache).cacheStats();
                LOGGER.info("{}", cacheStats);
            }

        } catch (Exception e) {
            LOGGER.warn("Failed to log cache/pool stats", e);
        }
    }

    @SuppressWarnings("unused")
    // only used during local testing.
    private void startCacheStatsTelemetry() {
        Thread loggerThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(Duration.ofSeconds(10));
                    logCacheAndPoolStats();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    LOGGER.warn("Error in collecting cache stats", t);
                }
            }
        });

        loggerThread.setDaemon(true);
        loggerThread.setName("DirectIOBufferPoolStatsLogger");
        loggerThread.start();
    }
}
