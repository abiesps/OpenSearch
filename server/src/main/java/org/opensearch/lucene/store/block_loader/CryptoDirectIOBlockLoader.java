/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lucene.store.block_loader;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import one.jasyncfio.AsyncFile;
import one.jasyncfio.OpenOption;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Randomness;
import org.opensearch.lucene.store.block.RefCountedMemorySegment;
import org.opensearch.lucene.store.block_cache.BlockCacheKey;
import org.opensearch.lucene.store.cipher.EncryptionMetadataCache;
import org.opensearch.lucene.store.footer.EncryptionFooter;
import org.opensearch.lucene.store.footer.EncryptionMetadataTrailer;
import org.opensearch.lucene.store.key.HkdfKeyDerivation;
import org.opensearch.lucene.store.key.KeyResolver;
import org.opensearch.lucene.store.pool.Pool;

import static org.opensearch.lucene.store.block_loader.DirectIOReaderUtil.directIOReadAligned;
import static org.opensearch.lucene.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.lucene.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.lucene.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;
import static org.opensearch.lucene.store.footer.EncryptionMetadataTrailer.MESSAGE_ID_SIZE;
import static org.opensearch.node.Node.eventExecutor;

/**
 * A {@link BlockLoader} implementation that loads encrypted file blocks using Direct I/O
 * and automatically decrypts them in-place.
 *
 * <p>This loader combines high-performance Direct I/O with transparent decryption to provide
 * efficient access to encrypted file data. It reads blocks directly from storage, bypassing
 * the OS buffer cache, then decrypts the data in memory using the configured key and IV resolver.
 *
 * <p>Key features:
 * <ul>
 * <li>Direct I/O for high performance and reduced memory pressure</li>
 * <li>Automatic in-place decryption of loaded blocks</li>
 * <li>Memory pool integration for efficient buffer management</li>
 * <li>Block-aligned operations for optimal storage performance</li>
 * </ul>
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
public class CryptoDirectIOBlockLoader implements BlockLoader<RefCountedMemorySegment> {
    private static final Logger LOGGER = LogManager.getLogger(CryptoDirectIOBlockLoader.class);

    private  KeyResolver keyResolver;
    private  Pool<RefCountedMemorySegment> segmentPool;
    private  EncryptionMetadataCache encryptionMetadataCache;


    public static byte[] fileKey;                                  // Derived file key (matches write path)
    public static byte[] masterKey = new byte[32];                                // Master key for IV computation
    public static byte[] messageId = new byte[MESSAGE_ID_SIZE];

    static {
        java.util.Random rnd = Randomness.get();
        rnd.nextBytes(masterKey);
        rnd.nextBytes(messageId);
        fileKey = HkdfKeyDerivation.deriveFileKey(masterKey, messageId);
    }

    /**
     * Constructs a new CryptoDirectIOBlockLoader with the specified memory pool and key resolver.
     *
     * @param segmentPool the memory segment pool for acquiring buffer space
     * @param keyResolver the resolver for obtaining encryption keys and initialization vectors
     */
    public CryptoDirectIOBlockLoader(
        Pool<RefCountedMemorySegment> segmentPool,
        KeyResolver keyResolver,
        EncryptionMetadataCache encryptionMetadataCache
    ) {
        this.segmentPool = segmentPool;
        this.keyResolver = keyResolver;
        this.encryptionMetadataCache = encryptionMetadataCache;
    }


    public CryptoDirectIOBlockLoader(
        Pool<RefCountedMemorySegment> segmentPool
    ) {
        this.segmentPool = segmentPool;
    }

    @Override
    public void forceIO(BlockCacheKey key, boolean useIoUring) throws Exception {
        Path filePath = key.filePath();
        long startOffset = key.offset();
        int blockCount = 1;

        if (!Files.exists(filePath)) {
            throw new NoSuchFileException(filePath.toString());
        }

        if ((startOffset & CACHE_BLOCK_MASK) != 0) {
            throw new IllegalArgumentException("startOffset must be block-aligned: " + startOffset);
        }

        if (blockCount <= 0) {
            throw new IllegalArgumentException("blockCount must be positive: " + blockCount);
        }

        try (
            Arena arena = Arena.ofConfined();
        ) {
            RefCountedMemorySegment[] result = new RefCountedMemorySegment[(int) blockCount];
            long readLength = blockCount << CACHE_BLOCK_SIZE_POWER;
            MemorySegment readBytes = null;
            if (useIoUring) {
                CompletableFuture<AsyncFile> asyncFileCompletableFuture = AsyncFile.open(filePath, eventExecutor,
                    OpenOption.DIRECT);
                AsyncFile asyncFile = asyncFileCompletableFuture.get();
                readBytes = directIOReadAligned(asyncFile, startOffset, readLength, arena);
                asyncFile.close().get();//aggressively close file descriptor.
            } else {
                FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ, DirectIOReaderUtil.getDirectOpenOption());
                readBytes = directIOReadAligned(channel, startOffset, readLength, arena);
                channel.close();
            }
            long bytesRead = readBytes.byteSize();
            if (bytesRead == 0) {
                throw new java.io.EOFException("Unexpected EOF or empty read at offset " + startOffset + " for file " + filePath);
            }
        }

    }

    @Override
    public RefCountedMemorySegment[] load(Path filePath, long startOffset, long blockCount, long poolTimeoutMs,
                                          boolean useIOuring) throws Exception {
        if (!Files.exists(filePath)) {
            throw new NoSuchFileException(filePath.toString());
        }

        if ((startOffset & CACHE_BLOCK_MASK) != 0) {
            throw new IllegalArgumentException("startOffset must be block-aligned: " + startOffset);
        }

        if (blockCount <= 0) {
            throw new IllegalArgumentException("blockCount must be positive: " + blockCount);
        }

        RefCountedMemorySegment[] result = new RefCountedMemorySegment[(int) blockCount];
        long readLength = blockCount << CACHE_BLOCK_SIZE_POWER;

        try (
            Arena arena = Arena.ofConfined();
        ) {
            MemorySegment readBytes = null;
            if (useIOuring) {
                CompletableFuture<AsyncFile> asyncFileCompletableFuture = AsyncFile.open(filePath, eventExecutor,
                    OpenOption.DIRECT);
                AsyncFile asyncFile = asyncFileCompletableFuture.get();
                readBytes = directIOReadAligned(asyncFile, startOffset, readLength, arena);
                asyncFile.close().get();//aggressively close file descriptor.
            } else {
                FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ, DirectIOReaderUtil.getDirectOpenOption());
                readBytes = directIOReadAligned(channel, startOffset, readLength, arena);
                channel.close();
            }
            long bytesRead = readBytes.byteSize();

            if (bytesRead == 0) {
                throw new java.io.EOFException("Unexpected EOF or empty read at offset " + startOffset + " for file " + filePath);
            }

            int blockIndex = 0;
            long bytesCopied = 0;

            try {
                while (blockIndex < blockCount && bytesCopied < bytesRead) {
                    // Use caller-specified timeout (5s for critical loads, 50ms for prefetch)
                    RefCountedMemorySegment handle =
                        segmentPool.tryAcquire(poolTimeoutMs, TimeUnit.MILLISECONDS);

                    MemorySegment pooled = handle.segment();

                    int remaining = (int) (bytesRead - bytesCopied);
                    int toCopy = Math.min(CACHE_BLOCK_SIZE, remaining);

                    if (toCopy > 0) {
                        MemorySegment.copy(readBytes, bytesCopied, pooled, 0, toCopy);
                    }

                    result[blockIndex++] = handle;  // Store the handle, not the segment
                    bytesCopied += toCopy;
                }

            } catch (InterruptedException e) {
                releaseHandles(result, blockIndex);
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while acquiring pool segment", e);
            } catch (IOException e) {
                releaseHandles(result, blockIndex);
                throw e;
            }

            return result;

        } catch (NoSuchFileException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.error("Bulk read failed: path={} offset={} length={} err={}", filePath, startOffset, readLength, e.toString());
            throw e;
        }
    }

    private void releaseHandles(RefCountedMemorySegment[] handles, int upTo) {
        for (int i = 0; i < upTo; i++) {
            if (handles[i] != null) {
                handles[i].close();
            }
        }
    }

    private EncryptionFooter readFooterFromDisk(Path filePath, byte[] masterKey) throws IOException {
        String normalizedPath = filePath.toAbsolutePath().normalize().toString();

        // Check cache first for fast path
        EncryptionFooter cachedFooter = encryptionMetadataCache.getFooter(normalizedPath);
//        if (cachedFooter != null) {
//            return cachedFooter;
//        }

        // Cache miss - read from disk
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            if (fileSize < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
                throw new IOException("File too small to contain footer: " + filePath);
            }

            // Read minimum footer to check OSEF magic bytes
            ByteBuffer minBuffer = ByteBuffer.allocate(EncryptionMetadataTrailer.MIN_FOOTER_SIZE);
            channel.read(minBuffer, fileSize - EncryptionMetadataTrailer.MIN_FOOTER_SIZE);
            byte[] minFooterBytes = minBuffer.array();

            // Check if this is an OSEF file
            if (!isValidOSEFFile(minFooterBytes)) {
                // Not an OSEF file
                throw new IOException("Not an OSEF file -" + filePath);
            }

            return EncryptionFooter.readViaFileChannel(normalizedPath, channel, masterKey, encryptionMetadataCache);
        }
    }

    /**
     * Check if file has valid OSEF magic bytes
     */
    private boolean isValidOSEFFile(byte[] minFooterBytes) {
        int magicOffset = minFooterBytes.length - EncryptionMetadataTrailer.MAGIC.length;
        for (int i = 0; i < EncryptionMetadataTrailer.MAGIC.length; i++) {
            if (minFooterBytes[magicOffset + i] != EncryptionMetadataTrailer.MAGIC[i]) {
                return false;
            }
        }
        return true;
    }
}
