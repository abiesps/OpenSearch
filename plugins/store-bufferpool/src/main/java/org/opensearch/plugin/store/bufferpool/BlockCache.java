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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

/**
 * Node-wide cache of fixed-size file blocks, backed by Caffeine.
 *
 * <p>Each block is a read-only, little-endian, direct {@link ByteBuffer} of {@link #blockSize()} bytes (the last block of
 * a file is shorter). The cache is bounded by the total bytes of the cached blocks. There is no explicit memory pool and no
 * reference counting: an evicted block is freed by the garbage collector once no {@link BufferPoolIndexInput} still
 * points at it.
 *
 * <p>Storage is read in IO windows that can be larger than a block. A miss reads the aligned window of the reader's read
 * size that holds the missed block ({@link #randomReadSize()} for inputs opened for random access, else
 * {@link #sequentialReadSize()}), and a prefetch reads the aligned windows of {@link #sequentialReadSize()} that its range
 * touches. Every block of a window that is not cached and not already being read is inserted; cached blocks are never
 * read again or replaced, so a window with cached blocks is read as one read per run of missing blocks. Windows are
 * clipped at the end of the file. With both read sizes equal to the block size, every read is one block.
 *
 * <p>Concurrent misses are de-duplicated twice: per window (one thread reads a window of a given size, the others wait
 * for it) and per block (a block that a read of another size is loading is waited for, never read a second time).
 *
 * <p>For experiments, the cache counts block requests, reads and inserted blocks per file type (see {@link FileStats}) and
 * can add a fixed delay to every storage read to simulate remote storage such as EFS.
 */
final class BlockCache {

    private static final Logger logger = LogManager.getLogger(BlockCache.class);

    /** Default block size in bytes (128 KiB). */
    static final int DEFAULT_BLOCK_SIZE = 1 << 17;
    static final int MIN_BLOCK_SIZE = 512;
    static final int MAX_BLOCK_SIZE = 8 << 20;
    /** Largest accepted read size. */
    static final int MAX_READ_SIZE = 8 << 20;
    /** Size classes of {@link FileStats#readsBySize}: class {@code c} counts reads of {@code (2^(c-1), 2^c]} bytes. */
    static final int READ_SIZE_CLASSES = Integer.numberOfTrailingZeros(MAX_READ_SIZE) + 1;
    /** Upper bound of the bytes held by idle read buffers of multi-block reads. */
    private static final long SCRATCH_POOL_BYTES = 32L << 20;

    private final int blockSize;
    private final int blockSizePower;
    private final long blockMask;
    private final int randomReadSize;
    private final int sequentialReadSize;
    private final Cache<BlockKey, ByteBuffer> cache;
    private final Executor prefetchExecutor;
    private final ConcurrentMap<String, FileStats> stats = new ConcurrentHashMap<>();
    /** Blocks being read from storage, by the thread that claimed them. Entries exist only while that read runs. */
    private final ConcurrentMap<BlockKey, CompletableFuture<ByteBuffer>> loadingBlocks = new ConcurrentHashMap<>();
    /** Windows being read, by the thread that claimed them. Entries exist only while that read runs. */
    private final ConcurrentMap<WindowKey, CompletableFuture<Void>> loadingWindows = new ConcurrentHashMap<>();
    /**
     * Idle direct buffers of {@link #maxReadSize} bytes that multi-block reads read into before copying each block out.
     * Bounded; a read that finds the pool empty allocates a buffer and offers it back afterwards.
     */
    private final ArrayBlockingQueue<ByteBuffer> readBuffers;
    private final int maxReadSize;
    private volatile long simulatedLoadLatencyNanos;
    /** Non-null while a trace is being recorded, see {@link #startTrace(int)}. */
    private volatile Trace trace;

    /**
     * Creates a cache with {@link #DEFAULT_BLOCK_SIZE} blocks that reads one block per miss.
     *
     * @param maxBytes         upper bound of the total size of all cached blocks
     * @param prefetchExecutor executor that loads prefetched blocks in the background
     */
    BlockCache(long maxBytes, Executor prefetchExecutor) {
        this(maxBytes, DEFAULT_BLOCK_SIZE, prefetchExecutor);
    }

    /**
     * Creates a cache that reads one block per miss.
     *
     * @param maxBytes         upper bound of the total size of all cached blocks
     * @param blockSize        block size in bytes, a power of two between {@link #MIN_BLOCK_SIZE} and {@link #MAX_BLOCK_SIZE}
     * @param prefetchExecutor executor that loads prefetched blocks in the background
     */
    BlockCache(long maxBytes, int blockSize, Executor prefetchExecutor) {
        this(maxBytes, blockSize, blockSize, blockSize, prefetchExecutor);
    }

    /**
     * @param maxBytes           upper bound of the total size of all cached blocks
     * @param blockSize          block size in bytes, a power of two between {@link #MIN_BLOCK_SIZE} and {@link #MAX_BLOCK_SIZE}
     * @param randomReadSize     bytes read per miss of an input opened for random access, see {@link #validateReadSize}
     * @param sequentialReadSize bytes read per miss of any other input and per prefetch window, see {@link #validateReadSize}
     * @param prefetchExecutor   executor that loads prefetched blocks in the background
     */
    BlockCache(long maxBytes, int blockSize, int randomReadSize, int sequentialReadSize, Executor prefetchExecutor) {
        validateBlockSize(blockSize);
        validateReadSize("random read size", randomReadSize, blockSize);
        validateReadSize("sequential read size", sequentialReadSize, blockSize);
        this.blockSize = blockSize;
        this.blockSizePower = Integer.numberOfTrailingZeros(blockSize);
        this.blockMask = blockSize - 1L;
        this.randomReadSize = randomReadSize;
        this.sequentialReadSize = sequentialReadSize;
        this.maxReadSize = Math.max(randomReadSize, sequentialReadSize);
        this.readBuffers = new ArrayBlockingQueue<>((int) Math.max(1, SCRATCH_POOL_BYTES / maxReadSize));
        this.cache = Caffeine.newBuilder()
            .maximumWeight(maxBytes)
            .weigher((BlockKey key, ByteBuffer block) -> block.capacity())
            // run cache maintenance on the calling thread so the cache owns no background threads
            .executor(Runnable::run)
            .build();
        this.prefetchExecutor = prefetchExecutor;
    }

    static void validateBlockSize(long blockSize) {
        if (Long.bitCount(blockSize) != 1 || blockSize < MIN_BLOCK_SIZE || blockSize > MAX_BLOCK_SIZE) {
            throw new IllegalArgumentException(
                "block size must be a power of two between " + MIN_BLOCK_SIZE + " and " + MAX_BLOCK_SIZE + ", got " + blockSize
            );
        }
    }

    /**
     * Checks that {@code readSize} is a power of two between {@code blockSize} and {@link #MAX_READ_SIZE}, so a read is a
     * whole number of blocks and an aligned read window never splits a block.
     */
    static void validateReadSize(String name, long readSize, long blockSize) {
        if (Long.bitCount(readSize) != 1 || readSize < blockSize || readSize > MAX_READ_SIZE) {
            throw new IllegalArgumentException(
                name
                    + " must be a power of two between the block size ["
                    + blockSize
                    + "] and ["
                    + MAX_READ_SIZE
                    + "], got ["
                    + readSize
                    + "]"
            );
        }
    }

    int blockSize() {
        return blockSize;
    }

    int blockSizePower() {
        return blockSizePower;
    }

    long blockMask() {
        return blockMask;
    }

    /** Bytes read per miss of an input opened for random access. */
    int randomReadSize() {
        return randomReadSize;
    }

    /** Bytes read per miss of an input not opened for random access, and per prefetch window. */
    int sequentialReadSize() {
        return sequentialReadSize;
    }

    /** Read size of an input opened for random ({@code true}) or other ({@code false}) access. */
    int readSize(boolean random) {
        return random ? randomReadSize : sequentialReadSize;
    }

    /**
     * Size of the storage "node" that prefetch planners request in: one prefetch window, so that a planner's request is
     * one storage read.
     */
    int prefetchNodeBytes() {
        return sequentialReadSize;
    }

    /**
     * Returns the block for {@code key}, reading the window of {@code readSize} bytes that holds it from {@code channel}
     * on a miss.
     *
     * @param fileLength length of the whole file, which bounds the window and the size of the file's last block
     * @param readSize   the reader's read size, {@link #randomReadSize()} or {@link #sequentialReadSize()}
     * @param fileStats  counters of the file's type, from {@link #statsFor(String)}
     */
    ByteBuffer getOrLoad(BlockKey key, FileChannel channel, long fileLength, int readSize, FileStats fileStats) throws IOException {
        fileStats.requests.increment();
        final Trace t = trace;
        if (t == null) {
            return getOrLoadBlock(key, channel, fileLength, readSize, fileStats);
        }
        // tracing only: record reads that blocked, on their own load or on a load already in flight (e.g. a prefetch)
        final long start = System.nanoTime();
        final ByteBuffer block = getOrLoadBlock(key, channel, fileLength, readSize, fileStats);
        final long waited = System.nanoTime() - start;
        if (waited > TimeUnit.MICROSECONDS.toNanos(100)) {
            t.recordWait(key, waited);
        }
        t.recordRead(key);
        return block;
    }

    private ByteBuffer getOrLoadBlock(BlockKey key, FileChannel channel, long fileLength, int readSize, FileStats fileStats)
        throws IOException {
        assert readSize == randomReadSize || readSize == sequentialReadSize : "unknown read size " + readSize;
        final long windowStart = key.blockOffset() & -(long) readSize;
        while (true) {
            final ByteBuffer cached = cache.getIfPresent(key);
            if (cached != null) {
                return cached;
            }
            final CompletableFuture<ByteBuffer> inFlight = loadingBlocks.get(key);
            if (inFlight != null) {
                final ByteBuffer loaded = inFlight.join();
                if (loaded != null) {
                    return loaded;
                }
                // that read failed or skipped the block: look again, and read it here if it is still missing
                continue;
            }
            final ByteBuffer loaded = loadWindow(
                key.file(),
                key.fileId(),
                channel,
                fileLength,
                readSize,
                windowStart,
                key,
                0,
                0,
                fileStats,
                null
            );
            if (loaded != null) {
                return loaded;
            }
            // another thread was reading the window, or the block was cached or claimed meanwhile: look again
        }
    }

    /**
     * Whether all {@code blockCount} blocks that start at {@code firstBlockOffset} are cached (a block still loading is
     * not). Does not count as an access.
     */
    boolean contains(Path file, long fileId, long firstBlockOffset, long blockCount) {
        for (long i = 0; i < blockCount; i++) {
            if (cache.asMap().containsKey(new BlockKey(file, fileId, firstBlockOffset + (i << blockSizePower))) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Loads the missing blocks among {@code blockCount} blocks that start at {@code firstBlockOffset}, asynchronously, in
     * aligned windows of {@link #sequentialReadSize()} bytes (adjacent missing blocks of a window are read together, and
     * the window's other missing blocks with them). Best effort: the request is dropped when the prefetch queue is full,
     * and load failures are only logged.
     */
    void prefetch(
        Path file,
        long fileId,
        FileChannel channel,
        long fileLength,
        long firstBlockOffset,
        long blockCount,
        FileStats fileStats
    ) {
        fileStats.prefetchRequests.add(blockCount);
        boolean anyMissing = false;
        for (long i = 0; i < blockCount && anyMissing == false; i++) {
            final BlockKey key = new BlockKey(file, fileId, firstBlockOffset + (i << blockSizePower));
            // containsKey does not count as an access, so a prefetch does not skew the eviction policy
            anyMissing = cache.asMap().containsKey(key) == false && loadingBlocks.containsKey(key) == false;
        }
        if (anyMissing == false) {
            return;
        }
        final long lastBlockOffset = firstBlockOffset + ((blockCount - 1) << blockSizePower);
        // tracing only: the prefetch runs on another thread, so remember which code asked for it
        final String[] requester = trace == null ? null : Trace.callers();
        try {
            prefetchExecutor.execute(() -> {
                final long windowMask = -(long) sequentialReadSize;
                for (long window = firstBlockOffset & windowMask; window <= lastBlockOffset; window += sequentialReadSize) {
                    try {
                        loadWindow(
                            file,
                            fileId,
                            channel,
                            fileLength,
                            sequentialReadSize,
                            window,
                            null,
                            Math.max(firstBlockOffset, window),
                            Math.min(lastBlockOffset, window + sequentialReadSize - blockSize),
                            fileStats,
                            requester
                        );
                    } catch (IOException | RuntimeException e) {
                        // e.g. the input was closed before the prefetch ran; a later read loads the block on demand
                        final long w = window;
                        logger.debug(() -> "prefetch of [" + file + "] window at [" + w + "] failed", e);
                        return;
                    }
                }
            });
        } catch (OpenSearchRejectedExecutionException e) {
            logger.trace("prefetch queue is full, dropping prefetch of [{}] blocks of [{}]", blockCount, file);
        }
    }

    /**
     * Reads the blocks of one window that are neither cached nor being read, if one of the requested blocks is among them,
     * and inserts them. Returns without reading if another thread is reading the same window (a demand read waits for it
     * first). Never waits while it holds a claim, so concurrent window reads cannot deadlock.
     *
     * @param wanted         for a demand read, the block the reader needs; null for a prefetch
     * @param requestedFirst for a prefetch, offset of the first requested block in this window
     * @param requestedLast  for a prefetch, offset of the last requested block in this window
     * @param requester      while tracing, the callers that requested a prefetch, else null
     * @return the wanted block if this call read it, else null (the caller looks it up again)
     */
    private ByteBuffer loadWindow(
        Path file,
        long fileId,
        FileChannel channel,
        long fileLength,
        int readSize,
        long windowStart,
        BlockKey wanted,
        long requestedFirst,
        long requestedLast,
        FileStats fileStats,
        String[] requester
    ) throws IOException {
        final WindowKey windowKey = new WindowKey(file, fileId, windowStart, readSize);
        final CompletableFuture<Void> window = new CompletableFuture<>();
        final CompletableFuture<Void> other = loadingWindows.putIfAbsent(windowKey, window);
        if (other != null) {
            if (wanted != null) {
                other.join();
            }
            return null;
        }
        final boolean prefetch = wanted == null;
        final long windowEnd = Math.min(windowStart + readSize, fileLength);
        final List<Claim> claims = new ArrayList<>();
        try {
            boolean anyRequested = false;
            for (long offset = windowStart; offset < windowEnd; offset += blockSize) {
                final boolean requested = prefetch ? offset >= requestedFirst && offset <= requestedLast : offset == wanted.blockOffset();
                final BlockKey key = requested && prefetch == false ? wanted : new BlockKey(file, fileId, offset);
                if (cache.asMap().containsKey(key)) {
                    continue;
                }
                final CompletableFuture<ByteBuffer> loading = new CompletableFuture<>();
                if (loadingBlocks.putIfAbsent(key, loading) != null) {
                    continue; // a read of another size or origin is loading it
                }
                // a read that finished between containsKey and the claim already inserted the block
                if (cache.asMap().containsKey(key)) {
                    loadingBlocks.remove(key, loading);
                    loading.complete(null);
                    continue;
                }
                claims.add(new Claim(key, loading, requested));
                anyRequested |= requested;
            }
            if (anyRequested == false) {
                return null; // the finally block releases the claims: never read a window only for its neighbours
            }
            // runs of adjacent claimed blocks; the run with the wanted block is read first
            final List<int[]> runs = new ArrayList<>();
            int wantedRun = -1;
            for (int i = 0; i < claims.size();) {
                int j = i;
                while (j + 1 < claims.size() && claims.get(j + 1).key.blockOffset() == claims.get(j).key.blockOffset() + blockSize) {
                    j++;
                }
                for (int k = i; k <= j && prefetch == false; k++) {
                    if (claims.get(k).requested) {
                        wantedRun = runs.size();
                    }
                }
                runs.add(new int[] { i, j });
                i = j + 1;
            }
            ByteBuffer result = null;
            if (wantedRun >= 0) {
                result = readRun(channel, fileLength, claims, runs.get(wantedRun), false, fileStats, requester);
            }
            for (int r = 0; r < runs.size(); r++) {
                if (r == wantedRun) {
                    continue;
                }
                if (prefetch) {
                    readRun(channel, fileLength, claims, runs.get(r), true, fileStats, requester);
                } else {
                    try {
                        readRun(channel, fileLength, claims, runs.get(r), false, fileStats, requester);
                    } catch (IOException | RuntimeException e) {
                        // only neighbours of the wanted block: a reader that needs them reads them again
                        logger.debug(() -> "read-ahead in [" + file + "] window at [" + windowStart + "] failed", e);
                    }
                }
            }
            return result;
        } finally {
            // every claim is released even if a read failed, so no reader waits forever; waiters on a failed block retry
            for (Claim claim : claims) {
                if (claim.loading.isDone() == false) {
                    loadingBlocks.remove(claim.key, claim.loading);
                    claim.loading.complete(null);
                }
            }
            loadingWindows.remove(windowKey, window);
            window.complete(null);
        }
    }

    /**
     * Reads the claimed blocks {@code run[0]..run[1]} (adjacent) with one storage read, inserts them and releases their
     * claims.
     *
     * @return the block of the run that was requested by a demand read, or null
     */
    private ByteBuffer readRun(
        FileChannel channel,
        long fileLength,
        List<Claim> claims,
        int[] run,
        boolean prefetch,
        FileStats fileStats,
        String[] requester
    ) throws IOException {
        final long start = System.nanoTime();
        final long runStart = claims.get(run[0]).key.blockOffset();
        final long runEnd = Math.min(claims.get(run[1]).key.blockOffset() + blockSize, fileLength);
        final int size = Math.toIntExact(runEnd - runStart);
        final ByteBuffer[] blocks = new ByteBuffer[run[1] - run[0] + 1];
        if (blocks.length == 1) {
            blocks[0] = ByteBuffer.allocateDirect(size);
            Channels.readFromFileChannelWithEofException(channel, runStart, blocks[0]);
        } else {
            ByteBuffer buffer = readBuffers.poll();
            if (buffer == null) {
                buffer = ByteBuffer.allocateDirect(maxReadSize);
            }
            try {
                buffer.clear().limit(size);
                Channels.readFromFileChannelWithEofException(channel, runStart, buffer);
                for (int b = 0; b < blocks.length; b++) {
                    final int from = b << blockSizePower;
                    final int length = Math.min(blockSize, size - from);
                    blocks[b] = ByteBuffer.allocateDirect(length).put(buffer.slice(from, length));
                }
            } finally {
                readBuffers.offer(buffer);
            }
        }
        final long latency = simulatedLoadLatencyNanos;
        if (latency > 0) {
            final long deadline = start + latency;
            for (long now = System.nanoTime(); now < deadline; now = System.nanoTime()) {
                LockSupport.parkNanos(deadline - now);
            }
        }
        (prefetch ? fileStats.prefetchReads : fileStats.reads).increment();
        fileStats.bytesRead.add(size);
        fileStats.readsBySize[sizeClass(size)].increment();
        fileStats.loadNanos.add(System.nanoTime() - start);
        final Trace t = trace;
        ByteBuffer result = null;
        for (int b = 0; b < blocks.length; b++) {
            final Claim claim = claims.get(run[0] + b);
            // readers only use absolute gets, so the shared buffer is never mutated and needs no position/limit reset
            final ByteBuffer block = blocks[b].asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
            final ByteBuffer existing = cache.asMap().putIfAbsent(claim.key, block);
            final ByteBuffer value = existing == null ? block : existing;
            if (existing == null) {
                final boolean readahead = claim.requested == false;
                (readahead ? fileStats.readaheadLoads : prefetch ? fileStats.prefetchLoads : fileStats.loads).increment();
                fileStats.bytesLoaded.add(block.capacity());
                if (t != null) {
                    t.record(claim.key, block.capacity(), prefetch, readahead, requester);
                }
            }
            if (prefetch == false && claim.requested) {
                result = value;
            }
            loadingBlocks.remove(claim.key, claim.loading);
            claim.loading.complete(value);
        }
        return result;
    }

    /** Size class of a read of {@code size >= 1} bytes: the exponent of the smallest power of two that is at least {@code size}. */
    static int sizeClass(int size) {
        return 32 - Integer.numberOfLeadingZeros(size - 1);
    }

    /** Removes the blocks of one incarnation of a file. */
    void invalidateFile(Path file, long fileId, long fileLength) {
        for (long blockOffset = 0; blockOffset < fileLength; blockOffset += blockSize) {
            cache.invalidate(new BlockKey(file, fileId, blockOffset));
        }
    }

    /** Removes the blocks of all files under {@code directory}. Scans the whole cache. */
    void invalidateDirectory(Path directory) {
        cache.asMap().keySet().removeIf(key -> key.file().startsWith(directory));
    }

    /** Removes all blocks. */
    void clear() {
        cache.invalidateAll();
        cache.cleanUp();
    }

    /** Number of cached blocks. */
    long size() {
        cache.cleanUp();
        return cache.estimatedSize();
    }

    /** Total bytes of the cached blocks. */
    long sizeInBytes() {
        cache.cleanUp();
        return cache.policy().eviction().map(e -> e.weightedSize().orElse(-1L)).orElse(-1L);
    }

    /** Number of blocks and windows being read right now (0 when idle). For tests and stats. */
    int inFlightReads() {
        return loadingBlocks.size() + loadingWindows.size();
    }

    /** Sets a delay added to every storage read; 0 disables it. */
    void setSimulatedLoadLatencyNanos(long nanos) {
        this.simulatedLoadLatencyNanos = Math.max(0, nanos);
    }

    long simulatedLoadLatencyNanos() {
        return simulatedLoadLatencyNanos;
    }

    /**
     * Returns the counters for the type of {@code fileName}. Callers look this up once per file open and reuse it, so the
     * block read path does no map lookup.
     */
    FileStats statsFor(String fileName) {
        return stats.computeIfAbsent(fileType(fileName), k -> new FileStats());
    }

    /** Snapshot of all counters, sorted by file type. */
    Map<String, FileStats> stats() {
        return new TreeMap<>(stats);
    }

    /** Sets all counters to zero. Inputs keep their {@link FileStats} references, so entries are reset, not removed. */
    void resetStats() {
        stats.values().forEach(FileStats::reset);
    }

    /**
     * Maps a Lucene file name to a type key by dropping the segment name: {@code _0_Lucene104Nav_0.doc} becomes
     * {@code Lucene104Nav_0.doc}, {@code _3.fdt} becomes {@code fdt}, and {@code segments_5} becomes {@code segments}.
     * Per-field formats keep their suffix, so the files of different postings formats are counted apart.
     */
    static String fileType(String fileName) {
        if (fileName.startsWith("segments")) {
            return "segments";
        }
        if (fileName.startsWith("_")) {
            for (int i = 1; i < fileName.length(); i++) {
                final char c = fileName.charAt(i);
                if (c == '_' || c == '.') {
                    return fileName.substring(i + 1);
                }
            }
        }
        return fileName;
    }

    /** A window being read: windows of different sizes that overlap are different keys (blocks are de-duplicated). */
    private record WindowKey(Path file, long fileId, long offset, int size) {
    }

    /** A block this thread claimed for its read; {@code requested} unless it is only read along with its window. */
    private record Claim(BlockKey key, CompletableFuture<ByteBuffer> loading, boolean requested) {
    }

    /**
     * Starts recording every block load (not hits) until {@link #stopTrace()}, replacing any previous trace. For experiments
     * only: each recorded load walks the stack to find which Lucene code asked for it.
     *
     * @param maxEvents loads beyond this count are counted but not recorded
     */
    void startTrace(int maxEvents) {
        trace = new Trace(maxEvents);
    }

    /** Stops recording and returns the trace, or null if none was running. */
    Trace stopTrace() {
        final Trace t = trace;
        trace = null;
        return t;
    }

    /** Returns the running trace without stopping it, or null. */
    Trace currentTrace() {
        return trace;
    }

    /**
     * Block loads recorded in order. A demand load carries the Lucene callers on the loading thread; a prefetch load
     * carries the callers that requested the prefetch (the load itself runs on a prefetch thread). A read-ahead load is a
     * block that nobody requested, inserted because it shares a read window with a requested block.
     */
    static final class Trace {
        private static final StackWalker WALKER = StackWalker.getInstance();

        final int maxEvents;
        final long startNanos = System.nanoTime();
        final AtomicInteger seq = new AtomicInteger();
        final ConcurrentLinkedQueue<Event> events = new ConcurrentLinkedQueue<>();
        /**
         * Blocks loaded by a prefetch during this trace that no reader has read since (not bounded by maxEvents), with the
         * code that requested the prefetch ("codec caller / search caller"). Read-ahead blocks are not counted here.
         */
        private final Map<BlockKey, String> prefetchedUnread = new ConcurrentHashMap<>();
        /** Read-ahead blocks loaded during this trace that no reader has read since. */
        private final Set<BlockKey> readaheadUnread = ConcurrentHashMap.newKeySet();

        Trace(int maxEvents) {
            this.maxEvents = maxEvents;
        }

        /**
         * @param prefetch  whether a prefetch read loaded the block
         * @param readahead whether the block was not requested and only shares a read window with a requested block
         * @param requester for a prefetch load, the callers that requested it ({@link #callers()} on that thread), or null
         */
        void record(BlockKey key, int size, boolean prefetch, boolean readahead, String[] requester) {
            if (readahead) {
                readaheadUnread.add(key);
            } else if (prefetch) {
                prefetchedUnread.put(key, requester == null ? "null / null" : requester[0] + " / " + requester[1]);
            }
            add(key, size, prefetch, readahead, 0, prefetch ? requester : null);
        }

        /** A reader's block read returned (a hit, its own load, or a load it joined). */
        void recordRead(BlockKey key) {
            prefetchedUnread.remove(key);
            readaheadUnread.remove(key);
        }

        /** Number of blocks loaded by a prefetch during this trace that no reader read. */
        int prefetchedUnreadCount() {
            return prefetchedUnread.size();
        }

        /** Number of read-ahead blocks loaded during this trace that no reader read. */
        int readaheadUnreadCount() {
            return readaheadUnread.size();
        }

        /** Up to {@code limit} of the prefetched-but-unread blocks as "file:block", sorted. */
        List<String> prefetchedUnread(int blockSize, int limit) {
            return prefetchedUnread.keySet()
                .stream()
                .sorted(Comparator.comparing((BlockKey k) -> k.file().getFileName().toString()).thenComparingLong(BlockKey::blockOffset))
                .limit(limit)
                .map(k -> k.file().getFileName() + ":" + k.blockOffset() / blockSize)
                .collect(Collectors.toList());
        }

        /** Prefetched-but-unread blocks counted by the code that requested the prefetch, sorted by requester. */
        Map<String, Integer> prefetchedUnreadByRequester() {
            final Map<String, Integer> counts = new TreeMap<>();
            for (String requester : prefetchedUnread.values()) {
                counts.merge(requester, 1, Integer::sum);
            }
            return counts;
        }

        /** A reader's block read that blocked for {@code waitedNanos}; recorded with size -1. */
        void recordWait(BlockKey key, long waitedNanos) {
            add(key, -1, false, false, waitedNanos, null);
        }

        /** @param requester callers recorded for the event, or null for the callers on this thread */
        private void add(BlockKey key, int size, boolean prefetch, boolean readahead, long waitedNanos, String[] requester) {
            final int n = seq.getAndIncrement();
            if (n >= maxEvents) {
                return;
            }
            final String[] callers = requester != null ? requester : callers();
            events.add(
                new Event(
                    n,
                    System.nanoTime() - startNanos,
                    key.file().getFileName().toString(),
                    key.blockOffset(),
                    size,
                    prefetch,
                    readahead,
                    Thread.currentThread().getName(),
                    callers[0],
                    callers[1],
                    waitedNanos
                )
            );
        }

        /** The innermost Lucene codec frame and the innermost Lucene search frame on the stack, as "Class.method". */
        static String[] callers() {
            return WALKER.walk(frames -> {
                String codec = null;
                String search = null;
                for (StackWalker.StackFrame f : (Iterable<StackWalker.StackFrame>) frames::iterator) {
                    final String c = f.getClassName();
                    if (codec == null && (c.startsWith("org.apache.lucene.codecs.") || c.startsWith("org.apache.lucene.index."))) {
                        codec = c.substring(c.lastIndexOf('.') + 1) + "." + f.getMethodName();
                    } else if (search == null && c.startsWith("org.apache.lucene.search.")) {
                        search = c.substring(c.lastIndexOf('.') + 1) + "." + f.getMethodName();
                    }
                    if (codec != null && search != null) {
                        break;
                    }
                }
                return new String[] { codec, search };
            });
        }

        int dropped() {
            return Math.max(0, seq.get() - maxEvents);
        }
    }

    /** One recorded block load, or (size -1) one read that waited. */
    record Event(int seq, long nanos, String file, long blockOffset, int size, boolean prefetch, boolean readahead, String thread,
        String codecCaller, String searchCaller, long waitedNanos) {
    }

    /** IO counters of one file type. */
    static final class FileStats {
        /** Block lookups by readers. {@code requests - loads} were served from the cache (or joined a concurrent load). */
        final LongAdder requests = new LongAdder();
        /** Blocks inserted because a reader needed them and missed (demand misses). */
        final LongAdder loads = new LongAdder();
        /** Blocks requested by {@code IndexInput.prefetch}, cached or not. */
        final LongAdder prefetchRequests = new LongAdder();
        /** Requested blocks inserted by a prefetch read. */
        final LongAdder prefetchLoads = new LongAdder();
        /** Blocks inserted that nobody requested, because they share a read window with a requested block. */
        final LongAdder readaheadLoads = new LongAdder();
        /** Bytes of all inserted blocks ({@code loads + prefetchLoads + readaheadLoads} blocks). */
        final LongAdder bytesLoaded = new LongAdder();
        /** Storage reads issued by readers' misses. */
        final LongAdder reads = new LongAdder();
        /** Storage reads issued by prefetches. */
        final LongAdder prefetchReads = new LongAdder();
        /** Bytes read from storage by all reads. */
        final LongAdder bytesRead = new LongAdder();
        /** Storage reads by size class, see {@link #sizeClass(int)}. */
        final LongAdder[] readsBySize = new LongAdder[READ_SIZE_CLASSES];
        /** Time spent in all storage reads, including the simulated latency. */
        final LongAdder loadNanos = new LongAdder();

        FileStats() {
            for (int i = 0; i < readsBySize.length; i++) {
                readsBySize[i] = new LongAdder();
            }
        }

        /** Storage reads by size: upper bound of the size class in bytes, to count; classes without reads are left out. */
        Map<Long, Long> readsBySize() {
            final Map<Long, Long> counts = new TreeMap<>();
            for (int i = 0; i < readsBySize.length; i++) {
                final long n = readsBySize[i].sum();
                if (n > 0) {
                    counts.put(1L << i, n);
                }
            }
            return counts;
        }

        void reset() {
            requests.reset();
            loads.reset();
            prefetchRequests.reset();
            prefetchLoads.reset();
            readaheadLoads.reset();
            bytesLoaded.reset();
            reads.reset();
            prefetchReads.reset();
            bytesRead.reset();
            for (LongAdder adder : readsBySize) {
                adder.reset();
            }
            loadNanos.reset();
        }
    }
}
