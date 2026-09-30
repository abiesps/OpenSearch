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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Node-wide cache of fixed-size file blocks, backed by Caffeine.
 *
 * <p>Each block is a read-only, little-endian, direct {@link ByteBuffer} of {@link #blockSize()} bytes (the last block of
 * a file is shorter). The cache is bounded by the total bytes of the cached blocks. There is no explicit memory pool and no
 * reference counting: an evicted block is freed by the garbage collector once no {@link BufferPoolIndexInput} still
 * points at it.
 *
 * <p>Concurrent misses on the same block are de-duplicated by Caffeine: one thread reads the block, the others wait for it.
 *
 * <p>For experiments, the cache counts block requests and loads per file type (see {@link FileStats}) and can add a fixed
 * delay to every load to simulate remote storage such as EFS.
 */
final class BlockCache {

    private static final Logger logger = LogManager.getLogger(BlockCache.class);

    /** Default block size in bytes (128 KiB). */
    static final int DEFAULT_BLOCK_SIZE = 1 << 17;
    static final int MIN_BLOCK_SIZE = 512;
    static final int MAX_BLOCK_SIZE = 8 << 20;

    private final int blockSize;
    private final int blockSizePower;
    private final long blockMask;
    private final Cache<BlockKey, ByteBuffer> cache;
    private final Executor prefetchExecutor;
    private final ConcurrentMap<String, FileStats> stats = new ConcurrentHashMap<>();
    private volatile long simulatedLoadLatencyNanos;
    /** Non-null while a trace is being recorded, see {@link #startTrace(int)}. */
    private volatile Trace trace;

    /**
     * Creates a cache with {@link #DEFAULT_BLOCK_SIZE} blocks.
     *
     * @param maxBytes         upper bound of the total size of all cached blocks
     * @param prefetchExecutor executor that loads prefetched blocks in the background
     */
    BlockCache(long maxBytes, Executor prefetchExecutor) {
        this(maxBytes, DEFAULT_BLOCK_SIZE, prefetchExecutor);
    }

    /**
     * @param maxBytes         upper bound of the total size of all cached blocks
     * @param blockSize        block size in bytes, a power of two between {@link #MIN_BLOCK_SIZE} and {@link #MAX_BLOCK_SIZE}
     * @param prefetchExecutor executor that loads prefetched blocks in the background
     */
    BlockCache(long maxBytes, int blockSize, Executor prefetchExecutor) {
        if (Integer.bitCount(blockSize) != 1 || blockSize < MIN_BLOCK_SIZE || blockSize > MAX_BLOCK_SIZE) {
            throw new IllegalArgumentException(
                "block size must be a power of two between " + MIN_BLOCK_SIZE + " and " + MAX_BLOCK_SIZE + ", got " + blockSize
            );
        }
        this.blockSize = blockSize;
        this.blockSizePower = Integer.numberOfTrailingZeros(blockSize);
        this.blockMask = blockSize - 1L;
        this.cache = Caffeine.newBuilder()
            .maximumWeight(maxBytes)
            .weigher((BlockKey key, ByteBuffer block) -> block.capacity())
            // run cache maintenance on the calling thread so the cache owns no background threads
            .executor(Runnable::run)
            .build();
        this.prefetchExecutor = prefetchExecutor;
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

    /**
     * Returns the block for {@code key}, reading it from {@code channel} on a miss.
     *
     * @param fileLength length of the whole file, which bounds the size of its last block
     * @param fileStats  counters of the file's type, from {@link #statsFor(String)}
     */
    ByteBuffer getOrLoad(BlockKey key, FileChannel channel, long fileLength, FileStats fileStats) throws IOException {
        fileStats.requests.increment();
        final Trace t = trace;
        if (t == null) {
            return getOrLoad(key, channel, fileLength, fileStats, false);
        }
        // tracing only: record reads that blocked, on their own load or on a load already in flight (e.g. a prefetch)
        final long start = System.nanoTime();
        final ByteBuffer block = getOrLoad(key, channel, fileLength, fileStats, false);
        final long waited = System.nanoTime() - start;
        if (waited > TimeUnit.MICROSECONDS.toNanos(100)) {
            t.recordWait(key, waited);
        }
        return block;
    }

    private ByteBuffer getOrLoad(BlockKey key, FileChannel channel, long fileLength, FileStats fileStats, boolean prefetch)
        throws IOException {
        try {
            return cache.get(key, k -> load(k, channel, fileLength, fileStats, prefetch));
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * Loads the missing blocks among {@code blockCount} blocks that start at {@code firstBlockOffset}, asynchronously.
     * Best effort: the request is dropped when the prefetch queue is full, and load failures are only logged.
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
        final List<BlockKey> missing = new ArrayList<>();
        for (long i = 0; i < blockCount; i++) {
            BlockKey key = new BlockKey(file, fileId, firstBlockOffset + (i << blockSizePower));
            // containsKey does not count as an access, so a prefetch does not skew the eviction policy
            if (cache.asMap().containsKey(key) == false) {
                missing.add(key);
            }
        }
        fileStats.prefetchRequests.add(blockCount);
        if (missing.isEmpty()) {
            return;
        }
        try {
            prefetchExecutor.execute(() -> {
                for (BlockKey key : missing) {
                    try {
                        getOrLoad(key, channel, fileLength, fileStats, true);
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

    /** Sets a delay added to every block load; 0 disables it. */
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

    private ByteBuffer load(BlockKey key, FileChannel channel, long fileLength, FileStats fileStats, boolean prefetch) {
        final long start = System.nanoTime();
        final int size = (int) Math.min(blockSize, fileLength - key.blockOffset());
        final ByteBuffer block = ByteBuffer.allocateDirect(size);
        try {
            Channels.readFromFileChannelWithEofException(channel, key.blockOffset(), block);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        final long latency = simulatedLoadLatencyNanos;
        if (latency > 0) {
            final long deadline = start + latency;
            for (long now = System.nanoTime(); now < deadline; now = System.nanoTime()) {
                LockSupport.parkNanos(deadline - now);
            }
        }
        final Trace t = trace;
        if (t != null) {
            t.record(key, size, prefetch);
        }
        (prefetch ? fileStats.prefetchLoads : fileStats.loads).increment();
        fileStats.bytesLoaded.add(size);
        fileStats.loadNanos.add(System.nanoTime() - start);
        // readers only use absolute gets, so the shared buffer is never mutated and needs no position/limit reset
        return block.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
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

    /** Block loads recorded in order. */
    static final class Trace {
        private static final StackWalker WALKER = StackWalker.getInstance();

        final int maxEvents;
        final long startNanos = System.nanoTime();
        final AtomicInteger seq = new AtomicInteger();
        final ConcurrentLinkedQueue<Event> events = new ConcurrentLinkedQueue<>();

        Trace(int maxEvents) {
            this.maxEvents = maxEvents;
        }

        void record(BlockKey key, int size, boolean prefetch) {
            add(key, size, prefetch, 0);
        }

        /** A reader's block read that blocked for {@code waitedNanos}; recorded with size -1. */
        void recordWait(BlockKey key, long waitedNanos) {
            add(key, -1, false, waitedNanos);
        }

        private void add(BlockKey key, int size, boolean prefetch, long waitedNanos) {
            final int n = seq.getAndIncrement();
            if (n >= maxEvents) {
                return;
            }
            final String[] callers = callers();
            events.add(
                new Event(
                    n,
                    System.nanoTime() - startNanos,
                    key.file().getFileName().toString(),
                    key.blockOffset(),
                    size,
                    prefetch,
                    Thread.currentThread().getName(),
                    callers[0],
                    callers[1],
                    waitedNanos
                )
            );
        }

        /** The innermost Lucene codec frame and the innermost Lucene search frame on the stack, as "Class.method". */
        private static String[] callers() {
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

    /** One recorded block load. */
    record Event(int seq, long nanos, String file, long blockOffset, int size, boolean prefetch, String thread, String codecCaller,
        String searchCaller, long waitedNanos) {
    }

    /** IO counters of one file type. */
    static final class FileStats {
        /** Block lookups by readers. {@code requests - loads} were served from the cache (or joined a concurrent load). */
        final LongAdder requests = new LongAdder();
        /** Blocks loaded from the file because a reader needed them. */
        final LongAdder loads = new LongAdder();
        /** Blocks requested by {@code IndexInput.prefetch}, cached or not. */
        final LongAdder prefetchRequests = new LongAdder();
        /** Blocks loaded from the file by prefetch. */
        final LongAdder prefetchLoads = new LongAdder();
        /** Bytes read from the file by all loads. */
        final LongAdder bytesLoaded = new LongAdder();
        /** Time spent in all loads, including the simulated latency. */
        final LongAdder loadNanos = new LongAdder();

        void reset() {
            requests.reset();
            loads.reset();
            prefetchRequests.reset();
            prefetchLoads.reset();
            bytesLoaded.reset();
            loadNanos.reset();
        }
    }
}
