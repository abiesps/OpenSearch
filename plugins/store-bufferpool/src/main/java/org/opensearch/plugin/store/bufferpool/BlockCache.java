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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
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
 * touches. A window is always one storage read of the whole window, clipped at the end of the file, even if some of its
 * blocks are cached or being read by another thread: those blocks are neither replaced nor waited for, their bytes are
 * dropped and counted ({@link FileStats#bytesOverread}). Every other block of the window is inserted. With both read sizes
 * equal to the block size, every read is one block. Each read goes through {@link StorageFile#read}, which announces the
 * window to the kernel when read hints are on, so the window is also one storage IO (see {@link NativeReadHints}).
 *
 * <p>Concurrent misses are de-duplicated twice: per window (one thread reads a window of a given size, the others wait
 * for it) and per block (a block that a read of another size is loading is waited for, never inserted a second time).
 *
 * <p>Memory: the cache weight counts the cached blocks only. Outside it, a multi-block read uses a direct read buffer of
 * the largest read size, from a pool of idle buffers bounded to 32 MiB ({@link #SCRATCH_POOL_BYTES}; a read that finds the
 * pool empty allocates one, so the transient amount grows with the number of concurrent window reads), and copies each
 * inserted block into its own direct buffer. Size {@code -XX:MaxDirectMemorySize} for the cache plus these buffers.
 *
 * <p>Prefetch reads run on the workers of a {@link PrefetchScheduler}, which bounds them by the node read budget. Every
 * demand storage read is bracketed by {@link PrefetchScheduler#demandReadStarted()} and
 * {@link PrefetchScheduler#demandReadFinished()}, so that a budget that counts total reads leaves prefetch only the slots
 * that demand reads do not use; demand reads are counted, never throttled.
 *
 * <p>For experiments, the cache counts block requests, reads, inserted and skipped blocks, and waits per file type (see
 * {@link FileStats}), records storage-read latencies per origin and size class and demand waits in histograms, and can add
 * a fixed delay to every storage read to simulate remote storage such as EFS.
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
    static final long SCRATCH_POOL_BYTES = 32L << 20;
    /**
     * Size classes of the read latency histograms, keyed by the upper bound of the {@link FileStats#readsBySize} class:
     * {@code 32768} holds reads of more than 16 KiB up to 32 KiB, {@code 131072} reads of more than 64 KiB up to 128 KiB, and
     * {@code other} every other size.
     */
    static final List<String> LATENCY_CLASSES = List.of("32768", "131072", "other");
    /** Defaults of a scheduler built for the {@link Executor} constructors: today's fixed prefetch pool and queue. */
    static final int DEFAULT_MAX_IN_FLIGHT = 8;
    static final int DEFAULT_QUEUE_SIZE = 1024;

    private final int blockSize;
    private final int blockSizePower;
    private final long blockMask;
    private final int randomReadSize;
    private final int sequentialReadSize;
    private final Cache<BlockKey, ByteBuffer> cache;
    private final PrefetchScheduler scheduler;
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
    private final NativeReadHints readHints;
    /** Prefetch requests that spanned more than one window and had a missing block. */
    private final LongAdder multiWindowPrefetches = new LongAdder();
    /** Prefetch storage reads running now, and the most that ran at once since start or the last {@link #resetStats()}. */
    private final AtomicInteger prefetchReadsInFlight = new AtomicInteger();
    private final AtomicInteger maxPrefetchReadsInFlight = new AtomicInteger();
    /** The most prefetch plus demand storage reads at once, sampled when either kind of read starts. */
    private final AtomicInteger maxTotalReadsInFlight = new AtomicInteger();
    /** Prefetch reads that started on a thread without a prefetch worker slot (a scheduler defect; must stay 0). */
    private final LongAdder prefetchReadsOutsideSlot = new LongAdder();
    /** Prefetch reads that started while the same thread had one in flight (a defect; must stay 0). */
    private final LongAdder nestedPrefetchReads = new LongAdder();
    private final AtomicBoolean slotDefectLogged = new AtomicBoolean();
    /** Prefetch reads in flight on the current thread. */
    private final ThreadLocal<int[]> prefetchReadsOnThread = ThreadLocal.withInitial(() -> new int[1]);
    /** Bytes and total wall time of prefetch storage reads, and of demand storage reads. */
    private final LongAdder prefetchBytesRead = new LongAdder();
    private final LongAdder prefetchReadNanos = new LongAdder();
    private final LongAdder demandBytesRead = new LongAdder();
    private final LongAdder demandReadNanos = new LongAdder();
    /** Total time with at least one prefetch read in flight, and the start of the current such interval. */
    private final LongAdder prefetchBusyNanos = new LongAdder();
    private final AtomicLong prefetchBusySince = new AtomicLong();
    /** Windows of multi-window prefetch items not read because the item's search was cancelled. */
    private final LongAdder windowsSkippedCancelled = new LongAdder();
    /** Storage-read latency per origin (prefetch, demand) and size class ({@link #LATENCY_CLASSES}). */
    private final LatencyHistogram[] prefetchReadLatency = newHistograms();
    private final LatencyHistogram[] demandReadLatency = newHistograms();
    /** Time a demand reader waited for a load that another thread was running. */
    private final LatencyHistogram demandWait = new LatencyHistogram();
    private volatile Runnable betweenResetStepsHook;
    /** Whether each window of a prefetch is its own task, see {@link #setPrefetchTaskPerWindow(boolean)}. */
    private volatile boolean prefetchTaskPerWindow;
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
        this(maxBytes, blockSize, randomReadSize, sequentialReadSize, NativeReadHints.DISABLED, prefetchExecutor);
    }

    /**
     * @param readHints        how files opened for this cache announce their reads to the kernel, see {@link #readHints()}
     */
    BlockCache(
        long maxBytes,
        int blockSize,
        int randomReadSize,
        int sequentialReadSize,
        NativeReadHints readHints,
        Executor prefetchExecutor
    ) {
        this(maxBytes, blockSize, randomReadSize, sequentialReadSize, readHints, defaultScheduler(prefetchExecutor));
    }

    /**
     * A scheduler with today's defaults over {@code executor}: budget {@value #DEFAULT_MAX_IN_FLIGHT} prefetch reads, scope
     * {@code prefetch}, queue {@value #DEFAULT_QUEUE_SIZE}, {@code fifo}.
     */
    static PrefetchScheduler defaultScheduler(Executor executor) {
        return new PrefetchScheduler(
            executor::execute,
            DEFAULT_MAX_IN_FLIGHT,
            PrefetchScheduler.BudgetScope.PREFETCH,
            DEFAULT_QUEUE_SIZE,
            PrefetchScheduler.QueuePolicy.FIFO,
            PrefetchScheduler.PrefetchOwner::ofCurrentThread
        );
    }

    /**
     * @param scheduler runs the prefetch items and counts the demand reads against the node read budget
     */
    BlockCache(
        long maxBytes,
        int blockSize,
        int randomReadSize,
        int sequentialReadSize,
        NativeReadHints readHints,
        PrefetchScheduler scheduler
    ) {
        validateBlockSize(blockSize);
        validateReadSize("random read size", randomReadSize, blockSize);
        validateReadSize("sequential read size", sequentialReadSize, blockSize);
        this.blockSize = blockSize;
        this.blockSizePower = Integer.numberOfTrailingZeros(blockSize);
        this.blockMask = blockSize - 1L;
        this.randomReadSize = randomReadSize;
        this.sequentialReadSize = sequentialReadSize;
        this.maxReadSize = Math.max(randomReadSize, sequentialReadSize);
        this.readHints = readHints;
        this.readBuffers = new ArrayBlockingQueue<>((int) Math.max(1, SCRATCH_POOL_BYTES / maxReadSize));
        this.cache = Caffeine.newBuilder()
            .maximumWeight(maxBytes)
            .weigher((BlockKey key, ByteBuffer block) -> block.capacity())
            // run cache maintenance on the calling thread so the cache owns no background threads
            .executor(Runnable::run)
            .build();
        this.scheduler = scheduler;
    }

    private static LatencyHistogram[] newHistograms() {
        final LatencyHistogram[] histograms = new LatencyHistogram[LATENCY_CLASSES.size()];
        for (int i = 0; i < histograms.length; i++) {
            histograms[i] = new LatencyHistogram();
        }
        return histograms;
    }

    /** Index into {@link #LATENCY_CLASSES} of a read of {@code size} bytes. */
    static int latencyClass(int size) {
        return switch (sizeClass(size)) {
            case 15 -> 0;
            case 17 -> 1;
            default -> 2;
        };
    }

    PrefetchScheduler scheduler() {
        return scheduler;
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

    /** Read hints of the files opened for this cache ({@link StorageFile#open(Path, NativeReadHints)}). */
    NativeReadHints readHints() {
        return readHints;
    }

    /**
     * Whether each aligned window of a prefetch request is submitted as its own prefetch task, so the windows of one
     * request are read concurrently (up to the prefetch thread count), instead of one task that reads them one after
     * another. Tasks that do not fit in the prefetch queue are dropped and counted.
     */
    void setPrefetchTaskPerWindow(boolean taskPerWindow) {
        this.prefetchTaskPerWindow = taskPerWindow;
    }

    boolean prefetchTaskPerWindow() {
        return prefetchTaskPerWindow;
    }

    /**
     * Prefetch tasks submitted and not finished, queued or running. Together with {@link #inFlightReads()} == 0 this means
     * that no prefetch will insert blocks any more, until the next prefetch request.
     */
    int pendingPrefetchTasks() {
        return scheduler.pending();
    }

    /** Prefetch tasks dropped for any reason (see {@link PrefetchScheduler.DropReason}), since start or the last reset. */
    long rejectedPrefetchTasks() {
        return scheduler.droppedTotal();
    }

    /** Prefetch requests that spanned more than one window and had a missing block. */
    long multiWindowPrefetches() {
        return multiWindowPrefetches.sum();
    }

    /** The most prefetch storage reads that ran at the same time, since start or the last {@link #resetStats()}. */
    int maxPrefetchReadsInFlight() {
        return maxPrefetchReadsInFlight.get();
    }

    /** Prefetch storage reads running now. */
    int prefetchReadsInFlight() {
        return prefetchReadsInFlight.get();
    }

    /** The most prefetch plus demand storage reads at once (sampled when a read starts), since start or the last reset. */
    int maxTotalReadsInFlight() {
        return maxTotalReadsInFlight.get();
    }

    long prefetchReadsOutsideSlot() {
        return prefetchReadsOutsideSlot.sum();
    }

    long nestedPrefetchReads() {
        return nestedPrefetchReads.sum();
    }

    long prefetchBytesRead() {
        return prefetchBytesRead.sum();
    }

    long prefetchReadTimeMicros() {
        return TimeUnit.NANOSECONDS.toMicros(prefetchReadNanos.sum());
    }

    long demandBytesRead() {
        return demandBytesRead.sum();
    }

    long demandReadTimeMicros() {
        return TimeUnit.NANOSECONDS.toMicros(demandReadNanos.sum());
    }

    /** Total time with at least one prefetch storage read in flight. */
    long prefetchBusyTimeMicros() {
        return TimeUnit.NANOSECONDS.toMicros(prefetchBusyNanos.sum());
    }

    long windowsSkippedCancelled() {
        return windowsSkippedCancelled.sum();
    }

    /** Storage-read latency of prefetch ({@code true}) or demand reads, for the size class {@code LATENCY_CLASSES.get(c)}. */
    LatencyHistogram.Snapshot readLatency(boolean prefetch, int c) {
        return (prefetch ? prefetchReadLatency : demandReadLatency)[c].snapshot();
    }

    /** Waits of demand readers for a load that another thread was running. */
    LatencyHistogram.Snapshot demandWait() {
        return demandWait.snapshot();
    }

    /**
     * Returns the block for {@code key}, reading the window of {@code readSize} bytes that holds it from {@code channel}
     * on a miss.
     *
     * @param fileLength length of the whole file, which bounds the window and the size of the file's last block
     * @param readSize   the reader's read size, {@link #randomReadSize()} or {@link #sequentialReadSize()}
     * @param fileStats  counters of the file's type, from {@link #statsFor(String)}
     */
    ByteBuffer getOrLoad(BlockKey key, StorageFile storage, long fileLength, int readSize, FileStats fileStats) throws IOException {
        fileStats.requests.increment();
        final Trace t = trace;
        if (t == null) {
            return getOrLoadBlock(key, storage, fileLength, readSize, fileStats);
        }
        // tracing only: record reads that blocked, on their own load or on a load already in flight (e.g. a prefetch)
        final long start = System.nanoTime();
        final ByteBuffer block = getOrLoadBlock(key, storage, fileLength, readSize, fileStats);
        final long waited = System.nanoTime() - start;
        if (waited > TimeUnit.MICROSECONDS.toNanos(100)) {
            t.recordWait(key, waited);
        }
        t.recordRead(key);
        return block;
    }

    private ByteBuffer getOrLoadBlock(BlockKey key, StorageFile storage, long fileLength, int readSize, FileStats fileStats)
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
                final long waitStart = System.nanoTime();
                final ByteBuffer loaded = inFlight.join();
                final long waited = System.nanoTime() - waitStart;
                fileStats.recordWait(waited);
                demandWait.recordNanos(waited);
                if (loaded != null) {
                    return loaded;
                }
                // that read failed: look again, and read it here if it is still missing
                continue;
            }
            final ByteBuffer loaded = loadWindow(
                key.file(),
                key.fileId(),
                storage,
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
     * aligned windows of {@link #sequentialReadSize()} bytes: each window that holds a missing requested block is one read
     * of the whole window. The windows are read by one prefetch task one after another, or by one task each (see
     * {@link #setPrefetchTaskPerWindow(boolean)}). The tasks are items of the {@link PrefetchScheduler}. Best effort: a task
     * is dropped when the prefetch queue is full or its search is cancelled, and load failures are only logged.
     */
    void prefetch(
        Path file,
        long fileId,
        StorageFile storage,
        long fileLength,
        long firstBlockOffset,
        long blockCount,
        FileStats fileStats
    ) {
        fileStats.prefetchRequests.add(blockCount);
        final long lastBlockOffset = firstBlockOffset + ((blockCount - 1) << blockSizePower);
        if (anyMissing(file, fileId, firstBlockOffset, lastBlockOffset) == false) {
            return;
        }
        // tracing only: the prefetch runs on another thread, so remember which code asked for it
        final String[] requester = trace == null ? null : Trace.callers();
        final long windowMask = -(long) sequentialReadSize;
        final long firstWindow = firstBlockOffset & windowMask;
        final boolean multiWindow = firstWindow + sequentialReadSize <= lastBlockOffset;
        if (multiWindow) {
            multiWindowPrefetches.increment();
        }
        if (prefetchTaskPerWindow && multiWindow) {
            for (long window = firstWindow; window <= lastBlockOffset; window += sequentialReadSize) {
                final long w = window;
                final long first = Math.max(firstBlockOffset, w);
                final long last = Math.min(lastBlockOffset, w + sequentialReadSize - blockSize);
                if (anyMissing(file, fileId, first, last)) {
                    submitPrefetch(
                        file,
                        cancelled -> prefetchWindows(file, fileId, storage, fileLength, w, w, first, last, fileStats, requester, cancelled)
                    );
                }
            }
        } else {
            submitPrefetch(
                file,
                cancelled -> prefetchWindows(
                    file,
                    fileId,
                    storage,
                    fileLength,
                    firstWindow,
                    lastBlockOffset,
                    firstBlockOffset,
                    lastBlockOffset,
                    fileStats,
                    requester,
                    cancelled
                )
            );
        }
    }

    /** Whether a block between {@code first} and {@code last} (block offsets, inclusive) is neither cached nor being read. */
    private boolean anyMissing(Path file, long fileId, long first, long last) {
        for (long offset = first; offset <= last; offset += blockSize) {
            final BlockKey key = new BlockKey(file, fileId, offset);
            // containsKey does not count as an access, so a prefetch does not skew the eviction policy
            if (cache.asMap().containsKey(key) == false && loadingBlocks.containsKey(key) == false) {
                return true;
            }
        }
        return false;
    }

    private void submitPrefetch(Path file, Consumer<BooleanSupplier> task) {
        // a dropped item costs nothing for correctness: a later miss reads the window on demand
        scheduler.submit(new PrefetchScheduler.Item() {
            @Override
            public void run(BooleanSupplier cancelled) {
                task.accept(cancelled);
            }

            @Override
            public String description() {
                return file.toString();
            }
        });
    }

    /**
     * Reads the windows of a prefetch of {@code blockCount} blocks from {@code firstBlockOffset} on the calling thread, as a
     * prefetch item would. For tests of the read-count checks only (a call outside a worker counts as a read outside a slot).
     */
    void prefetchNowForTests(
        Path file,
        long fileId,
        StorageFile storage,
        long fileLength,
        long firstBlockOffset,
        long blockCount,
        FileStats fileStats
    ) {
        final long lastBlockOffset = firstBlockOffset + ((blockCount - 1) << blockSizePower);
        final long firstWindow = firstBlockOffset & -(long) sequentialReadSize;
        prefetchWindows(
            file,
            fileId,
            storage,
            fileLength,
            firstWindow,
            lastBlockOffset,
            firstBlockOffset,
            lastBlockOffset,
            fileStats,
            null,
            PrefetchScheduler.PrefetchOwner.NEVER
        );
    }

    /**
     * Reads the sequential windows that start at {@code fromWindow} up to the one that holds {@code toBlock}, each only if a
     * requested block in it (between {@code requestedFirst} and {@code requestedLast}) is missing. Stops at the first failure,
     * and before a window once {@code cancelled} is true (the worker checked it before the first window).
     */
    private void prefetchWindows(
        Path file,
        long fileId,
        StorageFile storage,
        long fileLength,
        long fromWindow,
        long toBlock,
        long requestedFirst,
        long requestedLast,
        FileStats fileStats,
        String[] requester,
        BooleanSupplier cancelled
    ) {
        for (long window = fromWindow; window <= toBlock; window += sequentialReadSize) {
            if (window != fromWindow && cancelled.getAsBoolean()) {
                windowsSkippedCancelled.add((toBlock - window) / sequentialReadSize + 1);
                return;
            }
            try {
                loadWindow(
                    file,
                    fileId,
                    storage,
                    fileLength,
                    sequentialReadSize,
                    window,
                    null,
                    Math.max(requestedFirst, window),
                    Math.min(requestedLast, window + sequentialReadSize - blockSize),
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
    }

    /**
     * Reads one window with one storage read if one of the requested blocks in it is neither cached nor being read, and
     * inserts every block of the window that is neither cached nor being read. The bytes of the other blocks are dropped:
     * a cached block is never replaced, and a block that another read is loading is left to that read. Returns without
     * reading if another thread is reading the same window (a demand read waits for it first). Never waits while it holds
     * a claim, so concurrent window reads cannot deadlock.
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
        StorageFile storage,
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
                final long waitStart = System.nanoTime();
                other.join();
                final long waited = System.nanoTime() - waitStart;
                fileStats.recordWait(waited);
                demandWait.recordNanos(waited);
            }
            return null;
        }
        final boolean prefetch = wanted == null;
        final long windowEnd = Math.min(windowStart + readSize, fileLength);
        final int windowBlocks = Math.toIntExact((windowEnd - windowStart + blockSize - 1) >>> blockSizePower);
        // claims[i] is this read's claim on block i of the window, or null if the block is cached or another read loads it
        final Claim[] claims = new Claim[windowBlocks];
        try {
            boolean anyRequested = false;
            int cachedBlocks = 0;
            int inFlightBlocks = 0;
            for (int i = 0; i < windowBlocks; i++) {
                final long offset = windowStart + ((long) i << blockSizePower);
                final boolean requested = prefetch ? offset >= requestedFirst && offset <= requestedLast : offset == wanted.blockOffset();
                final BlockKey key = requested && prefetch == false ? wanted : new BlockKey(file, fileId, offset);
                if (cache.asMap().containsKey(key)) {
                    cachedBlocks++;
                    continue;
                }
                final CompletableFuture<ByteBuffer> loading = new CompletableFuture<>();
                if (loadingBlocks.putIfAbsent(key, loading) != null) {
                    inFlightBlocks++; // a read of another size or origin is loading it
                    continue;
                }
                // a read that finished between containsKey and the claim already inserted the block
                if (cache.asMap().containsKey(key)) {
                    loadingBlocks.remove(key, loading);
                    loading.complete(null);
                    cachedBlocks++;
                    continue;
                }
                claims[i] = new Claim(key, loading, requested);
                anyRequested |= requested;
            }
            if (anyRequested == false) {
                return null; // the finally block releases the claims: never read a window only for its neighbours
            }
            return readWindow(storage, windowStart, windowEnd, claims, cachedBlocks, inFlightBlocks, prefetch, fileStats, requester);
        } finally {
            // every claim is released even if the read failed, so no reader waits forever; waiters on a failed block retry
            for (Claim claim : claims) {
                if (claim != null && claim.loading.isDone() == false) {
                    loadingBlocks.remove(claim.key, claim.loading);
                    claim.loading.complete(null);
                }
            }
            loadingWindows.remove(windowKey, window);
            window.complete(null);
        }
    }

    /**
     * Reads the window {@code [windowStart, windowEnd)} with one storage read, inserts the claimed blocks, drops the bytes
     * of the others and releases the claims.
     *
     * @param claims         per block of the window, this read's claim, or null for a block that is not inserted
     * @param cachedBlocks   blocks of the window that were cached when it was claimed
     * @param inFlightBlocks blocks of the window that another read was loading
     * @return the block requested by a demand read, or null
     */
    private ByteBuffer readWindow(
        StorageFile storage,
        long windowStart,
        long windowEnd,
        Claim[] claims,
        int cachedBlocks,
        int inFlightBlocks,
        boolean prefetch,
        FileStats fileStats,
        String[] requester
    ) throws IOException {
        final int size = Math.toIntExact(windowEnd - windowStart);
        final ByteBuffer[] blocks = new ByteBuffer[claims.length];
        final int[] onThread = prefetch ? prefetchReadStarted() : null;
        if (prefetch == false) {
            final int demand = scheduler.demandReadStarted();
            updateMax(maxTotalReadsInFlight, demand + prefetchReadsInFlight.get());
        }
        // the span of the read counters: from the in-flight increment to the decrement, as Little's law needs
        final long start = System.nanoTime();
        try {
            if (claims.length == 1) {
                blocks[0] = ByteBuffer.allocateDirect(size);
                storage.read(windowStart, blocks[0]);
            } else {
                ByteBuffer buffer = readBuffers.poll();
                if (buffer == null) {
                    buffer = ByteBuffer.allocateDirect(maxReadSize);
                }
                try {
                    buffer.clear().limit(size);
                    storage.read(windowStart, buffer);
                    for (int b = 0; b < claims.length; b++) {
                        if (claims[b] != null) {
                            final int from = b << blockSizePower;
                            final int length = Math.min(blockSize, size - from);
                            blocks[b] = ByteBuffer.allocateDirect(length).put(buffer.slice(from, length));
                        }
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
        } finally {
            final long took = System.nanoTime() - start;
            final int latencyClass = latencyClass(size);
            if (prefetch) {
                prefetchReadNanos.add(took);
                prefetchReadLatency[latencyClass].recordNanos(took);
                prefetchReadFinished(onThread);
            } else {
                demandReadNanos.add(took);
                demandReadLatency[latencyClass].recordNanos(took);
                scheduler.demandReadFinished();
            }
        }
        (prefetch ? prefetchBytesRead : demandBytesRead).add(size);
        (prefetch ? fileStats.prefetchReads : fileStats.reads).increment();
        fileStats.bytesRead.add(size);
        fileStats.readsBySize[sizeClass(size)].increment();
        fileStats.loadNanos.add(System.nanoTime() - start);
        fileStats.windowBlocks.add(claims.length);
        fileStats.windowBlocksInFlight.add(inFlightBlocks);
        final Trace t = trace;
        ByteBuffer result = null;
        long overread = 0;
        for (int b = 0; b < claims.length; b++) {
            final Claim claim = claims[b];
            if (claim == null) {
                overread += Math.min(blockSize, size - (b << blockSizePower));
                continue;
            }
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
            } else {
                // only claim holders insert, so this does not happen; count it like a cached block to keep the identity
                cachedBlocks++;
                overread += block.capacity();
            }
            if (prefetch == false && claim.requested) {
                result = value;
            }
            loadingBlocks.remove(claim.key, claim.loading);
            claim.loading.complete(value);
        }
        fileStats.windowBlocksCached.add(cachedBlocks);
        fileStats.bytesOverread.add(overread);
        return result;
    }

    private static void updateMax(AtomicInteger max, int value) {
        if (value > max.get()) {
            max.accumulateAndGet(value, Math::max);
        }
    }

    /**
     * Counts a prefetch storage read that starts on this thread: the in-flight counters, the busy interval, and the two
     * checks that every prefetch read runs on a worker that holds a slot and is the only one in flight on its thread.
     *
     * @return the per-thread count of prefetch reads in flight, for {@link #prefetchReadFinished}
     */
    int[] prefetchReadStarted() {
        final int inFlight = prefetchReadsInFlight.incrementAndGet();
        if (inFlight == 1) {
            prefetchBusySince.set(System.nanoTime());
        }
        updateMax(maxPrefetchReadsInFlight, inFlight);
        updateMax(maxTotalReadsInFlight, inFlight + scheduler.demandReadsInFlight());
        if (scheduler.currentThreadHoldsSlot() == false) {
            prefetchReadsOutsideSlot.increment();
            logSlotDefect("a prefetch read started on a thread without a prefetch worker slot");
        }
        final int[] onThread = prefetchReadsOnThread.get();
        if (onThread[0]++ > 0) {
            nestedPrefetchReads.increment();
            logSlotDefect("a prefetch read started while the same thread had one in flight");
        }
        return onThread;
    }

    void prefetchReadFinished(int[] onThread) {
        onThread[0]--;
        if (prefetchReadsInFlight.decrementAndGet() == 0) {
            // a new first read can start between the decrement and this get: then the added interval is near zero, never negative
            final long since = prefetchBusySince.get();
            prefetchBusyNanos.add(Math.max(0, System.nanoTime() - since));
        }
    }

    private void logSlotDefect(String what) {
        if (slotDefectLogged.compareAndSet(false, true)) {
            logger.warn("{} (logged once per node; see prefetch_reads_outside_slot and nested_prefetch_reads)", what);
        }
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

    /**
     * Number of blocks and windows being read right now (0 when idle). For tests and stats. This counts running reads
     * only: a prefetch task still in the queue is not counted, see {@link #pendingPrefetchTasks()}.
     */
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
        // the scheduler first: it sets max_active_workers before this sets max_reads_in_flight, so that a worker start after
        // the reset raises max_active_workers (under the scheduler lock, before its first read) and the pair stays ordered
        scheduler.resetStats();
        final Runnable hook = betweenResetStepsHook;
        if (hook != null) {
            hook.run();
        }
        stats.values().forEach(FileStats::reset);
        multiWindowPrefetches.reset();
        maxPrefetchReadsInFlight.set(prefetchReadsInFlight.get());
        maxTotalReadsInFlight.set(prefetchReadsInFlight.get() + scheduler.demandReadsInFlight());
        prefetchReadsOutsideSlot.reset();
        nestedPrefetchReads.reset();
        prefetchBytesRead.reset();
        prefetchReadNanos.reset();
        demandBytesRead.reset();
        demandReadNanos.reset();
        prefetchBusyNanos.reset();
        windowsSkippedCancelled.reset();
        for (LatencyHistogram h : prefetchReadLatency) {
            h.reset();
        }
        for (LatencyHistogram h : demandReadLatency) {
            h.reset();
        }
        demandWait.reset();
        readHints.resetCounters();
    }

    /** Test hook (null in production): runs in {@link #resetStats()} between the scheduler reset and the cache reset. */
    void setBetweenResetStepsHookForTests(Runnable hook) {
        this.betweenResetStepsHook = hook;
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
        /**
         * Blocks spanned by all storage reads. Per file type, {@code windowBlocks == blocksInserted + windowBlocksCached +
         * windowBlocksInFlight}, where blocksInserted is {@code loads + prefetchLoads + readaheadLoads}.
         */
        final LongAdder windowBlocks = new LongAdder();
        /** Blocks of a read window that were not inserted because they were cached already. */
        final LongAdder windowBlocksCached = new LongAdder();
        /** Blocks of a read window that were not inserted because another read was loading them. */
        final LongAdder windowBlocksInFlight = new LongAdder();
        /** Bytes read from storage but not inserted (the skipped blocks of read windows): {@code bytesRead - bytesLoaded}. */
        final LongAdder bytesOverread = new LongAdder();
        /** Block reads by readers that waited for another thread's read of the block or of its window. */
        final LongAdder waits = new LongAdder();
        /** Time readers spent in those waits. */
        final LongAdder waitNanos = new LongAdder();

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
            windowBlocks.reset();
            windowBlocksCached.reset();
            windowBlocksInFlight.reset();
            bytesOverread.reset();
            waits.reset();
            waitNanos.reset();
        }

        void recordWait(long nanos) {
            waits.increment();
            waitNanos.add(nanos);
        }

        long blocksInserted() {
            return loads.sum() + prefetchLoads.sum() + readaheadLoads.sum();
        }
    }
}
