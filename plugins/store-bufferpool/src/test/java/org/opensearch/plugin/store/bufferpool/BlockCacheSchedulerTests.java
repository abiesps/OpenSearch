/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.BudgetScope;
import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.PrefetchOwner;
import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.QueuePolicy;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** The block cache with a prefetch scheduler: the read bound, the read-count checks and the read counters and histograms. */
public class BlockCacheSchedulerTests extends OpenSearchTestCase {

    private static final int BLOCK = 1024;

    private final List<ExecutorService> executors = new ArrayList<>();
    private final List<StorageFile> files = new ArrayList<>();
    private Path file;
    private int length;
    private StorageFile channel;

    @Override
    public void tearDown() throws Exception {
        for (ExecutorService executor : executors) {
            executor.shutdown();
            assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));
        }
        for (StorageFile f : files) {
            f.close();
        }
        super.tearDown();
    }

    private void open(int bytes) throws IOException {
        final byte[] data = new byte[bytes];
        random().nextBytes(data);
        length = bytes;
        file = createTempDir().resolve("_0.dvd");
        Files.write(file, data);
        channel = StorageFile.open(file, NativeReadHints.DISABLED);
        files.add(channel);
    }

    private ExecutorService pool(int threads) {
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        executors.add(executor);
        return executor;
    }

    private static PrefetchScheduler scheduler(ExecutorService executor, int maxInFlight, BudgetScope scope) {
        return new PrefetchScheduler(executor::execute, maxInFlight, scope, 1024, QueuePolicy.FIFO, PrefetchOwner::ofCurrentThread);
    }

    private static BlockCache cache(int readSize, PrefetchScheduler scheduler) {
        return new BlockCache(1L << 26, BLOCK, readSize, readSize, NativeReadHints.DISABLED, scheduler);
    }

    private BlockCache.FileStats stats(BlockCache cache) {
        return cache.statsFor(file.getFileName().toString());
    }

    private void prefetch(BlockCache cache, int block, int blocks) {
        cache.prefetch(file, 1, channel, length, (long) block * BLOCK, blocks, stats(cache));
    }

    private void demand(BlockCache cache, int block, int readSize) throws IOException {
        cache.getOrLoad(new BlockKey(file, 1, (long) block * BLOCK), channel, length, readSize, stats(cache));
    }

    private static void assertNoSlotDefects(BlockCache cache) {
        assertEquals(0, cache.prefetchReadsOutsideSlot());
        assertEquals(0, cache.nestedPrefetchReads());
        final int maxActive = cache.scheduler().stats().maxActiveWorkers();
        assertTrue(
            "max reads " + cache.maxPrefetchReadsInFlight() + " max workers " + maxActive,
            cache.maxPrefetchReadsInFlight() <= maxActive
        );
    }

    public void testPrefetchReadsInFlightNeverExceedTheBound() throws Exception {
        open(1000 * BLOCK);
        for (int bound : new int[] { 8, 10, 16, 32, 64 }) {
            final BlockCache cache = cache(BLOCK, scheduler(pool(bound), bound, BudgetScope.PREFETCH));
            cache.setSimulatedLoadLatencyNanos(TimeUnit.MILLISECONDS.toNanos(20));
            // 1,000 single-window prefetches on distinct windows: 1,000 items, 984 or fewer queued at once
            for (int b = 0; b < 1000; b++) {
                prefetch(cache, b, 1);
            }
            assertBusy(() -> assertEquals(0, cache.pendingPrefetchTasks()), 60, TimeUnit.SECONDS);
            assertEquals("bound " + bound, bound, cache.maxPrefetchReadsInFlight());
            assertEquals(1000, stats(cache).prefetchReads.sum());
            assertEquals(0, cache.rejectedPrefetchTasks());
            assertNoSlotDefects(cache);
            assertEquals(bound, cache.scheduler().stats().maxActiveWorkers());
        }
    }

    public void testReadCountChecksUnderRandomLoad() throws Exception {
        open(512 * BLOCK);
        final int budget = randomIntBetween(2, 8);
        final BudgetScope scope = randomFrom(BudgetScope.values());
        final BlockCache cache = cache(4 * BLOCK, scheduler(pool(budget), budget, scope));
        if (randomBoolean()) {
            cache.setSimulatedLoadLatencyNanos(TimeUnit.MICROSECONDS.toNanos(200));
        }
        final int threads = 6;
        final CyclicBarrier barrier = new CyclicBarrier(threads);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final List<Thread> workers = new ArrayList<>();
        final long seed = random().nextLong();
        for (int t = 0; t < threads; t++) {
            final java.util.Random r = new java.util.Random(seed + t);
            final Thread worker = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < 200; i++) {
                        final int block = r.nextInt(512);
                        if (r.nextBoolean()) {
                            demand(cache, block, 4 * BLOCK);
                        } else {
                            prefetch(cache, block, Math.min(512 - block, 1 + r.nextInt(8)));
                        }
                        if (r.nextInt(50) == 0) {
                            cache.invalidateFile(file, 1, length);
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
        assertBusy(() -> assertEquals(0, cache.pendingPrefetchTasks()));
        assertNoSlotDefects(cache);
        final PrefetchScheduler.Stats stats = cache.scheduler().stats();
        if (scope == BudgetScope.TOTAL) {
            assertTrue(stats.maxTotalReadsAtItemStart() <= budget);
        }
        assertEquals(0, stats.demandReadsInFlight());
        assertEquals("every demand read is counted by the gate", stats(cache).reads.sum(), stats.demandReadsStarted());
        assertTrue(cache.maxTotalReadsInFlight() >= cache.maxPrefetchReadsInFlight());
    }

    public void testReadCountChecksDetectDefects() throws Exception {
        open(64 * BLOCK);
        final PrefetchScheduler scheduler = scheduler(pool(2), 2, BudgetScope.PREFETCH);
        final BlockCache cache = cache(4 * BLOCK, scheduler);
        // a prefetch read on a thread without a worker slot
        cache.prefetchNowForTests(file, 1, channel, length, 0, 1, stats(cache));
        assertEquals(1, stats(cache).prefetchReads.sum());
        assertEquals(1, cache.prefetchReadsOutsideSlot());
        assertEquals(0, cache.nestedPrefetchReads());
        // a prefetch read while the same thread has one in flight, on a worker that holds a slot
        final CountDownLatch done = new CountDownLatch(1);
        scheduler.submit(c -> {
            final int[] onThread = cache.prefetchReadStarted();
            try {
                cache.prefetchNowForTests(file, 1, channel, length, 8L * BLOCK, 1, stats(cache));
            } finally {
                cache.prefetchReadFinished(onThread);
                done.countDown();
            }
        });
        assertTrue(done.await(30, TimeUnit.SECONDS));
        assertBusy(() -> assertEquals(0, cache.pendingPrefetchTasks()));
        assertEquals(1, cache.prefetchReadsOutsideSlot());
        assertEquals(1, cache.nestedPrefetchReads());
        cache.resetStats();
        assertEquals(0, cache.prefetchReadsOutsideSlot());
        assertEquals(0, cache.nestedPrefetchReads());

        // the gate counts every demand storage read: equal to the per-file reads after 100 misses
        final BlockCache demandCache = cache(BLOCK, scheduler(pool(1), 1, BudgetScope.TOTAL));
        for (int b = 0; b < 64; b++) {
            demand(demandCache, b, BLOCK);
        }
        demandCache.invalidateFile(file, 1, length);
        for (int b = 0; b < 36; b++) {
            demand(demandCache, b, BLOCK);
        }
        assertEquals(100, stats(demandCache).reads.sum());
        assertEquals(100, demandCache.scheduler().stats().demandReadsStarted());
        // a demand read whose storage read throws is counted by the gate, not by reads, and leaves no read in flight
        final StorageFile closed = StorageFile.open(file, NativeReadHints.DISABLED);
        closed.close();
        expectThrows(
            ClosedChannelException.class,
            () -> demandCache.getOrLoad(new BlockKey(file, 1, 60L * BLOCK), closed, length, BLOCK, stats(demandCache))
        );
        assertEquals(100, stats(demandCache).reads.sum());
        assertEquals(101, demandCache.scheduler().stats().demandReadsStarted());
        assertEquals(0, demandCache.scheduler().demandReadsInFlight());
    }

    public void testResetOrderKeepsMaxReadsAtMostMaxWorkers() throws Exception {
        open(64 * BLOCK);
        final PrefetchScheduler scheduler = scheduler(pool(4), 4, BudgetScope.PREFETCH);
        final BlockCache cache = cache(4 * BLOCK, scheduler);
        cache.setSimulatedLoadLatencyNanos(TimeUnit.MILLISECONDS.toNanos(300));
        for (int b = 0; b < 3; b++) {
            prefetch(cache, 4 * b, 1);
        }
        assertBusy(() -> assertEquals(3, cache.prefetchReadsInFlight()));
        // the workers exit between the two reset steps: the scheduler step saw 3 workers, the cache step sees fewer reads
        cache.setBetweenResetStepsHookForTests(() -> {
            try {
                assertBusy(() -> assertEquals(0, scheduler.activeWorkers()));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        cache.resetStats();
        cache.setBetweenResetStepsHookForTests(null);
        assertEquals(3, scheduler.stats().maxActiveWorkers());
        assertEquals(0, cache.maxPrefetchReadsInFlight());
        assertNoSlotDefects(cache);
        cache.setSimulatedLoadLatencyNanos(0);
        prefetch(cache, 32, 1);
        assertBusy(() -> assertEquals(0, cache.pendingPrefetchTasks()));
        assertNoSlotDefects(cache);
    }

    public void testDemandWaitIsRecordedForAJoinedLoadOnly() throws Exception {
        open(64 * BLOCK);
        final BlockCache cache = cache(4 * BLOCK, scheduler(pool(1), 1, BudgetScope.PREFETCH));
        cache.setSimulatedLoadLatencyNanos(TimeUnit.MILLISECONDS.toNanos(100));
        prefetch(cache, 0, 4);
        assertBusy(() -> assertEquals(1, cache.prefetchReadsInFlight()));
        final long start = System.nanoTime();
        demand(cache, 1, 4 * BLOCK);
        final long waitedNanos = System.nanoTime() - start;
        final LatencyHistogram.Snapshot wait = cache.demandWait();
        assertEquals(1, wait.count());
        assertEquals(1, stats(cache).waits.sum());
        assertTrue("wait " + wait.median() + " of " + waitedNanos, wait.median() > 0 && wait.median() <= waitedNanos * 1.01 + 1);
        assertEquals(0, stats(cache).reads.sum());
        // a hit records nothing
        demand(cache, 2, 4 * BLOCK);
        assertEquals(1, cache.demandWait().count());
        assertBusy(() -> assertEquals(0, cache.pendingPrefetchTasks()));
    }

    public void testDemandReadsDoNotWaitForPrefetchSlots() throws Exception {
        open(64 * BLOCK);
        final long latencyNanos = TimeUnit.MILLISECONDS.toNanos(300);
        final BlockCache cache = cache(4 * BLOCK, scheduler(pool(1), 1, BudgetScope.TOTAL));
        cache.setSimulatedLoadLatencyNanos(latencyNanos);
        // the only slot is busy until released (a long prefetch); the prefetch of the window of blocks 16 to 19 is queued
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch busy = new CountDownLatch(1);
        cache.scheduler().submit(c -> {
            busy.countDown();
            try {
                assertTrue(release.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        });
        assertTrue(busy.await(30, TimeUnit.SECONDS));
        prefetch(cache, 16, 1);
        assertEquals(1, cache.scheduler().queued());
        // a demand miss of another window reads it at once, in one read latency
        long start = System.nanoTime();
        demand(cache, 8, 4 * BLOCK);
        assertTrue(System.nanoTime() - start < 2 * latencyNanos);
        // a demand miss of the window whose prefetch is still queued reads it itself and does not wait
        assertEquals(1, cache.scheduler().queued());
        start = System.nanoTime();
        demand(cache, 17, 4 * BLOCK);
        assertTrue(System.nanoTime() - start < 2 * latencyNanos);
        assertEquals(0, stats(cache).waits.sum());
        assertEquals(2, stats(cache).reads.sum());
        assertEquals(1, cache.scheduler().queued());
        release.countDown();
        assertBusy(() -> assertEquals(0, cache.pendingPrefetchTasks()));
        // the late prefetch found its window cached and read nothing
        assertEquals(0, stats(cache).prefetchReads.sum());
        assertEquals(2, cache.scheduler().stats().demandReadsStarted());
        assertEquals(1, cache.maxTotalReadsInFlight());
    }

    public void testReadLatencyHistogramsAndCounters() throws Exception {
        // 3 windows of 128 KiB and a last window of 100 KiB, 1 KiB blocks
        open(3 * 131072 + 102400);
        final BlockCache cache = cache(131072, scheduler(pool(4), 4, BudgetScope.PREFETCH));
        cache.setSimulatedLoadLatencyNanos(TimeUnit.MILLISECONDS.toNanos(50));
        // 4 overlapping prefetch reads of 50 ms
        for (int w = 0; w < 4; w++) {
            prefetch(cache, w * 128, 1);
        }
        assertBusy(() -> assertEquals(0, cache.pendingPrefetchTasks()));
        assertEquals(4, cache.readLatency(true, 1).count());
        assertEquals(0, cache.readLatency(true, 0).count());
        assertEquals(0, cache.readLatency(true, 2).count());
        final long median = cache.readLatency(true, 1).median();
        assertTrue("median " + median, median >= 49_500_000L && median < 75_000_000L);
        final long readTime = cache.prefetchReadTimeMicros();
        final long busy = cache.prefetchBusyTimeMicros();
        assertTrue("busy " + busy + " read time " + readTime, busy >= 50_000 && busy <= readTime);
        assertEquals(3 * 131072 + 102400, cache.prefetchBytesRead());
        assertEquals(0, cache.demandBytesRead());
        // a demand read is counted apart: bytes, time and its own histogram
        cache.clear();
        cache.setSimulatedLoadLatencyNanos(0);
        demand(cache, 3 * 128 + 5, 131072);
        assertEquals(102400, cache.demandBytesRead());
        assertEquals(1, cache.readLatency(false, 1).count());
        assertEquals(3 * 131072 + 102400, cache.prefetchBytesRead());
        assertTrue(cache.demandReadTimeMicros() >= 0);
        cache.resetStats();
        assertEquals(0, cache.readLatency(true, 1).count());
        assertEquals(0, cache.prefetchBytesRead());
        assertEquals(0, cache.prefetchBusyTimeMicros());
    }

    public void testLatencyClasses() {
        assertEquals(0, BlockCache.latencyClass(32768));
        assertEquals(0, BlockCache.latencyClass(16385));
        assertEquals(2, BlockCache.latencyClass(16384));
        assertEquals(1, BlockCache.latencyClass(131072));
        assertEquals(1, BlockCache.latencyClass(102400));
        assertEquals(2, BlockCache.latencyClass(65536));
        assertEquals(2, BlockCache.latencyClass(8192));
        assertEquals(List.of("32768", "131072", "other"), BlockCache.LATENCY_CLASSES);
    }

    public void testCancelledMultiWindowItemStopsAtTheNextWindow() throws Exception {
        open(64 * BLOCK);
        final AtomicBoolean cancelled = new AtomicBoolean();
        final PrefetchScheduler scheduler = new PrefetchScheduler(
            pool(1)::execute,
            1,
            BudgetScope.PREFETCH,
            16,
            QueuePolicy.FIFO,
            () -> new PrefetchOwner(5L, 5, cancelled::get, PrefetchOwner.NOT_TRACKED)
        );
        final BlockCache cache = cache(4 * BLOCK, scheduler);
        cache.setSimulatedLoadLatencyNanos(TimeUnit.MILLISECONDS.toNanos(200));
        // one item of 4 windows
        prefetch(cache, 0, 16);
        assertBusy(() -> assertEquals(1, cache.prefetchReadsInFlight()));
        cancelled.set(true);
        assertBusy(() -> assertEquals(0, cache.pendingPrefetchTasks()));
        assertEquals(1, stats(cache).prefetchReads.sum());
        assertEquals(3, cache.windowsSkippedCancelled());
        // an item of a cancelled search that reaches a worker is not run
        prefetch(cache, 32, 4);
        assertBusy(() -> assertEquals(0, cache.pendingPrefetchTasks()));
        assertEquals(1, stats(cache).prefetchReads.sum());
        assertEquals(1, (long) scheduler.stats().dropped().get(PrefetchScheduler.DropReason.CANCELLED));
    }

    public void testHistogramResolutionAndBucketDeltas() {
        final LatencyHistogram h = new LatencyHistogram();
        for (int i = 0; i < 100; i++) {
            h.recordMicros(20_000);
        }
        final LatencyHistogram.Snapshot first = h.snapshot();
        assertEquals(100, first.count());
        assertEquals(20_000_000L, first.median(), 200_000);
        final LatencyHistogram other = new LatencyHistogram();
        other.recordMicros(22_000);
        assertTrue(Math.abs(other.snapshot().median() - first.median()) > 0.09 * 20_000_000L);
        // the bucket difference of two readings is the histogram of the values recorded in between
        final List<Long> second = new ArrayList<>();
        for (int i = 0; i < 51; i++) {
            final long v = 1_000 + random().nextInt(100_000);
            second.add(v);
            h.recordMicros(v);
        }
        final LatencyHistogram alone = new LatencyHistogram();
        second.forEach(alone::recordMicros);
        final List<long[]> after = h.snapshot().buckets();
        final java.util.Map<Long, Long> delta = new java.util.TreeMap<>();
        for (long[] b : after) {
            delta.merge(b[0], b[1], Long::sum);
        }
        for (long[] b : first.buckets()) {
            delta.merge(b[0], -b[1], Long::sum);
        }
        long seen = 0;
        long median = -1;
        for (java.util.Map.Entry<Long, Long> b : delta.entrySet()) {
            seen += b.getValue();
            if (median < 0 && seen >= 26) {
                median = b.getKey();
            }
        }
        assertEquals(51, seen);
        assertEquals(alone.snapshot().median(), median);
        // out of range values are clamped, an empty histogram reports zeros
        h.recordMicros(-5);
        h.recordNanos(LatencyHistogram.MAX_NANOS * 10);
        assertTrue(h.snapshot().max() >= LatencyHistogram.MAX_NANOS);
        // code review iteration 1, finding 5: values are kept in nanoseconds, so a 10 % shift resolves below 100
        // microseconds too (10 to 11 microseconds), and a sub-microsecond wait is not truncated to 0
        final LatencyHistogram ten = new LatencyHistogram();
        final LatencyHistogram eleven = new LatencyHistogram();
        for (int i = 0; i < 100; i++) {
            ten.recordNanos(10_000 + random().nextInt(20));
            eleven.recordNanos(11_000 + random().nextInt(20));
        }
        final long tenMedian = ten.snapshot().median();
        final long elevenMedian = eleven.snapshot().median();
        assertEquals(10_000, tenMedian, 0.01 * 10_000);
        assertEquals(11_000, elevenMedian, 0.01 * 11_000);
        assertTrue(tenMedian + " " + elevenMedian, elevenMedian - tenMedian > 0.09 * 10_000);
        final LatencyHistogram tiny = new LatencyHistogram();
        tiny.recordNanos(400);
        assertEquals(400, tiny.snapshot().median(), 4);
        assertEquals(0.4, RestBufferPoolStatsAction.micros(tiny.snapshot().median()), 0.004);
        final LatencyHistogram.Snapshot empty = new LatencyHistogram().snapshot();
        assertEquals(0, empty.count());
        assertEquals(0, empty.median());
        assertEquals(0, empty.max());
        h.reset();
        assertEquals(0, h.snapshot().count());
    }
}
