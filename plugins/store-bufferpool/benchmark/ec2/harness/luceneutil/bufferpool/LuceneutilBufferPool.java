/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The store-bufferpool plugin's directory for luceneutil (coldpath harness, g-luceneutil branch): the plugin jar is used
 * unchanged (the same jar as the OpenSearch configurations), and this class, compiled into its own jar in the plugin's
 * package, builds the block cache and the prefetch scheduler from {@code -Dcoldpath.bp.*} properties the way
 * {@code BufferPoolStorePlugin.createComponents} builds them from node settings, then opens
 * {@link BufferPoolDirectory} instances on it. One cache per JVM, shared by every directory (index and taxonomy), as
 * one node shares one cache. Prefetch workers run on a fixed pool of {@code max_in_flight} daemon threads (the plugin's
 * prefetch thread pool has the same size). Called by luceneutil's {@code OpenDirectory} by reflection, so luceneutil
 * compiles without the plugin.
 * <p>
 * Properties (defaults are the plugin's setting defaults; the arms config passes every value explicitly):
 * coldpath.bp.cache_bytes (required), coldpath.bp.block_size (8192), coldpath.bp.random_read_size (32768),
 * coldpath.bp.sequential_read_size (131072), coldpath.bp.read_hint (auto), coldpath.bp.max_in_flight
 * (min(8, processors)), coldpath.bp.budget_scope (prefetch), coldpath.bp.queue_size (1024),
 * coldpath.bp.queue_policy (fifo), coldpath.bp.task_per_window (false).
 */
public final class LuceneutilBufferPool {
    private static BlockCache cache;
    private static PrefetchScheduler scheduler;
    private static ExecutorService workers;

    private LuceneutilBufferPool() {}

    private static String prop(String name, String def) {
        final String v = System.getProperty("coldpath.bp." + name);
        if (v == null || v.isBlank()) {
            if (def == null) {
                throw new IllegalArgumentException("-Dcoldpath.bp." + name + " is required");
            }
            return def;
        }
        return v.trim();
    }

    private static synchronized BlockCache cache() {
        if (cache != null) {
            return cache;
        }
        final long maxBytes = Long.parseLong(prop("cache_bytes", null));
        final int blockSize = Integer.parseInt(prop("block_size", "8192"));
        final int randomReadSize = Integer.parseInt(prop("random_read_size", "32768"));
        final int sequentialReadSize = Integer.parseInt(prop("sequential_read_size", "131072"));
        final int maxInFlight = Integer.parseInt(prop("max_in_flight", Integer.toString(Math.min(8, Runtime.getRuntime().availableProcessors()))));
        final AtomicInteger n = new AtomicInteger();
        workers = Executors.newFixedThreadPool(maxInFlight, r -> {
            final Thread t = new Thread(r, "coldpath-bufferpool-prefetch-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        final ExecutorService pool = workers;
        scheduler = new PrefetchScheduler(
            pool::execute,
            maxInFlight,
            PrefetchScheduler.BudgetScope.parse("coldpath.bp.budget_scope", prop("budget_scope", "prefetch")),
            Integer.parseInt(prop("queue_size", Integer.toString(BlockCache.DEFAULT_QUEUE_SIZE))),
            PrefetchScheduler.QueuePolicy.parse("coldpath.bp.queue_policy", prop("queue_policy", "fifo")),
            PrefetchScheduler.PrefetchOwner::ofCurrentThread
        );
        final NativeReadHints hints = NativeReadHints.create(NativeReadHints.Mode.parse(prop("read_hint", "auto")));
        cache = new BlockCache(maxBytes, blockSize, randomReadSize, sequentialReadSize, hints, scheduler);
        cache.setPrefetchTaskPerWindow(Boolean.parseBoolean(prop("task_per_window", "false")));
        System.out.println("COLDPATH bufferpool " + configJson());
        return cache;
    }

    /** A bufferpool directory on {@code path}, on the JVM's one block cache. */
    public static Directory open(Path path) throws IOException {
        return new BufferPoolDirectory(path, FSLockFactory.getDefault(), cache());
    }

    /** Drops every cached block (the cold clear, like {@code POST /_bufferpool/cache/_clear}). */
    public static void clear() {
        cache().clear();
    }

    /** Cached blocks now (0 after a clear with no read in flight). */
    public static long cachedBlocks() {
        return cache().size();
    }

    /** Waits until no prefetch item is queued or running (at most timeoutMillis); returns the millis waited. */
    public static long waitPrefetchIdle(long timeoutMillis) throws InterruptedException {
        final long t0 = System.nanoTime();
        final BlockCache c = cache();
        while ((c.pendingPrefetchTasks() > 0 || c.prefetchReadsInFlight() > 0)
            && System.nanoTime() - t0 < TimeUnit.MILLISECONDS.toNanos(timeoutMillis)) {
            Thread.sleep(1);
        }
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
    }

    /** The cache configuration, as the plugin's stats report it. */
    public static String configJson() {
        final BlockCache c = cache;
        return String.format(
            Locale.ROOT,
            "{\"block_size\":%d,\"random_read_size\":%d,\"sequential_read_size\":%d,\"prefetch_node_bytes\":%d,\"read_hint\":\"%s\","
                + "\"prefetch_task_per_window\":%b,\"max_in_flight\":%d,\"budget_scope\":\"%s\",\"queue_size\":%d,\"queue_policy\":\"%s\","
                + "\"cache_bytes\":%s}",
            c.blockSize(),
            c.randomReadSize(),
            c.sequentialReadSize(),
            c.prefetchNodeBytes(),
            c.readHints().mode(),
            c.prefetchTaskPerWindow(),
            scheduler.maxInFlight(),
            scheduler.scope().value(),
            scheduler.queueSize(),
            scheduler.policy().value(),
            prop("cache_bytes", null)
        );
    }

    /**
     * The cache counters in the shape of {@code GET /_bufferpool/stats} (the fields the harness reads: the configuration,
     * cached blocks and bytes, prefetch pool state, read hints, and per file the same counters under the same names).
     */
    public static String statsJson() {
        final BlockCache c = cache();
        final StringBuilder b = new StringBuilder(4096);
        b.append("{\"block_size\":").append(c.blockSize());
        b.append(",\"random_read_size\":").append(c.randomReadSize());
        b.append(",\"sequential_read_size\":").append(c.sequentialReadSize());
        b.append(",\"read_hint\":\"").append(c.readHints().mode()).append('"');
        b.append(",\"read_hints\":").append(c.readHints().hints());
        b.append(",\"read_hint_errors\":").append(c.readHints().errors());
        b.append(",\"in_flight_reads\":").append(c.inFlightReads());
        b.append(",\"pending_prefetch_tasks\":").append(c.pendingPrefetchTasks());
        b.append(",\"rejected_prefetch_tasks\":").append(c.rejectedPrefetchTasks());
        b.append(",\"max_prefetch_reads_in_flight\":").append(c.maxPrefetchReadsInFlight());
        b.append(",\"cached_blocks\":").append(c.size());
        b.append(",\"cached_bytes\":").append(c.sizeInBytes());
        b.append(",\"files\":{");
        boolean first = true;
        for (Map.Entry<String, BlockCache.FileStats> e : c.stats().entrySet()) {
            final BlockCache.FileStats s = e.getValue();
            if (first == false) {
                b.append(',');
            }
            first = false;
            b.append('"').append(e.getKey().replace("\\", "\\\\").replace("\"", "\\\"")).append("\":{");
            b.append("\"requests\":").append(s.requests.sum());
            b.append(",\"loads\":").append(s.loads.sum());
            b.append(",\"prefetch_requests\":").append(s.prefetchRequests.sum());
            b.append(",\"prefetch_loads\":").append(s.prefetchLoads.sum());
            b.append(",\"readahead_loads\":").append(s.readaheadLoads.sum());
            b.append(",\"blocks_inserted\":").append(s.blocksInserted());
            b.append(",\"bytes_loaded\":").append(s.bytesLoaded.sum());
            b.append(",\"reads\":").append(s.reads.sum());
            b.append(",\"prefetch_reads\":").append(s.prefetchReads.sum());
            b.append(",\"bytes_read\":").append(s.bytesRead.sum());
            b.append(",\"bytes_overread\":").append(s.bytesOverread.sum());
            b.append(",\"window_blocks\":").append(s.windowBlocks.sum());
            b.append(",\"window_blocks_cached\":").append(s.windowBlocksCached.sum());
            b.append(",\"window_blocks_in_flight\":").append(s.windowBlocksInFlight.sum());
            b.append(",\"waits\":").append(s.waits.sum());
            b.append(",\"wait_time_micros\":").append(TimeUnit.NANOSECONDS.toMicros(s.waitNanos.sum()));
            b.append(",\"reads_by_size\":{");
            boolean f2 = true;
            for (Map.Entry<Long, Long> size : s.readsBySize().entrySet()) {
                if (f2 == false) {
                    b.append(',');
                }
                f2 = false;
                b.append('"').append(size.getKey()).append("\":").append(size.getValue());
            }
            b.append("},\"load_time_micros\":").append(TimeUnit.NANOSECONDS.toMicros(s.loadNanos.sum()));
            b.append('}');
        }
        b.append("}}");
        return b.toString();
    }
}
