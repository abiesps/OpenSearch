/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Statistics and cache control endpoints for the block cache of the node that receives the request (not cluster-wide).
 *
 * <p>A prefetch "node" below is {@link BlockCache#prefetchNodeBytes()} bytes: the configured
 * {@code bufferpool.io.sequential_read_size}, so one node is one storage read whatever the cache block size is. Parameters
 * named {@code blocks} count such nodes (they equal cache blocks only when the read size is the block size).
 *
 * <ul>
 *   <li>{@code GET /_bufferpool/stats}: cache size, read sizes and per-file-type IO counters. Per file type,
 *       {@code loads}, {@code prefetch_loads} and {@code readahead_loads} count inserted blocks (demand misses, requested
 *       prefetch blocks, and blocks inserted only because they share a read window with one of those), {@code bytes_loaded}
 *       their bytes; {@code reads}, {@code prefetch_reads}, {@code bytes_read} and {@code reads_by_size} (count per size
 *       class, keyed by the class's upper bound in bytes) count the storage reads, one per read window. {@code window_blocks}
 *       counts the blocks those windows span, {@code window_blocks_cached} and {@code window_blocks_in_flight} the ones not
 *       inserted because they were cached or another read was loading them ({@code window_blocks == blocks_inserted +
 *       window_blocks_cached + window_blocks_in_flight}), and {@code bytes_overread} their bytes ({@code bytes_read ==
 *       bytes_loaded + bytes_overread}). {@code waits} and {@code wait_time_micros} count block reads that waited for
 *       another thread's read. Node-wide: {@code read_hint} (effective mode), {@code read_hints} and
 *       {@code read_hint_errors}; {@code in_flight_reads} counts running reads only, {@code pending_prefetch_tasks} the
 *       prefetch tasks queued or running, and the cache is quiet when both are 0;
 *       {@code rejected_prefetch_tasks} counts prefetch tasks dropped for any reason, {@code multi_window_prefetches}
 *       the prefetch requests that span more than one window, and {@code max_prefetch_reads_in_flight} the most prefetch
 *       reads that ran at once.
 *       <p>{@code prefetch_scheduler}: the node read budget and its use. Configuration: {@code max_in_flight},
 *       {@code budget_scope} ({@code prefetch} or {@code total}), {@code queue_size}, {@code queue_policy}. Reads in flight:
 *       {@code reads_in_flight} and {@code max_reads_in_flight} (prefetch), {@code demand_reads_in_flight} and
 *       {@code max_demand_reads_in_flight}, {@code max_total_reads_in_flight} (prefetch plus demand, sampled when a read
 *       starts), {@code max_total_reads_at_item_start} (prefetch workers plus demand reads when an item started, from the
 *       values the gate checked: with scope {@code total} above {@code max_in_flight} only by a defect). Checks that must
 *       stay 0: {@code prefetch_reads_outside_slot}, {@code nested_prefetch_reads}; {@code max_reads_in_flight} never
 *       exceeds {@code max_active_workers}, and over an operation the delta of {@code demand_reads_started} is at least the
 *       summed delta of the per-file {@code reads}. Queue: {@code active_workers}, {@code max_active_workers},
 *       {@code queued}, {@code max_queued}, {@code pending} (queued plus running items), {@code requesters} and
 *       {@code max_requesters} (requesters with queued items), {@code registered_tasks} (shard tasks with a search phase
 *       running), {@code items_admitted}, {@code items_started} (items that a worker ran: an item that reaches a worker
 *       after its search was cancelled is counted in {@code dropped.cancelled} instead), {@code items_finished},
 *       {@code dropped} per reason
 *       ({@code queue_full}, {@code longest_queue}, {@code cancelled}, {@code rejected_by_executor}, {@code shutdown}),
 *       {@code budget_held_dispatches} (items queued although a worker was free, because demand reads filled the budget),
 *       {@code items_started_after_phase_end}, {@code windows_skipped_cancelled}, {@code queue_wait_time_micros}. Time:
 *       {@code bytes_read}, {@code read_time_micros} and {@code busy_time_micros} (time with at least one prefetch read in
 *       flight) of prefetch reads, {@code demand_bytes_read} and {@code demand_read_time_micros} of demand reads; read
 *       time divided by an interval is the mean number of reads in flight over it.
 *       <p>Histograms, each {@code count}, {@code median}, {@code percentile_90}, {@code percentile_99} and {@code max}
 *       in microseconds with a fraction (recorded in nanoseconds, 2 significant digits, values within 1 % at every
 *       value), and with {@code ?histogram_buckets=true} also
 *       {@code buckets}, a list of {@code [upper_value_micros, count]} for the non-empty buckets (the difference of two
 *       readings is the histogram of the values in between): {@code read_latency_micros.prefetch} and {@code .demand}
 *       per read size class ({@code 32768}: more than 16 KiB up to 32 KiB; {@code 131072}: more than 64 KiB up to
 *       128 KiB; {@code other}); {@code prefetch_queue_wait_micros.short_requester} and {@code .long_requester} (queue
 *       wait of items handed to a worker whose requester had at most, or more than, {@code max_in_flight} items queued at
 *       admission); {@code demand_wait_micros} (waits of demand readers for a load that another thread ran)</li>
 *   <li>{@code POST /_bufferpool/stats/_reset}: sets the counters to zero and the maxima to the current values</li>
 *   <li>{@code POST /_bufferpool/cache/_clear}: drops all cached blocks, so the next reads are cold</li>
 *   <li>the experiment endpoints of the proof-of-concept build, see {@link ExperimentHooks} (the build for stock OpenSearch
 *       has none)</li>
 * </ul>
 */
final class RestBufferPoolStatsAction extends BaseRestHandler {

    private final Supplier<BlockCache> blockCache;
    private final IntSupplier registeredTasks;

    RestBufferPoolStatsAction(Supplier<BlockCache> blockCache, IntSupplier registeredTasks) {
        this.blockCache = blockCache;
        this.registeredTasks = registeredTasks;
    }

    @Override
    public String getName() {
        return "bufferpool_stats_action";
    }

    @Override
    public List<Route> routes() {
        final List<Route> routes = new ArrayList<>();
        routes.add(new Route(GET, "/_bufferpool/stats"));
        routes.add(new Route(POST, "/_bufferpool/stats/_reset"));
        routes.add(new Route(POST, "/_bufferpool/cache/_clear"));
        routes.addAll(ExperimentHooks.routes());
        return List.copyOf(routes);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final BlockCache cache = blockCache.get();
        final String path = request.path();
        final CheckedConsumer<RestChannel, Exception> experiment = ExperimentHooks.prepareRequest(request, cache);
        if (experiment != null) {
            return experiment::accept;
        }
        final boolean histogramBuckets = request.paramAsBoolean("histogram_buckets", false);
        if (path.endsWith("/_reset")) {
            cache.resetStats();
            ExperimentHooks.resetCounters();
        } else if (path.endsWith("/_clear")) {
            cache.clear();
        }
        return channel -> {
            final XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("build_target", ExperimentHooks.target());
            builder.field("block_size", cache.blockSize());
            builder.field("random_read_size", cache.randomReadSize());
            builder.field("sequential_read_size", cache.sequentialReadSize());
            builder.field("prefetch_node_bytes", cache.prefetchNodeBytes());
            builder.field("read_hint", cache.readHints().mode().toString());
            builder.field("read_hints", cache.readHints().hints());
            builder.field("read_hint_errors", cache.readHints().errors());
            builder.field("prefetch_task_per_window", cache.prefetchTaskPerWindow());
            builder.field("in_flight_reads", cache.inFlightReads());
            builder.field("pending_prefetch_tasks", cache.pendingPrefetchTasks());
            builder.field("rejected_prefetch_tasks", cache.rejectedPrefetchTasks());
            builder.field("multi_window_prefetches", cache.multiWindowPrefetches());
            builder.field("max_prefetch_reads_in_flight", cache.maxPrefetchReadsInFlight());
            writeScheduler(builder, cache, registeredTasks.getAsInt(), histogramBuckets);
            builder.field("cached_blocks", cache.size());
            builder.field("cached_bytes", cache.sizeInBytes());
            ExperimentHooks.writeStats(builder);
            builder.field("simulated_load_latency_micros", TimeUnit.NANOSECONDS.toMicros(cache.simulatedLoadLatencyNanos()));
            builder.startObject("files");
            for (Map.Entry<String, BlockCache.FileStats> entry : cache.stats().entrySet()) {
                final BlockCache.FileStats s = entry.getValue();
                builder.startObject(entry.getKey());
                builder.field("requests", s.requests.sum());
                builder.field("loads", s.loads.sum());
                builder.field("prefetch_requests", s.prefetchRequests.sum());
                builder.field("prefetch_loads", s.prefetchLoads.sum());
                builder.field("readahead_loads", s.readaheadLoads.sum());
                builder.field("blocks_inserted", s.blocksInserted());
                builder.field("bytes_loaded", s.bytesLoaded.sum());
                builder.field("reads", s.reads.sum());
                builder.field("prefetch_reads", s.prefetchReads.sum());
                builder.field("bytes_read", s.bytesRead.sum());
                builder.field("bytes_overread", s.bytesOverread.sum());
                builder.field("window_blocks", s.windowBlocks.sum());
                builder.field("window_blocks_cached", s.windowBlocksCached.sum());
                builder.field("window_blocks_in_flight", s.windowBlocksInFlight.sum());
                builder.field("waits", s.waits.sum());
                builder.field("wait_time_micros", TimeUnit.NANOSECONDS.toMicros(s.waitNanos.sum()));
                builder.startObject("reads_by_size");
                for (Map.Entry<Long, Long> size : s.readsBySize().entrySet()) {
                    builder.field(Long.toString(size.getKey()), size.getValue());
                }
                builder.endObject();
                builder.field("load_time_micros", TimeUnit.NANOSECONDS.toMicros(s.loadNanos.sum()));
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }

    private static void writeScheduler(XContentBuilder builder, BlockCache cache, int registeredTasks, boolean buckets) throws IOException {
        final PrefetchScheduler.Stats s = cache.scheduler().stats();
        builder.startObject("prefetch_scheduler");
        builder.field("max_in_flight", s.maxInFlight());
        builder.field("budget_scope", s.scope().value());
        builder.field("queue_size", s.queueSize());
        builder.field("queue_policy", s.policy().value());
        builder.field("reads_in_flight", cache.prefetchReadsInFlight());
        builder.field("max_reads_in_flight", cache.maxPrefetchReadsInFlight());
        builder.field("demand_reads_in_flight", s.demandReadsInFlight());
        builder.field("max_demand_reads_in_flight", s.maxDemandReadsInFlight());
        builder.field("max_total_reads_in_flight", cache.maxTotalReadsInFlight());
        builder.field("max_total_reads_at_item_start", s.maxTotalReadsAtItemStart());
        builder.field("budget_held_dispatches", s.budgetHeldDispatches());
        builder.field("items_started_after_phase_end", s.itemsStartedAfterPhaseEnd());
        builder.field("prefetch_reads_outside_slot", cache.prefetchReadsOutsideSlot());
        builder.field("nested_prefetch_reads", cache.nestedPrefetchReads());
        builder.field("demand_reads_started", s.demandReadsStarted());
        builder.field("active_workers", s.activeWorkers());
        builder.field("max_active_workers", s.maxActiveWorkers());
        builder.field("queued", s.queued());
        builder.field("max_queued", s.maxQueued());
        builder.field("pending", s.pending());
        builder.field("requesters", s.requesters());
        builder.field("max_requesters", s.maxRequesters());
        builder.field("registered_tasks", registeredTasks);
        builder.field("items_admitted", s.itemsAdmitted());
        builder.field("items_started", s.itemsStarted());
        builder.field("items_finished", s.itemsFinished());
        builder.startObject("dropped");
        for (Map.Entry<PrefetchScheduler.DropReason, Long> drop : s.dropped().entrySet()) {
            builder.field(drop.getKey().fieldName(), drop.getValue());
        }
        builder.endObject();
        builder.field("windows_skipped_cancelled", cache.windowsSkippedCancelled());
        builder.field("bytes_read", cache.prefetchBytesRead());
        builder.field("busy_time_micros", cache.prefetchBusyTimeMicros());
        builder.field("read_time_micros", cache.prefetchReadTimeMicros());
        builder.field("queue_wait_time_micros", s.queueWaitTimeMicros());
        builder.field("demand_bytes_read", cache.demandBytesRead());
        builder.field("demand_read_time_micros", cache.demandReadTimeMicros());
        builder.endObject();
        builder.startObject("read_latency_micros");
        for (boolean prefetch : new boolean[] { true, false }) {
            builder.startObject(prefetch ? "prefetch" : "demand");
            for (int c = 0; c < BlockCache.LATENCY_CLASSES.size(); c++) {
                writeHistogram(builder, BlockCache.LATENCY_CLASSES.get(c), cache.readLatency(prefetch, c), buckets);
            }
            builder.endObject();
        }
        builder.endObject();
        builder.startObject("prefetch_queue_wait_micros");
        writeHistogram(builder, "short_requester", s.shortRequesterWait(), buckets);
        writeHistogram(builder, "long_requester", s.longRequesterWait(), buckets);
        builder.endObject();
        writeHistogram(builder, "demand_wait_micros", cache.demandWait(), buckets);
    }

    private static void writeHistogram(XContentBuilder builder, String name, LatencyHistogram.Snapshot h, boolean buckets)
        throws IOException {
        builder.startObject(name);
        builder.field("count", h.count());
        builder.field("median", micros(h.median()));
        builder.field("percentile_90", micros(h.percentile90()));
        builder.field("percentile_99", micros(h.percentile99()));
        builder.field("max", micros(h.max()));
        if (buckets) {
            builder.startArray("buckets");
            for (long[] bucket : h.buckets()) {
                builder.startArray().value(micros(bucket[0])).value(bucket[1]).endArray();
            }
            builder.endArray();
        }
        builder.endObject();
    }

    /** A histogram value in microseconds with its fraction: the histogram records nanoseconds. */
    static double micros(long nanos) {
        return nanos / 1000.0;
    }
}
