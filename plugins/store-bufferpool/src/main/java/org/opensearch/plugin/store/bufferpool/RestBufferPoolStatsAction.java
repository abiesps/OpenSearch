/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.lucene.codecs.lucene104.Lucene104DualNavPostingsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104DualNavPostingsFormat.ReadMode;
import org.apache.lucene.search.CollectExperiments;
import org.apache.lucene.search.DisjunctionPrefetch;
import org.apache.lucene.search.TopKPrefetch;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.aggregations.BatchCollection;
import org.opensearch.search.aggregations.DocValuesPrefetch;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Experiment endpoints for the block cache of the node that receives the request (not cluster-wide).
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
 *       {@code rejected_prefetch_tasks} counts prefetch tasks dropped on a full queue, {@code multi_window_prefetches}
 *       the prefetch requests that span more than one window, and {@code max_prefetch_reads_in_flight} the most prefetch
 *       reads that ran at once</li>
 *   <li>{@code POST /_bufferpool/stats/_reset}: sets the counters to zero</li>
 *   <li>{@code POST /_bufferpool/cache/_clear}: drops all cached blocks, so the next reads are cold</li>
 *   <li>{@code POST /_bufferpool/dual_nav/_mode?mode=doc|nav}: where {@code Lucene104DualNav} postings read skip data
 *       from, for postings lists opened from now on (JVM-wide)</li>
 *   <li>{@code POST /_bufferpool/disjunction_prefetch?blocks=N[&aligned=true]}: exhaustive OR queries keep N prefetch nodes
 *       of each clause's postings requested ahead (JVM-wide, see Lucene's {@code DisjunctionPrefetch}); 0 disables it. With
 *       {@code aligned=true} requests are whole nodes: when a clause starts reading node k, nodes up to k + N are
 *       requested</li>
 *   <li>{@code POST /_bufferpool/topk_prefetch?norms_blocks=N[&filter=true|false][&doc_blocks=D]}: top-k OR queries
 *       keep each clause's postings requested D prefetch nodes ahead, and request norms up to N
 *       nodes ahead (N * node size doc IDs, for 1-byte norms), in whole nodes, only for doc windows whose max
 *       score can beat the current threshold unless {@code filter=false} (JVM-wide, see Lucene's {@code TopKPrefetch});
 *       0 disables it</li>
 *   <li>{@code POST /_bufferpool/agg_batch?mode=off|runend|vec|vecdec|pf|pfs|pfl|pfsl|pfw|pfwg|pfwc[&docs=N]}: batch collection experiments for scorers and
 *       leaf collectors created from now on (JVM-wide). {@code runend}: Lucene's dense conjunction keeps each clause's
 *       doc ID run end instead of recomputing it per window (see Lucene's {@code CollectExperiments}); {@code vec}:
 *       runend plus OpenSearch batch aggregation collection ({@code BatchCollection}); {@code vecdec}: vec plus Lucene
 *       bulk doc-values reads that decode a span of packed values in one pass; {@code pf}: vecdec plus doc-values prefetch
 *       one node ahead, doc-ID aligned ({@code DocValuesPrefetch}); {@code pfs}, {@code pfl}, {@code pfsl}: pf with the
 *       look-ahead shared per segment (s), built as a leapfrog conjunction (l), or both; {@code pfw}: vecdec plus
 *       doc-values prefetch proven by the main scorer's own matches, buffered {@code docs} doc IDs (default 131072)
 *       ahead of collection (run-ahead, no second scorer); {@code pfwg}: pfw plus the gate (collection waits at a
 *       planner's next requested doc until the read after its node is known); {@code pfwc}: pfwg that passes docs
 *       straight through while every node the collectors read from is cached; {@code off} is stock</li>
 *   <li>{@code POST /_bufferpool/sort_opt?bkd_prefetch=&bkd_chunks=&whole_index=&whole_index_bytes=&index_child_prefetch=&
 *       approx_single=&approx_bool=&skipper_range=&sort_prefetch=&sort_docs=&clamp=&sample_docs=&skipper_mode=&run_cap=}:
 *       cold-path sort experiment switches (JVM-wide, see {@link SortOptParams}); an absent parameter leaves its switch
 *       unchanged, a bad value returns 400 and changes nothing. Every POST also sets the node size of the BKD and sort
 *       prefetches to the prefetch node size. {@code GET /_bufferpool/sort_opt} returns every value; {@code GET
 *       /_bufferpool/stats} reports them as {@code sort_opt_*}</li>
 * </ul>
 */
final class RestBufferPoolStatsAction extends BaseRestHandler {

    private final Supplier<BlockCache> blockCache;

    RestBufferPoolStatsAction(Supplier<BlockCache> blockCache) {
        this.blockCache = blockCache;
    }

    @Override
    public String getName() {
        return "bufferpool_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_bufferpool/stats"),
            new Route(POST, "/_bufferpool/stats/_reset"),
            new Route(POST, "/_bufferpool/cache/_clear"),
            new Route(POST, "/_bufferpool/dual_nav/_mode"),
            new Route(POST, "/_bufferpool/disjunction_prefetch"),
            new Route(POST, "/_bufferpool/topk_prefetch"),
            new Route(POST, "/_bufferpool/agg_batch"),
            new Route(POST, "/_bufferpool/sort_opt"),
            new Route(GET, "/_bufferpool/sort_opt")
        );
    }

    private static void setAggBatch(boolean cacheRunEnd, boolean batchCollection, boolean bulkDecode) {
        DocValuesPrefetch.setEnabled(false);
        DocValuesPrefetch.setShareLookahead(false);
        DocValuesPrefetch.setLeapfrogLookahead(false);
        DocValuesPrefetch.setRunAhead(false);
        DocValuesPrefetch.setRunAheadGate(false);
        DocValuesPrefetch.setRunAheadBypass(false);
        CollectExperiments.setCacheRunEnd(cacheRunEnd);
        BatchCollection.setEnabled(batchCollection);
        CollectExperiments.setBulkDecode(bulkDecode);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final BlockCache cache = blockCache.get();
        final String path = request.path();
        if (path.endsWith("/sort_opt")) {
            return prepareSortOpt(request, cache);
        }
        if (path.endsWith("/_reset")) {
            cache.resetStats();
            BatchCollection.resetCounters();
            DocValuesPrefetch.resetCounters();
        } else if (path.endsWith("/_clear")) {
            cache.clear();
        } else if (path.endsWith("/_mode")) {
            final String mode = request.param("mode");
            if (mode == null) {
                throw new IllegalArgumentException("missing [mode], expected doc or nav");
            }
            Lucene104DualNavPostingsFormat.setReadMode(ReadMode.valueOf(mode.toUpperCase(Locale.ROOT)));
        } else if (path.endsWith("/topk_prefetch")) {
            final int blocks = request.paramAsInt("norms_blocks", -1);
            final int docBlocks = request.paramAsInt("doc_blocks", 0);
            if (blocks < 0 || docBlocks < 0) {
                throw new IllegalArgumentException("missing or negative [norms_blocks], or negative [doc_blocks]");
            }
            if (docBlocks > 0) {
                // Lucene104DualNav plans postings prefetch in whole nodes only when the node size is set
                DisjunctionPrefetch.setNodeBytes(cache.prefetchNodeBytes());
            }
            TopKPrefetch.setDocNodesAhead(docBlocks);
            // norms are 1 byte per doc for BM25 text fields: N nodes of norms = N * node size doc IDs
            TopKPrefetch.setNodeBytes(cache.prefetchNodeBytes());
            TopKPrefetch.setFilter(request.paramAsBoolean("filter", true));
            TopKPrefetch.setNormsDocsAhead(Math.toIntExact((long) blocks * cache.prefetchNodeBytes()));
        } else if (path.endsWith("/agg_batch")) {
            final String mode = request.param("mode");
            switch (mode == null ? "" : mode) {
                case "off" -> setAggBatch(false, false, false);
                case "runend" -> setAggBatch(true, false, false);
                case "vec" -> setAggBatch(true, true, false);
                case "vecdec" -> setAggBatch(true, true, true);
                case "pf", "pfs", "pfl", "pfsl" -> {
                    setAggBatch(true, true, true);
                    DocValuesPrefetch.setNodeBytes(cache.prefetchNodeBytes());
                    DocValuesPrefetch.setShareLookahead(mode.contains("s"));
                    DocValuesPrefetch.setLeapfrogLookahead(mode.contains("l"));
                    DocValuesPrefetch.setEnabled(true);
                }
                case "pfw", "pfwg", "pfwc" -> {
                    setAggBatch(true, true, true);
                    DocValuesPrefetch.setNodeBytes(cache.prefetchNodeBytes());
                    DocValuesPrefetch.setRunAheadDocs(request.paramAsInt("docs", 1 << 17));
                    DocValuesPrefetch.setRunAheadGate(mode.equals("pfwg") || mode.equals("pfwc"));
                    DocValuesPrefetch.setRunAheadBypass(mode.equals("pfwc"));
                    DocValuesPrefetch.setRunAhead(true);
                    DocValuesPrefetch.setEnabled(true);
                }
                default -> throw new IllegalArgumentException(
                    "[mode] must be off, runend, vec, vecdec, pf, pfs, pfl, pfsl, pfw, pfwg or pfwc, got [" + mode + "]"
                );
            }
        } else if (path.endsWith("/disjunction_prefetch")) {
            final int blocks = request.paramAsInt("blocks", -1);
            if (blocks < 0) {
                throw new IllegalArgumentException("missing or negative [blocks]");
            }
            DisjunctionPrefetch.setNodeBytes(request.paramAsBoolean("aligned", false) ? cache.prefetchNodeBytes() : 0);
            DisjunctionPrefetch.setBytesAhead((long) blocks * cache.prefetchNodeBytes());
        }
        return channel -> {
            final XContentBuilder builder = channel.newBuilder();
            builder.startObject();
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
            builder.field("cached_blocks", cache.size());
            builder.field("cached_bytes", cache.sizeInBytes());
            builder.field("dual_nav_read_mode", Lucene104DualNavPostingsFormat.getReadMode().name().toLowerCase(Locale.ROOT));
            builder.field("disjunction_prefetch_bytes_ahead", DisjunctionPrefetch.getBytesAhead());
            builder.field("disjunction_prefetch_node_bytes", DisjunctionPrefetch.getNodeBytes());
            builder.field("topk_prefetch_norms_docs_ahead", TopKPrefetch.getNormsDocsAhead());
            builder.field("topk_prefetch_filter", TopKPrefetch.isFilter());
            builder.field("topk_prefetch_doc_nodes_ahead", TopKPrefetch.getDocNodesAhead());
            builder.field("agg_batch_cache_run_end", CollectExperiments.isCacheRunEnd());
            builder.field("agg_batch_collection", BatchCollection.isEnabled());
            builder.field("agg_batch_bulk_decode", CollectExperiments.isBulkDecode());
            builder.field("agg_batch_bulk_chunks", BatchCollection.bulkChunks());
            builder.field("agg_batch_stream_runs", BatchCollection.streamRuns());
            builder.field("agg_batch_per_doc_streams", BatchCollection.perDocStreams());
            builder.field("agg_prefetch", DocValuesPrefetch.isEnabled());
            builder.field("agg_prefetch_planners", DocValuesPrefetch.planners());
            builder.field("agg_prefetch_requests", DocValuesPrefetch.requests());
            builder.field("agg_prefetch_share_lookahead", DocValuesPrefetch.isShareLookahead());
            builder.field("agg_prefetch_leapfrog_lookahead", DocValuesPrefetch.isLeapfrogLookahead());
            builder.field("agg_prefetch_lookaheads", DocValuesPrefetch.lookaheads());
            builder.field("agg_prefetch_leapfrogs", DocValuesPrefetch.leapfrogs());
            builder.field("agg_prefetch_shared_hits", DocValuesPrefetch.sharedHits());
            builder.field("agg_prefetch_shared_misses", DocValuesPrefetch.sharedMisses());
            builder.field("agg_prefetch_shared_searches", DocValuesPrefetch.sharedSearches());
            builder.field("agg_prefetch_run_ahead", DocValuesPrefetch.isRunAhead());
            builder.field("agg_prefetch_run_ahead_docs", DocValuesPrefetch.runAheadDocs());
            builder.field("agg_prefetch_run_ahead_gate", DocValuesPrefetch.isRunAheadGate());
            builder.field("agg_prefetch_run_ahead_bypass", DocValuesPrefetch.isRunAheadBypass());
            builder.field("agg_prefetch_run_ahead_switches", DocValuesPrefetch.runAheadSwitches());
            builder.field("agg_prefetch_run_ahead_leaves", DocValuesPrefetch.runAheadLeaves());
            builder.field("agg_prefetch_run_ahead_replays", DocValuesPrefetch.runAheadReplays());
            builder.field("sort_prefetch_planners", DocValuesPrefetch.sortPlanners());
            builder.field("sort_prefetch_requests", DocValuesPrefetch.sortRequests());
            for (Map.Entry<String, Object> entry : SortOptParams.current().entrySet()) {
                builder.field("sort_opt_" + entry.getKey(), entry.getValue());
            }
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

    /**
     * {@code POST} parses every sort_opt parameter here, so a bad value fails the request with 400 before anything changes,
     * and applies them when the request runs (after the REST layer has rejected unknown parameters). {@code GET} only reads.
     */
    private static RestChannelConsumer prepareSortOpt(RestRequest request, BlockCache cache) {
        final SortOptParams params;
        if (request.method() == POST) {
            final Map<String, String> values = new HashMap<>();
            for (String name : SortOptParams.NAMES) {
                final String value = request.param(name);
                if (value != null) {
                    values.put(name, value);
                }
            }
            params = SortOptParams.parse(values);
        } else {
            params = null;
        }
        return channel -> {
            if (params != null) {
                params.apply(cache.prefetchNodeBytes());
            }
            final XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            for (Map.Entry<String, Object> entry : SortOptParams.current().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }
}
