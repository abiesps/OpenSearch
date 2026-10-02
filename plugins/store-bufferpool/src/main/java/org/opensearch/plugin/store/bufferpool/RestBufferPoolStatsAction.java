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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Experiment endpoints for the block cache of the node that receives the request (not cluster-wide):
 *
 * <ul>
 *   <li>{@code GET /_bufferpool/stats}: cache size and per-file-type IO counters</li>
 *   <li>{@code POST /_bufferpool/stats/_reset}: sets the counters to zero</li>
 *   <li>{@code POST /_bufferpool/cache/_clear}: drops all cached blocks, so the next reads are cold</li>
 *   <li>{@code POST /_bufferpool/dual_nav/_mode?mode=doc|nav}: where {@code Lucene104DualNav} postings read skip data
 *       from, for postings lists opened from now on (JVM-wide)</li>
 *   <li>{@code POST /_bufferpool/disjunction_prefetch?blocks=N[&aligned=true]}: exhaustive OR queries keep N cache blocks
 *       of each clause's postings requested ahead (JVM-wide, see Lucene's {@code DisjunctionPrefetch}); 0 disables it. With
 *       {@code aligned=true} requests are whole cache blocks: when a clause starts reading block k, blocks up to k + N are
 *       requested</li>
 *   <li>{@code POST /_bufferpool/topk_prefetch?norms_blocks=N[&filter=true|false][&doc_blocks=D]}: top-k OR queries
 *       keep each clause's postings requested D cache blocks ahead, and request norms up to N
 *       cache blocks ahead (N * block size doc IDs, for 1-byte norms), in whole blocks, only for doc windows whose max
 *       score can beat the current threshold unless {@code filter=false} (JVM-wide, see Lucene's {@code TopKPrefetch});
 *       0 disables it</li>
 *   <li>{@code POST /_bufferpool/agg_batch?mode=off|runend|vec|vecdec|pf|pfs|pfl|pfsl|pfw|pfwg[&docs=N]}: batch collection experiments for scorers and
 *       leaf collectors created from now on (JVM-wide). {@code runend}: Lucene's dense conjunction keeps each clause's
 *       doc ID run end instead of recomputing it per window (see Lucene's {@code CollectExperiments}); {@code vec}:
 *       runend plus OpenSearch batch aggregation collection ({@code BatchCollection}); {@code vecdec}: vec plus Lucene
 *       bulk doc-values reads that decode a span of packed values in one pass; {@code pf}: vecdec plus doc-values prefetch
 *       one node ahead, doc-ID aligned ({@code DocValuesPrefetch}); {@code pfs}, {@code pfl}, {@code pfsl}: pf with the
 *       look-ahead shared per segment (s), built as a leapfrog conjunction (l), or both; {@code pfw}: vecdec plus
 *       doc-values prefetch proven by the main scorer's own matches, buffered {@code docs} doc IDs (default 131072)
 *       ahead of collection (run-ahead, no second scorer); {@code pfwg}: pfw plus the gate (collection waits at a
 *       planner's next requested doc until the read after its node is known); {@code off} is stock</li>
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
            new Route(POST, "/_bufferpool/agg_batch")
        );
    }

    private static void setAggBatch(boolean cacheRunEnd, boolean batchCollection, boolean bulkDecode) {
        DocValuesPrefetch.setEnabled(false);
        DocValuesPrefetch.setShareLookahead(false);
        DocValuesPrefetch.setLeapfrogLookahead(false);
        DocValuesPrefetch.setRunAhead(false);
        DocValuesPrefetch.setRunAheadGate(false);
        CollectExperiments.setCacheRunEnd(cacheRunEnd);
        BatchCollection.setEnabled(batchCollection);
        CollectExperiments.setBulkDecode(bulkDecode);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final BlockCache cache = blockCache.get();
        final String path = request.path();
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
                DisjunctionPrefetch.setNodeBytes(cache.blockSize());
            }
            TopKPrefetch.setDocNodesAhead(docBlocks);
            // norms are 1 byte per doc for BM25 text fields: N blocks of norms = N * block size doc IDs
            TopKPrefetch.setNodeBytes(cache.blockSize());
            TopKPrefetch.setFilter(request.paramAsBoolean("filter", true));
            TopKPrefetch.setNormsDocsAhead(Math.toIntExact((long) blocks * cache.blockSize()));
        } else if (path.endsWith("/agg_batch")) {
            final String mode = request.param("mode");
            switch (mode == null ? "" : mode) {
                case "off" -> setAggBatch(false, false, false);
                case "runend" -> setAggBatch(true, false, false);
                case "vec" -> setAggBatch(true, true, false);
                case "vecdec" -> setAggBatch(true, true, true);
                case "pf", "pfs", "pfl", "pfsl" -> {
                    setAggBatch(true, true, true);
                    DocValuesPrefetch.setNodeBytes(cache.blockSize());
                    DocValuesPrefetch.setShareLookahead(mode.contains("s"));
                    DocValuesPrefetch.setLeapfrogLookahead(mode.contains("l"));
                    DocValuesPrefetch.setEnabled(true);
                }
                case "pfw", "pfwg" -> {
                    setAggBatch(true, true, true);
                    DocValuesPrefetch.setNodeBytes(cache.blockSize());
                    DocValuesPrefetch.setRunAheadDocs(request.paramAsInt("docs", 1 << 17));
                    DocValuesPrefetch.setRunAheadGate(mode.equals("pfwg"));
                    DocValuesPrefetch.setRunAhead(true);
                    DocValuesPrefetch.setEnabled(true);
                }
                default -> throw new IllegalArgumentException(
                    "[mode] must be off, runend, vec, vecdec, pf, pfs, pfl, pfsl, pfw or pfwg, got [" + mode + "]"
                );
            }
        } else if (path.endsWith("/disjunction_prefetch")) {
            final int blocks = request.paramAsInt("blocks", -1);
            if (blocks < 0) {
                throw new IllegalArgumentException("missing or negative [blocks]");
            }
            DisjunctionPrefetch.setNodeBytes(request.paramAsBoolean("aligned", false) ? cache.blockSize() : 0);
            DisjunctionPrefetch.setBytesAhead((long) blocks * cache.blockSize());
        }
        return channel -> {
            final XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("block_size", cache.blockSize());
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
            builder.field("agg_prefetch_run_ahead_leaves", DocValuesPrefetch.runAheadLeaves());
            builder.field("agg_prefetch_run_ahead_replays", DocValuesPrefetch.runAheadReplays());
            builder.field("simulated_load_latency_micros", TimeUnit.NANOSECONDS.toMicros(cache.simulatedLoadLatencyNanos()));
            builder.startObject("files");
            for (Map.Entry<String, BlockCache.FileStats> entry : cache.stats().entrySet()) {
                final BlockCache.FileStats s = entry.getValue();
                builder.startObject(entry.getKey());
                builder.field("requests", s.requests.sum());
                builder.field("loads", s.loads.sum());
                builder.field("prefetch_requests", s.prefetchRequests.sum());
                builder.field("prefetch_loads", s.prefetchLoads.sum());
                builder.field("bytes_loaded", s.bytesLoaded.sum());
                builder.field("load_time_micros", TimeUnit.NANOSECONDS.toMicros(s.loadNanos.sum()));
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }
}
