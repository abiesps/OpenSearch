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
import org.apache.lucene.util.bkd.BKDExperiments;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.aggregations.BatchCollection;
import org.opensearch.search.aggregations.DocValuesPrefetch;
import org.opensearch.search.query.SortIoExperiments;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * The parts of the plugin that use APIs of the proof-of-concept OpenSearch and Lucene fork (this source set is built with
 * {@code -Dbufferpool.target=poc}, the default; {@code src/stock} has the same class without them, for stock OpenSearch and
 * stock Lucene). The store itself does not depend on them.
 *
 * <ul>
 *   <li>the prefetch planners' node size ({@link #setPrefetchNodeBytes});</li>
 *   <li>the per-field format codecs and the mapping check of their names ({@link #codecServiceFactory},
 *       {@link #actionFilters});</li>
 *   <li>the experiment endpoints and their fields in {@code GET /_bufferpool/stats}:
 *   <ul>
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
 *   </ul>
 *   </li>
 * </ul>
 * A prefetch "node" is {@link BlockCache#prefetchNodeBytes()} bytes, and parameters named {@code blocks} count such nodes,
 * see {@link RestBufferPoolStatsAction}.
 */
final class ExperimentHooks {

    private ExperimentHooks() {}

    /**
     * Which OpenSearch and Lucene this build is for. A method, not a constant, so callers in {@code src/main} do not inline
     * it and their class files stay the same in both builds.
     */
    static String target() {
        return "poc";
    }

    /**
     * Sets the node size of every prefetch planner to {@code nodeBytes}, the configured sequential read size, so a planner
     * requests whole storage reads and its node size does not depend on the cache block size. The planners are JVM-wide;
     * the experiment endpoints set the same value again when they enable one.
     */
    static void setPrefetchNodeBytes(long nodeBytes) {
        DocValuesPrefetch.setNodeBytes(nodeBytes);
        TopKPrefetch.setNodeBytes(nodeBytes);
        // the BKD planner does not accept nodes below its minimum; a larger node is still read in whole windows
        BKDExperiments.setNodeBytes(Math.max(BKDExperiments.MIN_NODE_BYTES, nodeBytes));
        SortIoExperiments.setSortPrefetchNodeBytes(nodeBytes);
        // DisjunctionPrefetch stays unaligned (node size 0) until its endpoint enables aligned requests
    }

    /**
     * The codec service of {@value BufferPoolStorePlugin#STORE_TYPE} indices: every codec ({@code index.codec}, any mode)
     * lets each field choose its postings format and its points format through the {@code meta.postings_format} and
     * {@code meta.points_format} mapping entries, see {@link BufferPoolCodecService}.
     */
    static Optional<CodecServiceFactory> codecServiceFactory() {
        return Optional.of(BufferPoolCodecService::new);
    }

    /**
     * Refuses a {@code meta.postings_format} or {@code meta.points_format} name that is not available in the mapping of a
     * {@value BufferPoolStorePlugin#STORE_TYPE} index, when the index is created or its mapping is updated, see
     * {@link FormatMetaMappingValidator}.
     */
    static List<ActionFilter> actionFilters(
        Supplier<ClusterState> clusterState,
        Supplier<IndexNameExpressionResolver> indexNameExpressionResolver
    ) {
        return List.of(new FormatMetaMappingValidator(clusterState, indexNameExpressionResolver));
    }

    /** The experiment endpoints, served by {@link RestBufferPoolStatsAction}. */
    static List<Route> routes() {
        return List.of(
            new Route(POST, "/_bufferpool/dual_nav/_mode"),
            new Route(POST, "/_bufferpool/disjunction_prefetch"),
            new Route(POST, "/_bufferpool/topk_prefetch"),
            new Route(POST, "/_bufferpool/agg_batch"),
            new Route(POST, "/_bufferpool/sort_opt"),
            new Route(GET, "/_bufferpool/sort_opt")
        );
    }

    /**
     * Handles an experiment endpoint of {@link #routes()}: {@code sort_opt} answers with its own body (returned); a switch
     * endpoint applies its parameters and returns null, so the caller answers with the stats. Any other path returns null
     * and changes nothing.
     */
    static CheckedConsumer<RestChannel, Exception> prepareRequest(RestRequest request, BlockCache cache) {
        final String path = request.path();
        if (path.endsWith("/sort_opt")) {
            return prepareSortOpt(request, cache);
        }
        if (path.endsWith("/_mode")) {
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
        return null;
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

    /** {@code POST /_bufferpool/stats/_reset}: the experiment counters. */
    static void resetCounters() {
        BatchCollection.resetCounters();
        DocValuesPrefetch.resetCounters();
    }

    /** The experiment fields of {@code GET /_bufferpool/stats}. */
    static void writeStats(XContentBuilder builder) throws IOException {
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
    }

    /**
     * {@code POST} parses every sort_opt parameter here, so a bad value fails the request with 400 before anything changes,
     * and applies them when the request runs (after the REST layer has rejected unknown parameters). {@code GET} only reads.
     */
    private static CheckedConsumer<RestChannel, Exception> prepareSortOpt(RestRequest request, BlockCache cache) {
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
