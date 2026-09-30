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
import org.apache.lucene.search.DisjunctionPrefetch;
import org.apache.lucene.search.TopKPrefetch;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
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
            new Route(POST, "/_bufferpool/topk_prefetch")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final BlockCache cache = blockCache.get();
        final String path = request.path();
        if (path.endsWith("/_reset")) {
            cache.resetStats();
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
