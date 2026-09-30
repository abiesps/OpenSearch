/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
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
            new Route(POST, "/_bufferpool/cache/_clear")
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
        }
        return channel -> {
            final XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("block_size", cache.blockSize());
            builder.field("cached_blocks", cache.size());
            builder.field("cached_bytes", cache.sizeInBytes());
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
