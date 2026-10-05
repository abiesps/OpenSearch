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
import java.util.function.Supplier;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Experiment endpoints that record the block loads of the node that receives the request:
 *
 * <ul>
 *   <li>{@code POST /_bufferpool/trace/_start?max_events=N}: starts a new trace (default 100,000 events)</li>
 *   <li>{@code GET /_bufferpool/trace}: returns the events recorded so far, trace keeps running</li>
 *   <li>{@code POST /_bufferpool/trace/_stop}: stops the trace and returns its events</li>
 * </ul>
 *
 * Each event is one block load (a cache miss or a prefetch load) with its file, block offset, and the innermost Lucene
 * codec and search methods on the stack (for a prefetch load: on the stack that requested the prefetch). A load with
 * {@code readahead: true} is a block nobody requested, inserted because it shares a read window with a requested block;
 * {@code readahead_unread} counts those that no reader read, apart from {@code prefetched_unread}.
 * {@code prefetched_unread} holds the count of blocks that a prefetch loaded during the trace and that no reader read
 * before the response, the first 20 of them as {@code file:block}, and the count per requesting code
 * ({@code by_requester}, "codec caller / search caller").
 */
final class RestBufferPoolTraceAction extends BaseRestHandler {

    private final Supplier<BlockCache> blockCache;

    RestBufferPoolTraceAction(Supplier<BlockCache> blockCache) {
        this.blockCache = blockCache;
    }

    @Override
    public String getName() {
        return "bufferpool_trace_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_bufferpool/trace/_start"),
            new Route(GET, "/_bufferpool/trace"),
            new Route(POST, "/_bufferpool/trace/_stop")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final BlockCache cache = blockCache.get();
        final String path = request.path();
        final BlockCache.Trace trace;
        final boolean started;
        if (path.endsWith("/_start")) {
            cache.startTrace(request.paramAsInt("max_events", 100_000));
            trace = null;
            started = true;
        } else if (path.endsWith("/_stop")) {
            trace = cache.stopTrace();
            started = false;
        } else {
            trace = cache.currentTrace();
            started = false;
        }
        return channel -> {
            final XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("block_size", cache.blockSize());
            if (started) {
                builder.field("started", true);
            } else if (trace == null) {
                builder.field("running", false);
            } else {
                builder.field("dropped", trace.dropped());
                builder.startObject("prefetched_unread");
                builder.field("count", trace.prefetchedUnreadCount());
                builder.field("blocks", trace.prefetchedUnread(cache.blockSize(), 20));
                builder.field("by_requester", trace.prefetchedUnreadByRequester());
                builder.endObject();
                builder.field("readahead_unread", trace.readaheadUnreadCount());
                builder.startArray("events");
                for (BlockCache.Event e : trace.events) {
                    builder.startObject();
                    builder.field("seq", e.seq());
                    builder.field("micros", e.nanos() / 1000);
                    builder.field("file", e.file());
                    builder.field("block", e.blockOffset() / cache.blockSize());
                    builder.field("offset", e.blockOffset());
                    builder.field("size", e.size());
                    builder.field("prefetch", e.prefetch());
                    builder.field("readahead", e.readahead());
                    builder.field("thread", e.thread());
                    builder.field("codec", e.codecCaller());
                    builder.field("search", e.searchCaller());
                    builder.field("waited_micros", e.waitedNanos() / 1000);
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }
}
