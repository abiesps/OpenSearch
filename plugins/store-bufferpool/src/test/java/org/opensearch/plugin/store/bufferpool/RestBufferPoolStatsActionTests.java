/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.BudgetScope;
import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.PrefetchOwner;
import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.QueuePolicy;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** {@code GET /_bufferpool/stats}: the scheduler object, the histograms and their buckets, and the reset. */
public class RestBufferPoolStatsActionTests extends OpenSearchTestCase {

    private Map<String, Object> call(RestBufferPoolStatsAction action, RestRequest.Method method, String path, Map<String, String> params)
        throws Exception {
        final FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(method)
            .withPath(path)
            .withParams(new java.util.HashMap<>(params))
            .build();
        final FakeRestChannel channel = new FakeRestChannel(request, true, 1);
        action.handleRequest(request, channel, null);
        assertEquals(RestStatus.OK, channel.capturedResponse().status());
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, channel.capturedResponse().content().utf8ToString(), true);
    }

    @SuppressWarnings("unchecked")
    public void testSchedulerFieldsAndHistograms() throws Exception {
        final Path file = createTempDir().resolve("_0.dvd");
        final byte[] data = new byte[64 * 1024];
        random().nextBytes(data);
        Files.write(file, data);
        final PrefetchScheduler scheduler = new PrefetchScheduler(
            Runnable::run,
            32,
            BudgetScope.TOTAL,
            128,
            QueuePolicy.FAIR,
            PrefetchOwner::ofCurrentThread
        );
        final BlockCache cache = new BlockCache(1L << 24, 1024, 4096, 32768, NativeReadHints.DISABLED, scheduler);
        final RestBufferPoolStatsAction action = new RestBufferPoolStatsAction(() -> cache, () -> 3);
        try (StorageFile channel = StorageFile.open(file, NativeReadHints.DISABLED)) {
            final BlockCache.FileStats stats = cache.statsFor("_0.dvd");
            cache.prefetch(file, 1, channel, data.length, 0, 32, stats);
            cache.getOrLoad(new BlockKey(file, 1, 40 * 1024), channel, data.length, 32768, stats);
        }
        Map<String, Object> body = call(action, RestRequest.Method.GET, "/_bufferpool/stats", Map.of());
        assertEquals(0, body.get("pending_prefetch_tasks"));
        assertEquals(0, body.get("rejected_prefetch_tasks"));
        final Map<String, Object> s = (Map<String, Object>) body.get("prefetch_scheduler");
        assertEquals(32, s.get("max_in_flight"));
        assertEquals("total", s.get("budget_scope"));
        assertEquals(128, s.get("queue_size"));
        assertEquals("fair", s.get("queue_policy"));
        assertEquals(1, s.get("max_reads_in_flight"));
        assertEquals(1, s.get("max_active_workers"));
        assertEquals(1, s.get("items_admitted"));
        assertEquals(1, s.get("items_started"));
        assertEquals(1, s.get("items_finished"));
        assertEquals(1, s.get("demand_reads_started"));
        assertEquals(1, s.get("max_demand_reads_in_flight"));
        assertEquals(3, s.get("registered_tasks"));
        assertEquals(32768, s.get("bytes_read"));
        assertEquals(32768, s.get("demand_bytes_read"));
        assertEquals(0, s.get("prefetch_reads_outside_slot"));
        assertEquals(0, s.get("nested_prefetch_reads"));
        assertEquals(
            Map.of("queue_full", 0, "longest_queue", 0, "cancelled", 0, "rejected_by_executor", 0, "shutdown", 0),
            s.get("dropped")
        );
        for (String field : List.of(
            "reads_in_flight",
            "demand_reads_in_flight",
            "max_total_reads_in_flight",
            "max_total_reads_at_item_start",
            "budget_held_dispatches",
            "items_started_after_phase_end",
            "active_workers",
            "queued",
            "max_queued",
            "pending",
            "requesters",
            "max_requesters",
            "windows_skipped_cancelled",
            "busy_time_micros",
            "read_time_micros",
            "queue_wait_time_micros",
            "demand_read_time_micros"
        )) {
            assertTrue(field, s.containsKey(field));
        }
        final Map<String, Object> latency = (Map<String, Object>) body.get("read_latency_micros");
        final Map<String, Object> prefetch = (Map<String, Object>) latency.get("prefetch");
        assertEquals(1, ((Map<String, Object>) prefetch.get("32768")).get("count"));
        assertEquals(0, ((Map<String, Object>) prefetch.get("131072")).get("count"));
        assertEquals(0, ((Map<String, Object>) prefetch.get("other")).get("count"));
        final Map<String, Object> demand = (Map<String, Object>) ((Map<String, Object>) latency.get("demand")).get("32768");
        assertEquals(1, demand.get("count"));
        assertEquals(List.of("count", "median", "percentile_90", "percentile_99", "max"), List.copyOf(demand.keySet()));
        final Map<String, Object> waits = (Map<String, Object>) body.get("prefetch_queue_wait_micros");
        assertEquals(1, ((Map<String, Object>) waits.get("short_requester")).get("count"));
        assertEquals(0, ((Map<String, Object>) waits.get("long_requester")).get("count"));
        assertEquals(0, ((Map<String, Object>) body.get("demand_wait_micros")).get("count"));
        assertFalse(demand.containsKey("buckets"));

        // with buckets
        body = call(action, RestRequest.Method.GET, "/_bufferpool/stats", Map.of("histogram_buckets", "true"));
        final Map<String, Object> withBuckets = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) body.get(
            "read_latency_micros"
        )).get("demand")).get("32768");
        final List<List<Number>> buckets = (List<List<Number>>) withBuckets.get("buckets");
        assertEquals(1, buckets.size());
        assertEquals(1, buckets.get(0).get(1).intValue());
        assertEquals(List.of(), ((Map<String, Object>) body.get("demand_wait_micros")).get("buckets"));

        // the reset clears the counters and histograms
        call(action, RestRequest.Method.POST, "/_bufferpool/stats/_reset", Map.of());
        body = call(action, RestRequest.Method.GET, "/_bufferpool/stats", Map.of());
        final Map<String, Object> reset = (Map<String, Object>) body.get("prefetch_scheduler");
        assertEquals(0, reset.get("items_started"));
        assertEquals(0, reset.get("demand_reads_started"));
        assertEquals(0, reset.get("bytes_read"));
        assertEquals(0, reset.get("max_reads_in_flight"));
        assertEquals(0, reset.get("max_active_workers"));
        assertEquals(
            0,
            ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) body.get("read_latency_micros")).get("demand")).get(
                "32768"
            )).get("count")
        );
    }
}
