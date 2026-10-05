/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * A node with the prefetch budget settings: the prefetch executor is sized to the budget, the scheduler takes the
 * configured values, cold searches of a {@code bufferpoolfs} index go through the demand counter and the task listener,
 * and the node is idle again afterwards (no queued or pending item, no demand read in flight, no registered task).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class BufferPoolPrefetchSchedulerIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "prefetch_it";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(BufferPoolStorePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(BufferPoolStorePlugin.CACHE_SIZE_SETTING.getKey(), "32mb")
            .put(BufferPoolStorePlugin.BLOCK_SIZE_SETTING.getKey(), "8kb")
            .put(BufferPoolStorePlugin.SEQUENTIAL_READ_SIZE_SETTING.getKey(), "128kb")
            .put(BufferPoolStorePlugin.PREFETCH_MAX_IN_FLIGHT_SETTING.getKey(), 12)
            .put(BufferPoolStorePlugin.PREFETCH_BUDGET_SCOPE_SETTING.getKey(), "total")
            .put(BufferPoolStorePlugin.PREFETCH_QUEUE_SIZE_SETTING.getKey(), 100)
            .put(BufferPoolStorePlugin.PREFETCH_QUEUE_POLICY_SETTING.getKey(), "fair")
            .build();
    }

    private BufferPoolStorePlugin plugin() {
        // the data node's instance: the cluster may also have dedicated cluster-manager nodes, which hold no shard
        return internalCluster().getDataNodeInstance(PluginsService.class).filterPlugins(BufferPoolStorePlugin.class).get(0);
    }

    public void testBudgetSettingsReachTheNode() throws Exception {
        final ThreadPool.Info info = internalCluster().getDataNodeInstance(ThreadPool.class)
            .info(BufferPoolStorePlugin.PREFETCH_THREAD_POOL);
        assertEquals(12, info.getMax());
        assertEquals(12, info.getQueueSize().singles());
        final PrefetchScheduler scheduler = plugin().scheduler();
        assertEquals(12, scheduler.maxInFlight());
        assertEquals(PrefetchScheduler.BudgetScope.TOTAL, scheduler.scope());
        assertEquals(100, scheduler.queueSize());
        assertEquals(PrefetchScheduler.QueuePolicy.FAIR, scheduler.policy());

        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), BufferPoolStorePlugin.STORE_TYPE)
                    .put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 0)
            ).setMapping("{\"properties\":{\"n\":{\"type\":\"long\"},\"k\":{\"type\":\"keyword\"}}}")
        );
        ensureGreen(INDEX);
        final BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 5000; i++) {
            bulk.add(
                client().prepareIndex(INDEX).setId(Integer.toString(i)).setSource("n", randomLong(), "k", "v" + randomIntBetween(0, 50))
            );
        }
        final BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        refresh(INDEX);
        flush(INDEX);
        final BlockCache cache = plugin().blockCache();
        cache.clear();
        cache.resetStats();
        for (int q = 0; q < 5; q++) {
            final SearchResponse search = client().prepareSearch(INDEX)
                .setQuery(QueryBuilders.termQuery("k", "v" + q))
                .addSort("n", SortOrder.DESC)
                .addAggregation(AggregationBuilders.max("max_n").field("n"))
                .setRequestCache(false)
                .get();
            assertEquals(0, search.getFailedShards());
        }
        assertBusy(() -> {
            assertEquals(0, scheduler.pending());
            assertEquals(0, scheduler.demandReadsInFlight());
            assertEquals(0, plugin().taskListener().registeredTasks());
        });
        final PrefetchScheduler.Stats stats = scheduler.stats();
        long reads = 0;
        for (BlockCache.FileStats s : cache.stats().values()) {
            reads += s.reads.sum();
        }
        assertTrue("cold searches read from storage", reads > 0);
        assertEquals("every demand storage read is counted by the gate", reads, stats.demandReadsStarted());
        assertTrue(stats.maxTotalReadsAtItemStart() <= 12);
        assertEquals(0, cache.prefetchReadsOutsideSlot());
        assertEquals(0, cache.nestedPrefetchReads());
        assertTrue(cache.maxPrefetchReadsInFlight() <= stats.maxActiveWorkers());
    }
}
