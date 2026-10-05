/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.lucene.search.TopKPrefetch;
import org.apache.lucene.util.bkd.BKDExperiments;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.search.aggregations.DocValuesPrefetch;
import org.opensearch.search.query.SortIoExperiments;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;

import static org.opensearch.plugin.store.bufferpool.BufferPoolStorePlugin.BLOCK_SIZE_SETTING;
import static org.opensearch.plugin.store.bufferpool.BufferPoolStorePlugin.PREFETCH_BUDGET_SCOPE_SETTING;
import static org.opensearch.plugin.store.bufferpool.BufferPoolStorePlugin.PREFETCH_MAX_IN_FLIGHT_SETTING;
import static org.opensearch.plugin.store.bufferpool.BufferPoolStorePlugin.PREFETCH_QUEUE_POLICY_SETTING;
import static org.opensearch.plugin.store.bufferpool.BufferPoolStorePlugin.PREFETCH_QUEUE_SIZE_SETTING;
import static org.opensearch.plugin.store.bufferpool.BufferPoolStorePlugin.PREFETCH_TASK_PER_WINDOW_SETTING;
import static org.opensearch.plugin.store.bufferpool.BufferPoolStorePlugin.RANDOM_READ_SIZE_SETTING;
import static org.opensearch.plugin.store.bufferpool.BufferPoolStorePlugin.READ_HINT_SETTING;
import static org.opensearch.plugin.store.bufferpool.BufferPoolStorePlugin.SEQUENTIAL_READ_SIZE_SETTING;

public class BufferPoolStorePluginSettingsTests extends OpenSearchTestCase {

    private static BlockCache create(Settings settings) {
        return BufferPoolStorePlugin.createBlockCache(settings, 1 << 20, Runnable::run);
    }

    public void testReadSizesDefaultToTheBlockSize() {
        BlockCache cache = create(Settings.EMPTY);
        assertEquals(BlockCache.DEFAULT_BLOCK_SIZE, cache.blockSize());
        assertEquals(BlockCache.DEFAULT_BLOCK_SIZE, cache.randomReadSize());
        assertEquals(BlockCache.DEFAULT_BLOCK_SIZE, cache.sequentialReadSize());
        cache = create(Settings.builder().put(BLOCK_SIZE_SETTING.getKey(), "8kb").build());
        assertEquals(8192, cache.blockSize());
        assertEquals(8192, cache.randomReadSize());
        assertEquals(8192, cache.sequentialReadSize());
        assertEquals(8192, cache.prefetchNodeBytes());
    }

    public void testConfiguredReadSizes() {
        final BlockCache cache = create(
            Settings.builder()
                .put(BLOCK_SIZE_SETTING.getKey(), "8kb")
                .put(RANDOM_READ_SIZE_SETTING.getKey(), "32kb")
                .put(SEQUENTIAL_READ_SIZE_SETTING.getKey(), "128kb")
                .build()
        );
        assertEquals(8192, cache.blockSize());
        assertEquals(32768, cache.randomReadSize());
        assertEquals(131072, cache.sequentialReadSize());
        assertEquals(131072, cache.prefetchNodeBytes());
        assertEquals(32768, cache.readSize(true));
        assertEquals(131072, cache.readSize(false));
    }

    public void testInvalidSizesAreRejected() {
        // not a power of two
        assertInvalid(Settings.builder().put(BLOCK_SIZE_SETTING.getKey(), "3000b").build(), "block size must be a power of two");
        assertInvalid(
            Settings.builder().put(BLOCK_SIZE_SETTING.getKey(), "8kb").put(RANDOM_READ_SIZE_SETTING.getKey(), "24kb").build(),
            "[bufferpool.io.random_read_size] must be a power of two between the block size [8192]"
        );
        // smaller than the block size, so not a multiple of it
        assertInvalid(
            Settings.builder().put(BLOCK_SIZE_SETTING.getKey(), "8kb").put(SEQUENTIAL_READ_SIZE_SETTING.getKey(), "4kb").build(),
            "[bufferpool.io.sequential_read_size] must be a power of two between the block size [8192]"
        );
        // a block size that the read sizes default to, but larger than an explicit read size
        assertInvalid(
            Settings.builder().put(BLOCK_SIZE_SETTING.getKey(), "256kb").put(RANDOM_READ_SIZE_SETTING.getKey(), "128kb").build(),
            "[bufferpool.io.random_read_size] must be a power of two between the block size [262144]"
        );
        // above the largest read size
        assertInvalid(Settings.builder().put(SEQUENTIAL_READ_SIZE_SETTING.getKey(), "16mb").build(), "bufferpool.io.sequential_read_size");
    }

    private static void assertInvalid(Settings settings, String message) {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> create(settings));
        assertTrue(e.getMessage() + " / cause " + e.getCause(), messages(e).contains(message));
    }

    private static String messages(Throwable e) {
        final StringBuilder b = new StringBuilder();
        for (Throwable t = e; t != null; t = t.getCause()) {
            b.append(t.getMessage()).append('\n');
        }
        return b.toString();
    }

    public void testPlannerNodeSizesComeFromTheSequentialReadSize() {
        try {
            final BlockCache cache = create(
                Settings.builder()
                    .put(BLOCK_SIZE_SETTING.getKey(), "8kb")
                    .put(RANDOM_READ_SIZE_SETTING.getKey(), "32kb")
                    .put(SEQUENTIAL_READ_SIZE_SETTING.getKey(), "256kb")
                    .build()
            );
            BufferPoolStorePlugin.setPrefetchNodeBytes(cache.prefetchNodeBytes());
            assertEquals(262144, DocValuesPrefetch.nodeBytes());
            assertEquals(262144, TopKPrefetch.getNodeBytes());
            assertEquals(262144, BKDExperiments.getNodeBytes());
            assertEquals(262144, SortIoExperiments.sortPrefetchNodeBytes());
            // below the BKD planner's minimum node size: that planner uses its minimum
            BufferPoolStorePlugin.setPrefetchNodeBytes(1024);
            assertEquals(1024, DocValuesPrefetch.nodeBytes());
            assertEquals(BKDExperiments.MIN_NODE_BYTES, BKDExperiments.getNodeBytes());
        } finally {
            BufferPoolStorePlugin.setPrefetchNodeBytes(BlockCache.DEFAULT_BLOCK_SIZE);
        }
    }

    public void testReadHintSetting() {
        assertEquals("auto", READ_HINT_SETTING.get(Settings.EMPTY));
        assertEquals(NativeReadHints.isAvailable(), create(Settings.EMPTY).readHints().enabled());
        final BlockCache none = create(Settings.builder().put(READ_HINT_SETTING.getKey(), "none").build());
        assertFalse(none.readHints().enabled());
        assertEquals(NativeReadHints.Mode.NONE, none.readHints().mode());
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> create(Settings.builder().put(READ_HINT_SETTING.getKey(), "always").build())
        );
        assertTrue(messages(e), messages(e).contains("must be auto, willneed or none, got [always]"));
        final Settings willneed = Settings.builder().put(READ_HINT_SETTING.getKey(), "WillNeed").build();
        if (NativeReadHints.isAvailable()) {
            assertEquals(NativeReadHints.Mode.WILLNEED, create(willneed).readHints().mode());
        } else {
            assertInvalid(willneed, "posix_fadvise is not available");
        }
    }

    public void testPrefetchTaskPerWindowSetting() {
        assertFalse(PREFETCH_TASK_PER_WINDOW_SETTING.get(Settings.EMPTY));
        assertTrue(PREFETCH_TASK_PER_WINDOW_SETTING.isDynamic());
        assertFalse(create(Settings.EMPTY).prefetchTaskPerWindow());
    }

    private static PrefetchScheduler scheduler(Settings settings) {
        return BufferPoolStorePlugin.createScheduler(settings, Runnable::run, PrefetchScheduler.PrefetchOwner::ofCurrentThread);
    }

    public void testPrefetchBudgetDefaultsToTodaysPool() {
        final int expected = Math.min(8, OpenSearchExecutors.allocatedProcessors(Settings.EMPTY));
        assertEquals(expected, (int) PREFETCH_MAX_IN_FLIGHT_SETTING.get(Settings.EMPTY));
        final Settings four = Settings.builder().put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 4).build();
        assertEquals(4, (int) PREFETCH_MAX_IN_FLIGHT_SETTING.get(four));
        final PrefetchScheduler s = scheduler(Settings.EMPTY);
        assertEquals(expected, s.maxInFlight());
        assertEquals(PrefetchScheduler.BudgetScope.PREFETCH, s.scope());
        assertEquals(1024, s.queueSize());
        assertEquals(PrefetchScheduler.QueuePolicy.FIFO, s.policy());
        assertEquals("prefetch", PREFETCH_BUDGET_SCOPE_SETTING.get(Settings.EMPTY));
        assertEquals("fifo", PREFETCH_QUEUE_POLICY_SETTING.get(Settings.EMPTY));
        assertEquals(1024, (int) PREFETCH_QUEUE_SIZE_SETTING.get(Settings.EMPTY));
        for (Setting<?> setting : List.of(
            PREFETCH_MAX_IN_FLIGHT_SETTING,
            PREFETCH_BUDGET_SCOPE_SETTING,
            PREFETCH_QUEUE_SIZE_SETTING,
            PREFETCH_QUEUE_POLICY_SETTING
        )) {
            assertFalse(setting.isDynamic());
            assertTrue(new BufferPoolStorePlugin().getSettings().contains(setting));
        }
    }

    public void testConfiguredPrefetchScheduler() {
        final PrefetchScheduler s = scheduler(
            Settings.builder()
                .put(PREFETCH_MAX_IN_FLIGHT_SETTING.getKey(), 32)
                .put(PREFETCH_BUDGET_SCOPE_SETTING.getKey(), "total")
                .put(PREFETCH_QUEUE_SIZE_SETTING.getKey(), 0)
                .put(PREFETCH_QUEUE_POLICY_SETTING.getKey(), "fair")
                .build()
        );
        assertEquals(32, s.maxInFlight());
        assertEquals(PrefetchScheduler.BudgetScope.TOTAL, s.scope());
        assertEquals(0, s.queueSize());
        assertEquals(PrefetchScheduler.QueuePolicy.FAIR, s.policy());
        assertEquals(65536, scheduler(Settings.builder().put(PREFETCH_QUEUE_SIZE_SETTING.getKey(), 65536).build()).queueSize());
        assertEquals(256, scheduler(Settings.builder().put(PREFETCH_MAX_IN_FLIGHT_SETTING.getKey(), 256).build()).maxInFlight());
        assertEquals(1, scheduler(Settings.builder().put(PREFETCH_MAX_IN_FLIGHT_SETTING.getKey(), 1).build()).maxInFlight());
    }

    public void testInvalidPrefetchSettingsAreRejected() {
        final String budget = PREFETCH_MAX_IN_FLIGHT_SETTING.getKey();
        for (String value : List.of("0", "-1", "257", "abc")) {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> scheduler(Settings.builder().put(budget, value).build())
            );
            assertTrue(messages(e), messages(e).contains("[" + budget + "]"));
            assertTrue(messages(e), messages(e).contains("[" + value + "]"));
        }
        assertTrue(
            messages(expectThrows(IllegalArgumentException.class, () -> scheduler(Settings.builder().put(budget, 257).build()))).contains(
                "must be <= 256"
            )
        );
        final String queue = PREFETCH_QUEUE_SIZE_SETTING.getKey();
        for (String value : List.of("-1", "65537")) {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> scheduler(Settings.builder().put(queue, value).build())
            );
            assertTrue(messages(e), messages(e).contains(queue));
        }
        for (String value : List.of("FIFO", "lifo", "")) {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> scheduler(Settings.builder().put(PREFETCH_QUEUE_POLICY_SETTING.getKey(), value).build())
            );
            assertTrue(messages(e), messages(e).contains("[bufferpool.prefetch.queue_policy] must be fifo or fair, got [" + value + "]"));
        }
        for (String value : List.of("Total", "demand", "")) {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> scheduler(Settings.builder().put(PREFETCH_BUDGET_SCOPE_SETTING.getKey(), value).build())
            );
            assertTrue(
                messages(e),
                messages(e).contains("[bufferpool.prefetch.budget_scope] must be prefetch or total, got [" + value + "]")
            );
        }
    }

    public void testPrefetchExecutorIsSizedToTheBudget() {
        final int budget = randomIntBetween(1, 64);
        final Settings settings = Settings.builder().put(PREFETCH_MAX_IN_FLIGHT_SETTING.getKey(), budget).put("node.name", "test").build();
        final ThreadPool threadPool = new TestThreadPool(
            "test",
            settings,
            new BufferPoolStorePlugin().getExecutorBuilders(settings).toArray(new org.opensearch.threadpool.ExecutorBuilder<?>[0])
        );
        try {
            final ThreadPool.Info info = threadPool.info(BufferPoolStorePlugin.PREFETCH_THREAD_POOL);
            assertEquals(budget, info.getMax());
            assertEquals(budget, info.getMin());
            assertEquals(budget, info.getQueueSize().singles());
        } finally {
            ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
        }
        for (String key : List.of("thread_pool.bufferpool_prefetch.size", "thread_pool.bufferpool_prefetch.queue_size")) {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new BufferPoolStorePlugin().getExecutorBuilders(Settings.builder().put(key, 4).build())
            );
            assertEquals("[" + key + "] is not supported; set [bufferpool.prefetch.max_in_flight]", e.getMessage());
        }
    }

    public void testCloseWithoutComponentsDoesNothing() throws Exception {
        final BufferPoolStorePlugin plugin = new BufferPoolStorePlugin();
        plugin.close();
        plugin.close();
        assertEquals(0, plugin.taskListener().registeredTasks());
    }

    public void testExecutorFactoryKeepsTodaysDefaults() {
        final BlockCache cache = create(Settings.builder().put(PREFETCH_MAX_IN_FLIGHT_SETTING.getKey(), 32).build());
        assertEquals(8, cache.scheduler().maxInFlight());
        assertEquals(PrefetchScheduler.BudgetScope.PREFETCH, cache.scheduler().scope());
        assertEquals(1024, cache.scheduler().queueSize());
        assertEquals(PrefetchScheduler.QueuePolicy.FIFO, cache.scheduler().policy());
    }
}
