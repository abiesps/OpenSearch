/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Adds the {@value #STORE_TYPE} store type ({@code index.store.type: bufferpoolfs}). Shards of such indices read their
 * files through one node-wide, Caffeine-backed {@link BlockCache} of off-heap blocks instead of the OS page cache mappings
 * that {@code mmapfs} uses.
 */
public class BufferPoolStorePlugin extends Plugin implements IndexStorePlugin, EnginePlugin, ActionPlugin {

    private static final Logger logger = LogManager.getLogger(BufferPoolStorePlugin.class);

    /** Creates the plugin; the block cache is created when the node starts. */
    public BufferPoolStorePlugin() {}

    /** Value of {@code index.store.type} that selects {@link BufferPoolDirectory}. */
    public static final String STORE_TYPE = "bufferpoolfs";

    /** Thread pool that loads prefetched blocks. */
    public static final String PREFETCH_THREAD_POOL = "bufferpool_prefetch";

    /**
     * Upper bound of the total size of the cached blocks on this node, as bytes or as a percentage of the heap size. The
     * blocks are direct buffers, so this must stay below {@code -XX:MaxDirectMemorySize}, which OpenSearch sets to half of
     * the heap by default. Direct memory outside this bound: the idle read buffers of multi-block reads (at most 32 MiB),
     * one read buffer of the largest read size per concurrent window read beyond those (search threads plus prefetch
     * threads), and the copy of each block being inserted. Leave room for them, and for Netty and other direct buffers.
     */
    public static final Setting<ByteSizeValue> CACHE_SIZE_SETTING = Setting.memorySizeSetting(
        "bufferpool.cache.size",
        "10%",
        Property.NodeScope
    );

    /** Size of a cached block, a power of two. Changing it needs a node restart. */
    public static final Setting<ByteSizeValue> BLOCK_SIZE_SETTING = new Setting<>(
        "bufferpool.cache.block_size",
        new ByteSizeValue(BlockCache.DEFAULT_BLOCK_SIZE).getStringRep(),
        new Setting.ByteSizeValueParser(
            new ByteSizeValue(BlockCache.MIN_BLOCK_SIZE),
            new ByteSizeValue(BlockCache.MAX_BLOCK_SIZE),
            "bufferpool.cache.block_size"
        ),
        value -> BlockCache.validateBlockSize(value.getBytes()),
        Property.NodeScope
    );

    /**
     * Bytes read from storage per miss of a file opened for random access (Lucene {@code DataAccessHint.RANDOM}, for
     * example stored fields): the aligned window that holds the missed block, clipped at the end of the file. A power of two
     * and at least the block size; defaults to the block size (one block per miss). Changing it needs a node restart.
     */
    public static final Setting<ByteSizeValue> RANDOM_READ_SIZE_SETTING = readSizeSetting("bufferpool.io.random_read_size");

    /**
     * Bytes read from storage per miss of any other file, and per prefetch window (prefetched ranges are aligned to and
     * coalesced into windows of this size). Also the node size of the prefetch planners. A power of two and at least the
     * block size; defaults to the block size. Changing it needs a node restart.
     */
    public static final Setting<ByteSizeValue> SEQUENTIAL_READ_SIZE_SETTING = readSizeSetting("bufferpool.io.sequential_read_size");

    private static Setting<ByteSizeValue> readSizeSetting(String key) {
        return new Setting<>(
            new Setting.SimpleKey(key),
            settings -> BLOCK_SIZE_SETTING.get(settings).getStringRep(),
            new Setting.ByteSizeValueParser(new ByteSizeValue(BlockCache.MIN_BLOCK_SIZE), new ByteSizeValue(BlockCache.MAX_READ_SIZE), key),
            new Setting.Validator<ByteSizeValue>() {
                @Override
                public void validate(ByteSizeValue value) {}

                @Override
                public void validate(ByteSizeValue value, Map<Setting<?>, Object> settings) {
                    final ByteSizeValue blockSize = (ByteSizeValue) settings.get(BLOCK_SIZE_SETTING);
                    BlockCache.validateReadSize("[" + key + "]", value.getBytes(), blockSize.getBytes());
                }

                @Override
                public Iterator<Setting<?>> settings() {
                    final List<Setting<?>> dependencies = List.of(BLOCK_SIZE_SETTING);
                    return dependencies.iterator();
                }
            },
            Property.NodeScope
        );
    }

    /**
     * How storage reads are announced to the kernel, see {@link NativeReadHints}: {@code willneed} calls
     * {@code posix_fadvise(POSIX_FADV_WILLNEED)} on each read window first, so the window is one storage IO even with
     * kernel read-ahead off (without it, Linux reads a buffered window page by page when {@code read_ahead_kb} is 0); the node
     * fails to start if the platform cannot do it. {@code none} reads without hints. {@code auto} (default) is
     * {@code willneed} on Linux and {@code none} elsewhere. With hints on, each open file holds a second, read-only file
     * descriptor. Changing it needs a node restart.
     */
    public static final Setting<String> READ_HINT_SETTING = Setting.simpleString(
        "bufferpool.io.read_hint",
        "auto",
        NativeReadHints.Mode::parse,
        Property.NodeScope
    );

    /**
     * Whether each aligned window of a prefetch request is its own prefetch task (read concurrently with the request's other
     * windows, up to the prefetch thread count) instead of one task per request that reads its windows one after another.
     * Tasks that do not fit in the prefetch queue are dropped. Default false. Dynamic.
     */
    public static final Setting<Boolean> PREFETCH_TASK_PER_WINDOW_SETTING = Setting.boolSetting(
        "bufferpool.prefetch.task_per_window",
        false,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Experiment knob: a delay added to every block load, to simulate a remote storage backend (for example about 4ms for
     * EFS) on a local disk. 0 disables it.
     */
    public static final Setting<TimeValue> SIMULATED_LOAD_LATENCY_SETTING = Setting.timeSetting(
        "bufferpool.simulated_load_latency",
        TimeValue.ZERO,
        TimeValue.ZERO,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * The node read budget: the most prefetch storage reads in flight on this node, and with
     * {@code bufferpool.prefetch.budget_scope: total} the most prefetch plus demand storage reads at which a prefetch still
     * starts. Demand reads never wait for it. 1 to 256; default {@code min(8, allocated processors)}, the size of the prefetch
     * pool before this setting. The {@value #PREFETCH_THREAD_POOL} executor is sized to it, so it needs a node restart. The
     * value depends on the storage (for example EBS gp3 8 and EFS 24 with scope {@code total}, from the storage model of the
     * cold-path work); on EFS stay below the NFS session slots of the mount minus a margin.
     */
    public static final Setting<Integer> PREFETCH_MAX_IN_FLIGHT_SETTING = new Setting<>(
        "bufferpool.prefetch.max_in_flight",
        s -> Integer.toString(Math.min(8, OpenSearchExecutors.allocatedProcessors(s))),
        s -> Setting.parseInt(s, 1, PrefetchScheduler.MAX_IN_FLIGHT_LIMIT, "bufferpool.prefetch.max_in_flight"),
        Property.NodeScope
    );

    /**
     * What {@code bufferpool.prefetch.max_in_flight} counts: {@code prefetch} (default) prefetch reads only, as before this
     * setting; {@code total} prefetch plus demand reads, so prefetch uses only the slots that demand reads leave free. Needs a
     * node restart.
     */
    public static final Setting<String> PREFETCH_BUDGET_SCOPE_SETTING = Setting.simpleString(
        "bufferpool.prefetch.budget_scope",
        "prefetch",
        value -> PrefetchScheduler.BudgetScope.parse("bufferpool.prefetch.budget_scope", value),
        Property.NodeScope
    );

    /**
     * The most prefetch items queued on this node while every prefetch slot is busy (0 to 65,536, default 1024). An item that
     * does not fit is dropped; 0 drops an item when no slot is free. Needs a node restart.
     */
    public static final Setting<Integer> PREFETCH_QUEUE_SIZE_SETTING = Setting.intSetting(
        "bufferpool.prefetch.queue_size",
        BlockCache.DEFAULT_QUEUE_SIZE,
        0,
        PrefetchScheduler.MAX_QUEUE_SIZE,
        Property.NodeScope
    );

    /**
     * Dispatch order of queued prefetch items: {@code fifo} (default) oldest first; {@code fair} round robin over the queries
     * with queued items, and when the queue is full the longest queue loses an item. Needs a node restart.
     */
    public static final Setting<String> PREFETCH_QUEUE_POLICY_SETTING = Setting.simpleString(
        "bufferpool.prefetch.queue_policy",
        "fifo",
        value -> PrefetchScheduler.QueuePolicy.parse("bufferpool.prefetch.queue_policy", value),
        Property.NodeScope
    );

    private final SetOnce<BlockCache> blockCache = new SetOnce<>();
    private final SetOnce<PrefetchScheduler> scheduler = new SetOnce<>();
    /** Registered on every {@value #STORE_TYPE} index; resolves the scheduler when a phase fails. */
    private final PrefetchTaskListener taskListener = new PrefetchTaskListener(scheduler::get);
    private final SetOnce<ClusterService> clusterService = new SetOnce<>();
    private final SetOnce<IndexNameExpressionResolver> indexNameExpressionResolver = new SetOnce<>();

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            CACHE_SIZE_SETTING,
            BLOCK_SIZE_SETTING,
            RANDOM_READ_SIZE_SETTING,
            SEQUENTIAL_READ_SIZE_SETTING,
            READ_HINT_SETTING,
            PREFETCH_TASK_PER_WINDOW_SETTING,
            PREFETCH_MAX_IN_FLIGHT_SETTING,
            PREFETCH_BUDGET_SCOPE_SETTING,
            PREFETCH_QUEUE_SIZE_SETTING,
            PREFETCH_QUEUE_POLICY_SETTING,
            SIMULATED_LOAD_LATENCY_SETTING
        );
    }

    /**
     * The prefetch executor: {@code bufferpool.prefetch.max_in_flight} threads, one per prefetch worker, and an executor queue
     * of as many worker runnables (a worker that released its slot still holds its thread until it returns, so up to twice
     * the budget of worker runnables exist at once). The items queue in the {@link PrefetchScheduler}, not here, so the
     * executor's own size settings are refused.
     */
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final String prefix = "thread_pool." + PREFETCH_THREAD_POOL;
        for (String key : List.of(prefix + ".size", prefix + ".queue_size")) {
            if (settings.hasValue(key)) {
                throw new IllegalArgumentException("[" + key + "] is not supported; set [" + PREFETCH_MAX_IN_FLIGHT_SETTING.getKey() + "]");
            }
        }
        final int maxInFlight = PREFETCH_MAX_IN_FLIGHT_SETTING.get(settings);
        return List.of(new FixedExecutorBuilder(settings, PREFETCH_THREAD_POOL, maxInFlight, maxInFlight, prefix));
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        final Settings settings = environment.settings();
        final long maxBytes = CACHE_SIZE_SETTING.get(settings).getBytes();
        final int maxInFlight = PREFETCH_MAX_IN_FLIGHT_SETTING.get(settings);
        final int poolSize = threadPool.info(PREFETCH_THREAD_POOL).getMax();
        if (poolSize != maxInFlight) {
            throw new IllegalStateException(
                "[" + PREFETCH_THREAD_POOL + "] has [" + poolSize + "] threads, expected [" + maxInFlight + "] (the prefetch budget)"
            );
        }
        final Executor executor = threadPool.executor(PREFETCH_THREAD_POOL);
        final ThreadContext threadContext = threadPool.getThreadContext();
        // executors capture the thread context at execute: stash it, so a worker never carries one query's context while it
        // serves other queries
        final Consumer<Runnable> workerStarter = worker -> {
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                executor.execute(worker);
            }
        };
        final PrefetchScheduler prefetchScheduler = createScheduler(settings, workerStarter, () -> {
            final Object taskId = threadContext.getTransient(TaskResourceTrackingService.TASK_ID);
            return taskListener.ownerOf(taskId instanceof Long id ? id : null);
        });
        scheduler.set(prefetchScheduler);
        final BlockCache cache = createBlockCache(settings, maxBytes, prefetchScheduler);
        ExperimentHooks.setPrefetchNodeBytes(cache.prefetchNodeBytes());
        cache.setSimulatedLoadLatencyNanos(SIMULATED_LOAD_LATENCY_SETTING.get(settings).nanos());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SIMULATED_LOAD_LATENCY_SETTING, latency -> cache.setSimulatedLoadLatencyNanos(latency.nanos()));
        cache.setPrefetchTaskPerWindow(PREFETCH_TASK_PER_WINDOW_SETTING.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(PREFETCH_TASK_PER_WINDOW_SETTING, cache::setPrefetchTaskPerWindow);
        logger.info(
            "block cache: size [{}], block [{}], random read [{}], sequential read [{}], read hint [{}], prefetch task per window [{}], "
                + "prefetch max in flight [{}], budget scope [{}], queue size [{}], queue policy [{}]",
            new ByteSizeValue(maxBytes),
            new ByteSizeValue(cache.blockSize()),
            new ByteSizeValue(cache.randomReadSize()),
            new ByteSizeValue(cache.sequentialReadSize()),
            cache.readHints().mode(),
            cache.prefetchTaskPerWindow(),
            prefetchScheduler.maxInFlight(),
            prefetchScheduler.scope().value(),
            prefetchScheduler.queueSize(),
            prefetchScheduler.policy().value()
        );
        blockCache.set(cache);
        this.clusterService.set(clusterService);
        this.indexNameExpressionResolver.set(indexNameExpressionResolver);
        return Collections.emptyList();
    }

    /**
     * Creates the block cache from the node settings; fails if the block or read sizes are invalid, or if read hints are
     * required and not available.
     */
    static BlockCache createBlockCache(Settings settings, long maxBytes, Executor prefetchExecutor) {
        // today's defaults (budget 8, scope prefetch, queue 1024, fifo), whatever the prefetch settings say
        return createBlockCache(settings, maxBytes, BlockCache.defaultScheduler(prefetchExecutor));
    }

    static BlockCache createBlockCache(Settings settings, long maxBytes, PrefetchScheduler scheduler) {
        final int blockSize = Math.toIntExact(BLOCK_SIZE_SETTING.get(settings).getBytes());
        final int randomReadSize = Math.toIntExact(RANDOM_READ_SIZE_SETTING.get(settings).getBytes());
        final int sequentialReadSize = Math.toIntExact(SEQUENTIAL_READ_SIZE_SETTING.get(settings).getBytes());
        final NativeReadHints readHints = NativeReadHints.create(NativeReadHints.Mode.parse(READ_HINT_SETTING.get(settings)));
        return new BlockCache(maxBytes, blockSize, randomReadSize, sequentialReadSize, readHints, scheduler);
    }

    /**
     * Creates the prefetch scheduler from the node settings; fails if a prefetch setting is invalid.
     *
     * @param workerStarter        starts one prefetch worker on the prefetch executor
     * @param ownerOfCurrentThread who submits a prefetch from the current thread
     */
    static PrefetchScheduler createScheduler(
        Settings settings,
        Consumer<Runnable> workerStarter,
        Supplier<PrefetchScheduler.PrefetchOwner> ownerOfCurrentThread
    ) {
        return new PrefetchScheduler(
            workerStarter,
            PREFETCH_MAX_IN_FLIGHT_SETTING.get(settings),
            PrefetchScheduler.BudgetScope.parse(PREFETCH_BUDGET_SCOPE_SETTING.getKey(), PREFETCH_BUDGET_SCOPE_SETTING.get(settings)),
            PREFETCH_QUEUE_SIZE_SETTING.get(settings),
            PrefetchScheduler.QueuePolicy.parse(PREFETCH_QUEUE_POLICY_SETTING.getKey(), PREFETCH_QUEUE_POLICY_SETTING.get(settings)),
            ownerOfCurrentThread
        );
    }

    /** Registers the prefetch cancellation listener on {@value #STORE_TYPE} indices; other indices pay nothing. */
    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (STORE_TYPE.equals(IndexModule.INDEX_STORE_TYPE_SETTING.get(indexModule.getSettings()))) {
            indexModule.addSearchOperationListener(taskListener);
        }
    }

    PrefetchTaskListener taskListener() {
        return taskListener;
    }

    /** The node's prefetch scheduler, or null before {@link #createComponents}. */
    PrefetchScheduler scheduler() {
        return scheduler.get();
    }

    /** The node's block cache, or null before {@link #createComponents}. */
    BlockCache blockCache() {
        return blockCache.get();
    }

    /** Closes the prefetch scheduler if the node created it: queued items are dropped, running reads finish. */
    @Override
    public void close() {
        final PrefetchScheduler s = scheduler.get();
        if (s != null) {
            s.close();
        }
    }

    /**
     * For {@value #STORE_TYPE} indices, the codec service of {@link ExperimentHooks#codecServiceFactory()}: in the
     * proof-of-concept build every codec ({@code index.codec}, any mode) lets each field choose its postings format and its
     * points format through the {@code meta.postings_format} and {@code meta.points_format} mapping entries; the build for
     * stock OpenSearch has none, so the index uses the codec service of OpenSearch.
     *
     * @param indexSettings settings of the index the codec is for
     */
    @Override
    public Optional<CodecServiceFactory> getCustomCodecServiceFactory(IndexSettings indexSettings) {
        if (STORE_TYPE.equals(indexSettings.getValue(IndexModule.INDEX_STORE_TYPE_SETTING)) == false) {
            return Optional.empty();
        }
        return ExperimentHooks.codecServiceFactory();
    }

    /**
     * The action filters of {@link ExperimentHooks#actionFilters}: in the proof-of-concept build, the check that refuses a
     * {@code meta.postings_format} or {@code meta.points_format} name that is not available in the mapping of a
     * {@value #STORE_TYPE} index; none in the build for stock OpenSearch.
     */
    @Override
    public List<ActionFilter> getActionFilters() {
        return ExperimentHooks.actionFilters(() -> {
            final ClusterService service = clusterService.get();
            return service == null ? null : service.state();
        }, indexNameExpressionResolver::get);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(
            new RestBufferPoolStatsAction(blockCache::get, taskListener::registeredTasks),
            new RestBufferPoolTraceAction(blockCache::get)
        );
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        // the node asks for factories before createComponents runs, so the factory resolves the cache on first use
        return Map.of(STORE_TYPE, new BufferPoolDirectoryFactory(blockCache::get));
    }

    /** Creates a {@link BufferPoolDirectory} per shard. */
    static final class BufferPoolDirectoryFactory extends FsDirectoryFactory {
        private final Supplier<BlockCache> blockCache;

        BufferPoolDirectoryFactory(Supplier<BlockCache> blockCache) {
            this.blockCache = blockCache;
        }

        @Override
        public Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
            final BlockCache cache = blockCache.get();
            if (cache == null) {
                throw new IllegalStateException("[" + STORE_TYPE + "] block cache is not initialized");
            }
            return new BufferPoolDirectory(location, lockFactory, cache);
        }
    }
}
