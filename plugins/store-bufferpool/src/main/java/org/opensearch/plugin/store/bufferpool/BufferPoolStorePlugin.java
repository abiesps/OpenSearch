/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.lucene.search.TopKPrefetch;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.bkd.BKDExperiments;
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
import org.opensearch.search.aggregations.DocValuesPrefetch;
import org.opensearch.search.query.SortIoExperiments;
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
import java.util.function.Supplier;

/**
 * Adds the {@value #STORE_TYPE} store type ({@code index.store.type: bufferpoolfs}). Shards of such indices read their
 * files through one node-wide, Caffeine-backed {@link BlockCache} of off-heap blocks instead of the OS page cache mappings
 * that {@code mmapfs} uses.
 */
public class BufferPoolStorePlugin extends Plugin implements IndexStorePlugin, EnginePlugin, ActionPlugin {

    /** Creates the plugin; the block cache is created when the node starts. */
    public BufferPoolStorePlugin() {}

    /** Value of {@code index.store.type} that selects {@link BufferPoolDirectory}. */
    public static final String STORE_TYPE = "bufferpoolfs";

    /** Thread pool that loads prefetched blocks. */
    public static final String PREFETCH_THREAD_POOL = "bufferpool_prefetch";

    /**
     * Upper bound of the total size of the cached blocks on this node, as bytes or as a percentage of the heap size. The
     * blocks are direct buffers, so this must stay below {@code -XX:MaxDirectMemorySize}, which OpenSearch sets to half of
     * the heap by default.
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

    private static final int PREFETCH_QUEUE_SIZE = 1024;

    private final SetOnce<BlockCache> blockCache = new SetOnce<>();

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            CACHE_SIZE_SETTING,
            BLOCK_SIZE_SETTING,
            RANDOM_READ_SIZE_SETTING,
            SEQUENTIAL_READ_SIZE_SETTING,
            SIMULATED_LOAD_LATENCY_SETTING
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final int threads = Math.min(8, OpenSearchExecutors.allocatedProcessors(settings));
        return List.of(
            new FixedExecutorBuilder(settings, PREFETCH_THREAD_POOL, threads, PREFETCH_QUEUE_SIZE, "thread_pool." + PREFETCH_THREAD_POOL)
        );
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
        final BlockCache cache = createBlockCache(settings, maxBytes, threadPool.executor(PREFETCH_THREAD_POOL));
        setPrefetchNodeBytes(cache.prefetchNodeBytes());
        cache.setSimulatedLoadLatencyNanos(SIMULATED_LOAD_LATENCY_SETTING.get(settings).nanos());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SIMULATED_LOAD_LATENCY_SETTING, latency -> cache.setSimulatedLoadLatencyNanos(latency.nanos()));
        blockCache.set(cache);
        return Collections.emptyList();
    }

    /** Creates the block cache from the node settings; fails if the block or read sizes are invalid. */
    static BlockCache createBlockCache(Settings settings, long maxBytes, Executor prefetchExecutor) {
        final int blockSize = Math.toIntExact(BLOCK_SIZE_SETTING.get(settings).getBytes());
        final int randomReadSize = Math.toIntExact(RANDOM_READ_SIZE_SETTING.get(settings).getBytes());
        final int sequentialReadSize = Math.toIntExact(SEQUENTIAL_READ_SIZE_SETTING.get(settings).getBytes());
        return new BlockCache(maxBytes, blockSize, randomReadSize, sequentialReadSize, prefetchExecutor);
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
     * For {@value #STORE_TYPE} indices, the default codec lets each field choose its postings format through the
     * {@code meta.postings_format} mapping entry, see {@link PostingsFormatSelectingCodec}.
     *
     * @param indexSettings settings of the index the codec is for
     */
    @Override
    public Optional<CodecServiceFactory> getCustomCodecServiceFactory(IndexSettings indexSettings) {
        if (STORE_TYPE.equals(indexSettings.getValue(IndexModule.INDEX_STORE_TYPE_SETTING)) == false) {
            return Optional.empty();
        }
        return Optional.of(BufferPoolCodecService::new);
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
        return List.of(new RestBufferPoolStatsAction(blockCache::get), new RestBufferPoolTraceAction(blockCache::get));
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
