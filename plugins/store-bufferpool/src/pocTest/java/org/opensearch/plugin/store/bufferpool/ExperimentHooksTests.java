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
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.bkd.BKDExperiments;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.search.aggregations.DocValuesPrefetch;
import org.opensearch.search.query.SortIoExperiments;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.opensearch.plugin.store.bufferpool.BufferPoolStorePlugin.BLOCK_SIZE_SETTING;
import static org.opensearch.plugin.store.bufferpool.BufferPoolStorePlugin.RANDOM_READ_SIZE_SETTING;
import static org.opensearch.plugin.store.bufferpool.BufferPoolStorePlugin.SEQUENTIAL_READ_SIZE_SETTING;

/** The proof-of-concept build's {@link ExperimentHooks}: what the build for stock OpenSearch leaves out. */
public class ExperimentHooksTests extends OpenSearchTestCase {

    public void testTarget() {
        assertEquals("poc", ExperimentHooks.target());
    }

    public void testPlannerNodeSizesComeFromTheSequentialReadSize() {
        try {
            final BlockCache cache = BufferPoolStorePlugin.createBlockCache(
                Settings.builder()
                    .put(BLOCK_SIZE_SETTING.getKey(), "8kb")
                    .put(RANDOM_READ_SIZE_SETTING.getKey(), "32kb")
                    .put(SEQUENTIAL_READ_SIZE_SETTING.getKey(), "256kb")
                    .build(),
                1 << 20,
                Runnable::run
            );
            ExperimentHooks.setPrefetchNodeBytes(cache.prefetchNodeBytes());
            assertEquals(262144, DocValuesPrefetch.nodeBytes());
            assertEquals(262144, TopKPrefetch.getNodeBytes());
            assertEquals(262144, BKDExperiments.getNodeBytes());
            assertEquals(262144, SortIoExperiments.sortPrefetchNodeBytes());
            // below the BKD planner's minimum node size: that planner uses its minimum
            ExperimentHooks.setPrefetchNodeBytes(1024);
            assertEquals(1024, DocValuesPrefetch.nodeBytes());
            assertEquals(BKDExperiments.MIN_NODE_BYTES, BKDExperiments.getNodeBytes());
        } finally {
            ExperimentHooks.setPrefetchNodeBytes(BlockCache.DEFAULT_BLOCK_SIZE);
        }
    }

    public void testCodecsAndMappingCheck() {
        assertTrue(ExperimentHooks.codecServiceFactory().isPresent());
        final List<?> filters = ExperimentHooks.actionFilters(() -> null, () -> null);
        assertEquals(1, filters.size());
        assertTrue(filters.get(0) instanceof FormatMetaMappingValidator);
    }

    public void testExperimentRoutes() {
        final List<String> routes = ExperimentHooks.routes().stream().map(r -> r.getMethod() + " " + r.getPath()).toList();
        assertEquals(
            List.of(
                "POST /_bufferpool/dual_nav/_mode",
                "POST /_bufferpool/disjunction_prefetch",
                "POST /_bufferpool/topk_prefetch",
                "POST /_bufferpool/agg_batch",
                "POST /_bufferpool/sort_opt",
                "GET /_bufferpool/sort_opt"
            ),
            routes
        );
        final List<String> all = new RestBufferPoolStatsAction(() -> null, () -> 0).routes()
            .stream()
            .map(Route::getPath)
            .distinct()
            .toList();
        assertEquals(
            List.of(
                "/_bufferpool/stats",
                "/_bufferpool/stats/_reset",
                "/_bufferpool/cache/_clear",
                "/_bufferpool/dual_nav/_mode",
                "/_bufferpool/disjunction_prefetch",
                "/_bufferpool/topk_prefetch",
                "/_bufferpool/agg_batch",
                "/_bufferpool/sort_opt"
            ),
            all
        );
    }

    /**
     * {@link BufferPoolIndexInput#isLoaded(long, long)} has no {@code @Override} (stock Lucene has no such method), so this
     * checks that it still overrides the fork's {@link RandomAccessInput#isLoaded(long, long)}: the call goes through the Lucene
     * type.
     */
    public void testRangedIsLoadedOverridesTheForkMethod() throws IOException {
        final BlockCache cache = new BlockCache(1 << 20, 1024, 1024, 1024, Runnable::run);
        try (Directory dir = new BufferPoolDirectory(createTempDir(), FSLockFactory.getDefault(), cache)) {
            try (IndexOutput out = dir.createOutput("f", IOContext.DEFAULT)) {
                out.writeBytes(new byte[4096], 4096);
            }
            try (IndexInput in = dir.openInput("f", IOContext.DEFAULT)) {
                final RandomAccessInput r = in.randomAccessSlice(0, in.length());
                assertEquals(Optional.of(false), r.isLoaded(0, 2048));
                in.readByte();
                assertEquals(Optional.of(true), r.isLoaded(0, 1024));
                assertEquals(Optional.of(false), r.isLoaded(0, 2048));
                assertEquals(Optional.of(true), r.isLoaded(0, 0));
            }
        }
    }
}
