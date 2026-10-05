/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/** The build for stock OpenSearch: {@link ExperimentHooks} adds nothing, the store and its endpoints stay. */
public class ExperimentHooksTests extends OpenSearchTestCase {

    public void testTarget() {
        assertEquals("stock", ExperimentHooks.target());
    }

    public void testNoCodecsNoMappingCheck() {
        assertTrue(ExperimentHooks.codecServiceFactory().isEmpty());
        assertEquals(List.of(), ExperimentHooks.actionFilters(() -> null, () -> null));
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), BufferPoolStorePlugin.STORE_TYPE)
            .build();
        final IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("idx").settings(settings).build(), Settings.EMPTY);
        final BufferPoolStorePlugin plugin = new BufferPoolStorePlugin();
        assertTrue(plugin.getCustomCodecServiceFactory(indexSettings).isEmpty());
        assertEquals(List.of(), plugin.getActionFilters());
    }

    public void testOnlyTheStoreEndpoints() {
        assertEquals(List.of(), ExperimentHooks.routes());
        assertNull(ExperimentHooks.prepareRequest(null, null));
        final List<String> routes = new RestBufferPoolStatsAction(() -> null, () -> 0).routes()
            .stream()
            .map(r -> r.getMethod() + " " + r.getPath())
            .toList();
        assertEquals(List.of("GET /_bufferpool/stats", "POST /_bufferpool/stats/_reset", "POST /_bufferpool/cache/_clear"), routes);
    }

    public void testPlannerNodeSizeIsANoOp() {
        ExperimentHooks.setPrefetchNodeBytes(131072);
        ExperimentHooks.resetCounters();
    }
}
