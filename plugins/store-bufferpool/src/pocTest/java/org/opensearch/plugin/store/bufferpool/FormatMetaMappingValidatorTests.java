/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingAction;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicReference;

public class FormatMetaMappingValidatorTests extends OpenSearchTestCase {
    private static final String GOOD = "{\"properties\":{"
        + "\"ts_split\":{\"type\":\"date\",\"meta\":{\"points_format\":\"Lucene90Split\"}},"
        + "\"kw_nav\":{\"type\":\"keyword\",\"meta\":{\"postings_format\":\"Lucene104Nav\"}},"
        + "\"kw\":{\"type\":\"keyword\",\"meta\":{\"owner\":\"team\"}}}}";

    private static IllegalArgumentException refused(String mapping) {
        return expectThrows(IllegalArgumentException.class, () -> FormatMetaMappingValidator.validate("idx", mapping));
    }

    public void testAvailableNamesAndOtherMetaPass() {
        FormatMetaMappingValidator.validate("idx", GOOD);
        FormatMetaMappingValidator.validate("idx", "{\"_doc\":" + GOOD + "}");
        FormatMetaMappingValidator.validate("idx", "{}");
        FormatMetaMappingValidator.validate("idx", "");
        FormatMetaMappingValidator.validate("idx", null);
    }

    public void testUnknownPostingsFormatIsRefused() {
        final IllegalArgumentException e = refused(
            "{\"_doc\":{\"properties\":{\"kw_nav\":{\"type\":\"keyword\",\"meta\":{\"postings_format\":\"Lucene104Navv\"}}}}}"
        );
        assertTrue(e.getMessage(), e.getMessage().contains("index [idx] field [kw_nav]: [meta.postings_format] [Lucene104Navv]"));
        assertTrue(e.getMessage(), e.getMessage().contains("Lucene104Nav"));
    }

    public void testUnknownPointsFormatIsRefused() {
        final IllegalArgumentException e = refused(
            "{\"properties\":{\"ts\":{\"type\":\"date\",\"meta\":{\"points_format\":\"Lucene90Splitt\"}}}}"
        );
        assertTrue(e.getMessage(), e.getMessage().contains("field [ts]: [meta.points_format] [Lucene90Splitt]"));
        assertTrue(e.getMessage(), e.getMessage().contains("available: [Lucene90Split]"));
    }

    public void testObjectPropertiesMultiFieldsAndDynamicTemplatesAreChecked() {
        IllegalArgumentException e = refused(
            "{\"properties\":{\"a\":{\"properties\":{\"b\":{\"type\":\"long\",\"meta\":{\"points_format\":\"x\"}}}}}}"
        );
        assertTrue(e.getMessage(), e.getMessage().contains("field [a.b]"));
        e = refused(
            "{\"properties\":{\"t\":{\"type\":\"text\",\"fields\":{\"raw\":{\"type\":\"keyword\",\"meta\":{\"postings_format\":\"x\"}}}}}}"
        );
        assertTrue(e.getMessage(), e.getMessage().contains("field [t.raw]"));
        e = refused(
            "{\"dynamic_templates\":[{\"strings\":{\"match_mapping_type\":\"string\","
                + "\"mapping\":{\"type\":\"keyword\",\"meta\":{\"postings_format\":\"x\"}}}}]}"
        );
        assertTrue(e.getMessage(), e.getMessage().contains("field [dynamic template [strings]]"));
    }

    private static ClusterState state(String index, String storeType) {
        final Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        if (storeType != null) {
            settings.put("index.store.type", storeType);
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(IndexMetadata.builder(index).settings(settings).build(), false))
            .build();
    }

    /** Runs the filter; returns the failure, or null when the request proceeds. */
    private static Exception apply(FormatMetaMappingValidator filter, String action, ActionRequest request) {
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final boolean proceed = filter.apply(action, request, ActionListener.wrap(r -> fail("no response expected"), failure::set));
        assertEquals(proceed, failure.get() == null);
        return failure.get();
    }

    private static final String BAD = "{\"properties\":{\"kw_nav\":{\"type\":\"keyword\",\"meta\":{\"postings_format\":\"Nope\"}}}}";

    public void testCreateIndexIsCheckedOnlyForTheStoreType() {
        final FormatMetaMappingValidator filter = new FormatMetaMappingValidator(() -> null, () -> null);
        final CreateIndexRequest bufferpool = new CreateIndexRequest("a").settings(
            Settings.builder().put(randomFrom("index.store.type", "store.type"), BufferPoolStorePlugin.STORE_TYPE)
        ).mapping(BAD);
        assertTrue(apply(filter, CreateIndexAction.NAME, bufferpool) instanceof IllegalArgumentException);
        final CreateIndexRequest good = new CreateIndexRequest("a").settings(
            Settings.builder().put("index.store.type", BufferPoolStorePlugin.STORE_TYPE)
        ).mapping(GOOD);
        assertNull(apply(filter, CreateIndexAction.NAME, good));
        // other store types: the entries are plain field metadata
        final CreateIndexRequest other = new CreateIndexRequest("a").settings(Settings.builder().put("index.store.type", "mmapfs"))
            .mapping(BAD);
        assertNull(apply(filter, CreateIndexAction.NAME, other));
        assertNull(apply(filter, CreateIndexAction.NAME, new CreateIndexRequest("a").mapping(BAD)));
    }

    public void testPutMappingIsCheckedOnlyForBufferPoolIndices() {
        final IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        final ClusterState bufferpool = state("bp", BufferPoolStorePlugin.STORE_TYPE);
        FormatMetaMappingValidator filter = new FormatMetaMappingValidator(() -> bufferpool, () -> resolver);
        assertTrue(
            apply(
                filter,
                PutMappingAction.NAME,
                new PutMappingRequest("bp").source(BAD, MediaTypeRegistry.JSON)
            ) instanceof IllegalArgumentException
        );
        assertNull(apply(filter, PutMappingAction.NAME, new PutMappingRequest("bp").source(GOOD, MediaTypeRegistry.JSON)));
        // an index that does not exist: the action reports it
        assertNull(apply(filter, PutMappingAction.NAME, new PutMappingRequest("missing").source(BAD, MediaTypeRegistry.JSON)));
        final ClusterState other = state("plain", null);
        filter = new FormatMetaMappingValidator(() -> other, () -> resolver);
        assertNull(apply(filter, PutMappingAction.NAME, new PutMappingRequest("plain").source(BAD, MediaTypeRegistry.JSON)));
        // other actions pass through
        assertNull(apply(filter, "indices:data/write/index", new PutMappingRequest("bp").source(BAD, MediaTypeRegistry.JSON)));
    }
}
