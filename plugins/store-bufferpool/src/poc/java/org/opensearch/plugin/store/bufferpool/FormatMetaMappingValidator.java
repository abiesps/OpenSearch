/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPointsFormat;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingAction;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexModule;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * Refuses a create-index or put-mapping request for a {@value BufferPoolStorePlugin#STORE_TYPE} index whose mapping
 * names a {@code meta.postings_format} that is not an available Lucene postings format, or a {@code meta.points_format}
 * other than {@value PerFieldPointsFormat#SPLIT_FORMAT_NAME}, with an {@link IllegalArgumentException} (HTTP 400), before
 * any document is written. It checks the fields, their sub-fields and object properties, and the mappings of dynamic
 * templates. The request decides: a create-index request is checked when its settings set the store type, a put-mapping
 * request when one of its target indices has it. Other indices are not checked, because for them the entries are plain
 * field metadata. Mappings and store types that only come from index templates are not seen here; for those the codecs
 * keep their own format at flush and log a WARN ({@link PostingsFormatSelectingCodec}, {@link PointsFormatSelectingCodec}).
 */
final class FormatMetaMappingValidator extends ActionFilter.Simple {
    private final Supplier<ClusterState> clusterState;
    private final Supplier<IndexNameExpressionResolver> resolver;

    /**
     * @param clusterState the node's cluster state, for the store type of a put-mapping request's indices; may return null
     *                     before the node started
     * @param resolver     resolves the indices of a put-mapping request; may return null before the node started
     */
    FormatMetaMappingValidator(Supplier<ClusterState> clusterState, Supplier<IndexNameExpressionResolver> resolver) {
        this.clusterState = clusterState;
        this.resolver = resolver;
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
        try {
            if (CreateIndexAction.NAME.equals(action) && request instanceof CreateIndexRequest create) {
                if (isBufferPool(create.settings())) {
                    validate(create.index(), create.mappings());
                }
            } else if (PutMappingAction.NAME.equals(action) && request instanceof PutMappingRequest put) {
                final String target = bufferPoolTarget(put);
                if (target != null) {
                    validate(target, put.source());
                }
            }
        } catch (IllegalArgumentException e) {
            listener.onFailure(e);
            return false;
        }
        return true;
    }

    private static boolean isBufferPool(Settings settings) {
        // the create-index service adds the "index." prefix to keys without it
        final String type = settings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), settings.get("store.type"));
        return BufferPoolStorePlugin.STORE_TYPE.equals(type);
    }

    /** The name of a {@code bufferpoolfs} index the request targets, or null. */
    private String bufferPoolTarget(PutMappingRequest request) {
        final ClusterState state = clusterState.get();
        final IndexNameExpressionResolver indexResolver = resolver.get();
        if (state == null || indexResolver == null) {
            return null;
        }
        final Index[] indices;
        if (request.getConcreteIndex() != null) {
            indices = new Index[] { request.getConcreteIndex() };
        } else {
            try {
                indices = indexResolver.concreteIndices(state, request);
            } catch (RuntimeException e) {
                // the action reports a target that does not resolve
                return null;
            }
        }
        for (Index index : indices) {
            final IndexMetadata metadata = state.metadata().index(index);
            if (metadata != null && isBufferPool(metadata.getSettings())) {
                return index.getName();
            }
        }
        return null;
    }

    /** Throws if {@code mapping} (JSON, with or without the {@code _doc} wrapper) names a format that is not available. */
    static void validate(String index, String mapping) {
        if (mapping == null || mapping.isBlank()) {
            return;
        }
        Map<String, Object> root = XContentHelper.convertToMap(MediaTypeRegistry.xContent(mapping).xContent(), mapping, false);
        if (root.size() == 1 && root.containsKey("properties") == false && root.containsKey("dynamic_templates") == false) {
            // the type wrapper, {"_doc": {...}}
            final Object inner = root.values().iterator().next();
            if (inner instanceof Map<?, ?> map) {
                root = asMap(map);
            }
        }
        checkMapping(index, "", root);
    }

    private static void checkMapping(String index, String prefix, Map<String, Object> mapping) {
        if (mapping.get("properties") instanceof Map<?, ?> properties) {
            checkProperties(index, prefix, asMap(properties));
        }
        if (mapping.get("dynamic_templates") instanceof List<?> templates) {
            for (Object template : templates) {
                if (template instanceof Map<?, ?> named) {
                    for (Map.Entry<?, ?> e : named.entrySet()) {
                        if (e.getValue() instanceof Map<?, ?> body && body.get("mapping") instanceof Map<?, ?> field) {
                            checkField(index, "dynamic template [" + e.getKey() + "]", asMap(field));
                        }
                    }
                }
            }
        }
    }

    private static void checkProperties(String index, String prefix, Map<String, Object> properties) {
        for (Map.Entry<String, Object> e : properties.entrySet()) {
            if (e.getValue() instanceof Map<?, ?> field) {
                checkField(index, prefix + e.getKey(), asMap(field));
            }
        }
    }

    private static void checkField(String index, String path, Map<String, Object> field) {
        if (field.get("meta") instanceof Map<?, ?> meta) {
            final Object postings = meta.get(PostingsFormatSelectingCodec.META_KEY);
            if (postings != null && PostingsFormat.availablePostingsFormats().contains(postings.toString()) == false) {
                throw refused(index, path, PostingsFormatSelectingCodec.META_KEY, postings, PostingsFormat.availablePostingsFormats());
            }
            final Object points = meta.get(PointsFormatSelectingCodec.META_KEY);
            if (points != null && PerFieldPointsFormat.SPLIT_FORMAT_NAME.equals(points.toString()) == false) {
                throw refused(index, path, PointsFormatSelectingCodec.META_KEY, points, Set.of(PerFieldPointsFormat.SPLIT_FORMAT_NAME));
            }
        }
        if (field.get("properties") instanceof Map<?, ?> properties) {
            checkProperties(index, path + ".", asMap(properties));
        }
        if (field.get("fields") instanceof Map<?, ?> fields) {
            checkProperties(index, path + ".", asMap(fields));
        }
    }

    private static IllegalArgumentException refused(String index, String path, String key, Object value, Set<String> available) {
        return new IllegalArgumentException(
            "index ["
                + index
                + "] field ["
                + path
                + "]: [meta."
                + key
                + "] ["
                + value
                + "] is not an available format on a ["
                + BufferPoolStorePlugin.STORE_TYPE
                + "] index; available: "
                + new TreeSet<>(available)
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Map<?, ?> map) {
        return (Map<String, Object>) map;
    }
}
