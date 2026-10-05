/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The build for stock OpenSearch and stock Lucene ({@code -Dbufferpool.target=stock}): none of the parts that need the
 * proof-of-concept fork ({@code src/poc} has them). There are no prefetch planners to size, no per-field format codecs
 * (indices use the codec OpenSearch gives them, as with any other store type), no mapping check of format names and no
 * experiment endpoints. The store is the same as in the proof-of-concept build.
 */
final class ExperimentHooks {

    private ExperimentHooks() {}

    /**
     * Which OpenSearch and Lucene this build is for. A method, not a constant, so callers in {@code src/main} do not inline
     * it and their class files stay the same in both builds.
     */
    static String target() {
        return "stock";
    }

    /** Stock OpenSearch and Lucene have no prefetch planners: nothing to size. */
    static void setPrefetchNodeBytes(long nodeBytes) {}

    /** No codec service of its own: the index uses the codec service of OpenSearch. */
    static Optional<CodecServiceFactory> codecServiceFactory() {
        return Optional.empty();
    }

    /** No mapping check: without the per-field format codecs, {@code meta} entries are plain mapping metadata. */
    static List<ActionFilter> actionFilters(
        Supplier<ClusterState> clusterState,
        Supplier<IndexNameExpressionResolver> indexNameExpressionResolver
    ) {
        return List.of();
    }

    /** No experiment endpoints. */
    static List<Route> routes() {
        return List.of();
    }

    /** No experiment endpoint to handle: returns null and changes nothing. */
    static CheckedConsumer<RestChannel, Exception> prepareRequest(RestRequest request, BlockCache cache) {
        return null;
    }

    /** No experiment counters. */
    static void resetCounters() {}

    /** No experiment fields in {@code GET /_bufferpool/stats}. */
    static void writeStats(XContentBuilder builder) {}
}
