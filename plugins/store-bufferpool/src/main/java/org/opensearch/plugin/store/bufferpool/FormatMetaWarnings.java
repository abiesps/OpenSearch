/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.opensearch.index.mapper.MapperService;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Deduplicates the WARN logs of the format-selecting codecs per index: a codec service is built per shard engine, so a
 * per-codec set would log the same warning once per shard and engine open. The keys are the index UUID, the field, the
 * mapping key and what makes the warning differ (the requested name, the codec name). Memory is bounded: at most
 * {@link #MAX_KEYS} keys are kept, the oldest are dropped first, so a dropped key can warn once more.
 */
final class FormatMetaWarnings {
    /** Keys kept; about 100 bytes each. */
    static final int MAX_KEYS = 10_000;

    private static final Map<String, Boolean> WARNED = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > MAX_KEYS;
        }
    });

    private FormatMetaWarnings() {}

    /** Whether this is the first warning for these parts of {@code mapperService}'s index (the caller then logs it). */
    static boolean first(MapperService mapperService, String... parts) {
        final StringBuilder key = new StringBuilder(mapperService.index().getUUID());
        for (String part : parts) {
            key.append('\u0000').append(part);
        }
        return WARNED.putIfAbsent(key.toString(), Boolean.TRUE) == null;
    }

    /** Keys currently kept, for tests. */
    static int size() {
        return WARNED.size();
    }
}
