/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.lucene.search.CollectExperiments;
import org.apache.lucene.search.comparators.ComparatorExperiments;
import org.apache.lucene.search.comparators.ComparatorExperiments.SkipperMode;
import org.apache.lucene.util.bkd.BKDExperiments;
import org.opensearch.search.query.SortIoExperiments;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The parameters of {@code POST /_bufferpool/sort_opt}: the cold-path sort experiment switches of the Lucene fork
 * ({@link BKDExperiments}, {@link ComparatorExperiments}, {@link CollectExperiments}) and of OpenSearch
 * ({@link SortIoExperiments}). Every parameter is optional; an absent one leaves its switch unchanged. All parameters are
 * parsed and checked before any switch changes, so a bad value changes nothing.
 */
final class SortOptParams {

    static final String BKD_PREFETCH = "bkd_prefetch";
    static final String BKD_CHUNKS = "bkd_chunks";
    static final String WHOLE_INDEX = "whole_index";
    static final String WHOLE_INDEX_BYTES = "whole_index_bytes";
    static final String INDEX_CHILD_PREFETCH = "index_child_prefetch";
    static final String APPROX_SINGLE = "approx_single";
    static final String APPROX_BOOL = "approx_bool";
    static final String SKIPPER_RANGE = "skipper_range";
    static final String SORT_PREFETCH = "sort_prefetch";
    static final String SORT_DOCS = "sort_docs";
    static final String CLAMP = "clamp";
    static final String SAMPLE_DOCS = "sample_docs";
    static final String SKIPPER_MODE = "skipper_mode";
    static final String RUN_CAP = "run_cap";

    /** Every parameter name, in the order of the GET response. */
    static final List<String> NAMES = List.of(
        BKD_PREFETCH,
        BKD_CHUNKS,
        WHOLE_INDEX,
        WHOLE_INDEX_BYTES,
        INDEX_CHILD_PREFETCH,
        APPROX_SINGLE,
        APPROX_BOOL,
        SKIPPER_RANGE,
        SORT_PREFETCH,
        SORT_DOCS,
        CLAMP,
        SAMPLE_DOCS,
        SKIPPER_MODE,
        RUN_CAP
    );

    private Boolean bkdPrefetch;
    private Integer bkdChunks;
    private Boolean wholeIndex;
    private Long wholeIndexBytes;
    private Boolean indexChildPrefetch;
    private Boolean approxSingle;
    private Boolean approxBool;
    private Boolean skipperRange;
    private Boolean sortPrefetch;
    private Integer sortDocs;
    private Boolean clamp;
    private Integer sampleDocs;
    private SkipperMode skipperMode;
    private Boolean runCap;

    private SortOptParams() {}

    /**
     * Parses and checks the parameters. Names not in {@link #NAMES} are ignored (the REST layer rejects them).
     *
     * @throws IllegalArgumentException naming the parameter if a value is malformed or out of range
     */
    static SortOptParams parse(Map<String, String> params) {
        final SortOptParams p = new SortOptParams();
        p.bkdPrefetch = parseBoolean(params, BKD_PREFETCH);
        p.bkdChunks = parseInt(params, BKD_CHUNKS, 1, BKDExperiments.MAX_PREFETCH_CHUNKS);
        p.wholeIndex = parseBoolean(params, WHOLE_INDEX);
        p.wholeIndexBytes = parseLong(params, WHOLE_INDEX_BYTES, 0, BKDExperiments.MAX_WHOLE_INDEX_PREFETCH_BYTES);
        p.indexChildPrefetch = parseBoolean(params, INDEX_CHILD_PREFETCH);
        p.approxSingle = parseBoolean(params, APPROX_SINGLE);
        p.approxBool = parseBoolean(params, APPROX_BOOL);
        p.skipperRange = parseBoolean(params, SKIPPER_RANGE);
        p.sortPrefetch = parseBoolean(params, SORT_PREFETCH);
        p.sortDocs = parseInt(params, SORT_DOCS, SortIoExperiments.MIN_SORT_PREFETCH_DOCS, SortIoExperiments.MAX_SORT_PREFETCH_DOCS);
        p.clamp = parseBoolean(params, CLAMP);
        p.sampleDocs = parseInt(params, SAMPLE_DOCS, 0, ComparatorExperiments.MAX_SAMPLE_DOCS);
        if (p.sampleDocs != null && p.sampleDocs != 0 && p.sampleDocs < ComparatorExperiments.MIN_SAMPLE_DOCS) {
            throw new IllegalArgumentException(
                "["
                    + SAMPLE_DOCS
                    + "] must be 0 or in "
                    + ComparatorExperiments.MIN_SAMPLE_DOCS
                    + ".."
                    + ComparatorExperiments.MAX_SAMPLE_DOCS
                    + ", got ["
                    + p.sampleDocs
                    + "]"
            );
        }
        final String mode = params.get(SKIPPER_MODE);
        if (mode != null) {
            switch (mode) {
                case "off" -> p.skipperMode = SkipperMode.OFF;
                case "fallback" -> p.skipperMode = SkipperMode.FALLBACK;
                case "first" -> p.skipperMode = SkipperMode.FIRST;
                default -> throw new IllegalArgumentException("[" + SKIPPER_MODE + "] must be off, fallback or first, got [" + mode + "]");
            }
        }
        p.runCap = parseBoolean(params, RUN_CAP);
        return p;
    }

    /**
     * Sets the switches of the parameters that were present, and the node size of both prefetches to {@code nodeBytes}
     * (the cache block size).
     */
    void apply(long nodeBytes) {
        BKDExperiments.setNodeBytes(nodeBytes);
        SortIoExperiments.setSortPrefetchNodeBytes(nodeBytes);
        if (bkdPrefetch != null) {
            BKDExperiments.setIntersectPrefetch(bkdPrefetch);
        }
        if (bkdChunks != null) {
            BKDExperiments.setPrefetchChunks(bkdChunks);
        }
        if (wholeIndex != null) {
            BKDExperiments.setWholeIndexPrefetch(wholeIndex);
        }
        if (wholeIndexBytes != null) {
            BKDExperiments.setWholeIndexPrefetchBytes(wholeIndexBytes);
        }
        if (indexChildPrefetch != null) {
            BKDExperiments.setIndexChildPrefetch(indexChildPrefetch);
        }
        if (approxSingle != null) {
            SortIoExperiments.setApproxSingle(approxSingle);
        }
        if (approxBool != null) {
            SortIoExperiments.setApproxBool(approxBool);
        }
        if (skipperRange != null) {
            SortIoExperiments.setSkipperRange(skipperRange);
        }
        if (sortPrefetch != null) {
            SortIoExperiments.setSortPrefetch(sortPrefetch);
        }
        if (sortDocs != null) {
            SortIoExperiments.setSortPrefetchDocs(sortDocs);
        }
        if (clamp != null) {
            SortIoExperiments.setClamp(clamp);
        }
        if (sampleDocs != null) {
            ComparatorExperiments.setSampleDocs(sampleDocs);
        }
        if (skipperMode != null) {
            ComparatorExperiments.setSkipperMode(skipperMode);
        }
        if (runCap != null) {
            CollectExperiments.setCompetitiveRunCap(runCap);
        }
    }

    /** The current value of every switch, keyed by parameter name, plus the node sizes of both prefetches. */
    static Map<String, Object> current() {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(BKD_PREFETCH, BKDExperiments.isIntersectPrefetch());
        values.put(BKD_CHUNKS, BKDExperiments.getPrefetchChunks());
        values.put(WHOLE_INDEX, BKDExperiments.isWholeIndexPrefetch());
        values.put(WHOLE_INDEX_BYTES, BKDExperiments.getWholeIndexPrefetchBytes());
        values.put(INDEX_CHILD_PREFETCH, BKDExperiments.isIndexChildPrefetch());
        values.put(APPROX_SINGLE, SortIoExperiments.isApproxSingle());
        values.put(APPROX_BOOL, SortIoExperiments.isApproxBool());
        values.put(SKIPPER_RANGE, SortIoExperiments.isSkipperRange());
        values.put(SORT_PREFETCH, SortIoExperiments.isSortPrefetch());
        values.put(SORT_DOCS, SortIoExperiments.sortPrefetchDocs());
        values.put(CLAMP, SortIoExperiments.isClamp());
        values.put(SAMPLE_DOCS, ComparatorExperiments.getSampleDocs());
        values.put(SKIPPER_MODE, ComparatorExperiments.getSkipperMode().name().toLowerCase(Locale.ROOT));
        values.put(RUN_CAP, CollectExperiments.isCompetitiveRunCap());
        values.put("bkd_node_bytes", BKDExperiments.getNodeBytes());
        values.put("sort_prefetch_node_bytes", SortIoExperiments.sortPrefetchNodeBytes());
        return values;
    }

    private static Boolean parseBoolean(Map<String, String> params, String name) {
        final String value = params.get(name);
        if (value == null) {
            return null;
        }
        return switch (value) {
            case "true" -> Boolean.TRUE;
            case "false" -> Boolean.FALSE;
            default -> throw new IllegalArgumentException("[" + name + "] must be true or false, got [" + value + "]");
        };
    }

    private static Integer parseInt(Map<String, String> params, String name, int min, int max) {
        final Long value = parseLong(params, name, min, max);
        return value == null ? null : Math.toIntExact(value);
    }

    private static Long parseLong(Map<String, String> params, String name, long min, long max) {
        final String value = params.get(name);
        if (value == null) {
            return null;
        }
        final long parsed;
        try {
            parsed = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("[" + name + "] must be an integer in " + min + ".." + max + ", got [" + value + "]");
        }
        if (parsed < min || parsed > max) {
            throw new IllegalArgumentException("[" + name + "] must be in " + min + ".." + max + ", got [" + value + "]");
        }
        return parsed;
    }
}
