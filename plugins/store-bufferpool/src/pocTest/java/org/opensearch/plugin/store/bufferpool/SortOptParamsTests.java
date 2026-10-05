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
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SortOptParamsTests extends OpenSearchTestCase {

    /** Every parameter set to its default, as the benchmark posts it for the stock variant. */
    private static final Map<String, String> DEFAULTS = Map.ofEntries(
        Map.entry("bkd_prefetch", "false"),
        Map.entry("bkd_chunks", "8"),
        Map.entry("whole_index", "false"),
        Map.entry("whole_index_bytes", "65536"),
        Map.entry("index_child_prefetch", "false"),
        Map.entry("approx_single", "false"),
        Map.entry("approx_bool", "false"),
        Map.entry("skipper_range", "false"),
        Map.entry("sort_prefetch", "false"),
        Map.entry("sort_docs", "65536"),
        Map.entry("clamp", "false"),
        Map.entry("sample_docs", "0"),
        Map.entry("skipper_mode", "off"),
        Map.entry("run_cap", "false")
    );

    @After
    public void resetSwitches() {
        SortOptParams.parse(DEFAULTS).apply(BKDExperiments.DEFAULT_NODE_BYTES);
        SortIoExperiments.resetForTest();
    }

    private static Map<String, String> allOn() {
        final Map<String, String> params = new HashMap<>();
        params.put("bkd_prefetch", "true");
        params.put("bkd_chunks", "16");
        params.put("whole_index", "true");
        params.put("whole_index_bytes", "1048576");
        params.put("index_child_prefetch", "true");
        params.put("approx_single", "true");
        params.put("approx_bool", "true");
        params.put("skipper_range", "true");
        params.put("sort_prefetch", "true");
        params.put("sort_docs", "524288");
        params.put("clamp", "true");
        params.put("sample_docs", "65536");
        params.put("skipper_mode", "first");
        params.put("run_cap", "true");
        return params;
    }

    public void testNamesCoverEveryParameter() {
        assertEquals(DEFAULTS.keySet(), allOn().keySet());
        assertEquals(DEFAULTS.keySet(), Set.copyOf(SortOptParams.NAMES));
        assertEquals(DEFAULTS.size(), SortOptParams.NAMES.size());
        assertTrue(SortOptParams.current().keySet().containsAll(SortOptParams.NAMES));
    }

    public void testDefaultsAreStock() {
        SortOptParams.parse(DEFAULTS).apply(131072);
        final Map<String, Object> current = SortOptParams.current();
        assertEquals(false, current.get("bkd_prefetch"));
        assertEquals(8, current.get("bkd_chunks"));
        assertEquals(65536L, current.get("whole_index_bytes"));
        assertEquals(65536, current.get("sort_docs"));
        assertEquals(0, current.get("sample_docs"));
        assertEquals("off", current.get("skipper_mode"));
        assertEquals(false, current.get("run_cap"));
        assertEquals(131072L, current.get("bkd_node_bytes"));
        assertEquals(131072L, current.get("sort_prefetch_node_bytes"));
    }

    public void testValidSetIsApplied() {
        SortOptParams.parse(allOn()).apply(1 << 16);
        assertTrue(BKDExperiments.isIntersectPrefetch());
        assertEquals(16, BKDExperiments.getPrefetchChunks());
        assertTrue(BKDExperiments.isWholeIndexPrefetch());
        assertEquals(1 << 20, BKDExperiments.getWholeIndexPrefetchBytes());
        assertTrue(BKDExperiments.isIndexChildPrefetch());
        assertEquals(1 << 16, BKDExperiments.getNodeBytes());
        assertTrue(SortIoExperiments.isApproxSingle());
        assertTrue(SortIoExperiments.isApproxBool());
        assertTrue(SortIoExperiments.isSkipperRange());
        assertTrue(SortIoExperiments.isSortPrefetch());
        assertEquals(524288, SortIoExperiments.sortPrefetchDocs());
        assertEquals(1 << 16, SortIoExperiments.sortPrefetchNodeBytes());
        assertTrue(SortIoExperiments.isClamp());
        assertEquals(65536, ComparatorExperiments.getSampleDocs());
        assertEquals(SkipperMode.FIRST, ComparatorExperiments.getSkipperMode());
        assertTrue(CollectExperiments.isCompetitiveRunCap());
    }

    public void testAbsentParametersAreUnchanged() {
        SortOptParams.parse(allOn()).apply(131072);
        SortOptParams.parse(Map.of("skipper_mode", "fallback", "sample_docs", "4096")).apply(131072);
        assertEquals(SkipperMode.FALLBACK, ComparatorExperiments.getSkipperMode());
        assertEquals(4096, ComparatorExperiments.getSampleDocs());
        assertTrue(BKDExperiments.isIntersectPrefetch());
        assertTrue(SortIoExperiments.isSortPrefetch());
        assertTrue(CollectExperiments.isCompetitiveRunCap());
        SortOptParams.parse(Map.of()).apply(131072);
        assertEquals(SkipperMode.FALLBACK, ComparatorExperiments.getSkipperMode());
    }

    public void testBadValueChangesNothing() {
        final String[][] bad = {
            { "bkd_prefetch", "maybe" },
            { "bkd_prefetch", "TRUE" },
            { "bkd_prefetch", "" },
            { "bkd_chunks", "0" },
            { "bkd_chunks", "65" },
            { "bkd_chunks", "eight" },
            { "whole_index", "1" },
            { "whole_index_bytes", "-1" },
            { "whole_index_bytes", "16777217" },
            { "index_child_prefetch", "yes" },
            { "approx_single", "no" },
            { "approx_bool", "on" },
            { "skipper_range", "t" },
            { "sort_prefetch", "False" },
            { "sort_docs", "65535" },
            { "sort_docs", "4194305" },
            { "sort_docs", "99999999999999999999" },
            { "clamp", "1" },
            { "sample_docs", "4095" },
            { "sample_docs", "1" },
            { "sample_docs", "16777217" },
            { "skipper_mode", "FIRST" },
            { "skipper_mode", "last" },
            { "run_cap", "0" } };
        final Map<String, Object> before = SortOptParams.current();
        for (String[] b : bad) {
            // a valid change of every other switch next to the bad one must not be applied either
            final Map<String, String> params = allOn();
            params.put(b[0], b[1]);
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SortOptParams.parse(params));
            assertTrue(e.getMessage(), e.getMessage().contains("[" + b[0] + "]"));
            assertTrue(e.getMessage(), e.getMessage().contains(b[1]));
            assertEquals(before, SortOptParams.current());
        }
    }
}
