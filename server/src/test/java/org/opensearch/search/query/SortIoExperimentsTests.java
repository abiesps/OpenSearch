/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

public class SortIoExperimentsTests extends OpenSearchTestCase {

    @After
    public void resetSwitches() {
        SortIoExperiments.resetForTest();
    }

    public void testDefaults() {
        SortIoExperiments.resetForTest();
        assertFalse(SortIoExperiments.isApproxSingle());
        assertFalse(SortIoExperiments.isApproxBool());
        assertFalse(SortIoExperiments.isSkipperRange());
        assertFalse(SortIoExperiments.isSortPrefetch());
        assertFalse(SortIoExperiments.isClamp());
        assertEquals(65536, SortIoExperiments.sortPrefetchDocs());
        assertEquals(131072L, SortIoExperiments.sortPrefetchNodeBytes());
    }

    public void testBooleansAndReset() {
        SortIoExperiments.setApproxSingle(true);
        SortIoExperiments.setApproxBool(true);
        SortIoExperiments.setSkipperRange(true);
        SortIoExperiments.setSortPrefetch(true);
        SortIoExperiments.setClamp(true);
        assertTrue(SortIoExperiments.isApproxSingle());
        assertTrue(SortIoExperiments.isApproxBool());
        assertTrue(SortIoExperiments.isSkipperRange());
        assertTrue(SortIoExperiments.isSortPrefetch());
        assertTrue(SortIoExperiments.isClamp());
        SortIoExperiments.resetForTest();
        assertFalse(SortIoExperiments.isApproxSingle());
        assertFalse(SortIoExperiments.isApproxBool());
        assertFalse(SortIoExperiments.isSkipperRange());
        assertFalse(SortIoExperiments.isSortPrefetch());
        assertFalse(SortIoExperiments.isClamp());
    }

    public void testSortPrefetchDocs() {
        final int docs = randomIntBetween(65536, 4194304);
        SortIoExperiments.setSortPrefetchDocs(docs);
        assertEquals(docs, SortIoExperiments.sortPrefetchDocs());
        for (int bad : new int[] { 0, -1, 65535, 4194305, Integer.MAX_VALUE }) {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> SortIoExperiments.setSortPrefetchDocs(bad)
            );
            assertTrue(e.getMessage(), e.getMessage().contains(Integer.toString(bad)));
        }
        assertEquals(docs, SortIoExperiments.sortPrefetchDocs());
    }

    public void testSortPrefetchNodeBytes() {
        final long bytes = 1L << randomIntBetween(0, 40);
        SortIoExperiments.setSortPrefetchNodeBytes(bytes);
        assertEquals(bytes, SortIoExperiments.sortPrefetchNodeBytes());
        for (long bad : new long[] { 0, -1, -131072, 3, 131071, 3L << 16 }) {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> SortIoExperiments.setSortPrefetchNodeBytes(bad)
            );
            assertTrue(e.getMessage(), e.getMessage().contains(Long.toString(bad)));
        }
        assertEquals(bytes, SortIoExperiments.sortPrefetchNodeBytes());
    }
}
