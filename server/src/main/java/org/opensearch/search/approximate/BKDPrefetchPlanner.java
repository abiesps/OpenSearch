/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import java.io.IOException;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.bkd.BKDConfig;

public final class BKDPrefetchPlanner {

    private static final Logger logger = LogManager.getLogger(BKDPrefetchPlanner.class);

    /** Split runs if gap between consecutive FPs exceeds this many bytes. */
    public static final long GAP_BREAK_BYTES = 4L * 1024L; // 4KB

    /** Describe a contiguous prefetch slice [start, start+length) */
    static final class Slice {
        final long start;
        final long length;

        Slice(long start, long length) {
            this.start = start;
            this.length = length <= 0 ? 1 : length; // never 0 to satisfy prefetch()
        }

        long endExclusive() {
            return start + length;
        }

        @Override
        public String toString() {
            return "Slice{start=" + start + ", length=" + length + ", end=" + endExclusive() + "}";
        }
    }

    private BKDPrefetchPlanner() {}

    /**
     * Build contiguous runs from leaf FPs (docIDs + docValues) using GAP_BREAK_BYTES,
     * then prefetch one slice per run. Assumes that leaf blocks are laid out in
     * increasing FP order within the same file.
     */
    public static void planAndPrefetch(
        IndexInput in,
        BKDConfig cfg,                // kept for API symmetry; not used here
        Collection<Long> docIdsFps,
        Collection<Long> docValuesFps,
        String segmentName
    ) throws IOException {

        // 1) Collect all FPs (docIDs-only and docValues) into one sorted/unique list
        ArrayList<Long> fps = new ArrayList<>(
            (docIdsFps == null ? 0 : docIdsFps.size()) +
                (docValuesFps == null ? 0 : docValuesFps.size())
        );
        if (docIdsFps != null) {
            for (Long fp : docIdsFps) if (fp != null) fps.add(fp);
        }
        if (docValuesFps != null) {
            for (Long fp : docValuesFps) if (fp != null) fps.add(fp);
        }
        if (fps.isEmpty()) {
            return;
        }

        // sort & unique
        fps.sort(Long::compare);
        ArrayList<Long> uniq = new ArrayList<>(fps.size());
        long last = Long.MIN_VALUE;
        for (long fp : fps) {
            if (fp != last) {
                uniq.add(fp);
                last = fp;
            }
        }
        if (uniq.size() == 1) {
            // single leaf: prefetch at least 1 byte
            long start = uniq.get(0);
            in.prefetch(start, 1);
           // logger.info("Prefetched 1 slice for {} -> [start={}, length=1]", segmentName, start);
            return;
        }

        // 2) Build contiguous runs based on the fixed 4KB gap-break
        ArrayList<Slice> slices = new ArrayList<>();
        long runStart = uniq.get(0);
        long prev = runStart;

        for (int i = 1; i < uniq.size(); i++) {
            long cur = uniq.get(i);
            long gap = cur - prev;

            if (gap <= GAP_BREAK_BYTES) {
                // still part of the same run
                prev = cur;
            } else {
                // close current run: [runStart .. prev], length is (prev - runStart)
                long length = Math.max(1L, prev - runStart);
                slices.add(new Slice(runStart, length));
                // start new run
                runStart = cur;
                prev = cur;
            }
        }
        // close the last run
        long lastLength = Math.max(1L, prev - runStart);
        slices.add(new Slice(runStart, lastLength));

        // 3) Prefetch once per slice
        for (Slice s : slices) {
            in.prefetch(s.start, s.length);
        }
       // logger.info("Prefetched {} slice(s) for {} -> {}", slices.size(), segmentName, slices);
    }
}
