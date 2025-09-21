/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.bkd.BKDConfig;

public final class BKDPrefetchPlanner {

    /** Tuning knobs */
    public static final int DEFAULT_MERGE_GAP_BYTES = 1 << 12;    // merge ranges if gap <= 4KB
    public static final int DEFAULT_MAX_WINDOW_BYTES = 1 << 20;   // cap a merged window at 1MB

    private static final Logger logger = LogManager.getLogger(BKDPrefetchPlanner.class);
    /** Describe a slice to prefetch */
    static final class Slice {
        long start;   // absolute file offset
        long length;  // length in bytes
        Slice(long s, long l) { this.start = s; this.length = l; }
        long end() { return start + length; }
    }


    public static void planAndPrefetch(
        IndexInput in,
        BKDConfig cfg,
        int pageSize,
        Collection<Long> docIdsFps,
        Collection<Long> docValuesFps,
        int mergeGapBytes,
        int maxWindowBytes,
        String name
    ) throws java.io.IOException {

        // 1) Build slices: for docIDs-only and for docValues.
        // We conservatively estimate the bytes we’ll read from each leaf block.
        //   Layout per leaf (simplified):
        //     [vInt count][docIDs chunk][commonPrefixes][(maybe min/max)][values...]
        //
        // readDocIDs(): needs [vInt count] + docIDs chunk.
        // readDocValues(): needs the whole block (we’ll be positioned right after docIDs).
        //
        // If ‘count’ is unknown ahead of time, we can upper bound with maxPointsInLeafNode().
        final int maxCount = cfg.maxPointsInLeafNode();
        final int packedBytesPerPoint = cfg.packedBytesLength(); // numDims * bytesPerDim (+ data dims)
        final int vIntMax = 5;

        // Estimators (cheap, conservative)
        // docIDs pessimistic: header + 4 bytes/id (worst case) + small packing overhead
        long docIdsLenWorst = vIntMax + (long)maxCount * Integer.BYTES + 8;
        // values pessimistic: commonPrefix + optional bounds + payload
        // Use a conservative “almost worst-case” multiplier to avoid prefetching the whole world.
        long valuesLenWorst = 64 /*prefix+bounds overhead-ish*/ + (long)maxCount * packedBytesPerPoint;

        // Build slices
        ArrayList<Slice> slices = new ArrayList<>(docIdsFps.size() + docValuesFps.size());

        // For docIDs-only leaves: prefetch just docIDs part
        for (Long fp : docIdsFps) {
            if (fp == null) continue;
            long start = fp;
            long len   = docIdsLenWorst;
            slices.add(new Slice(start, len));
        }
        // For docValues leaves: prefetch docIDs + values (single contiguous slice)
        for (Long fp : docValuesFps) {
            if (fp == null) continue;
            long start = fp;
            long len   = docIdsLenWorst + valuesLenWorst;
            slices.add(new Slice(start, len));
        }

        if (slices.isEmpty()) return;

        // 2) Sort by start offset
        slices.sort(Comparator.comparingLong(s -> s.start));

        // 3) Page-align and merge nearby slices into windows
        ArrayList<Slice> windows = new ArrayList<>();
        final long pageMask = ~(long)(pageSize - 1);

        // align first
        for (Slice s : slices) {
            long alignedStart = s.start & pageMask;
            long pad = s.start - alignedStart;
            long alignedLen = s.length + pad;
            // round length up to page
            long rem = alignedLen % pageSize;
            if (rem != 0) alignedLen += (pageSize - rem);
            windows.add(new Slice(alignedStart, alignedLen));
        }

        // merge pass
        ArrayList<Slice> merged = new ArrayList<>();
        Slice cur = windows.get(0);
        for (int i = 1; i < windows.size(); i++) {
            Slice nxt = windows.get(i);
            boolean smallGap = nxt.start - cur.end() <= Math.max(mergeGapBytes, pageSize);
            boolean fits = (nxt.end() - cur.start) <= maxWindowBytes;
            if (smallGap && fits) {
                // extend current
                long newLen = Math.max(cur.end(), nxt.end()) - cur.start;
                cur.length = newLen;
            } else {
                merged.add(cur);
                cur = nxt;
            }
        }
        merged.add(cur);
        logger.info("Leafs to be prefetched after merging {} for segment {}", merged, name);
        // 4) Issue prefetches: one syscall per merged window
        for (Slice w : merged) {
            // Your IndexInput has prefetch(offset,len). This should end up in a single madvise(WILLNEED)
            // (or equivalent) for this window inside IOInterceptingIndexInput.
            in.prefetch(w.start, w.length);
        }
    }

    // Convenience wrapper with sane defaults.
    public static void planAndPrefetch(
        IndexInput in,
        BKDConfig cfg,
        int pageSize,
        Collection<Long> docIdsFps,
        Collection<Long> docValuesFps,
        String name
    ) throws java.io.IOException {
        planAndPrefetch(in, cfg, pageSize, docIdsFps, docValuesFps,
            DEFAULT_MERGE_GAP_BYTES, DEFAULT_MAX_WINDOW_BYTES, name);
    }
}
