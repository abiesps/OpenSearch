/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.fielddata.ordinals;

import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link SortedSetDocValues} implementation that returns ordinals that are global.
 *
 * @opensearch.internal
 */
final class GlobalOrdinalMapping extends SortedSetDocValues {

    private final SortedSetDocValues values;
    private final OrdinalMap ordinalMap;
    private final LongValues mapping;
    private final TermsEnum[] lookups;
    private final SortedSetDocValues[] segmentDocValues;
    private int docValueCount = 0;
    private int nextOrd = 0;

    GlobalOrdinalMapping(OrdinalMap ordinalMap, SortedSetDocValues values, TermsEnum[] lookups, int segmentIndex) {
        this(ordinalMap, values, lookups, null, segmentIndex);
    }

    GlobalOrdinalMapping(
        OrdinalMap ordinalMap,
        SortedSetDocValues values,
        TermsEnum[] lookups,
        SortedSetDocValues[] segmentDocValues,
        int segmentIndex
    ) {
        super();
        this.values = values;
        this.lookups = lookups;
        this.segmentDocValues = segmentDocValues;
        this.ordinalMap = ordinalMap;
        this.mapping = ordinalMap.getGlobalOrds(segmentIndex);
    }

    @Override
    public long getValueCount() {
        return ordinalMap.getValueCount();
    }

    public long getGlobalOrd(long segmentOrd) {
        return mapping.get(segmentOrd);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        nextOrd = 0; /* reset next ordinal */
        docValueCount = 0; /* reset docValueCount */
        if (values.advanceExact(target)) {
            // Some SortedSetDocValues implementations like MultiOrdinals#MultiDocs do change
            // docValueCount() return value after each nextOrd() call, so we prefetch the value
            // here.
            docValueCount = values.docValueCount();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public long nextOrd() throws IOException {
        if (++nextOrd > docValueCount) {
            return SortedSetDocValues.NO_MORE_DOCS;
        }
        long segmentOrd = values.nextOrd();
        if (segmentOrd == SortedSetDocValues.NO_MORE_DOCS) {
            return SortedSetDocValues.NO_MORE_DOCS;
        } else {
            return getGlobalOrd(segmentOrd);
        }
    }

    @Override
    public BytesRef lookupOrd(long globalOrd) throws IOException {
        final long segmentOrd = ordinalMap.getFirstSegmentOrd(globalOrd);
        int readerIndex = ordinalMap.getFirstSegmentNumber(globalOrd);
        lookups[readerIndex].seekExact(segmentOrd);
        return lookups[readerIndex].term();
    }

    @Override
    public int docID() {
        return values.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return values.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        return values.advance(target);
    }

    @Override
    public long cost() {
        return values.cost();
    }

    @Override
    public int docValueCount() {
        return values.docValueCount();
    }

    @Override
    public void prefetchOrdinals(long[] globalOrds, int count) throws IOException {
        if (segmentDocValues == null || count == 0) {
            return;
        }

        // Group global ordinals by segment
        Map<Integer, List<Long>> segmentOrdinals = new HashMap<>();
        for (int i = 0; i < count; i++) {
            long globalOrd = globalOrds[i];
            int segmentIndex = ordinalMap.getFirstSegmentNumber(globalOrd);
            long segmentOrd = ordinalMap.getFirstSegmentOrd(globalOrd);
            segmentOrdinals.computeIfAbsent(segmentIndex, k -> new ArrayList<>()).add(segmentOrd);
        }

        // Call prefetchOrdinals on each segment's SortedSetDocValues
        for (Map.Entry<Integer, List<Long>> entry : segmentOrdinals.entrySet()) {
            int segIdx = entry.getKey();
            List<Long> ords = entry.getValue();
            if (segIdx < segmentDocValues.length && segmentDocValues[segIdx] != null) {
                long[] ordArray = new long[ords.size()];
                for (int i = 0; i < ords.size(); i++) {
                    ordArray[i] = ords.get(i);
                }
                segmentDocValues[segIdx].prefetchOrdinals(ordArray, ordArray.length);
            }
        }
    }

    /**
     * Prefetches the term dictionary block for the given global ordinal.
     * Maps the global ordinal to a segment ordinal and calls prepareSeekExact
     * on the per-segment TermsEnum (which is a TermsDict instance from Lucene90DocValuesProducer).
     *
     * <p>This is the ordinal-based equivalent of TermsEnum.prepareSeekExact(BytesRef):
     * phase 1 issues async IO for the target LZ4 block, phase 2 (lookupOrd) reads warm data.
     */
    public void prepareLookupOrd(long globalOrd) throws IOException {
        final long segmentOrd = ordinalMap.getFirstSegmentOrd(globalOrd);
        int readerIndex = ordinalMap.getFirstSegmentNumber(globalOrd);
        // The lookups[] TermsEnum instances support prefetch via IndexInput.prefetch()
        // when they are TermsDict instances from Lucene90DocValuesProducer.
        // We call seekExact which will find the block warm if prepareLookupOrd was called
        // on the underlying segment doc values. For now, prefetch via the segment doc values.
        if (segmentDocValues != null
            && readerIndex < segmentDocValues.length
            && segmentDocValues[readerIndex] != null) {
            // Delegate to segment-level prefetch using prefetchOrdinals with a single ordinal
            long[] singleOrd = new long[] { segmentOrd };
            segmentDocValues[readerIndex].prefetchOrdinals(singleOrd, 1);
        }
    }
}
