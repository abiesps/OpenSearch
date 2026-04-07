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

package org.opensearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.LongArray;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.StringFieldType;
import org.opensearch.index.fielddata.ordinals.GlobalOrdinalMapping;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;

import java.io.IOException;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_DOCS;

/**
 * A {@link SingleDimensionValuesSource} for global ordinals.
 *
 * @opensearch.internal
 */
class GlobalOrdinalValuesSource extends SingleDimensionValuesSource<BytesRef> {
    private final CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc;
    private LongArray values;
    private SortedSetDocValues lookup;
    private long currentValue;
    private Long afterValueGlobalOrd;
    private boolean isTopValueInsertionPoint;

    private long lastLookupOrd = -1;
    private BytesRef lastLookupValue;

    GlobalOrdinalValuesSource(
        BigArrays bigArrays,
        MappedFieldType type,
        CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc,
        DocValueFormat format,
        boolean missingBucket,
        MissingOrder missingOrder,
        int size,
        int reverseMul
    ) {
        super(bigArrays, format, type, missingBucket, missingOrder, size, reverseMul);
        this.docValuesFunc = docValuesFunc;
        this.values = bigArrays.newLongArray(Math.min(size, 100), false);
    }

    @Override
    void copyCurrent(int slot) {
        values = bigArrays.grow(values, slot + 1);
        values.set(slot, currentValue);
    }

    @Override
    int compare(int from, int to) {
        if (missingBucket) {
            int result = missingOrder.compare(() -> values.get(from) == -1, () -> values.get(to) == -1, reverseMul);
            if (MissingOrder.unknownOrder(result) == false) {
                return result;
            }
        }
        return Long.compare(values.get(from), values.get(to)) * reverseMul;
    }

    @Override
    int compareCurrent(int slot) {
        if (missingBucket) {
            int result = missingOrder.compare(() -> currentValue == -1, () -> values.get(slot) == -1, reverseMul);
            if (MissingOrder.unknownOrder(result) == false) {
                return result;
            }
        }
        return Long.compare(currentValue, values.get(slot)) * reverseMul;
    }

    @Override
    int compareCurrentWithAfter() {
        if (missingBucket) {
            int result = missingOrder.compare(() -> currentValue == -1, () -> afterValueGlobalOrd == -1, reverseMul);
            if (MissingOrder.unknownOrder(result) == false) {
                return result;
            }
        }
        int cmp = Long.compare(currentValue, afterValueGlobalOrd);
        if (cmp == 0 && isTopValueInsertionPoint) {
            // the top value is missing in this shard, the comparison is against
            // the insertion point of the top value so equality means that the value
            // is "after" the insertion point.
            return reverseMul;
        }
        return cmp * reverseMul;
    }

    @Override
    int hashCode(int slot) {
        return Long.hashCode(values.get(slot));
    }

    @Override
    int hashCodeCurrent() {
        return Long.hashCode(currentValue);
    }

    @Override
    void setAfter(Comparable value) {
        if (missingBucket && value == null) {
            afterValue = null;
            afterValueGlobalOrd = -1L;
        } else if (value.getClass() == String.class) {
            afterValue = format.parseBytesRef(value.toString());
        } else {
            throw new IllegalArgumentException("invalid value, expected string, got " + value.getClass().getSimpleName());
        }
    }

    @Override
    BytesRef toComparable(int slot) throws IOException {
        long globalOrd = values.get(slot);
        if (missingBucket && globalOrd == -1) {
            return null;
        } else if (globalOrd == lastLookupOrd) {
            return lastLookupValue;
        } else {
            lastLookupOrd = globalOrd;
            lastLookupValue = BytesRef.deepCopyOf(lookup.lookupOrd(values.get(slot)));
            return lastLookupValue;
        }
    }

    @Override
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
        final SortedSetDocValues dvs = docValuesFunc.apply(context);
        if (lookup == null) {
            initLookup(dvs);
        }

        // unwrapSingleton() returns non-null only if the field is single-valued
        final SortedDocValues singleton = DocValues.unwrapSingleton(dvs);

        // Direct ordinal access for single-valued fields
        if (singleton != null) {
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (singleton.advanceExact(doc)) {
                        currentValue = singleton.ordValue();
                        next.collect(doc, bucket);
                    } else if (missingBucket) {
                        currentValue = -1;
                        next.collect(doc, bucket);
                    }
                }

                @Override
                public void collectBulk(int[] docs, int count) throws IOException {
                    // Bulk ordinal resolution with prefetch.
                    // Uses SortedDocValues.ordValues() which prefetches byte ranges
                    // before reading packed ordinal data from the doc values file.
                    int[] ords = new int[count];
                    singleton.ordValues(count, docs, ords, -1);
                    for (int i = 0; i < count; i++) {
                        if (ords[i] != -1) {
                            currentValue = ords[i];
                            next.collect(docs[i], 0);
                        } else if (missingBucket) {
                            currentValue = -1;
                            next.collect(docs[i], 0);
                        }
                    }
                }
            };
        }

        return new LeafBucketCollector() {
            // For GlobalOrdinalMapping with single-valued fields, unwrap the segment-level
            // SortedDocValues to access ordValues() with prefetch, then map to global ordinals.
            private final SortedDocValues segmentSorted = unwrapSegmentSortedDocValues(dvs);

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (dvs.advanceExact(doc)) {
                    long ord;
                    int count = dvs.docValueCount();
                    while ((count-- > 0) && (ord = dvs.nextOrd()) != NO_MORE_DOCS) {
                        currentValue = ord;
                        next.collect(doc, bucket);
                    }
                } else if (missingBucket) {
                    currentValue = -1;
                    next.collect(doc, bucket);
                }
            }

            @Override
            public void collectBulk(int[] docs, int count) throws IOException {
                if (segmentSorted != null && dvs instanceof GlobalOrdinalMapping) {
                    // Fast path: bulk read segment ordinals with prefetch, then map to global
                    GlobalOrdinalMapping globalMapping = (GlobalOrdinalMapping) dvs;
                    int[] segmentOrds = new int[count];
                    segmentSorted.ordValues(count, docs, segmentOrds, -1);
                    for (int i = 0; i < count; i++) {
                        if (segmentOrds[i] != -1) {
                            currentValue = globalMapping.getGlobalOrd(segmentOrds[i]);
                            next.collect(docs[i], 0);
                        } else if (missingBucket) {
                            currentValue = -1;
                            next.collect(docs[i], 0);
                        }
                    }
                } else {
                    // Fallback: per-doc collection
                    for (int i = 0; i < count; i++) {
                        collect(docs[i], 0);
                    }
                }
            }
        };
    }

    @Override
    LeafBucketCollector getLeafCollector(Comparable value, LeafReaderContext context, LeafBucketCollector next) throws IOException {
        if (value.getClass() != BytesRef.class) {
            throw new IllegalArgumentException("Expected BytesRef, got " + value.getClass());
        }
        BytesRef term = (BytesRef) value;
        final SortedSetDocValues dvs = docValuesFunc.apply(context);
        if (lookup == null) {
            initLookup(dvs);
        }
        return new LeafBucketCollector() {
            boolean currentValueIsSet = false;

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (currentValueIsSet == false) {
                    if (dvs.advanceExact(doc)) {
                        long ord;
                        int count = dvs.docValueCount();
                        while ((count-- > 0) && (ord = dvs.nextOrd()) != NO_MORE_DOCS) {
                            if (term.equals(lookup.lookupOrd(ord))) {
                                currentValueIsSet = true;
                                currentValue = ord;
                                break;
                            }
                        }
                    }
                }
                assert currentValueIsSet;
                next.collect(doc, bucket);
            }

            @Override
            public void collectBulk(int[] docs, int count) throws IOException {
                // For the lead source with forced value, the ordinal is the same for all docs
                // in this term's postings. Resolve it once on the first doc, then propagate
                // bulk to the next collector in the chain (the second source dimension).
                if (count == 0) return;
                // Ensure ordinal is resolved on first doc we see
                if (currentValueIsSet == false) {
                    collect(docs[0], 0);
                    if (count == 1) return;
                    // Propagate remaining docs in bulk — ordinal is now set
                    int[] remaining = new int[count - 1];
                    System.arraycopy(docs, 1, remaining, 0, count - 1);
                    next.collectBulk(remaining, count - 1);
                } else {
                    // Ordinal already resolved, propagate entire batch to next collector
                    next.collectBulk(docs, count);
                }
            }
        };
    }

    @Override
    SortedDocsProducer createSortedDocsProducerOrNull(IndexReader reader, Query query) {
        if (checkIfSortedDocsIsApplicable(reader, fieldType) == false
            || (fieldType == null || fieldType.unwrap() instanceof StringFieldType == false)
            || (query != null && query.getClass() != MatchAllDocsQuery.class)) {
            return null;
        }
        return new TermsSortedDocsProducer(fieldType.name());
    }

    @Override
    public void close() {
        Releasables.close(values);
    }

    /**
     * Unwrap the segment-level SortedDocValues from a SortedSetDocValues chain.
     * For GlobalOrdinalMapping wrapping a SingletonSortedSetDocValues, returns the
     * underlying SortedDocValues which has the prefetch-aware ordValues() override.
     * Returns null if the chain cannot be unwrapped.
     */
    private static SortedDocValues unwrapSegmentSortedDocValues(SortedSetDocValues dvs) {
        if (dvs instanceof GlobalOrdinalMapping) {
            SortedSetDocValues inner = ((GlobalOrdinalMapping) dvs).getSegmentValues();
            return DocValues.unwrapSingleton(inner);
        }
        return DocValues.unwrapSingleton(dvs);
    }

    private void initLookup(SortedSetDocValues dvs) throws IOException {
        lookup = dvs;
        if (afterValue != null && afterValueGlobalOrd == null) {
            afterValueGlobalOrd = lookup.lookupTerm(afterValue);
            if (afterValueGlobalOrd < 0) {
                // convert negative insert position
                afterValueGlobalOrd = -afterValueGlobalOrd - 1;
                isTopValueInsertionPoint = true;
            }
        }
    }
}
