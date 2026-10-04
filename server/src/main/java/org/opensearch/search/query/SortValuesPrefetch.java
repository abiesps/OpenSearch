/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.Weight;
import org.opensearch.search.aggregations.DocValuesPrefetch;

import java.io.IOException;

/**
 * Experiment E ({@link SortIoExperiments#isSortPrefetch()}): prefetches the doc-values nodes the sort comparator will
 * read. Sits between the scorer and a top-field collector sorted on one numeric field. Each leaf passes docs straight
 * through while the nodes read are cached; at the first node that is not cached it buffers the scorer's matches
 * {@link SortIoExperiments#sortPrefetchDocs()} doc IDs ahead of collection, so the node of the next doc the comparator
 * reads is requested while the comparator works on the current node (look-ahead one node). Every requested node holds
 * the value of a doc that is delivered to the comparator, which reads it.
 *
 * @opensearch.internal
 */
public final class SortValuesPrefetch implements Collector {

    private final Collector in;
    private final SortField sortField;

    private SortValuesPrefetch(Collector in, SortField sortField) {
        this.in = in;
        this.sortField = sortField;
    }

    /**
     * Whether the prefetch can run for {@code sort}: exactly one field, a single-valued-capable LONG sort (a
     * {@link SortedNumericSortField} of type LONG or a {@link SortField} of type LONG).
     */
    public static boolean isEligibleSort(Sort sort) {
        if (sort == null || sort.getSort().length != 1) {
            return false;
        }
        final SortField field = sort.getSort()[0];
        if (field.getField() == null) {
            return false;
        }
        if (field instanceof SortedNumericSortField sorted) {
            return sorted.getNumericType() == SortField.Type.LONG;
        }
        return field.getClass() == SortField.class && field.getType() == SortField.Type.LONG;
    }

    /** Wraps {@code in}, a top-field collector sorted on {@code sort} ({@link #isEligibleSort} must hold). */
    public static Collector wrap(Collector in, Sort sort) {
        assert isEligibleSort(sort) : sort;
        return new SortValuesPrefetch(in, sort.getSort()[0]);
    }

    /** The wrapped collector. */
    public Collector getCollector() {
        return in;
    }

    @Override
    public ScoreMode scoreMode() {
        return in.scoreMode();
    }

    @Override
    public void setWeight(Weight weight) {
        in.setWeight(weight);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        final LeafCollector leaf = in.getLeafCollector(context);
        final Sort indexSort = context.reader().getMetaData().sort();
        if (indexSort != null && indexSort.getSort().length > 0 && indexSort.getSort()[0].equals(sortField)) {
            // the comparator stops reading values early on such leaves (TopFieldCollector.canEarlyTerminate)
            return leaf;
        }
        final FieldInfo info = context.reader().getFieldInfos().fieldInfo(sortField.getField());
        if (info == null || (info.getDocValuesType() != DocValuesType.NUMERIC && info.getDocValuesType() != DocValuesType.SORTED_NUMERIC)) {
            return leaf;
        }
        // a separate instance, used only for planning: it is never positioned, so the comparator's reads are unchanged
        final NumericDocValues values = DocValues.unwrapSingleton(DocValues.getSortedNumeric(context.reader(), sortField.getField()));
        if (values == null) {
            return leaf;
        }
        final long nodeBytes = SortIoExperiments.sortPrefetchNodeBytes();
        final DocValuesPrefetch.Field field = DocValuesPrefetch.of(values, nodeBytes);
        if (field == null) {
            return leaf;
        }
        final DocValuesPrefetch.RunAhead ra = DocValuesPrefetch.sortRunAhead(SortIoExperiments.sortPrefetchDocs());
        final DocValuesPrefetch.Planner planner = DocValuesPrefetch.sortPlanner(field, ra, DocValuesPrefetch.ALL_MATCHES, nodeBytes);
        return ra.wrapLeafCollector(leaf, planner);
    }

    @Override
    public String toString() {
        return "SortValuesPrefetch(" + in + ")";
    }
}
