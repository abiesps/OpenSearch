/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortAndFormats;

/**
 * K1: the range that every doc a query matches has on a field, from the query's required range clauses on it. The
 * sort comparator can clamp its competitive range to it ({@link SortField#setCompetitiveBounds}).
 *
 * @opensearch.internal
 */
public final class SortQueryBounds {

    private SortQueryBounds() {}

    /**
     * Returns {@code [min, max]}, the intersection of every 1-dimension 8-byte {@link PointRangeQuery} on {@code field}
     * that {@code query} requires (reached only through MUST and FILTER clauses), or null if it requires none.
     */
    public static long[] extract(Query query, String field) {
        final long[] bounds = { Long.MIN_VALUE, Long.MAX_VALUE };
        final boolean[] found = { false };
        query.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String f) {
                return field.equals(f);
            }

            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                return occur == BooleanClause.Occur.MUST || occur == BooleanClause.Occur.FILTER ? this : QueryVisitor.EMPTY_VISITOR;
            }

            @Override
            public void visitLeaf(Query leaf) {
                if (leaf instanceof PointRangeQuery range
                    && range.getField().equals(field)
                    && range.getNumDims() == 1
                    && range.getBytesPerDim() == Long.BYTES) {
                    bounds[0] = Math.max(bounds[0], LongPoint.decodeDimension(range.getLowerPoint(), 0));
                    bounds[1] = Math.min(bounds[1], LongPoint.decodeDimension(range.getUpperPoint(), 0));
                    found[0] = true;
                }
            }
        });
        return found[0] ? bounds : null;
    }

    /**
     * If the search sorts by one LONG sort on a {@code date} field with millisecond resolution and {@code query}
     * requires a range on it, sets the range as the sort field's competitive bounds. Returns whether it did.
     */
    static boolean apply(SearchContext searchContext, Query query) {
        final SortAndFormats sort = searchContext.sort();
        if (sort == null || sort.sort.getSort().length != 1) {
            return false;
        }
        final SortField sortField = sort.sort.getSort()[0];
        if (isLongSort(sortField) == false || sortField.getField() == null) {
            return false;
        }
        final MapperService mapperService = searchContext.mapperService();
        final MappedFieldType fieldType = mapperService == null ? null : mapperService.fieldType(sortField.getField());
        if (isMillisDate(fieldType) == false) {
            return false;
        }
        final long[] bounds = extract(query, sortField.getField());
        if (bounds == null) {
            return false;
        }
        sortField.setCompetitiveBounds(bounds[0], bounds[1]);
        return true;
    }

    private static boolean isLongSort(SortField sortField) {
        if (sortField instanceof SortedNumericSortField sorted) {
            return sorted.getNumericType() == SortField.Type.LONG;
        }
        return sortField.getType() == SortField.Type.LONG;
    }

    private static boolean isMillisDate(MappedFieldType fieldType) {
        return fieldType instanceof DateFieldMapper.DateFieldType dateType
            && dateType.resolution() == DateFieldMapper.Resolution.MILLISECONDS;
    }
}
