/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.DateRangeIncludingNowQuery;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.approximate.ApproximatePointRangeQuery;
import org.opensearch.search.approximate.ApproximateScoreQuery;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SortQueryBoundsTests extends OpenSearchTestCase {

    private static final String FIELD = "@timestamp";

    private static Query range(long lo, long hi) {
        return LongPoint.newRangeQuery(FIELD, lo, hi);
    }

    private static Query indexOrDv(long lo, long hi) {
        return new IndexOrDocValuesQuery(range(lo, hi), SortedNumericDocValuesField.newSlowRangeQuery(FIELD, lo, hi));
    }

    private static void assertBounds(long lo, long hi, Query query) {
        final long[] bounds = SortQueryBounds.extract(query, FIELD);
        assertNotNull(query.toString(), bounds);
        assertEquals(lo, bounds[0]);
        assertEquals(hi, bounds[1]);
    }

    public void testSingleRange() {
        assertBounds(10, 20, range(10, 20));
        assertNull(SortQueryBounds.extract(range(10, 20), "other"));
        assertNull(SortQueryBounds.extract(new MatchAllDocsQuery(), FIELD));
    }

    public void testBoolFilterAndMustRangesIntersect() {
        final Query q = new BooleanQuery.Builder().add(range(10, 100), BooleanClause.Occur.FILTER)
            .add(range(50, 200), BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term("service", "a")), BooleanClause.Occur.FILTER)
            .build();
        assertBounds(50, 100, q);
        assertBounds(50, 100, new ConstantScoreQuery(q));
        // nested bool
        assertBounds(
            60,
            100,
            new BooleanQuery.Builder().add(q, BooleanClause.Occur.FILTER).add(range(60, 1000), BooleanClause.Occur.FILTER).build()
        );
    }

    public void testShouldAndMustNotRangesIgnored() {
        final Query q = new BooleanQuery.Builder().add(range(10, 20), BooleanClause.Occur.SHOULD)
            .add(range(30, 40), BooleanClause.Occur.SHOULD)
            .build();
        assertNull(SortQueryBounds.extract(q, FIELD));
        final Query q2 = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER)
            .add(range(10, 20), BooleanClause.Occur.MUST_NOT)
            .build();
        assertNull(SortQueryBounds.extract(q2, FIELD));
        final Query q3 = new BooleanQuery.Builder().add(range(0, 100), BooleanClause.Occur.FILTER)
            .add(range(10, 20), BooleanClause.Occur.MUST_NOT)
            .add(range(30, 40), BooleanClause.Occur.SHOULD)
            .build();
        assertBounds(0, 100, q3);
    }

    public void testOtherPointShapesIgnored() {
        assertNull(SortQueryBounds.extract(IntPoint.newRangeQuery(FIELD, 1, 2), FIELD));
        assertNull(SortQueryBounds.extract(LongPoint.newRangeQuery(FIELD, new long[] { 1, 2 }, new long[] { 3, 4 }), FIELD));
    }

    public void testWrappersReachTheRange() {
        assertBounds(10, 20, indexOrDv(10, 20));
        assertBounds(10, 20, new DateRangeIncludingNowQuery(indexOrDv(10, 20)));
        final ApproximatePointRangeQuery approx = new ApproximatePointRangeQuery(
            FIELD,
            LongPoint.pack(10).bytes,
            LongPoint.pack(20).bytes,
            1,
            ApproximatePointRangeQuery.LONG_FORMAT
        );
        assertBounds(10, 20, new ApproximateScoreQuery(indexOrDv(10, 20), approx));
        assertBounds(
            10,
            20,
            new SkipperClusteredRangeQuery(FIELD, 10, 20, indexOrDv(10, 20), SortedNumericDocValuesField.newSlowRangeQuery(FIELD, 10, 20))
        );
        assertBounds(
            15,
            20,
            new BooleanQuery.Builder().add(new ApproximateScoreQuery(indexOrDv(10, 20), approx), BooleanClause.Occur.FILTER)
                .add(new DateRangeIncludingNowQuery(indexOrDv(15, 30)), BooleanClause.Occur.FILTER)
                .build()
        );
    }

    private static SearchContext context(Sort sort, MapperService mapperService) {
        final SearchContext ctx = mock(SearchContext.class);
        final DocValueFormat[] formats = new DocValueFormat[sort.getSort().length];
        java.util.Arrays.fill(formats, DocValueFormat.RAW);
        when(ctx.sort()).thenReturn(new SortAndFormats(sort, formats));
        when(ctx.mapperService()).thenReturn(mapperService);
        return ctx;
    }

    private static MapperService mapper(org.opensearch.index.mapper.MappedFieldType fieldType) {
        final MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType(FIELD)).thenReturn(fieldType);
        return mapperService;
    }

    public void testApply() {
        final Query q = indexOrDv(10, 20);
        final MapperService millis = mapper(new DateFieldMapper.DateFieldType(FIELD));
        final SortField sorted = new SortedNumericSortField(FIELD, SortField.Type.LONG, randomBoolean());
        assertTrue(SortQueryBounds.apply(context(new Sort(sorted), millis), q));
        assertTrue(SortQueryBounds.apply(context(new Sort(new SortField(FIELD, SortField.Type.LONG)), millis), q));
        // the bounds are not part of the sort field's identity
        assertEquals(new SortedNumericSortField(FIELD, SortField.Type.LONG, sorted.getReverse()), sorted);

        // no range, two sort fields, another numeric type, not a millis date, no mapping
        assertFalse(SortQueryBounds.apply(context(new Sort(sorted), millis), new MatchAllDocsQuery()));
        assertFalse(SortQueryBounds.apply(context(new Sort(sorted, new SortField("other", SortField.Type.LONG)), millis), q));
        assertFalse(SortQueryBounds.apply(context(new Sort(new SortedNumericSortField(FIELD, SortField.Type.INT)), millis), q));
        assertFalse(
            SortQueryBounds.apply(
                context(new Sort(sorted), mapper(new DateFieldMapper.DateFieldType(FIELD, DateFieldMapper.Resolution.NANOSECONDS))),
                q
            )
        );
        assertFalse(
            SortQueryBounds.apply(
                context(new Sort(sorted), mapper(new NumberFieldMapper.NumberFieldType(FIELD, NumberFieldMapper.NumberType.LONG))),
                q
            )
        );
        assertFalse(SortQueryBounds.apply(context(new Sort(sorted), mapper(null)), q));
        assertFalse(SortQueryBounds.apply(context(new Sort(sorted), null), q));
        final SearchContext noSort = mock(SearchContext.class);
        assertFalse(SortQueryBounds.apply(noSort, q));
    }
}
