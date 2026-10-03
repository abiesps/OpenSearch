/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104SplitPointsCodec;
import org.apache.lucene.codecs.perfield.PerFieldPointsFormat;
import org.apache.lucene.index.FieldInfo;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The codec of {@link PostingsFormatSelectingCodec}, except that a one-dimensional points field can pick the split BKD
 * points format in its mapping:
 *
 * <pre>
 * "@timestamp_split": { "type": "date", "meta": { "points_format": "Lucene90Split" } }
 * </pre>
 *
 * <p>Fields without the entry keep the stock points format, in the stock points files. The segment records the choice
 * in the field attributes and the codec name {@value Lucene104SplitPointsCodec#NAME}, which Lucene SPI resolves to
 * {@link Lucene104SplitPointsCodec} when the segment is read, so reading does not need this class. A mapping that asks
 * for the split format where it cannot be used (index sort, unknown name, more than one dimension) gets the stock
 * format and one WARN log per index and field; it never fails a flush or a merge.
 */
final class PointsFormatSelectingCodec extends FilterCodec {
    /** Key of the {@code meta} mapping entry that names the points format. */
    static final String META_KEY = "points_format";

    private final MapperService mapperService;
    private final Logger logger;
    private final PointsFormat pointsFormat = new PerFieldPointsFormat() {
        @Override
        public PointsFormat getPointsFormatForField(FieldInfo field) {
            return choose(field);
        }
    };
    // (index, field) and (index, field, value) keys already warned about
    private final Set<String> warned = ConcurrentHashMap.newKeySet();

    PointsFormatSelectingCodec(Codec delegate, MapperService mapperService, Logger logger) {
        super(Lucene104SplitPointsCodec.NAME, delegate);
        this.mapperService = mapperService;
        this.logger = logger;
    }

    @Override
    public PointsFormat pointsFormat() {
        return pointsFormat;
    }

    PointsFormat choose(FieldInfo fi) {
        // a field in the segment but not in the mapping
        final MappedFieldType fieldType = mapperService.fieldType(fi.name);
        if (fieldType == null) {
            return PerFieldPointsFormat.STOCK;
        }
        final String name = fieldType.meta().get(META_KEY);
        if (name == null) {
            return PerFieldPointsFormat.STOCK;
        }
        final String index = mapperService.index().getName();
        if (mapperService.getIndexSettings().getIndexSortConfig().hasIndexSort()) {
            // index-sorted merges would take the split writer's heap path
            if (warned.add(index + "\u0000" + fi.name)) {
                logger.warn(
                    "index [{}] field [{}]: points format [{}] is not used on an index with an index sort, using the stock format",
                    index,
                    fi.name,
                    name
                );
            }
            return PerFieldPointsFormat.STOCK;
        }
        if (PerFieldPointsFormat.SPLIT_FORMAT_NAME.equals(name)
            && fi.getPointIndexDimensionCount() == 1
            && fi.getPointDimensionCount() == 1) {
            return PerFieldPointsFormat.SPLIT;
        }
        if (warned.add(index + "\u0000" + fi.name + "\u0000" + name)) {
            logger.warn(
                "index [{}] field [{}]: points format [{}] is unknown or does not support {} dimensions, using the stock format",
                index,
                fi.name,
                name,
                fi.getPointDimensionCount()
            );
        }
        return PerFieldPointsFormat.STOCK;
    }
}
