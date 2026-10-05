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
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;

/**
 * A codec of the index that {@link BufferPoolCodecService} cannot wrap with a selecting codec for postings, for points,
 * or for both (a codec whose postings format is not per field, a codec that is not a Lucene104 codec, or any codec of
 * an index with a composite (star-tree) field). It writes and reads exactly as the wrapped codec and keeps its name; it
 * only logs a WARN when a segment it writes holds a field whose mapping asks for a format through
 * {@code meta.points_format} or {@code meta.postings_format}, so such an entry is never ignored silently, also when it
 * is added by a mapping update after the shard opened. The WARN is logged once per index, codec name, field and mapping
 * key ({@link FormatMetaWarnings}), not once per shard.
 *
 * <p>When the wrapped codec's postings format is a {@link PerFieldPostingsFormat}, this codec's postings format is one
 * too, and it returns the wrapped format's choice for every field: outer wrappers that test for a per-field postings
 * format (OpenSearch's {@code CriteriaBasedCodec}, which writes the segment's bucket attribute through it) see the same
 * type as without this codec. The points format is wrapped in a plain {@link PointsFormat}, whose writer is the wrapped
 * format's writer.
 */
final class UnusedFormatMetaWarningCodec extends FilterCodec {
    private final MapperService mapperService;
    private final Logger logger;
    private final String codecName;
    private final boolean composite;
    private final PointsFormat pointsFormat;
    private final PostingsFormat postingsFormat;

    /**
     * @param checkPoints   whether the codec ignores {@link PointsFormatSelectingCodec#META_KEY}
     * @param checkPostings whether the codec ignores {@link PostingsFormatSelectingCodec#META_KEY}
     * @param composite     whether the reason is a composite (star-tree) field of the index rather than the codec itself
     */
    UnusedFormatMetaWarningCodec(
        String codecName,
        Codec delegate,
        boolean checkPoints,
        boolean checkPostings,
        boolean composite,
        MapperService mapperService,
        Logger logger
    ) {
        super(delegate.getName(), delegate);
        this.codecName = codecName;
        this.composite = composite;
        this.mapperService = mapperService;
        this.logger = logger;
        this.pointsFormat = checkPoints ? new CheckingPointsFormat(delegate.pointsFormat()) : delegate.pointsFormat();
        final PostingsFormat postings = delegate.postingsFormat();
        if (checkPostings == false) {
            this.postingsFormat = postings;
        } else if (postings instanceof PerFieldPostingsFormat perField) {
            this.postingsFormat = new CheckingPerFieldPostingsFormat(perField);
        } else {
            this.postingsFormat = new CheckingPostingsFormat(postings);
        }
    }

    @Override
    public PointsFormat pointsFormat() {
        return pointsFormat;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    private void check(FieldInfos fieldInfos, String metaKey, boolean points) {
        for (FieldInfo fi : fieldInfos) {
            final boolean uses = points ? fi.getPointDimensionCount() > 0 : fi.getIndexOptions() != IndexOptions.NONE;
            if (uses) {
                check(fi.name, metaKey);
            }
        }
    }

    private void check(String field, String metaKey) {
        final MappedFieldType fieldType = mapperService.fieldType(field);
        final String wanted = fieldType == null ? null : fieldType.meta().get(metaKey);
        if (wanted == null || FormatMetaWarnings.first(mapperService, "unused", codecName, field, metaKey) == false) {
            return;
        }
        if (composite) {
            logger.warn(
                "index [{}] field [{}]: [{}] [{}] is not used: the index has a composite (star-tree) field, so codec [{}] "
                    + "keeps its own formats",
                mapperService.index().getName(),
                field,
                metaKey,
                wanted,
                codecName
            );
        } else {
            logger.warn(
                "index [{}] field [{}]: [{}] [{}] is not used: codec [{}] cannot choose that format per field, using its own",
                mapperService.index().getName(),
                field,
                metaKey,
                wanted,
                codecName
            );
        }
    }

    private final class CheckingPointsFormat extends PointsFormat {
        private final PointsFormat in;

        CheckingPointsFormat(PointsFormat in) {
            this.in = in;
        }

        @Override
        public PointsWriter fieldsWriter(SegmentWriteState state) throws IOException {
            check(state.fieldInfos, PointsFormatSelectingCodec.META_KEY, true);
            return in.fieldsWriter(state);
        }

        @Override
        public PointsReader fieldsReader(SegmentReadState state) throws IOException {
            return in.fieldsReader(state);
        }
    }

    /** For a postings format that is not per field: checks every indexed field of the segment when it is written. */
    private final class CheckingPostingsFormat extends PostingsFormat {
        private final PostingsFormat in;

        CheckingPostingsFormat(PostingsFormat in) {
            // the top-level postings format of a codec is not recorded by name in the segment; the codec reads it
            super(in.getName());
            this.in = in;
        }

        @Override
        public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
            check(state.fieldInfos, PostingsFormatSelectingCodec.META_KEY, false);
            return in.fieldsConsumer(state);
        }

        @Override
        public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
            return in.fieldsProducer(state);
        }
    }

    /**
     * For a per-field postings format: the same choice per field, so the same files and per-field attributes, and the
     * same type for outer wrappers. Each field is checked when its postings are written (flush and merge).
     */
    private final class CheckingPerFieldPostingsFormat extends PerFieldPostingsFormat {
        private final PerFieldPostingsFormat in;

        CheckingPerFieldPostingsFormat(PerFieldPostingsFormat in) {
            this.in = in;
        }

        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            check(field, PostingsFormatSelectingCodec.META_KEY);
            return in.getPostingsFormatForField(field);
        }
    }
}
