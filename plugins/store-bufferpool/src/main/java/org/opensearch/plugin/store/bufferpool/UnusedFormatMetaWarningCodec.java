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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A codec of the index that {@link BufferPoolCodecService} cannot wrap with a selecting codec for postings, for points,
 * or for both (a codec whose postings format is not per field, a codec that is not a Lucene104 codec, or the codec of a
 * composite index). It writes and reads exactly as the wrapped codec and keeps its name; it only logs one WARN per
 * index, field and mapping key when a segment it writes holds a field whose mapping asks for a format through that key,
 * so a {@code meta.points_format} or {@code meta.postings_format} entry is never ignored silently, also when it is
 * added by a mapping update after the shard opened.
 */
final class UnusedFormatMetaWarningCodec extends FilterCodec {
    private final MapperService mapperService;
    private final Logger logger;
    private final String codecName;
    private final PointsFormat pointsFormat;
    private final PostingsFormat postingsFormat;
    private final Set<String> warned = ConcurrentHashMap.newKeySet();

    /**
     * @param checkPoints   whether the codec ignores {@link PointsFormatSelectingCodec#META_KEY}
     * @param checkPostings whether the codec ignores {@link PostingsFormatSelectingCodec#META_KEY}
     */
    UnusedFormatMetaWarningCodec(
        String codecName,
        Codec delegate,
        boolean checkPoints,
        boolean checkPostings,
        MapperService mapperService,
        Logger logger
    ) {
        super(delegate.getName(), delegate);
        this.codecName = codecName;
        this.mapperService = mapperService;
        this.logger = logger;
        this.pointsFormat = checkPoints ? new CheckingPointsFormat(delegate.pointsFormat()) : delegate.pointsFormat();
        this.postingsFormat = checkPostings ? new CheckingPostingsFormat(delegate.postingsFormat()) : delegate.postingsFormat();
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
            if (uses == false) {
                continue;
            }
            final MappedFieldType fieldType = mapperService.fieldType(fi.name);
            final String wanted = fieldType == null ? null : fieldType.meta().get(metaKey);
            if (wanted != null && warned.add(fi.name + "\u0000" + metaKey)) {
                logger.warn(
                    "index [{}] field [{}]: [{}] [{}] is not used: codec [{}] cannot choose that format per field, using its own",
                    mapperService.index().getName(),
                    fi.name,
                    metaKey,
                    wanted,
                    codecName
                );
            }
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
}
