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
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene104.Lucene104SplitPointsCodec;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.mapper.MapperService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Codec service of {@code bufferpoolfs} indices. Every codec name {@link CodecService} knows ({@code default} and
 * {@code lz4}, {@code best_compression} and {@code zlib}, {@code lucene_default}, the Lucene SPI names and the codecs of
 * other plugins) resolves to that same codec, wrapped so that the mapping can pick formats per field:
 * <ul>
 *   <li>{@link PostingsFormatSelectingCodec} ({@code meta.postings_format}) when the codec records postings formats per
 *       field;</li>
 *   <li>{@link PointsFormatSelectingCodec} ({@code meta.points_format}) when the codec is a {@link Lucene104Codec} (or
 *       {@link Lucene104SplitPointsCodec} itself): its segments get the codec name {@code Lucene104SplitPoints}, which SPI
 *       reads with {@link Lucene104Codec}'s readers, and those read every other format of a {@link Lucene104Codec}
 *       segment from the segment itself (stored fields mode, per-field postings, doc values and vectors formats).</li>
 * </ul>
 * The stored-fields codec and every format without a mapping entry stay the requested codec's, so for example
 * {@code best_compression} keeps its compression. Because every Lucene104 segment of a {@code bufferpoolfs} index is
 * named {@code Lucene104SplitPoints}, also when no field asks for the split format, the codec name does not show which
 * format a field uses: its {@code PerFieldPointsFormat.format} field attribute and its {@code _Lucene90Split_0.kd*}
 * files do. A codec that cannot be wrapped for one of the two (a codec of another plugin that is not a Lucene104 codec,
 * a codec whose postings format is not per field, the codecs of composite (star-tree) indices) is used as it is,
 * through {@link UnusedFormatMetaWarningCodec}, which logs one WARN per field and mapping key when a segment it writes
 * holds a field whose mapping asks for a format it cannot write. A codec service is built per shard engine, so the
 * WARN and the per-field caches are per shard and engine open.
 */
final class BufferPoolCodecService extends CodecService {
    private final MapperService mapperService;
    private final Logger logger;
    private final Map<String, Codec> wrapped = new ConcurrentHashMap<>();

    BufferPoolCodecService(CodecServiceConfig config) {
        super(config.getMapperService(), config.getIndexSettings(), config.getLogger(), config.getAdditionalCodecs());
        this.mapperService = config.getMapperService();
        this.logger = config.getLogger();
    }

    @Override
    public Codec codec(String name) {
        final Codec codec = super.codec(name);
        if (mapperService == null) {
            return codec;
        }
        return wrapped.computeIfAbsent(name, n -> wrap(n, codec));
    }

    private Codec wrap(String name, Codec codec) {
        // composite (star-tree) indices wrap the codecs differently: keep them, but do not ignore a meta entry silently
        final boolean composite = mapperService.isCompositeIndexPresent();
        final boolean postings = composite == false && PostingsFormatSelectingCodec.supports(codec);
        final boolean points = composite == false && (codec instanceof Lucene104Codec || codec instanceof Lucene104SplitPointsCodec);
        Codec out = codec;
        if (postings == false || points == false) {
            out = new UnusedFormatMetaWarningCodec(name, out, points == false, postings == false, mapperService, logger);
        }
        if (postings) {
            out = new PostingsFormatSelectingCodec(out, mapperService, logger);
        }
        if (points) {
            out = new PointsFormatSelectingCodec(out, mapperService, logger);
        }
        return out;
    }
}
