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
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.mapper.MappedFieldType;
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
 *   <li>{@link PointsFormatSelectingCodec} ({@code meta.points_format}) when the codec is a {@link Lucene104Codec}: its
 *       segments get the codec name {@code Lucene104SplitPoints}, which SPI reads with {@link Lucene104Codec}'s readers,
 *       and those read every other format of a {@link Lucene104Codec} segment from the segment itself (stored fields
 *       mode, per-field postings, doc values and vectors formats).</li>
 * </ul>
 * The stored-fields codec and every format without a mapping entry stay the requested codec's, so for example
 * {@code best_compression} keeps its compression. A codec that cannot be wrapped for one of the two (a codec of another
 * plugin that is not a {@link Lucene104Codec}) is used as it is for that one, with one WARN log per index, codec and
 * mapping key when the mapping asks for it. Composite (star-tree) indices keep the stock codecs.
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
        // composite (star-tree) indices wrap the codecs differently; leave them alone
        if (mapperService == null || mapperService.isCompositeIndexPresent()) {
            return codec;
        }
        return wrapped.computeIfAbsent(name, n -> wrap(n, codec));
    }

    private Codec wrap(String name, Codec codec) {
        Codec out = codec;
        if (PostingsFormatSelectingCodec.supports(codec)) {
            out = new PostingsFormatSelectingCodec(out, mapperService);
        } else {
            warnIfMapped(name, PostingsFormatSelectingCodec.META_KEY, "does not record postings formats per field");
        }
        if (codec instanceof Lucene104Codec) {
            out = new PointsFormatSelectingCodec(out, mapperService, logger);
        } else {
            warnIfMapped(name, PointsFormatSelectingCodec.META_KEY, "is not a Lucene104 codec");
        }
        return out;
    }

    private void warnIfMapped(String codecName, String metaKey, String reason) {
        for (MappedFieldType fieldType : mapperService.fieldTypes()) {
            if (fieldType.meta().containsKey(metaKey)) {
                logger.warn(
                    "index [{}]: codec [{}] {}, so the [{}] mapping entries (field [{}] and any others) are not used",
                    mapperService.index().getName(),
                    codecName,
                    reason,
                    metaKey,
                    fieldType.name()
                );
                return;
            }
        }
    }
}
