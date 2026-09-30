/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.mapper.MapperService;

/**
 * Codec service of {@code bufferpoolfs} indices: {@code index.codec: default} (and its alias {@code lz4}) resolve to
 * {@link PostingsFormatSelectingCodec}. All other codec names behave as in {@link CodecService}.
 */
final class BufferPoolCodecService extends CodecService {

    private final Codec selectingCodec;

    BufferPoolCodecService(CodecServiceConfig config) {
        super(config.getMapperService(), config.getIndexSettings(), config.getLogger(), config.getAdditionalCodecs());
        final MapperService mapperService = config.getMapperService();
        // composite (star-tree) indices wrap the default codec differently; leave them alone
        this.selectingCodec = mapperService == null || mapperService.isCompositeIndexPresent()
            ? null
            : new PostingsFormatSelectingCodec(Lucene104Codec.Mode.BEST_SPEED, mapperService, config.getLogger());
    }

    @Override
    public Codec codec(String name) {
        if (selectingCodec != null && (DEFAULT_CODEC.equals(name) || LZ4.equals(name))) {
            return selectingCodec;
        }
        return super.codec(name);
    }
}
