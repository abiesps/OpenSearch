/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.PostingsFormat;
import org.opensearch.index.codec.PerFieldMappingPostingFormatCodec;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The default OpenSearch codec, except that a field can pick its Lucene postings format by name in its mapping:
 *
 * <pre>
 * "tag_nav": { "type": "keyword", "meta": { "postings_format": "Lucene104Nav" } }
 * </pre>
 *
 * <p>The value is a Lucene SPI postings format name. Fields without the entry keep the default postings format, so two
 * fields with the same values, one with and one without the entry, compare two formats on the same data in the same
 * segment. The segment records the format of each field, so reading does not need this codec.
 */
final class PostingsFormatSelectingCodec extends PerFieldMappingPostingFormatCodec {

    /** Key of the {@code meta} mapping entry that names the postings format. */
    static final String META_KEY = "postings_format";

    private final MapperService mapperService;
    private final Map<String, PostingsFormat> formats = new ConcurrentHashMap<>();

    PostingsFormatSelectingCodec(Mode compressionMode, MapperService mapperService, Logger logger) {
        super(compressionMode, mapperService, logger);
        this.mapperService = mapperService;
    }

    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
        final MappedFieldType fieldType = mapperService.fieldType(field);
        if (fieldType != null) {
            final String name = fieldType.meta().get(META_KEY);
            if (name != null) {
                return formats.computeIfAbsent(name, PostingsFormat::forName);
            }
        }
        return super.getPostingsFormatForField(field);
    }
}
