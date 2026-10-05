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
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A codec of the index (the one {@code index.codec} names), except that a field can pick its Lucene postings format by
 * name in its mapping:
 *
 * <pre>
 * "tag_nav": { "type": "keyword", "meta": { "postings_format": "Lucene104Nav" } }
 * </pre>
 *
 * <p>The value is a Lucene SPI postings format name; a name that is not available keeps the wrapped codec's format for
 * that field, with one WARN log per field and name (a flush never fails because of the mapping entry). Fields without
 * the entry keep the postings format the wrapped codec
 * chooses for them (OpenSearch's per-field rules: completion fields, the {@code _id} fuzzy set), and every other format
 * (stored fields and their compression mode, doc values, points, norms, vectors) is the wrapped codec's. The codec keeps
 * the wrapped codec's name: the wrapped codec's postings format is a {@link PerFieldPostingsFormat}, which records the
 * format of each field in the segment, so the codec that SPI resolves for that name reads the segment.
 */
final class PostingsFormatSelectingCodec extends FilterCodec {
    /** Key of the {@code meta} mapping entry that names the postings format. */
    static final String META_KEY = "postings_format";

    private final MapperService mapperService;
    private final Logger logger;
    private final PerFieldPostingsFormat delegatePostings;
    // (field, name) pairs already warned about
    private final Set<String> warned = ConcurrentHashMap.newKeySet();
    private final Map<String, PostingsFormat> formats = new ConcurrentHashMap<>();
    private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            return choose(field);
        }
    };

    /**
     * @param delegate the codec of the index; its postings format must be a {@link PerFieldPostingsFormat}
     *                 ({@link #supports}), so a segment records each field's format
     */
    PostingsFormatSelectingCodec(Codec delegate, MapperService mapperService, Logger logger) {
        super(delegate.getName(), delegate);
        if (supports(delegate) == false) {
            throw new IllegalArgumentException(
                "codec [" + delegate.getName() + "] does not choose postings formats per field: " + delegate.postingsFormat()
            );
        }
        this.mapperService = mapperService;
        this.logger = logger;
        this.delegatePostings = (PerFieldPostingsFormat) delegate.postingsFormat();
    }

    /** Whether {@code codec}'s segments record a postings format per field, so this codec can wrap it. */
    static boolean supports(Codec codec) {
        return codec.postingsFormat() instanceof PerFieldPostingsFormat;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    PostingsFormat choose(String field) {
        final MappedFieldType fieldType = mapperService.fieldType(field);
        if (fieldType != null) {
            final String name = fieldType.meta().get(META_KEY);
            if (name != null) {
                if (PostingsFormat.availablePostingsFormats().contains(name)) {
                    return formats.computeIfAbsent(name, PostingsFormat::forName);
                }
                if (warned.add(field + "\u0000" + name)) {
                    logger.warn(
                        "index [{}] field [{}]: postings format [{}] is not available, using the codec's own",
                        mapperService.index().getName(),
                        field,
                        name
                    );
                }
            }
        }
        return delegatePostings.getPostingsFormatForField(field);
    }
}
