/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104PostingsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * Stock {@link Lucene104PostingsFormat} under another name, for experiments.
 *
 * <p>Lucene's per-field postings format writes one set of files per format name. Fields on the default format share
 * their {@code .doc}/{@code .tim}/{@code .tip} files with {@code _id} and {@code _field_names}, which mixes other
 * fields' IO into the baseline numbers. A field mapped with {@code "meta": {"postings_format": "Lucene104Baseline"}}
 * gets its own {@code _N_Lucene104Baseline_0.*} files with exactly the bytes {@code Lucene104} would write.
 */
public final class Lucene104BaselinePostingsFormat extends PostingsFormat {

    /** SPI name of this format. */
    public static final String NAME = "Lucene104Baseline";

    private final PostingsFormat delegate = new Lucene104PostingsFormat();

    /** Creates the format; also used by SPI. */
    public Lucene104BaselinePostingsFormat() {
        super(NAME);
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return delegate.fieldsConsumer(state);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return delegate.fieldsProducer(state);
    }
}
