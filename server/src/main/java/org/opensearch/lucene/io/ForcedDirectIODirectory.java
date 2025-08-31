/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lucene.io;

import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.OptionalLong;

public class ForcedDirectIODirectory extends DirectIODirectory {
    public ForcedDirectIODirectory(FSDirectory delegate, int mergeBufferSize, long minBytesDirect) throws IOException {
        super(delegate, mergeBufferSize, minBytesDirect);
    }

    public ForcedDirectIODirectory(FSDirectory delegate) throws IOException {
        super(delegate);
    }

    @Override
    protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
        return true;
    }
}
