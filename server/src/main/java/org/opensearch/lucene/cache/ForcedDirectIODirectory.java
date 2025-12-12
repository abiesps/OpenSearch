package org.opensearch.lucene.cache;

import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.OptionalLong;

public class ForcedDirectIODirectory extends DirectIODirectory {


    public ForcedDirectIODirectory(FSDirectory delegate, int mergeBufferSize, long minBytesDirect) throws IOException {
        super(delegate, mergeBufferSize, minBytesDirect);
    }

    public ForcedDirectIODirectory(FSDirectory delegate) throws IOException {
        super(delegate);
    }

    protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
        return true;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        return in.createOutput(name, context);
    }

}
