package org.opensearch.lucene.cache;

import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.file.Files;
import java.util.OptionalLong;

public class ForcedDirectIODirectory extends DirectIODirectory {

    int blockSize;
    private long minBytesDirect=10 * 1024 * 1024;

    public ForcedDirectIODirectory(FSDirectory delegate, int mergeBufferSize, long minBytesDirect) throws IOException {
        super(delegate, mergeBufferSize, minBytesDirect);
        this.blockSize = Math.toIntExact(Files.getFileStore(delegate.getDirectory()).getBlockSize());
        System.out.println("====Block size returned from file system is ===" + blockSize);
    }

    public ForcedDirectIODirectory(FSDirectory delegate) throws IOException {
        super(delegate);
        this.blockSize = Math.toIntExact(Files.getFileStore(delegate.getDirectory()).getBlockSize());
        System.out.println("====Block size returned from file system is ===" + blockSize);
    }

    protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
       // if (fileLength.orElse(minBytesDirect) >= minBytesDirect) {
           // System.out.println("Using directIO for " + name + " size (mb) " + fileLength.orElse(minBytesDirect)/(1024*1024) );
            return true;
       // }
       //return false;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        return in.createOutput(name, context);
    }

//    @Override
//    public IndexInput openInput(String name, IOContext context) throws IOException {
//        ensureOpen();
//        if (useDirectIO(name, context, OptionalLong.of(fileLength(name)))) {
//            return new OSDirectIOIndexInput(getDirectory().resolve(name), blockSize, blockSize);
//        } else {
//            return in.openInput(name, context);
//        }
//    }
}
