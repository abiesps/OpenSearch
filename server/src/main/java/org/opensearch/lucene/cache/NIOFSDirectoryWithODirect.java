package org.opensearch.lucene.cache;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class NIOFSDirectoryWithODirect extends NIOFSDirectory {

    public NIOFSDirectoryWithODirect(Path path, LockFactory lockFactory) throws IOException {
        super(path, lockFactory);
    }


    public NIOFSDirectoryWithODirect(Path path) throws IOException {
        super(path);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);
        Path path = getDirectory().resolve(name);
        FileChannel fc = FileChannel.open(path, StandardOpenOption.READ);
        boolean success = false;
        try {
            final NIOFSDirectIOIndexInput indexInput =
                new NIOFSDirectIOIndexInput("NIOFSDirectIOIndexInput(path=\"" + path + "\")", fc, context);
            success = true;
            return indexInput;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(fc);
            }
        }
    }
}
