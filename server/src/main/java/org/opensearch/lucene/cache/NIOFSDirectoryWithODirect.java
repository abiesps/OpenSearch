package org.opensearch.lucene.cache;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class NIOFSDirectoryWithODirect extends NIOFSDirectory {

    public NIOFSDirectoryWithODirect(Path path, LockFactory lockFactory) throws IOException {
        super(path, lockFactory);
    }


    public NIOFSDirectoryWithODirect(Path path) throws IOException {
        super(path);
    }
    static final OpenOption ExtendedOpenOption_DIRECT; // visible for test

    static {
        OpenOption option;
        try {
            final Class<? extends OpenOption> clazz =
                Class.forName("com.sun.nio.file.ExtendedOpenOption").asSubclass(OpenOption.class);
            option =
                Arrays.stream(clazz.getEnumConstants())
                    .filter(e -> e.toString().equalsIgnoreCase("DIRECT"))
                    .findFirst()
                    .orElse(null);
        } catch (
            @SuppressWarnings("unused")
            Exception e) {
            option = null;
        }
        ExtendedOpenOption_DIRECT = option;
    }

    private static OpenOption getDirectOpenOption() {
        if (ExtendedOpenOption_DIRECT == null) {
            throw new UnsupportedOperationException(
                "com.sun.nio.file.ExtendedOpenOption.DIRECT is not available in the current JDK version.");
        }
        return ExtendedOpenOption_DIRECT;
    }


    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);
        Path path = getDirectory().resolve(name);
        FileChannel fc = FileChannel.open(path, StandardOpenOption.READ, getDirectOpenOption());
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
