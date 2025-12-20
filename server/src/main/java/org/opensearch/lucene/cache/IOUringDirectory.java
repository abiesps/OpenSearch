package org.opensearch.lucene.cache;
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import one.jasyncfio.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import static org.opensearch.node.Node.eventExecutor;



public class IOUringDirectory extends FSDirectory {

    private static final Logger logger = LogManager.getLogger(IOUringDirectory.class);
    private final int blockSize;
    private final FSDirectory delegate;


    /**
     */
    public IOUringDirectory(Path path, LockFactory lockFactory, FSDirectory primaryDirectory2) throws IOException {
        super(path, lockFactory);
        this.delegate = primaryDirectory2;
        this.blockSize = Math.toIntExact(Files.getFileStore(delegate.getDirectory()).getBlockSize());
        System.out.println("====Block size returned from file system is ===" + blockSize);
    }

    protected boolean useDirectIO(long fileLength, int blockSize) throws IOException {

        return true;
//        if (fileLength <= blockSize) return false;
//        return true;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);
        Path basePath = getDirectory();
        Path path = basePath.resolve(name);
        CompletableFuture<AsyncFile> asyncFileCompletableFuture = AsyncFile.open(path, eventExecutor,
            OpenOption.DIRECT);
        boolean success = false;
        AsyncFile asyncFile = null;
        try {
            asyncFile = asyncFileCompletableFuture.get();
            long fileLength = asyncFile.size().get();
            boolean usingDirectIO = true;
            if (!useDirectIO(fileLength, blockSize)) {
                asyncFileCompletableFuture = AsyncFile.open(path, eventExecutor,
                    OpenOption.READ_ONLY);
                asyncFile = asyncFileCompletableFuture.get();
                usingDirectIO = false;
            }
//            logger.info("Opening file [{}] in path [{}] fd {} file len {} using direct IO {} ", name, basePath,
//                asyncFile, fileLength, usingDirectIO);
            //String resourceDesc, AsyncFile fc, IOContext context
            final IOuringIndexInputV4 indexInput =
                new IOuringIndexInputV4(path, blockSize, blockSize, asyncFile);

            success = true;
            return indexInput;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (success == false) {
                if (asyncFile != null) {
                    asyncFile.close();
                }
            }
        }
    }
}


