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
import org.opensearch.common.util.io.IOUtils;
import one.jasyncfio.*;
import org.opensearch.node.Node;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import static org.opensearch.node.Node.eventExecutor;



public class IOUringDirectory extends FSDirectory {

    private static final Logger logger = LogManager.getLogger(IOUringDirectory.class);

    /**
     */
    public IOUringDirectory(Path path, LockFactory lockFactory) throws IOException {
        super(path, lockFactory);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);
        Path basePath = getDirectory();
        Path path = basePath.resolve(name);
        //FileChannel fc = FileChannel.open(path, StandardOpenOption.READ);
        //Not using read direct to get away from buffer alignment for now
        CompletableFuture<AsyncFile> asyncFileCompletableFuture = AsyncFile.open(path, eventExecutor,
            OpenOption.DIRECT);
        boolean success = false;
        try {
            AsyncFile asyncFile = asyncFileCompletableFuture.get();
            long fileLength = asyncFile.size().get();
            logger.info("Opening file [{}] in path [{}] fd {} file len {} ", name, basePath,
                asyncFile, fileLength);
            //String resourceDesc, AsyncFile fc, IOContext context
            final IOuringIndexInputV4 indexInput =
                new IOuringIndexInputV4("IOuringIndexInputV3(path=\"" + path + "\")",
                    asyncFile, context);
            success = true;
            return indexInput;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (success == false) {
                //asyncFile.close();
                //org.opensearch.core.internal.io.IOUtils.closeWhileHandlingException(fc);
            }
        }
    }
}


