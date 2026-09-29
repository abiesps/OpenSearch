/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.opensearch.common.util.io.IOUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link FSDirectory} whose reads go through the node-wide {@link BlockCache}. Writes use the plain {@link FSDirectory}
 * output, so the cache holds only blocks that were read.
 *
 * <p>Each file name maps to a file id that is part of the cache key. Deleting, renaming over, or re-creating a file drops
 * its id, so blocks cached from an old file can never be served for a new file with the same name, even when an old
 * reader re-populates them after the file was replaced. Those orphaned blocks age out through normal eviction or are
 * removed when the directory closes.
 */
public final class BufferPoolDirectory extends FSDirectory {

    private static final AtomicLong NEXT_FILE_ID = new AtomicLong();

    private final BlockCache cache;
    private final Path directory;
    private final ConcurrentMap<String, Long> fileIds = new ConcurrentHashMap<>();

    BufferPoolDirectory(Path path, LockFactory lockFactory, BlockCache cache) throws IOException {
        super(path, lockFactory);
        this.cache = cache;
        this.directory = getDirectory();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);
        final Path file = directory.resolve(name);
        final FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        boolean success = false;
        try {
            final long fileId = fileIds.computeIfAbsent(name, n -> NEXT_FILE_ID.incrementAndGet());
            final IndexInput input = new BufferPoolIndexInput("BufferPoolIndexInput(path=\"" + file + "\")", file, fileId, channel, cache);
            success = true;
            return input;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(channel);
            }
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        // a new file under a name that had an id (deleted outside this directory): give it a new id on its first open
        fileIds.remove(name);
        return super.createOutput(name, context);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        final long size = sizeOrZero(name);
        super.deleteFile(name);
        forget(name, size);
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        final long sourceSize = sizeOrZero(source);
        final long destSize = sizeOrZero(dest);
        super.rename(source, dest);
        forget(source, sourceSize);
        forget(dest, destSize);
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            super.close();
        } finally {
            fileIds.clear();
            cache.invalidateDirectory(directory);
        }
    }

    /** Drops the id of {@code name} and the cached blocks of that file. */
    private void forget(String name, long size) {
        final Long fileId = fileIds.remove(name);
        if (fileId != null && size > 0) {
            cache.invalidateFile(directory.resolve(name), fileId, size);
        }
    }

    private long sizeOrZero(String name) throws IOException {
        try {
            return Files.size(directory.resolve(name));
        } catch (NoSuchFileException e) {
            return 0L;
        }
    }
}
