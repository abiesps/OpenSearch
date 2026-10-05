/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.opensearch.common.io.Channels;
import org.opensearch.common.util.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * An open file that the block cache reads from: a {@link FileChannel} for positional buffered reads and, when read hints
 * are on, a {@link NativeReadHints.Handle} through which each read announces its range to the kernel first, so the range
 * reaches storage as one IO (see {@link NativeReadHints}).
 */
final class StorageFile implements Closeable {

    private final FileChannel channel;
    /** Null when hints are off or the hint descriptor could not be opened. */
    private final NativeReadHints.Handle hint;

    private StorageFile(FileChannel channel, NativeReadHints.Handle hint) {
        this.channel = channel;
        this.hint = hint;
    }

    /** Opens {@code file} for reading, with a hint descriptor if {@code hints} are on. */
    static StorageFile open(Path file, NativeReadHints hints) throws IOException {
        final FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        try {
            return new StorageFile(channel, hints.open(file));
        } catch (RuntimeException e) {
            IOUtils.closeWhileHandlingException(channel);
            throw e;
        }
    }

    long size() throws IOException {
        return channel.size();
    }

    /** Whether reads of this file are announced to the kernel. */
    boolean hinted() {
        return hint != null;
    }

    /**
     * Reads {@code dst.remaining()} bytes at {@code position}: one positional read, announced first when hints are on.
     *
     * @throws java.io.EOFException if the file ends before {@code dst} is full
     */
    void read(long position, ByteBuffer dst) throws IOException {
        if (hint != null) {
            hint.willNeed(position, dst.remaining());
        }
        Channels.readFromFileChannelWithEofException(channel, position, dst);
    }

    @Override
    public void close() throws IOException {
        try {
            if (hint != null) {
                hint.close();
            }
        } finally {
            channel.close();
        }
    }
}
