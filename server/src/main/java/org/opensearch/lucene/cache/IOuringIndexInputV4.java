/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lucene.cache;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import one.jasyncfio.*;

public class IOuringIndexInputV4 extends OSDirectIOIndexInput {

    public IOuringIndexInputV4(Path path, int blockSize, int bufferSize, AsyncFile fc) throws IOException {
        super(path, blockSize, bufferSize, fc);
    }

    protected void refill(int bytesToRead) throws IOException {
        filePos += buffer.capacity();

        // BaseDirectoryTestCase#testSeekPastEOF test for consecutive read past EOF,
        // hence throwing EOFException early to maintain buffer state (position in particular)
        if (filePos > offset + length || ((offset + length) - filePos < bytesToRead)) {
            throw new EOFException("read past EOF: " + this);
        }

        buffer.clear();
        try {
            // read may return -1 here iff filePos == channel.size(), but that's ok as it just reaches
            // EOF
            // when filePos > channel.size(), an EOFException will be thrown from above
            CompletableFuture<Integer> read = fc.read(buffer, filePos);
            read.get();
            //channel.read(buffer, filePos);
        } catch (Exception ioe) {
            throw new IOException(ioe.getMessage() + ": " + this, ioe);
        }

        buffer.flip();
    }

}


