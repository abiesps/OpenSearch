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

    private static final int CHUNK_SIZE = 4096;
        //16384;

    /** the file channel we will read from */
    protected AsyncFile channel;

    /** is this instance a clone and hence does not own the file to close it */
    boolean isClone = false;

    /** start offset: non-zero in the slice case */
    protected final long off;

    /** end offset (start+length) */
    protected final long end;


    public IOuringIndexInputV4(String resourceDesc, AsyncFile fc, IOContext context)
        throws IOException {
        this.channel = fc;
        this.off = 0L;
        try {
            this.end = fc.size().get();
        } catch(Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public IOuringIndexInputV4(
        String resourceDesc, AsyncFile fc, long off, long length, int bufferSize) {
        this.channel = fc;
        this.off = off;
        this.end = off + length;
        this.isClone = true;
    }



    @Override
    public void close() throws IOException {
        if (!isClone) {
            if (channel != null) {
                channel.close();
            }
        }
    }

    @Override
    public IOuringIndexInputV4 clone() {
        IOuringIndexInputV4 clone = (IOuringIndexInputV4) super.clone();
        clone.isClone = true;
        return clone;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if ((length | offset) < 0 || length > this.length() - offset) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + this.length()
                    + ": "
                    + this);
        }
        return new IOuringIndexInputV4(
            getFullSliceDescription(sliceDescription),
            channel,
            off + offset,
            length,
            4096);
    }

    @Override
    public final long length() {
        return end - off;
    }

    protected void refill(int bytesToRead) throws IOException {
//        filePos += buffer.capacity();
//
//        // BaseDirectoryTestCase#testSeekPastEOF test for consecutive read past EOF,
//        // hence throwing EOFException early to maintain buffer state (position in particular)
//        if (filePos > offset + length || ((offset + length) - filePos < bytesToRead)) {
//            throw new EOFException("read past EOF: " + this);
//        }
//
//        buffer.clear();
//        try {
//            // read may return -1 here iff filePos == channel.size(), but that's ok as it just reaches
//            // EOF
//            // when filePos > channel.size(), an EOFException will be thrown from above
//            channel.read(buffer, filePos);
//        } catch (IOException ioe) {
//            throw new IOException(ioe.getMessage() + ": " + this, ioe);
//        }
//
//        buffer.flip();
    }

//    @Override
//    protected void readInternal(ByteBuffer b) throws IOException {
//        long pos = getFilePointer() + off;
//
//        if (pos + b.remaining() > end) {
//            throw new EOFException("read past EOF: " + this);
//        }
//
//        try {
//            int readLength = b.remaining();
//            while (readLength > 0) {
//                final int toRead = Math.min(CHUNK_SIZE, readLength);
//                b.limit(b.position() + toRead);
//                assert b.remaining() == toRead;
//                CompletableFuture<Integer> iF = channel.read(b, pos);
//                int i = iF.get();
//                if (i < 0) {
//                    // be defensive here, even though we checked before hand, something could have changed
//                    throw new EOFException(
//                        "read past EOF: "
//                            + this
//                            + " buffer: "
//                            + b
//                            + " chunkLen: "
//                            + toRead
//                            + " end: "
//                            + end);
//                }
//                assert i > 0
//                    : "FileChannel.read with non zero-length bb.remaining() must always read at least "
//                    + "one byte (FileChannel is in blocking mode, see spec of ReadableByteChannel)";
//                pos += i;
//                readLength -= i;
//            }
//            assert readLength == 0;
//        } catch (IOException  ioe) {
//            throw new IOException(ioe.getMessage() + ": " + this, ioe);
//        } catch (ExecutionException e) {
//            throw new RuntimeException(e);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @Override
//    protected void seekInternal(long pos) throws IOException {
//        if (pos > length()) {
//            throw new EOFException(
//                "read past EOF: pos=" + pos + " vs length=" + length() + ": " + this);
//        }
//    }
}


