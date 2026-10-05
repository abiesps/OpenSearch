/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * {@link IndexInput} that serves every read from blocks of the shared {@link BlockCache}.
 *
 * <p>The input keeps a reference to the block that holds the current position, so sequential reads within a block do not
 * touch the cache. When a read leaves that block, the input looks up the next block and, on a miss, reads the aligned
 * window of its read size that holds the block: {@link BlockCache#randomReadSize()} if the input was opened (or its
 * {@link IOContext} was updated) for random access, else {@link BlockCache#sequentialReadSize()}. Clones keep the read size
 * of their input; a slice takes the read size of the {@link IOContext} it is created with, if any.
 * {@link #prefetch(long, long)} loads blocks in the background on request.
 *
 * <p>The input that {@link BufferPoolDirectory} opens owns the {@link StorageFile}. Clones and slices share it and are
 * not usable after that input is closed.
 */
final class BufferPoolIndexInput extends IndexInput implements RandomAccessInput {

    private final Path file;
    private final long fileId;
    private final StorageFile storage;
    private final BlockCache cache;
    private final BlockCache.FileStats stats;
    private final long blockMask;
    private final int blockSizePower;
    /** Length of the whole file. */
    private final long fileLength;
    /** Offset of this input (a slice, or the whole file) in the file. */
    private final long sliceOffset;
    /** Length of this input. */
    private final long length;
    /** Bytes read per miss, {@link BlockCache#randomReadSize()} or {@link BlockCache#sequentialReadSize()}. */
    private int readSize;
    /** True for clones and slices, which do not own the storage file. */
    private boolean isClone;
    private boolean closed;

    /** Position in this input. */
    private long pos;
    /** Block that holds the most recently read position, or null. */
    private ByteBuffer block;
    /** Start of {@link #block}, relative to this input. Can be negative for a slice that starts mid-block. */
    private long blockStart;
    /** End (exclusive) of the readable part of {@link #block}, relative to this input. */
    private long blockEnd;

    /** @param readSize bytes read per miss, {@link BlockCache#randomReadSize()} or {@link BlockCache#sequentialReadSize()} */
    BufferPoolIndexInput(String resourceDescription, Path file, long fileId, StorageFile storage, BlockCache cache, int readSize)
        throws IOException {
        this(
            resourceDescription,
            file,
            fileId,
            storage,
            cache,
            cache.statsFor(file.getFileName().toString()),
            storage.size(),
            0L,
            storage.size(),
            readSize,
            false
        );
    }

    private BufferPoolIndexInput(
        String resourceDescription,
        Path file,
        long fileId,
        StorageFile storage,
        BlockCache cache,
        BlockCache.FileStats stats,
        long fileLength,
        long sliceOffset,
        long length,
        int readSize,
        boolean isClone
    ) {
        super(resourceDescription);
        this.file = file;
        this.fileId = fileId;
        this.storage = storage;
        this.cache = cache;
        this.stats = stats;
        this.blockMask = cache.blockMask();
        this.blockSizePower = cache.blockSizePower();
        this.fileLength = fileLength;
        this.sliceOffset = sliceOffset;
        this.length = length;
        this.readSize = readSize;
        this.isClone = isClone;
    }

    /**
     * Makes the block that holds {@code p} the current block. Callers check that {@code 0 <= p < length}.
     */
    private void loadBlock(long p) throws IOException {
        if (closed) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
        final long blockOffset = (sliceOffset + p) & ~blockMask;
        final ByteBuffer b;
        try {
            b = cache.getOrLoad(new BlockKey(file, fileId, blockOffset), storage, fileLength, readSize, stats);
        } catch (ClosedChannelException e) {
            throw new AlreadyClosedException("Already closed: " + this, e);
        }
        block = b;
        blockStart = blockOffset - sliceOffset;
        blockEnd = Math.min(blockStart + b.limit(), length);
    }

    private EOFException eof(long p, long n) {
        return new EOFException("read past EOF: pos=" + p + " len=" + n + " length=" + length + ": " + this);
    }

    @Override
    public byte readByte() throws IOException {
        final long p = pos;
        if (p < blockStart || p >= blockEnd) {
            if (p >= length) {
                throw eof(p, 1);
            }
            loadBlock(p);
        }
        pos = p + 1;
        return block.get((int) (p - blockStart));
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        long p = pos;
        if (len > length - p) {
            throw eof(p, len);
        }
        while (len > 0) {
            if (p < blockStart || p >= blockEnd) {
                loadBlock(p);
            }
            final int n = (int) Math.min(len, blockEnd - p);
            block.get((int) (p - blockStart), b, offset, n);
            p += n;
            offset += n;
            len -= n;
        }
        pos = p;
    }

    @Override
    public short readShort() throws IOException {
        final long p = pos;
        if (p >= blockStart && p + Short.BYTES <= blockEnd) {
            pos = p + Short.BYTES;
            return block.getShort((int) (p - blockStart));
        }
        return super.readShort(); // crosses a block boundary, or needs the next block
    }

    @Override
    public int readInt() throws IOException {
        final long p = pos;
        if (p >= blockStart && p + Integer.BYTES <= blockEnd) {
            pos = p + Integer.BYTES;
            return block.getInt((int) (p - blockStart));
        }
        return super.readInt();
    }

    @Override
    public long readLong() throws IOException {
        final long p = pos;
        if (p >= blockStart && p + Long.BYTES <= blockEnd) {
            pos = p + Long.BYTES;
            return block.getLong((int) (p - blockStart));
        }
        return super.readLong();
    }

    // RandomAccessInput: absolute reads that do not move the position

    @Override
    public byte readByte(long p) throws IOException {
        if (p < blockStart || p >= blockEnd) {
            if (p < 0 || p >= length) {
                throw eof(p, 1);
            }
            loadBlock(p);
        }
        return block.get((int) (p - blockStart));
    }

    @Override
    public void readBytes(long p, byte[] b, int offset, int len) throws IOException {
        if (p < 0 || len > length - p) {
            throw eof(p, len);
        }
        // whole-block copies; the default implementation reads one byte at a time
        while (len > 0) {
            if (p < blockStart || p >= blockEnd) {
                loadBlock(p);
            }
            final int n = (int) Math.min(len, blockEnd - p);
            block.get((int) (p - blockStart), b, offset, n);
            p += n;
            offset += n;
            len -= n;
        }
    }

    @Override
    public short readShort(long p) throws IOException {
        if (inBlockAfterLoad(p, Short.BYTES)) {
            return block.getShort((int) (p - blockStart));
        }
        return (short) ((readByte(p) & 0xFF) | (readByte(p + 1) & 0xFF) << 8);
    }

    @Override
    public int readInt(long p) throws IOException {
        if (inBlockAfterLoad(p, Integer.BYTES)) {
            return block.getInt((int) (p - blockStart));
        }
        return (readShort(p) & 0xFFFF) | (readShort(p + 2) & 0xFFFF) << 16;
    }

    @Override
    public long readLong(long p) throws IOException {
        if (inBlockAfterLoad(p, Long.BYTES)) {
            return block.getLong((int) (p - blockStart));
        }
        return (readInt(p) & 0xFFFFFFFFL) | ((long) readInt(p + 4)) << 32;
    }

    /** True if the {@code n} bytes at {@code p} are in the current block, after loading the block of {@code p} if needed. */
    private boolean inBlockAfterLoad(long p, int n) throws IOException {
        if (p < 0 || p > length - n) {
            throw eof(p, n);
        }
        if (p < blockStart || p >= blockEnd) {
            loadBlock(p);
        }
        return p + n <= blockEnd;
    }

    @Override
    public void prefetch(long offset, long len) throws IOException {
        if (closed) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
        if (offset < 0 || len < 0 || offset > length - len) {
            throw new IllegalArgumentException(
                "prefetch out of bounds: offset=" + offset + ",length=" + len + ",fileLength=" + length + ": " + this
            );
        }
        if (len == 0) {
            return;
        }
        final long firstBlock = (sliceOffset + offset) >>> blockSizePower;
        final long lastBlock = (sliceOffset + offset + len - 1) >>> blockSizePower;
        cache.prefetch(file, fileId, storage, fileLength, firstBlock << blockSizePower, lastBlock - firstBlock + 1, stats);
    }

    @Override
    public Optional<Boolean> isLoaded(long offset, long len) {
        if (offset < 0 || len < 0 || offset > length - len) {
            throw new IllegalArgumentException(
                "isLoaded out of bounds: offset=" + offset + ",length=" + len + ",fileLength=" + length + ": " + this
            );
        }
        if (len == 0) {
            return Optional.of(true);
        }
        final long firstBlock = (sliceOffset + offset) >>> blockSizePower;
        final long lastBlock = (sliceOffset + offset + len - 1) >>> blockSizePower;
        return Optional.of(cache.contains(file, fileId, firstBlock << blockSizePower, lastBlock - firstBlock + 1));
    }

    @Override
    public long getFilePointer() {
        return pos;
    }

    @Override
    public void seek(long p) throws IOException {
        if (closed) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
        if (p < 0) {
            throw new IllegalArgumentException("seeking to negative position: " + this);
        }
        if (p > length) {
            throw new EOFException("seek past EOF: pos=" + p + " length=" + length + ": " + this);
        }
        pos = p;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public BufferPoolIndexInput clone() {
        if (closed) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
        final BufferPoolIndexInput clone = (BufferPoolIndexInput) super.clone();
        clone.isClone = true;
        // start without a current block, so the clone's first read goes through the cache and is counted
        clone.block = null;
        clone.blockStart = 0;
        clone.blockEnd = 0;
        return clone;
    }

    /** Reads of this input from now on use the read size of {@code context}; clones made before keep theirs. */
    @Override
    public void updateIOContext(IOContext context) throws IOException {
        if (closed) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
        readSize = cache.readSize(BufferPoolDirectory.isRandomAccess(context));
    }

    /** Read size of this input, for tests. */
    int readSize() {
        return readSize;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long len) throws IOException {
        return slice(sliceDescription, offset, len, readSize);
    }

    /** A slice whose reads use the read size of {@code context}, the way compound files open their sub-files. */
    @Override
    public IndexInput slice(String sliceDescription, long offset, long len, IOContext context) throws IOException {
        return slice(sliceDescription, offset, len, cache.readSize(BufferPoolDirectory.isRandomAccess(context)));
    }

    private IndexInput slice(String sliceDescription, long offset, long len, int sliceReadSize) throws IOException {
        if (closed) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
        if (offset < 0 || len < 0 || offset > length - len) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + len
                    + ",fileLength="
                    + length
                    + ": "
                    + this
            );
        }
        return new BufferPoolIndexInput(
            getFullSliceDescription(sliceDescription),
            file,
            fileId,
            storage,
            cache,
            stats,
            fileLength,
            sliceOffset + offset,
            len,
            sliceReadSize,
            true
        );
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        // drop the block so every later read goes through loadBlock, which throws AlreadyClosedException
        block = null;
        blockStart = 0;
        blockEnd = 0;
        if (isClone == false) {
            storage.close();
        }
    }
}
