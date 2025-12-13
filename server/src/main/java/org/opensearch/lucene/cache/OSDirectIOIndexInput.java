package org.opensearch.lucene.cache;

import one.jasyncfio.AsyncFile;
import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class OSDirectIOIndexInput extends IndexInput {
    protected ByteBuffer buffer;
    private  FileChannel channel;
    protected AsyncFile fc;
    private  int blockSize;
    protected long offset;
    protected long length;
    private  boolean isClosable; // clones and slices are not closable
    private boolean isOpen;
    protected long filePos;


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

    /**
     * Creates a new instance of DirectIOIndexInput for reading index input with direct IO bypassing
     * OS buffer
     *
     * @throws UnsupportedOperationException if the JDK does not support Direct I/O
     * @throws IOException if the operating system or filesystem does not support support Direct I/O
     *     or a sufficient equivalent.
     */
    //Path path, int blockSize, int bufferSize,
    //                                AsyncFile fc
    public OSDirectIOIndexInput(Path path, int blockSize, int bufferSize,
                                AsyncFile fc) throws IOException {
        super("DirectIOIndexInput(path=\"" + path + "\")");
        this.fc = fc;
        this.blockSize = blockSize;
        this.buffer = allocateBuffer(bufferSize, blockSize);
        this.isOpen = true;
        this.isClosable = true;
        if (channel != null) {
            this.length = channel.size();
        } else {
            CompletableFuture<Long> size = fc.size();
            try {
                this.length = size.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        this.offset = 0L;
        this.filePos = -bufferSize;
        this.buffer.limit(0);
    }


    /**
     * Creates a new instance of DirectIOIndexInput for reading index input with direct IO bypassing
     * OS buffer
     *
     * @throws UnsupportedOperationException if the JDK does not support Direct I/O
     * @throws IOException if the operating system or filesystem does not support support Direct I/O
     *     or a sufficient equivalent.
     */
    public OSDirectIOIndexInput(Path path, int blockSize, int bufferSize) throws IOException {
        super("DirectIOIndexInput(path=\"" + path + "\")");
        this.channel = FileChannel.open(path, StandardOpenOption.READ, getDirectOpenOption());
        this.blockSize = blockSize;
        this.buffer = allocateBuffer(bufferSize, blockSize);
        this.isOpen = true;
        this.isClosable = true;
        this.length = channel.size();
        this.offset = 0L;
        this.filePos = -bufferSize;
        this.buffer.limit(0);
    }

    // for clone/slice
    private OSDirectIOIndexInput(
        String description, OSDirectIOIndexInput other, long offset, long length) throws IOException {
        super(description);
        //Objects.checkFromIndexSize(offset, length, other.channel.size());
        final int bufferSize = other.buffer.capacity();
        this.buffer = allocateBuffer(bufferSize, other.blockSize);
        this.blockSize = other.blockSize;
        this.channel = other.channel;
        this.fc = other.fc;
        this.isOpen = true;
        this.isClosable = false;
        this.length = length;
        this.offset = offset;
        this.filePos = -bufferSize;
        buffer.limit(0);
    }

    private static ByteBuffer allocateBuffer(int bufferSize, int blockSize) {
        return ByteBuffer.allocateDirect(bufferSize + blockSize - 1)
            .alignedSlice(blockSize)
            .order(LITTLE_ENDIAN);
    }

    @Override
    public void close() throws IOException {
        if (isOpen && isClosable) {
            if (channel != null) {
                channel.close();
                isOpen = false;
            } if (fc != null) {
                fc.close();
                isOpen = false;
            }
        }
    }

    @Override
    public long getFilePointer() {
        long filePointer = filePos + buffer.position() - offset;

        // opening the input and immediately calling getFilePointer without calling readX (and thus
        // refill) first,
        // will result in negative value equal to bufferSize being returned,
        // due to the initialization method filePos = -bufferSize used in constructor.
        assert filePointer == -buffer.capacity() - offset || filePointer >= 0
            : "filePointer should either be initial value equal to negative buffer capacity, or larger than or equal to 0";
        return Math.max(filePointer, 0);
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos != getFilePointer()) {
            final long absolutePos = pos + offset;
            if (absolutePos >= filePos && absolutePos <= filePos + buffer.limit()) {
                // the new position is within the existing buffer
                buffer.position(Math.toIntExact(absolutePos - filePos));
            } else {
                seekInternal(pos); // do an actual seek/read
            }
        }
        assert pos == getFilePointer();
    }

    private void seekInternal(long pos) throws IOException {
        final long absPos = pos + offset;
        final long alignedPos = absPos - (absPos % blockSize);
        filePos = alignedPos - buffer.capacity();

        final int delta = (int) (absPos - alignedPos);
        refill(delta);
        buffer.position(delta);
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public byte readByte() throws IOException {
        if (!buffer.hasRemaining()) {
            refill(1);
        }

        return buffer.get();
    }

    @Override
    public short readShort() throws IOException {
        if (buffer.remaining() >= Short.BYTES) {
            return buffer.getShort();
        } else {
            return super.readShort();
        }
    }

    @Override
    public int readInt() throws IOException {
        if (buffer.remaining() >= Integer.BYTES) {
            return buffer.getInt();
        } else {
            return super.readInt();
        }
    }

    @Override
    public long readLong() throws IOException {
        if (buffer.remaining() >= Long.BYTES) {
            return buffer.getLong();
        } else {
            return super.readLong();
        }
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
            if (channel != null) {
                channel.read(buffer, filePos);
            } else {
                CompletableFuture<Integer> read = fc.read(buffer, filePos);
                read.get();
            }
        } catch (Exception ioe) {
            throw new IOException(ioe.getMessage() + ": " + this, ioe);
        }

        buffer.flip();
    }

    @Override
    public void readBytes(byte[] dst, int offset, int len) throws IOException {
        int toRead = len;
        while (true) {
            final int left = buffer.remaining();
            if (left < toRead) {
                buffer.get(dst, offset, left);
                toRead -= left;
                offset += left;
                refill(toRead);
            } else {
                buffer.get(dst, offset, toRead);
                break;
            }
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int len) throws IOException {
        int remainingDst = len;
        while (remainingDst > 0) {
            int cnt = Math.min(buffer.remaining() / Integer.BYTES, remainingDst);
            buffer.asIntBuffer().get(dst, offset + len - remainingDst, cnt);
            buffer.position(buffer.position() + Integer.BYTES * cnt);
            remainingDst -= cnt;
            if (remainingDst > 0) {
                if (buffer.hasRemaining()) {
                    dst[offset + len - remainingDst] = readInt();
                    --remainingDst;
                } else {
                    refill(remainingDst * Integer.BYTES);
                }
            }
        }
    }

    @Override
    public void readFloats(float[] dst, int offset, int len) throws IOException {
        int remainingDst = len;
        while (remainingDst > 0) {
            int cnt = Math.min(buffer.remaining() / Float.BYTES, remainingDst);
            buffer.asFloatBuffer().get(dst, offset + len - remainingDst, cnt);
            buffer.position(buffer.position() + Float.BYTES * cnt);
            remainingDst -= cnt;
            if (remainingDst > 0) {
                if (buffer.hasRemaining()) {
                    dst[offset + len - remainingDst] = Float.intBitsToFloat(readInt());
                    --remainingDst;
                } else {
                    refill(remainingDst * Float.BYTES);
                }
            }
        }
    }

    @Override
    public void readLongs(long[] dst, int offset, int len) throws IOException {
        int remainingDst = len;
        while (remainingDst > 0) {
            int cnt = Math.min(buffer.remaining() / Long.BYTES, remainingDst);
            buffer.asLongBuffer().get(dst, offset + len - remainingDst, cnt);
            buffer.position(buffer.position() + Long.BYTES * cnt);
            remainingDst -= cnt;
            if (remainingDst > 0) {
                if (buffer.hasRemaining()) {
                    dst[offset + len - remainingDst] = readLong();
                    --remainingDst;
                } else {
                    refill(remainingDst * Long.BYTES);
                }
            }
        }
    }

    @Override
    public OSDirectIOIndexInput clone() {
        try {
            var clone = new OSDirectIOIndexInput("clone:" + this, this, offset, length);
            clone.seekInternal(getFilePointer());
            return clone;
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if ((length | offset) < 0 || length > this.length - offset) {
            throw new IllegalArgumentException(
                "slice() " + sliceDescription + " out of bounds: " + this);
        }
        var slice = new OSDirectIOIndexInput(sliceDescription, this, this.offset + offset, length);
        slice.seekInternal(0L);
        return slice;
    }
}
