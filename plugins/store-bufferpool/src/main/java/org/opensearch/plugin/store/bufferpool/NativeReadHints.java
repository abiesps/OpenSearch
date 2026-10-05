/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import com.sun.jna.Native;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.opensearch.secure_sm.AccessController;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tells the kernel which byte range a buffered read is about to read, so that the read reaches storage as one IO of that
 * range whatever the kernel read-ahead setting is.
 *
 * <p>The block cache reads through the OS page cache (no {@code O_DIRECT}: EFS needs 1 MiB direct IOs). On a miss, Linux
 * fills the page cache from storage in chunks that its read-ahead state decides, not in the size of the {@code pread}. With
 * the device's {@code read_ahead_kb} at 0, every missing 4 KiB page is a separate, serial storage read (a 128 KiB
 * {@code pread} on EFS becomes 32 NFS READs). With {@code posix_fadvise(POSIX_FADV_WILLNEED)} on the range first, the kernel
 * submits one read of the whole range at once and the {@code pread} then waits for it, so the storage IO is exactly the
 * read window (up to the device's maximum IO size; NFS splits at {@code rsize}). The hint is not speculative: it names only
 * bytes that the {@code pread} right after it reads.
 *
 * <p>Java's {@code FileChannel} has no fadvise and does not expose its descriptor, so the hint goes through a second,
 * read-only descriptor of the same file that this class opens with libc (JNA). The page cache is per file, not per
 * descriptor, so a hint on that descriptor serves the channel's read. {@link Handle} counts its users, so the descriptor is
 * never closed while a hint is running and never used after it is closed.
 *
 * <p>Available on Linux only. Each open file then holds two descriptors.
 */
final class NativeReadHints {

    private static final Logger logger = LogManager.getLogger(NativeReadHints.class);

    /** Value of {@code bufferpool.io.read_hint}. */
    enum Mode {
        /** {@link #WILLNEED} where the platform supports it, else {@link #NONE}. */
        AUTO,
        /** Announce every storage read with {@code posix_fadvise(POSIX_FADV_WILLNEED)}; the node fails to start without it. */
        WILLNEED,
        /** Plain buffered reads; the kernel read-ahead decides the storage IO size. */
        NONE;

        static Mode parse(String value) {
            try {
                return valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("[bufferpool.io.read_hint] must be auto, willneed or none, got [" + value + "]", e);
            }
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    // Linux values, the same on x86_64 and aarch64
    private static final int O_RDONLY = 0;
    private static final int O_CLOEXEC = 0x80000;
    private static final int POSIX_FADV_WILLNEED = 3;

    private static final boolean AVAILABLE = Constants.LINUX && LibC.LINKED;

    /** Hints that are off: {@link #open(Path)} returns null. */
    static final NativeReadHints DISABLED = new NativeReadHints(false);

    private final boolean enabled;
    /** Ranges announced to the kernel. */
    private final LongAdder hints = new LongAdder();
    /** Hints or descriptor opens that failed; the read itself still runs, with the kernel's IO size. */
    private final LongAdder errors = new LongAdder();

    private NativeReadHints(boolean enabled) {
        this.enabled = enabled;
    }

    /** Whether this platform can announce reads (Linux with libc linked). */
    static boolean isAvailable() {
        return AVAILABLE;
    }

    /**
     * @throws IllegalArgumentException if {@code mode} is {@link Mode#WILLNEED} and the platform cannot announce reads
     */
    static NativeReadHints create(Mode mode) {
        switch (mode) {
            case NONE:
                return new NativeReadHints(false);
            case WILLNEED:
                if (AVAILABLE == false) {
                    throw new IllegalArgumentException(
                        "[bufferpool.io.read_hint] is [willneed] but posix_fadvise is not available on this platform; use auto or none"
                    );
                }
                return new NativeReadHints(true);
            default:
                if (AVAILABLE == false) {
                    logger.info("posix_fadvise is not available, the block cache reads without read hints");
                }
                return new NativeReadHints(AVAILABLE);
        }
    }

    /** Whether reads are announced. */
    boolean enabled() {
        return enabled;
    }

    /** Effective mode, for stats. */
    Mode mode() {
        return enabled ? Mode.WILLNEED : Mode.NONE;
    }

    long hints() {
        return hints.sum();
    }

    long errors() {
        return errors.sum();
    }

    void resetCounters() {
        hints.reset();
        errors.reset();
    }

    /**
     * Opens a hint descriptor for {@code file}, or returns null if hints are off or the open fails (reads then run
     * without hints; the failure is counted).
     */
    Handle open(Path file) {
        if (enabled == false) {
            return null;
        }
        final byte[] path = (file.toAbsolutePath().toString() + '\0').getBytes(StandardCharsets.UTF_8);
        final int fd = LibC.open(path, O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            errors.increment();
            logger.debug("cannot open [{}] for read hints, errno [{}]; reading it without hints", file, Native.getLastError());
            return null;
        }
        return new Handle(fd);
    }

    /** A read-only descriptor used only for hints. Thread safe. */
    final class Handle {
        private final int fd;
        /** 1 for the owner plus 1 per running hint; 0 once closed and idle (the descriptor is closed then). */
        private final AtomicInteger refs = new AtomicInteger(1);
        private final AtomicInteger closed = new AtomicInteger();

        private Handle(int fd) {
            this.fd = fd;
        }

        /** Announces that {@code length} bytes at {@code offset} are read next. Does nothing once closed. */
        void willNeed(long offset, long length) {
            int r;
            do {
                r = refs.get();
                if (r <= 0) {
                    return;
                }
            } while (refs.compareAndSet(r, r + 1) == false);
            try {
                final int rc = LibC.posix_fadvise(fd, offset, length, POSIX_FADV_WILLNEED);
                if (rc == 0) {
                    hints.increment();
                } else {
                    errors.increment();
                }
            } finally {
                release();
            }
        }

        /** Closes the descriptor once no hint runs on it. Idempotent. */
        void close() {
            if (closed.compareAndSet(0, 1)) {
                release();
            }
        }

        private void release() {
            if (refs.decrementAndGet() == 0) {
                LibC.close(fd);
            }
        }
    }

    /** libc functions, bound with JNA direct mapping. */
    private static final class LibC {
        static final boolean LINKED;

        static {
            boolean linked = false;
            if (Constants.LINUX) {
                try {
                    AccessController.doPrivileged(() -> Native.register(LibC.class, "c"));
                    linked = true;
                } catch (Throwable e) {
                    // UnsatisfiedLinkError or NoClassDefFoundError: JNA or libc not usable here
                    logger.warn("cannot link libc for read hints; the block cache reads without them", e);
                }
            }
            LINKED = linked;
        }

        /** {@code path} is NUL-terminated. */
        static native int open(byte[] path, int flags);

        static native int close(int fd);

        /** Returns 0 or an error number (not -1 and errno). {@code off_t} is 64 bits on 64-bit Linux. */
        static native int posix_fadvise(int fd, long offset, long length, int advice);
    }
}
