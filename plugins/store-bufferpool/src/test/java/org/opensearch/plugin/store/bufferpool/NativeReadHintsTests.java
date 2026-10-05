/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.lucene.util.Constants;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

public class NativeReadHintsTests extends OpenSearchTestCase {

    private Path writeFile(int length) throws IOException {
        final byte[] data = new byte[length];
        random().nextBytes(data);
        final Path file = createTempDir().resolve("_0.dvd");
        Files.write(file, data);
        return file;
    }

    public void testModes() {
        assertEquals(NativeReadHints.Mode.AUTO, NativeReadHints.Mode.parse("auto"));
        assertEquals(NativeReadHints.Mode.WILLNEED, NativeReadHints.Mode.parse("WILLNEED"));
        assertEquals(NativeReadHints.Mode.NONE, NativeReadHints.Mode.parse("none"));
        assertEquals("willneed", NativeReadHints.Mode.WILLNEED.toString());
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> NativeReadHints.Mode.parse("random"));
        assertEquals("[bufferpool.io.read_hint] must be auto, willneed or none, got [random]", e.getMessage());
    }

    public void testAvailableOnLinux() {
        // JNA and libc are part of every Linux node
        assertEquals(Constants.LINUX, NativeReadHints.isAvailable());
        assertEquals(NativeReadHints.isAvailable(), NativeReadHints.create(NativeReadHints.Mode.AUTO).enabled());
    }

    public void testNoneOpensNoDescriptor() throws IOException {
        final Path file = writeFile(4096);
        final NativeReadHints hints = NativeReadHints.create(NativeReadHints.Mode.NONE);
        assertFalse(hints.enabled());
        assertNull(hints.open(file));
        assertNull(NativeReadHints.DISABLED.open(file));
        try (StorageFile storage = StorageFile.open(file, hints)) {
            assertFalse(storage.hinted());
            final ByteBuffer dst = ByteBuffer.allocate(1024);
            storage.read(0, dst);
            assertFalse(dst.hasRemaining());
        }
        assertEquals(0, hints.hints());
    }

    public void testWillneedNeedsPlatformSupport() {
        assumeFalse("posix_fadvise is available", NativeReadHints.isAvailable());
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> NativeReadHints.create(NativeReadHints.Mode.WILLNEED)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("posix_fadvise is not available"));
    }

    public void testEveryReadIsAnnounced() throws IOException {
        assumeTrue("needs posix_fadvise", NativeReadHints.isAvailable());
        final NativeReadHints hints = NativeReadHints.create(NativeReadHints.Mode.WILLNEED);
        final Path file = writeFile(64 * 1024 + 100);
        final byte[] expected = Files.readAllBytes(file);
        final BlockCache cache = new BlockCache(1L << 26, 1024, 4096, 16384, hints, Runnable::run);
        final BlockCache.FileStats stats = cache.statsFor(file.getFileName().toString());
        try (StorageFile storage = StorageFile.open(file, hints)) {
            assertTrue(storage.hinted());
            for (int block = 0; block * 1024 < expected.length; block += 3) {
                final ByteBuffer b = cache.getOrLoad(new BlockKey(file, 1, block * 1024L), storage, expected.length, 16384, stats);
                for (int i = 0; i < b.limit(); i++) {
                    assertEquals(expected[block * 1024 + i], b.get(i));
                }
            }
            cache.prefetch(file, 1, storage, expected.length, 0, 65, stats);
        }
        assertEquals("one hint per storage read", stats.reads.sum() + stats.prefetchReads.sum(), hints.hints());
        assertEquals(0, hints.errors());
        hints.resetCounters();
        assertEquals(0, hints.hints());
    }

    public void testClosedHandleIsNeverUsed() throws IOException {
        assumeTrue("needs posix_fadvise", NativeReadHints.isAvailable());
        final NativeReadHints hints = NativeReadHints.create(NativeReadHints.Mode.WILLNEED);
        final NativeReadHints.Handle handle = hints.open(writeFile(8192));
        assertNotNull(handle);
        handle.willNeed(0, 4096);
        assertEquals(1, hints.hints());
        handle.close();
        handle.close();
        handle.willNeed(0, 4096);
        assertEquals(1, hints.hints());
        assertEquals(0, hints.errors());
    }

    public void testCloseWhileHintsRun() throws Exception {
        assumeTrue("needs posix_fadvise", NativeReadHints.isAvailable());
        final NativeReadHints hints = NativeReadHints.create(NativeReadHints.Mode.WILLNEED);
        final Path file = writeFile(1 << 20);
        for (int iter = 0; iter < 20; iter++) {
            final NativeReadHints.Handle handle = hints.open(file);
            final int threads = 4;
            final CyclicBarrier barrier = new CyclicBarrier(threads + 1);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final List<Thread> workers = new ArrayList<>();
            final long seed = random().nextLong();
            for (int t = 0; t < threads; t++) {
                final java.util.Random r = new java.util.Random(seed + t);
                final Thread worker = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < 200; i++) {
                            handle.willNeed((long) r.nextInt(256) * 4096, 4096);
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                });
                workers.add(worker);
                worker.start();
            }
            barrier.await();
            handle.close();
            for (Thread worker : workers) {
                worker.join();
            }
            assertNull(failure.get());
        }
        // a hint on a closed descriptor would fail with EBADF
        assertEquals(0, hints.errors());
    }
}
