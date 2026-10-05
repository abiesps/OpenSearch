/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.BudgetScope;
import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.DropReason;
import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.Item;
import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.PrefetchOwner;
import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.QueuePolicy;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** The prefetch scheduler alone: the budget gate, dispatch, drops, fairness and its counters, with test items. */
public class PrefetchSchedulerTests extends OpenSearchTestCase {

    private final List<ExecutorService> executors = new ArrayList<>();
    private final List<Thread> threads = new CopyOnWriteArrayList<>();
    /** The owner that the next submission on this thread uses; the thread itself if unset. */
    private final ThreadLocal<PrefetchOwner> owner = new ThreadLocal<>();

    @Override
    public void tearDown() throws Exception {
        for (ExecutorService executor : executors) {
            executor.shutdown();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
        for (Thread thread : threads) {
            thread.join(30_000);
            assertFalse(thread.isAlive());
        }
        super.tearDown();
    }

    private Consumer<Runnable> executor() {
        final ExecutorService executor = Executors.newCachedThreadPool();
        executors.add(executor);
        return executor::execute;
    }

    private PrefetchScheduler scheduler(Consumer<Runnable> starter, int maxInFlight, BudgetScope scope, int queueSize, QueuePolicy policy) {
        return new PrefetchScheduler(starter, maxInFlight, scope, queueSize, policy, () -> {
            final PrefetchOwner o = owner.get();
            return o == null ? PrefetchOwner.ofCurrentThread() : o;
        });
    }

    private static PrefetchOwner requester(Object key, long shardTaskId) {
        return new PrefetchOwner(key, shardTaskId, PrefetchOwner.NEVER, PrefetchOwner.NOT_TRACKED);
    }

    private Thread fork(Runnable body) {
        final Thread thread = new Thread(body);
        threads.add(thread);
        thread.start();
        return thread;
    }

    /** An item that records its start and blocks until released. */
    private static final class TestItem implements Item {
        final String name;
        final CountDownLatch release;
        final ConcurrentLinkedQueue<String> started;
        final CountDownLatch running = new CountDownLatch(1);
        final AtomicInteger runs = new AtomicInteger();

        TestItem(String name, CountDownLatch release, ConcurrentLinkedQueue<String> started) {
            this.name = name;
            this.release = release;
            this.started = started;
        }

        @Override
        public void run(BooleanSupplier cancelled) {
            runs.incrementAndGet();
            if (started != null) {
                started.add(name);
            }
            running.countDown();
            try {
                assertTrue(release.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }

        @Override
        public String description() {
            return name;
        }
    }

    private static final CountDownLatch RELEASED = new CountDownLatch(0);

    private static void assertQuiescent(PrefetchScheduler s) {
        final PrefetchScheduler.Stats stats = s.stats();
        assertEquals(0, s.pending());
        assertEquals(0, s.queued());
        assertEquals(0, s.activeWorkers());
        assertEquals(stats.itemsAdmitted(), stats.itemsFinished() + stats.droppedAfterAdmission());
    }

    public void testBudgetGateStartsPrefetchOnlyInTheSlackOfDemandReads() throws Exception {
        final List<Thread> starters = new CopyOnWriteArrayList<>();
        final Consumer<Runnable> executor = executor();
        final PrefetchScheduler s = scheduler(w -> {
            starters.add(Thread.currentThread());
            executor.accept(w);
        }, 8, BudgetScope.TOTAL, 1024, QueuePolicy.FIFO);
        for (int i = 0; i < 5; i++) {
            s.demandReadStarted();
        }
        final CountDownLatch release = new CountDownLatch(1);
        final List<TestItem> items = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final TestItem item = new TestItem("i" + i, release, null);
            items.add(item);
            assertTrue(s.submit(item));
        }
        assertBusy(() -> assertEquals(3, items.stream().filter(i -> i.runs.get() > 0).count()));
        assertEquals(3, s.activeWorkers());
        assertEquals(7, s.queued());
        assertEquals(10, s.pending());
        assertEquals(7, s.stats().budgetHeldDispatches());
        assertEquals(3, starters.size());
        // 2 demand reads finish on another thread: exactly 2 more workers start, from that thread
        final Thread demand = fork(() -> {
            s.demandReadFinished();
            s.demandReadFinished();
        });
        demand.join();
        assertBusy(() -> assertEquals(5, items.stream().filter(i -> i.runs.get() > 0).count()));
        assertEquals(5, s.activeWorkers());
        assertEquals(5, starters.size());
        assertSame(demand, starters.get(3));
        assertSame(demand, starters.get(4));
        assertEquals(8, s.stats().maxTotalReadsAtItemStart());
        // 8 demand reads in flight: no item starts, and demand reads never wait while another thread holds the lock
        for (int i = 0; i < 5; i++) {
            s.demandReadStarted();
        }
        release.countDown();
        assertBusy(() -> assertEquals(0, s.activeWorkers()));
        assertEquals(5, s.queued());
        final CountDownLatch inHook = new CountDownLatch(1);
        final CountDownLatch leaveHook = new CountDownLatch(1);
        s.setAfterGateReadHookForTests(() -> {
            inHook.countDown();
            try {
                assertTrue(leaveHook.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        });
        final Thread submitter = fork(() -> s.submit(new TestItem("held", RELEASED, null)));
        assertTrue(inHook.await(30, TimeUnit.SECONDS));
        // the submitter holds the scheduler lock now
        s.demandReadStarted();
        assertEquals(9, s.demandReadsInFlight());
        s.demandReadFinished();
        assertEquals(8, s.demandReadsInFlight());
        assertEquals(0, s.activeWorkers());
        s.setAfterGateReadHookForTests(null);
        leaveHook.countDown();
        submitter.join();
        assertEquals(6, s.queued());
        for (int i = 0; i < 8; i++) {
            s.demandReadFinished();
        }
        assertBusy(() -> assertQuiescent(s));
        assertTrue(s.stats().maxTotalReadsAtItemStart() <= 8);
        assertEquals(11, s.stats().itemsFinished());
    }

    public void testPrefetchScopeIgnoresDemandReads() throws Exception {
        final PrefetchScheduler s = scheduler(executor(), 8, BudgetScope.PREFETCH, 1024, QueuePolicy.FIFO);
        for (int i = 0; i < 12; i++) {
            s.demandReadStarted();
        }
        final CountDownLatch release = new CountDownLatch(1);
        for (int i = 0; i < 10; i++) {
            s.submit(new TestItem("i" + i, release, null));
        }
        assertBusy(() -> assertEquals(8, s.activeWorkers()));
        assertEquals(2, s.queued());
        assertEquals(0, s.stats().budgetHeldDispatches());
        // with scope prefetch the gate check counter is reported only: 8 workers plus 12 demand reads
        assertEquals(20, s.stats().maxTotalReadsAtItemStart());
        release.countDown();
        for (int i = 0; i < 12; i++) {
            s.demandReadFinished();
        }
        assertBusy(() -> assertQuiescent(s));
        assertEquals(12, s.stats().maxDemandReadsInFlight());
        assertEquals(12, s.stats().demandReadsStarted());
    }

    /**
     * Review iteration 4, finding 1: the last demand read finishes between the submitter's gate read and its enqueue, so
     * that reader sees nothing queued; the submitter's own re-check after the unlock must start the item.
     */
    public void testNoLostWakeUpBetweenGateReadAndEnqueue() throws Exception {
        final List<Thread> starters = new CopyOnWriteArrayList<>();
        final AtomicBoolean afterHook = new AtomicBoolean();
        final AtomicBoolean startedAfterHook = new AtomicBoolean();
        final Consumer<Runnable> executor = executor();
        final PrefetchScheduler s = scheduler(w -> {
            starters.add(Thread.currentThread());
            startedAfterHook.set(afterHook.get());
            executor.accept(w);
        }, 1, BudgetScope.TOTAL, 16, QueuePolicy.FIFO);
        final AtomicReference<Thread> demandThread = new AtomicReference<>();
        final CountDownLatch demandStarted = new CountDownLatch(1);
        final CountDownLatch demandFinish = new CountDownLatch(1);
        final CountDownLatch demandDone = new CountDownLatch(1);
        demandThread.set(fork(() -> {
            s.demandReadStarted();
            demandStarted.countDown();
            try {
                assertTrue(demandFinish.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            s.demandReadFinished();
            demandDone.countDown();
        }));
        assertTrue(demandStarted.await(30, TimeUnit.SECONDS));
        s.setAfterGateReadHookForTests(() -> {
            // the gate was read closed (0 workers + 1 demand read); the demand read finishes now and sees nothing queued
            demandFinish.countDown();
            try {
                assertTrue(demandDone.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            afterHook.set(true);
        });
        final TestItem item = new TestItem("held", RELEASED, null);
        assertTrue(s.submit(item));
        s.setAfterGateReadHookForTests(null);
        assertBusy(() -> {
            assertEquals(1, item.runs.get());
            assertEquals(0, s.pending());
        });
        assertEquals(1, starters.size());
        assertSame("the submitter's re-check started the item", Thread.currentThread(), starters.get(0));
        assertNotSame(demandThread.get(), starters.get(0));
        assertTrue(startedAfterHook.get());
        assertEquals(1, s.stats().budgetHeldDispatches());
        assertQuiescent(s);
    }

    /**
     * The worker-exit path: a worker reads the gate closed by a demand read with an item queued; that demand read finishes
     * while the worker holds the lock, so its kick cannot take the lock; the exiting worker's re-check must start the item.
     */
    public void testNoLostWakeUpAtWorkerExit() throws Exception {
        final List<Thread> starters = new CopyOnWriteArrayList<>();
        final AtomicBoolean afterHook = new AtomicBoolean();
        final AtomicBoolean startedAfterHook = new AtomicBoolean();
        final Consumer<Runnable> executor = executor();
        final PrefetchScheduler s = scheduler(w -> {
            starters.add(Thread.currentThread());
            startedAfterHook.set(afterHook.get());
            executor.accept(w);
        }, 1, BudgetScope.TOTAL, 16, QueuePolicy.FIFO);
        final CountDownLatch releaseFirst = new CountDownLatch(1);
        final TestItem first = new TestItem("first", releaseFirst, null);
        assertTrue(s.submit(first));
        assertTrue(first.running.await(30, TimeUnit.SECONDS));
        final AtomicReference<Thread> worker = new AtomicReference<>();
        final CountDownLatch demandStarted = new CountDownLatch(1);
        final CountDownLatch demandFinish = new CountDownLatch(1);
        final CountDownLatch demandDone = new CountDownLatch(1);
        final Thread demand = fork(() -> {
            s.demandReadStarted();
            demandStarted.countDown();
            try {
                assertTrue(demandFinish.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            // sees an item queued and kicks, but the exiting worker holds the lock: tryLock fails, nothing blocks
            s.demandReadFinished();
            demandDone.countDown();
        });
        assertTrue(demandStarted.await(30, TimeUnit.SECONDS));
        final TestItem second = new TestItem("second", RELEASED, null);
        assertTrue(s.submit(second));
        assertEquals(1, s.queued());
        final AtomicInteger hookRuns = new AtomicInteger();
        s.setBeforeWorkerExitHookForTests(() -> {
            hookRuns.incrementAndGet();
            worker.set(Thread.currentThread());
            demandFinish.countDown();
            try {
                assertTrue(demandDone.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            afterHook.set(true);
        });
        releaseFirst.countDown();
        assertBusy(() -> {
            assertEquals(1, second.runs.get());
            assertEquals(0, s.pending());
        });
        demand.join();
        assertEquals(1, hookRuns.get());
        assertEquals(2, starters.size());
        assertSame("the exiting worker's re-check started the item", worker.get(), starters.get(1));
        assertNotSame(demand, starters.get(1));
        assertTrue(startedAfterHook.get());
        s.setBeforeWorkerExitHookForTests(null);
        assertBusy(() -> assertQuiescent(s));
    }

    private void joinFork(Runnable body) {
        try {
            fork(body).join(30_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    /**
     * Code review iteration 1, finding 1: the stats readers take the scheduler lock. The demand read that reopens the gate
     * finishes on another thread while a stats reader holds the lock, so its kick cannot take the lock; with no worker
     * left, the stats reader's re-check after its unlock must start the held item.
     */
    public void testNoLostWakeUpThroughAStatsLockHold() throws Exception {
        final List<Consumer<PrefetchScheduler>> readers = List.of(
            PrefetchScheduler::stats,
            PrefetchScheduler::droppedTotal,
            PrefetchScheduler::resetStats
        );
        for (Consumer<PrefetchScheduler> reader : readers) {
            final List<Thread> starters = new CopyOnWriteArrayList<>();
            final Consumer<Runnable> executor = executor();
            final PrefetchScheduler s = scheduler(w -> {
                starters.add(Thread.currentThread());
                executor.accept(w);
            }, 1, BudgetScope.TOTAL, 16, QueuePolicy.FIFO);
            // one demand read fills the budget: the item is held and no worker runs
            s.demandReadStarted();
            final TestItem item = new TestItem("held", RELEASED, null);
            assertTrue(s.submit(item));
            assertEquals(1, s.queued());
            assertEquals(0, s.activeWorkers());
            final AtomicInteger hookRuns = new AtomicInteger();
            s.setStatsLockedHookForTests(() -> {
                hookRuns.incrementAndGet();
                joinFork(s::demandReadFinished);
                assertEquals(0, s.demandReadsInFlight());
                assertEquals("the demand reader's kick could not take the lock", 1, s.queued());
                assertTrue(starters.isEmpty());
            });
            reader.accept(s);
            s.setStatsLockedHookForTests(null);
            assertEquals(1, hookRuns.get());
            // no further submit and no further demand read: only the stats reader's re-check can start the item
            assertBusy(() -> {
                assertEquals(1, item.runs.get());
                assertEquals(0, s.pending());
            });
            assertEquals(1, starters.size());
            assertSame("the stats reader's re-check started the item", Thread.currentThread(), starters.get(0));
            assertBusy(() -> {
                assertEquals(0, s.activeWorkers());
                assertEquals(0, s.queued());
            });
            assertEquals(1, s.stats().itemsStarted());
            assertEquals(1, s.stats().itemsFinished());
        }
    }

    /**
     * Code review iteration 1, finding 2: with scope total the gate reopens while items are held. A submit that finds it
     * open must give the free slot to the next queued item by the policy, not to its own new item.
     */
    public void testSubmitDoesNotBypassQueuedItemsWhenTheGateReopens() {
        for (QueuePolicy policy : QueuePolicy.values()) {
            final List<Runnable> workers = new CopyOnWriteArrayList<>();
            final PrefetchScheduler s = scheduler(workers::add, 2, BudgetScope.TOTAL, 64, policy);
            final ConcurrentLinkedQueue<String> started = new ConcurrentLinkedQueue<>();
            // two demand reads fill the budget: every item is held
            s.demandReadStarted();
            s.demandReadStarted();
            owner.set(requester("heavy", 1));
            for (int i = 0; i < 3; i++) {
                assertTrue(s.submit(new TestItem("H" + i, RELEASED, started)));
            }
            owner.set(requester("light", 2));
            for (int i = 0; i < 2; i++) {
                assertTrue(s.submit(new TestItem("L" + i, RELEASED, started)));
            }
            owner.remove();
            assertEquals(5, s.queued());
            assertTrue(workers.isEmpty());
            // a demand read finishes while a stats reader holds the lock, so its kick cannot run; before the reader's
            // re-check, the heavy requester submits again and finds the gate open with 5 items queued
            s.setStatsLockedHookForTests(() -> {
                joinFork(s::demandReadFinished);
                owner.set(requester("heavy", 1));
                assertTrue(s.submit(new TestItem("H3", RELEASED, started)));
                owner.remove();
            });
            s.stats();
            s.setStatsLockedHookForTests(null);
            assertEquals(1, workers.size());
            assertEquals(5, s.queued());
            assertEquals(6, s.pending());
            // the items held by demand reads count as budget-held dispatches; the new item queued for its turn does not
            assertEquals(5, s.stats().budgetHeldDispatches());
            // the worker runs its items one after another on this thread: one slot is free while one demand read runs
            workers.remove(0).run();
            assertTrue(workers.isEmpty());
            final List<String> expected = policy == QueuePolicy.FIFO
                ? List.of("H0", "H1", "H2", "L0", "L1", "H3")
                : List.of("H0", "L0", "H1", "L1", "H2", "H3");
            assertEquals(policy.value(), expected, new ArrayList<>(started));
            s.demandReadFinished();
            assertQuiescent(s);
        }
    }

    public void testGateCheckCounterNeverExceedsTheBudgetWithScopeTotal() throws Exception {
        final int budget = randomIntBetween(1, 12);
        final PrefetchScheduler s = scheduler(executor(), budget, BudgetScope.TOTAL, 64, QueuePolicy.FIFO);
        final int submitters = randomIntBetween(1, 4);
        final int demanders = randomIntBetween(1, 4);
        final CyclicBarrier barrier = new CyclicBarrier(submitters + demanders);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final List<Thread> workers = new ArrayList<>();
        final long seed = random().nextLong();
        for (int t = 0; t < submitters + demanders; t++) {
            final boolean submitter = t < submitters;
            final java.util.Random r = new java.util.Random(seed + t);
            workers.add(fork(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < 300; i++) {
                        if (submitter) {
                            s.submit(c -> {
                                if (r.nextInt(4) == 0) {
                                    Thread.yield();
                                }
                            });
                        } else {
                            final int n = r.nextInt(budget + 3);
                            for (int d = 0; d < n; d++) {
                                s.demandReadStarted();
                            }
                            Thread.yield();
                            for (int d = 0; d < n; d++) {
                                s.demandReadFinished();
                            }
                        }
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            }));
        }
        for (Thread worker : workers) {
            worker.join();
        }
        assertNull(failure.get());
        assertBusy(() -> assertQuiescent(s));
        final PrefetchScheduler.Stats stats = s.stats();
        assertTrue("at item start " + stats.maxTotalReadsAtItemStart(), stats.maxTotalReadsAtItemStart() <= budget);
        assertTrue(stats.maxActiveWorkers() <= budget);
        assertEquals(0, s.demandReadsInFlight());
        assertEquals(submitters * 300L, stats.itemsFinished() + stats.droppedTotal());
    }

    public void testKickLoopStartsOneWorkerPerQueuedItem() throws Exception {
        final AtomicInteger starts = new AtomicInteger();
        final Consumer<Runnable> executor = executor();
        final PrefetchScheduler s = scheduler(w -> {
            starts.incrementAndGet();
            executor.accept(w);
        }, 4, BudgetScope.TOTAL, 16, QueuePolicy.FIFO);
        for (int i = 0; i < 4; i++) {
            s.demandReadStarted();
        }
        final CountDownLatch release = new CountDownLatch(1);
        final List<TestItem> items = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            final TestItem item = new TestItem("i" + i, release, null);
            items.add(item);
            s.submit(item);
        }
        assertEquals(3, s.queued());
        assertEquals(0, starts.get());
        // 4 demand reads finish at once: the kick loops start exactly 3 workers, each with one of the queued items
        final CyclicBarrier barrier = new CyclicBarrier(4);
        final List<Thread> demand = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            demand.add(fork(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                s.demandReadFinished();
            }));
        }
        for (Thread t : demand) {
            t.join();
        }
        assertBusy(() -> {
            for (TestItem item : items) {
                assertEquals(1, item.runs.get());
            }
        });
        assertEquals(3, starts.get());
        assertEquals(3, s.activeWorkers());
        assertEquals(0, s.queued());
        assertTrue(s.stats().maxTotalReadsAtItemStart() <= 4);
        release.countDown();
        assertBusy(() -> assertQuiescent(s));
        assertEquals(3, starts.get());
    }

    public void testAdmissionOrderAndQueueBound() throws Exception {
        final CountDownLatch release = new CountDownLatch(1);
        final PrefetchScheduler s = scheduler(executor(), 8, BudgetScope.PREFETCH, 1024, QueuePolicy.FIFO);
        int accepted = 0;
        for (int i = 0; i < 1100; i++) {
            if (s.submit(new TestItem("i" + i, release, null))) {
                accepted++;
            }
            assertTrue(s.queued() <= 1024);
        }
        assertEquals(1032, accepted);
        assertEquals(68, (long) s.stats().dropped().get(DropReason.QUEUE_FULL));
        assertEquals(1024, s.stats().maxQueued());
        assertEquals(1032, s.pending());
        release.countDown();
        assertBusy(() -> assertEquals(0, s.pending()));
        assertEquals(1032, s.stats().itemsFinished());

        // queue size 0: drop when no slot is free
        final CountDownLatch release0 = new CountDownLatch(1);
        final PrefetchScheduler zero = scheduler(executor(), 8, BudgetScope.PREFETCH, 0, QueuePolicy.FIFO);
        for (int i = 0; i < 8; i++) {
            assertTrue(zero.submit(new TestItem("z" + i, release0, null)));
        }
        assertFalse(zero.submit(new TestItem("z8", release0, null)));
        assertEquals(1, (long) zero.stats().dropped().get(DropReason.QUEUE_FULL));
        assertEquals(0, zero.stats().maxQueued());
        release0.countDown();
        assertBusy(() -> assertEquals(0, zero.pending()));
        assertEquals(8, zero.stats().itemsFinished());
    }

    /** With one requester, {@code fifo} starts items in submission order and drops the newest when full, like the old executor. */
    public void testFifoStartOrderAndTailDrop() throws Exception {
        final List<Runnable> workers = new ArrayList<>();
        final PrefetchScheduler s = scheduler(workers::add, 1, BudgetScope.PREFETCH, 5, QueuePolicy.FIFO);
        final ConcurrentLinkedQueue<String> started = new ConcurrentLinkedQueue<>();
        final List<String> accepted = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            if (s.submit(new TestItem("i" + i, RELEASED, started))) {
                accepted.add("i" + i);
            }
        }
        assertEquals(List.of("i0", "i1", "i2", "i3", "i4", "i5"), accepted);
        assertEquals(1, workers.size());
        workers.remove(0).run();
        assertEquals(accepted, new ArrayList<>(started));
        assertEquals(4, (long) s.stats().dropped().get(DropReason.QUEUE_FULL));
        assertQuiescent(s);
    }

    public void testFairSharesSlotsPerRequester() {
        final int bound = randomIntBetween(1, 3);
        final List<Runnable> workers = new ArrayList<>();
        final PrefetchScheduler s = scheduler(workers::add, bound, BudgetScope.PREFETCH, 2048, QueuePolicy.FAIR);
        final ConcurrentLinkedQueue<String> started = new ConcurrentLinkedQueue<>();
        owner.set(requester("A", 1));
        for (int i = 0; i < 1000; i++) {
            s.submit(new TestItem("A" + i, RELEASED, started));
        }
        owner.set(requester("B", 2));
        for (int i = 0; i < 10; i++) {
            s.submit(new TestItem("B" + i, RELEASED, started));
        }
        owner.remove();
        assertEquals(2, s.stats().maxRequesters());
        // run the workers one after another on this thread: the start order is deterministic
        while (workers.isEmpty() == false) {
            workers.remove(0).run();
        }
        final List<String> order = new ArrayList<>(started);
        assertEquals(1010, order.size());
        final int lastB = order.indexOf("B9");
        final long aBeforeLastB = order.subList(0, lastB).stream().filter(n -> n.startsWith("A")).count();
        // the bound items that started at once, then one A item per B item
        assertTrue("A items before the last B item: " + aBeforeLastB, aBeforeLastB <= 10 + bound);
        assertQuiescent(s);
        assertEquals(0, s.stats().requesters());
    }

    public void testFairDropsFromTheLongestQueue() {
        final List<Runnable> workers = new ArrayList<>();
        final PrefetchScheduler s = scheduler(workers::add, 1, BudgetScope.PREFETCH, 10, QueuePolicy.FAIR);
        final ConcurrentLinkedQueue<String> started = new ConcurrentLinkedQueue<>();
        owner.set(requester("A", 1));
        // A0 starts (the worker is not run yet), A1..A10 fill the queue
        for (int i = 0; i <= 10; i++) {
            assertTrue(s.submit(new TestItem("A" + i, RELEASED, started)));
        }
        assertFalse("a single requester with a full queue loses its new item", s.submit(new TestItem("A11", RELEASED, started)));
        assertEquals(1, (long) s.stats().dropped().get(DropReason.LONGEST_QUEUE));
        // B's items are admitted while A's queue is longer, each by dropping A's newest queued item
        owner.set(requester("B", 2));
        for (int i = 0; i < 5; i++) {
            assertTrue(s.submit(new TestItem("B" + i, RELEASED, started)));
        }
        assertEquals(6, (long) s.stats().dropped().get(DropReason.LONGEST_QUEUE));
        assertEquals(10, s.queued());
        // now A and B have 5 queued each: B's new item is dropped, not A's
        assertFalse(s.submit(new TestItem("B5", RELEASED, started)));
        // C gets in by dropping from the tie between A and B: A's oldest queued item was admitted first, so A loses one
        owner.set(requester("C", 3));
        assertTrue(s.submit(new TestItem("C0", RELEASED, started)));
        owner.remove();
        while (workers.isEmpty() == false) {
            workers.remove(0).run();
        }
        final List<String> ran = new ArrayList<>(started);
        Collections.sort(ran);
        assertEquals(List.of("A0", "A1", "A2", "A3", "A4", "B0", "B1", "B2", "B3", "B4", "C0"), ran);
        assertEquals(8, (long) s.stats().dropped().get(DropReason.LONGEST_QUEUE));
        assertEquals(0, (long) s.stats().dropped().get(DropReason.QUEUE_FULL));
        assertQuiescent(s);
    }

    public void testFairWaitBoundOfANewRequester() throws Exception {
        final List<Runnable> workers = new ArrayList<>();
        final PrefetchScheduler s = scheduler(workers::add, 1, BudgetScope.PREFETCH, 64, QueuePolicy.FAIR);
        final ConcurrentLinkedQueue<String> started = new ConcurrentLinkedQueue<>();
        for (String r : List.of("A", "A", "A", "B", "B", "C", "C")) {
            owner.set(requester(r, r.charAt(0)));
            s.submit(new TestItem(r, RELEASED, started));
        }
        owner.set(requester("D", 'D'));
        s.submit(new TestItem("D", RELEASED, started));
        owner.remove();
        workers.remove(0).run();
        final List<String> order = new ArrayList<>(started);
        // A runs; A, B and C have queued items; D's first item is among the next R_ahead + 1 = 4 items started
        assertEquals("A", order.get(0));
        assertTrue(order.toString(), order.subList(1, 5).contains("D"));
        assertQuiescent(s);
    }

    public void testDropQueuedRemovesOnlyThatShardTask() throws Exception {
        for (QueuePolicy policy : QueuePolicy.values()) {
            final List<Runnable> workers = new ArrayList<>();
            final PrefetchScheduler s = scheduler(workers::add, 1, BudgetScope.PREFETCH, 64, policy);
            final ConcurrentLinkedQueue<String> started = new ConcurrentLinkedQueue<>();
            // shard tasks 11 and 12 of one query, and shard task 21 of another
            for (int i = 0; i < 4; i++) {
                owner.set(requester("q1", 11));
                s.submit(new TestItem("t11-" + i, RELEASED, started));
                owner.set(requester("q1", 12));
                s.submit(new TestItem("t12-" + i, RELEASED, started));
                owner.set(requester("q2", 21));
                s.submit(new TestItem("t21-" + i, RELEASED, started));
            }
            owner.remove();
            // t11-0 was started at once (its worker is not run yet), so 3 queued items of task 11 are dropped
            s.dropQueued(11, DropReason.CANCELLED);
            assertEquals(3, (long) s.stats().dropped().get(DropReason.CANCELLED));
            assertEquals(8, s.queued());
            assertEquals(9, s.pending());
            workers.remove(0).run();
            final List<String> ran = new ArrayList<>(started);
            assertEquals(policy + " " + ran, 9, ran.size());
            assertEquals(1, ran.stream().filter(n -> n.startsWith("t11")).count());
            assertEquals(4, ran.stream().filter(n -> n.startsWith("t12")).count());
            assertEquals(4, ran.stream().filter(n -> n.startsWith("t21")).count());
            assertEquals(0, s.stats().requesters());
            assertQuiescent(s);
        }
    }

    public void testCancelledItemIsDroppedWhenItReachesAWorker() {
        final List<Runnable> workers = new ArrayList<>();
        final PrefetchScheduler s = scheduler(workers::add, 1, BudgetScope.PREFETCH, 8, QueuePolicy.FIFO);
        final AtomicBoolean cancelled = new AtomicBoolean();
        owner.set(new PrefetchOwner(1L, 1, cancelled::get, PrefetchOwner.NOT_TRACKED));
        final TestItem a = new TestItem("a", RELEASED, null);
        final TestItem b = new TestItem("b", RELEASED, null);
        s.submit(a);
        s.submit(b);
        owner.remove();
        cancelled.set(true);
        workers.remove(0).run();
        assertEquals(0, a.runs.get());
        assertEquals(0, b.runs.get());
        assertEquals(2, (long) s.stats().dropped().get(DropReason.CANCELLED));
        // code review iteration 1, finding 3: an item that a worker drops as cancelled is not counted as started
        assertEquals(0, s.stats().itemsStarted());
        assertEquals(0, s.stats().itemsFinished());
        assertQuiescent(s);
    }

    public void testPendingAccountingAfterEveryKindOfDrop() throws Exception {
        // longest-queue eviction
        final List<Runnable> workers = new ArrayList<>();
        final PrefetchScheduler fair = scheduler(workers::add, 1, BudgetScope.PREFETCH, 2, QueuePolicy.FAIR);
        owner.set(requester("A", 1));
        fair.submit(new TestItem("A0", RELEASED, null));
        fair.submit(new TestItem("A1", RELEASED, null));
        fair.submit(new TestItem("A2", RELEASED, null));
        owner.set(requester("B", 2));
        fair.submit(new TestItem("B0", RELEASED, null));
        owner.remove();
        assertEquals(fair.queued() + 1, fair.pending());
        workers.remove(0).run();
        assertQuiescent(fair);
        assertEquals(4, fair.stats().itemsAdmitted());
        assertEquals(3, fair.stats().itemsFinished());

        // executor rejection: the worker's item and, with no worker left, every queued item
        final AtomicInteger calls = new AtomicInteger();
        final CountDownLatch release = new CountDownLatch(1);
        final Consumer<Runnable> executor = executor();
        final PrefetchScheduler rejecting = scheduler(w -> {
            if (calls.incrementAndGet() > 1) {
                throw new OpenSearchRejectedExecutionException("shutting down");
            }
            executor.accept(w);
        }, 2, BudgetScope.PREFETCH, 8, QueuePolicy.FIFO);
        final TestItem running = new TestItem("running", release, null);
        rejecting.submit(running);
        assertTrue(running.running.await(30, TimeUnit.SECONDS));
        assertFalse(rejecting.submit(new TestItem("rejected", RELEASED, null)));
        assertEquals(1, rejecting.pending());
        assertEquals(1, (long) rejecting.stats().dropped().get(DropReason.REJECTED_BY_EXECUTOR));
        release.countDown();
        assertBusy(() -> assertQuiescent(rejecting));

        // cancellation drop and close
        final List<Runnable> idle = new ArrayList<>();
        final PrefetchScheduler closing = scheduler(idle::add, 1, BudgetScope.PREFETCH, 8, QueuePolicy.FIFO);
        owner.set(requester(7L, 7));
        for (int i = 0; i < 4; i++) {
            closing.submit(new TestItem("c" + i, RELEASED, null));
        }
        owner.remove();
        closing.dropQueued(7, DropReason.CANCELLED);
        assertEquals(1, closing.pending());
        closing.submit(new TestItem("d0", RELEASED, null));
        closing.close();
        assertEquals(1, (long) closing.stats().dropped().get(DropReason.SHUTDOWN));
        assertEquals(1, closing.pending());
        idle.remove(0).run();
        assertQuiescent(closing);
        assertEquals(5, closing.stats().itemsAdmitted());
        assertEquals(1, closing.stats().itemsFinished());
    }

    public void testNoLostWakeUpUnderConcurrentSubmitsDemandReadsAndWorkerExits() throws Exception {
        for (int iteration = 0; iteration < 20; iteration++) {
            final BudgetScope scope = randomFrom(BudgetScope.values());
            final int budget = randomIntBetween(1, 6);
            final PrefetchScheduler s = scheduler(executor(), budget, scope, randomIntBetween(0, 32), randomFrom(QueuePolicy.values()));
            final int submitters = randomIntBetween(1, 4);
            final int demanders = randomIntBetween(0, 3);
            final CyclicBarrier barrier = new CyclicBarrier(submitters + demanders);
            final AtomicInteger ran = new AtomicInteger();
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < submitters + demanders; t++) {
                final boolean submitter = t < submitters;
                final int thread = t;
                workers.add(fork(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < 200; i++) {
                            if (submitter) {
                                owner.set(requester("r" + (thread % 2), thread));
                                s.submit(c -> ran.incrementAndGet());
                            } else {
                                s.demandReadStarted();
                                Thread.yield();
                                s.demandReadFinished();
                            }
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                }));
            }
            for (Thread worker : workers) {
                worker.join();
            }
            assertNull(failure.get());
            assertBusy(() -> {
                assertEquals(0, s.queued());
                assertEquals(0, s.pending());
            });
            final PrefetchScheduler.Stats stats = s.stats();
            assertEquals(submitters * 200L, ran.get() + stats.droppedTotal());
            assertEquals(ran.get(), stats.itemsFinished());
            if (scope == BudgetScope.TOTAL) {
                assertTrue(stats.maxTotalReadsAtItemStart() <= budget);
            }
            assertTrue(stats.maxActiveWorkers() <= budget);
        }
    }

    public void testThrowableInAnItem() throws Exception {
        final AtomicReference<Throwable> uncaught = new AtomicReference<>();
        // each worker on its own thread, which records an Error that escapes the worker
        final PrefetchScheduler s = scheduler(w -> fork(() -> {
            try {
                w.run();
            } catch (AssertionError e) {
                uncaught.set(e);
            }
        }), 1, BudgetScope.PREFETCH, 16, QueuePolicy.FIFO);
        final CountDownLatch release = new CountDownLatch(1);
        s.submit(c -> {
            try {
                assertTrue(release.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            throw new AssertionError("item failed");
        });
        final AtomicInteger ran = new AtomicInteger();
        final List<Thread> runners = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 3; i++) {
            s.submit(c -> {
                runners.add(Thread.currentThread());
                ran.incrementAndGet();
            });
        }
        assertEquals(3, s.queued());
        release.countDown();
        assertBusy(() -> assertEquals(3, ran.get()));
        assertNotNull(uncaught.get());
        assertEquals("item failed", uncaught.get().getMessage());
        assertBusy(() -> assertQuiescent(s));
        assertEquals(4, s.stats().itemsFinished());

        // a RuntimeException is counted as finished and the same worker runs the next item
        final List<Runnable> workers = new ArrayList<>();
        final PrefetchScheduler inline = scheduler(workers::add, 1, BudgetScope.PREFETCH, 16, QueuePolicy.FIFO);
        final List<Thread> ranOn = new ArrayList<>();
        inline.submit(c -> {
            ranOn.add(Thread.currentThread());
            throw new IllegalStateException("item failed");
        });
        inline.submit(c -> ranOn.add(Thread.currentThread()));
        assertEquals(1, workers.size());
        workers.remove(0).run();
        assertEquals(List.of(Thread.currentThread(), Thread.currentThread()), ranOn);
        assertEquals(2, inline.stats().itemsFinished());
        assertQuiescent(inline);
    }

    public void testShutdown() throws Exception {
        final List<Runnable> workers = new ArrayList<>();
        final PrefetchScheduler s = scheduler(workers::add, 1, BudgetScope.TOTAL, 16, QueuePolicy.FAIR);
        final TestItem running = new TestItem("running", RELEASED, null);
        s.submit(running);
        for (int i = 0; i < 5; i++) {
            s.submit(new TestItem("q" + i, RELEASED, null));
        }
        s.close();
        assertEquals(5, (long) s.stats().dropped().get(DropReason.SHUTDOWN));
        assertEquals(0, s.queued());
        assertEquals(1, s.pending());
        assertFalse(s.submit(new TestItem("late", RELEASED, null)));
        assertEquals(6, (long) s.stats().dropped().get(DropReason.SHUTDOWN));
        s.close();
        assertEquals(6, (long) s.stats().dropped().get(DropReason.SHUTDOWN));
        // the running item finishes
        workers.remove(0).run();
        assertEquals(1, running.runs.get());
        assertEquals(0, s.pending());
        assertEquals(0, s.activeWorkers());
    }

    public void testResetSetsMaximaToCurrentValues() throws Exception {
        final CountDownLatch release = new CountDownLatch(1);
        final PrefetchScheduler s = scheduler(executor(), 4, BudgetScope.TOTAL, 16, QueuePolicy.FIFO);
        s.demandReadStarted();
        final List<TestItem> items = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final TestItem item = new TestItem("i" + i, release, null);
            items.add(item);
            s.submit(item);
        }
        assertBusy(() -> assertEquals(3, items.stream().filter(i -> i.runs.get() > 0).count()));
        s.resetStats();
        PrefetchScheduler.Stats stats = s.stats();
        assertEquals(3, stats.maxActiveWorkers());
        assertEquals(2, stats.maxQueued());
        assertEquals(1, stats.maxDemandReadsInFlight());
        assertEquals(0, stats.itemsStarted());
        assertEquals(0, stats.demandReadsStarted());
        assertEquals(0, stats.shortRequesterWait().count());
        s.demandReadFinished();
        release.countDown();
        assertBusy(() -> assertEquals(0, s.pending()));
        stats = s.stats();
        assertEquals(2, stats.itemsStarted());
        assertEquals(5, stats.itemsFinished());
        assertEquals(2, stats.shortRequesterWait().count());
    }

    public void testInvalidArguments() {
        expectThrows(IllegalArgumentException.class, () -> scheduler(Runnable::run, 0, BudgetScope.PREFETCH, 1, QueuePolicy.FIFO));
        expectThrows(IllegalArgumentException.class, () -> scheduler(Runnable::run, 257, BudgetScope.PREFETCH, 1, QueuePolicy.FIFO));
        expectThrows(IllegalArgumentException.class, () -> scheduler(Runnable::run, 8, BudgetScope.PREFETCH, -1, QueuePolicy.FIFO));
        expectThrows(IllegalArgumentException.class, () -> scheduler(Runnable::run, 8, BudgetScope.PREFETCH, 65_537, QueuePolicy.FIFO));
        assertEquals(256, scheduler(Runnable::run, 256, BudgetScope.PREFETCH, 65_536, QueuePolicy.FIFO).maxInFlight());
    }

    public void testThreadOwnerKeysCannotCollideWithTaskIds() {
        final Object key = PrefetchOwner.ofCurrentThread().fairnessKey();
        assertTrue((Long) key < 0);
        assertEquals(0, PrefetchOwner.ofCurrentThread().shardTaskId());
        assertEquals(key, PrefetchOwner.ofCurrentThread().fairnessKey());
    }
}
