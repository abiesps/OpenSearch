/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Node-wide scheduler of prefetch reads: a bounded queue of prefetch items in front of at most {@code maxInFlight}
 * prefetch workers, each of which runs one item at a time and so holds at most one storage read.
 *
 * <p>The node read budget {@code maxInFlight} counts prefetch reads only ({@link BudgetScope#PREFETCH}, the default and
 * the behaviour of the fixed prefetch executor before this class) or prefetch plus demand reads ({@link BudgetScope#TOTAL}).
 * Demand reads never wait for the budget: {@link BlockCache} brackets every demand storage read with
 * {@link #demandReadStarted()} and {@link #demandReadFinished()}, which only count, never block and never throw. With scope
 * {@code TOTAL} a prefetch item starts only while prefetch workers plus demand reads in flight are below the budget, so
 * prefetch uses only the part of the budget that demand reads leave free.
 *
 * <p>Items that cannot start at once are queued, at most {@code queueSize} of them, and dispatched first in first out
 * ({@link QueuePolicy#FIFO}) or round robin per requester ({@link QueuePolicy#FAIR}). An item that does not fit is dropped,
 * never deferred, and the submitter never blocks. A queued item of a cancelled search is dropped, never read.
 *
 * <p>No lost wake-up: a submitter that queues an item while only demand reads close the gate writes the queued count and
 * then reads the demand count again (in the kick loop after its unlock); a finishing demand read decrements the demand
 * count and then reads the queued count. Both are synchronization actions in one total order, so at least one of them
 * sees the other and starts a worker.
 */
final class PrefetchScheduler implements Closeable {

    private static final Logger logger = LogManager.getLogger(PrefetchScheduler.class);

    /** Largest node read budget: the deepest queue depth that the storage model measured. */
    static final int MAX_IN_FLIGHT_LIMIT = 256;
    /** Largest queue of prefetch items. */
    static final int MAX_QUEUE_SIZE = 65_536;

    /** Order in which queued items are dispatched. */
    enum QueuePolicy {
        /** Oldest item first; a new item that does not fit is dropped. */
        FIFO,
        /** Round robin over the requesters with queued items, oldest item first within a requester. */
        FAIR;

        String value() {
            return name().toLowerCase(Locale.ROOT);
        }

        /** Parses {@code fifo} or {@code fair} (case-sensitive) for the setting {@code key}. */
        static QueuePolicy parse(String key, String value) {
            return switch (value) {
                case "fifo" -> FIFO;
                case "fair" -> FAIR;
                default -> throw new IllegalArgumentException("[" + key + "] must be fifo or fair, got [" + value + "]");
            };
        }
    }

    /** What the node read budget counts. */
    enum BudgetScope {
        /** Prefetch reads only. */
        PREFETCH,
        /** Prefetch reads plus demand reads. */
        TOTAL;

        String value() {
            return name().toLowerCase(Locale.ROOT);
        }

        /** Parses {@code prefetch} or {@code total} (case-sensitive) for the setting {@code key}. */
        static BudgetScope parse(String key, String value) {
            return switch (value) {
                case "prefetch" -> PREFETCH;
                case "total" -> TOTAL;
                default -> throw new IllegalArgumentException("[" + key + "] must be prefetch or total, got [" + value + "]");
            };
        }
    }

    /** Why an item was not run. */
    enum DropReason {
        /** The queue was full ({@code fifo}). */
        QUEUE_FULL,
        /** The queue was full and the longest-queue rule dropped this item or the newest item of a longer queue ({@code fair}). */
        LONGEST_QUEUE,
        /** The item's search was cancelled. */
        CANCELLED,
        /** The executor rejected the worker that would have run the item. */
        REJECTED_BY_EXECUTOR,
        /** The scheduler was closed. */
        SHUTDOWN;

        String fieldName() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Who submitted an item.
     *
     * @param fairnessKey the query (the parent task identifier of the shard task), else the shard task identifier (a
     *                    {@link Long}), else the thread (a {@link Long} equal to minus the thread identifier minus 1, which
     *                    cannot collide with a positive task identifier); compared with {@code equals}
     * @param shardTaskId the shard task identifier, or 0 if none; the key of {@link #dropQueued}
     * @param cancelled   whether the submitting search was cancelled
     * @param phaseEnded  whether no search phase of the shard task runs on this node any more; {@link #NOT_TRACKED} for
     *                    owners without a registered task. Read once when the item starts.
     */
    record PrefetchOwner(Object fairnessKey, long shardTaskId, BooleanSupplier cancelled, BooleanSupplier phaseEnded) {
        static final BooleanSupplier NEVER = () -> false;
        static final BooleanSupplier NOT_TRACKED = () -> false;

        static PrefetchOwner ofCurrentThread() {
            return new PrefetchOwner(-Thread.currentThread().threadId() - 1, 0, NEVER, NOT_TRACKED);
        }
    }

    /** One prefetch task. {@link #run} is called at most once, on a prefetch worker. */
    interface Item {
        /** @param cancelled whether the submitting search was cancelled; a multi-window item checks it before each window */
        void run(BooleanSupplier cancelled);

        /** For log messages: what the item reads. */
        default String description() {
            return toString();
        }
    }

    /** An admitted item. */
    private static final class Entry {
        final Item item;
        final PrefetchOwner owner;
        final long admittedNanos;
        /** Admission order, for the tie rule of the longest-queue drop. */
        final long sequence;
        /** Whether the requester had more than {@code maxInFlight} items queued when this one was admitted. */
        final boolean longRequester;

        Entry(Item item, PrefetchOwner owner, long admittedNanos, long sequence, boolean longRequester) {
            this.item = item;
            this.owner = owner;
            this.admittedNanos = admittedNanos;
            this.sequence = sequence;
            this.longRequester = longRequester;
        }
    }

    private final Consumer<Runnable> workerStarter;
    private final int maxInFlight;
    private final BudgetScope scope;
    private final int queueSize;
    private final QueuePolicy policy;
    private final Supplier<PrefetchOwner> ownerOfCurrentThread;

    /** Guards every field below that is not atomic, and every write of the volatile copies. */
    private final ReentrantLock lock = new ReentrantLock();
    /** {@code fifo}: the queued items, oldest first. */
    private final ArrayDeque<Entry> fifoQueue = new ArrayDeque<>();
    /** {@code fair}: the queued items per requester, oldest first. */
    private final Map<Object, ArrayDeque<Entry>> fairQueues = new HashMap<>();
    /** {@code fair}: the requesters with queued items, in round-robin order. */
    private final ArrayDeque<Object> ring = new ArrayDeque<>();
    /** Queued items per requester (both policies); an entry is removed when its count reaches 0. */
    private final Map<Object, Integer> queuedPerRequester = new HashMap<>();
    private int queued;
    private long sequence;
    private boolean closed;

    /** Copies written under the lock, read without it. */
    private volatile int queuedCount;
    private volatile int activeWorkers;
    private volatile int pending;

    private final AtomicInteger demandReadsInFlight = new AtomicInteger();
    private final AtomicBoolean kickPending = new AtomicBoolean();
    /** Whether the current thread is a prefetch worker that holds a slot. */
    private final ThreadLocal<boolean[]> holdsSlot = ThreadLocal.withInitial(() -> new boolean[1]);

    // counters under the lock
    private int maxActiveWorkers;
    private int maxQueued;
    private int maxRequesters;
    private int maxTotalReadsAtItemStart;
    private long budgetHeldDispatches;
    private long itemsStartedAfterPhaseEnd;
    private long itemsAdmitted;
    private long itemsStarted;
    private long itemsFinished;
    private long queueWaitNanos;
    private long droppedAfterAdmission;
    private final long[] dropped = new long[DropReason.values().length];
    // counters outside the lock
    private final AtomicInteger maxDemandReadsInFlight = new AtomicInteger();
    private final LongAdder demandReadsStarted = new LongAdder();
    private final LatencyHistogram shortRequesterWait = new LatencyHistogram();
    private final LatencyHistogram longRequesterWait = new LatencyHistogram();

    private volatile Runnable afterGateReadHook;
    private volatile Runnable beforeWorkerExitHook;

    /**
     * @param workerStarter        starts one worker runnable; may throw {@link RejectedExecutionException}
     * @param maxInFlight          the node read budget, 1 to {@value #MAX_IN_FLIGHT_LIMIT}
     * @param scope                what the budget counts
     * @param queueSize            the most queued items, 0 to {@value #MAX_QUEUE_SIZE}; 0 drops an item when no slot is free
     * @param policy               the dispatch order of queued items
     * @param ownerOfCurrentThread who submits from the current thread
     */
    PrefetchScheduler(
        Consumer<Runnable> workerStarter,
        int maxInFlight,
        BudgetScope scope,
        int queueSize,
        QueuePolicy policy,
        Supplier<PrefetchOwner> ownerOfCurrentThread
    ) {
        if (maxInFlight < 1 || maxInFlight > MAX_IN_FLIGHT_LIMIT) {
            throw new IllegalArgumentException("max in flight must be between 1 and " + MAX_IN_FLIGHT_LIMIT + ", got " + maxInFlight);
        }
        if (queueSize < 0 || queueSize > MAX_QUEUE_SIZE) {
            throw new IllegalArgumentException("queue size must be between 0 and " + MAX_QUEUE_SIZE + ", got " + queueSize);
        }
        this.workerStarter = workerStarter;
        this.maxInFlight = maxInFlight;
        this.scope = scope;
        this.queueSize = queueSize;
        this.policy = policy;
        this.ownerOfCurrentThread = ownerOfCurrentThread;
    }

    int maxInFlight() {
        return maxInFlight;
    }

    BudgetScope scope() {
        return scope;
    }

    int queueSize() {
        return queueSize;
    }

    QueuePolicy policy() {
        return policy;
    }

    /** Whether a worker may start or continue while {@code workers} other workers hold slots and {@code demand} demand reads run. */
    private boolean gateOpen(int workers, int demand) {
        return workers < maxInFlight && (scope == BudgetScope.PREFETCH || workers + demand < maxInFlight);
    }

    /**
     * Admits, queues or drops the item. Never blocks.
     *
     * @return false if the item was dropped at once
     */
    boolean submit(Item item) {
        final PrefetchOwner owner = ownerOfCurrentThread.get();
        Entry first = null;
        boolean accepted = true;
        lock.lock();
        try {
            if (closed) {
                drop(DropReason.SHUTDOWN, 1, false);
                logger.trace("prefetch scheduler is closed, dropping a prefetch of [{}]", item.description());
                return false;
            }
            final int demand = demandReadsInFlight.get();
            final boolean open = gateOpen(activeWorkers, demand);
            final Runnable hook = afterGateReadHook;
            if (hook != null) {
                hook.run();
            }
            final long now = System.nanoTime();
            if (open) {
                activeWorkers++;
                setPending(pending + 1);
                itemsAdmitted++;
                first = new Entry(item, owner, now, sequence++, false);
                recordStart(first, activeWorkers + demand, now);
            } else if (queued < queueSize) {
                enqueue(newQueuedEntry(item, owner, now));
                afterQueued();
            } else if (policy == QueuePolicy.FIFO) {
                drop(DropReason.QUEUE_FULL, 1, false);
                logger.trace("prefetch queue is full, dropping a prefetch of [{}]", item.description());
                accepted = false;
            } else {
                accepted = admitByLongestQueue(item, owner, now);
            }
        } finally {
            lock.unlock();
        }
        if (first != null) {
            accepted = startWorker(first);
        }
        runKickLoop();
        return accepted;
    }

    /** Under the lock, after an item was queued: counts a dispatch held by demand reads and arranges the re-check. */
    private void afterQueued() {
        setPending(pending + 1);
        itemsAdmitted++;
        if (activeWorkers < maxInFlight) {
            // only demand reads close the gate: re-read the demand count after the queued count was written
            budgetHeldDispatches++;
            kickPending.set(true);
        }
    }

    private Entry newQueuedEntry(Item item, PrefetchOwner owner, long now) {
        final int ahead = queuedPerRequester.getOrDefault(owner.fairnessKey(), 0);
        return new Entry(item, owner, now, sequence++, ahead > maxInFlight);
    }

    /**
     * {@code fair} with a full queue: drops the new item if its requester has at least as many queued items as the longest
     * queue of the other requesters, else drops the newest item of that longest queue and queues the new item.
     */
    private boolean admitByLongestQueue(Item item, PrefetchOwner owner, long now) {
        final Object key = owner.fairnessKey();
        Object victim = null;
        int longest = 0;
        long victimOldest = Long.MAX_VALUE;
        for (Map.Entry<Object, ArrayDeque<Entry>> requester : fairQueues.entrySet()) {
            if (requester.getKey().equals(key)) {
                continue;
            }
            final ArrayDeque<Entry> items = requester.getValue();
            final long oldest = items.peekFirst().sequence;
            if (items.size() > longest || (items.size() == longest && oldest < victimOldest)) {
                victim = requester.getKey();
                longest = items.size();
                victimOldest = oldest;
            }
        }
        if (queuedPerRequester.getOrDefault(key, 0) >= longest) {
            drop(DropReason.LONGEST_QUEUE, 1, false);
            logger.trace("prefetch queue is full, dropping a prefetch of [{}] (longest queue)", item.description());
            return false;
        }
        final Entry evicted = removeNewest(victim);
        setPending(pending - 1);
        drop(DropReason.LONGEST_QUEUE, 1, true);
        logger.trace("prefetch queue is full, dropping a queued prefetch of [{}] (longest queue)", evicted.item.description());
        enqueue(newQueuedEntry(item, owner, now));
        afterQueued();
        return true;
    }

    private void enqueue(Entry entry) {
        final Object key = entry.owner.fairnessKey();
        if (policy == QueuePolicy.FIFO) {
            fifoQueue.addLast(entry);
        } else {
            final ArrayDeque<Entry> items = fairQueues.computeIfAbsent(key, k -> new ArrayDeque<>());
            if (items.isEmpty()) {
                ring.addLast(key);
            }
            items.addLast(entry);
        }
        queuedPerRequester.merge(key, 1, Integer::sum);
        setQueued(queued + 1);
        maxQueued = Math.max(maxQueued, queued);
        maxRequesters = Math.max(maxRequesters, queuedPerRequester.size());
    }

    /** Under the lock, with {@code queued > 0}: the next item by the policy. */
    private Entry dequeue() {
        final Entry entry;
        if (policy == QueuePolicy.FIFO) {
            entry = fifoQueue.pollFirst();
        } else {
            final Object key = ring.pollFirst();
            final ArrayDeque<Entry> items = fairQueues.get(key);
            entry = items.pollFirst();
            if (items.isEmpty()) {
                fairQueues.remove(key);
            } else {
                ring.addLast(key);
            }
        }
        uncount(entry);
        return entry;
    }

    /** {@code fair}: removes the newest queued item of {@code key}. */
    private Entry removeNewest(Object key) {
        final ArrayDeque<Entry> items = fairQueues.get(key);
        final Entry entry = items.pollLast();
        if (items.isEmpty()) {
            fairQueues.remove(key);
            ring.remove(key);
        }
        uncount(entry);
        return entry;
    }

    private void uncount(Entry entry) {
        queuedPerRequester.computeIfPresent(entry.owner.fairnessKey(), (k, n) -> n == 1 ? null : n - 1);
        setQueued(queued - 1);
    }

    private void setQueued(int value) {
        queued = value;
        queuedCount = value;
    }

    private void setPending(int value) {
        pending = value;
    }

    /** @param admitted whether the items were admitted (queued or started) before they were dropped */
    private void drop(DropReason reason, int count, boolean admitted) {
        dropped[reason.ordinal()] += count;
        if (admitted) {
            droppedAfterAdmission += count;
        }
    }

    /** Under the lock, right after the gate check that admitted the item: the start counters. */
    private void recordStart(Entry entry, int totalReadsAtStart, long now) {
        itemsStarted++;
        maxActiveWorkers = Math.max(maxActiveWorkers, activeWorkers);
        maxTotalReadsAtItemStart = Math.max(maxTotalReadsAtItemStart, totalReadsAtStart);
        if (entry.owner.phaseEnded().getAsBoolean()) {
            itemsStartedAfterPhaseEnd++;
        }
        final long wait = Math.max(0, now - entry.admittedNanos);
        queueWaitNanos += wait;
        (entry.longRequester ? longRequesterWait : shortRequesterWait).recordNanos(wait);
    }

    /** Starts a worker whose first item is {@code first}; on rejection drops it (and every queued item if no worker is left). */
    private boolean startWorker(Entry first) {
        try {
            workerStarter.accept(() -> runWorker(first));
            return true;
        } catch (RejectedExecutionException e) {
            logger.debug("prefetch worker rejected by the executor", e);
            lock.lock();
            try {
                activeWorkers--;
                setPending(pending - 1);
                drop(DropReason.REJECTED_BY_EXECUTOR, 1, true);
                if (activeWorkers == 0) {
                    drainQueue(DropReason.REJECTED_BY_EXECUTOR);
                }
            } finally {
                lock.unlock();
            }
            return false;
        }
    }

    /** Under the lock: drops every queued item. */
    private void drainQueue(DropReason reason) {
        int n = 0;
        while (queued > 0) {
            dequeue();
            n++;
        }
        setPending(pending - n);
        drop(reason, n, true);
    }

    private void runWorker(Entry first) {
        final boolean[] slot = holdsSlot.get();
        slot[0] = true;
        Entry current = first;
        boolean accounted = false;
        boolean released = false;
        try {
            while (current != null) {
                accounted = false;
                final boolean ran = runItem(current);
                Entry next = null;
                lock.lock();
                try {
                    setPending(pending - 1);
                    if (ran) {
                        itemsFinished++;
                    } else {
                        drop(DropReason.CANCELLED, 1, true);
                    }
                    accounted = true;
                    if (queued > 0 && closed == false) {
                        final int demand = demandReadsInFlight.get();
                        // the worker counts its own slot as released
                        if (gateOpen(activeWorkers - 1, demand)) {
                            next = dequeue();
                            recordStart(next, activeWorkers + demand, System.nanoTime());
                        } else {
                            final Runnable hook = beforeWorkerExitHook;
                            if (hook != null) {
                                hook.run();
                            }
                            // demand reads close the gate: re-check after the unlock
                            kickPending.set(true);
                        }
                    }
                    if (next == null) {
                        slot[0] = false;
                        activeWorkers--;
                        released = true;
                    }
                } finally {
                    lock.unlock();
                }
                runKickLoop();
                current = next;
            }
        } finally {
            if (released == false) {
                // an Error escaped an item (a RuntimeException is caught in runItem)
                onWorkerError(slot, accounted);
            }
        }
    }

    /** Runs the item unless its search was cancelled. Returns false if it was not run. */
    private static boolean runItem(Entry entry) {
        final BooleanSupplier cancelled = entry.owner.cancelled();
        if (cancelled.getAsBoolean()) {
            return false;
        }
        try {
            entry.item.run(cancelled);
        } catch (RuntimeException e) {
            logger.warn(() -> "prefetch of [" + entry.item.description() + "] failed", e);
        }
        return true;
    }

    /** Releases the slot of a worker whose item threw an Error, and starts a replacement worker if items are queued. */
    private void onWorkerError(boolean[] slot, boolean accounted) {
        Entry replacement = null;
        lock.lock();
        try {
            if (accounted == false) {
                setPending(pending - 1);
                itemsFinished++;
            }
            slot[0] = false;
            activeWorkers--;
            if (queued > 0 && closed == false) {
                final int demand = demandReadsInFlight.get();
                if (gateOpen(activeWorkers, demand)) {
                    activeWorkers++;
                    replacement = dequeue();
                    recordStart(replacement, activeWorkers + demand, System.nanoTime());
                } else {
                    kickPending.set(true);
                }
            }
        } finally {
            lock.unlock();
        }
        if (replacement != null) {
            startWorker(replacement);
        }
        runKickLoop();
    }

    /**
     * Starts workers for queued items while the gate is open, if a kick is pending and the lock is free. Never blocks on the
     * lock: if another thread holds it, that thread runs this loop after its unlock and sees the pending kick.
     */
    private void runKickLoop() {
        while (kickPending.get() && lock.tryLock()) {
            List<Entry> starts = null;
            try {
                kickPending.set(false);
                while (queued > 0 && closed == false) {
                    final int demand = demandReadsInFlight.get();
                    if (gateOpen(activeWorkers, demand) == false) {
                        break;
                    }
                    final Entry entry = dequeue();
                    activeWorkers++;
                    recordStart(entry, activeWorkers + demand, System.nanoTime());
                    if (starts == null) {
                        starts = new ArrayList<>();
                    }
                    starts.add(entry);
                }
            } finally {
                lock.unlock();
            }
            if (starts != null) {
                for (Entry entry : starts) {
                    startWorker(entry);
                }
            }
        }
    }

    /** Drops every queued item whose shard task is {@code shardTaskId} (not 0). */
    void dropQueued(long shardTaskId, DropReason reason) {
        if (shardTaskId == 0) {
            return;
        }
        lock.lock();
        try {
            int n = 0;
            if (policy == QueuePolicy.FIFO) {
                for (Iterator<Entry> it = fifoQueue.iterator(); it.hasNext();) {
                    final Entry entry = it.next();
                    if (entry.owner.shardTaskId() == shardTaskId) {
                        it.remove();
                        uncount(entry);
                        n++;
                    }
                }
            } else {
                for (Iterator<Map.Entry<Object, ArrayDeque<Entry>>> it = fairQueues.entrySet().iterator(); it.hasNext();) {
                    final Map.Entry<Object, ArrayDeque<Entry>> requester = it.next();
                    for (Iterator<Entry> items = requester.getValue().iterator(); items.hasNext();) {
                        final Entry entry = items.next();
                        if (entry.owner.shardTaskId() == shardTaskId) {
                            items.remove();
                            uncount(entry);
                            n++;
                        }
                    }
                    if (requester.getValue().isEmpty()) {
                        it.remove();
                        ring.remove(requester.getKey());
                    }
                }
            }
            setPending(pending - n);
            drop(reason, n, true);
        } finally {
            lock.unlock();
        }
        runKickLoop();
    }

    /**
     * Counts a demand storage read that starts now. Never blocks.
     *
     * @return the demand reads in flight after the increment
     */
    int demandReadStarted() {
        demandReadsStarted.increment();
        final int inFlight = demandReadsInFlight.incrementAndGet();
        if (inFlight > maxDemandReadsInFlight.get()) {
            maxDemandReadsInFlight.accumulateAndGet(inFlight, Math::max);
        }
        return inFlight;
    }

    /**
     * Counts a demand storage read that finished. With scope {@code TOTAL} and items queued, starts workers for them if the
     * gate opened, with a non-blocking {@code tryLock} only. Never blocks on the scheduler lock and never throws.
     */
    void demandReadFinished() {
        demandReadsInFlight.decrementAndGet();
        if (scope == BudgetScope.TOTAL && queuedCount > 0) {
            kickPending.set(true);
            runKickLoop();
        }
    }

    int demandReadsInFlight() {
        return demandReadsInFlight.get();
    }

    /** Admitted items not yet finished or dropped: queued plus running. */
    int pending() {
        return pending;
    }

    int queued() {
        return queuedCount;
    }

    /** Workers that hold a slot, running or waiting for an executor thread. */
    int activeWorkers() {
        return activeWorkers;
    }

    /** True while the current thread is a prefetch worker that holds a slot. */
    boolean currentThreadHoldsSlot() {
        return holdsSlot.get()[0];
    }

    /** Items dropped for every reason. */
    long droppedTotal() {
        lock.lock();
        try {
            long n = 0;
            for (long d : dropped) {
                n += d;
            }
            return n;
        } finally {
            lock.unlock();
        }
    }

    /** The queue, worker, drop and dispatch counters. */
    Stats stats() {
        final Map<DropReason, Long> drops = new EnumMap<>(DropReason.class);
        lock.lock();
        try {
            for (DropReason reason : DropReason.values()) {
                drops.put(reason, dropped[reason.ordinal()]);
            }
            return new Stats(
                maxInFlight,
                scope,
                queueSize,
                policy,
                demandReadsInFlight.get(),
                maxDemandReadsInFlight.get(),
                maxTotalReadsAtItemStart,
                budgetHeldDispatches,
                itemsStartedAfterPhaseEnd,
                demandReadsStarted.sum(),
                activeWorkers,
                maxActiveWorkers,
                queued,
                maxQueued,
                pending,
                queuedPerRequester.size(),
                maxRequesters,
                itemsAdmitted,
                itemsStarted,
                itemsFinished,
                drops,
                droppedAfterAdmission,
                queueWaitNanos / 1000,
                shortRequesterWait.snapshot(),
                longRequesterWait.snapshot()
            );
        } finally {
            lock.unlock();
        }
    }

    /** Sets the counters to zero and the maxima to the current values. */
    void resetStats() {
        lock.lock();
        try {
            maxActiveWorkers = activeWorkers;
            maxQueued = queued;
            maxRequesters = queuedPerRequester.size();
            maxTotalReadsAtItemStart = 0;
            budgetHeldDispatches = 0;
            itemsStartedAfterPhaseEnd = 0;
            itemsAdmitted = 0;
            itemsStarted = 0;
            itemsFinished = 0;
            queueWaitNanos = 0;
            droppedAfterAdmission = 0;
            Arrays.fill(dropped, 0);
        } finally {
            lock.unlock();
        }
        maxDemandReadsInFlight.set(demandReadsInFlight.get());
        demandReadsStarted.reset();
        shortRequesterWait.reset();
        longRequesterWait.reset();
    }

    /** Marks the scheduler closed and drops every queued item; later submissions are dropped. Idempotent. */
    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            drainQueue(DropReason.SHUTDOWN);
        } finally {
            lock.unlock();
        }
    }

    /** Test hook (null in production): runs in {@link #submit} between the gate read and the enqueue, under the lock. */
    void setAfterGateReadHookForTests(Runnable hook) {
        this.afterGateReadHook = hook;
    }

    /**
     * Test hook (null in production): runs in the worker loop, under the lock, after the worker read the gate as closed with
     * items still queued, and before it releases its slot.
     */
    void setBeforeWorkerExitHookForTests(Runnable hook) {
        this.beforeWorkerExitHook = hook;
    }

    /**
     * Snapshot of the scheduler counters. At every quiescent point {@code itemsAdmitted == itemsFinished +
     * droppedAfterAdmission + pending}.
     */
    record Stats(int maxInFlight, BudgetScope scope, int queueSize, QueuePolicy policy, int demandReadsInFlight, int maxDemandReadsInFlight,
        int maxTotalReadsAtItemStart, long budgetHeldDispatches, long itemsStartedAfterPhaseEnd, long demandReadsStarted, int activeWorkers,
        int maxActiveWorkers, int queued, int maxQueued, int pending, int requesters, int maxRequesters, long itemsAdmitted,
        long itemsStarted, long itemsFinished, Map<DropReason, Long> dropped, long droppedAfterAdmission, long queueWaitTimeMicros,
        LatencyHistogram.Snapshot shortRequesterWait, LatencyHistogram.Snapshot longRequesterWait) {
        long droppedTotal() {
            return dropped.values().stream().mapToLong(Long::longValue).sum();
        }
    }
}
