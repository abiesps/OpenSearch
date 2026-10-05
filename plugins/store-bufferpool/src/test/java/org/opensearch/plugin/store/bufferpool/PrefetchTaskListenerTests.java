/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.BudgetScope;
import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.DropReason;
import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.PrefetchOwner;
import org.opensearch.plugin.store.bufferpool.PrefetchScheduler.QueuePolicy;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.TestSearchContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** The cancellation and owner hook: task registration per search phase, owners, fairness keys and drops of cancelled tasks. */
public class PrefetchTaskListenerTests extends OpenSearchTestCase {

    private final AtomicReference<PrefetchScheduler> scheduler = new AtomicReference<>();
    private final PrefetchTaskListener listener = new PrefetchTaskListener(scheduler::get);
    /** The TASK_ID transient that a submission on the test thread carries, or null. */
    private final AtomicReference<Long> taskIdOfThread = new AtomicReference<>();
    private final List<Runnable> workers = new ArrayList<>();

    private PrefetchScheduler newScheduler(int maxInFlight, BudgetScope scope, QueuePolicy policy) {
        final PrefetchScheduler s = new PrefetchScheduler(
            workers::add,
            maxInFlight,
            scope,
            64,
            policy,
            () -> listener.ownerOf(taskIdOfThread.get())
        );
        scheduler.set(s);
        return s;
    }

    private static SearchShardTask task(long id, TaskId parent) {
        return new SearchShardTask(id, "transport", "indices:data/read/search[phase/query]", "test", parent, Map.of());
    }

    private static TestSearchContext context(SearchShardTask task) {
        final TestSearchContext context = new TestSearchContext((QueryShardContext) null);
        context.setTask(task);
        return context;
    }

    private void runWorkers() {
        while (workers.isEmpty() == false) {
            workers.remove(0).run();
        }
    }

    public void testPhaseWithoutTaskDoesNothing() {
        final TestSearchContext context = context(null);
        listener.onPreQueryPhase(context);
        listener.onPreFetchPhase(context);
        assertEquals(0, listener.registeredTasks());
        listener.onQueryPhase(context, 1);
        listener.onFailedQueryPhase(context);
        listener.onFetchPhase(context, 1);
        listener.onFailedFetchPhase(context);
        assertEquals(0, listener.registeredTasks());
    }

    public void testRegistrationLastsWhileAPhaseRuns() {
        final SearchShardTask task = task(10, new TaskId("node", 1));
        final TestSearchContext context = context(task);
        listener.onPreQueryPhase(context);
        assertTrue(listener.isRegistered(10));
        assertEquals(1, listener.registeredTasks());
        listener.onQueryPhase(context, 1);
        assertFalse(listener.isRegistered(10));
        listener.onPreFetchPhase(context);
        assertTrue(listener.isRegistered(10));
        listener.onFetchPhase(context, 1);
        assertEquals(0, listener.registeredTasks());
        // overlapping phases on one task keep the entry until the last phase ends
        listener.onPreQueryPhase(context);
        listener.onPreFetchPhase(context);
        listener.onQueryPhase(context, 1);
        assertTrue(listener.isRegistered(10));
        listener.onFailedFetchPhase(context);
        assertEquals(0, listener.registeredTasks());
        // a scheduler that is not set yet: a failed phase of a cancelled task does nothing
        task.cancel("test");
        listener.onPreQueryPhase(context);
        listener.onFailedQueryPhase(context);
        assertEquals(0, listener.registeredTasks());
    }

    public void testOwnersAndFairnessKeys() {
        final TaskId query = new TaskId("coordinator", 7);
        final SearchShardTask a = task(21, query);
        final SearchShardTask b = task(22, query);
        final SearchShardTask other = task(31, new TaskId("coordinator", 8));
        final SearchShardTask orphan = task(41, TaskId.EMPTY_TASK_ID);
        for (SearchShardTask t : List.of(a, b, other, orphan)) {
            listener.onPreQueryPhase(context(t));
        }
        assertEquals(query, listener.ownerOf(21L).fairnessKey());
        assertEquals(listener.ownerOf(21L).fairnessKey(), listener.ownerOf(22L).fairnessKey());
        assertEquals(21, listener.ownerOf(21L).shardTaskId());
        assertNotEquals(listener.ownerOf(21L).fairnessKey(), listener.ownerOf(31L).fairnessKey());
        // a shard task without a parent, or not registered, is its own requester
        assertEquals(41L, listener.ownerOf(41L).fairnessKey());
        final PrefetchOwner unregistered = listener.ownerOf(99L);
        assertEquals(99L, unregistered.fairnessKey());
        assertEquals(99, unregistered.shardTaskId());
        assertSame(PrefetchOwner.NOT_TRACKED, unregistered.phaseEnded());
        assertFalse(unregistered.cancelled().getAsBoolean());
        // no TASK_ID: the thread
        final PrefetchOwner thread = listener.ownerOf(null);
        assertEquals(PrefetchOwner.ofCurrentThread().fairnessKey(), thread.fairnessKey());
        assertEquals(0, thread.shardTaskId());
        // a registered owner follows the task's state
        final PrefetchOwner owner = listener.ownerOf(21L);
        assertFalse(owner.cancelled().getAsBoolean());
        assertFalse(owner.phaseEnded().getAsBoolean());
        a.cancel("test");
        assertTrue(owner.cancelled().getAsBoolean());
        listener.onQueryPhase(context(a), 1);
        assertTrue(owner.phaseEnded().getAsBoolean());
    }

    public void testShardTasksOfOneQueryAreOneRequester() {
        final PrefetchScheduler s = newScheduler(1, BudgetScope.PREFETCH, QueuePolicy.FAIR);
        final TaskId query = new TaskId("coordinator", 7);
        listener.onPreQueryPhase(context(task(21, query)));
        listener.onPreQueryPhase(context(task(22, query)));
        listener.onPreQueryPhase(context(task(31, new TaskId("coordinator", 8))));
        final AtomicInteger ran = new AtomicInteger();
        // the first item takes the slot; the next items are queued
        taskIdOfThread.set(21L);
        s.submit(c -> ran.incrementAndGet());
        s.submit(c -> ran.incrementAndGet());
        taskIdOfThread.set(22L);
        s.submit(c -> ran.incrementAndGet());
        assertEquals(1, s.stats().maxRequesters());
        taskIdOfThread.set(31L);
        s.submit(c -> ran.incrementAndGet());
        assertEquals(2, s.stats().maxRequesters());
        taskIdOfThread.set(null);
        runWorkers();
        assertEquals(4, ran.get());
    }

    public void testQueuedItemsOfACancelledTaskAreDropped() {
        final PrefetchScheduler s = newScheduler(1, BudgetScope.PREFETCH, randomFrom(QueuePolicy.values()));
        final TaskId query = new TaskId("coordinator", 7);
        final SearchShardTask cancelled = task(21, query);
        final SearchShardTask sibling = task(22, query);
        listener.onPreQueryPhase(context(cancelled));
        listener.onPreQueryPhase(context(sibling));
        final AtomicInteger ranCancelled = new AtomicInteger();
        final AtomicInteger ranSibling = new AtomicInteger();
        taskIdOfThread.set(22L);
        s.submit(c -> ranSibling.incrementAndGet());
        taskIdOfThread.set(21L);
        for (int i = 0; i < 5; i++) {
            s.submit(c -> ranCancelled.incrementAndGet());
        }
        taskIdOfThread.set(22L);
        s.submit(c -> ranSibling.incrementAndGet());
        taskIdOfThread.set(null);
        assertEquals(6, s.queued());
        assertEquals(2, listener.registeredTasks());
        cancelled.cancel("test");
        listener.onFailedQueryPhase(context(cancelled));
        assertEquals(5, (long) s.stats().dropped().get(DropReason.CANCELLED));
        assertEquals(1, s.queued());
        listener.onQueryPhase(context(sibling), 1);
        assertEquals(0, listener.registeredTasks());
        runWorkers();
        assertEquals(0, ranCancelled.get());
        assertEquals(2, ranSibling.get());
        assertEquals(0, s.pending());
    }

    public void testItemsStartedAfterThePhaseEnded() {
        final PrefetchScheduler s = newScheduler(1, BudgetScope.TOTAL, QueuePolicy.FIFO);
        final SearchShardTask task = task(21, new TaskId("coordinator", 7));
        listener.onPreQueryPhase(context(task));
        // one demand read fills the budget: the item is held
        s.demandReadStarted();
        taskIdOfThread.set(21L);
        s.submit(c -> {});
        taskIdOfThread.set(null);
        assertEquals(1, s.queued());
        listener.onQueryPhase(context(task), 1);
        s.demandReadFinished();
        assertEquals(1, s.stats().itemsStartedAfterPhaseEnd());
        runWorkers();
        // an owner without a registered task is never counted
        taskIdOfThread.set(99L);
        s.submit(c -> {});
        taskIdOfThread.set(null);
        s.submit(c -> {});
        runWorkers();
        assertEquals(1, s.stats().itemsStartedAfterPhaseEnd());
        assertEquals(0, s.pending());
    }
}
