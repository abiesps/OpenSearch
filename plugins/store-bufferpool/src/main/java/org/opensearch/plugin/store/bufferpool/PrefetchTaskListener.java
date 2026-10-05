/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.tasks.CancellableTask;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Tracks the shard tasks of the search phases running on this node for {@code bufferpoolfs} indices, so that a prefetch
 * item knows its query (the fairness key of {@link PrefetchScheduler.QueuePolicy#FAIR}) and whether its search was
 * cancelled, and so that the queued prefetches of a cancelled shard task are dropped when its phase fails.
 *
 * <p>Cost: two {@link ConcurrentHashMap} operations per shard phase (one at the start, one at the end). Nothing happens
 * for a phase without a task.
 */
final class PrefetchTaskListener implements SearchOperationListener {

    /** A shard task with search phases running on this node; {@code phases} counts them, in case phases overlap. */
    private record Registration(CancellableTask task, int phases) {
    }

    private final Map<Long, Registration> registrations = new ConcurrentHashMap<>();
    private final Supplier<PrefetchScheduler> scheduler;

    /** @param scheduler the node's scheduler, or null before the node created it */
    PrefetchTaskListener(Supplier<PrefetchScheduler> scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        register(searchContext);
    }

    @Override
    public void onPreFetchPhase(SearchContext searchContext) {
        register(searchContext);
    }

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        unregister(searchContext, false);
    }

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        unregister(searchContext, false);
    }

    @Override
    public void onFailedQueryPhase(SearchContext searchContext) {
        unregister(searchContext, true);
    }

    @Override
    public void onFailedFetchPhase(SearchContext searchContext) {
        unregister(searchContext, true);
    }

    private void register(SearchContext context) {
        final CancellableTask task = context.getTask();
        if (task == null) {
            return;
        }
        registrations.compute(task.getId(), (id, r) -> r == null ? new Registration(task, 1) : new Registration(r.task, r.phases + 1));
    }

    private void unregister(SearchContext context, boolean failed) {
        final CancellableTask task = context.getTask();
        if (task == null) {
            return;
        }
        final long id = task.getId();
        registrations.computeIfPresent(id, (k, r) -> r.phases <= 1 ? null : new Registration(r.task, r.phases - 1));
        if (failed && task.isCancelled()) {
            final PrefetchScheduler s = scheduler.get();
            if (s != null) {
                s.dropQueued(id, PrefetchScheduler.DropReason.CANCELLED);
            }
        }
    }

    /** Whether a search phase of the shard task {@code taskId} runs on this node. */
    boolean isRegistered(long taskId) {
        return registrations.containsKey(taskId);
    }

    /** Shard tasks with a search phase running on this node. */
    int registeredTasks() {
        return registrations.size();
    }

    /**
     * The owner of a prefetch submitted by a thread whose {@code TASK_ID} transient is {@code taskId} (null if absent):
     * for a registered shard task, its query (the parent task, or the shard task if it has no parent) with the task's
     * cancellation and phase state; for an unregistered one, the shard task alone; without a task, the thread.
     */
    PrefetchScheduler.PrefetchOwner ownerOf(Long taskId) {
        if (taskId == null) {
            return PrefetchScheduler.PrefetchOwner.ofCurrentThread();
        }
        final long id = taskId;
        final Registration registration = registrations.get(id);
        if (registration == null) {
            return new PrefetchScheduler.PrefetchOwner(
                taskId,
                id,
                PrefetchScheduler.PrefetchOwner.NEVER,
                PrefetchScheduler.PrefetchOwner.NOT_TRACKED
            );
        }
        final CancellableTask task = registration.task;
        final TaskId parent = task.getParentTaskId();
        final Object key = parent == null || parent.isSet() == false ? taskId : parent;
        return new PrefetchScheduler.PrefetchOwner(key, id, task::isCancelled, () -> isRegistered(id) == false);
    }
}
