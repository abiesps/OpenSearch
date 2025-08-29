/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lucene.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ReadEventLogger implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(ReadEventLogger.class);

    private static final DateTimeFormatter SECOND_FMT =
        DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);

    private final Duration flushInterval;
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> task;
    private volatile boolean closed;

    /**
     * Raw event buffers per shard; we aggregate on flush.
     */
    private final ConcurrentMap<Integer, Queue<ReadEvent>> eventsByShard = new ConcurrentHashMap<>();

    /** Create with a default 5s flush interval. */
    public ReadEventLogger() {
        this(Duration.ofSeconds(5));
    }

    public ReadEventLogger(Duration flushInterval) {
        this.flushInterval = flushInterval == null ? Duration.ofSeconds(5) : flushInterval;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "read-event-logger");
            t.setDaemon(true);
            return t;
        });
        start();
    }

    /** Accept events concurrently from any thread. */
    public void accept(ReadEvent event) {
        if (event == null) return;
        if (closed) {
            return; // dropped after close; could also log immediately if you prefer
        }
        eventsByShard
            .computeIfAbsent(event.getShardId(), k -> new ConcurrentLinkedQueue<>())
            .add(event);
    }

    /** Force a synchronous flush now. */
    public void flushNow() {
        flushInternal();
    }

    private void start() {
        long period = flushInterval.toMillis();
        task = scheduler.scheduleAtFixedRate(() -> {
            try {
                flushInternal();
            } catch (Throwable t) {
                // keep task alive
                logger.error("Unexpected error during ReadEvent flush", t);
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    private void flushInternal() {
        if (eventsByShard.isEmpty()) return;

        for (Map.Entry<Integer, Queue<ReadEvent>> entry : eventsByShard.entrySet()) {
            final int shardId = entry.getKey();
            final Queue<ReadEvent> q = entry.getValue();
            if (q == null) continue;

            // Drain the queue
            List<ReadEvent> drained = new ArrayList<>(q.size());
            for (ReadEvent e; (e = q.poll()) != null; ) drained.add(e);
            if (drained.isEmpty()) continue;

            logConcurrencySummaries(shardId, drained);
        }
    }

    /**
     * Prints:
     *  1) Per shard + phase + second: distinct threads (concurrency)
     *  2) Per shard + phase + segGen + second: distinct threads (concurrency)
     */
    private void logConcurrencySummaries(int shardId, List<ReadEvent> batch) {
        // ---- per-phase per-second ----
        // phase -> secondEpoch -> distinct thread set
        Map<String, Map<Long, Set<String>>> byPhaseSecond = new HashMap<>();

        // ---- per-phase + segGen per-second ----
        // phase -> segGen -> secondEpoch -> distinct thread set
        Map<String, Map<String, Map<Long, Set<String>>>> byPhaseSegSecond = new HashMap<>();

        for (ReadEvent e : batch) {
            String phase = nonNull(e.getPhaseName());
            String segGen = nonNull(e.getSegGen());
            String thread = nonNull(e.getThreadName());

            // IMPORTANT: we assume readTime is epoch millis; change this conversion if it's nanos or relative.
            long second = TimeUnit.MILLISECONDS.toSeconds(e.getReadTime());

            byPhaseSecond
                .computeIfAbsent(phase, k -> new HashMap<>())
                .computeIfAbsent(second, k -> new HashSet<>())
                .add(thread);

            byPhaseSegSecond
                .computeIfAbsent(phase, k -> new HashMap<>())
                .computeIfAbsent(segGen, k -> new HashMap<>())
                .computeIfAbsent(second, k -> new HashSet<>())
                .add(thread);
        }

        // Build nice, deterministic output (seconds sorted)
        StringBuilder sb = new StringBuilder(512);
        sb.append("=== ReadEvent Concurrency Summary (per second) ===\n")
            .append("shardId=").append(shardId)
            .append(", windowSize=").append(batch.size()).append(" events")
            .append(", at ").append(Instant.now()).append('\n');

        // 1) Per PHASE
        sb.append("---- Per Phase ----\n");
        for (String phase : sortedKeys(byPhaseSecond)) {
            Map<Long, Set<String>> secMap = byPhaseSecond.get(phase);
            sb.append("phase=").append(phase).append('\n');
            for (Long sec : new TreeSet<>(secMap.keySet())) {
                int threads = secMap.get(sec).size();
                sb.append("  ").append(formatSecond(sec)).append("  threads=").append(threads).append('\n');
            }
            // Optional: min/avg/max concurrency over the window
            IntSummaryStatistics stats = secMap.values().stream().mapToInt(Set::size).summaryStatistics();
            sb.append("  summary: min=").append(stats.getMin())
                .append(", avg=").append(String.format(Locale.ROOT, "%.2f", stats.getAverage()))
                .append(", max=").append(stats.getMax()).append('\n');
        }

        // 2) Per PHASE + SEGMENT GENERATION
        sb.append("---- Per Phase + SegGen ----\n");
        for (String phase : sortedKeys(byPhaseSegSecond)) {
            Map<String, Map<Long, Set<String>>> segMap = byPhaseSegSecond.get(phase);
            sb.append("phase=").append(phase).append('\n');
            for (String segGen : sortedKeys(segMap)) {
                Map<Long, Set<String>> secMap = segMap.get(segGen);
                sb.append("  segGen=").append(segGen).append('\n');
                for (Long sec : new TreeSet<>(secMap.keySet())) {
                    int threads = secMap.get(sec).size();
                    sb.append("    ").append(formatSecond(sec)).append("  threads=").append(threads).append('\n');
                }
                IntSummaryStatistics stats = secMap.values().stream().mapToInt(Set::size).summaryStatistics();
                sb.append("    summary: min=").append(stats.getMin())
                    .append(", avg=").append(String.format(Locale.ROOT, "%.2f", stats.getAverage()))
                    .append(", max=").append(stats.getMax()).append('\n');
            }
        }

        // Trim
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) == '\n') sb.setLength(sb.length() - 1);
        logger.info(sb.toString());
    }

    private static String nonNull(String s) {
        return s == null ? "(null)" : s;
    }

    private static <T extends Comparable<T>> List<T> sortedKeys(Map<T, ?> m) {
        return m.keySet().stream().sorted().collect(Collectors.toList());
    }

    private static String formatSecond(long epochSecond) {
        return SECOND_FMT.format(Instant.ofEpochSecond(epochSecond));
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (task != null) task.cancel(false);
        scheduler.shutdown();
        try {
            flushInternal(); // final flush
            if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
    }
}
