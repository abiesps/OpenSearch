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

    // singleton with 30s flush
    public static ReadEventLogger instance = new ReadEventLogger(Duration.ofSeconds(30));

    // max characters per log event (configurable via -DreadEventLogger.chunkSize=...)
    private static final int LOG_CHUNK_SIZE =
        Math.max(1024, Integer.getInteger("readEventLogger.chunkSize", 16_000));

    private final Duration flushInterval;
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> task;
    private volatile boolean closed;

    /** Raw event buffers per shard; we aggregate on flush. */
    private final ConcurrentMap<Integer, Queue<ReadEvent>> eventsByShard = new ConcurrentHashMap<>();

    /** Private default (unused externally) */
    private ReadEventLogger() {
        this(Duration.ofSeconds(50000));
    }

    private ReadEventLogger(Duration flushInterval) {
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
        if (closed) return; // drop after close
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
              //  flushInternal();
            } catch (Throwable t) {
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
     *  1) Per shard + phase + second: distinct threads (IO concurrency)
     *  2) Per shard + phase + segGen + second: distinct threads
     *
     * Assumes readTime is epoch millis.
     */
    private void logConcurrencySummaries(int shardId, List<ReadEvent> batch) {
        // phase -> second -> threads
        Map<String, Map<Long, Set<String>>> byPhaseSecond = new HashMap<>();

        // phase -> segGen -> second -> threads
        Map<String, Map<String, Map<Long, Set<String>>>> byPhaseSegSecond = new HashMap<>();

        for (ReadEvent e : batch) {
            String phase = nonNull(e.getPhaseName());
            String segGen = nonNull(e.getSegGen());
            String thread = nonNull(e.getThreadName());
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

        // Build deterministic output
        StringBuilder sb = new StringBuilder(1024);
        sb.append("=== ReadEvent Concurrency Summary (per second) ===\n")
            .append("shardId=").append(shardId)
            .append(", windowSize=").append(batch.size()).append(" events")
            .append(", at ").append(Instant.now()).append('\n');

        // Per Phase
        sb.append("---- Per Phase ----\n");
        for (String phase : sortedKeys(byPhaseSecond)) {
            Map<Long, Set<String>> secMap = byPhaseSecond.get(phase);
            sb.append("phase=").append(phase).append('\n');
            for (Long sec : new TreeSet<>(secMap.keySet())) {
                int threads = secMap.get(sec).size();
                sb.append("  ").append(formatSecond(sec)).append("  threads=").append(threads).append('\n');
            }
            IntSummaryStatistics stats = secMap.values().stream().mapToInt(Set::size).summaryStatistics();
            sb.append("  summary: min=").append(stats.getMin())
                .append(", avg=").append(String.format(Locale.ROOT, "%.2f", stats.getAverage()))
                .append(", max=").append(stats.getMax()).append('\n');
        }

        // Per Phase + SegGen
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

        // emit in chunks (no truncation)
        logInfoChunked(sb.toString());
    }

    /** Splits very large log messages into parts so nothing gets truncated downstream. */
    private static void logInfoChunked(String msg) {
        if (msg == null) return;
        final int len = msg.length();
        if (len <= LOG_CHUNK_SIZE) {
            logger.info(msg);
            return;
        }
        final int parts = (len + LOG_CHUNK_SIZE - 1) / LOG_CHUNK_SIZE;
        int start = 0;
        int part = 1;
        while (start < len) {
            int end = Math.min(start + LOG_CHUNK_SIZE, len);
            // try not to split mid-line if possible
            int lastNl = msg.lastIndexOf('\n', end - 1);
            if (lastNl > start && (end - lastNl) < 256) { // small look-back to keep chunks tidy
                end = lastNl + 1;
            }
            String chunk = msg.substring(start, end);
            logger.info("[read-event-logger chunk {}/{}]\n{}", part, parts, chunk);
            start = end;
            part++;
        }
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
