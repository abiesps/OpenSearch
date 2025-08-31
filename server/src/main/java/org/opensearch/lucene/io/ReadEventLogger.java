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

    private void logConcurrencySummaries(int shardId, List<ReadEvent> batch) {
        // phase -> second -> threads
        Map<String, Map<Long, Set<String>>> byPhaseSecond = new HashMap<>();

        // phase -> segGen -> second -> threads
        Map<String, Map<String, Map<Long, Set<String>>>> byPhaseSegSecond = new HashMap<>();

        // Raw event counts per phase/second
        Map<String, Map<Long, Integer>> eventsPerPhaseSecond = new HashMap<>();

        // NEW: totals per phase + segGen (all threads)
        Map<String, Map<String, Integer>> totalEventsByPhaseSeg = new HashMap<>();

        // NEW: totals per phase + segGen from SEARCH threads only
        Map<String, Map<String, Integer>> searchEventsByPhaseSeg = new HashMap<>();
        Map<String, Map<String, Set<String>>> searchThreadsByPhaseSeg = new HashMap<>();

        for (ReadEvent e : batch) {
            String phase = nonNull(e.getPhaseName());
            String segGen = nonNull(e.getSegGen());
            String thread = nonNull(e.getThreadName());
            long second = TimeUnit.MILLISECONDS.toSeconds(e.getReadTime()); // epoch millis -> second

            // existing aggregations
            byPhaseSecond
                .computeIfAbsent(phase, k -> new HashMap<>())
                .computeIfAbsent(second, k -> new HashSet<>())
                .add(thread);

            byPhaseSegSecond
                .computeIfAbsent(phase, k -> new HashMap<>())
                .computeIfAbsent(segGen, k -> new HashMap<>())
                .computeIfAbsent(second, k -> new HashSet<>())
                .add(thread);

            eventsPerPhaseSecond
                .computeIfAbsent(phase, k -> new HashMap<>())
                .merge(second, 1, Integer::sum);

            // NEW: totals for this segGen (all threads)
            totalEventsByPhaseSeg
                .computeIfAbsent(phase, k -> new HashMap<>())
                .merge(segGen, 1, Integer::sum);

            // NEW: search-thread-only tallies
            if (isSearchThread(thread)) {
                searchEventsByPhaseSeg
                    .computeIfAbsent(phase, k -> new HashMap<>())
                    .merge(segGen, 1, Integer::sum);

                searchThreadsByPhaseSeg
                    .computeIfAbsent(phase, k -> new HashMap<>())
                    .computeIfAbsent(segGen, k -> new HashSet<>())
                    .add(thread);
            }
        }

        StringBuilder sb = new StringBuilder(1024);
        sb.append("=== ReadEvent Concurrency Summary (per second) ===\n")
            .append("shardId=").append(shardId)
            .append(", windowSize=").append(batch.size()).append(" events")
            .append(", at ").append(Instant.now()).append('\n');

        // ---- Per Phase ----
        sb.append("---- Per Phase ----\n");
        for (String phase : sortedKeys(byPhaseSecond)) {
            Map<Long, Set<String>> secMap = byPhaseSecond.get(phase);
            sb.append("phase=").append(phase).append('\n');
            for (Long sec : new TreeSet<>(secMap.keySet())) {
                Set<String> names = secMap.get(sec);
                int threads = names.size();
                sb.append("  ").append(formatSecond(sec))
                    .append("  threads=").append(threads)
                    .append("  threadNames=").append(formatThreadList(names))
                    .append('\n');
            }
            IntSummaryStatistics stats = secMap.values().stream().mapToInt(Set::size).summaryStatistics();
            sb.append("  summary: min=").append(stats.getMin())
                .append(", avg=").append(String.format(Locale.ROOT, "%.2f", stats.getAverage()))
                .append(", max=").append(stats.getMax()).append('\n');
        }

        // ---- Per Phase + SegGen ----
        sb.append("---- Per Phase + SegGen ----\n");
        for (String phase : sortedKeys(byPhaseSegSecond)) {
            Map<String, Map<Long, Set<String>>> segMap = byPhaseSegSecond.get(phase);
            sb.append("phase=").append(phase).append('\n');
            for (String segGen : sortedKeys(segMap)) {
                Map<Long, Set<String>> secMap = segMap.get(segGen);
                sb.append("  segGen=").append(segGen).append('\n');
                for (Long sec : new TreeSet<>(secMap.keySet())) {
                    Set<String> names = secMap.get(sec);
                    int threads = names.size();
                    sb.append("    ").append(formatSecond(sec))
                        .append("  threads=").append(threads)
                        .append("  threadNames=").append(formatThreadList(names))
                        .append('\n');
                }
                IntSummaryStatistics stats = secMap.values().stream().mapToInt(Set::size).summaryStatistics();
                sb.append("    summary: min=").append(stats.getMin())
                    .append(", avg=").append(String.format(Locale.ROOT, "%.2f", stats.getAverage()))
                    .append(", max=").append(stats.getMax()).append('\n');
            }
        }

        // ---- Totals Per Second (threads + active segs + events + threadNames) ----
        sb.append("---- Totals Per Second ----\n");
        for (String phase : sortedKeys(byPhaseSecond)) {
            sb.append("phase=").append(phase).append('\n');

            Map<Long, Set<String>> threadsPerSecond = byPhaseSecond.get(phase);
            Map<String, Map<Long, Set<String>>> segMap = byPhaseSegSecond.getOrDefault(phase, Map.of());
            Map<Long, Integer> activeSegsPerSecond = new HashMap<>();
            for (Map<Long, Set<String>> secMap : segMap.values()) {
                for (Map.Entry<Long, Set<String>> e : secMap.entrySet()) {
                    if (!e.getValue().isEmpty()) {
                        activeSegsPerSecond.merge(e.getKey(), 1, Integer::sum);
                    }
                }
            }

            TreeSet<Long> seconds = new TreeSet<>();
            seconds.addAll(threadsPerSecond.keySet());
            seconds.addAll(activeSegsPerSecond.keySet());
            seconds.addAll(eventsPerPhaseSecond.getOrDefault(phase, Map.of()).keySet());

            for (Long sec : seconds) {
                Set<String> names = threadsPerSecond.getOrDefault(sec, Set.of());
                int threads = names.size();
                int activeSegs = activeSegsPerSecond.getOrDefault(sec, 0);
                int events = eventsPerPhaseSecond.getOrDefault(phase, Map.of()).getOrDefault(sec, 0);
                sb.append("  ").append(formatSecond(sec))
                    .append("  threads=").append(threads)
                    .append("  activeSegs=").append(activeSegs)
                    .append("  events=").append(events)
                    .append("  threadNames=").append(formatThreadList(names))
                    .append('\n');
            }
        }

        // ---- NEW: Search Threads: IO by SegGen ----
        sb.append("---- Search Threads: IO by SegGen ----\n");
        for (String phase : sortedKeys(searchEventsByPhaseSeg)) {
            sb.append("phase=").append(phase).append('\n');

            Map<String, Integer> perSeg = searchEventsByPhaseSeg.get(phase);
            Map<String, Integer> totals = totalEventsByPhaseSeg.getOrDefault(phase, Map.of());
            Map<String, Set<String>> perSegThreads = searchThreadsByPhaseSeg.getOrDefault(phase, Map.of());

            // sort segGens by descending searchEvents (then segGen name)
            List<Map.Entry<String, Integer>> rows = new ArrayList<>(perSeg.entrySet());
            rows.sort((a, b) -> {
                int c = Integer.compare(b.getValue(), a.getValue());
                return c != 0 ? c : a.getKey().compareTo(b.getKey());
            });

            for (Map.Entry<String, Integer> e : rows) {
                String segGen = e.getKey();
                int searchEvents = e.getValue();
                int total = totals.getOrDefault(segGen, 0);
                double pct = total == 0 ? 0.0 : (100.0 * searchEvents / total);
                Set<String> tnames = perSegThreads.getOrDefault(segGen, Set.of());

                sb.append("  segGen=").append(segGen)
                    .append("  searchEvents=").append(searchEvents)
                    .append("  totalEvents=").append(total)
                    .append("  pct=").append(String.format(Locale.ROOT, "%.1f%%", pct))
                    .append("  uniqueThreads=").append(tnames.size())
                    .append("  threadNames=").append(formatThreadList(tnames))
                    .append('\n');
            }
        }

        // Emit (chunked to avoid downstream truncation)
        logInfoChunked(sb.toString());
    }

    /** Treat only the OpenSearch search thread-pool as "search threads". */
    private static boolean isSearchThread(String name) {
        if (name == null) return false;
        // explicitly exclude these
        if (name.startsWith("lookup-thread-")) return false;
        if (name.contains("[index_searcher]")) return false;

        // accept only the search pool, e.g. "...[search][T#11]"
        return name.contains("[search]");
    }


    private static String formatThreadList(Collection<String> threads) {
        if (threads == null || threads.isEmpty()) return "[]";
        return threads.stream()
            .sorted()
            .collect(Collectors.joining(", ", "[", "]"));
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
