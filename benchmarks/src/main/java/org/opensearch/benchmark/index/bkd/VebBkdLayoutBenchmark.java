/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.index.bkd;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares the number of distinct storage block seeks incurred while navigating a balanced BKD tree
 * under the regular flat ({@link BlockSeekModel.Layout#LEVEL_ORDER}) layout versus the cache-oblivious
 * van Emde Boas ({@link BlockSeekModel.Layout#VAN_EMDE_BOAS}) layout.
 *
 * <p>The headline metric is reported through {@link SeekCounters} as auxiliary counters:
 * <ul>
 *   <li>{@code blockSeeks} &ndash; total distinct blocks fetched across all measured queries</li>
 *   <li>{@code queries} &ndash; total queries executed</li>
 *   <li>{@code nodeVisits} &ndash; total tree nodes visited (the CPU-side work)</li>
 * </ul>
 * The figure of interest is <b>blockSeeks / queries</b> (average block seeks per query); because both
 * counters are normalized by JMH in the same way, their ratio is invariant. {@code nodeVisits} is
 * reported so the CPU-vs-IO trade-off is visible: vEB should reduce {@code blockSeeks} at roughly equal
 * {@code nodeVisits}, illustrating that we trade nothing on instruction count while saving IO.
 *
 * <p>Run, for example:
 * <pre>
 *   ./gradlew -p benchmarks run --args 'VebBkdLayoutBenchmark -prof gc'
 *   # or build the uberjar and:
 *   java -jar benchmarks/build/distributions/opensearch-benchmarks.jar VebBkdLayoutBenchmark
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class VebBkdLayoutBenchmark {

    /** Total indexed points; drives the tree height. */
    @Param({ "10000000", "200000000" })
    private int numPoints;

    /** Points per leaf block (Lucene's default max is 512). */
    @Param({ "512" })
    private int pointsPerLeaf;

    /** Fixed-size record per tree node in the simulated file. */
    @Param({ "16" })
    private int bytesPerNode;

    /** Fetch unit: 4 KiB page vs a larger 64 KiB block (e.g. a remote-store fetch granule). */
    @Param({ "4096", "65536" })
    private int blockSize;

    /** Number of contiguous leaves a range query matches. */
    @Param({ "64" })
    private int rangeLeaves;

    @Param
    private BlockSeekModel.Layout layout;

    private BlockSeekModel model;

    // Pre-generated, seeded workloads so seek counts are deterministic and reproducible.
    private static final int WORKLOAD = 4096;
    private int[] pointTargets;
    private int[] rangeLo;
    private int cursor;

    @Setup(Level.Trial)
    public void setup() {
        int numLeaves = BlockSeekModel.nextPowerOfTwo((numPoints + pointsPerLeaf - 1) / pointsPerLeaf);
        model = new BlockSeekModel(numLeaves, bytesPerNode, blockSize, layout);

        Random rnd = new Random(0xB6D7L ^ numPoints ^ ((long) blockSize << 20));
        pointTargets = new int[WORKLOAD];
        rangeLo = new int[WORKLOAD];
        int width = Math.min(rangeLeaves, numLeaves);
        for (int i = 0; i < WORKLOAD; i++) {
            pointTargets[i] = rnd.nextInt(numLeaves);
            rangeLo[i] = (numLeaves > width) ? rnd.nextInt(numLeaves - width) : 0;
        }
    }

    @Benchmark
    public void pointQuery(SeekCounters counters, Blackhole bh) {
        int i = (cursor++ & (WORKLOAD - 1));
        int seeks = model.pointQuerySeeks(pointTargets[i]);
        counters.blockSeeks += seeks;
        counters.nodeVisits += model.height();
        counters.queries += 1;
        bh.consume(seeks);
    }

    @Benchmark
    public void rangeQuery(SeekCounters counters, Blackhole bh) {
        int i = (cursor++ & (WORKLOAD - 1));
        int width = Math.min(rangeLeaves, model.numLeaves());
        int lo = rangeLo[i];
        int seeks = model.rangeQuerySeeks(lo, lo + width - 1);
        counters.blockSeeks += seeks;
        counters.nodeVisits += (long) width * model.height();
        counters.queries += 1;
        bh.consume(seeks);
    }

    /**
     * Auxiliary counters surfaced by JMH alongside the timing score. Reset at the start of every
     * iteration so each measured iteration reports its own totals.
     */
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class SeekCounters {
        public long blockSeeks;
        public long queries;
        public long nodeVisits;

        @Setup(Level.Iteration)
        public void clean() {
            blockSeeks = 0;
            queries = 0;
            nodeVisits = 0;
        }
    }
}
