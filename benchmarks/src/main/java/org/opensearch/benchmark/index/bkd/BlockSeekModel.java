/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.index.bkd;

/**
 * A self-contained model of a balanced BKD (block KD-tree) used to compare the number of distinct
 * storage <em>block seeks</em> incurred while navigating the tree under two on-disk node orderings:
 *
 * <ul>
 *   <li>{@link Layout#LEVEL_ORDER} &ndash; a flat, breadth-first ordering. In a complete binary tree
 *       this places all internal nodes first (level by level) and all leaf blocks contiguously at the
 *       tail in left-to-right order. The contiguous in-order leaf tail approximates how Lucene's
 *       {@code BKDWriter} serializes leaf blocks today, and the level-order interior approximates a
 *       straightforward flat index serialization. This is the "regular BKD" baseline.</li>
 *   <li>{@link Layout#VAN_EMDE_BOAS} &ndash; the cache-oblivious recursive layout. The tree is split at
 *       half its height; the top sub-tree is laid out contiguously, then each bottom sub-tree
 *       (~&radic;N nodes) is laid out contiguously and recursively. A root-to-leaf descent then touches
 *       {@code O(log_B N)} blocks for <em>any</em> block size {@code B}, without the layout knowing
 *       {@code B}.</li>
 * </ul>
 *
 * <p><b>What this models, and what it does not.</b> This is a layout/IO-locality model, not a
 * byte-exact reproduction of the Lucene 10 points format. Every tree node (internal split node or leaf
 * block header) is treated as a fixed-size record of {@code bytesPerNode} bytes placed at a slot in a
 * single simulated file. A "block" is a fixed {@code blockSize}-byte fetch unit (a page, an SSD block,
 * or a remote object-store range). A query's cost is the count of <em>distinct</em> blocks that the
 * accessed nodes fall into &mdash; i.e. the number of read operations a paged Directory would issue if
 * nothing were already cached. Real Lucene interior-node packing and variable leaf encoding differ; the
 * salient contrast preserved here is flat ordering vs. vEB recursive ordering.
 *
 * <p>The logical tree is a complete binary tree addressed with 1-based heap indices: the root is id
 * {@code 1}, and a node {@code i} has children {@code 2i} and {@code 2i+1}. With {@code numLeaves} a
 * power of two, the leaves occupy ids {@code numLeaves .. 2*numLeaves-1} and form the deepest level.
 */
public final class BlockSeekModel {

    /** On-disk ordering of tree nodes. */
    public enum Layout {
        /** Breadth-first / flat ordering (regular BKD baseline). */
        LEVEL_ORDER,
        /** Cache-oblivious van Emde Boas recursive ordering. */
        VAN_EMDE_BOAS
    }

    private final int numLeaves;     // power of two
    private final int height;        // number of levels including the leaf level
    private final int numNodes;      // 2 * numLeaves - 1
    private final int bytesPerNode;
    private final int blockSize;
    private final Layout layout;

    /** slotOf[id] = 0-based position of logical node {@code id} in the simulated file. Index 0 unused. */
    private final int[] slotOf;

    public BlockSeekModel(int numLeaves, int bytesPerNode, int blockSize, Layout layout) {
        if (Integer.bitCount(numLeaves) != 1) {
            throw new IllegalArgumentException("numLeaves must be a power of two, got " + numLeaves);
        }
        this.numLeaves = numLeaves;
        this.height = Integer.numberOfTrailingZeros(numLeaves) + 1; // log2(numLeaves) + 1
        this.numNodes = 2 * numLeaves - 1;
        this.bytesPerNode = bytesPerNode;
        this.blockSize = blockSize;
        this.layout = layout;
        this.slotOf = new int[numNodes + 1];
        assignSlots();
    }

    private void assignSlots() {
        if (layout == Layout.LEVEL_ORDER) {
            // Heap numbering is already breadth-first: slot = id - 1.
            for (int id = 1; id <= numNodes; id++) {
                slotOf[id] = id - 1;
            }
        } else {
            veb(1, height, new int[] { 0 });
        }
    }

    /**
     * Emits nodes of the complete sub-tree rooted at {@code root} (of {@code subHeight} levels) in van
     * Emde Boas order, assigning each a monotonically increasing slot. Splitting at half the height and
     * laying out the top sub-tree before each bottom sub-tree yields the recursive &radic;N chunking.
     */
    private void veb(long root, int subHeight, int[] cursor) {
        if (subHeight == 1) {
            slotOf[(int) root] = cursor[0]++;
            return;
        }
        int topH = (subHeight + 1) / 2;   // ceil(h/2)
        int botH = subHeight - topH;      // floor(h/2)
        veb(root, topH, cursor);          // top sub-tree, contiguous
        long base = root << topH;         // first node at relative depth topH = a bottom sub-tree root
        long count = 1L << topH;          // there are 2^topH bottom sub-trees
        for (long i = 0; i < count; i++) {
            veb(base + i, botH, cursor);  // each bottom sub-tree, contiguous and recursive
        }
    }

    /** Block (fetch unit) that logical node {@code id} resides in. */
    private int blockOf(int id) {
        long offset = (long) slotOf[id] * bytesPerNode;
        return (int) (offset / blockSize);
    }

    /** Logical heap id of the {@code leafIndex}-th leaf (0-based, left to right). */
    private int leafId(int leafIndex) {
        return numLeaves + leafIndex;
    }

    /**
     * Distinct block seeks for a point descent to a single leaf: the root-to-leaf path of interior
     * split nodes plus the target leaf block.
     */
    public int pointQuerySeeks(int leafIndex) {
        int id = leafId(leafIndex);
        // Path length is at most `height`; track distinct blocks with a small scratch array.
        int[] blocks = new int[height];
        int n = 0;
        while (id >= 1) {
            int b = blockOf(id);
            if (!contains(blocks, n, b)) {
                blocks[n++] = b;
            }
            id >>= 1; // move to parent
        }
        return n;
    }

    /**
     * Distinct block seeks for a range query that matches the contiguous leaf range
     * {@code [loLeaf, hiLeaf]} (inclusive): every interior node on any root-to-leaf path into the range,
     * plus every matched leaf block.
     */
    public int rangeQuerySeeks(int loLeaf, int hiLeaf) {
        // Collect distinct blocks for all visited nodes. Bounded by (#leaves in range) * height, deduped.
        java.util.HashSet<Integer> blocks = new java.util.HashSet<>();
        for (int leaf = loLeaf; leaf <= hiLeaf; leaf++) {
            int id = leafId(leaf);
            while (id >= 1) {
                blocks.add(blockOf(id));
                id >>= 1;
            }
        }
        return blocks.size();
    }

    private static boolean contains(int[] a, int len, int v) {
        for (int i = 0; i < len; i++) {
            if (a[i] == v) {
                return true;
            }
        }
        return false;
    }

    public int numLeaves() {
        return numLeaves;
    }

    public int height() {
        return height;
    }

    public int numNodes() {
        return numNodes;
    }

    /** Smallest power of two &ge; {@code v} (and &ge; 1). */
    public static int nextPowerOfTwo(int v) {
        if (v <= 1) {
            return 1;
        }
        return Integer.highestOneBit(v - 1) << 1;
    }
}
