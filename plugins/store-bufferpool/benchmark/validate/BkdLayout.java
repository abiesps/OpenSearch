
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;

/**
 * Prints the byte layout of a single-segment index's BKD points (Lucene90 points format) as JSON: per points field, the
 * tree config, the inner-node index range in {@code .kdi}, the leaf range in {@code .kdd}, and every leaf in file order
 * with its file pointer, point count, the bytes of its doc ID block and of its value block, the doc ID encoding, the
 * min/max doc ID, the bounds the index gives the leaf and the leaf's tight (actual) min/max value. Also emits
 * "regions" in the format of DocValuesLayout.java ({@code field:points-index} in .kdi, {@code field:points-leaves} in
 * .kdd) so dv_trace.py can attribute points loads to fields. Reads private BKDReader state by reflection; opens the
 * index read-only. Only 1-dimension long fields decode values as longs (other fields report bytes only).
 *
 * <p>Fields in the split points format ({@code Lucene90Split}, files {@code _N_Lucene90Split_0.{kdm,kdi,kdd,kdv}}) are
 * reported from the split {@code .kdm} metadata the reader holds: regions {@code points-split-index} (inner nodes) and
 * {@code points-split-dir} (leaf directory) in the split {@code .kdi}, {@code points-split-docs} in the split
 * {@code .kdd} and {@code points-split-values} in {@code .kdv}, and for the listed fields every leaf's directory entry
 * (leaf ID in value order, count, tight min/max, min/max doc ID, doc block FP/length, value block FP/length). The
 * "split" object gives the four file sizes and the share of {@code .kdi} in {@code .kdd + .kdv}.
 *
 * <p>Run (Java 21+): java -cp 'DISTRO/lib/*' validate/BkdLayout.java SHARD_INDEX_DIR [OUT.json] [--leaves FIELD,...]
 * (per-leaf arrays are written only for the listed fields; default @timestamp).
 */
public class BkdLayout {
    public static void main(String[] args) throws Exception {
        final Path dir = Path.of(args[0]);
        final PrintStream out = args.length > 1 && args[1].startsWith("--") == false ? new PrintStream(args[1]) : System.out;
        List<String> leafFields = List.of("@timestamp");
        for (int i = 1; i < args.length - 1; i++) {
            if (args[i].equals("--leaves")) {
                leafFields = Arrays.asList(args[i + 1].split(","));
            }
        }
        try (Directory d = new NIOFSDirectory(dir); DirectoryReader reader = DirectoryReader.open(d)) {
            if (reader.leaves().size() != 1) {
                throw new IllegalStateException("expected one segment, found " + reader.leaves().size());
            }
            final LeafReader leaf = reader.leaves().get(0).reader();
            // stock points files are _N.kdd/_N.kdi; the split format's are _N_Lucene90Split_0.*
            String kdd = null, kdi = null;
            final String segment = ((org.apache.lucene.index.SegmentReader) leaf).getSegmentName();
            for (String f : d.listAll()) {
                if (f.equals(segment + ".kdd")) kdd = f;
                if (f.equals(segment + ".kdi")) kdi = f;
            }
            final String splitPrefix = segment + "_Lucene90Split_0.";
            final long kddLength = kdd == null ? 0 : Files.size(dir.resolve(kdd));
            final long kdiLength = kdi == null ? 0 : Files.size(dir.resolve(kdi));
            // leaf FPs of every field, to find where each leaf ends (the next leaf, of any field, or the footer)
            final List<FieldLayout> fields = new ArrayList<>();
            final TreeMap<Long, Boolean> allFps = new TreeMap<>();
            final List<SplitFieldLayout> splitFields = new ArrayList<>();
            for (FieldInfo fi : leaf.getFieldInfos()) {
                if (fi.getPointDimensionCount() == 0) continue;
                final PointValues pv = leaf.getPointValues(fi.name);
                if (pv.getClass().getSimpleName().equals("SplitBKDReader")) {
                    splitFields.add(new SplitFieldLayout(fi.name, pv, splitPrefix, leafFields.contains(fi.name)));
                    continue;
                }
                final FieldLayout fl = new FieldLayout(fi.name, pv, fi.getPointNumBytes());
                fl.walk(leafFields.contains(fi.name));
                for (long fp : fl.fps)
                    allFps.put(fp, true);
                fields.add(fl);
            }
            final long footer = kddLength - 16;
            out.println("{");
            out.printf(
                Locale.ROOT,
                "\"kdd\": \"%s\", \"kdd_bytes\": %d, \"kdi\": \"%s\", \"kdi_bytes\": %d,%n",
                kdd,
                kddLength,
                kdi,
                kdiLength
            );
            if (splitFields.isEmpty() == false) {
                final long[] len = new long[4];
                final String[] ext = { "kdm", "kdi", "kdd", "kdv" };
                for (int i = 0; i < 4; i++)
                    len[i] = Files.size(dir.resolve(splitPrefix + ext[i]));
                out.printf(
                    Locale.ROOT,
                    "\"split\": {\"prefix\": \"%s\", \"kdm_bytes\": %d, \"kdi_bytes\": %d, \"kdd_bytes\": %d, \"kdv_bytes\": %d, "
                        + "\"kdi_share_of_kdd_kdv\": %.5f},%n",
                    splitPrefix,
                    len[0],
                    len[1],
                    len[2],
                    len[3],
                    (double) len[1] / (len[2] + len[3])
                );
                out.println("\"split_fields\": [");
                for (int i = 0; i < splitFields.size(); i++) {
                    splitFields.get(i).print(out, i == splitFields.size() - 1);
                }
                out.println("],");
            }
            out.println("\"fields\": [");
            final StringBuilder regions = new StringBuilder();
            for (SplitFieldLayout sf : splitFields) {
                sf.regions(regions);
            }
            for (int f = 0; f < fields.size(); f++) {
                final FieldLayout fl = fields.get(f);
                final long[] ends = new long[fl.fps.size()];
                for (int i = 0; i < ends.length; i++) {
                    final Long next = allFps.higherKey(fl.fps.get(i));
                    ends[i] = next == null ? footer : next;
                }
                fl.print(out, ends, f == fields.size() - 1);
                final long dataStart = fl.fps.get(0), dataEnd = ends[ends.length - 1];
                regions.append(
                    String.format(
                        Locale.ROOT,
                        "{\"field\": \"%s\", \"region\": \"points-leaves\", \"file\": \"%s\", \"start\": %d, \"end\": %d, \"info\": \"%d leaves\"},%n",
                        fl.name,
                        kdd,
                        dataStart,
                        dataEnd,
                        fl.numLeaves
                    )
                );
                regions.append(
                    String.format(
                        Locale.ROOT,
                        "{\"field\": \"%s\", \"region\": \"points-index\", \"file\": \"%s\", \"start\": %d, \"end\": %d, \"info\": \"\"}%s%n",
                        fl.name,
                        kdi,
                        fl.indexStart,
                        fl.indexStart + fl.indexBytes,
                        f == fields.size() - 1 ? "" : ","
                    )
                );
            }
            out.println("],");
            out.println("\"regions\": [");
            // the last region line must not end with a comma (only split fields: their last line does)
            String r = regions.toString().stripTrailing();
            if (r.endsWith(",")) r = r.substring(0, r.length() - 1);
            out.println(r);
            out.println("]}");
        }
    }

    static Object get(Object o, String name) throws ReflectiveOperationException {
        Class<?> c = o.getClass();
        while (c != null) {
            try {
                final Field f = c.getDeclaredField(name);
                f.setAccessible(true);
                return f.get(o);
            } catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    static Method method(Object o, String name, Class<?>... types) throws ReflectiveOperationException {
        final Method m = o.getClass().getDeclaredMethod(name, types);
        m.setAccessible(true);
        return m;
    }

    static long decodeLong(byte[] b, int off) {
        long v = 0;
        for (int i = 0; i < 8; i++)
            v = (v << 8) | (b[off + i] & 0xFF);
        return v ^ 0x8000000000000000L;
    }

    /** A field in the split points format: regions from the reader's metadata, leaf directory entries on request. */
    static final class SplitFieldLayout {
        final String name, prefix;
        final PointValues pv;
        final int numLeaves, numPages, pageShift, numIndexBytes;
        final long pointCount, dataStart, docDataEnd, valDataStart, valDataEnd, indexStart, directoryStart, directoryEnd;
        final int docCount;
        final List<Object> entries = new ArrayList<>();

        SplitFieldLayout(String name, PointValues pv, String prefix, boolean details) throws Exception {
            this.name = name;
            this.pv = pv;
            this.prefix = prefix;
            this.numLeaves = (int) get(pv, "numLeaves");
            this.numPages = (int) get(pv, "numPages");
            this.pageShift = (int) get(pv, "pageShift");
            this.numIndexBytes = (int) get(pv, "numIndexBytes");
            this.pointCount = pv.size();
            this.docCount = pv.getDocCount();
            this.dataStart = (long) get(pv, "dataStartFP");
            this.docDataEnd = (long) get(pv, "docDataEndFP");
            this.valDataStart = (long) get(pv, "valDataStartFP");
            this.valDataEnd = (long) get(pv, "valDataEndFP");
            this.indexStart = (long) get(pv, "indexStartFP");
            this.directoryStart = (long) get(pv, "directoryStartFP");
            this.directoryEnd = (long) get(pv, "directoryEndFP");
            if (details) {
                final Method leafEntry = pv.getClass().getMethod("leafEntry", int.class);
                for (int i = 0; i < numLeaves; i++) {
                    entries.add(leafEntry.invoke(pv, i));
                }
            }
        }

        void regions(StringBuilder regions) {
            final String fmt = "{\"field\": \"%s\", \"region\": \"%s\", \"file\": \"%s\", \"start\": %d, \"end\": %d, \"info\": \"%s\"},%n";
            regions.append(
                String.format(Locale.ROOT, fmt, name, "points-split-index", prefix + "kdi", indexStart, indexStart + numIndexBytes, "")
            );
            regions.append(
                String.format(Locale.ROOT, fmt, name, "points-split-dir", prefix + "kdi", directoryStart, directoryEnd, numPages + " pages")
            );
            regions.append(
                String.format(Locale.ROOT, fmt, name, "points-split-docs", prefix + "kdd", dataStart, docDataEnd, numLeaves + " leaves")
            );
            regions.append(
                String.format(
                    Locale.ROOT,
                    fmt,
                    name,
                    "points-split-values",
                    prefix + "kdv",
                    valDataStart,
                    valDataEnd,
                    numLeaves + " leaves"
                )
            );
        }

        void print(PrintStream out, boolean last) throws Exception {
            out.printf(
                Locale.ROOT,
                "{\"field\": \"%s\", \"format\": \"Lucene90Split\", \"num_leaves\": %d, \"point_count\": %d, \"doc_count\": %d, "
                    + "\"index_start\": %d, \"index_bytes\": %d, \"directory_start\": %d, \"directory_end\": %d, "
                    + "\"directory_bytes\": %d, \"pages\": %d, \"leaves_per_page\": %d, \"docs_start\": %d, \"docs_end\": %d, "
                    + "\"values_start\": %d, \"values_end\": %d",
                name,
                numLeaves,
                pointCount,
                docCount,
                indexStart,
                numIndexBytes,
                directoryStart,
                directoryEnd,
                directoryEnd - directoryStart,
                numPages,
                1 << pageShift,
                dataStart,
                docDataEnd,
                valDataStart,
                valDataEnd
            );
            if (entries.isEmpty() == false) {
                out.print(
                    ",\n \"leaf_columns\": [\"leaf_id\", \"count\", \"min\", \"max\", \"min_doc\", \"max_doc\", \"doc_fp\", "
                        + "\"doc_bytes\", \"value_fp\", \"value_bytes\"],\n \"leaves\": ["
                );
                final boolean isLong = pv.getBytesPerDimension() == 8;
                for (int i = 0; i < entries.size(); i++) {
                    final Object e = entries.get(i);
                    final byte[] min = (byte[]) call(e, "minPackedValue"), max = (byte[]) call(e, "maxPackedValue");
                    out.printf(
                        Locale.ROOT,
                        "%s[%d,%d,%s,%s,%d,%d,%d,%d,%d,%d]",
                        i == 0 ? "" : ",",
                        i,
                        (int) call(e, "count"),
                        isLong ? Long.toString(decodeLong(min, 0)) : "null",
                        isLong ? Long.toString(decodeLong(max, 0)) : "null",
                        (int) call(e, "minDocID"),
                        (int) call(e, "maxDocID"),
                        (long) call(e, "docBlockFP"),
                        (int) call(e, "docBlockLength"),
                        (long) call(e, "valueBlockFP"),
                        (int) call(e, "valueBlockLength")
                    );
                }
                out.print("]");
            }
            out.println(last ? "}" : "},");
        }

        static Object call(Object o, String name) throws ReflectiveOperationException {
            return o.getClass().getMethod(name).invoke(o);
        }
    }

    static final class FieldLayout {
        final String name;
        final PointValues pv;
        final int bytesPerDim, numDims, numIndexDims, maxPointsInLeaf, numLeaves;
        final long indexStart, indexBytes, pointCount;
        final int docCount;
        final List<Long> fps = new ArrayList<>();
        // per leaf (only when walked with details)
        final List<long[]> leafInfo = new ArrayList<>(); // count, docIdsEnd, type, minDoc, maxDoc, idxMin, idxMax, min, max

        FieldLayout(String name, PointValues pv, int bytesPerDim) throws ReflectiveOperationException, IOException {
            this.name = name;
            this.pv = pv;
            this.bytesPerDim = bytesPerDim;
            this.numDims = pv.getNumDimensions();
            this.numIndexDims = pv.getNumIndexDimensions();
            final Object config = get(pv, "config");
            this.maxPointsInLeaf = (int) config.getClass().getMethod("maxPointsInLeafNode").invoke(config);
            this.numLeaves = (int) get(pv, "numLeaves");
            this.indexStart = (long) get(pv, "indexStartPointer");
            this.indexBytes = (int) get(pv, "numIndexBytes");
            this.pointCount = pv.size();
            this.docCount = pv.getDocCount();
        }

        boolean isLong() {
            return numDims == 1 && bytesPerDim == 8;
        }

        void walk(boolean details) throws Exception {
            final PointValues.PointTree tree = pv.getPointTree();
            final Method fpMethod = method(tree, "getLeafBlockFP");
            final IndexInput leafNodes = (IndexInput) get(tree, "leafNodes");
            final Object scratch = get(tree, "scratchIterator");
            final Method readDocIDs = method(tree, "readDocIDs", IndexInput.class, long.class, scratch.getClass());
            walk(tree, fpMethod, leafNodes, scratch, readDocIDs, details);
            if (fps.size() != numLeaves) throw new IllegalStateException(
                name + ": walked " + fps.size() + " leaves, expected " + numLeaves
            );
        }

        private void walk(
            PointValues.PointTree tree,
            Method fpMethod,
            IndexInput leafNodes,
            Object scratch,
            Method readDocIDs,
            boolean details
        ) throws Exception {
            if (tree.moveToChild()) {
                do {
                    walk(tree, fpMethod, leafNodes, scratch, readDocIDs, details);
                } while (tree.moveToSibling());
                tree.moveToParent();
                return;
            }
            final long fp = (long) fpMethod.invoke(tree);
            if (fps.isEmpty() == false && fp <= fps.get(fps.size() - 1)) throw new IllegalStateException("leaves out of file order");
            fps.add(fp);
            if (details == false) return;
            leafNodes.seek(fp);
            final int count = leafNodes.readVInt();
            final int type = leafNodes.readByte();
            final int n = (int) readDocIDs.invoke(tree, leafNodes, fp, scratch);
            final long docIdsEnd = leafNodes.getFilePointer();
            final int[] docs = (int[]) get(scratch, "docIDs");
            int minDoc = Integer.MAX_VALUE, maxDoc = -1;
            for (int i = 0; i < n; i++) {
                minDoc = Math.min(minDoc, docs[i]);
                maxDoc = Math.max(maxDoc, docs[i]);
            }
            long idxMin = 0, idxMax = 0;
            final long[] mm = { Long.MAX_VALUE, Long.MIN_VALUE };
            if (isLong()) {
                idxMin = decodeLong(tree.getMinPackedValue(), 0);
                idxMax = decodeLong(tree.getMaxPackedValue(), 0);
                tree.visitDocValues(new PointValues.IntersectVisitor() {
                    @Override
                    public void visit(int docID) {
                        throw new AssertionError();
                    }

                    @Override
                    public void visit(int docID, byte[] packedValue) {
                        final long v = decodeLong(packedValue, 0);
                        mm[0] = Math.min(mm[0], v);
                        mm[1] = Math.max(mm[1], v);
                    }

                    @Override
                    public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                        final long v = decodeLong(packedValue, 0);
                        mm[0] = Math.min(mm[0], v);
                        mm[1] = Math.max(mm[1], v);
                    }

                    @Override
                    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                        return PointValues.Relation.CELL_CROSSES_QUERY;
                    }
                });
            }
            leafInfo.add(new long[] { count, docIdsEnd, type, minDoc, maxDoc, idxMin, idxMax, mm[0], mm[1] });
        }

        void print(PrintStream out, long[] ends, boolean last) {
            out.printf(
                Locale.ROOT,
                "{\"field\": \"%s\", \"num_dims\": %d, \"num_index_dims\": %d, \"bytes_per_dim\": %d, \"max_points_in_leaf\": %d, "
                    + "\"num_leaves\": %d, \"point_count\": %d, \"doc_count\": %d, \"index_start\": %d, \"index_bytes\": %d, "
                    + "\"leaves_start\": %d, \"leaves_end\": %d",
                name,
                numDims,
                numIndexDims,
                bytesPerDim,
                maxPointsInLeaf,
                numLeaves,
                pointCount,
                docCount,
                indexStart,
                indexBytes,
                fps.get(0),
                ends[ends.length - 1]
            );
            if (leafInfo.isEmpty() == false) {
                // columns: fp, count, doc ID bytes (count vint + doc IDs), value bytes, doc ID encoding, min doc, max doc,
                // index min, index max, tight min, tight max
                out.print(
                    ",\n \"leaf_columns\": [\"fp\", \"count\", \"doc_bytes\", \"value_bytes\", \"doc_encoding\", \"min_doc\", "
                        + "\"max_doc\", \"index_min\", \"index_max\", \"min\", \"max\"],\n \"leaves\": ["
                );
                for (int i = 0; i < leafInfo.size(); i++) {
                    final long[] l = leafInfo.get(i);
                    final long fp = fps.get(i);
                    out.printf(
                        Locale.ROOT,
                        "%s[%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d]",
                        i == 0 ? "" : ",",
                        fp,
                        l[0],
                        l[1] - fp,
                        ends[i] - l[1],
                        l[2],
                        l[3],
                        l[4],
                        l[5],
                        l[6],
                        l[7],
                        l[8]
                    );
                }
                out.print("]");
            }
            out.println(last ? "}" : "},");
        }
    }
}
