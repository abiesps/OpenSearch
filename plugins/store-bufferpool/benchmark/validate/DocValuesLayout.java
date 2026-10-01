/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Prints the byte layout of a single-segment index's Lucene90 doc values as JSON: for every field with doc values, the
 * regions of {@code .dvd} (values, value jump table, docs-with-field set, ordinals, terms dictionary, addresses) and of
 * {@code .dvs} (skipper), each as [start, end) byte offsets. Used to attribute traced block loads to fields and regions.
 * Reads the producer's metadata entries by reflection (they are private); opens the index read-only.
 *
 * <p>Run (Java 21+), with the Lucene fork jar and the plugin jar on the classpath:
 * <pre>
 * java -cp lucene-core-10.5.1-SNAPSHOT.jar:store-bufferpool.jar validate/DocValuesLayout.java &lt;shard index dir&gt;
 * </pre>
 */
public class DocValuesLayout {

    record Region(String field, String region, String file, long start, long end, String info) {}

    public static void main(String[] args) throws Exception {
        final List<Region> regions = new ArrayList<>();
        try (Directory dir = new NIOFSDirectory(Path.of(args[0])); DirectoryReader reader = DirectoryReader.open(dir)) {
            if (reader.leaves().size() != 1) {
                throw new IllegalStateException("expected one segment, found " + reader.leaves().size());
            }
            final LeafReaderContext leaf = reader.leaves().get(0);
            final SegmentReader segment = (SegmentReader) leaf.reader();
            final Object perField = segment.getDocValuesReader();
            final Object producersByNumber = get(perField, "fields");
            final Method getProducer = producersByNumber.getClass().getMethod("get", int.class);
            for (FieldInfo fi : segment.getFieldInfos()) {
                if (fi.getDocValuesType() == DocValuesType.NONE) {
                    continue;
                }
                final Object producer = getProducer.invoke(producersByNumber, fi.number);
                if (producer == null || producer.getClass().getSimpleName().equals("Lucene90DocValuesProducer") == false) {
                    regions.add(new Region(fi.name, "unknown-producer", "", 0, 0, String.valueOf(producer)));
                    continue;
                }
                final String dvd = segment.getSegmentName() + "_" + fi.getAttribute("PerFieldDocValuesFormat.format") + "_"
                    + fi.getAttribute("PerFieldDocValuesFormat.suffix") + ".dvd";
                final String dvs = dvd.substring(0, dvd.length() - 1) + "s";
                switch (fi.getDocValuesType()) {
                    case NUMERIC -> numeric(regions, fi.name, "", dvd, entry(producer, "numerics", fi.number));
                    case SORTED_NUMERIC -> sortedNumeric(regions, fi.name, dvd, entry(producer, "sortedNumerics", fi.number));
                    case SORTED -> sorted(regions, fi.name, dvd, entry(producer, "sorted", fi.number));
                    case SORTED_SET -> {
                        final Object e = entry(producer, "sortedSets", fi.number);
                        final Object single = get(e, "singleValueEntry");
                        if (single != null) {
                            sorted(regions, fi.name, dvd, single);
                        } else {
                            sortedNumeric(regions, fi.name + ".ords", dvd, get(e, "ordsEntry"));
                            termsDict(regions, fi.name, dvd, get(e, "termsDictEntry"));
                        }
                    }
                    case BINARY -> {
                        final Object e = entry(producer, "binaries", fi.number);
                        add(regions, fi.name, "binary-data", dvd, (long) get(e, "dataOffset"), (long) get(e, "dataLength"), "");
                        add(regions, fi.name, "binary-addresses", dvd, (long) get(e, "addressesOffset"),
                            (long) get(e, "addressesLength"), "");
                        disi(regions, fi.name, dvd, e);
                    }
                    default -> {}
                }
                final Object skipper = entry(producer, "skippers", fi.number);
                if (skipper != null) {
                    final Method offset = skipper.getClass().getDeclaredMethod("offset");
                    final Method length = skipper.getClass().getDeclaredMethod("length");
                    offset.setAccessible(true);
                    length.setAccessible(true);
                    final boolean ownFile = get(producer, "skipIndexData") != null;
                    add(regions, fi.name, "skipper", ownFile ? dvs : dvd, (long) offset.invoke(skipper), (long) length.invoke(skipper),
                        "");
                }
            }
            final StringBuilder out = new StringBuilder("{\"maxDoc\": ").append(segment.maxDoc()).append(", \"regions\": [\n");
            for (int i = 0; i < regions.size(); i++) {
                final Region r = regions.get(i);
                out.append(String.format(java.util.Locale.ROOT,
                    "  {\"field\": \"%s\", \"region\": \"%s\", \"file\": \"%s\", \"start\": %d, \"end\": %d, \"info\": \"%s\"}%s%n",
                    r.field, r.region, r.file, r.start, r.end, r.info, i + 1 < regions.size() ? "," : ""));
            }
            System.out.print(out.append("]}\n"));
        }
    }

    static void numeric(List<Region> regions, String field, String prefix, String file, Object e) throws Exception {
        final long numValues = (long) get(e, "numValues");
        final int blockShift = (int) get(e, "blockShift");
        final byte bpv = (byte) get(e, "bitsPerValue");
        final long jump = (long) get(e, "valueJumpTableOffset");
        final String info = "numValues=" + numValues + " bpv=" + bpv + " blockShift=" + blockShift
            + (get(e, "table") != null ? " table" : "") + " gcd=" + get(e, "gcd");
        add(regions, field, prefix + "values", file, (long) get(e, "valuesOffset"), (long) get(e, "valuesLength"), info);
        if (jump >= 0 && blockShift >= 0) {
            final long blocks = (numValues + (1L << blockShift) - 1) >>> blockShift;
            add(regions, field, prefix + "value-jump-table", file, jump, (blocks + 1) * Long.BYTES, blocks + " blocks");
        }
        disi(regions, field, file, e);
    }

    static void sortedNumeric(List<Region> regions, String field, String file, Object e) throws Exception {
        numeric(regions, field, "", file, e);
        add(regions, field, "addresses", file, (long) get(e, "addressesOffset"), (long) get(e, "addressesLength"), "");
    }

    static void sorted(List<Region> regions, String field, String file, Object e) throws Exception {
        numeric(regions, field, "ords-", file, get(e, "ordsEntry"));
        termsDict(regions, field, file, get(e, "termsDictEntry"));
    }

    static void termsDict(List<Region> regions, String field, String file, Object t) throws Exception {
        final String info = "terms=" + get(t, "termsDictSize");
        add(regions, field, "terms-data", file, (long) get(t, "termsDataOffset"), (long) get(t, "termsDataLength"), info);
        add(regions, field, "terms-addresses", file, (long) get(t, "termsAddressesOffset"), (long) get(t, "termsAddressesLength"), "");
        add(regions, field, "terms-index", file, (long) get(t, "termsIndexOffset"), (long) get(t, "termsIndexLength"), "");
        add(regions, field, "terms-index-addresses", file, (long) get(t, "termsIndexAddressesOffset"),
            (long) get(t, "termsIndexAddressesLength"), "");
    }

    static void disi(List<Region> regions, String field, String file, Object e) throws Exception {
        final long offset = (long) get(e, "docsWithFieldOffset");
        if (offset >= 0) {
            add(regions, field, "docs-with-field", file, offset, (long) get(e, "docsWithFieldLength"),
                "jumpTableEntries=" + get(e, "jumpTableEntryCount"));
        }
    }

    static void add(List<Region> regions, String field, String region, String file, long start, long length, String info) {
        if (length > 0) {
            regions.add(new Region(field, region, file, start, start + length, info));
        }
    }

    static Object entry(Object producer, String map, int number) throws Exception {
        final Object m = get(producer, map);
        return m.getClass().getMethod("get", int.class).invoke(m, number);
    }

    static Object get(Object o, String name) throws Exception {
        for (Class<?> c = o.getClass(); c != null; c = c.getSuperclass()) {
            try {
                final Field f = c.getDeclaredField(name);
                f.setAccessible(true);
                return f.get(o);
            } catch (NoSuchFieldException e) {
                // try the superclass
            }
        }
        throw new NoSuchFieldException(o.getClass() + "." + name);
    }
}
