/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import org.apache.lucene.codecs.lucene104.Lucene104NavPostingsFormat.NavIntBlockTermState;
import org.apache.lucene.codecs.lucene104.Lucene104PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Offline IO calibration for the postings layout POC. Opens one shard's index directory read-only through a directory
 * wrapper that records every block (default 128 KiB) any read touches, then for each benchmark query and field:
 *
 * <ul>
 *   <li>{@code layout}: where each term's postings sit in .doc (and .nav): start pointer, byte length, blocks spanned</li>
 *   <li>{@code lucene}: blocks touched by the real Lucene query, IndexSearcher.count() of the same bool filter the
 *       benchmark sends to OpenSearch, with the query cache off</li>
 *   <li>{@code advance}: blocks touched by the model's assumption, a two-iterator conjunction loop that advances the
 *       dense list once per lead candidate (the loop of Lucene's ConjunctionBulkScorer)</li>
 * </ul>
 *
 * A block counts once per query, like a cold cache: touched blocks = cold loads. Prints JSON to stdout.
 *
 * <p>Run (Java 21+), with the Lucene fork jar and the plugin jar (for the Lucene104Baseline format) on the classpath:
 * <pre>
 * java -cp lucene-core-10.5.1-SNAPSHOT.jar:store-bufferpool.jar PostingsIoTrace.java &lt;shard index dir&gt; [block size]
 * </pre>
 */
public class PostingsIoTrace {

    static final String[] TERMS = { "d10", "d50", "r2", "r3", "r4", "r5", "r6" };
    static final String[][] QUERIES = {
        { "r6", "d50" },
        { "r5", "d50" },
        { "r4", "d50" },
        { "r3", "d50" },
        { "r2", "d50" },
        { "r4", "d10" },
        { "d10", "d50" } };
    static final String[] FIELDS = { "tag", "tag_nav" };

    public static void main(String[] args) throws IOException {
        final Path indexDir = Path.of(args[0]);
        final int blockSize = args.length > 1 ? Integer.parseInt(args[1]) : 128 * 1024;
        final Tracker tracker = new Tracker(Integer.numberOfTrailingZeros(blockSize));
        final StringBuilder out = new StringBuilder();
        try (
            Directory dir = new TrackingDirectory(new NIOFSDirectory(indexDir), tracker);
            DirectoryReader reader = DirectoryReader.open(dir)
        ) {
            if (reader.leaves().size() != 1) {
                throw new IllegalStateException("expected one segment, found " + reader.leaves().size());
            }
            final LeafReader leaf = reader.leaves().get(0).reader();
            final IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setQueryCache(null);
            out.append("{\"block_size\":").append(blockSize).append(",\"max_doc\":").append(leaf.maxDoc());
            out.append(",\"layout\":{");
            for (int f = 0; f < FIELDS.length; f++) {
                out.append(f == 0 ? "" : ",").append('"').append(FIELDS[f]).append("\":").append(layout(leaf, FIELDS[f], blockSize));
            }
            out.append("},\"queries\":[");
            for (int q = 0; q < QUERIES.length; q++) {
                for (int f = 0; f < FIELDS.length; f++) {
                    final String field = FIELDS[f];
                    final String lead = QUERIES[q][0], dense = QUERIES[q][1];

                    tracker.reset();
                    final BooleanQuery query = new BooleanQuery.Builder().add(
                        new TermQuery(new Term(field, lead)),
                        BooleanClause.Occur.FILTER
                    ).add(new TermQuery(new Term(field, dense)), BooleanClause.Occur.FILTER).build();
                    final int hits = searcher.count(query);
                    final String lucene = tracker.toJson();

                    tracker.reset();
                    final long[] sim = advanceLoop(leaf, field, lead, dense);
                    final String advance = tracker.toJson();

                    out.append(q + f == 0 ? "" : ",");
                    out.append("{\"query\":\"").append(lead).append(" AND ").append(dense).append("\",\"field\":\"").append(field);
                    out.append("\",\"hits\":").append(hits).append(",\"lucene\":").append(lucene);
                    out.append(",\"advance\":{\"hits\":").append(sim[0]).append(",\"dense_advances\":").append(sim[1]);
                    out.append(",\"blocks\":").append(advance).append("}}");
                }
            }
            out.append("]}");
        }
        System.out.println(out);
    }

    /** Per term: docFreq, .doc start pointer and byte length, and for the nav field the .nav start pointer and length. */
    static String layout(LeafReader leaf, String field, int blockSize) throws IOException {
        final TermsEnum te = leaf.terms(field).iterator();
        final List<String> names = new ArrayList<>();
        final List<long[]> rows = new ArrayList<>(); // docFreq, docStartFP, navStartFP (-1 if none)
        for (BytesRef t = te.next(); t != null; t = te.next()) {
            final TermState st = te.termState();
            final long docStart, navStart;
            if (st instanceof NavIntBlockTermState nav) {
                docStart = nav.docStartFP;
                navStart = nav.docFreq >= 256 ? nav.navStartFP : -1;
            } else {
                docStart = ((IntBlockTermState) st).docStartFP;
                navStart = -1;
            }
            names.add(t.utf8ToString());
            rows.add(new long[] { te.docFreq(), docStart, navStart });
        }
        // terms are written in order, so a term's bytes end where the next non-singleton term's start
        final StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < rows.size(); i++) {
            final long[] r = rows.get(i);
            long docEnd = -1, navEnd = -1;
            for (int j = i + 1; j < rows.size(); j++) {
                if (docEnd < 0 && rows.get(j)[1] > r[1]) {
                    docEnd = rows.get(j)[1];
                }
                if (navEnd < 0 && r[2] >= 0 && rows.get(j)[2] > r[2]) {
                    navEnd = rows.get(j)[2];
                }
            }
            sb.append(i == 0 ? "" : ",").append('"').append(names.get(i)).append("\":{\"doc_freq\":").append(r[0]);
            sb.append(",\"doc_start\":").append(r[1]).append(",\"doc_end\":").append(docEnd);
            if (r[1] >= 0 && docEnd > 0) {
                sb.append(",\"doc_blocks\":").append((docEnd - 1) / blockSize - r[1] / blockSize + 1);
            }
            sb.append(",\"nav_start\":").append(r[2]).append(",\"nav_end\":").append(navEnd);
            if (r[2] >= 0 && navEnd > 0) {
                sb.append(",\"nav_blocks\":").append((navEnd - 1) / blockSize - r[2] / blockSize + 1);
            }
            sb.append('}');
        }
        return sb.append('}').toString();
    }

    /** The two-iterator loop of ConjunctionBulkScorer: returns {hits, number of advance() calls on the dense list}. */
    static long[] advanceLoop(LeafReader leaf, String field, String leadTerm, String denseTerm) throws IOException {
        final PostingsEnum lead = postings(leaf, field, leadTerm);
        final PostingsEnum dense = postings(leaf, field, denseTerm);
        long hits = 0, advances = 0;
        int doc = lead.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            final int next = dense.advance(doc);
            advances++;
            if (next == doc) {
                hits++;
                doc = lead.nextDoc();
            } else if (next == DocIdSetIterator.NO_MORE_DOCS) {
                break;
            } else {
                doc = lead.advance(next);
            }
        }
        return new long[] { hits, advances };
    }

    static PostingsEnum postings(LeafReader leaf, String field, String term) throws IOException {
        final TermsEnum te = leaf.terms(field).iterator();
        if (te.seekExact(new BytesRef(term)) == false) {
            throw new IllegalStateException("missing term " + field + ":" + term);
        }
        return te.postings(null, PostingsEnum.NONE);
    }

    /** Distinct blocks touched per file since the last reset. */
    static final class Tracker {
        final int shift;
        final Map<String, TreeSet<Long>> touched = new TreeMap<>();

        Tracker(int shift) {
            this.shift = shift;
        }

        synchronized void touch(String file, long pos, long len) {
            final long first = pos >>> shift, last = (pos + Math.max(len, 1) - 1) >>> shift;
            final TreeSet<Long> set = touched.computeIfAbsent(file, k -> new TreeSet<>());
            for (long b = first; b <= last; b++) {
                set.add(b);
            }
        }

        synchronized void reset() {
            touched.clear();
        }

        synchronized String toJson() {
            final StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, TreeSet<Long>> e : touched.entrySet()) {
                sb.append(first ? "" : ",").append('"').append(e.getKey()).append("\":").append(e.getValue().toString().replace(" ", ""));
                first = false;
            }
            return sb.append('}').toString();
        }
    }

    static final class TrackingDirectory extends FilterDirectory {
        final Tracker tracker;

        TrackingDirectory(Directory in, Tracker tracker) {
            super(in);
            this.tracker = tracker;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return new TrackingInput(in.openInput(name, context), name, 0, tracker);
        }
    }

    /** Records the absolute position and length of every read, and of every prefetch, before delegating. */
    static final class TrackingInput extends IndexInput {
        final IndexInput in;
        final String file;
        final long base; // offset of this input (a slice or the whole file) in the file
        final Tracker tracker;

        TrackingInput(IndexInput in, String file, long base, Tracker tracker) {
            super("tracking(" + in + ")");
            this.in = in;
            this.file = file;
            this.base = base;
            this.tracker = tracker;
        }

        private void touch(long len) {
            tracker.touch(file, base + in.getFilePointer(), len);
        }

        @Override
        public byte readByte() throws IOException {
            touch(1);
            return in.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            touch(len);
            in.readBytes(b, offset, len);
        }

        @Override
        public short readShort() throws IOException {
            touch(Short.BYTES);
            return in.readShort();
        }

        @Override
        public int readInt() throws IOException {
            touch(Integer.BYTES);
            return in.readInt();
        }

        @Override
        public long readLong() throws IOException {
            touch(Long.BYTES);
            return in.readLong();
        }

        @Override
        public void readInts(int[] dst, int offset, int len) throws IOException {
            touch((long) len * Integer.BYTES);
            in.readInts(dst, offset, len);
        }

        @Override
        public void readLongs(long[] dst, int offset, int len) throws IOException {
            touch((long) len * Long.BYTES);
            in.readLongs(dst, offset, len);
        }

        @Override
        public void readFloats(float[] dst, int offset, int len) throws IOException {
            touch((long) len * Float.BYTES);
            in.readFloats(dst, offset, len);
        }

        @Override
        public void skipBytes(long numBytes) throws IOException {
            in.seek(in.getFilePointer() + numBytes);
        }

        @Override
        public void prefetch(long offset, long length) throws IOException {
            tracker.touch(file, base + offset, length);
            in.prefetch(offset, length);
        }

        @Override
        public long getFilePointer() {
            return in.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException {
            in.seek(pos);
        }

        @Override
        public long length() {
            return in.length();
        }

        @Override
        public TrackingInput clone() {
            return new TrackingInput(in.clone(), file, base, tracker);
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new TrackingInput(in.slice(sliceDescription, offset, length), file, base + offset, tracker);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
