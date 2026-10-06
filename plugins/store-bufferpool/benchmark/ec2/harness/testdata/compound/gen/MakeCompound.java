/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
// Writes one small compound segment with stock Lucene (the default codec of the jar on the class path) for
// selftest_compound.py: text (postings, positions, norms), keyword with sorted doc values, a long point with numeric
// doc values, a stored field. Also writes expected.json: every entry of the compound file with its length, from
// Lucene's own CompoundDirectory (ground truth for the Python .cfe parser).
//   java -cp lucene-core-10.5.1.jar MakeCompound.java OUT_DIR
import java.nio.file.*;
import java.util.*;
import org.apache.lucene.codecs.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRef;

public class MakeCompound {
    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args[0]);
        Path tmp = Files.createTempDirectory("mkcfs");
        try (Directory dir = FSDirectory.open(tmp)) {
            IndexWriterConfig c = new IndexWriterConfig().setUseCompoundFile(true).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter w = new IndexWriter(dir, c)) {
                Random r = new Random(7);
                String[] words = {"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"};
                for (int i = 0; i < 2000; i++) {
                    Document d = new Document();
                    StringBuilder sb = new StringBuilder();
                    for (int k = 0; k < 12; k++) sb.append(words[r.nextInt(words.length)]).append(' ');
                    d.add(new TextField("message", sb.toString(), Field.Store.YES));
                    String kw = "host-" + r.nextInt(50);
                    d.add(new KeywordField("host", kw, Field.Store.NO));
                    long ts = 1_700_000_000_000L + i * 1000L;
                    d.add(new LongPoint("ts", ts));
                    d.add(new NumericDocValuesField("ts", ts));
                    w.addDocument(d);
                }
                w.commit();
            }
            SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
            SegmentCommitInfo sci = sis.info(0);
            SegmentInfo si = sci.info;
            if (!si.getUseCompoundFile()) throw new IllegalStateException("segment is not compound");
            StringBuilder js = new StringBuilder("{\"codec\": \"" + si.getCodec().getName() + "\", \"segment\": \"" + si.name + "\", \"entries\": {");
            try (CompoundDirectory cd = si.getCodec().compoundFormat().getCompoundReader(dir, si)) {
                String[] names = cd.listAll();
                Arrays.sort(names);
                for (int i = 0; i < names.length; i++) {
                    js.append(i == 0 ? "" : ", ").append('"').append(names[i]).append("\": ").append(cd.fileLength(names[i]));
                }
            }
            js.append("}}\n");
            Files.createDirectories(out);
            for (String f : dir.listAll()) {
                if (f.endsWith(".cfs") || f.endsWith(".cfe") || f.endsWith(".si")) {
                    Files.copy(tmp.resolve(f), out.resolve(f), StandardCopyOption.REPLACE_EXISTING);
                }
            }
            Files.writeString(out.resolve("expected.json"), js.toString());
            System.out.print(js);
        }
    }
}
