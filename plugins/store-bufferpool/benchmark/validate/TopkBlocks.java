import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.lucene.codecs.lucene104.Lucene104DualNavPostingsFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

/**
 * Offline and read-only: for top-k OR queries, which 128 KiB blocks of each file are read, (a) scoring the segment in
 * one call as plain Lucene does, and (b) in growing doc-ID chunks as OpenSearch's CancellableBulkScorer does (4,096
 * docs doubling up to 1M). Same top-k either way. Every run starts with an empty record, so counts are cold blocks.
 *
 * Usage: java -cp lucene-core.jar:store-bufferpool.jar TopkBlocks.java INDEX_DIR [field] [doc|nav]
 */
public class TopkBlocks {
  static final int SHIFT = 17; // 128 KiB
  static final Map<String, Set<Long>> touched = new TreeMap<>();
  static final boolean DEBUG = Boolean.getBoolean("debug");

  static synchronized void touch(String file, long pos, long len) {
    if (len <= 0) return;
    final String type = file.contains("_Lucene") ? file.substring(file.indexOf("_Lucene") + 1) : file.substring(file.lastIndexOf('.') + 1);
    Set<Long> s = touched.computeIfAbsent(type, k -> new HashSet<>());
    for (long b = pos >>> SHIFT; b <= (pos + len - 1) >>> SHIFT; b++) s.add(b);
  }

  public static void main(String[] args) throws Exception {
    final String field = args.length > 1 ? args[1] : "body";
    if (args.length > 2) {
      Lucene104DualNavPostingsFormat.setReadMode(Lucene104DualNavPostingsFormat.ReadMode.valueOf(args[2].toUpperCase()));
    }
    try (Directory dir = new Recording(FSDirectory.open(Path.of(args[0]))); DirectoryReader r = DirectoryReader.open(dir)) {
      IndexSearcher s = new IndexSearcher(r);
      s.setQueryCache(null);
      String[][] queries = {{"t50", "t20"}, {"t50", "t5"}, {"t50", "t1"}, {"t20", "t5", "t1"}, {"t5", "t1", "t01"}};
      for (String[] terms : queries) {
        BooleanQuery.Builder b = new BooleanQuery.Builder();
        for (String t : terms) b.add(new TermQuery(new Term(field, t)), BooleanClause.Occur.SHOULD);
        Query q = b.build();
        touched.clear();
        TopDocs plain = s.search(q, new TopScoreDocCollectorManager(10, null, 1));
        final String plainBlocks = counts();
        touched.clear();
        TopDocs chunked = chunked(s, q, 10);
        final String chunkedBlocks = counts();
        boolean same = plain.scoreDocs.length == chunked.scoreDocs.length;
        for (int i = 0; same && i < plain.scoreDocs.length; i++) {
          same = plain.scoreDocs[i].doc == chunked.scoreDocs[i].doc && plain.scoreDocs[i].score == chunked.scoreDocs[i].score;
        }
        System.out.printf("%-18s plain: %s | chunked: %s | same top-10: %s%n", String.join(" OR ", terms), plainBlocks, chunkedBlocks, same);
      }
    }
  }

  static String counts() {
    StringBuilder sb = new StringBuilder("{");
    touched.forEach((k, v) -> {
      if (k.endsWith(".doc") || k.endsWith(".nav") || k.equals("nvd")) sb.append(k).append('=').append(v.size()).append(' ');
      else if (DEBUG) sb.append('(').append(k).append('=').append(v.size()).append(") ");
    });
    return sb.append('}').toString();
  }

  /** OpenSearch's CancellableBulkScorer loop over Lucene's own BulkScorer, with a hit-count threshold of 1. */
  static TopDocs chunked(IndexSearcher s, Query q, int k) throws IOException {
    Weight w = s.createWeight(s.rewrite(q), ScoreMode.TOP_SCORES, 1f);
    TopScoreDocCollector c = new TopScoreDocCollectorManager(k, null, 1).newCollector();
    for (LeafReaderContext ctx : s.getIndexReader().leaves()) {
      BulkScorer bs = w.bulkScorer(ctx);
      if (bs == null) continue;
      LeafCollector lc = c.getLeafCollector(ctx);
      int min = 0, interval = 1 << 12;
      final int max = ctx.reader().maxDoc();
      while (min < max) {
        final int newMax = (int) Math.min((long) min + interval, max);
        min = bs.score(lc, ctx.reader().getLiveDocs(), min, newMax);
        interval = Math.min(interval << 1, 1 << 20);
      }
      lc.finish();
    }
    return c.topDocs();
  }

  static final class Recording extends FilterDirectory {
    Recording(Directory in) {
      super(in);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      return new In(in.openInput(name, context), name, 0);
    }
  }

  static final class In extends IndexInput {
    final IndexInput in;
    final String file;
    final long base;

    In(IndexInput in, String file, long base) {
      super("rec(" + in + ")");
      this.in = in;
      this.file = file;
      this.base = base;
    }

    private void rec(long len) {
      touch(file, base + in.getFilePointer(), len);
    }

    @Override public byte readByte() throws IOException { rec(1); return in.readByte(); }
    @Override public void readBytes(byte[] b, int o, int l) throws IOException { rec(l); in.readBytes(b, o, l); }
    @Override public short readShort() throws IOException { rec(2); return in.readShort(); }
    @Override public int readInt() throws IOException { rec(4); return in.readInt(); }
    @Override public long readLong() throws IOException { rec(8); return in.readLong(); }
    @Override public void readInts(int[] d, int o, int l) throws IOException { rec(4L * l); in.readInts(d, o, l); }
    @Override public void readLongs(long[] d, int o, int l) throws IOException { rec(8L * l); in.readLongs(d, o, l); }
    @Override public void readFloats(float[] d, int o, int l) throws IOException { rec(4L * l); in.readFloats(d, o, l); }
    @Override public void close() throws IOException { in.close(); }
    @Override public long getFilePointer() { return in.getFilePointer(); }
    @Override public void seek(long p) throws IOException { in.seek(p); }
    @Override public long length() { return in.length(); }
    @Override public In clone() { return new In(in.clone(), file, base); }

    @Override
    public IndexInput slice(String d, long off, long len) throws IOException {
      return new In(in.slice(d, off, len), file, base + off);
    }

    @Override
    public RandomAccessInput randomAccessSlice(long off, long len) throws IOException {
      final RandomAccessInput r = in.randomAccessSlice(off, len);
      final long start = base + off;
      return new RandomAccessInput() {
        @Override public long length() { return r.length(); }
        @Override public byte readByte(long p) throws IOException { touch(file, start + p, 1); return r.readByte(p); }
        @Override public short readShort(long p) throws IOException { touch(file, start + p, 2); return r.readShort(p); }
        @Override public int readInt(long p) throws IOException { touch(file, start + p, 4); return r.readInt(p); }
        @Override public long readLong(long p) throws IOException { touch(file, start + p, 8); return r.readLong(p); }
      };
    }
  }
}
