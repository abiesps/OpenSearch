import java.nio.file.Path;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;

/** Offline: for top-k OR queries on the given field, how many docs top-k collected vs how many match. Read-only. */
public class TopkPruning {
  public static void main(String[] args) throws Exception {
    try (Directory dir = FSDirectory.open(Path.of(args[0])); DirectoryReader r = DirectoryReader.open(dir)) {
      IndexSearcher s = new IndexSearcher(r);
      s.setQueryCache(null);
      String field = args.length > 1 ? args[1] : "body";
      String[][] queries = {{"t50", "t20"}, {"t50", "t5"}, {"t50", "t1"}, {"t20", "t5", "t1"}, {"t5", "t1", "t01"}};
      for (int k : new int[] {10, 100}) {
        for (String[] terms : queries) {
          BooleanQuery.Builder b = new BooleanQuery.Builder();
          for (String t : terms) b.add(new TermQuery(new Term(field, t)), BooleanClause.Occur.SHOULD);
          Query q = b.build();
          int matches = s.count(q);
          long t0 = System.nanoTime();
          TopDocs td = s.search(q, new TopScoreDocCollectorManager(k, k));
          long ms = (System.nanoTime() - t0) / 1_000_000;
          System.out.printf("k=%d %-18s matches=%,d collected=%,d (%s) top=%.4f kth=%.4f warm=%dms%n",
              k, String.join(" OR ", terms), matches, td.totalHits.value(), td.totalHits.relation(),
              td.scoreDocs[0].score, td.scoreDocs[td.scoreDocs.length - 1].score, ms);
        }
      }
    }
  }
}
