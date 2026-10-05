/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene104.Lucene104NavPostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPointsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.codec.CriteriaBasedCodec;
import org.opensearch.index.codec.composite.composite104.Composite104Codec;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * {@link UnusedFormatMetaWarningCodec} and the codecs {@link BufferPoolCodecService} builds around it: the same bytes as
 * the wrapped codec, the same postings format type for outer wrappers ({@link CriteriaBasedCodec}), composite
 * (star-tree) indices, and warnings bounded per index rather than per flush or per shard.
 */
public class UnusedFormatMetaWarningCodecTests extends OpenSearchTestCase {
    private static final Logger LOGGER = LogManager.getLogger(UnusedFormatMetaWarningCodecTests.class);

    private static final String MAPPING = "{\"_doc\":{\"properties\":{"
        + "\"ts\":{\"type\":\"date\"},"
        + "\"ts_split\":{\"type\":\"date\",\"meta\":{\"points_format\":\"Lucene90Split\"}},"
        + "\"kw\":{\"type\":\"keyword\"},"
        + "\"kw_nav\":{\"type\":\"keyword\",\"meta\":{\"postings_format\":\"Lucene104Nav\"}},"
        + "\"body\":{\"type\":\"keyword\",\"index\":false}"
        + "}}}";

    /** The same fields, with a star-tree field: the index is composite. */
    private static final String COMPOSITE_MAPPING = "{\"_doc\":{"
        + "\"composite\":{\"startree\":{\"type\":\"star_tree\",\"config\":{"
        + "\"ordered_dimensions\":[{\"name\":\"status\"},{\"name\":\"port\"}],"
        + "\"metrics\":[{\"name\":\"size\",\"stats\":[\"sum\",\"value_count\"]}]}}},"
        + "\"properties\":{"
        + "\"status\":{\"type\":\"integer\"},"
        + "\"port\":{\"type\":\"integer\"},"
        + "\"size\":{\"type\":\"integer\"},"
        + "\"ts\":{\"type\":\"date\"},"
        + "\"ts_split\":{\"type\":\"date\",\"meta\":{\"points_format\":\"Lucene90Split\"}},"
        + "\"kw\":{\"type\":\"keyword\"},"
        + "\"kw_nav\":{\"type\":\"keyword\",\"meta\":{\"postings_format\":\"Lucene104Nav\"}},"
        + "\"body\":{\"type\":\"keyword\",\"index\":false}"
        + "}}}";

    /** Collects the WARN events of the test logger. */
    private static final class Warnings implements MockLogAppender.LoggingExpectation {
        final List<String> messages = new CopyOnWriteArrayList<>();

        @Override
        public void match(LogEvent event) {
            if (event.getLevel() == Level.WARN) {
                messages.add(event.getMessage().getFormattedMessage());
            }
        }

        @Override
        public void assertMatched() {}
    }

    private MapperService mapperService(String mapping, boolean composite) throws Exception {
        final Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            // a new index per test: FormatMetaWarnings deduplicates the warnings per index UUID, JVM-wide
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()));
        if (composite) {
            settings.put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
                .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true);
        }
        final IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).putMapping(mapping).build();
        final MapperService mapperService = MapperTestUtils.newMapperService(
            new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
            createTempDir(),
            settings.build(),
            "test"
        );
        mapperService.merge(indexMetadata, MapperService.MergeReason.MAPPING_UPDATE);
        assertEquals(composite, mapperService.isCompositeIndexPresent());
        return mapperService;
    }

    private static BufferPoolCodecService service(MapperService mapperService) {
        return new BufferPoolCodecService(new CodecServiceConfig(mapperService.getIndexSettings(), mapperService, LOGGER, List.of()));
    }

    private static CodecService stock(MapperService mapperService) {
        return new CodecService(mapperService, mapperService.getIndexSettings(), LOGGER, List.of());
    }

    /** The same documents in three flushes, optionally force-merged to one segment; deterministic. */
    private static void write(Directory dir, Codec codec, boolean merge) throws Exception {
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec)
            .setUseCompoundFile(false)
            .setMergeScheduler(new SerialMergeScheduler())
            .setMergePolicy(new LogDocMergePolicy());
        iwc.getMergePolicy().setNoCFSRatio(0.0);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int s = 0; s < 3; s++) {
                for (int i = 0; i < 2000; i++) {
                    final long v = (i * 7919L + s * 104729L) % 1_000_003L;
                    final Document doc = new Document();
                    doc.add(new StringField(CriteriaBasedCodec.ATTRIBUTE_BINDING_TARGET_FIELD, s + ":" + i, Field.Store.NO));
                    doc.add(new LongPoint("ts", v));
                    doc.add(new LongPoint("ts_split", v));
                    doc.add(new StringField("kw", "v" + (i % 37), Field.Store.NO));
                    doc.add(new StringField("kw_nav", "v" + (i % 37), Field.Store.NO));
                    doc.add(new StoredField("body", "doc " + s + ":" + i));
                    doc.add(new SortedNumericDocValuesField("status", i % 5));
                    doc.add(new SortedNumericDocValuesField("port", i % 3));
                    doc.add(new SortedNumericDocValuesField("size", i));
                    writer.addDocument(doc);
                }
                writer.flush();
            }
            if (merge) {
                writer.forceMerge(1);
            }
            writer.commit();
        }
    }

    private static byte[] read(Directory dir, String file) throws Exception {
        try (IndexInput in = dir.openInput(file, IOContext.READONCE)) {
            final byte[] b = new byte[(int) in.length()];
            in.readBytes(b, 0, b.length);
            return b;
        }
    }

    /** Zeroes every occurrence of the segment id (random per write) and the footer checksum, which covers it. */
    private static void mask(byte[] b, byte[] id) {
        for (int i = 0; i + id.length <= b.length; i++) {
            boolean eq = true;
            for (int j = 0; j < id.length && eq; j++) {
                eq = b[i + j] == id[j];
            }
            if (eq) {
                Arrays.fill(b, i, i + id.length, (byte) 0);
            }
        }
        if (b.length >= 8) {
            Arrays.fill(b, b.length - 8, b.length, (byte) 0);
        }
    }

    /** Every file of every segment but {@code .si}, byte by byte, with the segment ids and checksums masked. */
    private static void assertSameBytes(String label, Directory a, Directory b) throws Exception {
        final SegmentInfos ia = SegmentInfos.readLatestCommit(a);
        final SegmentInfos ib = SegmentInfos.readLatestCommit(b);
        assertEquals(label, ia.size(), ib.size());
        int compared = 0;
        for (int s = 0; s < ia.size(); s++) {
            final SegmentCommitInfo ca = ia.info(s);
            final SegmentCommitInfo cb = ib.info(s);
            assertEquals(label, ca.info.getCodec().getName(), cb.info.getCodec().getName());
            assertEquals(label, ca.info.getAttributes(), cb.info.getAttributes());
            assertEquals(label, new TreeSet<>(ca.files()), new TreeSet<>(cb.files()));
            for (String f : new TreeSet<>(ca.files())) {
                if (f.endsWith(".si")) {
                    continue;
                }
                final byte[] x = read(a, f);
                final byte[] y = read(b, f);
                assertEquals(label + " " + f + " length", x.length, y.length);
                mask(x, ca.info.getId());
                mask(y, cb.info.getId());
                assertArrayEquals(label + " " + f, x, y);
                compared++;
            }
        }
        assertTrue(label + " compared " + compared, compared > 5);
    }

    private static String attribute(LeafReader leaf, String field, String key) {
        final FieldInfo fi = leaf.getFieldInfos().fieldInfo(field);
        return fi == null ? null : fi.getAttribute(key);
    }

    /** The warning codec over a per-field codec and over Lucene104 writes the same bytes, flushed and merged. */
    public void testWarningCodecWritesTheSameBytes() throws Exception {
        final MapperService ms = mapperService(MAPPING, false);
        final CodecService stock = stock(ms);
        for (String name : new String[] {
            CodecService.DEFAULT_CODEC,
            CodecService.BEST_COMPRESSION_CODEC,
            CodecService.LUCENE_DEFAULT_CODEC }) {
            final boolean merge = randomBoolean();
            final Codec base = stock.codec(name);
            final Codec wrapped = new UnusedFormatMetaWarningCodec(name, base, true, true, randomBoolean(), ms, LOGGER);
            assertEquals(
                name,
                base.postingsFormat() instanceof PerFieldPostingsFormat,
                wrapped.postingsFormat() instanceof PerFieldPostingsFormat
            );
            try (Directory da = FSDirectory.open(createTempDir()); Directory db = FSDirectory.open(createTempDir())) {
                write(da, base, merge);
                write(db, wrapped, merge);
                assertSameBytes(name + " merge=" + merge, da, db);
            }
        }
    }

    /** SimpleText through the service (the warning codec, not per field): same file names and lengths. */
    public void testSimpleTextThroughServiceSameFiles() throws Exception {
        final MapperService ms = mapperService(MAPPING, false);
        final CodecService stock = stock(ms);
        final BufferPoolCodecService service = service(ms);
        assumeTrue("SimpleText", Arrays.asList(stock.availableCodecs()).contains("SimpleText"));
        try (Directory da = FSDirectory.open(createTempDir()); Directory db = FSDirectory.open(createTempDir())) {
            write(da, stock.codec("SimpleText"), true);
            write(db, service.codec("SimpleText"), true);
            final SegmentCommitInfo a = SegmentInfos.readLatestCommit(da).info(0);
            final SegmentCommitInfo b = SegmentInfos.readLatestCommit(db).info(0);
            assertEquals(a.info.getCodec().getName(), b.info.getCodec().getName());
            assertEquals(new TreeSet<>(a.files()), new TreeSet<>(b.files()));
            for (String f : a.files()) {
                if (f.endsWith(".si") == false) {
                    assertEquals(f, da.fileLength(f), db.fileLength(f));
                }
            }
        }
    }

    /** Many fields with entries, many flushes and merges: one WARN per field and key, not per flush. */
    public void testWarningsDoNotGrowWithFlushes() throws Exception {
        final StringBuilder m = new StringBuilder("{\"_doc\":{\"properties\":{");
        final int fields = 40;
        for (int f = 0; f < fields; f++) {
            m.append(f == 0 ? "" : ",")
                .append("\"p")
                .append(f)
                .append("\":{\"type\":\"long\",\"meta\":{\"points_format\":\"Lucene90Split\",\"postings_format\":\"Lucene104Nav\"}}");
        }
        m.append("}}}");
        final MapperService ms = mapperService(m.toString(), false);
        final BufferPoolCodecService service = service(ms);
        assumeTrue("SimpleText", Arrays.asList(service.availableCodecs()).contains("SimpleText"));
        final Warnings warnings = new Warnings();
        try (Directory dir = FSDirectory.open(createTempDir()); MockLogAppender appender = MockLogAppender.createForLoggers(LOGGER)) {
            appender.addExpectation(warnings);
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(service.codec("SimpleText")))) {
                for (int s = 0; s < 30; s++) {
                    final Document doc = new Document();
                    for (int f = 0; f < fields; f++) {
                        doc.add(new LongPoint("p" + f, s));
                        doc.add(new StringField("p" + f, "x" + s, Field.Store.NO));
                    }
                    writer.addDocument(doc);
                    writer.flush();
                    if (s % 10 == 9) {
                        writer.forceMerge(1);
                    }
                }
                writer.commit();
            }
        }
        // points and postings per field (SimpleText records neither per field)
        assertEquals(2 * fields, warnings.messages.size());
    }

    /**
     * Every shard of an index builds its own codec service; the warnings are logged once per index. Another index (another
     * UUID) logs them again.
     */
    public void testWarningsOncePerIndexAcrossShards() throws Exception {
        final MapperService ms = mapperService(MAPPING, false);
        final MapperService other = mapperService(MAPPING, false);
        assumeTrue("SimpleText", Arrays.asList(service(ms).availableCodecs()).contains("SimpleText"));
        final Warnings warnings = new Warnings();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LOGGER)) {
            appender.addExpectation(warnings);
            final int shards = randomIntBetween(2, 5);
            for (int shard = 0; shard < shards; shard++) {
                try (Directory dir = FSDirectory.open(createTempDir())) {
                    // a new codec service per shard and engine open, as the engine builds it
                    write(dir, service(ms).codec("SimpleText"), randomBoolean());
                }
            }
            assertEquals(warnings.messages.toString(), 2, warnings.messages.size());
            try (Directory dir = FSDirectory.open(createTempDir())) {
                write(dir, service(other).codec("SimpleText"), false);
            }
            assertEquals(warnings.messages.toString(), 4, warnings.messages.size());
        }
    }

    /** The deduplication keeps at most {@link FormatMetaWarnings#MAX_KEYS} keys. */
    public void testWarningKeysAreBounded() throws Exception {
        final MapperService ms = mapperService(MAPPING, false);
        assertTrue(FormatMetaWarnings.first(ms, "bound", "f0"));
        assertFalse(FormatMetaWarnings.first(ms, "bound", "f0"));
        for (int i = 1; i <= FormatMetaWarnings.MAX_KEYS + 100; i++) {
            FormatMetaWarnings.first(ms, "bound", "f" + i);
        }
        assertEquals(FormatMetaWarnings.MAX_KEYS, FormatMetaWarnings.size());
        // the oldest key was dropped, so it warns once more
        assertTrue(FormatMetaWarnings.first(ms, "bound", "f0"));
    }

    /**
     * A context-aware index wraps the engine codec in {@link CriteriaBasedCodec}, which writes the segment's bucket
     * attribute only through a {@link PerFieldPostingsFormat}. Every codec of the service keeps that type.
     */
    public void testCriteriaBasedCodecOverServiceCodecs() throws Exception {
        final MapperService ms = mapperService(MAPPING, false);
        final CodecService stock = stock(ms);
        final BufferPoolCodecService service = service(ms);
        final Codec warning = new UnusedFormatMetaWarningCodec(
            CodecService.DEFAULT_CODEC,
            stock.codec(CodecService.DEFAULT_CODEC),
            true,
            true,
            true,
            ms,
            LOGGER
        );
        for (Codec c : new Codec[] {
            stock.codec(CodecService.DEFAULT_CODEC),
            service.codec(CodecService.DEFAULT_CODEC),
            service.codec(CodecService.BEST_COMPRESSION_CODEC),
            warning }) {
            final Codec criteria = new CriteriaBasedCodec(c, "bucket1");
            assertTrue(c.getClass().getSimpleName(), criteria.postingsFormat() instanceof PerFieldPostingsFormat);
            try (Directory dir = FSDirectory.open(createTempDir())) {
                write(dir, criteria, randomBoolean());
                for (SegmentCommitInfo info : SegmentInfos.readLatestCommit(dir)) {
                    assertEquals(c.getClass().getSimpleName(), "bucket1", info.info.getAttribute(CriteriaBasedCodec.BUCKET_NAME));
                }
            }
        }
    }

    /**
     * An index with a star-tree field: the service's codec, inside {@link CriteriaBasedCodec}, writes the bucket attribute
     * and exactly the files and per-field formats of the stock composite codec (the mapping entries are not used), with
     * one WARN per field and key that names the composite field as the reason.
     */
    public void testCompositeIndexInsideCriteriaBasedCodec() throws Exception {
        final MapperService ms = mapperService(COMPOSITE_MAPPING, true);
        final CodecService stock = stock(ms);
        final BufferPoolCodecService service = service(ms);
        final String name = randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC);
        final Codec base = stock.codec(name);
        final Codec codec = service.codec(name);
        assertTrue(base.getClass().getName(), base instanceof Composite104Codec);
        assertTrue(codec.getClass().getName(), codec instanceof UnusedFormatMetaWarningCodec);
        assertTrue(codec.postingsFormat() instanceof PerFieldPostingsFormat);
        final boolean merge = randomBoolean();
        final Warnings warnings = new Warnings();
        try (
            Directory da = FSDirectory.open(createTempDir());
            Directory db = FSDirectory.open(createTempDir());
            MockLogAppender appender = MockLogAppender.createForLoggers(LOGGER)
        ) {
            appender.addExpectation(warnings);
            write(da, new CriteriaBasedCodec(base, "bucket1"), merge);
            write(db, new CriteriaBasedCodec(codec, "bucket1"), merge);
            assertSameBytes(name + " merge=" + merge, da, db);
            for (SegmentCommitInfo info : SegmentInfos.readLatestCommit(db)) {
                assertEquals("bucket1", info.info.getAttribute(CriteriaBasedCodec.BUCKET_NAME));
                assertEquals(Composite104Codec.COMPOSITE_INDEX_CODEC_NAME, info.info.getCodec().getName());
                for (String f : info.files()) {
                    assertFalse(f, f.contains("_" + PerFieldPointsFormat.SPLIT_FORMAT_NAME + "_"));
                    assertFalse(f, f.contains("_" + Lucene104NavPostingsFormat.NAME + "_"));
                }
            }
            try (DirectoryReader a = DirectoryReader.open(da); DirectoryReader b = DirectoryReader.open(db)) {
                for (int i = 0; i < b.leaves().size(); i++) {
                    final LeafReader la = a.leaves().get(i).reader();
                    final LeafReader lb = b.leaves().get(i).reader();
                    for (String field : new String[] {
                        "ts",
                        "ts_split",
                        "kw",
                        "kw_nav",
                        CriteriaBasedCodec.ATTRIBUTE_BINDING_TARGET_FIELD }) {
                        for (String key : new String[] {
                            PerFieldPointsFormat.PER_FIELD_FORMAT_KEY,
                            PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY,
                            PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY }) {
                            assertEquals(field + " " + key, attribute(la, field, key), attribute(lb, field, key));
                        }
                    }
                    assertNull(attribute(lb, "ts_split", PerFieldPointsFormat.PER_FIELD_FORMAT_KEY));
                    assertEquals(
                        attribute(lb, "kw", PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY),
                        attribute(lb, "kw_nav", PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY)
                    );
                }
                final IndexSearcher searcher = new IndexSearcher(b);
                assertEquals(searcher.count(new TermQuery(new Term("kw", "v3"))), searcher.count(new TermQuery(new Term("kw_nav", "v3"))));
                assertEquals(
                    searcher.count(LongPoint.newRangeQuery("ts", 10, 500_000)),
                    searcher.count(LongPoint.newRangeQuery("ts_split", 10, 500_000))
                );
                for (LeafReaderContext ctx : b.leaves()) {
                    assertNotNull(ctx.reader().storedFields().document(0).get("body"));
                }
            }
        }
        assertEquals(warnings.messages.toString(), 2, warnings.messages.size());
        for (String m : warnings.messages) {
            assertTrue(m, m.contains("is not used: the index has a composite (star-tree) field, so codec [" + name + "]"));
        }
        assertTrue(warnings.messages.toString(), warnings.messages.stream().anyMatch(m -> m.contains("field [ts_split]: [points_format]")));
        assertTrue(warnings.messages.toString(), warnings.messages.stream().anyMatch(m -> m.contains("field [kw_nav]: [postings_format]")));
    }
}
