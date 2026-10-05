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
import org.apache.lucene.codecs.lucene104.Lucene104SplitPointsCodec;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPointsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The selecting codecs of {@link BufferPoolCodecService} across segment histories: stock segments next to wrapped
 * ones, a codec change on reopen and back to a node without the plugin, random documents, deletes, compound files and
 * merges; a postings format name that is not available; a mapping entry a codec cannot honour, added after the codec was
 * resolved; and {@code Lucene104SplitPoints} as the index codec.
 */
public class BufferPoolCodecServiceSegmentsTests extends OpenSearchTestCase {
    private static final Logger LOGGER = LogManager.getLogger(BufferPoolCodecServiceSegmentsTests.class);
    private static final String MAPPING = "{\"_doc\":{\"properties\":{"
        + "\"ts\":{\"type\":\"date\"},"
        + "\"ts_split\":{\"type\":\"date\",\"meta\":{\"points_format\":\"Lucene90Split\"}},"
        + "\"kw\":{\"type\":\"keyword\"},"
        + "\"kw_nav\":{\"type\":\"keyword\",\"meta\":{\"postings_format\":\"Lucene104Nav\"}},"
        + "\"body\":{\"type\":\"keyword\",\"index\":false}"
        + "}}}";

    /** Collects the WARN events of the codec service's logger. */
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

    private MapperService mapperService(String mapping) throws Exception {
        final IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putMapping(mapping)
            .build();
        final MapperService mapperService = MapperTestUtils.newMapperService(
            new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
            createTempDir(),
            Settings.EMPTY,
            "test"
        );
        mapperService.merge(indexMetadata, MapperService.MergeReason.MAPPING_UPDATE);
        return mapperService;
    }

    private static BufferPoolCodecService service(MapperService mapperService) {
        return new BufferPoolCodecService(new CodecServiceConfig(mapperService.getIndexSettings(), mapperService, LOGGER, List.of()));
    }

    /** Random documents in 1-3 flushed segments (random compound files, random deletes), optionally merged to one. */
    private void addSegments(Directory dir, Codec codec, int segments, boolean merge) throws Exception {
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setUseCompoundFile(randomBoolean());
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int s = 0; s < segments; s++) {
                final int n = randomIntBetween(1, 3000);
                for (int i = 0; i < n; i++) {
                    final long v = randomLongBetween(-1_000_000, 1_000_000);
                    final String t = "v" + randomIntBetween(0, 50);
                    final Document doc = new Document();
                    doc.add(new LongPoint("ts", v));
                    doc.add(new LongPoint("ts_split", v));
                    doc.add(new StringField("kw", t, Field.Store.NO));
                    doc.add(new StringField("kw_nav", t, Field.Store.NO));
                    doc.add(new StoredField("body", t + ":" + v));
                    writer.addDocument(doc);
                }
                if (randomBoolean()) {
                    writer.deleteDocuments(new Term("kw", "v" + randomIntBetween(0, 50)));
                }
                writer.flush();
            }
            if (merge) {
                writer.forceMerge(1);
            }
            writer.commit();
        }
    }

    /** The meta fields return exactly what their twin stock fields return. */
    private void checkCounts(Directory dir) throws Exception {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            final IndexSearcher searcher = new IndexSearcher(reader);
            for (int i = 0; i < 20; i++) {
                final long lo = randomLongBetween(-1_000_000, 1_000_000);
                final long hi = lo + randomLongBetween(0, 500_000);
                assertEquals(
                    searcher.count(LongPoint.newRangeQuery("ts", lo, hi)),
                    searcher.count(LongPoint.newRangeQuery("ts_split", lo, hi))
                );
                final String t = "v" + randomIntBetween(0, 50);
                assertEquals(searcher.count(new TermQuery(new Term("kw", t))), searcher.count(new TermQuery(new Term("kw_nav", t))));
            }
            for (LeafReaderContext ctx : reader.leaves()) {
                final LeafReader leaf = ctx.reader();
                if (leaf.maxDoc() > 0) {
                    assertNotNull(leaf.storedFields().document(0).get("body"));
                }
            }
        }
    }

    private static String attribute(LeafReader leaf, String field, String key) {
        final FieldInfo fi = leaf.getFieldInfos().fieldInfo(field);
        return fi == null ? null : fi.getAttribute(key);
    }

    public void testMixedStockAndWrappedSegmentsCodecChangeAndRevert() throws Exception {
        final MapperService mapperService = mapperService(MAPPING);
        final CodecService stock = new CodecService(mapperService, mapperService.getIndexSettings(), LOGGER, List.of());
        final BufferPoolCodecService service = service(mapperService);
        try (Directory dir = FSDirectory.open(createTempDir())) {
            // segments of an older build: stock best_compression
            addSegments(dir, stock.codec(CodecService.BEST_COMPRESSION_CODEC), randomIntBetween(1, 3), false);
            checkCounts(dir);
            // reopen with the wrapped best_compression, add, merge everything
            addSegments(dir, service.codec(CodecService.BEST_COMPRESSION_CODEC), randomIntBetween(1, 3), true);
            checkCounts(dir);
            final SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
            assertEquals(1, infos.size());
            SegmentCommitInfo info = infos.info(0);
            assertEquals(Lucene104SplitPointsCodec.NAME, info.info.getCodec().getName());
            assertEquals("BEST_COMPRESSION", info.info.getAttribute(Lucene90StoredFieldsFormat.MODE_KEY));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                assertEquals(
                    PerFieldPointsFormat.SPLIT_FORMAT_NAME,
                    attribute(leaf, "ts_split", PerFieldPointsFormat.PER_FIELD_FORMAT_KEY)
                );
                assertEquals(Lucene104NavPostingsFormat.NAME, attribute(leaf, "kw_nav", PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY));
                assertNull(attribute(leaf, "ts", PerFieldPointsFormat.PER_FIELD_FORMAT_KEY));
            }
            // codec change on reopen: default (BEST_SPEED), merge again
            addSegments(dir, service.codec(CodecService.DEFAULT_CODEC), randomIntBetween(1, 2), true);
            checkCounts(dir);
            info = SegmentInfos.readLatestCommit(dir).info(0);
            assertEquals("BEST_SPEED", info.info.getAttribute(Lucene90StoredFieldsFormat.MODE_KEY));
            // a node without the selecting codecs (stock codec service) merges the split and Nav segments back to stock
            addSegments(dir, stock.codec(CodecService.BEST_COMPRESSION_CODEC), 1, true);
            checkCounts(dir);
            info = SegmentInfos.readLatestCommit(dir).info(0);
            assertEquals("Lucene104", info.info.getCodec().getName());
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                assertEquals("Lucene104", attribute(leaf, "kw_nav", PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY));
            }
        }
    }

    public void testUnavailablePostingsFormatKeepsTheCodecsOwnWithOneWarning() throws Exception {
        final MapperService mapperService = mapperService(
            "{\"_doc\":{\"properties\":{\"kw\":{\"type\":\"keyword\"},"
                + "\"kw_nav\":{\"type\":\"keyword\",\"meta\":{\"postings_format\":\"Lucene104Navv\"}}}}}"
        );
        final BufferPoolCodecService service = service(mapperService);
        final Warnings warnings = new Warnings();
        try (Directory dir = FSDirectory.open(createTempDir()); MockLogAppender appender = MockLogAppender.createForLoggers(LOGGER)) {
            appender.addExpectation(warnings);
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(service.codec(CodecService.BEST_COMPRESSION_CODEC));
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int s = 0; s < 3; s++) {
                    final Document doc = new Document();
                    doc.add(new StringField("kw", "a", Field.Store.NO));
                    doc.add(new StringField("kw_nav", "a", Field.Store.NO));
                    writer.addDocument(doc);
                    writer.flush();
                }
                writer.forceMerge(1);
                writer.commit();
                assertTrue(writer.isOpen());
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                assertEquals(
                    attribute(leaf, "kw", PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY),
                    attribute(leaf, "kw_nav", PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY)
                );
                assertEquals(3, new IndexSearcher(reader).count(new TermQuery(new Term("kw_nav", "a"))));
            }
        }
        // three flushes and a merge ask for the field's format four times: one warning
        assertEquals(warnings.messages.toString(), 1, warnings.messages.size());
        assertTrue(warnings.messages.get(0), warnings.messages.get(0).contains("field [kw_nav]: postings format [Lucene104Navv]"));
    }

    public void testMetaAddedAfterTheCodecWasResolvedIsWarnedAtFlush() throws Exception {
        final MapperService mapperService = mapperService("{\"_doc\":{\"properties\":{\"ts\":{\"type\":\"date\"}}}}");
        final BufferPoolCodecService service = service(mapperService);
        assumeTrue("SimpleText codec on the test classpath", Arrays.asList(service.availableCodecs()).contains("SimpleText"));
        // a codec that records neither format per field
        final Codec codec = service.codec("SimpleText");
        mapperService.merge(
            "_doc",
            new CompressedXContent(
                "{\"_doc\":{\"properties\":{\"ts2\":{\"type\":\"date\",\"meta\":{\"points_format\":\"Lucene90Split\"}}}}}"
            ),
            MapperService.MergeReason.MAPPING_UPDATE
        );
        final Warnings warnings = new Warnings();
        try (Directory dir = FSDirectory.open(createTempDir()); MockLogAppender appender = MockLogAppender.createForLoggers(LOGGER)) {
            appender.addExpectation(warnings);
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec))) {
                for (int s = 0; s < 2; s++) {
                    final Document doc = new Document();
                    doc.add(new LongPoint("ts", 5));
                    doc.add(new LongPoint("ts2", 5));
                    writer.addDocument(doc);
                    writer.flush();
                }
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(2, new IndexSearcher(reader).count(LongPoint.newExactQuery("ts2", 5)));
            }
        }
        assertEquals(warnings.messages.toString(), 1, warnings.messages.size());
        assertTrue(
            warnings.messages.get(0),
            warnings.messages.get(0).contains("field [ts2]: [points_format] [Lucene90Split] is not used: codec [SimpleText]")
        );
    }

    public void testSplitPointsCodecAsIndexCodec() throws Exception {
        final MapperService mapperService = mapperService(MAPPING);
        final BufferPoolCodecService service = service(mapperService);
        try (Directory dir = FSDirectory.open(createTempDir())) {
            addSegments(dir, service.codec(Lucene104SplitPointsCodec.NAME), randomIntBetween(1, 3), randomBoolean());
            checkCounts(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    assertEquals(
                        PerFieldPointsFormat.SPLIT_FORMAT_NAME,
                        attribute(ctx.reader(), "ts_split", PerFieldPointsFormat.PER_FIELD_FORMAT_KEY)
                    );
                }
            }
        }
    }
}
