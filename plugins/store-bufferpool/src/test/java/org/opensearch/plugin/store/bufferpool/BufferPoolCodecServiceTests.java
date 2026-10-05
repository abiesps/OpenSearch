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
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
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
import org.apache.lucene.index.NoMergePolicy;
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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Every codec name of a {@code bufferpoolfs} index, with a points-format mapping entry, a postings-format mapping entry
 * and neither: the segment's codec name, the per-field format names and the stored-fields mode match what the requested
 * codec writes, except the two fields whose mapping asks for another format.
 */
public class BufferPoolCodecServiceTests extends OpenSearchTestCase {
    private static final Logger LOGGER = LogManager.getLogger(BufferPoolCodecServiceTests.class);
    private static final String MAPPING = "{\"_doc\":{\"properties\":{"
        + "\"ts\":{\"type\":\"date\"},"
        + "\"ts_split\":{\"type\":\"date\",\"meta\":{\"points_format\":\"Lucene90Split\"}},"
        + "\"kw\":{\"type\":\"keyword\"},"
        + "\"kw_nav\":{\"type\":\"keyword\",\"meta\":{\"postings_format\":\"Lucene104Nav\"}},"
        + "\"body\":{\"type\":\"keyword\",\"index\":false}"
        + "}}}";
    private static final int DOCS = 500;

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

    private MapperService mapperService() throws IOException {
        final IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putMapping(MAPPING)
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

    /** One flushed, non-compound segment; ts and ts_split, kw and kw_nav hold the same values. */
    private static void index(Directory dir, Codec codec) throws IOException {
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec)
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setUseCompoundFile(false);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < DOCS; i++) {
                final Document doc = new Document();
                doc.add(new LongPoint("ts", 1000L * i));
                doc.add(new LongPoint("ts_split", 1000L * i));
                doc.add(new StringField("kw", "v" + (i % 7), Field.Store.NO));
                doc.add(new StringField("kw_nav", "v" + (i % 7), Field.Store.NO));
                doc.add(new StoredField("body", "document " + i));
                writer.addDocument(doc);
            }
            writer.commit();
        }
    }

    private static SegmentCommitInfo onlySegment(Directory dir) throws IOException {
        final SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
        assertEquals(1, infos.size());
        return infos.info(0);
    }

    private static String attribute(LeafReader leaf, String field, String key) {
        final FieldInfo fi = leaf.getFieldInfos().fieldInfo(field);
        return fi == null ? null : fi.getAttribute(key);
    }

    public void testEveryCodecNameKeepsItsFormatsExceptTheMappedOnes() throws Exception {
        final MapperService mapperService = mapperService();
        final CodecService stock = new CodecService(mapperService, mapperService.getIndexSettings(), LOGGER, List.of());
        final BufferPoolCodecService service = new BufferPoolCodecService(
            new CodecServiceConfig(mapperService.getIndexSettings(), mapperService, LOGGER, List.of())
        );
        final List<String> names = new ArrayList<>(Arrays.asList(stock.availableCodecs()));
        assertEquals(names.size(), service.availableCodecs().length);
        final List<String> written = new ArrayList<>();
        final List<String> readOnly = new ArrayList<>();
        final Warnings warnings = new Warnings();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LOGGER)) {
            appender.addExpectation(warnings);
            for (String name : names) {
                final Codec base = stock.codec(name);
                final Codec codec = service.codec(name);
                assertSame("one wrapped codec per name", codec, service.codec(name));
                try (Directory baseDir = FSDirectory.open(createTempDir()); Directory dir = FSDirectory.open(createTempDir())) {
                    try {
                        index(baseDir, base);
                    } catch (UnsupportedOperationException e) {
                        // read-only (backward) codecs: the wrapped codec cannot write either
                        expectThrows(UnsupportedOperationException.class, () -> index(dir, codec));
                        readOnly.add(name);
                        continue;
                    }
                    index(dir, codec);
                    written.add(name);
                    assertSegment(name, base, baseDir, dir);
                }
            }
        }
        // the OpenSearch names are all writable and covered
        assertTrue(
            written.toString(),
            written.containsAll(
                List.of(
                    CodecService.DEFAULT_CODEC,
                    CodecService.LZ4,
                    CodecService.BEST_COMPRESSION_CODEC,
                    CodecService.ZLIB,
                    CodecService.LUCENE_DEFAULT_CODEC
                )
            )
        );
        // one warning per codec name that cannot split points, none for the others
        assertEquals(names.size(), written.size() + readOnly.size());
        LOGGER.info("codec names written and checked: {}; read-only (cannot write): {}", written, readOnly);
        // one WARN per codec name and mapping key it cannot honour, at the first flush (a read-only codec may warn before
        // it refuses to write)
        for (String name : names) {
            final Codec base = stock.codec(name);
            final String codecTag = "codec [" + name + "] ";
            final long points = warnings.messages.stream().filter(m -> m.contains("[points_format]") && m.contains(codecTag)).count();
            final long postings = warnings.messages.stream().filter(m -> m.contains("[postings_format]") && m.contains(codecTag)).count();
            if (written.contains(name)) {
                assertEquals(name + " " + warnings.messages, splits(base) ? 0 : 1, points);
                assertEquals(name + " " + warnings.messages, PostingsFormatSelectingCodec.supports(base) ? 0 : 1, postings);
            } else {
                assertTrue(name + " " + warnings.messages, points <= (splits(base) ? 0 : 1) && postings <= 1);
            }
        }
        assertTrue(warnings.messages.stream().allMatch(m -> m.contains("field [ts_split]") || m.contains("field [kw_nav]")));
    }

    /** Whether the service writes the split points format for meta fields with {@code codec} as the index codec. */
    private static boolean splits(Codec codec) {
        return codec instanceof Lucene104Codec || codec instanceof Lucene104SplitPointsCodec;
    }

    public void testStoredFieldsModeOfTheOpenSearchNames() throws Exception {
        final MapperService mapperService = mapperService();
        final BufferPoolCodecService service = new BufferPoolCodecService(
            new CodecServiceConfig(mapperService.getIndexSettings(), mapperService, LOGGER, List.of())
        );
        final Map<String, String> modes = Map.of(
            CodecService.DEFAULT_CODEC,
            "BEST_SPEED",
            CodecService.LZ4,
            "BEST_SPEED",
            CodecService.BEST_COMPRESSION_CODEC,
            "BEST_COMPRESSION",
            CodecService.ZLIB,
            "BEST_COMPRESSION"
        );
        for (Map.Entry<String, String> e : modes.entrySet()) {
            try (Directory dir = FSDirectory.open(createTempDir())) {
                index(dir, service.codec(e.getKey()));
                final SegmentCommitInfo info = onlySegment(dir);
                assertEquals(e.getKey(), Lucene104SplitPointsCodec.NAME, info.info.getCodec().getName());
                assertEquals(e.getKey(), e.getValue(), info.info.getAttribute(Lucene90StoredFieldsFormat.MODE_KEY));
                final List<String> files = new ArrayList<>(info.files());
                for (String ext : new String[] { "kdm", "kdi", "kdd", "kdv" }) {
                    assertTrue(e.getKey() + " " + files, files.contains(info.info.name + "_Lucene90Split_0." + ext));
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    final LeafReader leaf = reader.leaves().get(0).reader();
                    assertEquals(
                        PerFieldPointsFormat.SPLIT_FORMAT_NAME,
                        attribute(leaf, "ts_split", PerFieldPointsFormat.PER_FIELD_FORMAT_KEY)
                    );
                    assertEquals(Lucene104NavPostingsFormat.NAME, attribute(leaf, "kw_nav", PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY));
                    assertEquals("document 7", leaf.storedFields().document(7).get("body"));
                }
            }
        }
    }

    /** {@code dir} (wrapped codec) against {@code baseDir} (the requested codec itself), same documents. */
    private static void assertSegment(String name, Codec base, Directory baseDir, Directory dir) throws IOException {
        final SegmentCommitInfo baseInfo = onlySegment(baseDir);
        final SegmentCommitInfo info = onlySegment(dir);
        final boolean split = splits(base);
        final boolean postings = PostingsFormatSelectingCodec.supports(base);
        assertEquals(name, split ? Lucene104SplitPointsCodec.NAME : baseInfo.info.getCodec().getName(), info.info.getCodec().getName());
        // the stored-fields codec is the requested one (best_compression keeps BEST_COMPRESSION)
        assertEquals(
            name,
            baseInfo.info.getAttribute(Lucene90StoredFieldsFormat.MODE_KEY),
            info.info.getAttribute(Lucene90StoredFieldsFormat.MODE_KEY)
        );
        final List<String> files = new ArrayList<>(info.files());
        for (String ext : new String[] { "kdm", "kdi", "kdd", "kdv" }) {
            assertEquals(name + " " + files, split, files.contains(info.info.name + "_Lucene90Split_0." + ext));
        }
        // read back through SPI (the codec name of the segment), not through the writing codec
        try (DirectoryReader baseReader = DirectoryReader.open(baseDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            final LeafReader b = baseReader.leaves().get(0).reader();
            final LeafReader l = reader.leaves().get(0).reader();
            // neither: the same formats as the requested codec writes
            for (String field : new String[] { "ts", "kw" }) {
                for (String key : new String[] { PerFieldPointsFormat.PER_FIELD_FORMAT_KEY, PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY }) {
                    assertEquals(name + " " + field + " " + key, attribute(b, field, key), attribute(l, field, key));
                }
            }
            // points meta
            assertEquals(
                name,
                split ? PerFieldPointsFormat.SPLIT_FORMAT_NAME : attribute(b, "ts_split", PerFieldPointsFormat.PER_FIELD_FORMAT_KEY),
                attribute(l, "ts_split", PerFieldPointsFormat.PER_FIELD_FORMAT_KEY)
            );
            assertEquals(name, b.getPointValues("ts").size(), l.getPointValues("ts_split").size());
            assertEquals(
                name,
                new IndexSearcher(baseReader).count(LongPoint.newRangeQuery("ts", 7000, 201000)),
                new IndexSearcher(reader).count(LongPoint.newRangeQuery("ts_split", 7000, 201000))
            );
            // postings meta
            assertEquals(
                name,
                postings ? Lucene104NavPostingsFormat.NAME : attribute(b, "kw_nav", PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY),
                attribute(l, "kw_nav", PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY)
            );
            assertEquals(
                name,
                new IndexSearcher(baseReader).count(new TermQuery(new Term("kw", "v3"))),
                new IndexSearcher(reader).count(new TermQuery(new Term("kw_nav", "v3")))
            );
            assertEquals(name, b.storedFields().document(11).get("body"), l.storedFields().document(11).get("body"));
        }
    }
}
