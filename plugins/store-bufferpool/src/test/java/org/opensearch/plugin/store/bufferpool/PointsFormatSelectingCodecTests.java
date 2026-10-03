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
import org.apache.lucene.codecs.lucene104.Lucene104SplitPointsCodec;
import org.apache.lucene.codecs.perfield.PerFieldPointsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class PointsFormatSelectingCodecTests extends OpenSearchTestCase {
    private static final Logger LOGGER = LogManager.getLogger(PointsFormatSelectingCodecTests.class);

    private static final String MAPPING = "{\"_doc\":{\"properties\":{"
        + "\"ts\":{\"type\":\"date\"},"
        + "\"ts_split\":{\"type\":\"date\",\"meta\":{\"points_format\":\"Lucene90Split\"}},"
        + "\"unknown\":{\"type\":\"date\",\"meta\":{\"points_format\":\"Lucene99Nope\"}},"
        + "\"two_dims\":{\"type\":\"date\",\"meta\":{\"points_format\":\"Lucene90Split\"}}"
        + "}}}";

    /** Collects the WARN events of the codec's logger. */
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

        long count(String needle) {
            return messages.stream().filter(m -> m.contains(needle)).count();
        }
    }

    private MapperService mapperService(Settings settings) throws IOException {
        final IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(settings)
            )
            .putMapping(MAPPING)
            .build();
        final MapperService mapperService = MapperTestUtils.newMapperService(
            new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
            createTempDir(),
            settings,
            "test"
        );
        mapperService.merge(indexMetadata, MapperService.MergeReason.MAPPING_UPDATE);
        return mapperService;
    }

    private static PointsFormatSelectingCodec codec(MapperService mapperService) {
        return new PointsFormatSelectingCodec(new Lucene104Codec(), mapperService, LOGGER);
    }

    /** Writes {@code segments} flushed segments; every doc has the same value in ts, ts_split, unknown and unmapped. */
    private static void index(Directory dir, Codec codec, int segments, boolean forceMerge) throws IOException {
        // no compound files, so the test can list each segment's points files
        final LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
        mergePolicy.setNoCFSRatio(0.0);
        // 3 segments stay below the merge factor, so only forceMerge merges
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setMergePolicy(mergePolicy).setUseCompoundFile(false);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            long value = 0;
            for (int s = 0; s < segments; s++) {
                for (int i = 0; i < 1000; i++) {
                    final Document doc = new Document();
                    value += 1 + (i % 7);
                    doc.add(new LongPoint("ts", value));
                    doc.add(new LongPoint("ts_split", value));
                    doc.add(new LongPoint("unknown", value));
                    doc.add(new LongPoint("unmapped", value));
                    doc.add(new LongPoint("two_dims", value, -value));
                    writer.addDocument(doc);
                }
                writer.flush();
            }
            if (forceMerge) {
                writer.forceMerge(1);
            }
            writer.commit();
        }
    }

    private static String formatAttribute(LeafReader leaf, String field) {
        return leaf.getFieldInfos().fieldInfo(field).getAttribute(PerFieldPointsFormat.PER_FIELD_FORMAT_KEY);
    }

    public void testMetaSelectsSplitAndSegmentsReopenThroughSpi() throws Exception {
        final MapperService mapperService = mapperService(Settings.EMPTY);
        final Warnings warnings = new Warnings();
        try (Directory dir = newFSDirectory(createTempDir()); MockLogAppender appender = MockLogAppender.createForLoggers(LOGGER)) {
            appender.addExpectation(warnings);
            final boolean forceMerge = randomBoolean();
            index(dir, codec(mapperService), 3, forceMerge);

            final SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
            assertEquals(forceMerge ? 1 : 3, infos.size());
            for (SegmentCommitInfo info : infos) {
                assertEquals(Lucene104SplitPointsCodec.NAME, info.info.getCodec().getName());
                // read through SPI: the fork's reading codec, not the writing codec of this plugin
                assertSame(Lucene104SplitPointsCodec.class, info.info.getCodec().getClass());
                final List<String> files = new ArrayList<>(info.files());
                for (String ext : new String[] { "kdm", "kdi", "kdd", "kdv" }) {
                    assertTrue(files.toString(), files.contains(info.info.name + "_Lucene90Split_0." + ext));
                }
                // the stock fields keep the stock files
                assertTrue(files.toString(), files.contains(info.info.name + ".kdd"));
            }
            assertSame(Lucene104SplitPointsCodec.class, Codec.forName(Lucene104SplitPointsCodec.NAME).getClass());

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                for (LeafReaderContext ctx : reader.leaves()) {
                    final LeafReader leaf = ctx.reader();
                    assertEquals(PerFieldPointsFormat.SPLIT_FORMAT_NAME, formatAttribute(leaf, "ts_split"));
                    assertNull(formatAttribute(leaf, "ts"));
                    assertNull(formatAttribute(leaf, "unmapped"));
                    final PointValues stock = leaf.getPointValues("ts");
                    final PointValues split = leaf.getPointValues("ts_split");
                    assertEquals(stock.size(), split.size());
                    assertEquals(stock.getDocCount(), split.getDocCount());
                    assertArrayEquals(stock.getMinPackedValue(), split.getMinPackedValue());
                    assertArrayEquals(stock.getMaxPackedValue(), split.getMaxPackedValue());
                }
                for (int i = 0; i < 20; i++) {
                    final long lo = randomLongBetween(0, 12000);
                    final long hi = lo + randomLongBetween(0, 5000);
                    assertEquals(
                        searcher.count(LongPoint.newRangeQuery("ts", lo, hi)),
                        searcher.count(LongPoint.newRangeQuery("ts_split", lo, hi))
                    );
                }
            }
        }
        // a field in the segment but not in the mapping is stock without a log
        assertEquals(0, warnings.count("[unmapped]"));
        assertEquals(0, warnings.count("[ts]"));
        assertEquals(0, warnings.count("[ts_split]"));
    }

    public void testUnknownNameAndTwoDimensionsFallBackWithOneWarningEach() throws Exception {
        final MapperService mapperService = mapperService(Settings.EMPTY);
        final Warnings warnings = new Warnings();
        try (Directory dir = newDirectory(); MockLogAppender appender = MockLogAppender.createForLoggers(LOGGER)) {
            appender.addExpectation(warnings);
            // three flushes and a merge ask the chooser four times per field
            index(dir, codec(mapperService), 3, true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                assertNull(formatAttribute(leaf, "unknown"));
                assertNull(formatAttribute(leaf, "two_dims"));
                assertEquals(PerFieldPointsFormat.SPLIT_FORMAT_NAME, formatAttribute(leaf, "ts_split"));
                assertEquals(3000, leaf.getPointValues("unknown").size());
                assertEquals(3000, leaf.getPointValues("two_dims").size());
            }
        }
        assertEquals(warnings.messages.toString(), 1, warnings.count("field [unknown]: points format [Lucene99Nope]"));
        assertEquals(warnings.messages.toString(), 1, warnings.count("field [two_dims]: points format [Lucene90Split]"));
        assertEquals(warnings.messages.toString(), 2, warnings.messages.size());
    }

    public void testIndexSortKeepsStockWithOneWarning() throws Exception {
        final MapperService mapperService = mapperService(Settings.builder().put("index.sort.field", "ts").build());
        assertTrue(mapperService.getIndexSettings().getIndexSortConfig().hasIndexSort());
        final PointsFormatSelectingCodec codec = codec(mapperService);
        final Warnings warnings = new Warnings();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LOGGER)) {
            appender.addExpectation(warnings);
            for (int i = 0; i < 3; i++) {
                assertSame(PerFieldPointsFormat.STOCK, codec.choose(fieldInfo("ts_split", 1)));
                // no meta: stock, and nothing to warn about
                assertSame(PerFieldPointsFormat.STOCK, codec.choose(fieldInfo("ts", 1)));
            }
        }
        assertEquals(warnings.messages.toString(), 1, warnings.count("field [ts_split]: points format [Lucene90Split] is not used"));
        assertEquals(warnings.messages.toString(), 1, warnings.messages.size());
    }

    public void testChooserOrder() throws Exception {
        final PointsFormatSelectingCodec codec = codec(mapperService(Settings.EMPTY));
        final Warnings warnings = new Warnings();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LOGGER)) {
            appender.addExpectation(warnings);
            assertSame(PerFieldPointsFormat.SPLIT, codec.choose(fieldInfo("ts_split", 1)));
            assertSame(PerFieldPointsFormat.STOCK, codec.choose(fieldInfo("ts", 1)));
            assertSame(PerFieldPointsFormat.STOCK, codec.choose(fieldInfo("not_in_mapping", 1)));
            assertSame(PerFieldPointsFormat.STOCK, codec.choose(fieldInfo("ts_split", 2)));
            assertSame(PerFieldPointsFormat.STOCK, codec.choose(fieldInfo("ts_split", 2)));
            assertSame(PerFieldPointsFormat.STOCK, codec.choose(fieldInfo("unknown", 1)));
        }
        assertEquals(
            warnings.messages.toString(),
            Arrays.asList(1L, 1L),
            List.of(warnings.count("[ts_split]"), warnings.count("[unknown]"))
        );
        assertEquals(0, warnings.count("[not_in_mapping]"));
        assertEquals(Lucene104SplitPointsCodec.NAME, codec.getName());
    }

    /** A points field info with {@code dims} data and index dimensions of 8 bytes. */
    private static FieldInfo fieldInfo(String name, int dims) {
        return new FieldInfo(
            name,
            0,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NONE,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            dims,
            dims,
            Long.BYTES,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }
}
