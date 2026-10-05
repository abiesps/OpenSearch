/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import org.apache.lucene.codecs.lucene104.Lucene104NavPostingsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104SplitPointsCodec;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPointsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexModule;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * A {@code bufferpoolfs} index with {@code index.codec: best_compression}, a date field that asks for the split points
 * format and a keyword field that asks for the Nav postings format, on a node with the plugin: the codec service the
 * plugin gives the engine writes both formats and keeps the best-compression stored fields; after a full cluster restart
 * the field attributes and files are still there, the queries return the same results, and new segments get the formats
 * too. The mapping check refuses a format name that is not available.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class BufferPoolCodecIT extends OpenSearchIntegTestCase {
    private static final String INDEX = "codec_it";
    private static final String MAPPING = "{\"properties\":{"
        + "\"ts\":{\"type\":\"date\"},"
        + "\"ts_split\":{\"type\":\"date\",\"meta\":{\"points_format\":\"Lucene90Split\"}},"
        + "\"kw\":{\"type\":\"keyword\"},"
        + "\"kw_nav\":{\"type\":\"keyword\",\"meta\":{\"postings_format\":\"Lucene104Nav\"}},"
        + "\"body\":{\"type\":\"keyword\",\"index\":false}"
        + "}}";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(BufferPoolStorePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(BufferPoolStorePlugin.CACHE_SIZE_SETTING.getKey(), "32mb")
            .build();
    }

    private void createBufferPoolIndex() {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), BufferPoolStorePlugin.STORE_TYPE)
                    .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), CodecService.BEST_COMPRESSION_CODEC)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.refresh_interval", -1)
            ).setMapping(MAPPING)
        );
        ensureGreen(INDEX);
    }

    private void indexDocs(int from, int count) {
        final BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = from; i < from + count; i++) {
            final long ts = randomLongBetween(1_600_000_000_000L, 1_700_000_000_000L);
            final String kw = "v" + randomIntBetween(0, 40);
            bulk.add(
                client().prepareIndex(INDEX)
                    .setId(Integer.toString(i))
                    .setSource("ts", ts, "ts_split", ts, "kw", kw, "kw_nav", kw, "body", "document " + i)
            );
        }
        final BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
    }

    /** Hit counts and the ids of the first hits of each query, in a fixed order; the stock twin fields must agree. */
    private Map<String, List<String>> results() {
        final Map<String, List<String>> out = new LinkedHashMap<>();
        for (int q = 0; q < 10; q++) {
            final long lo = 1_600_000_000_000L + q * 9_000_000_000L;
            final long hi = lo + 20_000_000_000L;
            out.put("range " + lo + " " + hi, query("ts_split", QueryBuilders.rangeQuery("ts_split").gte(lo).lte(hi)));
            assertEquals(query("ts", QueryBuilders.rangeQuery("ts").gte(lo).lte(hi)), out.get("range " + lo + " " + hi));
            final String kw = "v" + q * 4;
            out.put("term " + kw, query("kw_nav", QueryBuilders.termQuery("kw_nav", kw)));
            assertEquals(query("kw", QueryBuilders.termQuery("kw", kw)), out.get("term " + kw));
        }
        return out;
    }

    private List<String> query(String field, QueryBuilder query) {
        final SearchResponse response = client().prepareSearch(INDEX)
            .setQuery(query)
            .setTrackTotalHits(true)
            .setSize(20)
            .addSort("ts", SortOrder.ASC)
            .addSort("kw", SortOrder.ASC)
            .setRequestCache(false)
            .get();
        final List<String> out = new ArrayList<>();
        out.add("total " + response.getHits().getTotalHits().value());
        for (SearchHit hit : response.getHits().getHits()) {
            out.add(hit.getId());
        }
        return out;
    }

    /** Every segment of the last commit, read from the shard's directory: codec, stored-fields mode, field formats, files. */
    private int checkSegments() throws Exception {
        final IndicesService indices = internalCluster().getDataNodeInstance(IndicesService.class);
        final IndexShard shard = indices.indexServiceSafe(resolveIndex(INDEX)).getShard(0);
        final Store store = shard.store();
        store.incRef();
        try {
            final Directory dir = store.directory();
            final SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
            assertTrue(infos.size() > 0);
            final Map<String, FieldInfos> fieldInfos = new LinkedHashMap<>();
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    fieldInfos.put(
                        FilterLeafReader.unwrap(ctx.reader()) instanceof SegmentReader sr ? sr.getSegmentName() : ctx.toString(),
                        ctx.reader().getFieldInfos()
                    );
                }
            }
            for (SegmentCommitInfo info : infos) {
                final String segment = info.info.name;
                assertEquals(segment, Lucene104SplitPointsCodec.NAME, info.info.getCodec().getName());
                assertEquals(segment, "BEST_COMPRESSION", info.info.getAttribute(Lucene90StoredFieldsFormat.MODE_KEY));
                final FieldInfos fis = fieldInfos.get(segment);
                assertNotNull(segment + " " + fieldInfos.keySet(), fis);
                assertEquals(PerFieldPointsFormat.SPLIT_FORMAT_NAME, attribute(fis, "ts_split", PerFieldPointsFormat.PER_FIELD_FORMAT_KEY));
                assertNull(attribute(fis, "ts", PerFieldPointsFormat.PER_FIELD_FORMAT_KEY));
                assertEquals(Lucene104NavPostingsFormat.NAME, attribute(fis, "kw_nav", PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY));
                assertNotEquals(Lucene104NavPostingsFormat.NAME, attribute(fis, "kw", PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY));
                final String navSuffix = attribute(fis, "kw_nav", PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY);
                final TreeSet<String> files = new TreeSet<>();
                if (info.info.getUseCompoundFile()) {
                    try (Directory cfs = info.info.getCodec().compoundFormat().getCompoundReader(dir, info.info)) {
                        files.addAll(Arrays.asList(cfs.listAll()));
                    }
                } else {
                    files.addAll(info.files());
                }
                for (String ext : new String[] { "kdm", "kdi", "kdd" }) {
                    final String file = segment + "_" + PerFieldPointsFormat.SPLIT_FORMAT_NAME + "_0." + ext;
                    assertTrue(file + " in " + files, files.contains(file));
                }
                final String nav = segment
                    + "_"
                    + Lucene104NavPostingsFormat.NAME
                    + "_"
                    + navSuffix
                    + "."
                    + Lucene104NavPostingsFormat.NAV_EXTENSION;
                assertTrue(nav + " in " + files, files.contains(nav));
            }
            return infos.size();
        } finally {
            store.decRef();
        }
    }

    private static String attribute(FieldInfos infos, String field, String key) {
        final FieldInfo fi = infos.fieldInfo(field);
        assertNotNull(field, fi);
        return fi.getAttribute(key);
    }

    public void testFormatsSurviveRestartAndQueriesAgree() throws Exception {
        createBufferPoolIndex();
        final int first = randomIntBetween(300, 1500);
        indexDocs(0, first);
        flush(INDEX);
        if (randomBoolean()) {
            indexDocs(first, randomIntBetween(1, 300));
            flush(INDEX);
        }
        refresh(INDEX);
        checkSegments();
        final Map<String, List<String>> before = results();

        internalCluster().fullRestart();
        ensureGreen(INDEX);

        checkSegments();
        assertEquals(before, results());

        // the codec service after the restart writes the formats into new and merged segments
        indexDocs(10_000, randomIntBetween(1, 300));
        flush(INDEX);
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
        flush(INDEX);
        refresh(INDEX);
        assertEquals(1, checkSegments());
        results();
    }

    public void testUnavailableFormatNameIsRefused() {
        final IllegalArgumentException create = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate("bad").setSettings(
                Settings.builder().put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), BufferPoolStorePlugin.STORE_TYPE)
            ).setMapping("{\"properties\":{\"kw\":{\"type\":\"keyword\",\"meta\":{\"postings_format\":\"Lucene104Navv\"}}}}").get()
        );
        assertTrue(create.getMessage(), create.getMessage().contains("[meta.postings_format] [Lucene104Navv]"));
        assertFalse(indexExists("bad"));

        createBufferPoolIndex();
        final IllegalArgumentException put = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .preparePutMapping(INDEX)
                .setSource(
                    "{\"properties\":{\"ts2\":{\"type\":\"date\",\"meta\":{\"points_format\":\"Lucene90\"}}}}",
                    MediaTypeRegistry.JSON
                )
                .get()
        );
        assertTrue(put.getMessage(), put.getMessage().contains("[meta.points_format] [Lucene90]"));
        final Map<?, ?> properties = (Map<?, ?>) client().admin()
            .indices()
            .prepareGetMappings(INDEX)
            .get()
            .getMappings()
            .get(INDEX)
            .getSourceAsMap()
            .get("properties");
        assertFalse(properties.toString(), properties.containsKey("ts2"));

        // another store type keeps the entries as plain field metadata
        assertAcked(
            prepareCreate("plain").setMapping(
                "{\"properties\":{\"kw\":{\"type\":\"keyword\",\"meta\":{\"postings_format\":\"Lucene104Navv\"}}}}"
            )
        );
    }
}
