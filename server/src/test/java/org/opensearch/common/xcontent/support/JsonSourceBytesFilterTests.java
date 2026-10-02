/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent.support;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JsonSourceBytesFilterTests extends OpenSearchTestCase {

    private static final String[] KEYS = { "a", "b", "c", "a.b", "x", "x.y", "y", "z", "é", "\uD83D\uDE00", "q\"t", "obj", "arr" };
    private static final String[] PATTERNS = {
        "a",
        "b",
        "c",
        "a.b",
        "a.c",
        "a.*",
        "*.b",
        "*",
        "x",
        "x.y",
        "x.y.z",
        "a*",
        "*a",
        "é",
        "\uD83D\uDE00",
        "obj.a",
        "arr",
        "arr.a",
        "arr.b.c",
        "*.a",
        "o*",
        "q\"t",
        "y.z",
        "z.*" };

    /** Map-based filter (today's path): the expected result. */
    private static Map<String, Object> expected(BytesReference source, String[] includes, String[] excludes) {
        Map<String, Object> map = XContentHelper.convertToMap(source, false, MediaTypeRegistry.JSON).v2();
        return XContentMapValues.filter(includes, excludes, true).apply(map);
    }

    private static Map<String, Object> actual(BytesReference source, String[] includes, String[] excludes) {
        BytesReference filtered = new JsonSourceBytesFilter(includes, excludes).filter(source);
        assertNotNull("byte filter must handle JSON sources", filtered);
        return XContentHelper.convertToMap(filtered, false, MediaTypeRegistry.JSON).v2();
    }

    private static void assertSame(String json, String[] includes, String[] excludes) {
        BytesReference source = new BytesArray(json);
        assertEquals(
            json + " includes=" + List.of(includes) + " excludes=" + List.of(excludes),
            expected(source, includes, excludes),
            actual(source, includes, excludes)
        );
    }

    public void testRandomizedEquivalenceWithMapFilter() throws IOException {
        for (int iter = 0; iter < 2000; iter++) {
            Map<String, Object> doc = randomObject(0);
            XContentBuilder builder = randomBoolean() ? JsonXContent.contentBuilder().prettyPrint() : JsonXContent.contentBuilder();
            BytesReference source = BytesReference.bytes(builder.map(doc));
            String[] includes = randomPatterns(4);
            String[] excludes = randomPatterns(3);
            assertEquals(
                source.utf8ToString() + " includes=" + List.of(includes) + " excludes=" + List.of(excludes),
                expected(source, includes, excludes),
                actual(source, includes, excludes)
            );
        }
    }

    public void testKnownCases() {
        // objects that end up empty, arrays of objects, dotted keys, excludes inside includes
        assertSame("{\"a\":{\"b\":1,\"c\":2},\"d\":3}", new String[] { "a.b" }, new String[0]);
        assertSame("{\"a\":{\"b\":1,\"c\":2},\"d\":3}", new String[] { "a" }, new String[] { "a.c" });
        assertSame("{\"a\":{},\"b\":{\"c\":{}}}", new String[] { "a", "b.c" }, new String[0]);
        assertSame("{\"a\":{},\"b\":{\"c\":{}}}", new String[0], new String[] { "b.c" });
        assertSame("{\"a.b\":1,\"a\":{\"b\":2,\"c\":3}}", new String[] { "a.b" }, new String[0]);
        assertSame("{\"arr\":[{\"a\":1,\"b\":2},{\"b\":3},1,[{\"a\":4}],[]]}", new String[] { "arr.a" }, new String[0]);
        assertSame("{\"arr\":[{\"a\":1,\"b\":2},{\"b\":3},1,[{\"a\":4}],[]]}", new String[] { "arr" }, new String[] { "arr.b" });
        assertSame("{\"arr\":[[1,2],[{\"a\":{}}]]}", new String[] { "arr.a" }, new String[0]);
        assertSame(
            "{\"n\":null,\"t\":true,\"f\":false,\"i\":-12,\"d\":1.5e3,\"s\":\"q\\\"\\\\\\u00e9\"}",
            new String[] { "*" },
            new String[] { "t" }
        );
        assertSame("{\"pdpUrl\":{\"url\":\"https://x/y\"},\"productCode\":\"HQ0504-133\"}", new String[] { "productCode" }, new String[0]);
        assertSame("{\"pdpUrl\":{\"url\":\"https://x/y\"},\"productCode\":\"HQ0504-133\"}", new String[0], new String[0]);
        assertSame("{\"a\":1}", new String[] { "missing" }, new String[0]);
        // comments are allowed in indexed JSON; they must not leak into the output
        assertSame("{\"a\":1 /* c */, // d\n \"b\":{\"c\":[1, /*x*/ 2]}}", new String[] { "a", "b" }, new String[0]);
    }

    public void testExactOutputForSimpleInclude() {
        BytesReference src = new BytesArray("{ \"x\" : [1, 2],\n  \"productCode\" : \"HQ0504-133\" , \"y\":{\"z\":\"é\"} }");
        BytesReference out = new JsonSourceBytesFilter(new String[] { "productCode" }, new String[0]).filter(src);
        assertEquals("{\"productCode\":\"HQ0504-133\"}", out.utf8ToString());
    }

    public void testSourceInsideLargerArray() {
        byte[] json = "{\"a\":1,\"b\":\"two\"}".getBytes(StandardCharsets.UTF_8);
        byte[] padded = new byte[json.length + 13];
        System.arraycopy(json, 0, padded, 7, json.length);
        BytesReference src = new BytesArray(padded, 7, json.length);
        assertEquals("{\"b\":\"two\"}", new JsonSourceBytesFilter(new String[] { "b" }, new String[0]).filter(src).utf8ToString());
    }

    public void testFallsBackForOtherFormatsAndBadInput() throws IOException {
        JsonSourceBytesFilter f = new JsonSourceBytesFilter(new String[] { "a" }, new String[0]);
        Map<String, Object> doc = Map.of("a", 1);
        for (XContentType type : new XContentType[] { XContentType.SMILE, XContentType.CBOR, XContentType.YAML }) {
            BytesReference bytes = BytesReference.bytes(XContentBuilder.builder(type.xContent()).map(doc));
            assertNull(type.toString(), f.filter(bytes));
        }
        assertNull(f.filter(new BytesArray("[1,2]")));
        assertNull(f.filter(new BytesArray("{\"a\":")));
        assertNull(f.filter(new BytesArray("{\"a\":1")));
        assertNull(f.filter(null));
    }

    private static String[] randomPatterns(int max) {
        int n = randomIntBetween(0, max);
        String[] p = new String[n];
        for (int i = 0; i < n; i++) {
            p[i] = randomFrom(PATTERNS);
        }
        return p;
    }

    private static Map<String, Object> randomObject(int depth) {
        Map<String, Object> m = new LinkedHashMap<>();
        int n = randomIntBetween(0, 5);
        for (int i = 0; i < n; i++) {
            m.put(randomFrom(KEYS), randomValue(depth + 1));
        }
        return m;
    }

    private static Object randomValue(int depth) {
        int kind = randomIntBetween(0, depth >= 4 ? 5 : 7);
        switch (kind) {
            case 0:
                return null;
            case 1:
                return randomBoolean();
            case 2:
                return randomInt();
            case 3:
                return randomDouble();
            case 4:
            case 5:
                return randomFrom(
                    "x",
                    "",
                    "q\"uote",
                    "back\\slash",
                    "é",
                    "\uD83D\uDE00",
                    "a/b//c",
                    "/* not a comment */",
                    randomAlphaOfLength(5)
                );
            case 6:
                return randomObject(depth);
            default:
                List<Object> l = new ArrayList<>();
                int len = randomIntBetween(0, 4);
                for (int i = 0; i < len; i++) {
                    l.add(randomValue(depth + 1));
                }
                return l;
        }
    }
}
