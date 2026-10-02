/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent.support;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.opensearch.common.Booleans;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;

/**
 * Filters a stored JSON {@code _source} while it is parsed, and writes the kept parts as raw bytes.
 * <p>
 * It applies exactly the include and exclude rules of {@link XContentMapValues#filter(String[], String[], boolean)}: the same
 * set-based rules when no pattern has a wildcard or a dot, otherwise the same automata and the same rules for objects, arrays,
 * dotted keys and objects that end up empty. It does not build a {@code Map}:
 * <ul>
 *   <li>values that are not kept are skipped by the tokenizer and never decoded;</li>
 *   <li>kept keys and kept scalar values are copied byte for byte from the source, so strings are not decoded and encoded again;</li>
 *   <li>the duplicate-key check is skipped, because the source was checked when it was indexed.</li>
 * </ul>
 * The output is the same JSON as the map-based filter, except that it keeps the source order of keys and the source spelling of
 * scalars (for example {@code 1.0e2} or a {@code \u00e9} escape stays as written instead of being normalized).
 * <p>
 * {@link #filter(BytesReference)} returns {@code null} when the source cannot be handled here (not JSON, compressed, or malformed);
 * the caller then uses the map-based filter, which also produces the same error for malformed sources.
 *
 * @opensearch.internal
 */
public final class JsonSourceBytesFilter {

    /** Kill switch for experiments: {@code -Dopensearch.search.fetch.source_bytes_filter=false} restores the map-based filter. */
    public static final boolean ENABLED = Booleans.parseBoolean(System.getProperty("opensearch.search.fetch.source_bytes_filter", "true"));

    private final CharacterRunAutomaton include;
    private final CharacterRunAutomaton exclude;
    private final CharacterRunAutomaton matchAll;
    /** Non-null when the patterns have no wildcards and no dots: XContentMapValues then uses its set-based filter, so do we. */
    private final Set<String> includeSet;
    private final Set<String> excludeSet;
    private final boolean setBased;

    public JsonSourceBytesFilter(String[] includes, String[] excludes) {
        this.setBased = XContentMapValues.hasNoWildcardsOrDots(includes) && XContentMapValues.hasNoWildcardsOrDots(excludes);
        if (setBased) {
            this.includeSet = (includes == null || includes.length == 0) ? null : new HashSet<>(Arrays.asList(includes));
            this.excludeSet = (excludes == null || excludes.length == 0) ? Collections.emptySet() : new HashSet<>(Arrays.asList(excludes));
            this.include = this.exclude = this.matchAll = null;
        } else {
            CharacterRunAutomaton[] a = XContentMapValues.filterAutomata(includes, excludes, true);
            this.include = a[0];
            this.exclude = a[1];
            this.matchAll = a[2];
            this.includeSet = this.excludeSet = null;
        }
    }

    /** Returns the filtered source as JSON bytes, or {@code null} if this source must go through the map-based filter. */
    public BytesReference filter(BytesReference source) {
        if (source == null || source.length() == 0) {
            return null;
        }
        final BytesRef ref = source.toBytesRef();
        final byte[] b = ref.bytes;
        final int end = ref.offset + ref.length;
        int start = ref.offset;
        while (start < end && isWhitespace(b[start])) {
            start++;
        }
        if (start == end || b[start] != '{') {
            return null; // SMILE, CBOR, YAML, compressed (the compressor headers never start with '{') or not an object
        }
        try (JsonParser p = JsonXContent.createRawParserWithoutDuplicateCheck(b, ref.offset, ref.length)) {
            if (p.nextToken() != JsonToken.START_OBJECT) {
                return null;
            }
            // Token offsets are relative to the parser start; check that once against the '{' we found.
            final int base = start - (int) p.currentTokenLocation().getByteOffset();
            if (base != ref.offset) {
                return null;
            }
            final Out out = new Out(Math.min(1024, ref.length));
            out.put('{');
            if (setBased) {
                filterTopLevelBySet(p, b, base, end, out);
            } else {
                filterObject(p, b, base, end, out, include, 0, exclude, 0);
            }
            out.put('}');
            return new BytesArray(out.buf, 0, out.len);
        } catch (JacksonException | MalformedException e) {
            return null;
        }
    }

    /** Mirrors XContentMapValues#createSetBasedFilter: top-level keys only, a dotted key matches on its part before the first dot. */
    private void filterTopLevelBySet(JsonParser p, byte[] b, int base, int end, Out out) {
        int written = 0;
        JsonToken t;
        while ((t = p.nextToken()) == JsonToken.PROPERTY_NAME) {
            String k = p.currentName();
            final int dotPos = k.indexOf('.');
            if (dotPos > 0) {
                k = k.substring(0, dotPos);
            }
            if ((includeSet == null || includeSet.contains(k)) && excludeSet.contains(k) == false) {
                final int keyStart = base + (int) p.currentTokenLocation().getByteOffset();
                written = member(out, written, b, keyStart, end);
                copyValue(p, p.nextToken(), b, base, end, out);
            } else {
                p.nextToken();
                p.skipChildren();
            }
        }
        if (t != JsonToken.END_OBJECT) {
            throw new MalformedException();
        }
    }

    /** Current token is START_OBJECT; consumes up to END_OBJECT. Mirrors XContentMapValues#filter(Map, ...). Returns members written. */
    private int filterObject(
        JsonParser p,
        byte[] b,
        int base,
        int end,
        Out out,
        CharacterRunAutomaton inc,
        int initialIncludeState,
        CharacterRunAutomaton exc,
        int initialExcludeState
    ) {
        int written = 0;
        JsonToken t;
        while ((t = p.nextToken()) == JsonToken.PROPERTY_NAME) {
            final String key = p.currentName();
            final int includeState = XContentMapValues.step(inc, key, initialIncludeState);
            int excludeState = includeState == -1 ? -1 : XContentMapValues.step(exc, key, initialExcludeState);
            if (includeState == -1 || (excludeState != -1 && exc.isAccept(excludeState))) {
                p.nextToken();
                p.skipChildren();
                continue;
            }
            // only kept keys pay for the token location
            final int keyStart = base + (int) p.currentTokenLocation().getByteOffset();
            final JsonToken v = p.nextToken();

            final boolean includeAccepted = inc.isAccept(includeState);
            CharacterRunAutomaton subInclude = inc;
            int subIncludeState = includeState;
            if (includeAccepted) {
                if (excludeState == -1 || exc.step(excludeState, '.') == -1) {
                    // the exclude has no chance to match inner properties: keep the whole value
                    written = member(out, written, b, keyStart, end);
                    copyValue(p, v, b, base, end, out);
                    continue;
                } else {
                    // the object matched, so every inner property is included; only excludes matter now
                    subInclude = matchAll;
                    subIncludeState = 0;
                }
            }

            if (v == JsonToken.START_OBJECT) {
                subIncludeState = subInclude.step(subIncludeState, '.');
                if (subIncludeState == -1) {
                    p.skipChildren();
                    continue;
                }
                if (excludeState != -1) {
                    excludeState = exc.step(excludeState, '.');
                }
                final int mark = out.len;
                final int markWritten = written;
                written = member(out, written, b, keyStart, end);
                out.put('{');
                final int n = filterObject(p, b, base, end, out, subInclude, subIncludeState, exc, excludeState);
                if (includeAccepted || n > 0) {
                    out.put('}');
                } else {
                    out.len = mark;
                    written = markWritten;
                }
            } else if (v == JsonToken.START_ARRAY) {
                final int mark = out.len;
                final int markWritten = written;
                written = member(out, written, b, keyStart, end);
                out.put('[');
                final int n = filterArray(p, b, base, end, out, subInclude, subIncludeState, exc, excludeState);
                if (includeAccepted || n > 0) {
                    out.put(']');
                } else {
                    out.len = mark;
                    written = markWritten;
                }
            } else if (includeAccepted && (excludeState == -1 || exc.isAccept(excludeState) == false)) {
                // leaf property
                written = member(out, written, b, keyStart, end);
                copyScalar(p, v, b, base, end, out);
            }
        }
        if (t != JsonToken.END_OBJECT) {
            throw new MalformedException();
        }
        return written;
    }

    /** Current token is START_ARRAY; consumes up to END_ARRAY. Mirrors XContentMapValues#filter(Iterable, ...). Returns elements written. */
    private int filterArray(
        JsonParser p,
        byte[] b,
        int base,
        int end,
        Out out,
        CharacterRunAutomaton inc,
        int initialIncludeState,
        CharacterRunAutomaton exc,
        int initialExcludeState
    ) {
        final boolean isInclude = inc.isAccept(initialIncludeState);
        int written = 0;
        JsonToken t;
        while ((t = p.nextToken()) != JsonToken.END_ARRAY) {
            if (t == null) {
                throw new MalformedException();
            }
            if (t == JsonToken.START_OBJECT) {
                final int includeState = inc.step(initialIncludeState, '.');
                if (includeState == -1) {
                    p.skipChildren(); // every key would be dropped, so the object is empty and dropped
                    continue;
                }
                final int excludeState = initialExcludeState == -1 ? -1 : exc.step(initialExcludeState, '.');
                final int mark = out.len;
                if (written > 0) {
                    out.put(',');
                }
                out.put('{');
                final int n = filterObject(p, b, base, end, out, inc, includeState, exc, excludeState);
                if (n > 0) {
                    out.put('}');
                    written++;
                } else {
                    out.len = mark;
                }
            } else if (t == JsonToken.START_ARRAY) {
                final int mark = out.len;
                if (written > 0) {
                    out.put(',');
                }
                out.put('[');
                final int n = filterArray(p, b, base, end, out, inc, initialIncludeState, exc, initialExcludeState);
                if (n > 0) {
                    out.put(']');
                    written++;
                } else {
                    out.len = mark;
                }
            } else if (isInclude) {
                if (written > 0) {
                    out.put(',');
                }
                copyScalar(p, t, b, base, end, out);
                written++;
            }
        }
        return written;
    }

    /** Writes the separator and the raw key bytes ({@code "key":}) of the current property. */
    private static int member(Out out, int written, byte[] b, int keyStart, int end) {
        if (b[keyStart] != '"') {
            throw new MalformedException();
        }
        if (written > 0) {
            out.put(',');
        }
        final int keyEnd = stringEnd(b, keyStart, end);
        out.write(b, keyStart, keyEnd - keyStart);
        out.put(':');
        return written + 1;
    }

    /** Copies a whole value (scalar, object or array) token by token, with raw scalar and key bytes. */
    private static void copyValue(JsonParser p, JsonToken t, byte[] b, int base, int end, Out out) {
        if (t == JsonToken.START_OBJECT) {
            out.put('{');
            int n = 0;
            while ((t = p.nextToken()) == JsonToken.PROPERTY_NAME) {
                n = member(out, n, b, base + (int) p.currentTokenLocation().getByteOffset(), end);
                copyValue(p, p.nextToken(), b, base, end, out);
            }
            if (t != JsonToken.END_OBJECT) {
                throw new MalformedException();
            }
            out.put('}');
        } else if (t == JsonToken.START_ARRAY) {
            out.put('[');
            int n = 0;
            while ((t = p.nextToken()) != JsonToken.END_ARRAY) {
                if (t == null) {
                    throw new MalformedException();
                }
                if (n++ > 0) {
                    out.put(',');
                }
                copyValue(p, t, b, base, end, out);
            }
            out.put(']');
        } else {
            copyScalar(p, t, b, base, end, out);
        }
    }

    /** Copies the raw bytes of the current scalar token (string, number, true, false or null). */
    private static void copyScalar(JsonParser p, JsonToken t, byte[] b, int base, int end, Out out) {
        final int s = base + (int) p.currentTokenLocation().getByteOffset();
        final int e;
        if (t == JsonToken.VALUE_STRING) {
            if (b[s] != '"') {
                throw new MalformedException();
            }
            e = stringEnd(b, s, end);
        } else if (t != null && t.isScalarValue()) {
            int i = s;
            while (i < end && isScalarDelimiter(b[i]) == false) {
                i++;
            }
            e = i;
        } else {
            throw new MalformedException();
        }
        if (e <= s) {
            throw new MalformedException();
        }
        out.write(b, s, e - s);
    }

    /** {@code b[s]} is the opening quote; returns the index after the closing quote. */
    private static int stringEnd(byte[] b, int s, int end) {
        for (int i = s + 1; i < end; i++) {
            final byte c = b[i];
            if (c == '"') {
                return i + 1;
            }
            if (c == '\\') {
                i++;
            }
        }
        throw new MalformedException();
    }

    private static boolean isWhitespace(byte c) {
        return c == ' ' || c == '\n' || c == '\r' || c == '\t';
    }

    private static boolean isScalarDelimiter(byte c) {
        return isWhitespace(c) || c == ',' || c == '}' || c == ']' || c == '/';
    }

    /** Thrown when an offset assumption does not hold; the caller falls back to the map-based filter. */
    private static final class MalformedException extends RuntimeException {
        MalformedException() {
            super(null, null, false, false);
        }
    }

    /** Minimal growable byte buffer that can be truncated (to drop objects that end up empty). */
    private static final class Out {
        byte[] buf;
        int len;

        Out(int capacity) {
            buf = new byte[Math.max(16, capacity)];
        }

        void put(char c) {
            if (len == buf.length) {
                buf = Arrays.copyOf(buf, buf.length << 1);
            }
            buf[len++] = (byte) c;
        }

        void write(byte[] src, int off, int n) {
            if (len + n > buf.length) {
                buf = Arrays.copyOf(buf, Math.max(buf.length << 1, len + n));
            }
            System.arraycopy(src, off, buf, len, n);
            len += n;
        }
    }
}
