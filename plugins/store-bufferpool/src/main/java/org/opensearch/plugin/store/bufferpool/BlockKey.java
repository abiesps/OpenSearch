/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.bufferpool;

import java.nio.file.Path;

/**
 * Cache key of one block of one file.
 *
 * @param file        absolute path of the file, used to purge a whole directory on close
 * @param fileId      id the directory assigned to this incarnation of the file; a file re-created under the same
 *                    name gets a new id, so blocks cached from the old file can never be served for the new one
 * @param blockOffset block-aligned offset of the block in the file
 */
record BlockKey(Path file, long fileId, long blockOffset) {
}
