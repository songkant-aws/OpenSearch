/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.test.OpenSearchTestCase;

public class OpenSearchRelMetadataQueryTests extends OpenSearchTestCase {

    public void testHashBuildEstimateUsesWidthOverheadAndPartitions() {
        OpenSearchRelMetadataQuery.HashBuildEstimate estimate = OpenSearchRelMetadataQuery.estimateHashBuild(1_000d, 200d, 4, true);

        assertEquals(1_000d, estimate.rows(), 0d);
        assertEquals(200d, estimate.rowWidthBytes(), 0d);
        assertEquals(75_000d, estimate.bytesPerWorker(), 0d);
        assertTrue(estimate.fitsHashJoin(2_000, 80_000));
        assertFalse("byte budget must reject an otherwise row-safe HJ", estimate.fitsHashJoin(2_000, 70_000));
        assertFalse("row budget remains an independent guard", estimate.fitsHashJoin(1_000, 80_000));
    }

    public void testUnknownHashBuildEstimatePrefersSortMerge() {
        // Calcite's nominal row fallback must not count when table statistics are unknown.
        OpenSearchRelMetadataQuery.HashBuildEstimate estimate = OpenSearchRelMetadataQuery.estimateHashBuild(100d, 8d, 4, false);

        assertFalse(estimate.fitsHashJoin(Long.MAX_VALUE, Long.MAX_VALUE));
        assertTrue(Double.isNaN(estimate.rows()));
        assertTrue(Double.isNaN(estimate.bytesPerWorker()));
    }
}
