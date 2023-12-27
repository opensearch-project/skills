/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.lucene.search.TotalHits;
import org.mockito.Mockito;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.transport.GetAnomalyDetectorResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;

public class TestHelpers {

    public static SearchResponse generateSearchResponse(SearchHit[] hits) {
        TotalHits totalHits = new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO);
        return new SearchResponse(
            new SearchResponseSections(new SearchHits(hits, totalHits, 0), new Aggregations(new ArrayList<>()), null, false, null, null, 0),
            null,
            0,
            0,
            0,
            0,
            null,
            null
        );
    }

    public static GetAnomalyDetectorResponse generateGetAnomalyDetectorResponses(String[] detectorIds, String[] detectorStates) {
        AnomalyDetector detector = Mockito.mock(AnomalyDetector.class);
        when(detector.getId()).thenReturn(detectorIds[0], Arrays.copyOfRange(detectorIds, 1, detectorIds.length));
        ADTask realtimeAdTask = Mockito.mock(ADTask.class);
        when(realtimeAdTask.getState()).thenReturn(detectorStates[0], Arrays.copyOfRange(detectorStates, 1, detectorStates.length));
        GetAnomalyDetectorResponse getDetectorProfileResponse = Mockito.mock(GetAnomalyDetectorResponse.class);
        when(getDetectorProfileResponse.getRealtimeAdTask()).thenReturn(realtimeAdTask);
        when(getDetectorProfileResponse.getDetector()).thenReturn(detector);
        return getDetectorProfileResponse;
    }

    public static SearchHit generateSearchDetectorHit(String detectorName, String detectorId) throws IOException {
        XContentBuilder content = XContentBuilder.builder(XContentType.JSON.xContent());
        content.startObject();
        content.field("name", detectorName);
        content.endObject();
        return new SearchHit(0, detectorId, null, null).sourceRef(BytesReference.bytes(content));
    }
}
