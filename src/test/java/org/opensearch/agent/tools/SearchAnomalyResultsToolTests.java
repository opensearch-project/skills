/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.agent.tools.utils.ToolConstants;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;

public class SearchAnomalyResultsToolTests {
    @Mock
    private NamedWriteableRegistry namedWriteableRegistry;
    @Mock
    private NodeClient nodeClient;

    private Map<String, String> nullParams;
    private Map<String, String> emptyParams;
    private Map<String, String> nonEmptyParams;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        SearchAnomalyResultsTool.Factory.getInstance().init(nodeClient, namedWriteableRegistry);

        nullParams = null;
        emptyParams = Collections.emptyMap();
        nonEmptyParams = Map.of("detectorId", "foo");
    }

    @Test
    public void testParseParams() throws Exception {
        Tool tool = SearchAnomalyResultsTool.Factory.getInstance().create(Collections.emptyMap());
        Map<String, String> validParams = new HashMap<String, String>();
        validParams.put("detectorId", "foo");
        validParams.put("realTime", "true");
        validParams.put("anomalyGradethreshold", "-1");
        validParams.put("dataStartTime", "1234");
        validParams.put("dataEndTime", "5678");
        validParams.put("sortOrder", "AsC");
        validParams.put("sortString", "foo.bar");
        validParams.put("size", "10");
        validParams.put("startIndex", "0");

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);
        assertDoesNotThrow(() -> tool.run(validParams, listener));
    }

    @Test
    public void testRunWithInvalidAnomalyGradeParam() throws Exception {
        Tool tool = SearchAnomalyResultsTool.Factory.getInstance().create(Collections.emptyMap());

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);
        assertThrows(NumberFormatException.class, () -> tool.run(Map.of("anomalyGradeThreshold", "foo"), listener));
    }

    @Test
    public void testRunWithNoResults() throws Exception {
        Tool tool = SearchAnomalyResultsTool.Factory.getInstance().create(Collections.emptyMap());

        SearchHit[] hits = new SearchHit[0];

        TotalHits totalHits = new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO);

        SearchResponse getResultsResponse = new SearchResponse(
            new SearchResponseSections(new SearchHits(hits, totalHits, 0), new Aggregations(new ArrayList<>()), null, false, null, null, 0),
            null,
            0,
            0,
            0,
            0,
            null,
            null
        );
        String expectedResponseStr = String.format(Locale.getDefault(), "AnomalyResults=[]TotalAnomalyResults=%d", hits.length);

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);

        doAnswer((invocation) -> {
            ActionListener<SearchResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(getResultsResponse);
            return null;
        }).when(nodeClient).execute(any(ActionType.class), any(), any());

        tool.run(emptyParams, listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(expectedResponseStr, responseCaptor.getValue());
    }

    @Test
    public void testRunWithSingleResult() throws Exception {
        final String detectorId = "detector-1-id";
        final double anomalyGrade = 0.5;
        final double confidence = 0.9;
        Tool tool = SearchAnomalyResultsTool.Factory.getInstance().create(Collections.emptyMap());

        XContentBuilder content = XContentBuilder.builder(XContentType.JSON.xContent());
        content.startObject();
        content.field("detector_id", detectorId);
        content.field("anomaly_grade", anomalyGrade);
        content.field("confidence", confidence);
        content.endObject();
        SearchHit[] hits = new SearchHit[1];
        hits[0] = new SearchHit(0, detectorId, null, null).sourceRef(BytesReference.bytes(content));

        TotalHits totalHits = new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO);

        SearchResponse getResultsResponse = new SearchResponse(
            new SearchResponseSections(new SearchHits(hits, totalHits, 0), new Aggregations(new ArrayList<>()), null, false, null, null, 0),
            null,
            0,
            0,
            0,
            0,
            null,
            null
        );
        String expectedResponseStr = String
            .format(
                "AnomalyResults=[{detectorId=%s,grade=%2.1f,confidence=%2.1f}]TotalAnomalyResults=%d",
                detectorId,
                anomalyGrade,
                confidence,
                hits.length
            );

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);

        doAnswer((invocation) -> {
            ActionListener<SearchResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(getResultsResponse);
            return null;
        }).when(nodeClient).execute(any(ActionType.class), any(), any());

        tool.run(emptyParams, listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(expectedResponseStr, responseCaptor.getValue());
    }

    @Test
    public void testDefaultIndexPatternIsSet() throws Exception {
        Tool tool = SearchAnomalyResultsTool.Factory.getInstance().create(Collections.emptyMap());

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);

        doAnswer((invocation) -> {
            SearchRequest generatedRequest = invocation.getArgument(1);
            String[] indices = generatedRequest.indices();
            assertNotNull(indices);
            assertEquals(1, indices.length);
            assertEquals(ToolConstants.AD_RESULTS_INDEX_PATTERN, indices[0]);
            return null;
        }).when(nodeClient).execute(any(ActionType.class), any(), any());

        tool.run(emptyParams, listener);
    }

    @Test
    public void testValidate() {
        Tool tool = SearchAnomalyResultsTool.Factory.getInstance().create(Collections.emptyMap());
        assertEquals(SearchAnomalyResultsTool.TYPE, tool.getType());
        assertTrue(tool.validate(emptyParams));
        assertTrue(tool.validate(nonEmptyParams));
        assertTrue(tool.validate(nullParams));
    }
}
